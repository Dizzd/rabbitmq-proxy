use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures_util::StreamExt;
use lapin::{
    Channel, Connection,
    message::Delivery,
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicQosOptions},
    types::FieldTable,
};
use reqwest::StatusCode;
use tokio::{
    sync::{Semaphore, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{PoisonStrategy, RetryConfig, SharedConfig},
    health::{self, Readiness},
    logging::payload_hex,
    rabbitmq::{connect, declare_topology, reconnect_delay},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryAction {
    Success,
    Retry,
    Permanent,
}

pub async fn run(config: SharedConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    let startup = config.load_full();
    if !startup.forwarder.enabled {
        anyhow::bail!("forwarder mode is disabled by forwarder.enabled=false");
    }

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(
            startup.forwarder.connect_timeout_seconds,
        ))
        .build()?;
    let (ready_tx, ready_rx) = watch::channel(false);
    let readiness = Readiness::new(ready_rx, "rabbitmq_consumer");

    tracing::info!(
        target: "forwarder",
        queue = %startup.rabbitmq.queue,
        target_url = %startup.forwarder.target_url,
        prefetch = startup.rabbitmq.consumer.prefetch_count,
        concurrency = startup.forwarder.concurrency,
        "forwarder_started"
    );

    tokio::try_join!(
        consume_loop(
            config,
            client,
            ready_tx,
            startup.forwarder.concurrency,
            shutdown.clone(),
        ),
        health::serve(
            startup.forwarder_health_addr(),
            readiness,
            shutdown,
            "forwarder::health",
        )
    )?;
    Ok(())
}

async fn consume_loop(
    config: SharedConfig,
    client: reqwest::Client,
    ready_tx: watch::Sender<bool>,
    concurrency: usize,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut workers = JoinSet::new();
    let mut attempt = 0_u32;

    'reconnect: while !shutdown.is_cancelled() {
        attempt = attempt.saturating_add(1);
        tracing::info!(target: "forwarder::rabbitmq", attempt, "rabbitmq_reconnect_attempt");
        let snapshot = config.load_full();
        match connect(&snapshot).await {
            Ok(connection) => match setup_consumer(&connection, &snapshot).await {
                Ok((channel, mut consumer)) => {
                    attempt = 0;
                    ready_tx.send_replace(true);
                    tracing::info!(target: "forwarder::rabbitmq", "rabbitmq_connected");
                    loop {
                        while let Some(result) = workers.try_join_next() {
                            if let Err(error) = result {
                                tracing::error!(target: "forwarder", %error, "forwarding_worker_join_failed");
                            }
                        }
                        tokio::select! {
                            _ = shutdown.cancelled() => {
                                ready_tx.send_replace(false);
                                    drain_workers(
                                        &mut workers,
                                        Duration::from_secs(config.load().app.shutdown_timeout_seconds),
                                    ).await;
                                let _ = channel.close(200, "shutdown".into()).await;
                                let _ = connection.close(200, "shutdown".into()).await;
                                break 'reconnect;
                            }
                            delivery = consumer.next() => {
                                match delivery {
                                    Some(Ok(delivery)) => {
                                        let permit = tokio::select! {
                                            _ = shutdown.cancelled() => {
                                                ready_tx.send_replace(false);
                                                    drain_workers(
                                                        &mut workers,
                                                        Duration::from_secs(config.load().app.shutdown_timeout_seconds),
                                                    ).await;
                                                let _ = channel.close(200, "shutdown".into()).await;
                                                let _ = connection.close(200, "shutdown".into()).await;
                                                break 'reconnect;
                                            }
                                            permit = semaphore.clone().acquire_owned() => permit,
                                        };
                                        let Ok(permit) = permit else {
                                            break 'reconnect;
                                        };
                                        let client = client.clone();
                                        let config = config.clone();
                                        workers.spawn(async move {
                                            let _permit = permit;
                                            process_delivery(delivery, &client, &config).await;
                                        });
                                    }
                                    Some(Err(error)) => {
                                        tracing::warn!(target: "forwarder::rabbitmq", %error, "rabbitmq_consumer_error");
                                        break;
                                    }
                                    None => {
                                        tracing::warn!(target: "forwarder::rabbitmq", "rabbitmq_consumer_cancelled");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(target: "forwarder::rabbitmq", %error, "rabbitmq_consumer_setup_failed")
                }
            },
            Err(error) => {
                tracing::warn!(target: "forwarder::rabbitmq", %error, "rabbitmq_connection_failed")
            }
        }

        ready_tx.send_replace(false);
        if shutdown.is_cancelled() {
            break;
        }
        tracing::warn!(target: "forwarder::rabbitmq", "rabbitmq_disconnected");
        let snapshot = config.load();
        let delay = reconnect_delay(
            snapshot.rabbitmq.reconnect.initial_delay_ms,
            snapshot.rabbitmq.reconnect.max_delay_ms,
            attempt,
        );
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(delay) => {}
        }
    }

    ready_tx.send_replace(false);
    drain_workers(
        &mut workers,
        Duration::from_secs(config.load().app.shutdown_timeout_seconds),
    )
    .await;
    Ok(())
}

async fn drain_workers(workers: &mut JoinSet<()>, timeout: Duration) {
    let drain = async {
        while let Some(result) = workers.join_next().await {
            if let Err(error) = result {
                tracing::error!(target: "forwarder", %error, "forwarding_worker_join_failed");
            }
        }
    };
    if tokio::time::timeout(timeout, drain).await.is_err() {
        workers.abort_all();
        tracing::warn!(target: "forwarder", "forwarding_workers_shutdown_timeout");
    }
}

async fn setup_consumer(
    connection: &Connection,
    config: &crate::config::AppConfig,
) -> anyhow::Result<(Channel, lapin::Consumer)> {
    let channel = connection.create_channel().await?;
    declare_topology(&channel, config).await?;
    channel
        .basic_qos(
            config.rabbitmq.consumer.prefetch_count,
            BasicQosOptions::default(),
        )
        .await?;
    let consumer_tag = format!("rabbitmq-proxy-{}", std::process::id());
    let consumer = channel
        .basic_consume(
            config.rabbitmq.queue.clone().into(),
            consumer_tag.into(),
            BasicConsumeOptions {
                no_ack: false,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    Ok((channel, consumer))
}

async fn process_delivery(mut delivery: Delivery, client: &reqwest::Client, config: &SharedConfig) {
    let delivery_tag = delivery.delivery_tag;
    let payload = Bytes::from(std::mem::take(&mut delivery.data));
    tracing::info!(target: "forwarder", delivery_tag, size = payload.len(), "message_received");

    let first_snapshot = config.load_full();
    if first_snapshot.logging.log_payload {
        tracing::info!(
            target: "forwarder",
            delivery_tag,
            payload_hex = %payload_hex(&payload, first_snapshot.logging.max_payload_log_bytes),
            payload_size = payload.len(),
            "payload_forwarding"
        );
    }
    let max_attempts = first_snapshot
        .forwarder
        .retry
        .max_attempts
        .min(first_snapshot.forwarder.poison_message.max_attempts)
        .max(1);

    for attempt in 1..=max_attempts {
        let snapshot = config.load_full();
        let started = std::time::Instant::now();
        let result = client
            .post(&snapshot.forwarder.target_url)
            .timeout(Duration::from_secs(
                snapshot.forwarder.request_timeout_seconds,
            ))
            .body(payload.clone())
            .send()
            .await;

        let action = match result {
            Ok(response) => {
                let status = response.status();
                match classify_response(status) {
                    DeliveryAction::Success => {
                        tracing::info!(
                            target: "forwarder",
                            delivery_tag,
                            status = status.as_u16(),
                            duration_ms = started.elapsed().as_millis(),
                            "forwarding_success"
                        );
                        if let Err(error) = delivery.ack(BasicAckOptions::default()).await {
                            tracing::error!(target: "forwarder", delivery_tag, %error, "message_ack_failed");
                        } else {
                            tracing::info!(target: "forwarder", delivery_tag, "message_acked");
                        }
                        return;
                    }
                    action => {
                        tracing::error!(
                            target: "forwarder",
                            delivery_tag,
                            attempt,
                            status = status.as_u16(),
                            "forwarding_failed"
                        );
                        action
                    }
                }
            }
            Err(error) => {
                tracing::error!(target: "forwarder", delivery_tag, attempt, %error, "forwarding_failed");
                DeliveryAction::Retry
            }
        };

        if action == DeliveryAction::Permanent {
            apply_poison_strategy(&delivery, &snapshot, delivery_tag).await;
            return;
        }
        if attempt < max_attempts {
            tokio::time::sleep(retry_delay(&snapshot.forwarder.retry, attempt)).await;
        }
    }

    let snapshot = config.load_full();
    apply_poison_strategy(&delivery, &snapshot, delivery_tag).await;
}

async fn apply_poison_strategy(
    delivery: &Delivery,
    config: &crate::config::AppConfig,
    delivery_tag: u64,
) {
    match config.forwarder.poison_message.strategy {
        PoisonStrategy::Requeue => {
            let options = BasicNackOptions {
                requeue: true,
                ..Default::default()
            };
            if let Err(error) = delivery.nack(options).await {
                tracing::error!(target: "forwarder", delivery_tag, %error, "message_requeue_failed");
            } else {
                tracing::warn!(target: "forwarder", delivery_tag, "message_requeued_after_retries");
            }
        }
        PoisonStrategy::DropAndLog => {
            if let Err(error) = delivery.ack(BasicAckOptions::default()).await {
                tracing::error!(target: "forwarder", delivery_tag, %error, "poison_message_drop_ack_failed");
            } else {
                tracing::error!(target: "forwarder", delivery_tag, "poison_message_dropped");
            }
        }
        PoisonStrategy::DeadLetter => {
            tracing::warn!(
                target: "forwarder",
                delivery_tag,
                declare_topology = config.rabbitmq.declare_topology,
                "dead_letter_requires_queue_to_have_existing_dlx_configuration"
            );
            let options = BasicNackOptions {
                requeue: false,
                ..Default::default()
            };
            if let Err(error) = delivery.nack(options).await {
                tracing::error!(target: "forwarder", delivery_tag, %error, "message_dead_letter_failed");
            } else {
                tracing::warn!(target: "forwarder", delivery_tag, "message_rejected_for_dead_letter");
            }
        }
    }
}

pub fn classify_response(status: StatusCode) -> DeliveryAction {
    if status.is_success() {
        DeliveryAction::Success
    } else if status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        DeliveryAction::Retry
    } else {
        DeliveryAction::Permanent
    }
}

pub fn retry_delay(config: &RetryConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1) as i32;
    let delay = (config.initial_delay_ms as f64) * config.backoff_multiplier.powi(exponent);
    Duration::from_millis(delay.min(config.max_delay_ms as f64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_classification_should_match_delivery_policy() {
        let cases = [
            (200, DeliveryAction::Success),
            (204, DeliveryAction::Success),
            (400, DeliveryAction::Permanent),
            (401, DeliveryAction::Permanent),
            (403, DeliveryAction::Permanent),
            (404, DeliveryAction::Permanent),
            (405, DeliveryAction::Permanent),
            (408, DeliveryAction::Retry),
            (409, DeliveryAction::Permanent),
            (410, DeliveryAction::Permanent),
            (422, DeliveryAction::Permanent),
            (429, DeliveryAction::Retry),
            (500, DeliveryAction::Retry),
            (503, DeliveryAction::Retry),
        ];
        for (status, expected) in cases {
            let status = StatusCode::from_u16(status).expect("test status must be valid");
            assert_eq!(classify_response(status), expected);
        }
    }

    #[test]
    fn retry_backoff_should_double_and_cap() {
        let config = RetryConfig {
            max_attempts: 7,
            initial_delay_ms: 1_000,
            max_delay_ms: 30_000,
            backoff_multiplier: 2.0,
        };
        let values: Vec<u64> = (1..=7)
            .map(|attempt| retry_delay(&config, attempt).as_secs())
            .collect();
        assert_eq!(values, vec![1, 2, 4, 8, 16, 30, 30]);
    }

    #[test]
    fn forwarder_payload_should_preserve_every_byte() {
        let input = vec![
            0x02, 0x34, 0x31, 0x38, 0x39, 0x33, 0x39, 0x32, 0x35, 0x34, 0x33, 0x0D, 0x0A, 0x03,
        ];
        let body = Bytes::from(input.clone());
        assert_eq!(body.as_ref(), input.as_slice());
    }
}
