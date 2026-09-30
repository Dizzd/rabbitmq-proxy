use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use lapin::{
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
    options::{
        BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{AppConfig, SharedConfig},
    error::PublishError,
};

pub struct PublisherManager {
    channel: RwLock<Option<Channel>>,
    ready_tx: watch::Sender<bool>,
    config: SharedConfig,
}

impl PublisherManager {
    pub fn new(config: SharedConfig) -> Arc<Self> {
        let (ready_tx, _) = watch::channel(false);
        Arc::new(Self {
            channel: RwLock::new(None),
            ready_tx,
            config,
        })
    }

    pub fn readiness(&self) -> watch::Receiver<bool> {
        self.ready_tx.subscribe()
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut attempt = 0_u32;
        while !shutdown.is_cancelled() {
            attempt = attempt.saturating_add(1);
            tracing::info!(target: "listener::rabbitmq", attempt, "rabbitmq_reconnect_attempt");
            let snapshot = self.config.load_full();
            match connect(&snapshot).await {
                Ok(connection) => match setup_publisher(&connection, &snapshot).await {
                    Ok(channel) => {
                        attempt = 0;
                        self.set_channel(Some(channel.clone())).await;
                        tracing::info!(target: "listener::rabbitmq", "rabbitmq_connected");
                        wait_for_disconnect(&connection, &shutdown).await;
                        self.set_channel(None).await;
                        if !shutdown.is_cancelled() {
                            tracing::warn!(target: "listener::rabbitmq", "rabbitmq_disconnected");
                        }
                        if shutdown.is_cancelled() {
                            let _ = channel.close(200, "shutdown".into()).await;
                            let _ = connection.close(200, "shutdown".into()).await;
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(target: "listener::rabbitmq", %error, "rabbitmq_setup_failed");
                    }
                },
                Err(error) => {
                    tracing::warn!(target: "listener::rabbitmq", %error, "rabbitmq_connection_failed");
                }
            }

            let snapshot = self.config.load();
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
        self.set_channel(None).await;
    }

    pub async fn publish(&self, payload: Bytes) -> Result<(), PublishError> {
        let channel = self
            .channel
            .read()
            .await
            .clone()
            .ok_or(PublishError::NotReady)?;
        let snapshot = self.config.load();
        let publish = channel
            .basic_publish(
                snapshot.rabbitmq.exchange.clone().into(),
                snapshot.rabbitmq.routing_key.clone().into(),
                BasicPublishOptions {
                    mandatory: true,
                    ..Default::default()
                },
                &payload,
                BasicProperties::default(),
            )
            .await?;
        let timeout = Duration::from_secs(snapshot.rabbitmq.publisher.confirm_timeout_seconds);
        let confirmation = tokio::time::timeout(timeout, publish)
            .await
            .map_err(|_| PublishError::ConfirmTimeout)??;

        match confirmation {
            lapin::Confirmation::Ack(None) => Ok(()),
            lapin::Confirmation::Ack(Some(_)) => Err(PublishError::Unroutable),
            lapin::Confirmation::Nack(_) => Err(PublishError::Nack),
            lapin::Confirmation::NotRequested => Err(PublishError::ConfirmNotRequested),
        }
    }

    async fn set_channel(&self, channel: Option<Channel>) {
        let ready = channel.is_some();
        *self.channel.write().await = channel;
        self.ready_tx.send_replace(ready);
    }
}

pub async fn connect(config: &AppConfig) -> anyhow::Result<Connection> {
    let uri = config.amqp_uri()?;
    let timeout = Duration::from_secs(config.rabbitmq.connection_timeout_seconds);
    tokio::time::timeout(
        timeout,
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .map_err(|_| anyhow::anyhow!("RabbitMQ connection timed out after {timeout:?}"))?
    .map_err(Into::into)
}

pub async fn declare_topology(channel: &Channel, config: &AppConfig) -> anyhow::Result<()> {
    if !config.rabbitmq.declare_topology {
        return Ok(());
    }
    channel
        .exchange_declare(
            config.rabbitmq.exchange.clone().into(),
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_declare(
            config.rabbitmq.queue.clone().into(),
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await?;
    channel
        .queue_bind(
            config.rabbitmq.queue.clone().into(),
            config.rabbitmq.exchange.clone().into(),
            config.rabbitmq.routing_key.clone().into(),
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;
    tracing::info!(declare_topology = true, "rabbitmq_topology_declared");
    Ok(())
}

pub fn reconnect_delay(initial_ms: u64, max_ms: u64, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(63);
    let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    Duration::from_millis(initial_ms.saturating_mul(multiplier).min(max_ms))
}

async fn setup_publisher(connection: &Connection, config: &AppConfig) -> anyhow::Result<Channel> {
    let channel = connection.create_channel().await?;
    declare_topology(&channel, config).await?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await?;
    Ok(channel)
}

async fn wait_for_disconnect(connection: &Connection, shutdown: &CancellationToken) {
    loop {
        if !connection.status().connected() {
            break;
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backoff_should_double_and_cap() {
        let values: Vec<u64> = (1..=7)
            .map(|attempt| reconnect_delay(1_000, 30_000, attempt).as_secs())
            .collect();
        assert_eq!(values, vec![1, 2, 4, 8, 16, 30, 30]);
    }
}
