use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::SharedConfig,
    error::PublishError,
    health::{self, Readiness},
    logging::payload_hex,
    rabbitmq::PublisherManager,
};

#[derive(Clone)]
struct ListenerState {
    config: SharedConfig,
    publisher: Arc<PublisherManager>,
}

pub async fn run(config: SharedConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    let startup = config.load_full();
    if !startup.listener.enabled {
        anyhow::bail!("listener mode is disabled by listener.enabled=false");
    }

    let publisher = PublisherManager::new(config.clone());
    let publisher_task = tokio::spawn(publisher.clone().run(shutdown.child_token()));
    let readiness = Readiness::new(publisher.readiness(), "rabbitmq");
    let state = Arc::new(ListenerState { config, publisher });
    let app = Router::new()
        .route(&startup.listener.path, post(receive))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(startup.listener_addr()).await?;

    tracing::info!(
        target: "listener",
        listen = %startup.listener_addr(),
        rabbitmq = %format_args!("{}:{}", startup.rabbitmq.host, startup.rabbitmq.port),
        exchange = %startup.rabbitmq.exchange,
        routing_key = %startup.rabbitmq.routing_key,
        "listener_started"
    );

    let http = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown.clone().cancelled_owned());
    let result = tokio::try_join!(
        async { http.await.map_err(anyhow::Error::from) },
        health::serve(
            startup.listener_health_addr(),
            readiness,
            shutdown.clone(),
            "listener::health",
        )
    );
    shutdown.cancel();
    if let Err(error) = publisher_task.await {
        tracing::error!(target: "listener", %error, "publisher_task_join_failed");
    }
    result.map(|_| ())
}

async fn receive(
    State(state): State<Arc<ListenerState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request<Body>,
) -> Response {
    let snapshot = state.config.load_full();
    if !is_allowed(peer.ip(), &snapshot.listener.allowed_ips) {
        tracing::warn!(target: "listener", source_ip = %peer.ip(), "request_forbidden");
        return text_response(StatusCode::FORBIDDEN, "Forbidden");
    }

    if let Some(length) = request
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        && length > snapshot.listener.max_body_size
    {
        return text_response(StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large");
    }

    let timeout = Duration::from_secs(snapshot.listener.request_timeout_seconds);
    let result = tokio::time::timeout(timeout, async {
        let payload = to_bytes(request.into_body(), snapshot.listener.max_body_size)
            .await
            .map_err(ReceiveError::Body)?;
        let size = payload.len();
        tracing::info!(target: "listener", source_ip = %peer.ip(), size, "request_received");
        if snapshot.logging.log_payload {
            tracing::info!(
                target: "listener",
                payload_hex = %payload_hex(&payload, snapshot.logging.max_payload_log_bytes),
                payload_size = size,
                "payload_received"
            );
        }
        let started = std::time::Instant::now();
        state.publisher.publish(payload).await?;
        tracing::info!(
            target: "listener",
            size,
            duration_ms = started.elapsed().as_millis(),
            "rabbitmq_publish_success"
        );
        Ok::<(), ReceiveError>(())
    })
    .await;

    match result {
        Ok(Ok(())) => text_response(StatusCode::OK, "OK"),
        Ok(Err(ReceiveError::Body(error))) => {
            tracing::warn!(target: "listener", %error, "request_body_rejected");
            text_response(StatusCode::PAYLOAD_TOO_LARGE, "Payload Too Large")
        }
        Ok(Err(ReceiveError::Publish(PublishError::ConfirmTimeout))) | Err(_) => {
            tracing::warn!(target: "listener", "rabbitmq_publish_confirm_timeout");
            text_response(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable")
        }
        Ok(Err(ReceiveError::Publish(error))) => {
            tracing::warn!(target: "listener", %error, "rabbitmq_publish_failed");
            text_response(StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable")
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ReceiveError {
    #[error("invalid request body: {0}")]
    Body(axum::Error),
    #[error(transparent)]
    Publish(#[from] PublishError),
}

fn is_allowed(source: std::net::IpAddr, allowed: &[std::net::IpAddr]) -> bool {
    allowed.contains(&source)
}

fn text_response(status: StatusCode, body: &'static str) -> Response {
    (status, body).into_response()
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use bytes::Bytes;

    use super::*;

    #[test]
    fn allowed_ip_should_be_accepted() {
        let source = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(is_allowed(source, &[source]));
    }

    #[test]
    fn unknown_ip_should_be_denied() {
        let allowed = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let source = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        assert!(!is_allowed(source, &[allowed]));
    }

    #[test]
    fn listener_payload_should_preserve_every_byte() {
        let input = [
            0x02, 0x34, 0x31, 0x38, 0x39, 0x33, 0x39, 0x32, 0x35, 0x34, 0x33, 0x0D, 0x0A, 0x03,
        ];
        let payload = Bytes::copy_from_slice(&input);
        assert_eq!(payload.as_ref(), input);
    }
}
