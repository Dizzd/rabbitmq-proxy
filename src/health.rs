use std::{net::SocketAddr, sync::Arc};

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Readiness {
    receiver: watch::Receiver<bool>,
    component: &'static str,
}

impl Readiness {
    pub fn new(receiver: watch::Receiver<bool>, component: &'static str) -> Self {
        Self {
            receiver,
            component,
        }
    }

    fn is_ready(&self) -> bool {
        *self.receiver.borrow()
    }
}

pub async fn serve(
    address: SocketAddr,
    readiness: Readiness,
    shutdown: CancellationToken,
    log_target: &'static str,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .with_state(Arc::new(readiness));
    let listener = tokio::net::TcpListener::bind(address).await?;
    if log_target.starts_with("forwarder") {
        tracing::info!(target: "forwarder::health", %address, "health_server_started");
    } else {
        tracing::info!(target: "listener::health", %address, "health_server_started");
    }
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await?;
    Ok(())
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn ready(State(readiness): State<Arc<Readiness>>) -> impl IntoResponse {
    if readiness.is_ready() {
        (
            StatusCode::OK,
            Json(json!({ "status": "ready", (readiness.component): "connected" })),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", (readiness.component): "disconnected" })),
        )
    }
}
