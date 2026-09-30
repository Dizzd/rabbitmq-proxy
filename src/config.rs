use std::{
    ffi::OsStr,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use arc_swap::ArcSwap;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{error::ConfigError, logging::LogReloadHandle};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub app: AppSection,
    pub listener: ListenerConfig,
    pub forwarder: ForwarderConfig,
    pub rabbitmq: RabbitMqConfig,
    pub logging: LoggingConfig,
    pub health: HealthConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppSection {
    pub environment: String,
    pub shutdown_timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListenerConfig {
    pub enabled: bool,
    pub bind: IpAddr,
    pub port: u16,
    pub path: String,
    pub allowed_ips: Vec<IpAddr>,
    pub max_body_size: usize,
    pub request_timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForwarderConfig {
    pub enabled: bool,
    pub target_url: String,
    pub connect_timeout_seconds: u64,
    pub request_timeout_seconds: u64,
    pub concurrency: usize,
    pub retry: RetryConfig,
    pub poison_message: PoisonMessageConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoisonMessageConfig {
    pub strategy: PoisonStrategy,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoisonStrategy {
    Requeue,
    DropAndLog,
    DeadLetter,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RabbitMqConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub virtual_host: String,
    pub exchange: String,
    pub routing_key: String,
    pub queue: String,
    pub heartbeat_seconds: u16,
    pub connection_timeout_seconds: u64,
    pub declare_topology: bool,
    pub reconnect: ReconnectConfig,
    pub publisher: PublisherConfig,
    pub consumer: ConsumerConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectConfig {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublisherConfig {
    pub confirm_timeout_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    pub prefetch_count: u16,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingConfig {
    pub level: String,
    pub directory: PathBuf,
    pub listener_file: String,
    pub forwarder_file: String,
    pub log_payload: bool,
    pub max_payload_log_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthConfig {
    pub listener: EndpointConfig,
    pub forwarder: EndpointConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointConfig {
    pub bind: IpAddr,
    pub port: u16,
}

impl AppConfig {
    pub async fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = tokio::fs::read_to_string(path)
            .await
            .map_err(|source| ConfigError::Read {
                path: path.to_path_buf(),
                source,
            })?;
        let config = serde_yaml::from_str::<Self>(&raw).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_port("listener.port", self.listener.port)?;
        validate_port("health.listener.port", self.health.listener.port)?;
        validate_port("health.forwarder.port", self.health.forwarder.port)?;
        validate_port("rabbitmq.port", self.rabbitmq.port)?;

        if self.app.shutdown_timeout_seconds == 0 {
            return validation("app.shutdown_timeout_seconds must be greater than zero");
        }

        if !self.listener.path.starts_with('/') {
            return validation("listener.path must start with '/'");
        }
        if self.listener.max_body_size == 0 {
            return validation("listener.max_body_size must be greater than zero");
        }
        if self.listener.request_timeout_seconds == 0 {
            return validation("listener.request_timeout_seconds must be greater than zero");
        }
        if self.forwarder.concurrency == 0 {
            return validation("forwarder.concurrency must be at least 1");
        }
        if self.forwarder.connect_timeout_seconds == 0
            || self.forwarder.request_timeout_seconds == 0
        {
            return validation("forwarder HTTP timeouts must be greater than zero");
        }
        validate_http_url(&self.forwarder.target_url)?;
        validate_retry(&self.forwarder.retry)?;
        if self.forwarder.poison_message.max_attempts == 0 {
            return validation("forwarder.poison_message.max_attempts must be at least 1");
        }
        if self.rabbitmq.host.trim().is_empty()
            || self.rabbitmq.username.is_empty()
            || self.rabbitmq.exchange.trim().is_empty()
            || self.rabbitmq.routing_key.trim().is_empty()
            || self.rabbitmq.queue.trim().is_empty()
        {
            return validation(
                "RabbitMQ host, username, exchange, routing_key, and queue must not be empty",
            );
        }
        if self.rabbitmq.heartbeat_seconds == 0
            || self.rabbitmq.connection_timeout_seconds == 0
            || self.rabbitmq.publisher.confirm_timeout_seconds == 0
        {
            return validation("RabbitMQ heartbeat and timeouts must be greater than zero");
        }
        if self.rabbitmq.consumer.prefetch_count == 0 {
            return validation("rabbitmq.consumer.prefetch_count must be at least 1");
        }
        if self.rabbitmq.reconnect.initial_delay_ms == 0
            || self.rabbitmq.reconnect.max_delay_ms == 0
            || self.rabbitmq.reconnect.initial_delay_ms > self.rabbitmq.reconnect.max_delay_ms
        {
            return validation("RabbitMQ reconnect delays are invalid");
        }
        if self.logging.max_payload_log_bytes == 0 {
            return validation("logging.max_payload_log_bytes must be greater than zero");
        }
        tracing_subscriber::EnvFilter::try_new(&self.logging.level).map_err(|error| {
            ConfigError::Validation(format!("invalid logging.level filter: {error}"))
        })?;
        self.amqp_uri()?;
        Ok(())
    }

    pub fn listener_addr(&self) -> SocketAddr {
        SocketAddr::new(self.listener.bind, self.listener.port)
    }

    pub fn listener_health_addr(&self) -> SocketAddr {
        SocketAddr::new(self.health.listener.bind, self.health.listener.port)
    }

    pub fn forwarder_health_addr(&self) -> SocketAddr {
        SocketAddr::new(self.health.forwarder.bind, self.health.forwarder.port)
    }

    pub fn amqp_uri(&self) -> Result<String, ConfigError> {
        let mut url = Url::parse("amqp://localhost").map_err(|error| {
            ConfigError::Validation(format!("cannot initialize AMQP URL: {error}"))
        })?;
        url.set_host(Some(&self.rabbitmq.host))
            .map_err(|error| ConfigError::Validation(format!("invalid RabbitMQ host: {error}")))?;
        url.set_port(Some(self.rabbitmq.port))
            .map_err(|()| ConfigError::Validation("invalid RabbitMQ port".into()))?;
        url.set_username(&self.rabbitmq.username)
            .map_err(|()| ConfigError::Validation("invalid RabbitMQ username".into()))?;
        url.set_password(Some(&self.rabbitmq.password))
            .map_err(|()| ConfigError::Validation("invalid RabbitMQ password".into()))?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|()| ConfigError::Validation("invalid RabbitMQ virtual_host".into()))?;
            segments.clear().push(&self.rabbitmq.virtual_host);
        }
        url.query_pairs_mut()
            .append_pair("heartbeat", &self.rabbitmq.heartbeat_seconds.to_string());
        Ok(url.into())
    }

    pub fn restart_required_changes(&self, next: &Self) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.listener.bind != next.listener.bind {
            fields.push("listener.bind");
        }
        if self.listener.port != next.listener.port {
            fields.push("listener.port");
        }
        if self.listener.path != next.listener.path {
            fields.push("listener.path");
        }
        if self.health != next.health {
            fields.push("health");
        }
        if self.rabbitmq.host != next.rabbitmq.host
            || self.rabbitmq.port != next.rabbitmq.port
            || self.rabbitmq.username != next.rabbitmq.username
            || self.rabbitmq.password != next.rabbitmq.password
            || self.rabbitmq.virtual_host != next.rabbitmq.virtual_host
            || self.rabbitmq.exchange != next.rabbitmq.exchange
            || self.rabbitmq.routing_key != next.rabbitmq.routing_key
            || self.rabbitmq.queue != next.rabbitmq.queue
            || self.rabbitmq.declare_topology != next.rabbitmq.declare_topology
            || self.rabbitmq.consumer != next.rabbitmq.consumer
        {
            fields.push("rabbitmq connection/topology/consumer");
        }
        if self.forwarder.connect_timeout_seconds != next.forwarder.connect_timeout_seconds {
            fields.push("forwarder.connect_timeout_seconds");
        }
        if self.forwarder.concurrency != next.forwarder.concurrency {
            fields.push("forwarder.concurrency");
        }
        if self.logging.directory != next.logging.directory
            || self.logging.listener_file != next.logging.listener_file
            || self.logging.forwarder_file != next.logging.forwarder_file
        {
            fields.push("logging file paths");
        }
        fields
    }
}

pub type SharedConfig = Arc<ArcSwap<AppConfig>>;

pub fn shared(config: AppConfig) -> SharedConfig {
    Arc::new(ArcSwap::from_pointee(config))
}

pub async fn watch_config(
    path: PathBuf,
    current: SharedConfig,
    log_reload: LogReloadHandle,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let watch_root = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let watched_name = path.file_name().map(ToOwned::to_owned);
    let (tx, mut rx) = mpsc::channel::<notify::Result<Event>>(16);
    let mut watcher = RecommendedWatcher::new(
        move |event| {
            let _ = tx.blocking_send(event);
        },
        notify::Config::default(),
    )?;
    watcher.watch(&watch_root, RecursiveMode::NonRecursive)?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            event = rx.recv() => {
                let Some(event) = event else { break; };
                match event {
                    Ok(event) if should_reload_config(&event, watched_name.as_deref()) => {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        while rx.try_recv().is_ok() {}
                        match AppConfig::load(&path).await {
                            Ok(next) => {
                                let old = current.load_full();
                                if old.as_ref() == &next {
                                    tracing::debug!("configuration_reload_unchanged");
                                    continue;
                                }
                                let restart_fields = old.restart_required_changes(&next);
                                if !restart_fields.is_empty() {
                                    tracing::warn!(fields = ?restart_fields, "configuration_requires_restart");
                                }
                                if let Err(error) = log_reload.reload(&next.logging.level) {
                                    tracing::error!(%error, "configuration_reload_rejected");
                                    continue;
                                }
                                current.store(Arc::new(next));
                                tracing::info!("configuration_reloaded");
                            }
                            Err(error) => tracing::error!(%error, "configuration_reload_invalid"),
                        }
                    }
                    Ok(_) => {}
                    Err(error) => tracing::error!(%error, "configuration_watch_error"),
                }
            }
        }
    }
    drop(watcher);
    Ok(())
}

fn should_reload_config(event: &Event, watched_name: Option<&OsStr>) -> bool {
    if event.need_rescan() {
        return true;
    }

    if !matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    ) {
        return false;
    }

    event
        .paths
        .iter()
        .any(|changed| changed.file_name() == watched_name)
}

fn validate_port(name: &str, port: u16) -> Result<(), ConfigError> {
    if port == 0 {
        return validation(format!("{name} must be between 1 and 65535"));
    }
    Ok(())
}

fn validate_http_url(value: &str) -> Result<(), ConfigError> {
    let url = Url::parse(value).map_err(|error| {
        ConfigError::Validation(format!("invalid forwarder.target_url: {error}"))
    })?;
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        return validation("forwarder.target_url must be an absolute http or https URL");
    }
    Ok(())
}

fn validate_retry(retry: &RetryConfig) -> Result<(), ConfigError> {
    if retry.max_attempts == 0
        || retry.initial_delay_ms == 0
        || retry.max_delay_ms == 0
        || retry.initial_delay_ms > retry.max_delay_ms
        || !retry.backoff_multiplier.is_finite()
        || retry.backoff_multiplier < 1.0
    {
        return validation("forwarder.retry settings are invalid");
    }
    Ok(())
}

fn validation<T>(message: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError::Validation(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, AccessMode, DataChange, ModifyKind};

    fn valid_yaml() -> String {
        include_str!("../config.example.yml").to_owned()
    }

    fn parse(yaml: &str) -> Result<AppConfig, ConfigError> {
        let config: AppConfig =
            serde_yaml::from_str(yaml).map_err(|source| ConfigError::Parse {
                path: PathBuf::from("test.yml"),
                source,
            })?;
        config.validate()?;
        Ok(config)
    }

    #[test]
    fn valid_config_should_pass_validation() {
        assert!(parse(&valid_yaml()).is_ok());
    }

    #[tokio::test]
    async fn missing_config_should_return_read_error() {
        let result = AppConfig::load(Path::new("definitely-missing-config.yml")).await;
        assert!(matches!(result, Err(ConfigError::Read { .. })));
    }

    #[test]
    fn zero_port_should_fail_validation() {
        let yaml = valid_yaml().replace("port: 85", "port: 0");
        assert!(parse(&yaml).is_err());
    }

    #[test]
    fn invalid_ip_should_fail_deserialization() {
        let yaml = valid_yaml().replace("202.151.162.154", "999.1.2.3");
        assert!(parse(&yaml).is_err());
    }

    #[test]
    fn invalid_url_should_fail_validation() {
        let yaml = valid_yaml().replace("http://127.0.0.1:81/pms/", "relative-without-a-host");
        assert!(parse(&yaml).is_err());
    }

    #[test]
    fn invalid_retry_should_fail_validation() {
        let yaml = valid_yaml().replace("backoff_multiplier: 2.0", "backoff_multiplier: 0.5");
        assert!(parse(&yaml).is_err());
    }

    #[test]
    fn zero_concurrency_should_fail_validation() {
        let yaml = valid_yaml().replace("concurrency: 1", "concurrency: 0");
        assert!(parse(&yaml).is_err());
    }

    #[test]
    fn zero_prefetch_should_fail_validation() {
        let yaml = valid_yaml().replace("prefetch_count: 10", "prefetch_count: 0");
        assert!(parse(&yaml).is_err());
    }

    #[test]
    fn zero_max_body_should_fail_validation() {
        let yaml = valid_yaml().replace("max_body_size: 1048576", "max_body_size: 0");
        assert!(parse(&yaml).is_err());
    }

    #[test]
    fn config_read_event_should_not_trigger_reload() {
        let event = Event::new(EventKind::Access(AccessKind::Open(AccessMode::Read)))
            .add_path(PathBuf::from("/etc/rabbitmq-proxy/config.yml"));

        assert!(!should_reload_config(
            &event,
            Some(OsStr::new("config.yml"))
        ));
    }

    #[test]
    fn config_write_event_should_trigger_reload() {
        let event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Content)))
            .add_path(PathBuf::from("/etc/rabbitmq-proxy/config.yml"));

        assert!(should_reload_config(&event, Some(OsStr::new("config.yml"))));
    }

    #[test]
    fn unrelated_file_event_should_not_trigger_reload() {
        let event = Event::new(EventKind::Modify(ModifyKind::Data(DataChange::Content)))
            .add_path(PathBuf::from("/etc/rabbitmq-proxy/other.yml"));

        assert!(!should_reload_config(
            &event,
            Some(OsStr::new("config.yml"))
        ));
    }
}
