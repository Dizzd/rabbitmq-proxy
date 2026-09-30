use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read configuration {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse configuration {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("invalid configuration: {0}")]
    Validation(String),
}

#[derive(Debug, Error)]
pub enum PublishError {
    #[error("RabbitMQ publisher is not connected")]
    NotReady,
    #[error("RabbitMQ publish failed: {0}")]
    Amqp(#[from] lapin::Error),
    #[error("RabbitMQ publish confirmation timed out")]
    ConfirmTimeout,
    #[error("RabbitMQ negatively acknowledged the message")]
    Nack,
    #[error("RabbitMQ publisher confirms are not enabled")]
    ConfirmNotRequested,
    #[error("RabbitMQ returned the message as unroutable")]
    Unroutable,
}
