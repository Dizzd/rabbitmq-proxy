use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "rabbitmq-proxy", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the HTTP-to-RabbitMQ listener.
    Listener(ConfigArg),
    /// Run the RabbitMQ-to-HTTP forwarder.
    Forwarder(ConfigArg),
    /// Run listener and forwarder in one process.
    All(ConfigArg),
    /// Validate the configuration and exit.
    CheckConfig(ConfigArg),
}

#[derive(Debug, clap::Args)]
pub struct ConfigArg {
    /// Path to the shared YAML configuration.
    #[arg(long, default_value = "config.yml")]
    pub config: PathBuf,
}
