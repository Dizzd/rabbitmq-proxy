mod cli;
mod config;
mod error;
mod forwarder;
mod health;
mod listener;
mod logging;
mod rabbitmq;
mod shutdown;

use std::{path::Path, process::ExitCode};

use clap::Parser;
use cli::{Cli, Command};
use config::AppConfig;
use logging::LogMode;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("rabbitmq-proxy: {error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let (mode, path) = match cli.command {
        Command::CheckConfig(args) => {
            return match AppConfig::load(&args.config).await {
                Ok(_) => {
                    println!("Config OK");
                    Ok(())
                }
                Err(error) => anyhow::bail!("Config ERROR: {error}"),
            };
        }
        Command::Listener(args) => (ServiceMode::Listener, args.config),
        Command::Forwarder(args) => (ServiceMode::Forwarder, args.config),
        Command::All(args) => (ServiceMode::All, args.config),
    };

    let loaded = AppConfig::load(&path).await?;
    let log_mode = match mode {
        ServiceMode::Listener => LogMode::Listener,
        ServiceMode::Forwarder => LogMode::Forwarder,
        ServiceMode::All => LogMode::All,
    };
    let (log_reload, _logging_guard) = logging::init(&loaded.logging, log_mode)?;
    log_startup(&loaded, mode, &path);

    let config = config::shared(loaded);
    let shutdown = CancellationToken::new();
    let signal_task = tokio::spawn(shutdown::wait_for_signal(shutdown.clone()));
    let watcher_task = tokio::spawn(config::watch_config(
        path,
        config.clone(),
        log_reload,
        shutdown.clone(),
    ));

    let service_result = match mode {
        ServiceMode::Listener => listener::run(config, shutdown.clone()).await,
        ServiceMode::Forwarder => forwarder::run(config, shutdown.clone()).await,
        ServiceMode::All => run_all(config, shutdown.clone()).await,
    };
    shutdown.cancel();

    match signal_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(%error, "signal_handler_failed"),
        Err(error) => tracing::error!(%error, "signal_task_join_failed"),
    }
    match watcher_task.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(%error, "config_watcher_failed"),
        Err(error) => tracing::error!(%error, "config_watcher_join_failed"),
    }
    service_result
}

async fn run_all(config: config::SharedConfig, shutdown: CancellationToken) -> anyhow::Result<()> {
    let mut services = JoinSet::new();
    let listener_config = config.clone();
    let listener_shutdown = shutdown.clone();
    services.spawn(async move {
        (
            "listener",
            listener::run(listener_config, listener_shutdown).await,
        )
    });
    let forwarder_shutdown = shutdown.clone();
    services.spawn(async move {
        (
            "forwarder",
            forwarder::run(config, forwarder_shutdown).await,
        )
    });

    let mut first_error = None;
    if let Some(result) = services.join_next().await {
        match result {
            Ok((service, Ok(()))) if !shutdown.is_cancelled() => {
                first_error = Some(anyhow::anyhow!("{service} stopped unexpectedly"));
            }
            Ok((_, Ok(()))) => {}
            Ok((service, Err(error))) => {
                first_error = Some(error.context(format!("{service} service failed")));
            }
            Err(error) => first_error = Some(anyhow::Error::from(error)),
        }
    }
    shutdown.cancel();

    while let Some(result) = services.join_next().await {
        match result {
            Ok((_, Ok(()))) => {}
            Ok((service, Err(error))) if first_error.is_none() => {
                first_error = Some(error.context(format!("{service} service failed")));
            }
            Err(error) if first_error.is_none() => {
                first_error = Some(anyhow::Error::from(error));
            }
            _ => {}
        }
    }

    first_error.map_or(Ok(()), Err)
}

#[derive(Debug, Clone, Copy)]
enum ServiceMode {
    Listener,
    Forwarder,
    All,
}

fn log_startup(config: &AppConfig, mode: ServiceMode, path: &Path) {
    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        mode = ?mode,
        config = %path.display(),
        environment = %config.app.environment,
        "RabbitMQ Proxy"
    );
}
