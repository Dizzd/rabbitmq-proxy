use tokio_util::sync::CancellationToken;

pub async fn wait_for_signal(shutdown: CancellationToken) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
            _ = shutdown.cancelled() => return Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = shutdown.cancelled() => return Ok(()),
        }
    }

    tracing::info!("shutdown_signal_received");
    shutdown.cancel();
    Ok(())
}
