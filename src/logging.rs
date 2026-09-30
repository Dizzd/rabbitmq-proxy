use std::{
    fs,
    io::{self, Write},
    path::Path,
};

use tracing::Metadata;
use tracing_appender::non_blocking::{NonBlocking, WorkerGuard};
use tracing_subscriber::{
    EnvFilter, Registry, fmt::writer::MakeWriter, layer::SubscriberExt, reload,
    util::SubscriberInitExt,
};

use crate::config::LoggingConfig;

pub fn payload_hex(payload: &[u8], max_bytes: usize) -> String {
    let shown = &payload[..payload.len().min(max_bytes)];
    let mut output = String::with_capacity(shown.len().saturating_mul(2) + 3);
    for byte in shown {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02X}");
    }
    if payload.len() > shown.len() {
        output.push_str("...");
    }
    output
}

#[derive(Debug, Clone, Copy)]
pub enum LogMode {
    Listener,
    Forwarder,
    All,
}

#[derive(Clone)]
pub struct LogReloadHandle(reload::Handle<EnvFilter, Registry>);

impl LogReloadHandle {
    pub fn reload(&self, level: &str) -> anyhow::Result<()> {
        let filter = EnvFilter::try_new(level)?;
        self.0.reload(filter)?;
        Ok(())
    }
}

pub struct LoggingGuard {
    _guards: Vec<WorkerGuard>,
}

#[derive(Clone)]
struct RoutedMakeWriter {
    mode: LogMode,
    listener: Option<NonBlocking>,
    forwarder: Option<NonBlocking>,
}

enum RoutedWriter {
    File(NonBlocking),
    Sink(io::Sink),
}

impl Write for RoutedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::File(writer) => writer.write(buf),
            Self::Sink(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::File(writer) => writer.flush(),
            Self::Sink(writer) => writer.flush(),
        }
    }
}

impl<'a> MakeWriter<'a> for RoutedMakeWriter {
    type Writer = RoutedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.listener
            .clone()
            .or_else(|| self.forwarder.clone())
            .map_or_else(|| RoutedWriter::Sink(io::sink()), RoutedWriter::File)
    }

    fn make_writer_for(&'a self, metadata: &Metadata<'_>) -> Self::Writer {
        let writer = match self.mode {
            LogMode::Listener => self.listener.clone(),
            LogMode::Forwarder => self.forwarder.clone(),
            LogMode::All if metadata.target().starts_with("forwarder") => self.forwarder.clone(),
            LogMode::All => self.listener.clone(),
        };
        writer.map_or_else(|| RoutedWriter::Sink(io::sink()), RoutedWriter::File)
    }
}

pub fn init(
    config: &LoggingConfig,
    mode: LogMode,
) -> anyhow::Result<(LogReloadHandle, LoggingGuard)> {
    let filter = EnvFilter::try_new(&config.level)?;
    let (filter_layer, reload_handle) = reload::Layer::new(filter);
    let (writer, guards, file_error) = build_writer(config, mode);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(io::stdout),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(writer),
        )
        .try_init()?;

    if let Some(error) = file_error {
        tracing::error!(error = %error, directory = %config.directory.display(), "file_logging_unavailable");
    }

    Ok((
        LogReloadHandle(reload_handle),
        LoggingGuard { _guards: guards },
    ))
}

fn build_writer(
    config: &LoggingConfig,
    mode: LogMode,
) -> (RoutedMakeWriter, Vec<WorkerGuard>, Option<String>) {
    if let Err(error) = fs::create_dir_all(&config.directory) {
        return (
            RoutedMakeWriter {
                mode,
                listener: None,
                forwarder: None,
            },
            Vec::new(),
            Some(error.to_string()),
        );
    }

    let mut guards = Vec::new();
    let mut errors = Vec::new();
    let listener = if matches!(mode, LogMode::Listener | LogMode::All) {
        match open_log(&config.directory, &config.listener_file, &mut guards) {
            Ok(writer) => Some(writer),
            Err(error) => {
                errors.push(error.to_string());
                None
            }
        }
    } else {
        None
    };
    let forwarder = if matches!(mode, LogMode::Forwarder | LogMode::All) {
        match open_log(&config.directory, &config.forwarder_file, &mut guards) {
            Ok(writer) => Some(writer),
            Err(error) => {
                errors.push(error.to_string());
                None
            }
        }
    } else {
        None
    };
    let file_error = (!errors.is_empty()).then(|| errors.join("; "));

    (
        RoutedMakeWriter {
            mode,
            listener,
            forwarder,
        },
        guards,
        file_error,
    )
}

fn open_log(
    directory: &Path,
    file_name: &str,
    guards: &mut Vec<WorkerGuard>,
) -> io::Result<NonBlocking> {
    let appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::NEVER)
        .filename_prefix(file_name)
        .build(directory)
        .map_err(io::Error::other)?;
    let (writer, guard) = tracing_appender::non_blocking(appender);
    guards.push(guard);
    Ok(writer)
}
