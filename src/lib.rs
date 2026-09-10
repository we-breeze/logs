mod config;
mod format;
mod writer;

use std::io;

pub use config::{
    DEFAULT_ARENA_CHUNK_BYTES, DEFAULT_LOG_DIRECTORY, DEFAULT_MAX_LINE_BYTES,
    DEFAULT_QUEUE_CAPACITY, FlushPolicy, LogsConfig, OverflowPolicy,
};
use thiserror::Error;
use tracing::Dispatch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::prelude::*;
pub use writer::LogsGuard;

#[derive(Debug, Error)]
pub enum InitError {
    #[error("invalid Breeze logs configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid Breeze logs filter: {0}")]
    InvalidFilter(String),
    #[error("failed to initialize Breeze log files: {0}")]
    Io(#[from] io::Error),
    #[error("a global tracing subscriber is already installed")]
    GlobalSubscriberAlreadyInstalled,
}

pub fn init_default() -> Result<LogsGuard, InitError> {
    init(LogsConfig::from_env())
}

pub fn init(config: LogsConfig) -> Result<LogsGuard, InitError> {
    let (dispatch, guard) = build_dispatch(config)?;
    tracing::dispatcher::set_global_default(dispatch)
        .map_err(|_| InitError::GlobalSubscriberAlreadyInstalled)?;
    Ok(guard)
}

fn build_dispatch(config: LogsConfig) -> Result<(Dispatch, LogsGuard), InitError> {
    config.validate().map_err(InitError::InvalidConfig)?;
    let filter = EnvFilter::try_new(&config.filter)
        .map_err(|error| InitError::InvalidFilter(error.to_string()))?;
    let (make_writer, guard) = writer::start(&config)?;
    let layer = tracing_subscriber::fmt::layer()
        .event_format(format::BreezeEventFormat)
        .with_ansi(false)
        .with_writer(make_writer)
        .with_filter(filter);
    let subscriber = tracing_subscriber::registry().with(layer);
    Ok((Dispatch::new(subscriber), guard))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use super::*;

    #[test]
    fn defaults_match_the_example_runtime_contract() {
        let config = LogsConfig::default();

        assert_eq!(config.directory.to_string_lossy(), "./logs");
        assert_eq!(config.filter, "info");
        assert_eq!(config.arena_chunk_bytes, DEFAULT_ARENA_CHUNK_BYTES);
        assert_eq!(config.flush_policy, FlushPolicy::default());
        assert_eq!(config.overflow_policy, OverflowPolicy::DropNewest);
    }

    #[test]
    fn routes_each_level_to_one_file_without_console_output() {
        let directory = tempfile::tempdir().unwrap();
        let config = LogsConfig::default()
            .with_directory(directory.path())
            .with_filter("trace")
            .with_flush_policy(FlushPolicy::EveryLine);
        let (dispatch, guard) = build_dispatch(config).unwrap();

        tracing::dispatcher::with_default(&dispatch, || {
            tracing::debug!("debug detail");
            tracing::info!(
                "ListStorage get new version. listId:6296, version:1684410575955, hash:4185ad0d, length:10000"
            );
            tracing::warn!("configuration is stale");
            tracing::error!("refresh failed");
        });
        guard.flush().unwrap();

        let info = fs::read_to_string(directory.path().join("info.log")).unwrap();
        let warn = fs::read_to_string(directory.path().join("warn.log")).unwrap();
        let error = fs::read_to_string(directory.path().join("error.log")).unwrap();
        assert!(info.contains(" [DEBUG] debug detail"));
        assert!(info.contains(" [INFO] ListStorage get new version."));
        assert!(!info.contains("configuration is stale"));
        assert!(!info.contains("refresh failed"));
        assert!(warn.contains(" [WARN] configuration is stale"));
        assert!(!warn.contains("refresh failed"));
        assert!(error.contains(" [ERROR] refresh failed"));
        for output in [&info, &warn, &error] {
            assert!(!output.contains("+08:00"));
            assert!(!output.contains("Asia/Shanghai"));
            assert!(!output.contains("CST"));
        }
    }

    #[test]
    fn rejects_unbounded_or_non_flushing_configuration() {
        let config = LogsConfig::default().with_queue_capacity(0);
        assert!(matches!(
            build_dispatch(config),
            Err(InitError::InvalidConfig(_))
        ));

        let config = LogsConfig::default().with_flush_policy(FlushPolicy::Interval(Duration::ZERO));
        assert!(matches!(
            build_dispatch(config),
            Err(InitError::InvalidConfig(_))
        ));

        let config = LogsConfig::default().with_arena_chunk_bytes(0);
        assert!(matches!(
            build_dispatch(config),
            Err(InitError::InvalidConfig(_))
        ));
    }
}
