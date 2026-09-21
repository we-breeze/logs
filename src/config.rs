use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

pub const DEFAULT_LOG_DIRECTORY: &str = "./logs";
pub const DEFAULT_QUEUE_CAPACITY: usize = 16_384;
pub const DEFAULT_ARENA_CHUNK_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_LINE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    #[default]
    DropNewest,
    Block,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    Interval(Duration),
    IntervalAndError(Duration),
    EveryLine,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RotationPolicy {
    #[default]
    Never,
    Hourly,
    Daily,
}

impl FromStr for RotationPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "never" => Ok(Self::Never),
            "hourly" => Ok(Self::Hourly),
            "daily" => Ok(Self::Daily),
            _ => Err(format!(
                "unsupported log rotation policy {value:?}; expected never, hourly, or daily"
            )),
        }
    }
}

impl Default for FlushPolicy {
    fn default() -> Self {
        Self::IntervalAndError(Duration::from_secs(1))
    }
}

impl FlushPolicy {
    pub(crate) fn interval(self) -> Option<Duration> {
        match self {
            Self::Interval(interval) | Self::IntervalAndError(interval) => Some(interval),
            Self::EveryLine => None,
        }
    }

    pub(crate) fn flush_after_error(self) -> bool {
        matches!(self, Self::IntervalAndError(_) | Self::EveryLine)
    }

    pub(crate) fn flush_after_every_line(self) -> bool {
        self == Self::EveryLine
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsConfig {
    pub directory: PathBuf,
    pub filter: String,
    pub queue_capacity: usize,
    pub arena_chunk_bytes: usize,
    pub flush_policy: FlushPolicy,
    pub rotation_policy: RotationPolicy,
    pub overflow_policy: OverflowPolicy,
    pub max_line_bytes: usize,
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from(DEFAULT_LOG_DIRECTORY),
            filter: "info".to_string(),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            arena_chunk_bytes: DEFAULT_ARENA_CHUNK_BYTES,
            flush_policy: FlushPolicy::default(),
            rotation_policy: RotationPolicy::default(),
            overflow_policy: OverflowPolicy::default(),
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
        }
    }
}

impl LogsConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();
        if let Some(directory) = std::env::var_os("BREEZE_LOG_DIR")
            && !directory.is_empty()
        {
            config.directory = PathBuf::from(directory);
        }
        if let Ok(filter) = std::env::var("RUST_LOG")
            && !filter.trim().is_empty()
        {
            config.filter = filter;
        }
        if let Ok(rotation_policy) = std::env::var("BREEZE_LOG_ROTATION")
            && let Ok(rotation_policy) = rotation_policy.parse()
        {
            config.rotation_policy = rotation_policy;
        }
        config
    }

    pub fn with_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.directory = directory.into();
        self
    }

    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = filter.into();
        self
    }

    pub fn with_queue_capacity(mut self, queue_capacity: usize) -> Self {
        self.queue_capacity = queue_capacity;
        self
    }

    pub fn with_arena_chunk_bytes(mut self, arena_chunk_bytes: usize) -> Self {
        self.arena_chunk_bytes = arena_chunk_bytes;
        self
    }

    pub fn with_flush_policy(mut self, flush_policy: FlushPolicy) -> Self {
        self.flush_policy = flush_policy;
        self
    }

    pub fn with_rotation_policy(mut self, rotation_policy: RotationPolicy) -> Self {
        self.rotation_policy = rotation_policy;
        self
    }

    pub fn with_overflow_policy(mut self, overflow_policy: OverflowPolicy) -> Self {
        self.overflow_policy = overflow_policy;
        self
    }

    pub fn with_max_line_bytes(mut self, max_line_bytes: usize) -> Self {
        self.max_line_bytes = max_line_bytes;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.directory.as_os_str().is_empty() {
            return Err("log directory must not be empty".to_string());
        }
        if self.filter.trim().is_empty() {
            return Err("log filter must not be empty".to_string());
        }
        if self.queue_capacity == 0 {
            return Err("log queue capacity must be greater than zero".to_string());
        }
        if self.arena_chunk_bytes == 0 || self.arena_chunk_bytes >= u32::MAX as usize {
            return Err(format!(
                "log arena chunk size must be in 1..{} bytes",
                u32::MAX
            ));
        }
        if self.max_line_bytes < 64 {
            return Err("maximum log line size must be at least 64 bytes".to_string());
        }
        if self
            .flush_policy
            .interval()
            .is_some_and(|interval| interval.is_zero())
        {
            return Err("log flush interval must be greater than zero".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rotation_policy_case_insensitively() {
        assert_eq!("never".parse(), Ok(RotationPolicy::Never));
        assert_eq!("HOURLY".parse(), Ok(RotationPolicy::Hourly));
        assert_eq!(" daily ".parse(), Ok(RotationPolicy::Daily));
        assert!("weekly".parse::<RotationPolicy>().is_err());
    }
}
