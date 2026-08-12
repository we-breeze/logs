// 日志宏已降级为 no-op，无外部依赖。
// 保留宏与函数签名，便于调用方（metrics/ds/context）无需改动即可编译。

#[macro_export]
macro_rules! trace {
    ($($arg:tt)+) => {
        {
            let _ = format_args!($($arg)+);
            ()
        }
    };
}
#[macro_export]
macro_rules! debug {
    ($($arg:tt)+) => {
        {
            let _ = format_args!($($arg)+);
            ()
        }
    };
}
#[macro_export]
macro_rules! info {
    ($($arg:tt)+) => {
        {
            let _ = format_args!($($arg)+);
            ()
        }
    };
}
#[macro_export]
macro_rules! warn {
    ($($arg:tt)+) => {
        {
            let _ = format_args!($($arg)+);
            ()
        }
    };
}
#[macro_export]
macro_rules! error {
    ($($arg:tt)+) => {
        {
            let _ = format_args!($($arg)+);
            ()
        }
    };
}
#[macro_export]
macro_rules! fatal {
    ($($arg:tt)+) => {
        {
            let _ = format_args!($($arg)+);
            ()
        }
    };
}

pub fn log_enabled() -> bool {
    false
}

use std::io::Result;
pub fn init(path: &str, _l: &str) -> Result<()> {
    std::fs::create_dir_all(path)?;
    Ok(())
}
