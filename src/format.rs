use std::fmt;

use time::{OffsetDateTime, UtcOffset};
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone, Copy)]
pub(crate) struct BreezeEventFormat;

impl<S, N> FormatEvent<S, N> for BreezeEventFormat
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        context: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        write_timestamp(&mut writer, shanghai_now())?;
        write!(&mut writer, " [{}] ", event.metadata().level())?;
        context
            .field_format()
            .format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

fn shanghai_now() -> OffsetDateTime {
    OffsetDateTime::now_utc().to_offset(shanghai_offset())
}

fn shanghai_offset() -> UtcOffset {
    UtcOffset::from_hms(8, 0, 0).expect("UTC+8 is a valid fixed offset")
}

pub(crate) fn write_timestamp(
    writer: &mut impl fmt::Write,
    timestamp: OffsetDateTime,
) -> fmt::Result {
    write!(
        writer,
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        timestamp.year(),
        timestamp.month() as u8,
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_fixed_utc_plus_eight_without_a_zone_suffix() {
        let timestamp = OffsetDateTime::from_unix_timestamp(0)
            .unwrap()
            .to_offset(shanghai_offset());
        let mut rendered = String::new();

        write_timestamp(&mut rendered, timestamp).unwrap();

        assert_eq!(rendered, "1970-01-01 08:00:00");
    }
}
