use std::array;
use std::fs::{File, OpenOptions};
use std::io::{self, IoSlice, Write as _};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use brz_ds::{EphemeralBytesArena, EphemeralBytesMut};
use tracing::{Level, Metadata};
use tracing_subscriber::fmt::MakeWriter;

use crate::{FlushPolicy, LogsConfig, OverflowPolicy};

#[cfg(feature = "metrics")]
use brz_metrics::Metric;

const TRUNCATED_SUFFIX: &[u8] = b"...[truncated]\n";
const MIN_INITIAL_LINE_BYTES: usize = 512;
const MAX_INITIAL_LINE_BYTES: usize = 2 * 1024;
const LOW_USAGE_SAMPLE_COUNT: usize = 512;
const MAX_VECTORED_SLICES: usize = 1024;

#[cfg(feature = "metrics")]
#[derive(Clone, Copy)]
struct LogMetrics {
    queue_dropped: Metric,
}

#[cfg(not(feature = "metrics"))]
#[derive(Clone, Copy)]
struct LogMetrics;

impl LogMetrics {
    #[cfg(feature = "metrics")]
    fn new() -> Self {
        Self {
            queue_dropped: Metric::log("queue_dropped"),
        }
    }

    #[cfg(not(feature = "metrics"))]
    fn new() -> Self {
        Self
    }

    #[inline]
    fn record_queue_drop(&self) {
        #[cfg(feature = "metrics")]
        self.queue_dropped.increment();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Destination {
    Info,
    Warn,
    Error,
}

impl Destination {
    fn for_level(level: &Level) -> Self {
        match *level {
            Level::ERROR => Self::Error,
            Level::WARN => Self::Warn,
            Level::INFO | Level::DEBUG | Level::TRACE => Self::Info,
        }
    }
}

struct QueuedLine {
    destination: Destination,
    bytes: SegmentedLine,
}

enum Command {
    Line(QueuedLine),
    Flush(SyncSender<Result<(), String>>),
    Shutdown(SyncSender<Result<(), String>>),
}

#[derive(Clone, Copy)]
enum EnqueueFailure {
    Full,
    Disconnected,
}

#[derive(Debug)]
struct LineCapacityHint {
    capacity: AtomicUsize,
    low_usage_count: AtomicUsize,
    minimum: usize,
    maximum: usize,
}

impl LineCapacityHint {
    fn new(max_line_bytes: usize) -> Self {
        let minimum = max_line_bytes.min(MIN_INITIAL_LINE_BYTES);
        let maximum = max_line_bytes.min(MAX_INITIAL_LINE_BYTES);
        Self {
            capacity: AtomicUsize::new(minimum),
            low_usage_count: AtomicUsize::new(0),
            minimum,
            maximum,
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.capacity.load(Ordering::Relaxed)
    }

    fn observe(&self, used: usize, overflowed: bool) {
        if overflowed {
            if self.low_usage_count.load(Ordering::Relaxed) != 0 {
                self.low_usage_count.store(0, Ordering::Relaxed);
            }
            if self.capacity() >= self.maximum {
                return;
            }
            let _ = self
                .capacity
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |capacity| {
                    (capacity < self.maximum).then(|| capacity.saturating_mul(2).min(self.maximum))
                });
            return;
        }

        let capacity = self.capacity();
        if capacity <= self.minimum {
            return;
        }
        if used > capacity / 4 {
            if self.low_usage_count.load(Ordering::Relaxed) != 0 {
                self.low_usage_count.store(0, Ordering::Relaxed);
            }
            return;
        }

        let samples = self.low_usage_count.fetch_add(1, Ordering::Relaxed) + 1;
        if samples >= LOW_USAGE_SAMPLE_COUNT
            && self.low_usage_count.swap(0, Ordering::Relaxed) >= LOW_USAGE_SAMPLE_COUNT
        {
            let _ = self
                .capacity
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |capacity| {
                    Some((capacity / 2).max(self.minimum))
                });
        }
    }
}

#[derive(Clone)]
pub(crate) struct LogMakeWriter {
    sender: SyncSender<Command>,
    dropped_lines: Arc<AtomicU64>,
    overflow_policy: OverflowPolicy,
    max_line_bytes: usize,
    arena: EphemeralBytesArena,
    line_capacity: Arc<LineCapacityHint>,
    metrics: LogMetrics,
}

impl<'writer> MakeWriter<'writer> for LogMakeWriter {
    type Writer = EventWriter<'writer>;

    fn make_writer(&'writer self) -> Self::Writer {
        self.writer(Destination::Info)
    }

    fn make_writer_for(&'writer self, metadata: &Metadata<'_>) -> Self::Writer {
        self.writer(Destination::for_level(metadata.level()))
    }
}

impl LogMakeWriter {
    fn writer(&self, destination: Destination) -> EventWriter<'_> {
        let initial_capacity = self.line_capacity.capacity();
        EventWriter {
            destination,
            sender: &self.sender,
            dropped_lines: &self.dropped_lines,
            overflow_policy: self.overflow_policy,
            max_line_bytes: self.max_line_bytes,
            arena: &self.arena,
            line_capacity: &self.line_capacity,
            metrics: &self.metrics,
            line: Some(SegmentedLine::new(&self.arena, initial_capacity)),
            truncated: false,
        }
    }
}

pub struct LogsGuard {
    sender: SyncSender<Command>,
    dropped_lines: Arc<AtomicU64>,
    last_error: Arc<Mutex<Option<String>>>,
    worker: Option<JoinHandle<io::Result<()>>>,
}

impl std::fmt::Debug for LogsGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LogsGuard")
            .field("dropped_lines", &self.dropped_lines())
            .field("last_error", &self.last_error())
            .finish_non_exhaustive()
    }
}

impl LogsGuard {
    pub fn dropped_lines(&self) -> u64 {
        self.dropped_lines.load(Ordering::Relaxed)
    }

    pub fn last_error(&self) -> Option<String> {
        self.last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn flush(&self) -> io::Result<()> {
        let (sender, receiver) = mpsc::sync_channel(0);
        self.sender
            .send(Command::Flush(sender))
            .map_err(|_| self.worker_unavailable())?;
        receiver
            .recv()
            .map_err(|_| self.worker_unavailable())?
            .map_err(io::Error::other)
    }

    fn worker_unavailable(&self) -> io::Error {
        io::Error::other(
            self.last_error()
                .unwrap_or_else(|| "Breeze log worker is unavailable".to_string()),
        )
    }
}

impl Drop for LogsGuard {
    fn drop(&mut self) {
        if self.worker.is_none() {
            return;
        }
        let (sender, receiver) = mpsc::sync_channel(0);
        if self.sender.send(Command::Shutdown(sender)).is_ok() {
            let _ = receiver.recv();
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

pub(crate) fn start(config: &LogsConfig) -> io::Result<(LogMakeWriter, LogsGuard)> {
    std::fs::create_dir_all(&config.directory)?;
    let files = LogFiles::open(&config.directory)?;
    let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
    let dropped_lines = Arc::new(AtomicU64::new(0));
    let last_error = Arc::new(Mutex::new(None));
    let arena = EphemeralBytesArena::new(config.arena_chunk_bytes);
    let line_capacity = Arc::new(LineCapacityHint::new(config.max_line_bytes));
    let worker_error = Arc::clone(&last_error);
    let flush_policy = config.flush_policy;
    let batch_limit = config.queue_capacity;
    let worker = thread::Builder::new()
        .name("breeze-logs".to_string())
        .spawn(move || run_worker(receiver, files, flush_policy, batch_limit, worker_error))?;
    let make_writer = LogMakeWriter {
        sender: sender.clone(),
        dropped_lines: Arc::clone(&dropped_lines),
        overflow_policy: config.overflow_policy,
        max_line_bytes: config.max_line_bytes,
        arena,
        line_capacity,
        metrics: LogMetrics::new(),
    };
    let guard = LogsGuard {
        sender,
        dropped_lines,
        last_error,
        worker: Some(worker),
    };
    Ok((make_writer, guard))
}

struct SegmentedLine {
    primary: EphemeralBytesMut,
    overflow: Vec<EphemeralBytesMut>,
    len: usize,
    ever_overflowed: bool,
}

impl SegmentedLine {
    fn new(arena: &EphemeralBytesArena, initial_capacity: usize) -> Self {
        Self {
            primary: arena.alloc(initial_capacity),
            overflow: Vec::new(),
            len: 0,
            ever_overflowed: false,
        }
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn overflowed(&self) -> bool {
        self.ever_overflowed
    }

    fn append(&mut self, arena: &EphemeralBytesArena, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if self.current().remaining() == 0 {
                let next_capacity = self
                    .current()
                    .capacity()
                    .saturating_mul(2)
                    .min(MAX_INITIAL_LINE_BYTES);
                self.overflow.push(arena.alloc(next_capacity));
                self.ever_overflowed = true;
            }
            let accepted = bytes.len().min(self.current().remaining());
            self.current_mut().extend_from_slice(&bytes[..accepted]);
            self.len += accepted;
            bytes = &bytes[accepted..];
        }
    }

    fn truncate(&mut self, len: usize) {
        let len = len.min(self.len);
        let mut removed = self.len - len;
        while removed > 0 {
            if let Some(last) = self.overflow.last_mut() {
                if last.len() <= removed {
                    removed -= last.len();
                    self.overflow.pop();
                } else {
                    last.truncate(last.len() - removed);
                    removed = 0;
                }
            } else {
                self.primary.truncate(self.primary.len() - removed);
                removed = 0;
            }
        }
        self.len = len;
    }

    #[inline]
    fn ends_with_newline(&self) -> bool {
        self.current().as_slice().last() == Some(&b'\n')
    }

    #[inline]
    fn current(&self) -> &EphemeralBytesMut {
        self.overflow.last().unwrap_or(&self.primary)
    }

    #[inline]
    fn current_mut(&mut self) -> &mut EphemeralBytesMut {
        self.overflow.last_mut().unwrap_or(&mut self.primary)
    }

    fn segments(&self) -> impl Iterator<Item = &[u8]> {
        std::iter::once(self.primary.as_slice())
            .chain(self.overflow.iter().map(EphemeralBytesMut::as_slice))
            .filter(|bytes| !bytes.is_empty())
    }
}

pub(crate) struct EventWriter<'arena> {
    destination: Destination,
    sender: &'arena SyncSender<Command>,
    dropped_lines: &'arena AtomicU64,
    overflow_policy: OverflowPolicy,
    max_line_bytes: usize,
    arena: &'arena EphemeralBytesArena,
    line_capacity: &'arena LineCapacityHint,
    metrics: &'arena LogMetrics,
    line: Option<SegmentedLine>,
    truncated: bool,
}

impl io::Write for EventWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let available = self.max_line_bytes.saturating_sub(self.line().len());
        let accepted = available.min(bytes.len());
        let arena = self.arena;
        self.line_mut().append(arena, &bytes[..accepted]);
        self.truncated |= accepted < bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for EventWriter<'_> {
    fn drop(&mut self) {
        if self.line().is_empty() {
            return;
        }
        self.finish_line();
        let line = self.line.take().expect("event line is present");
        self.line_capacity.observe(line.len(), line.overflowed());
        let command = Command::Line(QueuedLine {
            destination: self.destination,
            bytes: line,
        });
        let failure = match self.overflow_policy {
            OverflowPolicy::DropNewest => match self.sender.try_send(command) {
                Ok(()) => None,
                Err(TrySendError::Full(_)) => Some(EnqueueFailure::Full),
                Err(TrySendError::Disconnected(_)) => Some(EnqueueFailure::Disconnected),
            },
            OverflowPolicy::Block => self
                .sender
                .send(command)
                .err()
                .map(|_| EnqueueFailure::Disconnected),
        };
        if let Some(failure) = failure {
            self.dropped_lines.fetch_add(1, Ordering::Relaxed);
            if matches!(failure, EnqueueFailure::Full) {
                self.metrics.record_queue_drop();
            }
        }
    }
}

impl EventWriter<'_> {
    #[inline]
    fn line(&self) -> &SegmentedLine {
        self.line.as_ref().expect("event line is present")
    }

    #[inline]
    fn line_mut(&mut self) -> &mut SegmentedLine {
        self.line.as_mut().expect("event line is present")
    }

    fn finish_line(&mut self) {
        if self.truncated {
            let retained = self.max_line_bytes.saturating_sub(TRUNCATED_SUFFIX.len());
            self.line_mut().truncate(retained);
            let arena = self.arena;
            self.line_mut().append(arena, TRUNCATED_SUFFIX);
        } else if !self.line().ends_with_newline() {
            if self.line().len() < self.max_line_bytes {
                let arena = self.arena;
                self.line_mut().append(arena, b"\n");
            } else {
                let retained = self.max_line_bytes.saturating_sub(TRUNCATED_SUFFIX.len());
                self.line_mut().truncate(retained);
                let arena = self.arena;
                self.line_mut().append(arena, TRUNCATED_SUFFIX);
            }
        }
    }
}

struct LogFiles {
    info: File,
    warn: File,
    error: File,
}

impl LogFiles {
    fn open(directory: &Path) -> io::Result<Self> {
        Ok(Self {
            info: open_file(directory.join("info.log"))?,
            warn: open_file(directory.join("warn.log"))?,
            error: open_file(directory.join("error.log"))?,
        })
    }

    fn write_batch(&mut self, lines: &[QueuedLine]) -> io::Result<()> {
        write_destination(&mut self.info, Destination::Info, lines)?;
        write_destination(&mut self.warn, Destination::Warn, lines)?;
        write_destination(&mut self.error, Destination::Error, lines)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.info.flush()?;
        self.warn.flush()?;
        self.error.flush()
    }
}

fn open_file(path: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn write_destination(
    file: &mut File,
    destination: Destination,
    lines: &[QueuedLine],
) -> io::Result<()> {
    let mut segments = lines
        .iter()
        .filter(move |line| line.destination == destination)
        .flat_map(|line| line.bytes.segments());

    loop {
        let mut slices: [IoSlice<'_>; MAX_VECTORED_SLICES] = array::from_fn(|_| IoSlice::new(&[]));
        let mut count = 0;
        for slice in &mut slices {
            let Some(bytes) = segments.next() else {
                break;
            };
            *slice = IoSlice::new(bytes);
            count += 1;
        }
        if count == 0 {
            return Ok(());
        }
        write_all_vectored(file, &mut slices[..count])?;
    }
}

fn write_all_vectored(file: &mut File, mut slices: &mut [IoSlice<'_>]) -> io::Result<()> {
    while !slices.is_empty() {
        match file.write_vectored(slices) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::WriteZero)),
            Ok(written) => IoSlice::advance_slices(&mut slices, written),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn run_worker(
    receiver: Receiver<Command>,
    mut files: LogFiles,
    flush_policy: FlushPolicy,
    batch_limit: usize,
    last_error: Arc<Mutex<Option<String>>>,
) -> io::Result<()> {
    let result = worker_loop(&receiver, &mut files, flush_policy, batch_limit);
    if let Err(error) = &result {
        *last_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error.to_string());
    }
    result
}

fn worker_loop(
    receiver: &Receiver<Command>,
    files: &mut LogFiles,
    flush_policy: FlushPolicy,
    batch_limit: usize,
) -> io::Result<()> {
    let interval = flush_policy.interval();
    let mut next_flush = interval.map(|duration| Instant::now() + duration);
    let mut pending = None;
    let mut batch = Vec::with_capacity(batch_limit);

    loop {
        let received = match pending.take() {
            Some(command) => ReceiveResult::Command(command),
            None => receive_next(receiver, next_flush),
        };

        match received {
            ReceiveResult::Command(Command::Line(line)) => {
                let mut disconnected = false;
                batch.push(line);
                while batch.len() < batch_limit {
                    match receiver.try_recv() {
                        Ok(Command::Line(line)) => batch.push(line),
                        Ok(command) => {
                            pending = Some(command);
                            break;
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }

                let contains_error = batch
                    .iter()
                    .any(|line| line.destination == Destination::Error);
                files.write_batch(&batch)?;
                batch.clear();

                if flush_policy.flush_after_every_line()
                    || (contains_error && flush_policy.flush_after_error())
                    || next_flush.is_some_and(|deadline| Instant::now() >= deadline)
                {
                    files.flush()?;
                    next_flush = interval.map(|duration| Instant::now() + duration);
                }
                if disconnected {
                    files.flush()?;
                    return Ok(());
                }
            }
            ReceiveResult::Command(Command::Flush(reply)) => {
                let result = files.flush().map_err(|error| error.to_string());
                let failed = result.as_ref().err().cloned();
                let _ = reply.send(result);
                if let Some(error) = failed {
                    return Err(io::Error::other(error));
                }
                next_flush = interval.map(|duration| Instant::now() + duration);
            }
            ReceiveResult::Command(Command::Shutdown(reply)) => {
                let result = files.flush().map_err(|error| error.to_string());
                let failed = result.as_ref().err().cloned();
                let _ = reply.send(result);
                return failed.map_or(Ok(()), |error| Err(io::Error::other(error)));
            }
            ReceiveResult::FlushDeadline => {
                files.flush()?;
                next_flush = interval.map(|duration| Instant::now() + duration);
            }
            ReceiveResult::Disconnected => {
                files.flush()?;
                return Ok(());
            }
        }
    }
}

enum ReceiveResult {
    Command(Command),
    FlushDeadline,
    Disconnected,
}

fn receive_next(receiver: &Receiver<Command>, next_flush: Option<Instant>) -> ReceiveResult {
    match next_flush {
        Some(deadline) => {
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(command) => ReceiveResult::Command(command),
                Err(RecvTimeoutError::Timeout) => ReceiveResult::FlushDeadline,
                Err(RecvTimeoutError::Disconnected) => ReceiveResult::Disconnected,
            }
        }
        None => match receiver.recv() {
            Ok(command) => ReceiveResult::Command(command),
            Err(_) => ReceiveResult::Disconnected,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_buffer_grows_in_segments_without_moving_existing_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let config = LogsConfig::default()
            .with_directory(directory.path())
            .with_arena_chunk_bytes(8 * 1024)
            .with_max_line_bytes(8 * 1024)
            .with_flush_policy(FlushPolicy::EveryLine);
        let (factory, guard) = start(&config).unwrap();
        let mut writer = factory.writer(Destination::Info);

        assert_eq!(writer.line().primary.capacity(), MIN_INITIAL_LINE_BYTES);
        let primary_pointer = writer.line().primary.as_slice().as_ptr();
        writer.write_all(&[b'x'; 4 * 1024]).unwrap();
        let line = writer.line();
        assert_eq!(line.primary.as_slice().as_ptr(), primary_pointer);
        assert_eq!(line.primary.capacity(), 512);
        assert_eq!(line.overflow[0].capacity(), 1024);
        assert_eq!(line.overflow[1].capacity(), 2048);
        assert_eq!(line.overflow[2].capacity(), 2048);
        assert!(line.segments().all(|segment| !segment.is_empty()));
        drop(writer);
        guard.flush().unwrap();

        let bytes = std::fs::read(directory.path().join("info.log")).unwrap();
        assert_eq!(bytes.len(), 4 * 1024 + 1);
    }

    #[test]
    fn initial_capacity_adapts_between_512_bytes_and_2_kib() {
        let hint = LineCapacityHint::new(64 * 1024);
        assert_eq!(hint.capacity(), 512);

        hint.observe(513, true);
        assert_eq!(hint.capacity(), 1024);
        hint.observe(1025, true);
        assert_eq!(hint.capacity(), 2048);
        hint.observe(4096, true);
        assert_eq!(hint.capacity(), 2048);

        for _ in 0..LOW_USAGE_SAMPLE_COUNT {
            hint.observe(128, false);
        }
        assert_eq!(hint.capacity(), 1024);
        for _ in 0..LOW_USAGE_SAMPLE_COUNT {
            hint.observe(128, false);
        }
        assert_eq!(hint.capacity(), 512);
    }

    #[test]
    fn drop_newest_counts_a_line_when_the_queue_is_full() {
        let (sender, _receiver) = mpsc::sync_channel(1);
        let dropped_lines = Arc::new(AtomicU64::new(0));
        let factory = LogMakeWriter {
            sender,
            dropped_lines: Arc::clone(&dropped_lines),
            overflow_policy: OverflowPolicy::DropNewest,
            max_line_bytes: 4 * 1024,
            arena: EphemeralBytesArena::new(8 * 1024),
            line_capacity: Arc::new(LineCapacityHint::new(4 * 1024)),
            metrics: LogMetrics::new(),
        };
        #[cfg(feature = "metrics")]
        let metric_before = factory.metrics.queue_dropped.snapshot().total;

        let mut first = factory.writer(Destination::Info);
        first.write_all(b"first").unwrap();
        drop(first);
        let mut dropped = factory.writer(Destination::Info);
        dropped.write_all(b"dropped").unwrap();
        drop(dropped);

        assert_eq!(dropped_lines.load(Ordering::Relaxed), 1);
        #[cfg(feature = "metrics")]
        assert_eq!(
            factory.metrics.queue_dropped.snapshot().total,
            metric_before + 1
        );
    }

    #[test]
    fn long_lines_are_bounded_and_marked_across_segments() {
        let directory = tempfile::tempdir().unwrap();
        let config = LogsConfig::default()
            .with_directory(directory.path())
            .with_max_line_bytes(4 * 1024)
            .with_flush_policy(FlushPolicy::EveryLine);
        let (factory, guard) = start(&config).unwrap();
        let mut writer = factory.writer(Destination::Info);

        writer.write_all(&[b'x'; 8 * 1024]).unwrap();
        drop(writer);
        guard.flush().unwrap();

        let bytes = std::fs::read(directory.path().join("info.log")).unwrap();
        assert_eq!(bytes.len(), 4 * 1024);
        assert!(bytes.ends_with(TRUNCATED_SUFFIX));
    }

    #[test]
    fn file_batches_cross_the_vectored_slice_limit_without_reordering() {
        let directory = tempfile::tempdir().unwrap();
        let arena = EphemeralBytesArena::new(8 * 1024);
        let mut lines = Vec::new();
        for index in 0..(MAX_VECTORED_SLICES + 17) {
            let mut bytes = SegmentedLine::new(&arena, 8);
            bytes.append(&arena, format!("{index:04}\n").as_bytes());
            lines.push(QueuedLine {
                destination: Destination::Info,
                bytes,
            });
        }
        let mut warning = SegmentedLine::new(&arena, 8);
        warning.append(&arena, b"warning\n");
        lines.push(QueuedLine {
            destination: Destination::Warn,
            bytes: warning,
        });

        let mut files = LogFiles::open(directory.path()).unwrap();
        files.write_batch(&lines).unwrap();
        files.flush().unwrap();

        let info = std::fs::read_to_string(directory.path().join("info.log")).unwrap();
        let expected = (0..(MAX_VECTORED_SLICES + 17))
            .map(|index| format!("{index:04}\n"))
            .collect::<String>();
        assert_eq!(info, expected);
        assert_eq!(
            std::fs::read_to_string(directory.path().join("warn.log")).unwrap(),
            "warning\n"
        );
        assert_eq!(
            std::fs::read_to_string(directory.path().join("error.log")).unwrap(),
            ""
        );
    }
}
