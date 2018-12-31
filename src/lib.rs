//! Tools to record and display what's happening in your program
//!

#![feature(test)]

#[macro_use]
extern crate slog;
#[allow(unused_imports)]
#[macro_use]
extern crate money;
#[cfg(test)]
extern crate test;
#[cfg(feature = "zmq")]
extern crate zmq;
#[cfg(feature = "latency")]
extern crate pubsub as pub_sub;

use std::time::Duration;
use chrono::{DateTime, Utc, TimeZone as ChronoTZ};
#[allow(unused_imports)]
use sloggers::Build;
#[allow(unused_imports)]
pub use sloggers::types::Severity;
use sloggers::types::TimeZone;
#[allow(unused_imports)]
use sloggers::file::FileLoggerBuilder;
use slog::Drain;
use uuid::Uuid;

pub mod influx;
#[cfg(feature = "warnings")]
pub mod warnings;
#[cfg(feature = "latency")]
pub mod latency;
pub mod hist;

#[cfg(feature = "trace")]
pub const LOG_LEVEL: Severity = Severity::Trace;
#[cfg(all(feature = "debug", not(feature = "trace")))]
pub const LOG_LEVEL: Severity = Severity::Debug;
#[cfg(not(any(feature = "debug", feature = "trace")))]
pub const LOG_LEVEL: Severity = Severity::Info;

#[cfg(not(feature = "trace"))]
const CHANNEL_SIZE: usize = 32_768;
#[cfg(feature = "trace")]
const CHANNEL_SIZE: usize = 2_097_152;

/// converts a chrono::DateTime to an integer timestamp (ns)
///
#[inline]
pub fn nanos(t: DateTime<Utc>) -> u64 {
    (t.timestamp() as u64) * 1_000_000_000_u64 + (t.timestamp_subsec_nanos() as u64)
}

#[inline]
pub fn secs(d: Duration) -> f64 {
    d.as_secs() as f64 + d.subsec_nanos() as f64 / 1_000_000_000_f64
}

#[inline]
pub fn inanos(t: DateTime<Utc>) -> i64 {
    t.timestamp() * 1_000_000_000i64 + t.timestamp_subsec_nanos() as i64
}

//#[cfg(not(any(test, feature = "test")))]
pub fn file_logger<P: AsRef<std::path::Path>>(path: P, level: Severity) -> slog::Logger {
    let mut builder = FileLoggerBuilder::new(path);
    builder.level(level)
        .timezone(TimeZone::Utc)
        .channel_size(CHANNEL_SIZE)
        .rotate_size(1024 * 1024 * 1024)
        .rotate_keep(1000)
        .rotate_compress(true)
        .source_location(sloggers::types::SourceLocation::ModuleAndLine);
    builder.build().unwrap() // the sloggers impl can't actually fail (v0.3)
}

pub fn truncating_file_logger(path: &str, level: Severity) -> slog::Logger {
    let mut builder = FileLoggerBuilder::new(path);
    builder.level(level);
    builder.timezone(TimeZone::Utc);
    builder.truncate();
    builder.channel_size(CHANNEL_SIZE);
    builder.build().unwrap()
}

#[deprecated(since="0.4.0", note="Turns out the file logger in sloggers uses async, \
                                  making the async here duplicative")]
pub fn async_file_logger(path: &str, level: Severity) -> slog::Logger {
    let drain = file_logger(path, level);
    let async_drain =
        slog_async::Async::new(drain)
            .chan_size(100_000)
            .build();
    slog::Logger::root(async_drain.fuse(), o!())
}

//#[deprecated(since="0.4.3", note="Use `nanos(DateTime<Utc>) -> u64` instead")]
pub fn dt_nanos(t: DateTime<Utc>) -> i64 {
    (t.timestamp() as i64) * 1_000_000_000_i64 + (t.timestamp_subsec_nanos() as i64)
}

#[inline]
pub fn dur_nanos(d: ::std::time::Duration) -> i64 {
    (d.as_secs() * 1_000_000_000_u64 + (d.subsec_nanos() as u64)) as i64
}

#[inline]
pub fn nanos_utc(t: i64) -> DateTime<Utc> {
    Utc.timestamp(t / 1_000_000_000, (t % 1_000_000_000) as u32)
}

pub fn short_uuid(id: &Uuid) -> String {
    if cfg!(feature = "disable-short-uuid") {
        id.to_string()
    } else {
        format!("{}", &id.to_string()[..8])
    }
}

#[allow(unused)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_nanos_round_trip() {
        let utc = Utc::now();
        let ns = inanos(utc);
        let rt = nanos_utc(ns);
        assert_eq!(utc, rt);
        let utc = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
        let ns = inanos(utc);
        let rt = nanos_utc(ns);
        assert_eq!(utc, rt);
    }
}
