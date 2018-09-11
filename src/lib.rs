//! Tools to record and display what's happening in your program
//!

#![feature(test)]

#[macro_use] extern crate slog;

#[allow(unused_imports)]
#[macro_use] extern crate money;

extern crate test;
extern crate influent;
extern crate chrono;
extern crate hyper;
extern crate termion;
extern crate sloggers;
extern crate slog_term;
extern crate slog_async;
extern crate fnv;
extern crate ordermap;
extern crate decimal;
extern crate uuid;
extern crate hdrhistogram;
extern crate smallvec;
extern crate num;
extern crate dirs;
#[cfg(feature = "zmq")]
extern crate zmq;

extern crate pubsub as pub_sub;

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
pub fn inanos(t: DateTime<Utc>) -> i64 {
    t.timestamp() * 1_000_000_000i64 + t.timestamp_subsec_nanos() as i64
}

//#[cfg(not(any(test, feature = "test")))]
pub fn file_logger(path: &str, level: Severity) -> slog::Logger {
    let mut builder = FileLoggerBuilder::new(path);
    builder.level(level);
    builder.timezone(TimeZone::Utc);
    builder.channel_size(CHANNEL_SIZE);
    builder.build().unwrap()
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
