//! Tools to record and display what's happening in your program
//!

#![feature(test)]

#[macro_use] extern crate slog;

#[allow(unused_imports)]
#[macro_use] extern crate money;

extern crate test;
extern crate zmq;
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

extern crate windows;
extern crate pubsub as pub_sub;

use chrono::{DateTime, Utc};
#[allow(unused_imports)]
use sloggers::Build;
#[allow(unused_imports)]
pub use sloggers::types::Severity;
use sloggers::types::TimeZone;
#[allow(unused_imports)]
use sloggers::file::FileLoggerBuilder;
use slog::Drain;

pub mod influx;
pub mod warnings;
pub mod latency;
pub mod hist;

#[cfg(feature = "trace")]
pub const LOG_LEVEL                 : Severity          = Severity::Trace;
#[cfg(all(feature = "debug", not(feature = "trace")))]
pub const LOG_LEVEL                 : Severity          = Severity::Debug;
#[cfg(not(any(feature = "debug", feature = "trace")))]
pub const LOG_LEVEL                 : Severity          = Severity::Info;

/// converts a chrono::DateTime to an integer timestamp (ns)
///
pub fn nanos(t: DateTime<Utc>) -> u64 {
    (t.timestamp() as u64) * 1_000_000_000_u64 + (t.timestamp_subsec_nanos() as u64)
}

//#[cfg(not(any(test, feature = "test")))]
pub fn file_logger(path: &str, level: Severity) -> slog::Logger {
    let mut builder = FileLoggerBuilder::new(path);
    builder.level(level);
    builder.timezone(TimeZone::Utc);
    builder.build().unwrap()
}

// #[cfg(any(test, feature = "test"))]
// pub fn file_logger(_: &str, _: Severity) -> slog::Logger {
//     use slog::*;
//     Logger::root(Discard, o!())
// }

pub fn async_file_logger(path: &str, level: Severity) -> slog::Logger {
    let drain = file_logger(path, level);
    let async_drain =
        slog_async::Async::new(drain)
            .chan_size(100_000)
            .build();
    slog::Logger::root(async_drain.fuse(), o!())
}

pub fn dt_nanos(t: DateTime<Utc>) -> i64 {
    (t.timestamp() as i64) * 1_000_000_000_i64 + (t.timestamp_subsec_nanos() as i64)
}

pub fn dur_nanos(d: ::std::time::Duration) -> i64 {
    (d.as_secs() * 1_000_000_000_u64 + (d.subsec_nanos() as u64)) as i64
}


