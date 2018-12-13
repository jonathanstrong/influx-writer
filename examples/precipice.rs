#![allow(unused_imports)]

#[macro_use]
extern crate slog;
#[macro_use]
extern crate logging;

use std::io::{self, prelude::*};
use std::thread;
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::*;
use chrono::Utc;
use slog::Drain;
use pretty_toa::ThousandsSep;
use logging::influx::InfluxWriter;

const INTERVAL: Duration = Duration::from_micros(1); //from_millis(1);
const HB_EVERY: usize = 1_000_000;

fn main() {
    let to_file = logging::truncating_file_logger("var/log/precipice.log", sloggers::types::Severity::Debug);
    let decorator = slog_term::TermDecorator::new().stdout().force_color().build();
    let drain = slog_term::CompactFormat::new(decorator).use_utc_timestamp().build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(8192).thread_name("recv".into()).build().fuse();
    let drain = slog::Duplicate::new(drain, to_file).fuse();
    let root = slog::Logger::root(drain, o!());
    let logger = root.new(o!("thread" => "main"));
    info!(logger, "initializing...");
    let influx = InfluxWriter::with_logger("localhost", "precipice", 1024, root.new(o!("thread" => "InfluxWriter")));
    let stop = Arc::new(AtomicBool::new(false));
    let thread = {
        let stop = Arc::clone(&stop);
        let logger = root.new(o!("thread" => "blaster"));
        let influx = influx.clone();
        thread::spawn(move || {
            let mut i = 0;
            let mut sum = 0;
            while !stop.load(Ordering::Relaxed) {
                measure!(influx, xs, i(i), tm(logging::inanos(Utc::now())));
                sum += i;
                i += 1;
                if i % HB_EVERY == 0 {
                    info!(logger, "sent {} measurements", i.thousands_sep());
                }
                thread::sleep(INTERVAL);
            }
            info!(logger, "exiting"; "n_sent" => i, "sum" => sum);
        })
    };

    let mut keys = String::new();
    loop {
        if let Ok(_) = io::stdin().read_line(&mut keys) {
            break
        }
        thread::sleep(Duration::from_millis(1));
    }
    stop.store(true, Ordering::Relaxed);
    let _ = thread.join();
}


