#[macro_use]
extern crate slog;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::*;
use std::thread;
use slog::Drain;
use pretty_toa::ThousandsSep;
use chrono::prelude::*;
use influx_writer::{InfluxWriter, measure};

const DELAY: Duration = Duration::from_millis(1);
const N_PER: usize = 567;

fn main() {
    let start = Instant::now();
    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::SIGINT, Arc::clone(&term)).unwrap();
    signal_hook::flag::register(signal_hook::SIGTERM, Arc::clone(&term)).unwrap();
    signal_hook::flag::register(signal_hook::SIGQUIT, Arc::clone(&term)).unwrap();

    let decorator = slog_term::TermDecorator::new().stdout().force_color().build();
    let drain = slog_term::FullFormat::new(decorator).use_utc_timestamp().build().fuse();
    let drain = slog_async::Async::new(drain).chan_size(1024 * 64).thread_name("recv".into()).build().fuse();
    let root = slog::Logger::root(drain, o!("version" => "0.1"));

    let logger = root.new(o!("thread" => "main"));

    let influx = InfluxWriter::with_logger_and_opt_creds("localhost", "test", None, &root);

    let mut n = 0;

    loop {
        if term.load(Ordering::Relaxed) {
            info!(logger, "exiting...");
            break
        }

        let mut now = Utc::now().timestamp_nanos();
        for _ in 0..N_PER {
            measure!(influx, example, i(n, 1), tm(now));
            now += 1;
            n += 1;
        }

        thread::sleep(DELAY);
    }
    drop(influx);

    let took = Instant::now() - start;

    info!(logger, "wrote {} measurements in {:?}", n.thousands_sep(), took);

}
