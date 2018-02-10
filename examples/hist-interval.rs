
extern crate logging;

use std::time::{Instant, Duration};
use std::thread;

use logging::hist::{Entry, HistLog, nanos};

const N_SECS: u64 = 300;

fn main() {
    let start = Instant::now();
    let mut a = HistLog::new("test", "a", Duration::from_millis(100));
    let mut b = a.clone_with_tag("b");
    let mut c = b.clone_with_tag("c");

    thread::spawn(move || {

        let mut prev = Instant::now();
        let mut loop_time = Instant::now();
        let mut i = 0;

        loop {
            prev = loop_time;
            loop_time = Instant::now();
            a.record(nanos(loop_time - prev));
            a.check_send(loop_time);


            if loop_time - start > Duration::from_secs(N_SECS) { break }

            i += 1;
            if i % 100 == 0 { thread::sleep(Duration::new(0, 0)); }
        }
    });

    thread::spawn(move || {
        let mut prev = Instant::now();
        let mut loop_time = Instant::now();
        let mut i = 0;

        loop {
            prev = loop_time;
            loop_time = Instant::now();
            b.record(nanos(loop_time - prev));
            b.check_send(loop_time);

            if loop_time - start > Duration::from_secs(N_SECS) { break }

            i += 1;
            //if i % 1_000 == 0 { thread::sleep(Duration::new(0, 0)); }
            if i % 100 == 0 { thread::sleep(Duration::new(0, 0)); }
        }
    });

    let mut prev = Instant::now();
    let mut loop_time = Instant::now();
    let mut i = 0;

    loop {
        prev = loop_time;
        loop_time = Instant::now();
        c.record(nanos(loop_time - prev));
        c.check_send(loop_time);

        if loop_time - start > Duration::from_secs(N_SECS) { break }

        i += 1;
        //if i % 100_000 == 0 { thread::sleep(Duration::from_millis(10)); }
        if i % 100 == 0 { thread::sleep(Duration::new(0, 0)); }
    }
}

