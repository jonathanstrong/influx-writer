#![allow(unused)]

extern crate logging;

use std::time::{Instant, Duration};
use std::thread;

use logging::hist::{Entry, HistLog, nanos};

const N_SECS: u64 = 30;

//const N_THREADS: usize = 48;

macro_rules! n_threads {
    ($n:expr) => {
        const N_THREADS: usize = $n;
        const NAME: &'static str = concat!("sleeptest_", stringify!($n));
    }
}

n_threads!(96);

fn main() {
    ::std::process::exit(test_n());
}

fn test_n() -> i32 {
    let start = Instant::now();
    let hist = HistLog::new(NAME, "master", Duration::from_millis(100));
    for _ in 0..N_THREADS {
        let mut a = hist.clone_with_tag("sleep_1ns");
        thread::spawn(move || {

            let mut prev = Instant::now();
            let mut loop_time = Instant::now();

            loop {
                prev = loop_time;
                loop_time = Instant::now();
                a.record(nanos(loop_time - prev));
                a.check_send(loop_time);


                if loop_time - start > Duration::from_secs(N_SECS) { break }

                thread::sleep(Duration::new(0, 1));
                //thread::yield_now();
            }
        });
    }

    thread::sleep(Duration::from_secs(N_SECS));

    0
}

fn test_3() -> i32 {
    let start = Instant::now();
    let hist = HistLog::new("sleeptest", "master", Duration::from_millis(100));
    let mut a = hist.clone_with_tag("yield");
    let mut b = hist.clone_with_tag("sleep_0");
    let mut c = hist.clone_with_tag("sleep_100ns");

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
            //if i % 100 == 0 { thread::sleep(Duration::new(0, 0)); }
            //thread::sleep(Duration::new(0, 0));
            thread::yield_now();
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
            //if i % 100 == 0 { thread::sleep(Duration::new(0, 0)); }
            thread::sleep(Duration::new(0, 0));
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
        //if i % 100 == 0 { thread::sleep(Duration::new(0, 0)); }
        thread::sleep(Duration::new(0, 100));
    }

    0
}


