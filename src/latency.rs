use std::thread::{self, JoinHandle};
use std::sync::mpsc::{Sender, channel};
use std::fmt;
use std::time::{Instant, Duration};

use chrono::{self, DateTime, Utc};
use pub_sub::PubSub;
use sloggers::types::Severity;

use windows::{DurationWindow, Incremental, Window};
use money::{Ticker, Side, Exchange};

use super::file_logger;
use influx::{self, OwnedMeasurement, OwnedValue};

pub type Nanos = u64;

pub const SECOND: u64 = 1e9 as u64;
pub const MINUTE: u64 = SECOND * 60;
pub const HOUR: u64 = MINUTE * 60;
pub const MILLISECOND: u64 = SECOND / 1000;
pub const MICROSECOND: u64 = MILLISECOND / 1000;

pub fn nanos(d: Duration) -> Nanos {
    d.as_secs() * 1_000_000_000 + (d.subsec_nanos() as u64)
}

pub fn dt_nanos(t: DateTime<Utc>) -> i64 {
    (t.timestamp() as i64) * 1_000_000_000_i64 + (t.timestamp_subsec_nanos() as i64)
}

pub fn now() -> i64 { dt_nanos(Utc::now()) }

pub fn tfmt(ns: Nanos) -> String {
    match ns {
        t if t <= MICROSECOND => {
            format!("{}ns", t)
        }

        t if t > MICROSECOND && t < MILLISECOND => {
            format!("{}u", t / MICROSECOND)
        }
        t if t > MILLISECOND && t < SECOND => {
            format!("{}ms", t / MILLISECOND)
        }

        t => {
            format!("{}.{}sec", t / SECOND, t / MILLISECOND)
        }
    }
}

pub fn tfmt_dur(d: Duration) -> String {
    tfmt(nanos(d))
}

pub fn tfmt_dt(dt: DateTime<Utc>) -> String {
    Utc::now().signed_duration_since(dt)
        .to_std()
        .map(|dur| {
            tfmt_dur(dur)
        }).unwrap_or("?".into())
}


pub fn tfmt_write(ns: Nanos, f: &mut fmt::Formatter) -> fmt::Result {
    match ns {
        t if t <= MICROSECOND => {
            write!(f, "{}ns", t)
        }

        t if t > MICROSECOND && t < MILLISECOND => {
            write!(f, "{}u", t / MICROSECOND)
        }

        t if t > MILLISECOND && t < SECOND => {
            write!(f, "{}ms", t / MILLISECOND)
        }

        t => {
            write!(f, "{}.{}sec", t / SECOND, t / MILLISECOND)
        }
    }
}

#[derive(Debug)]
pub enum Latency {
    Ws(Exchange, Ticker, Duration),
    Http(Exchange, Duration),
    Trade(Exchange, Ticker, Duration),
    Terminate
}

#[derive(Debug)]
pub enum ExperiencedLatency {

    GdaxWebsocket(Duration),
    GdaxHttpPublic(Duration),
    GdaxHttpPrivate(Duration),
    PlnxHttpPublic(Duration),
    PlnxHttpPrivate(Duration),
    PlnxOrderBook(Duration),
    KrknHttpPublic(Duration),
    KrknHttpPrivate(Duration),
    KrknTrade(Duration, &'static str, Option<Ticker>, Option<Side>),
    PlnxWs(Ticker),

    Terminate
}

#[derive(Debug, Clone)]
pub struct Update {
    pub gdax_ws: Nanos,
    pub gdax_trade: Nanos,
    pub gdax_last: DateTime<Utc>
}

impl Default for Update {
    fn default() -> Self {
        Update {
            gdax_ws: 0,
            gdax_trade: 0,
            gdax_last: Utc::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyUpdate {
    pub gdax_ws: Nanos,
    pub krkn_pub: Nanos,
    pub krkn_priv: Nanos, 
    pub plnx_pub: Nanos,
    pub plnx_priv: Nanos,
    pub plnx_order: Nanos,
    pub krkn_trade_30_mean: Nanos,
    pub krkn_trade_30_max: Nanos,

    pub krkn_trade_300_mean: Nanos,
    pub krkn_trade_300_max: Nanos,

    pub plnx_last: DateTime<Utc>,
    pub krkn_last: DateTime<Utc>,

    pub plnx_ws_count: u64,
}

impl Default for LatencyUpdate {
    fn default() -> Self {
        LatencyUpdate {
            gdax_ws                 : 0,
            krkn_pub                : 0,
            krkn_priv               : 0, 
            plnx_pub                : 0,
            plnx_priv               : 0,
            plnx_order              : 0,
            krkn_trade_30_mean      : 0,
            krkn_trade_30_max       : 0,
            krkn_trade_300_mean     : 0,
            krkn_trade_300_max      : 0,
            plnx_ws_count           : 0,

            plnx_last               : Utc::now(),
            krkn_last               : Utc::now(),
        }
    }
}

pub struct Manager {
    pub tx: Sender<Latency>,
    pub channel: PubSub<Update>,
        thread: Option<JoinHandle<()>>,
}

pub struct LatencyManager {
    pub tx: Sender<ExperiencedLatency>,
    pub channel: PubSub<LatencyUpdate>,
        thread: Option<JoinHandle<()>>,
}

/// returns a DateTime equal to now - `dur`
///
pub fn dt_from_dur(dur: Duration) -> DateTime<Utc> {
    let old_dur = chrono::Duration::nanoseconds(nanos(dur) as i64);
    Utc::now() - old_dur
}

struct Last {
    broadcast: Instant,
    plnx: Instant,
    krkn: Instant,
    gdax: Instant,
}

impl Default for Last {
    fn default() -> Self {
        Last {
            broadcast: Instant::now(),
            plnx: Instant::now(),
            krkn: Instant::now(),
            gdax: Instant::now(),
        }
    }
}

impl Manager {
    pub fn new(window: Duration,
           log_path: &'static str, 
           measurements: Sender<OwnedMeasurement>) -> Self {

        let (tx, rx) = channel();
        let channel = PubSub::new();
        let channel_copy = channel.clone();
        let logger = file_logger(log_path, Severity::Info);
        
        info!(logger, "initializing");
 
        let mut gdax_ws = DurationWindow::new(window);
        let mut gdax_trade = DurationWindow::new(window);

        let mut last = Last::default();

        info!(logger, "entering loop");

        let thread = Some(thread::spawn(move || { 
            loop {

                let loop_time = Instant::now();

                if let Ok(msg) = rx.recv_timeout(Duration::from_millis(1)) {
                    debug!(logger, "rcvd {:?}", msg);

                    match msg {
                        Latency::Ws(_, _, dur) => {
                            gdax_ws.update(loop_time, dur);
                            last.gdax = loop_time;
                        }

                        Latency::Trade(_, ticker, dur) => {
                            gdax_trade.update(loop_time, dur);
                            last.gdax = loop_time;
                            let nanos = DurationWindow::nanos(dur);
                            measurements.send(
                                OwnedMeasurement::new("gdax_trade_api")
                                    .add_tag("ticker", ticker.as_str())
                                    .add_field("nanos", OwnedValue::Integer(nanos as i64))
                                    .set_timestamp(influx::now())).unwrap();
                        }

                        Latency::Terminate => break,

                        _ => {}
                    }
                }

                if loop_time - last.broadcast > Duration::from_millis(100) {
                    debug!(logger, "initalizing broadcast");

                    let update = Update {
                        gdax_ws: gdax_ws.refresh(&loop_time).mean_nanos(),
                        gdax_trade: gdax_trade.refresh(&loop_time).mean_nanos(),
                        gdax_last: dt_from_dur(loop_time - last.gdax)
                    };
                    channel.send(update).unwrap();
                    last.broadcast = loop_time;
                    debug!(logger, "sent broadcast");
                } 

            }
            debug!(logger, "latency manager terminating");
        }));

        Manager {
            tx,
            channel: channel_copy,
            thread,
        }
    }
}

impl Drop for LatencyManager {
    fn drop(&mut self) {
        for _ in 0..100 { self.tx.send(ExperiencedLatency::Terminate).unwrap(); }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        for _ in 0..100 { self.tx.send(Latency::Terminate).unwrap(); }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl LatencyManager {
    pub fn new(d: Duration) -> Self {
        let (tx, rx) = channel();
        let tx_copy = tx.clone();
        let channel = PubSub::new();
        let channel_copy = channel.clone();
        //let w = w.clone();

        let thread = Some(thread::spawn(move || { 
            let logger = file_logger("var/log/latency-manager.log", Severity::Info);
            info!(logger, "initializing zmq");

            info!(logger, "initializing DurationWindows");
            let mut gdax_ws = DurationWindow::new(d);
            let mut gdax_priv = DurationWindow::new(d);
            let mut krkn_pub = DurationWindow::new(d);
            let mut krkn_priv = DurationWindow::new(d);
            let mut plnx_pub = DurationWindow::new(d);
            let mut plnx_priv = DurationWindow::new(d);
            let mut plnx_order = DurationWindow::new(d);
            let mut plnx_ws_count: Window<u32> = Window::new(d);

            // yes I am intentionally breaking from the hard-typed duration
            // window ... that was a stupid idea
            //
            let mut krkn_trade_30 = DurationWindow::new(Duration::from_secs(30));
            let mut krkn_trade_300 = DurationWindow::new(Duration::from_secs(300));

            let mut last = Last::default();

            thread::sleep(Duration::from_millis(1));

            info!(logger, "entering loop");
            loop {
                let loop_time = Instant::now();

                if let Ok(msg) = rx.recv() {
                    debug!(logger, "new msg: {:?}", msg);

                    match msg {
                        ExperiencedLatency::Terminate => {
                            crit!(logger, "terminating");
                            break;
                        }

                        ExperiencedLatency::GdaxWebsocket(d) => gdax_ws.update(loop_time, d),

                        ExperiencedLatency::GdaxHttpPrivate(d) => gdax_priv.update(loop_time, d),

                        ExperiencedLatency::KrknHttpPublic(d) => {
                            last.krkn = loop_time;
                            krkn_pub.update(loop_time, d)
                        }

                        ExperiencedLatency::KrknHttpPrivate(d) => {
                            last.krkn = loop_time;
                            krkn_priv.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxHttpPublic(d) => {
                            last.plnx = loop_time;
                            plnx_pub.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxHttpPrivate(d) => {
                            last.plnx = loop_time;
                            plnx_priv.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxOrderBook(d) => {
                            last.plnx = loop_time;
                            plnx_order.update(loop_time, d)
                        }

                        ExperiencedLatency::PlnxWs(_) => {
                            last.plnx = loop_time;
                            plnx_ws_count.update(loop_time, 1_u32);
                        }

                        ExperiencedLatency::KrknTrade(d, cmd, _, _) => {
                            debug!(logger, "new KrknTrade";
                                   "cmd" => cmd);
                            last.krkn = loop_time;
                            krkn_trade_30.update(loop_time, d);
                            krkn_trade_300.update(loop_time, d);
                        }

                        other => {
                            warn!(logger, "unexpected msg: {:?}", other);
                        }
                    }
                }

                if loop_time - last.broadcast > Duration::from_millis(100) {
                    debug!(logger, "initalizing broadcast");
                    // note - because we mutated the Window instances
                    // above, we need a fresh Instant to avoid less than other
                    // panic
                    //
                    krkn_trade_30.refresh(&loop_time);
                    krkn_trade_300.refresh(&loop_time);
                    let update = LatencyUpdate {
                        gdax_ws: gdax_ws.refresh(&loop_time).mean_nanos(),
                        krkn_pub: krkn_pub.refresh(&loop_time).mean_nanos(),
                        krkn_priv: krkn_priv.refresh(&loop_time).mean_nanos(),
                        plnx_pub: plnx_pub.refresh(&loop_time).mean_nanos(),
                        plnx_priv: plnx_priv.refresh(&loop_time).mean_nanos(),
                        plnx_order: plnx_order.refresh(&loop_time).mean_nanos(),

                        krkn_trade_30_mean: krkn_trade_30.mean_nanos(),
                        krkn_trade_30_max: krkn_trade_30.max_nanos().unwrap_or(0),

                        krkn_trade_300_mean: krkn_trade_300.mean_nanos(),
                        krkn_trade_300_max: krkn_trade_300.max_nanos().unwrap_or(0),

                        plnx_last: dt_from_dur(loop_time - last.plnx),
                        krkn_last: dt_from_dur(loop_time - last.krkn),

                        plnx_ws_count: plnx_ws_count.refresh(&loop_time).count() as u64,

                    };
                    channel.send(update).unwrap();
                    last.broadcast = loop_time;
                    debug!(logger, "sent broadcast");
                }
            }
            crit!(logger, "goodbye");
        }));

        LatencyManager {
            tx: tx_copy,
            channel: channel_copy,
            thread
        }
    }
}
