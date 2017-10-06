use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::{self, Sender, Receiver, channel};
use std::collections::VecDeque;
use std::fmt::{self, Display, Write};
use std::time::{Instant, Duration};

use chrono::{self, DateTime, Utc, TimeZone};
use pub_sub::PubSub;
use zmq;
use influent::measurement::{Measurement, Value};
use sloggers::types::Severity;
use shuteye;
//use chashmap::CHashMap;

use windows::{DurationWindow, Incremental, Window};
use money::{Ticker, Side, ByExchange, Exchange};

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
    let mut f = String::new();
    match ns {
        t if t <= MICROSECOND => {
            write!(f, "{}ns", t);
        }

        t if t > MICROSECOND && t < MILLISECOND => {
            write!(f, "{}u", t / MICROSECOND);  
        }
        t if t > MILLISECOND && t < SECOND => {
            write!(f, "{}ms", t / MILLISECOND);  
        }

        t => {
            write!(f, "{}.{}sec", t / SECOND, t / MILLISECOND);
        }
    }
    f
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


pub fn tfmt_write(ns: Nanos, f: &mut fmt::Formatter) {
    match ns {
        t if t <= MICROSECOND => {
            write!(f, "{}ns", t);
        }

        t if t > MICROSECOND && t < MILLISECOND => {
            write!(f, "{}u", t / MICROSECOND);  
        }
        t if t > MILLISECOND && t < SECOND => {
            write!(f, "{}ms", t / MILLISECOND);  
        }

        t => {
            write!(f, "{}.{}sec", t / SECOND, t / MILLISECOND);
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

    //GdaxWebsocketNoLock(Duration),

    GdaxHttpPublic(Duration),

    GdaxHttpPrivate(Duration),

    PlnxHttpPublic(Duration),

    PlnxHttpPrivate(Duration),

    PlnxOrderBook(Duration),

    ExmoHttpPublic(Duration),

    KrknHttpPublic(Duration),

    KrknHttpPrivate(Duration),

    KrknTrade(Duration, &'static str, Option<Ticker>, Option<Side>),

    EventLoop(Duration),

    PlnxWs(Ticker),

    Terminate
}

// impl Message for ExperiencedLatency {
//     fn kill_switch() -> Self {
//         ExperiencedLatency::Terminate
//     }
// }

/// represents over what period of time
/// the latency measurements were taken
pub trait MeasurementWindow {
    fn duration(&self) -> Duration;
}

#[derive(Debug, Clone, Copy)]
pub struct WThirty;

impl Default for WThirty {
    fn default() -> Self { WThirty {} }
}

impl MeasurementWindow for WThirty {
    fn duration(&self) -> Duration { Duration::from_secs(30) }
}

#[derive(Debug, Clone, Copy)]
pub struct WTen;

impl Default for WTen {
    fn default() -> Self { WTen {} }
}

impl MeasurementWindow for WTen {
    fn duration(&self) -> Duration { Duration::from_secs(10) }
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

// impl Update {
//     pub fn new(window: Duration) -> Self {
//         Update {
//             window,
//             ws: OrderMap::new(),
//             http: 0,
//             trade: OrderMap::new(),
//         }
//     }
// }

// #[derive(Clone)]
// pub struct Updates {
//     pub gdax_5: Update,
//     pub gdax_30: Update,
//     pub last: ByExchange<DateTime<Utc>>
// }

#[derive(Debug, Clone)]
pub struct LatencyUpdate<W> 
    where W: MeasurementWindow
{
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

    //pub event_loop: Nanos,

    pub size: W,
}

impl<W> Default for LatencyUpdate<W> 
    where W: MeasurementWindow + Default
{
    fn default() -> Self {
        LatencyUpdate {
            gdax_ws: Nanos::default(),
            krkn_pub: Nanos::default(),
            krkn_priv: Nanos::default(), 
            plnx_pub: Nanos::default(),
            plnx_priv: Nanos::default(),
            plnx_order: Nanos::default(),
            krkn_trade_30_mean: Nanos::default(),
            krkn_trade_30_max: Nanos::default(),

            krkn_trade_300_mean: Nanos::default(),
            krkn_trade_300_max: Nanos::default(),

            plnx_ws_count: 0,

            plnx_last: Utc::now(),
            krkn_last: Utc::now(),

            size: W::default()
        }
    }
}

impl<W> Display for LatencyUpdate<W> 
    where W: MeasurementWindow
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, " gdax ws: ");
        tfmt_write(self.gdax_ws, f);
        write!(f, "\n krkn pub: ");
        tfmt_write(self.krkn_pub, f);
        write!(f, "\n krkn priv: ");
        tfmt_write(self.krkn_priv, f);

        write!(f, "\n krkn trade 30 mean: ");
        tfmt_write(self.krkn_trade_30_mean, f);

        write!(f, "\n krkn trade 30 max: ");
        tfmt_write(self.krkn_trade_30_max, f);

        write!(f, "\n krkn trade 300 mean: ");
        tfmt_write(self.krkn_trade_300_mean, f);

        write!(f, "\n krkn trade 300 max: ");
        tfmt_write(self.krkn_trade_300_max, f);

        write!(f, "\n plnx pub: ");
        tfmt_write(self.plnx_pub, f);
        write!(f, "\n plnx priv: ");
        tfmt_write(self.plnx_priv, f);
        write!(f, "\n plnx orderbook loop: ");
        tfmt_write(self.plnx_order, f);

        //write!(f, "\n gdax ws nolock: ");
        //tfmt_write(self.gdax_ws_nolock, f);
        //write!(f, "\n event loop: ");
        //tfmt(self.event_loop, f);
        write!(f,"")
    }
}

impl<W: MeasurementWindow> LatencyUpdate<W> {
    pub fn measurement_window(&self) -> Duration {
        self.size.duration()
    }

}

pub struct Manager {
    pub tx: Sender<Latency>,
    pub channel: PubSub<Update>,
        thread: Option<JoinHandle<()>>,
}

pub struct LatencyManager<W> 
    where W: MeasurementWindow + Clone + Send + Sync
{
    pub tx: Sender<ExperiencedLatency>,
    pub channel: PubSub<LatencyUpdate<W>>,
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
        let tx_copy = tx.clone();
        let channel = PubSub::new();
        let channel_copy = channel.clone();
        let logger = file_logger(log_path, Severity::Info);
        
        info!(logger, "initializing");
 
        let mut gdax_ws = DurationWindow::new(window);
        let mut gdax_trade = DurationWindow::new(window);

        let mut last = Last::default();

        info!(logger, "entering loop");
        let mut terminate = false;

        let thread = Some(thread::spawn(move || { 
            loop {

                let loop_time = Instant::now();

                rx.try_recv().map(|msg| {
                    debug!(logger, "rcvd {:?}", msg);

                    match msg {
                        Latency::Ws(exch, ticker, dur) => {
                            // shortcut
                            gdax_ws.update(loop_time, dur);
                            last.gdax = loop_time;
                        }

                        Latency::Trade(exch, ticker, dur) => {
                            //shorcut
                            gdax_trade.update(loop_time, dur);
                            last.gdax = loop_time;
                            let nanos = DurationWindow::nanos(dur);
                            measurements.send(
                                OwnedMeasurement::new("gdax_trade_api")
                                    .add_string_tag("ticker", ticker.to_string())
                                    .add_field("nanos", OwnedValue::Integer(nanos as i64))
                                    .set_timestamp(influx::now()));
                        }

                        Latency::Terminate => {
                            crit!(logger, "rcvd Terminate order");
                            terminate = true;
                        }

                        _ => {}
                    }
                });

                if loop_time - last.broadcast > Duration::from_millis(100) {
                    debug!(logger, "initalizing broadcast");

                    let update = Update {
                        gdax_ws: gdax_ws.refresh(&loop_time).mean_nanos(),
                        gdax_trade: gdax_trade.refresh(&loop_time).mean_nanos(),
                        gdax_last: dt_from_dur(loop_time - last.gdax)
                    };
                    channel.send(update);
                    last.broadcast = loop_time;
                    debug!(logger, "sent broadcast");
                } else {
                    #[cfg(feature = "no-thrash")]
                    shuteye::sleep(Duration::new(0, 1000));
                }

                if terminate { break }
            }
            crit!(logger, "goodbye");
        }));

        Manager {
            tx,
            channel: channel_copy,
            thread,
        }
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        self.tx.send(Latency::Terminate);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}



//impl<W: MeasurementWindow + Clone + Send + Sync> LatencyManager<W> {
impl LatencyManager<WTen> {
    pub fn new(w: WTen) -> Self {
        let (tx, rx) = channel();
        let tx_copy = tx.clone();
        let channel = PubSub::new();
        let channel_copy = channel.clone();
        let w = w.clone();

        let thread = Some(thread::spawn(move || { 
            let logger = file_logger("var/log/latency-manager.log", Severity::Info);
            info!(logger, "initializing zmq");

            let ctx = zmq::Context::new();
            let socket = influx::push(&ctx).unwrap();
            let mut buf = String::with_capacity(4096);
            info!(logger, "initializing DurationWindows");
            let mut gdax_ws = DurationWindow::new(w.duration());
            let mut gdax_priv = DurationWindow::new(w.duration());
            let mut krkn_pub = DurationWindow::new(w.duration());
            let mut krkn_priv = DurationWindow::new(w.duration());
            let mut plnx_pub = DurationWindow::new(w.duration());
            let mut plnx_priv = DurationWindow::new(w.duration());
            let mut plnx_order = DurationWindow::new(w.duration());
            let mut plnx_ws_count: Window<u32> = Window::new(w.duration());


            // yes I am intentionally breaking from the hard-typed duration
            // window ... that was a stupid idea
            //
            let mut krkn_trade_30 = DurationWindow::new(Duration::from_secs(30));
            let mut krkn_trade_300 = DurationWindow::new(Duration::from_secs(300));
            //let mut gdax_ws_nolock = DurationWindow::new(w.duration());
            //let mut event_loop = DurationWindow::new(w.duration());

            let mut last = Last::default();

            thread::sleep_ms(1);

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
                        //ExperiencedLatency::GdaxWebsocketNoLock(d) => gdax_ws_nolock.update(loop_time, d),
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

                        ExperiencedLatency::KrknTrade(d, cmd, ticker, side) => {
                            debug!(logger, "new KrknTrade";
                                   "cmd" => cmd);
                            last.krkn = loop_time;
                            let n = DurationWindow::nanos(d);
                            krkn_trade_30.update(loop_time, d);
                            krkn_trade_300.update(loop_time, d);
                            let ticker_s = ticker.map(|t| t.to_string()).unwrap_or("".into());
                            let side_s = side.map(|s| s.to_string()).unwrap_or("".into());
                            let mut m = Measurement::new("krkn_trade_api");
                            m.add_field("nanos", Value::Integer(n as i64));
                            m.add_tag("cmd", cmd);
                            if ticker.is_some() {
                                m.add_tag("ticker", &ticker_s);
                            }
                            if side.is_some() {
                                m.add_tag("side", &side_s);
                            }
                            m.set_timestamp(now());
                            influx::serialize(&m, &mut buf);
                            socket.send_str(&buf, 0);
                            buf.clear();
                        }
                        //ExperiencedLatency::EventLoop(d) => event_loop.update(Instant::now(), d),
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
                        //gdax_ws_nolock: gdax_ws_nolock.refresh(&loop_time).mean_nanos(),
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

                        //event_loop: event_loop.refresh(&now).mean_nanos(),
                        size: w.clone(),
                    };
                    channel.send(update);
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






