use std::thread::{self, JoinHandle};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::mpsc::{self, Sender, Receiver, channel};
use std::collections::VecDeque;
use std::fmt::{self, Display, Error as FmtError, Formatter, Write};
use std::time::{Instant, Duration};

use chrono::{DateTime, Utc, TimeZone};
use pub_sub::PubSub;
use zmq;
use influent::measurement::{Measurement, Value};

use windows::{DurationWindow, Incremental};
use influx;



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


pub fn tfmt_write(ns: Nanos, f: &mut Formatter) {
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

    KrknTrade(Duration),

    EventLoop(Duration),

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

#[derive(Debug, Clone, Default)]
pub struct LatencyUpdate<W> 
    where W: MeasurementWindow
{
    pub gdax_ws: Nanos,
    //pub gdax_ws_nolock: Nanos,
    pub krkn_pub: Nanos,
    pub krkn_priv: Nanos, 
    pub plnx_pub: Nanos,
    pub plnx_priv: Nanos,
    pub plnx_order: Nanos,
    pub krkn_trade_30_mean: Nanos,
    pub krkn_trade_30_max: Nanos,

    pub krkn_trade_300_mean: Nanos,
    pub krkn_trade_300_max: Nanos,

    //pub event_loop: Nanos,

    pub size: W,
}

impl<W> Display for LatencyUpdate<W> 
    where W: MeasurementWindow
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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

pub struct LatencyManager<W> 
    where W: MeasurementWindow + Clone + Send + Sync
{
    pub tx: Sender<ExperiencedLatency>,
    pub channel: PubSub<LatencyUpdate<W>>,
        thread: Option<JoinHandle<()>>,
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
            let ctx = zmq::Context::new();
            let socket = influx::push(&ctx).unwrap();
            let mut buf = String::with_capacity(4096);
            let w = w.clone();
            let mut gdax_ws = DurationWindow::new(w.duration());
            let mut gdax_priv = DurationWindow::new(w.duration());
            let mut krkn_pub = DurationWindow::new(w.duration());
            let mut krkn_priv = DurationWindow::new(w.duration());
            let mut plnx_pub = DurationWindow::new(w.duration());
            let mut plnx_priv = DurationWindow::new(w.duration());
            let mut plnx_order = DurationWindow::new(w.duration());

            // yes I am intentionally breaking from the hard-typed duration
            // window ... that was a stupid idea
            //
            let mut krkn_trade_30 = DurationWindow::new(Duration::from_secs(30));
            let mut krkn_trade_300 = DurationWindow::new(Duration::from_secs(300));
            //let mut gdax_ws_nolock = DurationWindow::new(w.duration());
            //let mut event_loop = DurationWindow::new(w.duration());
            let mut last_broadcast = Instant::now();
            loop {
                if let Ok(msg) = rx.recv() {
                    match msg {
                        ExperiencedLatency::Terminate => {
                            //println!("latency manager terminating");
                            break;
                        }
                        ExperiencedLatency::GdaxWebsocket(d) => gdax_ws.update(Instant::now(), d),
                        //ExperiencedLatency::GdaxWebsocketNoLock(d) => gdax_ws_nolock.update(Instant::now(), d),
                        ExperiencedLatency::GdaxHttpPrivate(d) => gdax_priv.update(Instant::now(), d),
                        ExperiencedLatency::KrknHttpPublic(d) => krkn_pub.update(Instant::now(), d),
                        ExperiencedLatency::KrknHttpPrivate(d) => krkn_priv.update(Instant::now(), d),
                        ExperiencedLatency::PlnxHttpPublic(d) => plnx_pub.update(Instant::now(), d),
                        ExperiencedLatency::PlnxHttpPrivate(d) => plnx_priv.update(Instant::now(), d),
                        ExperiencedLatency::PlnxOrderBook(d) => plnx_order.update(Instant::now(), d),
                        ExperiencedLatency::KrknTrade(d) => {
                            let n = DurationWindow::nanos(d);
                            krkn_trade_30.update(Instant::now(), d);
                            krkn_trade_300.update(Instant::now(), d);
                            let mut m = Measurement::new("krkn_trade_api");
                            m.add_field("nanos", Value::Integer(n as i64));
                            m.set_timestamp(now());
                            influx::serialize(&m, &mut buf);
                            socket.send_str(&buf, 0);
                            buf.clear();
                        }
                        //ExperiencedLatency::EventLoop(d) => event_loop.update(Instant::now(), d),
                        other => {}
                    }
                }

                if Instant::now() - last_broadcast > Duration::from_millis(100) {
                    let now = Instant::now();
                    krkn_trade_30.refresh(&now);
                    krkn_trade_300.refresh(&now);
                    let update = LatencyUpdate {
                        gdax_ws: gdax_ws.refresh(&now).mean_nanos(),
                        //gdax_ws_nolock: gdax_ws_nolock.refresh(&now).mean_nanos(),
                        krkn_pub: krkn_pub.refresh(&now).mean_nanos(),
                        krkn_priv: krkn_priv.refresh(&now).mean_nanos(),
                        plnx_pub: plnx_pub.refresh(&now).mean_nanos(),
                        plnx_priv: plnx_priv.refresh(&now).mean_nanos(),
                        plnx_order: plnx_order.refresh(&now).mean_nanos(),

                        krkn_trade_30_mean: krkn_trade_30.mean_nanos(),
                        krkn_trade_30_max: krkn_trade_30.max_nanos().unwrap_or(0),

                        krkn_trade_300_mean: krkn_trade_300.mean_nanos(),
                        krkn_trade_300_max: krkn_trade_300.max_nanos().unwrap_or(0),

                        //event_loop: event_loop.refresh(&now).mean_nanos(),
                        size: w.clone(),
                    };
                    channel.send(update);
                    last_broadcast = now;
                }
            }
        }));

        LatencyManager {
            tx: tx_copy,
            channel: channel_copy,
            thread
        }
    }
}






