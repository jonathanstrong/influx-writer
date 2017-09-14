#[macro_use] extern crate slog;
extern crate logging;
extern crate slog_term;

use slog::*;
use logging::warnings::ZmqDrain;

use std::io::Write;
use std::thread;
use std::time::Duration;

fn main() {
    //let term_decorator = slog_term::TermDecorator::new().build();
    //let term_drain = slog_term::CompactFormat::new(term_decorator).build().fuse();
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let plain_fuse = slog_term::FullFormat::new(plain).build().fuse();
    let w = logging::warnings::WarningsManager::new("test");
    let w_drain = logging::warnings::WarningsDrain::new(w.tx.clone(), plain_fuse);
    //let zmq_drain = ZmqDrain::new(plain_fuse);
    //let zmq_decorator = slog_term::PlainSyncDecorator::new(zmq_drain);
    //let zmq_fuse = slog_term::FullFormat::new(zmq_decorator).build().fuse();
    let logger = Logger::root(w_drain, o!());
    //let logger = 
    //    Logger::root(Duplicate::new(plain_fuse, zmq_fuse).fuse(), o!());

    let mut i = 0;

    loop {
        info!(logger, "hello world";
              "i" => i);
        i += 1;
        thread::sleep(Duration::from_secs(1));
    }

}
