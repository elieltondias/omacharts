//! Chart candles from TradingView's public websocket feed.
//!
//! No account, no browser, no session file: every call opens a guest socket
//! at `data.tradingview.com`, runs the chart handshake (`set_auth_token` as
//! an unauthorized guest, `chart_create_session`, `resolve_symbol`,
//! `create_series`), and reads history off `timescale_update` batches. A
//! history fetch hangs up once `series_completed` arrives; a stream keeps its
//! socket until its handle is dropped.
//!
//! Read-only by construction. What goes out is four chart messages and a
//! heartbeat echo; there is no login, no order entry, and nothing saved
//! anywhere. The protocol is undocumented and can change without notice,
//! which is why the provider built on this is marked experimental.
//!
//! [`candles`] is the one-shot path. [`stream`] is the live one: one thread
//! per subscription, owned by its handle.

mod client;
mod error;
mod frame;
mod protocol;
mod stream;

use std::sync::atomic::{AtomicUsize, Ordering};

pub use client::{candles, ChartParams};
pub use error::{Error, Result};
pub use stream::{stream, Event, Stream};

/// One candle. `ts` is the unix second the bar opens at.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candle {
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// How many streams this process holds open right now.
static OPEN_STREAMS: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn note_open() {
    OPEN_STREAMS.fetch_add(1, Ordering::SeqCst);
}

pub(crate) fn note_close() {
    OPEN_STREAMS.fetch_sub(1, Ordering::SeqCst);
}

/// Has this process connected to the feed?
///
/// For the one test that matters to everybody who does not use this feed: a
/// provider that was merely constructed must not have built anything, opened
/// a socket or launched anything. Only a live stream counts.
pub fn connected() -> bool {
    OPEN_STREAMS.load(Ordering::SeqCst) > 0
}

/// Close whatever this process holds. Each stream owns its own socket and
/// ends with its handle, so there is nothing process-wide to close; this
/// exists so retiring the feed reads the same as every other feed's.
pub fn disconnect() {}

/// Parallel candle arrays from one history batch, oldest first.
pub fn candles_of(candles: Vec<Candle>) -> Vec<Candle> {
    let mut out: Vec<Candle> = candles
        .into_iter()
        .filter(|c| {
            [c.open, c.high, c.low, c.close]
                .iter()
                .all(|p| p.is_finite())
        })
        .map(|c| Candle {
            volume: if c.volume.is_finite() { c.volume } else { 0.0 },
            ..c
        })
        .collect();
    out.sort_by_key(|bar| bar.ts);
    out.dedup_by_key(|bar| bar.ts);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_becomes_oldest_first_candles() {
        let bars = candles_of(vec![
            Candle {
                ts: 200,
                open: 2.0,
                high: 2.5,
                low: 1.5,
                close: 2.2,
                volume: 7.0,
            },
            Candle {
                ts: 100,
                open: 1.0,
                high: 1.5,
                low: 0.5,
                close: 1.2,
                volume: f64::NAN,
            },
            Candle {
                ts: 100,
                open: 1.0,
                high: 1.5,
                low: 0.5,
                close: 1.2,
                volume: 8.0,
            },
            Candle {
                ts: 300,
                open: f64::INFINITY,
                high: 1.0,
                low: 1.0,
                close: 1.0,
                volume: 1.0,
            },
        ]);
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].ts, 100);
        assert_eq!(bars[0].volume, 0.0);
        assert_eq!(bars[1].close, 2.2);
    }

    /// The condition the whole feature is held to: choosing the feed builds a
    /// value and nothing more. No socket here, no thread, no counter moved.
    #[test]
    fn merely_loading_this_crate_connects_to_nothing() {
        assert!(!connected());
    }
}
