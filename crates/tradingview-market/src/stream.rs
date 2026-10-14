//! One chart subscription: a snapshot, then the bars each tick touched.
//!
//! One thread per stream, blocked on the feed's frames. It makes one attempt:
//! it opens its own guest socket, runs the handshake, and turns what comes
//! back into [`Event`]s until the socket dies, the feed refuses, or the handle
//! is dropped — at which point it says so, once, and ends. Whether and when
//! to try again is the caller's: it knows how many charts are waiting, and
//! this thread knows only one socket.
//!
//! What a tick costs here is what the wire taught: a `du` names only the bars
//! that moved, so the forming bar is read straight off the update and the
//! history behind it is never walked.

use std::io::ErrorKind;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::client::{ChartParams, FETCH_TIMEOUT};
use crate::error::{Error, Result};
use crate::frame;
use crate::protocol;
use crate::Candle;

type Socket = WebSocket<MaybeTlsStream<TcpStream>>;

/// How long a stream read blocks before the thread notices its handle went.
const READ_TIMEOUT: Duration = Duration::from_secs(1);

/// What a stream delivers.
#[derive(Debug)]
pub enum Event {
    /// The whole series, oldest first: the first thing a stream delivers,
    /// and again whenever the feed re-sends the document.
    Snapshot(Vec<Candle>),
    /// The bars a tick changed, oldest first — the forming bar, or the bar
    /// that just closed and the one that opened after it.
    Candles(Vec<Candle>),
    /// The stream has ended and this is why. Nothing follows.
    Lost(Error),
}

/// An open stream. Dropping it ends the thread without waiting for a frame —
/// the read times out within [`READ_TIMEOUT`] and the thread sees the flag.
pub struct Stream {
    stopped: Arc<AtomicBool>,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

/// Start streaming `params`, handing each event to `on` from the stream's own
/// thread.
///
/// Returns at once. Everything that can fail — unreachable, unknown symbol,
/// refused resolution — arrives as [`Event::Lost`].
pub fn stream(params: ChartParams, on: Box<dyn FnMut(Event) + Send>) -> Stream {
    let stopped = Arc::new(AtomicBool::new(false));
    let handle = Stream {
        stopped: stopped.clone(),
    };
    let name = format!("tradingview {} {}", params.symbol, params.resolution);
    crate::note_open();
    let spawned = std::thread::Builder::new().name(name).spawn(move || {
        let mut on = on;
        serve(&params, &mut on, &stopped);
        crate::note_close();
    });
    if let Err(error) = spawned {
        eprintln!("tradingview-market: could not start a stream thread: {error}");
    }
    handle
}

fn serve(params: &ChartParams, on: &mut dyn FnMut(Event), stopped: &AtomicBool) {
    match subscribe(params, on, stopped) {
        Ok(()) => {}
        Err(error) => {
            if stopped.load(Ordering::SeqCst) {
                return;
            }
            on(Event::Lost(error));
        }
    }
}

fn subscribe(params: &ChartParams, on: &mut dyn FnMut(Event), stopped: &AtomicBool) -> Result<()> {
    let mut socket = open()?;
    let chart_session = protocol::session_id("cs_");
    let symbol_session = "sds_sym_1";
    let series_id = "sds_1";

    let _ = next_text(&mut socket, stopped)?;
    for frame in [
        protocol::auth_message(),
        protocol::create_session_message(&chart_session),
        protocol::resolve_message(&chart_session, symbol_session, &params.symbol),
        protocol::series_message(
            &chart_session,
            series_id,
            "s1",
            symbol_session,
            &params.resolution,
            params.count,
        ),
    ] {
        send_framed(&mut socket, &frame)?;
    }

    // The first batch is the whole document whatever the feed calls it.
    let mut first = true;
    loop {
        if stopped.load(Ordering::SeqCst) {
            let _ = socket.close(None);
            return Ok(());
        }
        let text = next_text(&mut socket, stopped)?;
        for payload in frame::split(&text) {
            if frame::is_heartbeat(payload) {
                send_framed(&mut socket, payload)?;
                continue;
            }
            let value: Value = match serde_json::from_str(payload) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let Some(parsed) = protocol::parse(payload, &value) else {
                continue;
            };
            if let Some(error) = protocol::refusal(parsed.method, parsed.params) {
                return Err(error);
            }
            match parsed.method {
                "timescale_update" | "du" => {
                    let mut bars = protocol::bars_in(parsed.params, series_id);
                    if bars.is_empty() {
                        continue;
                    }
                    if first {
                        first = false;
                        on(Event::Snapshot(std::mem::take(&mut bars)));
                    } else {
                        on(Event::Candles(bars));
                    }
                }
                "series_completed" => {
                    // A re-sent document after a reconnect reads as a fresh
                    // snapshot once there are bars to show for it.
                    first = false;
                }
                _ => {}
            }
        }
    }
}

fn next_text(socket: &mut Socket, stopped: &AtomicBool) -> Result<String> {
    loop {
        if stopped.load(Ordering::SeqCst) {
            return Err(Error::Closed);
        }
        match socket.read() {
            Ok(Message::Text(text)) => return Ok(text.to_string()),
            Ok(Message::Binary(bytes)) => {
                if let Ok(text) = String::from_utf8(bytes.to_vec()) {
                    return Ok(text);
                }
            }
            Ok(Message::Ping(payload)) => {
                socket.send(Message::Pong(payload)).map_err(Error::from)?;
            }
            Ok(Message::Close(_)) => return Err(Error::Closed),
            Ok(_) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) =>
            {
                continue;
            }
            Err(error) => return Err(Error::from(error)),
        }
    }
}

fn send_framed(socket: &mut Socket, payload: &str) -> Result<()> {
    socket
        .send(Message::text(frame::encode(payload)))
        .map_err(Error::from)
}

fn open() -> Result<Socket> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut request = protocol::WS_URL.into_client_request()?;
    let headers = request.headers_mut();
    headers.insert("Origin", protocol::ORIGIN.parse().unwrap());
    headers.insert("User-Agent", protocol::USER_AGENT.parse().unwrap());

    let uri = request.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| Error::Other(format!("{} names no host", protocol::WS_URL)))?
        .to_string();
    let port = uri.port_u16().unwrap_or(443);
    let address = (host.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| Error::Offline(format!("cannot resolve {host}: {e}")))?
        .next()
        .ok_or_else(|| Error::Offline(format!("{host} resolves to no address")))?;
    let stream =
        TcpStream::connect_timeout(&address, FETCH_TIMEOUT).map_err(|e| match e.kind() {
            ErrorKind::NetworkUnreachable | ErrorKind::HostUnreachable | ErrorKind::NetworkDown => {
                Error::Offline(format!("cannot reach {host}: {e}"))
            }
            _ => Error::Unreachable(format!("{host}: {e}")),
        })?;
    let _ = stream.set_nodelay(true);
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|e| Error::Unreachable(format!("{host}: {e}")))?;
    let (socket, _) = tungstenite::client_tls(request, stream)
        .map_err(|e| Error::Unreachable(format!("{host}: {e}")))?;
    read_timeout(&socket)?;
    Ok(socket)
}

fn read_timeout(socket: &Socket) -> Result<()> {
    let stream = match socket.get_ref() {
        MaybeTlsStream::Plain(stream) => stream,
        MaybeTlsStream::Rustls(stream) => &stream.sock,
        _ => return Err(Error::Unreachable("socket has no readable timeout".into())),
    };
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|e| Error::Unreachable(format!("socket timeout: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Letting go of a stream ends its thread: dropping the handle sets the
    /// flag the thread watches between reads, so it never waits for a frame
    /// that may be hours away.
    #[test]
    fn dropping_the_handle_is_the_stop() {
        let stopped = Arc::new(AtomicBool::new(false));
        {
            let handle = Stream {
                stopped: stopped.clone(),
            };
            assert!(!handle.stopped.load(Ordering::SeqCst));
        }
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[test]
    fn a_loss_is_terminal_and_says_why() {
        let event = Event::Lost(Error::Unreachable("nobody home".into()));
        assert!(matches!(event, Event::Lost(Error::Unreachable(_))));
        let snapshot = Event::Snapshot(Vec::new());
        assert!(matches!(snapshot, Event::Snapshot(_)));
    }
}
