//! Charts from TradingView's public feed.
//!
//! Candles come from [`tradingview_market`]: a guest websocket per fetch, a
//! guest websocket per stream, no account and nothing saved. This claims no
//! delay, like any live feed — and like any feed resting on an undocumented
//! gateway, it is marked experimental: it works until TradingView changes
//! their web client, and then it stops, through no fault of anybody here.

use std::time::{Duration, Instant};

use crate::bars::{Bar, Timeframe, Unit};
use crate::provider::{
    Capability, FetchFailure, Pacing, Provider, ProviderError, Sink, Stream, Subscription, Update,
};
use crate::symbols::{Instrument, InstrumentKind};

/// What TradingView serves directly, and how far back each goes.
///
/// Anything else is folded from one of these by the caller, which is why a
/// three-minute chart works at all.
const CAPABILITIES: &[Capability] = &[
    Capability { timeframe: Timeframe::minutes(1), history_days: Some(30), max_request_days: None },
    Capability { timeframe: Timeframe::minutes(5), history_days: Some(365), max_request_days: None },
    Capability { timeframe: Timeframe::minutes(15), history_days: Some(365), max_request_days: None },
    Capability { timeframe: Timeframe::minutes(30), history_days: Some(365), max_request_days: None },
    Capability { timeframe: Timeframe::hours(1), history_days: Some(730), max_request_days: None },
    Capability { timeframe: Timeframe::hours(2), history_days: Some(730), max_request_days: None },
    Capability { timeframe: Timeframe::hours(4), history_days: Some(730), max_request_days: None },
    Capability { timeframe: Timeframe::days(1), history_days: None, max_request_days: None },
    Capability { timeframe: Timeframe::weeks(1), history_days: None, max_request_days: None },
];

/// Smallest gap between two requests the user is waiting on, and between two
/// nobody asked for. A socket per fetch means bursts cost connections rather
/// than one queue slot, so both lean conservative — the cache above this
/// layer is what makes fetching feel fast, not the request rate.
const MIN_GAP: Duration = Duration::from_millis(500);
const MIN_GAP_SPECULATIVE: Duration = Duration::from_millis(2_000);

/// The two gaps as the queue reads them. See [`Provider::pacing`].
const PACING: Pacing = Pacing {
    min_gap: MIN_GAP,
    min_gap_speculative: MIN_GAP_SPECULATIVE,
};

/// How long to stop asking entirely after throttling, and the ceiling that
/// repeated throttling climbs to.
const COOLDOWN_START: Duration = Duration::from_secs(60);
const COOLDOWN_MAX: Duration = Duration::from_secs(900);

/// Transient failures worth two more tries, each twice as far out as the
/// last. The queue does the waiting; this only has to say how long.
const RETRIES: u32 = 2;
const BACKOFF: Duration = Duration::from_millis(400);

/// How many bars a first fetch asks for. The feed takes a count, not a date
/// window, so "everything" is the most it will hand over in one answer.
const FULL_COUNT: usize = 5_000;

/// Whether the feed has told us to go away, and for how long it would next
/// time. State, not timing: this remembers what was said and reports until
/// when it stands; sitting it out is the queue's job.
struct Cooldown {
    until: Option<Instant>,
    length: Duration,
}

impl Cooldown {
    fn new() -> Cooldown {
        Cooldown {
            until: None,
            length: COOLDOWN_START,
        }
    }

    fn throttled(&mut self, now: Instant) {
        self.until = Some(now + self.length);
        self.length = (self.length * 2).min(COOLDOWN_MAX);
    }

    fn succeeded(&mut self) {
        self.length = COOLDOWN_START;
    }
}

/// How TradingView is offered.
///
/// No setup — a guest socket charts public symbols — and experimental, for
/// the undocumented protocol underneath. What it does *not* say is how fresh
/// the charts are: [`crate::providers::freshness`] answers that from what
/// this provider reports, because the answer changes the day the stream goes
/// away.
pub const LISTED: crate::providers::Listed = crate::providers::Listed {
    id: "tradingview",
    label: "TradingView",
    serves: "Stocks, futures, FX and crypto on TradingView's public feed",
    setup: None,
    experimental: true,
};

pub struct TradingView {
    cooldown: std::sync::Mutex<Cooldown>,
}

impl Default for TradingView {
    fn default() -> TradingView {
        TradingView::new()
    }
}

impl TradingView {
    pub fn new() -> TradingView {
        TradingView {
            cooldown: std::sync::Mutex::new(Cooldown::new()),
        }
    }

    /// TradingView's own spelling of a resolution, for the ones it serves.
    fn resolution(timeframe: Timeframe) -> Option<&'static str> {
        match (timeframe.unit, timeframe.count) {
            (Unit::Minute, 1) => Some("1"),
            (Unit::Minute, 5) => Some("5"),
            (Unit::Minute, 15) => Some("15"),
            (Unit::Minute, 30) => Some("30"),
            (Unit::Hour, 1) => Some("60"),
            (Unit::Hour, 2) => Some("120"),
            (Unit::Hour, 4) => Some("240"),
            (Unit::Day, 1) => Some("1D"),
            (Unit::Week, 1) => Some("1W"),
            _ => None,
        }
    }

    /// How many bars to ask for: enough to reach `since`, or everything the
    /// feed hands over when there is nothing to reach from.
    fn count(timeframe: Timeframe, since: Option<i64>, now: i64) -> usize {
        match since {
            None => FULL_COUNT,
            Some(since) => {
                let span = now.saturating_sub(since).max(0) as u64;
                let bars = span / timeframe.seconds().max(1) as u64 + 10;
                bars.clamp(10, FULL_COUNT as u64) as usize
            }
        }
    }
}

/// This instrument in TradingView's `EXCHANGE:CODE` spelling, if it has one.
pub fn tv_symbol(instrument: &Instrument) -> Option<String> {
    if let Some(explicit) = instrument.override_for("tradingview") {
        return Some(explicit.to_string());
    }
    Some(match instrument.kind {
        InstrumentKind::Crypto => crypto_symbol(&instrument.symbol),
        InstrumentKind::Fx => format!("FX:{}", instrument.symbol.to_uppercase()),
        InstrumentKind::FutureRoot => future_symbol(&instrument.symbol)?.to_string(),
        InstrumentKind::Index => index_symbol(instrument)?,
        InstrumentKind::Equity | InstrumentKind::Etf => equity_symbol(instrument),
    })
}

/// `BTC` is `BINANCE:BTCUSDT`; a code already carrying its quote stays as is.
fn crypto_symbol(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    if upper.ends_with("USDT") || upper.ends_with("USD") || upper.contains(':') {
        return upper;
    }
    format!("BINANCE:{upper}USDT")
}

/// Continuous contracts, by the exchange code TradingView resolves:
/// `ES` is `CME_MINI:ES1!`. Verified against the live feed — the nearby
/// codes look right and do not resolve (`CME:ES1!`, `ICE:KC1!`).
fn future_symbol(root: &str) -> Option<&'static str> {
    Some(match root.to_uppercase().as_str() {
        "ES" => "CME_MINI:ES1!",
        "MES" => "CME_MINI:MES1!",
        "NQ" => "CME_MINI:NQ1!",
        "MNQ" => "CME_MINI:MNQ1!",
        "YM" => "CBOT_MINI:YM1!",
        "MYM" => "CBOT_MINI:MYM1!",
        "RTY" => "CME_MINI:RTY1!",
        "M2K" => "CME_MINI:M2K1!",
        "CL" => "NYMEX:CL1!",
        "MCL" => "NYMEX:MCL1!",
        "BZ" => "NYMEX:BZ1!",
        "NG" => "NYMEX:NG1!",
        "RB" => "NYMEX:RB1!",
        "HO" => "NYMEX:HO1!",
        "GC" => "COMEX:GC1!",
        "MGC" => "COMEX_MINI:MGC1!",
        "SI" => "COMEX:SI1!",
        "SIL" => "COMEX_MINI:SIL1!",
        "HG" => "COMEX:HG1!",
        "PL" => "NYMEX:PL1!",
        "PA" => "NYMEX:PA1!",
        "ZB" => "CBOT:ZB1!",
        "ZN" => "CBOT:ZN1!",
        "ZF" => "CBOT:ZF1!",
        "ZT" => "CBOT:ZT1!",
        "ZC" => "CBOT:ZC1!",
        "ZS" => "CBOT:ZS1!",
        "ZW" => "CBOT:ZW1!",
        "ZL" => "CBOT:ZL1!",
        "ZM" => "CBOT:ZM1!",
        "LE" => "CME:LE1!",
        "HE" => "CME:HE1!",
        "KC" => "ICEUS:KC1!",
        "SB" => "ICEUS:SB1!",
        "CC" => "ICEUS:CC1!",
        "CT" => "ICEUS:CT1!",
        "6E" => "CME:6E1!",
        "6J" => "CME:6J1!",
        "6B" => "CME:6B1!",
        "6A" => "CME:6A1!",
        "6C" => "CME:6C1!",
        "6S" => "CME:6S1!",
        _ => return None,
    })
}

/// The well-known indexes by the names TradingView resolves, verified
/// against the live feed. Anything else is tried on TradingView's own index
/// feed rather than guessed: a miss comes back as no such symbol, which a
/// chart can say out loud.
fn index_symbol(instrument: &Instrument) -> Option<String> {
    // A dotted index is a listing of its own: the FTSE MIB resolves under
    // TradingView's generic index namespace.
    if instrument.symbol.contains('.') {
        return Some("INDEX:FTSEMIB".to_string());
    }
    Some(
        match instrument.symbol.to_uppercase().as_str() {
            "GSPC" => "SP:SPX",
            "NDX" => "NASDAQ:NDX",
            "IXIC" => "NASDAQ:IXIC",
            "DJI" => "DJ:DJI",
            "RUT" => "TVC:RUT",
            "VIX" => "CBOE:VIX",
            "TNX" => "TVC:TNX",
            "TYX" => "TVC:TYX",
            "FVX" => "TVC:US05Y",
            "NYA" => "NYSE:NYA",
            "SOX" => "NASDAQ:SOX",
            "FTSE" => "TVC:UKX",
            "GDAXI" => "XETR:DAX",
            "FCHI" => "EURONEXT:CAC",
            "STOXX50E" => "TVC:SX5E",
            "IBEX" => "BME:IBC",
            "AEX" => "EURONEXT:AEX",
            "SSMI" => "SIX:SMI",
            "N225" => "TVC:NI225",
            "HSI" => "HSI:HSI",
            "AXJO" => "ASX:XJO",
            "GSPTSE" => "TSX:TSX",
            "BVSP" => "BMFBOVESPA:IBOV",
            "KS11" => "KRX:KOSPI",
            "TWII" => "TWSE:TAIEX",
            "BSESN" => "BSE:SENSEX",
            "DXY" => "TVC:DXY",
            _ => return Some(format!("TVC:{}", instrument.symbol.to_uppercase())),
        }
        .to_string(),
    )
}

/// Shares: the venue suffix names the exchange, and a US listing without one
/// uses the venue the inventory carries — defaulting to NASDAQ, which the
/// resolver forgives more often than the alternative.
fn equity_symbol(instrument: &Instrument) -> String {
    let base = instrument.symbol.to_uppercase();
    if let Some(suffix) = instrument.suffix.as_deref() {
        let exchange = suffix_exchange(suffix)
            .map(str::to_string)
            .unwrap_or_else(|| suffix.to_uppercase());
        return format!("{exchange}:{base}");
    }
    let exchange = instrument
        .exchange
        .as_deref()
        .map(us_exchange)
        .unwrap_or("NASDAQ");
    format!("{exchange}:{base}")
}

/// A Yahoo-style venue suffix as the exchange TradingView lists it under,
/// verified against the live feed: Xetra is `XETR`, Euronext Paris and
/// Amsterdam are `EURONEXT`, Milan is `MIL`.
fn suffix_exchange(suffix: &str) -> Option<&'static str> {
    Some(match suffix.to_uppercase().as_str() {
        "MC" => "BME",
        "L" => "LSE",
        "DE" => "XETR",
        "PA" => "EURONEXT",
        "SW" => "SIX",
        "T" => "TSE",
        "TO" => "TSX",
        "AS" => "EURONEXT",
        "MI" => "MIL",
        "HK" => "HKEX",
        "TW" => "TWSE",
        "TWO" => "TPEX",
        "SA" => "BMFBOVESPA",
        _ => return None,
    })
}

/// A US venue as TradingView spells it. The generated listings name them
/// with spaces (`NYSE Arca`, `Cboe BZX`), so punctuation is stripped before
/// comparing — `SPY` resolving as `NASDAQ:SPY` is how an ETF ended up with
/// no data.
fn us_exchange(exchange: &str) -> &'static str {
    let flat: String =
        exchange.to_uppercase().chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    match flat.as_str() {
        "NYSE" => "NYSE",
        "NASDAQ" => "NASDAQ",
        "AMEX" | "NYSEARCA" | "NYSEMKT" | "NYSEAMERICAN" => "AMEX",
        "BATS" | "CBOEBZX" | "CBOE" => "BATS",
        "OTC" | "OTCMKTS" | "OTCBB" => "OTC",
        _ => "NASDAQ",
    }
}

fn map_err(error: tradingview_market::Error) -> ProviderError {
    use tradingview_market::Error as E;
    match error {
        // The resolver's verdict on the ticker, and a series complaint in
        // the same words: the answer is a different ticker, not waiting.
        E::Symbol(_) => ProviderError::NotFound,
        E::Series(message) if mentions_symbol(&message) => ProviderError::NotFound,
        E::Series(message) | E::Protocol(message) => ProviderError::Malformed(message),
        E::RateLimited => ProviderError::RateLimited,
        E::Offline(message) => ProviderError::Offline(message),
        E::Timeout(message) | E::Unreachable(message) => ProviderError::Network(message),
        E::ConnectionLost { sent } => {
            ProviderError::Network(format!("connection lost (request sent: {sent})"))
        }
        E::Closed => ProviderError::Network("connection closed".into()),
        E::WebSocket(error) => ProviderError::Network(error.to_string()),
        E::Json(error) => ProviderError::Malformed(error.to_string()),
        E::Other(message) => ProviderError::Network(message),
    }
}

/// A refusal that is about the request rather than the wire. TradingView
/// answers unknown tickers through neighbouring methods, so the words
/// decide, not the variant.
fn mentions_symbol(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("symbol") || lower.contains("unknown") || lower.contains("not found")
}

fn bar_of(candle: tradingview_market::Candle) -> Bar {
    Bar {
        ts: candle.ts,
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume,
    }
}

/// An open chart stream. Dropping it ends the feed's thread.
struct Live(#[allow(dead_code)] tradingview_market::Stream);

impl Subscription for Live {}

/// The whole of what this feed does to stream: name the resolution, hand the
/// client a sink, and translate what comes back. Sharing one subscription
/// between charts, coalescing ticks to frames, writing behind to the cache
/// and deciding when to try again after a loss all happen above this line
/// and know nothing about the feed.
impl Stream for TradingView {
    fn subscribe(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        since: Option<i64>,
        sink: Sink,
    ) -> Result<Box<dyn Subscription>, ProviderError> {
        let resolution = Self::resolution(timeframe).ok_or_else(|| {
            ProviderError::Unsupported(format!("{} is folded, not fetched", timeframe.label()))
        })?;
        let count = Self::count(timeframe, since, chrono::Utc::now().timestamp());
        let params = tradingview_market::ChartParams::new(symbol, resolution, count);
        let handle = tradingview_market::stream(
            params,
            Box::new(move |event| match event {
                tradingview_market::Event::Snapshot(candles) => {
                    sink(Update::Snapshot(candles.into_iter().map(bar_of).collect()));
                }
                tradingview_market::Event::Candles(candles) => {
                    for candle in candles {
                        sink(Update::Bar(bar_of(candle)));
                    }
                }
                tradingview_market::Event::Lost(error) => {
                    sink(Update::Lost(FetchFailure::from(&map_err(error))));
                }
            }),
        );
        Ok(Box::new(Live(handle)))
    }
}

impl Provider for TradingView {
    fn id(&self) -> &'static str {
        "tradingview"
    }

    fn label(&self) -> &'static str {
        "TradingView"
    }

    /// The public feed prints as it trades. Claim no delay.
    fn delay_minutes(&self, _kind: InstrumentKind) -> u32 {
        0
    }

    fn symbol_for(&self, instrument: &Instrument) -> Option<String> {
        tv_symbol(instrument)
    }

    fn capabilities(&self) -> &'static [Capability] {
        CAPABILITIES
    }

    fn bars(
        &self,
        symbol: &str,
        timeframe: Timeframe,
        since: Option<i64>,
    ) -> Result<Vec<Bar>, ProviderError> {
        let resolution = Self::resolution(timeframe).ok_or_else(|| {
            ProviderError::Unsupported(format!("{} is folded, not fetched", timeframe.label()))
        })?;
        let count = Self::count(timeframe, since, chrono::Utc::now().timestamp());
        match tradingview_market::candles(symbol, resolution, count) {
            Ok(candles) => {
                self.cooldown
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .succeeded();
                let mut bars: Vec<Bar> = candles.into_iter().map(bar_of).collect();
                if let Some(since) = since {
                    bars.retain(|bar| bar.ts >= since);
                }
                Ok(bars)
            }
            Err(error) => {
                if matches!(error, tradingview_market::Error::RateLimited) {
                    self.cooldown
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .throttled(Instant::now());
                }
                Err(map_err(error))
            }
        }
    }

    fn pacing(&self) -> Pacing {
        PACING
    }

    fn cooldown_until(&self) -> Option<Instant> {
        self.cooldown
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .until
    }

    /// A dropped connection is worth two more tries, each twice as far out
    /// as the last. Throttling is not: asking again is what makes the
    /// cooldown longer. And an answer about the symbol will not have changed
    /// in half a second.
    fn retry_after(&self, error: &ProviderError, attempt: u32) -> Option<Duration> {
        let transient = matches!(
            error,
            ProviderError::Network(_) | ProviderError::Malformed(_)
        );
        (transient && attempt < RETRIES).then(|| BACKOFF * (1 << attempt))
    }

    /// The feed pushes each print, so this provider streams — and saying so
    /// is handing the stream over, nothing more.
    fn stream(&self) -> Option<&dyn Stream> {
        Some(self)
    }

    /// Each fetch and each stream owns its socket, so there is no connection
    /// to retire. Dropping the subscriptions already ended them.
    fn retire(&self) {
        tradingview_market::disconnect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument(kind: InstrumentKind, symbol: &str) -> Instrument {
        Instrument {
            symbol: symbol.into(),
            name: String::new(),
            kind,
            suffix: None,
            currency: None,
            tier: 0,
            session_origin: 0,
            overrides: Vec::new(),
            exchange: None,
            popularity: 0,
            local_name: None,
        }
    }

    #[test]
    fn tradingview_streams_so_charts_are_never_polled() {
        assert_eq!(
            TradingView::new().delivery(),
            crate::provider::Delivery::Streamed
        );
    }

    #[test]
    fn us_listings_futures_fx_and_crypto_have_names() {
        let tv = TradingView::new();
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Equity, "AAPL"))
                .as_deref(),
            Some("NASDAQ:AAPL")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Equity, "BRK.B"))
                .as_deref(),
            Some("NASDAQ:BRK.B")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Etf, "SPY"))
                .as_deref(),
            Some("NASDAQ:SPY")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::FutureRoot, "ES"))
                .as_deref(),
            Some("CME_MINI:ES1!")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::FutureRoot, "YM"))
                .as_deref(),
            Some("CBOT_MINI:YM1!")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::FutureRoot, "KC"))
                .as_deref(),
            Some("ICEUS:KC1!")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::FutureRoot, "MGC"))
                .as_deref(),
            Some("COMEX_MINI:MGC1!")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::FutureRoot, "BZ"))
                .as_deref(),
            Some("NYMEX:BZ1!")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::FutureRoot, "6E"))
                .as_deref(),
            Some("CME:6E1!")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Index, "GSPC"))
                .as_deref(),
            Some("SP:SPX")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Index, "DXY"))
                .as_deref(),
            Some("TVC:DXY")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Index, "GDAXI"))
                .as_deref(),
            Some("XETR:DAX")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Index, "FVX"))
                .as_deref(),
            Some("TVC:US05Y")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Index, "HSI"))
                .as_deref(),
            Some("HSI:HSI")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Index, "FTSEMIB.MI"))
                .as_deref(),
            Some("INDEX:FTSEMIB")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Fx, "EURUSD"))
                .as_deref(),
            Some("FX:EURUSD")
        );
        assert_eq!(
            tv.symbol_for(&instrument(InstrumentKind::Crypto, "BTC"))
                .as_deref(),
            Some("BINANCE:BTCUSDT")
        );
    }

    #[test]
    fn listings_abroad_use_the_venues_tradingview_lists_them_under() {
        let mut sap = instrument(InstrumentKind::Equity, "SAP");
        sap.suffix = Some("DE".into());
        assert_eq!(
            TradingView::new().symbol_for(&sap).as_deref(),
            Some("XETR:SAP")
        );

        let mut air = instrument(InstrumentKind::Equity, "AIR");
        air.suffix = Some("PA".into());
        assert_eq!(
            TradingView::new().symbol_for(&air).as_deref(),
            Some("EURONEXT:AIR")
        );

        let mut eni = instrument(InstrumentKind::Equity, "ENI");
        eni.suffix = Some("MI".into());
        assert_eq!(
            TradingView::new().symbol_for(&eni).as_deref(),
            Some("MIL:ENI")
        );

        let mut san = instrument(InstrumentKind::Equity, "SAN");
        san.suffix = Some("MC".into());
        assert_eq!(
            TradingView::new().symbol_for(&san).as_deref(),
            Some("BME:SAN")
        );

        let mut tsmc = instrument(InstrumentKind::Equity, "2330");
        tsmc.suffix = Some("TW".into());
        assert_eq!(
            TradingView::new().symbol_for(&tsmc).as_deref(),
            Some("TWSE:2330")
        );
    }

    #[test]
    fn the_venue_a_listing_carries_wins_over_the_default() {
        let mut listed = instrument(InstrumentKind::Equity, "A");
        listed.exchange = Some("NYSE".into());
        assert_eq!(
            TradingView::new().symbol_for(&listed).as_deref(),
            Some("NYSE:A")
        );
    }

    #[test]
    fn venues_spelled_with_spaces_still_resolve() {
        // The generated listings write `NYSE Arca` and `Cboe BZX`: without
        // normalising, an ETF went out as `NASDAQ:SPY` and came back with
        // no data.
        let mut spy = instrument(InstrumentKind::Etf, "SPY");
        spy.exchange = Some("NYSE Arca".into());
        assert_eq!(
            TradingView::new().symbol_for(&spy).as_deref(),
            Some("AMEX:SPY")
        );

        let mut bzx = instrument(InstrumentKind::Equity, "AAAA");
        bzx.exchange = Some("Cboe BZX".into());
        assert_eq!(
            TradingView::new().symbol_for(&bzx).as_deref(),
            Some("BATS:AAAA")
        );
    }

    #[test]
    fn an_explicit_override_wins() {
        let mut es = instrument(InstrumentKind::FutureRoot, "ES");
        es.overrides.push(("tradingview".into(), "CME_MINI:ES1!".into()));
        assert_eq!(
            TradingView::new().symbol_for(&es).as_deref(),
            Some("CME_MINI:ES1!")
        );
    }

    #[test]
    fn an_unknown_future_has_no_name_rather_than_a_guess() {
        assert!(
            TradingView::new()
                .symbol_for(&instrument(InstrumentKind::FutureRoot, "ZZZ"))
                .is_none()
        );
    }

    #[test]
    fn resolutions_tradingview_serves_are_its_own_codes() {
        assert_eq!(TradingView::resolution(Timeframe::minutes(1)), Some("1"));
        assert_eq!(TradingView::resolution(Timeframe::hours(1)), Some("60"));
        assert_eq!(TradingView::resolution(Timeframe::hours(4)), Some("240"));
        assert_eq!(TradingView::resolution(Timeframe::days(1)), Some("1D"));
        assert_eq!(TradingView::resolution(Timeframe::weeks(1)), Some("1W"));
    }

    #[test]
    fn derived_timeframes_are_never_fetched() {
        assert_eq!(TradingView::resolution(Timeframe::minutes(3)), None);
        assert_eq!(TradingView::resolution(Timeframe::hours(3)), None);
        let tv = TradingView::new();
        assert!(!tv.serves(Timeframe::minutes(3)));
        assert!(tv.serves(Timeframe::hours(1)));
        assert!(matches!(
            tv.bars("NASDAQ:AAPL", Timeframe::minutes(3), None),
            Err(ProviderError::Unsupported(_))
        ));
    }

    #[test]
    fn a_first_fetch_asks_for_depth_and_a_tail_for_a_handful() {
        let now = 1_700_000_000;
        assert_eq!(
            TradingView::count(Timeframe::days(1), None, now),
            FULL_COUNT
        );
        assert_eq!(
            TradingView::count(Timeframe::hours(1), Some(now - 3_600), now),
            11
        );
        assert_eq!(
            TradingView::count(Timeframe::minutes(1), Some(now - 100_000_000), now),
            FULL_COUNT,
            "a tail older than the cap still caps"
        );
    }

    /// Nothing here calls [`Provider::bars`] against the network. A test
    /// that did would reach TradingView from whatever machine ran it. The
    /// mapping is what has to be right, and the mapping can be asked
    /// directly — except for the refusal, which is refused before any
    /// thread starts.
    #[test]
    fn nothing_signed_in_is_needed_and_nothing_is_connected() {
        assert!(!tradingview_market::connected());
        let tv = TradingView::new();
        assert!(
            tv.subscribe("NASDAQ:AAPL", Timeframe::minutes(3), None, Box::new(|_| {}))
                .is_err()
        );
    }

    #[test]
    fn an_unknown_ticker_is_a_missing_symbol() {
        let mapped = map_err(tradingview_market::Error::Symbol(
            "symbol_error: unknown".into(),
        ));
        assert!(matches!(mapped, ProviderError::NotFound), "{mapped}");
        assert_eq!(
            crate::provider::FetchFailure::from(&mapped),
            crate::provider::FetchFailure::NoSuchSymbol
        );
    }

    #[test]
    fn throttling_sets_a_cooldown_the_queue_can_read() {
        let tv = TradingView::new();
        assert_eq!(tv.cooldown_until(), None);
        // What `bars` does on the way past a refusal, without the network.
        tv.cooldown.lock().unwrap().throttled(Instant::now());
        assert!(tv.cooldown_until().is_some());
    }

    #[test]
    fn a_transient_failure_is_worth_two_more_tries_with_a_doubling_backoff() {
        let tv = TradingView::new();
        let dropped = ProviderError::Network("connection reset".into());
        assert_eq!(
            tv.retry_after(&dropped, 0),
            Some(Duration::from_millis(400))
        );
        assert_eq!(
            tv.retry_after(&dropped, 1),
            Some(Duration::from_millis(800))
        );
        assert_eq!(tv.retry_after(&dropped, 2), None);

        for error in [
            ProviderError::RateLimited,
            ProviderError::NotFound,
            ProviderError::Unsupported("3m".into()),
            ProviderError::Offline("no route".into()),
        ] {
            assert_eq!(tv.retry_after(&error, 0), None, "{error}");
        }
    }

    #[test]
    fn speculative_work_is_paced_far_further_apart() {
        let pacing = TradingView::new().pacing();
        assert_eq!(pacing.gap(false), MIN_GAP);
        assert_eq!(pacing.gap(true), MIN_GAP_SPECULATIVE);
        assert!(
            pacing.min_gap_speculative > pacing.min_gap * 3,
            "{pacing:?}"
        );
    }
}
