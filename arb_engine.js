/**
 * arb_engine.js (v2 — RTDS + Chainlink lag detection)
 *
 * WHAT CHANGED FROM v1:
 *
 * 1. REPLACED polling fetchers with RTDSClient (WebSocket)
 *    - Single persistent connection to Polymarket's RTDS
 *    - Receives Binance + Chainlink prices as they happen (push, not pull)
 *    - No more 2-second polling delay on price data
 *
 * 2. CHAINLINK IS NOW THE PRIMARY SIGNAL
 *    - K (opening price) = Chainlink price at window start
 *    - Fair probability calculated against Chainlink (resolution source)
 *    - Binance used only for velocity and lag detection
 *
 * 3. BINANCE GAP = THE EDGE DETECTOR
 *    - binance_gap = (binance - chainlink) / chainlink
 *    - gap >= +0.4%: Chainlink update imminent, UP resolution likely
 *    - gap <= -0.4%: Chainlink update imminent, DOWN resolution likely
 *    - gap ≈ 0:      Chainlink current, no lag edge to exploit
 *
 * 4. NEW DATABASE: arb_signals_v2.db
 *    - Includes chainlink_price, binance_gap, lag_detected columns
 *    - Clean slate for lag-detection strategy
 *    - Old arb_signals.db preserved untouched
 *
 * THE FULL SIGNAL LOGIC:
 *   Every time RTDS pushes a price update:
 *   1. Compute binance_gap
 *   2. If lag detected AND spread tight AND timing right:
 *      → Run Bayesian to get fair probability
 *      → Run Edge to confirm positive EV
 *      → Log signal + fire Telegram alert
 */

const EventEmitter  = require('events');
const Database      = require('better-sqlite3');
const path          = require('path');
const fs            = require('fs');

const RTDSClient        = require('./rtds_client');
const PolymarketFetcher = require('./polymarket_fetcher');
const BayesianEstimator = require('./bayesian');
const EdgeCalculator    = require('./edge_calculator');
const TelegramControl   = require('./telegram_control');
const KellySizer        = require('./kelly');
const SpreadModel       = require('./spread_model');

// Load .env
try { require('dotenv').config({ path: './.env' }); } catch(e) {}

// ── CRASH CAPTURE ─────────────────────────────────────────────────────────────
// Route uncaught errors to stderr so watchdog captures them in crash.log

process.on('uncaughtException', (err) => {
    process.stderr.write(`[FATAL] uncaughtException: ${err.stack || err}\n`);
    process.exit(1);
});
process.on('unhandledRejection', (reason) => {
    process.stderr.write(`[FATAL] unhandledRejection: ${reason?.stack || reason}\n`);
    process.exit(1);
});

// ── DATABASE ──────────────────────────────────────────────────────────────────

const dataDir = path.resolve(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

const db = new Database(path.join(dataDir, 'arb_signals_v2.db'));
db.pragma('journal_mode = WAL');

db.exec(`
    CREATE TABLE IF NOT EXISTS signals (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp         TEXT NOT NULL,
        window_slug       TEXT NOT NULL,
        seconds_left      INTEGER,

        -- Price feeds
        binance_price     REAL,
        chainlink_price   REAL,
        binance_gap_pct   REAL,     -- (binance - chainlink) / chainlink * 100
        lag_detected      INTEGER,  -- 1 if gap >= 0.4%
        lag_direction     TEXT,     -- 'UP' or 'DOWN'
        btc_velocity      REAL,     -- $/sec Binance is moving
        chainlink_age_ms  INTEGER,  -- ms since last Chainlink update

        -- Polymarket
        poly_price_up     REAL,
        poly_price_down   REAL,
        spread            REAL,
        bid_depth         INTEGER,
        ask_depth         INTEGER,
        liquidity         TEXT,

        -- Bayesian model
        opening_price     REAL,     -- K: Chainlink price at window open
        fair_prob_up      REAL,
        distance_strike   REAL,
        volatility        REAL,
        drift             REAL,
        observations      INTEGER,

        -- Edge evaluation
        raw_edge          REAL,
        net_edge          REAL,
        total_cost        REAL,
        direction         TEXT,
        tradeable         INTEGER,
        filter_failures   TEXT,

        -- Outcome tracking (filled in after window resolves)
        resolved_up       INTEGER,
        paper_pnl         REAL,
        alert_sent        INTEGER DEFAULT 0,
        post_vacuum       INTEGER DEFAULT 0,  -- 1 if orderbook was empty in this window recently
        vacuum_age_s      REAL,               -- seconds since vacuum ended when signal fired
        suppression_flags TEXT                -- flags: warmup,win_lock,trend,gap,dist_from_k or 'none'
    );

    CREATE TABLE IF NOT EXISTS windows (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        window_slug       TEXT UNIQUE,
        window_ts         INTEGER,
        opening_chainlink REAL,     -- Chainlink K at window start
        opening_binance   REAL,     -- Binance price at window start
        closing_chainlink REAL,
        resolved_up       INTEGER,
        total_signals     INTEGER DEFAULT 0,
        tradeable_signals INTEGER DEFAULT 0,
        best_edge         REAL DEFAULT 0,
        lag_signals       INTEGER DEFAULT 0, -- signals triggered by lag detection
        resolved_up       INTEGER,            -- 1=UP resolved, 0=DOWN resolved, null=pending
        final_chainlink   REAL                -- Chainlink price at window close
    );

    CREATE TABLE IF NOT EXISTS paper_trades (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       TEXT NOT NULL,
        window_slug     TEXT NOT NULL,
        direction       TEXT NOT NULL,        -- 'UP' or 'DOWN'
        entry_price     REAL NOT NULL,        -- price paid per share
        stake_amount    REAL NOT NULL,        -- dollars risked
        shares          REAL NOT NULL,        -- shares bought
        kelly_fraction  REAL,                 -- Kelly fraction used
        net_edge_pct    REAL,                 -- edge at time of signal
        resolved_up     INTEGER,              -- outcome (filled at rollover)
        pnl             REAL,                 -- profit/loss (filled at rollover)
        balance_before  REAL,                 -- bankroll before trade
        balance_after   REAL,                 -- bankroll after trade
        tier            TEXT                  -- position tier: A=40%+, B=30-40%, C=20-30%, D=10-20%
    );
`);

// Add tier column to existing DBs (no-op if already present)
try { db.exec('ALTER TABLE paper_trades ADD COLUMN tier TEXT'); } catch (_) {}
try { db.exec('ALTER TABLE paper_trades ADD COLUMN had_up_signal_60s INTEGER DEFAULT 0'); } catch (_) {}
try { db.exec('ALTER TABLE paper_trades ADD COLUMN had_up_signal_300s INTEGER DEFAULT 0'); } catch (_) {}
try { db.exec('ALTER TABLE paper_trades ADD COLUMN max_up_edge_recent REAL'); } catch (_) {}
try { db.exec('ALTER TABLE paper_trades ADD COLUMN secs_since_last_up_signal REAL'); } catch (_) {}
try { db.exec('ALTER TABLE paper_trades ADD COLUMN seconds_at_entry INTEGER'); } catch (_) {}
// Add suppression_flags column to existing DBs (no-op if already present)
try { db.exec('ALTER TABLE signals ADD COLUMN suppression_flags TEXT'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN near_threshold INTEGER DEFAULT 0'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN raw_edge_pct REAL'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN final_edge_pct REAL'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN trend_penalty REAL'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN trend_would_block INTEGER DEFAULT 0'); } catch (_) {}
// Add orderbook imbalance columns to existing DBs (no-op if already present)
try { db.exec('ALTER TABLE signals ADD COLUMN imbalance_ratio REAL'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN imbalance_delta REAL'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN imbalance_bucket TEXT'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN would_trade_down_imbalance INTEGER'); } catch (_) {}
try { db.exec('ALTER TABLE signals ADD COLUMN would_trade_up_imbalance INTEGER'); } catch (_) {}

db.exec(`CREATE TABLE IF NOT EXISTS up_diagnostics (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp         TEXT,
    window_slug       TEXT,
    seconds_left      INTEGER,
    fair_prob_up      REAL,
    market_prob_up    REAL,
    net_edge          REAL,
    suppression_reason TEXT,
    dist_from_k       REAL,
    binance_gap_pct   REAL
)`);

const insertUpDiag = db.prepare(`
    INSERT INTO up_diagnostics
        (timestamp, window_slug, seconds_left, fair_prob_up, market_prob_up,
         net_edge, suppression_reason, dist_from_k, binance_gap_pct)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertSignal = db.prepare(`
    INSERT INTO signals (
        timestamp, window_slug, seconds_left,
        binance_price, chainlink_price, binance_gap_pct, lag_detected,
        lag_direction, btc_velocity, chainlink_age_ms,
        poly_price_up, poly_price_down, spread, bid_depth, ask_depth, liquidity,
        opening_price, fair_prob_up, distance_strike, volatility, drift, observations,
        raw_edge, net_edge, total_cost, direction, tradeable, filter_failures, alert_sent,
        post_vacuum, vacuum_age_s, suppression_flags,
        imbalance_ratio, imbalance_delta, imbalance_bucket,
        would_trade_down_imbalance, would_trade_up_imbalance, near_threshold,
        raw_edge_pct, final_edge_pct, trend_penalty, trend_would_block,
        up_best_ask, down_best_ask, combined_ask, down_spread
    ) VALUES (
        ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?
    )
`);

const insertPaperTrade = db.prepare(`
    INSERT INTO paper_trades (
        timestamp, window_slug, direction, entry_price, stake_amount,
        shares, kelly_fraction, net_edge_pct, balance_before, tier,
        had_up_signal_60s, had_up_signal_300s, max_up_edge_recent, secs_since_last_up_signal,
        seconds_at_entry
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

function getUpContext() {
    const upCtx = db.prepare(`
        SELECT
            MAX(net_edge) as max_edge,
            MIN((julianday(datetime('now')) - julianday(timestamp)) * 86400) as secs_ago
        FROM up_diagnostics
        WHERE timestamp > datetime('now', '-300 seconds')
    `).get();
    const had60s = db.prepare(`
        SELECT COUNT(*) as n FROM up_diagnostics
        WHERE timestamp > datetime('now', '-60 seconds')
        AND net_edge >= 0.25
    `).get().n > 0 ? 1 : 0;
    const had300s = db.prepare(`
        SELECT COUNT(*) as n FROM up_diagnostics
        WHERE timestamp > datetime('now', '-300 seconds')
        AND net_edge >= 0.25
    `).get().n > 0 ? 1 : 0;
    return {
        had60s,
        had300s,
        maxEdge: upCtx?.max_edge ?? null,
        secsAgo: upCtx?.secs_ago ?? null,
    };
}

const resolvePaperTrades = db.prepare(`
    UPDATE paper_trades
    SET resolved_up = ?,
        pnl = CASE
            WHEN (direction = 'UP' AND ? = 1) OR (direction = 'DOWN' AND ? = 0)
            THEN shares * (1.0 - entry_price)   -- won: collect (1 - entry) per share
            ELSE -stake_amount                   -- lost: lose entire stake
        END,
        balance_after = balance_before + CASE
            WHEN (direction = 'UP' AND ? = 1) OR (direction = 'DOWN' AND ? = 0)
            THEN shares * (1.0 - entry_price)
            ELSE -stake_amount
        END
    WHERE window_slug = ? AND resolved_up IS NULL
`);

const resolveWindow = db.prepare(`
    UPDATE windows SET resolved_up = ?, final_chainlink = ?
    WHERE window_slug = ?
`);

const upsertWindow = db.prepare(`
    INSERT INTO windows (window_slug, window_ts, opening_chainlink, opening_binance,
                         total_signals, tradeable_signals, best_edge, lag_signals)
    VALUES (?, ?, ?, ?, 1, ?, ?, ?)
    ON CONFLICT(window_slug) DO UPDATE SET
        total_signals     = total_signals + 1,
        tradeable_signals = tradeable_signals + excluded.tradeable_signals,
        best_edge         = MAX(best_edge, excluded.best_edge),
        lag_signals       = lag_signals + excluded.lag_signals
`);

// ── TELEGRAM ──────────────────────────────────────────────────────────────────

async function sendTelegram(message) {
    const token  = process.env.TELEGRAM_BOT_TOKEN;
    const chatId = process.env.TELEGRAM_CHAT_ID;

    if (!token || !chatId) {
        console.log('[Telegram] Not configured — alert:\n' + message);
        return false;
    }

    try {
        const fetch = require('node-fetch');
        const res   = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
            method:  'POST',
            headers: { 'Content-Type': 'application/json' },
            body:    JSON.stringify({ chat_id: chatId, text: message, parse_mode: 'HTML' }),
            timeout: 15000
        });
        return res.ok;
    } catch (err) {
        console.error('[Telegram] Failed:', err.message);
        return false;
    }
}

// ── ARB ENGINE ────────────────────────────────────────────────────────────────

class ArbEngine {
    constructor() {
        this.rtds          = new RTDSClient();
        this.polymarket    = new PolymarketFetcher('btc');
        this.polymarketEth = new PolymarketFetcher('eth');  // ETH 5m, fetched in parallel
        this.bayesian  = new BayesianEstimator();
        this.edge      = new EdgeCalculator({ minNetEdge: 0.10 });

        this.lastWindowTs    = null;
        this.tickCount       = 0;
        this.signalCount      = 0;
        this.lagSignalCount   = 0;
        this.ethSignalCount   = 0;   // ETH trades fired this session
        this.sessionBtcTrades = 0;   // BTC paper trades this session
        this.sessionEthTrades = 0;   // ETH paper trades this session
        this.pendingRolloverTg = null; // carries BTC rollover info until ETH resolves
        this.running         = false;

        // Throttle Polymarket fetches — CLOB API has rate limits
        // We fetch market price max once per 2 seconds
        this.lastPolyFetch   = 0;
        this.polyFetchTTL    = 2000;
        this.lastPolyData    = null;
        this.lastEthFetch    = 0;
        this.lastEthData     = null;
        this.paused          = false;

        // Paper trading balance — resume from last known balance in DB
        // Falls back to $100 if no trades recorded yet
        this.paperBalance    = (() => {
            try {
                const last = db.prepare(`
                    SELECT COALESCE(balance_after, balance_before) as bal
                    FROM paper_trades
                    WHERE balance_after IS NOT NULL OR balance_before IS NOT NULL
                    ORDER BY id DESC LIMIT 1
                `).get();
                if (last && last.bal && last.bal > 0) {
                    console.log(`[Engine] 💰 Resuming paper balance: $${last.bal.toFixed(2)}`);
                    return last.bal;
                }
            } catch(e) {}
            console.log(`[Engine] 💰 Starting fresh paper balance: $100.00`);
            return 100.00;
        })();
        this.sessionStartBalance = this.paperBalance;  // for per-session P/L in Telegram
        this.lastRolloverTime    = 0;       // suppress tick burst after window rollover
        this.paperTrades     = [];      // trade history for P&L calc

        // ETH Bayesian model + window tracking (separate from BTC)
        this.ethBayesian  = new BayesianEstimator();
        this.ethWindowTs  = null;  // track ETH window rollover separately
        this.lastEthSignalTime   = 0;       // per-window cooldown for ETH
        this.lastEthSignalWindow = null;    // which window last ETH signal fired in
        this.ethWindowCooldownMs = 90000;   // 90s per-window lock (Tier A overridden to 3s below)
        this.btcWindowTradeCount = 0;       // trades fired in current BTC window (cap: 7)
        this.ethWindowTradeCount = 0;       // trades fired in current ETH window (cap: 7)

        // Model 3: Spread Compression — independent trackers for BTC and ETH
        this.spreadModel    = new SpreadModel();
        this.ethSpreadModel = new SpreadModel();

        // Kelly position sizer
        this.kelly = new KellySizer({
            kellyFraction: 0.25,  // quarter Kelly
            maxStakePct:   0.01,  // 1% max — matches trader's current ~$6.60 on unknown bankroll
            minStakePct:   0.005  // 0.5% floor
        });

        // Signal cooldown — prevent signal spam on same direction
        this.lastSignalTime  = 0;       // timestamp of last signal
        this.lastSignalDir   = null;    // direction of last signal
        this.lastSignalWindow = null;   // window of last signal
        this.signalCooldownMs = 30000;  // 30 seconds between signals

        // Warm-up gate — don't signal until model has enough observations
        this.minObservationsBeforeSignal = 15;  // ~50s at 2s polling — enough for model to stabilize

        // Trend filter — track last 2 window resolutions
        // If last 2 resolved UP, suppress DOWN signals (and vice versa)
        this.recentResolutions    = [];   // rolling list of last 3 outcomes (1=UP, 0=DOWN)
        this.ethRecentResolutions = [];   // separate resolution history for ETH trend filter
        this.trendFilterEnabled   = true;

        // Liquidity vacuum tracking
        // A vacuum = CLOB orderbook went empty mid-window (spread=null)
        // Tracks the last time we saw an empty orderbook
        this.lastVacuumTime   = 0;    // timestamp of last empty orderbook
        this.inVacuum         = false; // currently in a vacuum?
        this.vacuumWindowSlug = null;  // which window the vacuum was in
        this.postVacuumWindowMs = 120000; // flag signals for 2min after vacuum

        // Deferred window resolution (fired when Chainlink was stale at rollover)
        this.pendingResolution    = null;  // { prevSlug, prevK } for BTC
        this.pendingEthResolution = null;  // { prevEthSlug, prevEthK } for ETH

        // Telegram control panel
        this.control = new TelegramControl(this);
    }

    async start() {
        this.running = true;

        console.log('╔══════════════════════════════════════════════════╗');
        console.log('║   POLYMARKET ARB ENGINE v2 — RTDS + CHAINLINK   ║');
        console.log('╚══════════════════════════════════════════════════╝');
        console.log('Database: data/arb_signals_v2.db');
        console.log('Telegram: ' + (process.env.TELEGRAM_BOT_TOKEN ? 'connected ✅' : 'not configured'));
        console.log('Waiting for RTDS connection...\n');

        // Wire RTDS events
        this.rtds.on('connected', () => {
            console.log('[Engine] RTDS connected — waiting for first prices...');
            // Delay startup message 3s to let telegram_control long-poll establish first
            setTimeout(() => {
                sendTelegram(`🟢 <b>Arb Engine v2 started</b>\nRTDS connected — Chainlink + Binance feeds live\n💰 Resuming balance: $${this.paperBalance.toFixed(2)}`);
            }, 3000);
        });

        this.rtds.on('update', (state) => {
            if (this.running) this._onPriceUpdate(state);
        });

        this.rtds.on('lag_detected', (state) => {
            // Extra log line when lag first crosses threshold
            console.log(
                `[RTDS] ⚡ LAG: Binance=$${state.binancePrice.toFixed(2)} ` +
                `Chainlink=$${state.chainlinkPrice.toFixed(2)} ` +
                `Gap=${state.binanceGapPct >= 0 ? '+' : ''}${state.binanceGapPct.toFixed(3)}% ` +
                `→ ${state.lagDirection}`
            );
        });

        this.rtds.on('disconnected', () => {
            console.warn('[Engine] RTDS disconnected — will reconnect automatically');
        });

        // Start Telegram control panel
        this.control.start();

        // Connect RTDS
        this.rtds.connect();

        // Write status.json every 5 minutes for OpenClaw
        this._statusInterval = setInterval(() => {
            if (this.running) this._writeStatusFile();
        }, 5 * 60 * 1000);
        this._writeStatusFile();
    }

    /**
     * Called on every RTDS price update (Binance or Chainlink).
     * This is the main evaluation loop — runs on every tick.
     */
    async _onPriceUpdate(rtdsState) {
        this.tickCount++;

        // Respect pause command from Telegram
        if (this.paused) {
            console.log(`[Engine] Skipped tick ${this.tickCount}: engine paused`);
            return;
        }

        try {
            // ── Chainlink freshness (used for grace-check resolution) ───────
            const chainlinkFresh = !rtdsState.chainlinkAge || rtdsState.chainlinkAge < 8000;

            // ── Resolve any deferred BTC window if Chainlink is now fresh ───
            if (this.pendingResolution && chainlinkFresh) {
                const { prevSlug, prevK } = this.pendingResolution;
                const finalCL = rtdsState.chainlinkPrice;
                if (finalCL) {
                    const resolvedUp = finalCL >= prevK ? 1 : 0;
                    resolveWindow.run(resolvedUp, finalCL, prevSlug);
                    resolvePaperTrades.run(resolvedUp, resolvedUp, resolvedUp, resolvedUp, resolvedUp, prevSlug);
                    const rt = db.prepare('SELECT SUM(pnl) as total_pnl FROM paper_trades WHERE window_slug = ?').get(prevSlug);
                    const pnlD = Number(rt?.total_pnl) || 0;
                    const oldBal = this.paperBalance;
                    this.paperBalance = Math.max(0, this.paperBalance + pnlD);
                    if (Math.abs(this.paperBalance - oldBal) > 0.01) {
                        console.log(`[Balance] ${oldBal.toFixed(2)} → ${this.paperBalance.toFixed(2)} (deferred BTC ${pnlD >= 0 ? '+' : ''}${pnlD.toFixed(2)})`);
                    }
                    this.recentResolutions.push(resolvedUp);
                    if (this.recentResolutions.length > 3) this.recentResolutions.shift();
                    console.log(`[Engine] ✅ Deferred BTC resolution: ${prevSlug} → ${resolvedUp ? '🟢 UP' : '🔴 DOWN'}`);
                }
                this.pendingResolution = null;
            }

            // ── Resolve any deferred ETH window if Chainlink is now fresh ───
            if (this.pendingEthResolution && chainlinkFresh) {
                const { prevEthSlug, prevEthK } = this.pendingEthResolution;
                const finalEthCL = rtdsState.ethChainlinkPrice;
                if (finalEthCL) {
                    const resolvedUp = finalEthCL >= prevEthK ? 1 : 0;
                    resolveWindow.run(resolvedUp, finalEthCL, prevEthSlug);
                    resolvePaperTrades.run(resolvedUp, resolvedUp, resolvedUp, resolvedUp, resolvedUp, prevEthSlug);
                    const rt = db.prepare('SELECT SUM(pnl) as total_pnl FROM paper_trades WHERE window_slug = ?').get(prevEthSlug);
                    const pnlD = Number(rt?.total_pnl) || 0;
                    const oldBal = this.paperBalance;
                    this.paperBalance = Math.max(0, this.paperBalance + pnlD);
                    if (Math.abs(this.paperBalance - oldBal) > 0.01) {
                        console.log(`[Balance] ${oldBal.toFixed(2)} → ${this.paperBalance.toFixed(2)} (deferred ETH ${pnlD >= 0 ? '+' : ''}${pnlD.toFixed(2)})`);
                    }
                    this.ethRecentResolutions.push(resolvedUp);
                    if (this.ethRecentResolutions.length > 3) this.ethRecentResolutions.shift();
                    console.log(`[Engine] ✅ Deferred ETH resolution: ${prevEthSlug} → ${resolvedUp ? '🟢 UP' : '🔴 DOWN'}`);
                }
                this.pendingEthResolution = null;
            }

            // Fetch BTC + ETH in parallel (throttled independently to 2s each)
            const now = Date.now();
            const btcStale = (now - this.lastPolyFetch) >= this.polyFetchTTL;
            const ethStale = (now - this.lastEthFetch)  >= this.polyFetchTTL;

            if (btcStale || ethStale) {
                // Promise.all fires both simultaneously — zero extra latency
                const [btcData, ethData] = await Promise.all([
                    btcStale ? this.polymarket.fetchMarketPrice()    : Promise.resolve(this.lastPolyData),
                    ethStale ? this.polymarketEth.fetchMarketPrice().catch(() => null) : Promise.resolve(this.lastEthData),
                ]);
                if (btcStale) { this.lastPolyData = btcData; this.lastPolyFetch = now; }
                if (ethStale) { this.lastEthData  = ethData; this.lastEthFetch  = now; }
            }

            const polyData = this.lastPolyData;
            if (!polyData) {
                console.log(`[Engine] Skipped tick ${this.tickCount}: no Polymarket data`);
                return;
            }

            // ── Liquidity vacuum detection ─────────────────────────────────
            const hasLiveOrderbook = polyData.spread !== null && polyData.spread !== undefined;
            if (!hasLiveOrderbook) {
                // Orderbook is empty — entering or continuing a vacuum
                if (!this.inVacuum) {
                    this.inVacuum         = true;
                    this.lastVacuumTime   = Date.now();
                    this.vacuumWindowSlug = polyData.slug;
                    console.log(`[Engine] 🕳  Liquidity vacuum started — ${polyData.slug} ${polyData.secondsRemaining}s left`);
                }
            } else if (this.inVacuum) {
                // Orderbook refilled — vacuum ended
                const vacuumDurationMs = Date.now() - this.lastVacuumTime;
                this.inVacuum = false;
                console.log(`[Engine] 💧 Liquidity restored after ${(vacuumDurationMs/1000).toFixed(1)}s — ${polyData.slug}`);
            }

            // A signal is "post-vacuum" if the orderbook emptied in this window
            // within the last 2 minutes — even if it has since refilled
            const isPostVacuum = (
                this.vacuumWindowSlug === polyData.slug &&
                (Date.now() - this.lastVacuumTime) < this.postVacuumWindowMs
            );

            const windowTs  = polyData.windowTs;
            const secsLeft  = polyData.secondsRemaining;

            // ── Window rollover detection ──────────────────────────────────
            if (this.lastWindowTs && this.lastWindowTs !== windowTs) {
                // Update IMMEDIATELY before await so rapid RTDS updates
                // don't trigger this block multiple times for same window
                // ── Resolve previous window ───────────────────────────────
                const prevSlug    = this.lastWindowTs ? `btc-updown-5m-${this.lastWindowTs}` : null;
                const prevK       = this.bayesian.openingPrice;
                const finalCL     = rtdsState.chainlinkPrice;

                if (prevSlug && prevK && finalCL) {
                    if (!chainlinkFresh) {
                        console.warn(`[Engine] ⚠️ Resolution deferred — Chainlink age ${rtdsState.chainlinkAge}ms, waiting for fresh tick`);
                        this.pendingResolution = { prevSlug, prevK };
                        this.pendingRolloverTg = { slug: polyData.slug, btcOutcome: '⏳ deferred', btcPnl: 0, btcK: null, btcFinal: null };
                    } else {
                        const resolvedUp = finalCL >= prevK ? 1 : 0;
                        resolveWindow.run(resolvedUp, finalCL, prevSlug);
                        resolvePaperTrades.run(resolvedUp, resolvedUp, resolvedUp, resolvedUp, resolvedUp, prevSlug);

                        // Update paper balance from resolved trades
                        const resolvedTrades = db.prepare(
                            `SELECT SUM(pnl) as total_pnl FROM paper_trades WHERE window_slug = ?`
                        ).get(prevSlug);
                        const pnlDelta = Number(resolvedTrades?.total_pnl) || 0;
                        const oldBalance = this.paperBalance;
                        this.paperBalance = Math.max(0, this.paperBalance + pnlDelta);
                        if (Math.abs(this.paperBalance - oldBalance) > 0.01) {
                            console.log(`[Balance] ${oldBalance.toFixed(2)} → ${this.paperBalance.toFixed(2)} (${pnlDelta >= 0 ? '+' : ''}${pnlDelta.toFixed(2)})`);
                        }

                        const outcome = resolvedUp ? '🟢 UP' : '🔴 DOWN';

                        const btcTradeCounts = db.prepare(
                            `SELECT SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                                    SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses
                             FROM paper_trades WHERE window_slug = ?`
                        ).get(prevSlug);

                        // Carry BTC outcome to ETH rollover block for combined message
                        this.pendingRolloverTg = {
                            slug: polyData.slug, btcOutcome: outcome,
                            btcPnl: pnlDelta, btcK: prevK, btcFinal: finalCL,
                            btcWins: btcTradeCounts?.wins || 0, btcLosses: btcTradeCounts?.losses || 0
                        };

                        // Update trend filter history
                        this.recentResolutions.push(resolvedUp);
                        if (this.recentResolutions.length > 3) this.recentResolutions.shift();
                        const trendStr = this.recentResolutions.map(r => r ? '↑' : '↓').join('');
                        console.log(
                            `\n[Engine] 📋 ${prevSlug} resolved ${outcome} ` +
                            `(K=$${prevK?.toFixed(2)} final=$${finalCL?.toFixed(2)}) ` +
                            `Trend: ${trendStr} Balance: $${this.paperBalance.toFixed(2)}`
                        );
                    }
                }

                this.lastWindowTs = windowTs;
                this.lastRolloverTime = Date.now();
                this.btcWindowTradeCount = 0;
                console.log(`\n[Engine] 🔄 Window rolled: ${polyData.slug}`);
                this.bayesian.reset();
                this.spreadModel.reset();
                // Ensure pendingRolloverTg is set (no-prior-window / session start case)
                if (!this.pendingRolloverTg) {
                    this.pendingRolloverTg = { slug: polyData.slug, btcOutcome: null, btcPnl: 0, btcK: null, btcFinal: null };
                }
                // Combined rollover message sent after ETH rollover block below
            } else {
                this.lastWindowTs = windowTs;
            }

            // ── Pure arbitrage scanner ────────────────────────────────────
            // If priceUp + priceDown < 0.98, buying both sides guarantees
            // profit regardless of outcome. No model needed — pure math.
            // Seen in the reference trader's ETH data (Mar 14).
            if (polyData.spread !== null && polyData.priceUp && polyData.priceDown) {
                const sumPrices = polyData.priceUp + polyData.priceDown;
                const arbGap    = 1.0 - sumPrices;
                if (arbGap > 0.02) {  // > 2¢ gap after fees
                    const gapCents = (arbGap * 100).toFixed(1);
                    const msg = `🎰 PURE ARB DETECTED\n` +
                        `📍 ${polyData.slug}\n` +
                        `⏱ ${polyData.secondsRemaining}s remaining\n` +
                        `UP:   ${(polyData.priceUp*100).toFixed(1)}¢\n` +
                        `DOWN: ${(polyData.priceDown*100).toFixed(1)}¢\n` +
                        `Sum:  ${(sumPrices*100).toFixed(1)}¢ (< 100¢)\n` +
                        `Gap:  ${gapCents}¢/share — guaranteed profit\n\n` +
                        `🟡 PAPER MODE — no order placed`;
                    console.log(`[Engine] 🎰 PURE ARB: ${polyData.slug} | ` +
                        `UP=${(polyData.priceUp*100).toFixed(1)}¢ + DOWN=${(polyData.priceDown*100).toFixed(1)}¢ ` +
                        `= ${(sumPrices*100).toFixed(1)}¢ | Gap=${gapCents}¢`);
                    await sendTelegram(msg);
                }
            }

            // ── ETH market processing ────────────────────────────────────────
            // Independent from BTC — runs in same tick, no extra latency
            const ethData = this.lastEthData;
            if (ethData && ethData.priceUp && ethData.priceDown) {

                // ETH pure arb scanner
                const ethSumPrices = ethData.priceUp + ethData.priceDown;
                const ethArbGap    = 1.0 - ethSumPrices;
                if (ethArbGap > 0.02) {
                    const gapCents = (ethArbGap * 100).toFixed(1);
                    console.log(
                        `[Engine] 🎰 ETH PURE ARB: ${ethData.slug} | ` +
                        `UP=${(ethData.priceUp*100).toFixed(1)}¢ + DOWN=${(ethData.priceDown*100).toFixed(1)}¢ ` +
                        `= ${(ethSumPrices*100).toFixed(1)}¢ | Gap=${gapCents}¢`
                    );
                    await sendTelegram(
                        `🎰 <b>ETH PURE ARB DETECTED</b>\n` +
                        `📍 ${ethData.slug}\n` +
                        `⏱ ${ethData.secondsRemaining}s remaining\n` +
                        `UP:   ${(ethData.priceUp*100).toFixed(1)}¢\n` +
                        `DOWN: ${(ethData.priceDown*100).toFixed(1)}¢\n` +
                        `Sum:  ${(ethSumPrices*100).toFixed(1)}¢ (< 100¢)\n` +
                        `Gap:  ${gapCents}¢/share — guaranteed profit if both sides filled\n\n` +
                        `🟡 PAPER MODE — no order placed`
                    );
                }

                // ETH directional signal — same Bayesian model, ETH reference price
                // For now: log the ETH market state so we can analyze it later
                // Full Bayesian integration coming in Phase 3
                if (this.tickCount % 60 === 0) {  // log every ~2 minutes
                    console.log(
                        `[ETH] ${ethData.slug} | ` +
                        `UP=${(ethData.priceUp*100).toFixed(1)}¢ ` +
                        `DOWN=${(ethData.priceDown*100).toFixed(1)}¢ ` +
                        `Spd=${ethData.spread ? (ethData.spread*100).toFixed(1)+'%' : 'N/A'} ` +
                        `${ethData.secondsRemaining}s`
                    );
                }

                // ── ETH full signal pipeline ──────────────────────────────
                // Same Bayesian + Edge model as BTC, using ETH Chainlink as K
                const ethCL = rtdsState.ethChainlinkPrice;
                if (!ethCL) {
                    console.log(`[Engine] Skipping tick ${this.tickCount}: no ETH Chainlink price`);
                } else if (ethData.windowTs) {

                    // ETH window rollover + resolution
                    if (this.ethWindowTs && this.ethWindowTs !== ethData.windowTs) {
                        const prevEthWindowTs = this.ethWindowTs;
                        this.ethWindowTs = ethData.windowTs; // close re-entry window before any await
                        this.ethWindowTradeCount = 0;
                        const prevEthSlug = `eth-updown-5m-${prevEthWindowTs}`;
                        const prevEthK    = this.ethBayesian.openingPrice;
                        const finalEthCL  = rtdsState.ethChainlinkPrice;

                        if (prevEthK && finalEthCL) {
                            if (!chainlinkFresh) {
                                console.warn(`[Engine] ⚠️ ETH resolution deferred — Chainlink age ${rtdsState.chainlinkAge}ms`);
                                this.pendingEthResolution = { prevEthSlug, prevEthK };
                            } else {
                            const resolvedUp = finalEthCL >= prevEthK ? 1 : 0;
                            resolveWindow.run(resolvedUp, finalEthCL, prevEthSlug);
                            resolvePaperTrades.run(resolvedUp, resolvedUp, resolvedUp, resolvedUp, resolvedUp, prevEthSlug);

                            const ethResolved = db.prepare(
                                `SELECT SUM(pnl) as total_pnl FROM paper_trades WHERE window_slug = ?`
                            ).get(prevEthSlug);
                            const pnlDelta = Number(ethResolved?.total_pnl) || 0;
                            const oldBalance = this.paperBalance;
                            this.paperBalance = Math.max(0, this.paperBalance + pnlDelta);
                            if (Math.abs(this.paperBalance - oldBalance) > 0.01) {
                                console.log(`[Balance] ${oldBalance.toFixed(2)} → ${this.paperBalance.toFixed(2)} (${pnlDelta >= 0 ? '+' : ''}${pnlDelta.toFixed(2)})`);
                            }

                            this.ethRecentResolutions.push(resolvedUp);
                            if (this.ethRecentResolutions.length > 3) this.ethRecentResolutions.shift();
                            const ethTrendStr = this.ethRecentResolutions.map(r => r ? '↑' : '↓').join('');
                            console.log(
                                `[ETH] 📋 ${prevEthSlug} resolved ` +
                                `${resolvedUp ? '🟢 UP' : '🔴 DOWN'} ` +
                                `(K=$${prevEthK.toFixed(2)} final=$${finalEthCL.toFixed(2)}) ` +
                                `Trend: ${ethTrendStr} Balance: $${this.paperBalance.toFixed(2)}`
                            );

                            // Augment pendingRolloverTg with ETH outcome
                            if (this.pendingRolloverTg) {
                                const ethTradeCounts = db.prepare(
                                    `SELECT SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                                            SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses
                                     FROM paper_trades WHERE window_slug = ?`
                                ).get(prevEthSlug);
                                this.pendingRolloverTg.ethOutcome = resolvedUp ? '🟢 UP' : '🔴 DOWN';
                                this.pendingRolloverTg.ethPnl     = pnlDelta;
                                this.pendingRolloverTg.ethK       = prevEthK;
                                this.pendingRolloverTg.ethFinal   = finalEthCL;
                                this.pendingRolloverTg.ethWins    = ethTradeCounts?.wins || 0;
                                this.pendingRolloverTg.ethLosses  = ethTradeCounts?.losses || 0;
                            }
                            } // end else (chainlinkFresh)
                        }

                        this.ethBayesian.reset();
                        this.ethSpreadModel.reset();
                        console.log(`[ETH] 🔄 Window rolled: eth-updown-5m-${ethData.windowTs}`);

                        // Send combined BTC + ETH rollover message
                        // Capture + clear synchronously before await to prevent duplicate sends
                        const _pendingCombined = this.pendingRolloverTg;
                        this.pendingRolloverTg = null;
                        if (_pendingCombined) {
                            const p = _pendingCombined;
                            const btcLine = p.btcOutcome
                                ? `₿ BTC ${p.btcOutcome}  K=$${p.btcK?.toFixed(2) ?? '?'} → $${p.btcFinal?.toFixed(2) ?? '?'}  P&L: ${p.btcPnl >= 0 ? '+' : ''}$${p.btcPnl.toFixed(2)}`
                                : `₿ BTC (session start)`;
                            const ethLine = p.ethOutcome
                                ? `Ξ ETH ${p.ethOutcome}  K=$${p.ethK?.toFixed(2) ?? '?'} → $${p.ethFinal?.toFixed(2) ?? '?'}  P&L: ${p.ethPnl >= 0 ? '+' : ''}$${p.ethPnl.toFixed(2)}`
                                : `Ξ ETH (session start)`;
                            const totalWins   = (p.btcWins || 0) + (p.ethWins || 0);
                            const totalLosses = (p.btcLosses || 0) + (p.ethLosses || 0);
                            const tradeBadge  = totalWins === 0 && totalLosses === 0 ? ''
                                : totalWins > totalLosses  ? (totalWins  === 1 ? ' (WIN)'              : ` (WIN X${totalWins})`)
                                : totalLosses > totalWins  ? (totalLosses === 1 ? ' (LOSS)'             : ` (LOSS X${totalLosses})`)
                                : ` (WIN X${totalWins} / LOSS X${totalLosses})`;
                            const sessDelta = this.paperBalance - this.sessionStartBalance;
                            await sendTelegram(
                                `💰 <b>$${this.paperBalance.toFixed(2)}  ${sessDelta >= 0 ? '+$' : '-$'}${Math.abs(sessDelta).toFixed(2)} session${tradeBadge}</b>\n` +
                                `${btcLine}\n` +
                                `${ethLine}\n\n` +
                                `₿ CL=$${rtdsState.chainlinkPrice?.toFixed(2) ?? '?'}  BN=$${rtdsState.binancePrice?.toFixed(2) ?? '?'}\n` +
                                `Ξ CL=$${rtdsState.ethChainlinkPrice?.toFixed(2) ?? '?'}  BN=$${rtdsState.ethBinancePrice?.toFixed(2) ?? '?'}\n` +
                                `🔄 ${p.slug}`
                            );
                        }
                    }
                    this.ethWindowTs = ethData.windowTs;

                    // ETH Bayesian estimate
                    const ethBayes = this.ethBayesian.estimate(
                        ethCL,
                        ethData.secondsRemaining,
                        rtdsState.ethBinanceVelocity ?? 0,  // $/sec from RTDS history
                        ethData.windowTs,
                        rtdsState.ethChainlinkAge
                    );
                    if (!ethBayes) {
                        console.log(`[Engine] ETH tick skipped: stale K (age ${rtdsState.ethChainlinkAge}ms)`);
                        // Skip ONLY ETH logic — continue with BTC processing
                    } else {

                    // ETH Edge evaluation — same evaluate() interface as BTC
                    const ethEdge = this.edge.evaluate(ethBayes, ethData);

                    // ── Model 3: ETH Spread Compression (observe-only, no gating yet) ──
                    const ethSlug = `eth-updown-5m-${ethData.windowTs}`;
                    const ethSpreadResult = this.ethSpreadModel.evaluate(ethData.spread, ethData.windowTs);

                    // ── ETH Orderbook imbalance (data collection only) ──
                    const ethImbalanceRatio = (ethData.bidDepth && ethData.askDepth && ethData.askDepth > 0)
                        ? ethData.bidDepth / ethData.askDepth : null;
                    const ethImbalanceDelta = (ethData.bidDepth && ethData.askDepth)
                        ? (ethData.bidDepth - ethData.askDepth) / (ethData.bidDepth + ethData.askDepth) : null;
                    const ethImbalanceBucket = ethImbalanceRatio === null ? null
                        : ethImbalanceRatio >= 2.0  ? 'bid_very_thin'
                        : ethImbalanceRatio >= 1.3  ? 'bid_thin'
                        : ethImbalanceRatio <= 0.5  ? 'ask_very_thin'
                        : ethImbalanceRatio <= 0.77 ? 'ask_thin'
                        : 'balanced';
                    const ethWouldTradeDown = ethImbalanceRatio !== null && ethImbalanceRatio >= 2.0 ? 1 : 0;
                    const ethWouldTradeUp   = ethImbalanceRatio !== null && ethImbalanceRatio <= 0.5 ? 1 : 0;

                    // ── Log ETH tick to database (mirrors BTC signal/window logging) ──
                    const ethFailedFilters = ethEdge.filters
                        .filter(f => !f.pass).map(f => f.name).join(',');
                    const ethNearThreshold = (
                        !ethEdge.tradeable &&
                        ethEdge.netEdge >= (this.edge.minNetEdge - 0.03) &&
                        ethEdge.netEdge < this.edge.minNetEdge
                    ) ? 1 : 0;
                    const ethPostVacuum = (
                        this.vacuumWindowSlug === ethSlug &&
                        (Date.now() - this.lastVacuumTime) < this.postVacuumWindowMs
                    );

                    // ── ETH suppression filter booleans (computed here for DB logging) ─
                    const ethNow        = Date.now();
                    const ethSameWin    = this.lastEthSignalWindow === ethSlug;
                    const ethEffCooldown = ethEdge.netEdge >= 0.40 ? 3000 : this.ethWindowCooldownMs; // Tier A: 3s
                    const ethWinLocked  = ethSameWin && (ethNow - this.lastEthSignalTime) < ethEffCooldown;
                    const ethWarmedUp   = ethBayes.observations >= this.minObservationsBeforeSignal;
                    const ethDistFromK  = ethBayes.openingPrice
                        ? (ethCL - ethBayes.openingPrice) / ethBayes.openingPrice
                        : 0;
                    const ethDistBlocked =
                        (ethEdge.direction === 'DOWN' && ethDistFromK >  0.0010) ||
                        (ethEdge.direction === 'UP'   && ethDistFromK < -0.0010);
                    let ethTrendBlocked = false;
                    let ethTrendPenalty = 0;
                    if (this.trendFilterEnabled && this.ethRecentResolutions.length >= 2) {
                        const ethLast2   = this.ethRecentResolutions.slice(-2);
                        const ethAllUp   = ethLast2.every(r => r === 1);
                        const ethAllDown = ethLast2.every(r => r === 0);
                        if (ethAllUp   && ethEdge.direction === 'DOWN') ethTrendPenalty = 0.05;
                        if (ethAllDown && ethEdge.direction === 'UP')   ethTrendPenalty = 0.05;
                        const ethEffectiveEdge = ethEdge.netEdge - ethTrendPenalty;
                        if (ethTrendPenalty > 0 && ethEffectiveEdge < this.edge.minNetEdge) {
                            ethTrendBlocked = true;
                        }
                    }
                    const ethClAgeBlocked = !!(rtdsState.ethChainlinkAge && rtdsState.ethChainlinkAge > 300000);
                    const ethSuppressionFlags = [
                        !ethWarmedUp    ? 'warmup'      : null,
                        ethWinLocked    ? 'win_lock'    : null,
                        ethDistBlocked  ? 'dist_from_k' : null,
                        ethTrendBlocked ? 'trend'       : null,
                        ethClAgeBlocked ? 'cl_stale'    : null,
                    ].filter(Boolean).join(',') || 'none';

                    const ethSignalRow = insertSignal.run(
                        new Date().toISOString(),
                        ethSlug,
                        ethData.secondsRemaining,
                        rtdsState.ethBinancePrice,
                        ethCL,
                        rtdsState.ethGapPct,
                        rtdsState.ethLagDetected ? 1 : 0,    // ETH lag detected (from ethGap vs threshold)
                        rtdsState.ethLagDirection ?? null,   // ETH lag direction ('UP'|'DOWN'|null)
                        rtdsState.ethBinanceVelocity ?? 0,
                        rtdsState.ethChainlinkAge,
                        ethData.priceUp, ethData.priceDown,
                        ethData.spread, ethData.bidDepth, ethData.askDepth, ethData.liquidity,
                        ethBayes.openingPrice, ethBayes.fairProbUp,
                        ethBayes.distanceFromStrike, ethBayes.volatility,
                        ethBayes.drift, ethBayes.observations,
                        ethEdge.rawEdge, ethEdge.netEdge, ethEdge.costs.total,
                        ethEdge.direction, ethEdge.tradeable ? 1 : 0,
                        ethFailedFilters,
                        0,    // alert_sent (updated by rowid after Telegram send)
                        ethPostVacuum ? 1 : 0,
                        ethPostVacuum ? ((Date.now() - this.lastVacuumTime) / 1000) : null,
                        ethSuppressionFlags,
                        ethImbalanceRatio, ethImbalanceDelta, ethImbalanceBucket,
                        ethWouldTradeDown, ethWouldTradeUp, ethNearThreshold,
                        ethEdge.rawEdge * 100,
                        (ethEdge.netEdge - (ethTrendPenalty || 0)) * 100,
                        ethTrendPenalty || 0,
                        ethTrendBlocked ? 1 : 0,
                        ethData.bestAsk ?? null,
                        ethData.downBestAsk ?? null,
                        (ethData.upRawMid != null && ethData.downRawMid != null)
                            ? ethData.upRawMid + ethData.downRawMid : null,
                        ethData.downSpread ?? null
                    );

                    upsertWindow.run(
                        ethSlug,
                        ethData.windowTs,
                        ethCL,
                        rtdsState.ethBinancePrice,
                        ethEdge.tradeable ? 1 : 0,
                        ethEdge.netEdge,
                        rtdsState.ethLagDetected ? 1 : 0  // ETH lag signals this window
                    );

                    if (ethEdge.tradeable) {
                        const ethSuppressed = !ethWarmedUp || ethWinLocked || ethDistBlocked || ethTrendBlocked || ethClAgeBlocked;

                        if (!ethSuppressed) {
                            // Race condition guard: re-check cooldown + claim slot synchronously
                            const ethNowGuard = Date.now();
                            const ethRaceBlocked = this.lastEthSignalWindow === ethSlug &&
                                (ethNowGuard - this.lastEthSignalTime) < ethEffCooldown;
                            const ethCapBlocked = this.ethWindowTradeCount >= 7;
                            if (ethRaceBlocked || ethCapBlocked) {
                                if (ethCapBlocked) console.log(`[Engine] ETH trade skipped: window cap (7)`);
                            } else {
                            // ETH Kelly pre-check: verify positive EV before claiming slot
                            const ethFpWin = ethEdge.direction === 'UP' ? ethBayes.fairProbUp : ethBayes.fairProbDown;
                            const ethEp = ethEdge.direction === 'UP' ? (ethData.bestAsk ?? ethData.priceUp) : (ethData.priceDown ?? (1 - ethData.priceUp));
                            const ethKellyB = ethEp > 0 && ethEp < 1 ? (1 - ethEp) / ethEp : 0;
                            const ethKellyPrecheck = ethKellyB * ethFpWin - (1 - ethFpWin);
                            if (ethKellyPrecheck <= 0) {
                                console.warn(`[Paper] ETH Kelly precheck fail — entryPrice:${ethEp.toFixed(4)} fairProbWin:${ethFpWin.toFixed(4)} (likely empty book)`);
                            } else {
                            this.lastEthSignalTime   = ethNowGuard; // claim slot before any await
                            this.lastEthSignalWindow = ethSlug;
                            this.ethWindowTradeCount++;
                            // Tier caps: A=40%+(1%), B=30%(0.5%), C=20%(0.25%), D=10%(0.1%)
                            const ethTier = ethEdge.netEdge >= 0.40 ? 'A'
                                : ethEdge.netEdge >= 0.30 ? 'B'
                                : ethEdge.netEdge >= 0.20 ? 'C'
                                : ethEdge.netEdge >= 0.10 ? 'D' : null;
                            if (ethTier) {
                            const ethMaxStake = { A: 0.020, B: 0.005, C: 0.0025, D: 0.001 }[ethTier];
                            const ethStake = new KellySizer({ maxStakePct: ethMaxStake, minStakePct: 0 })
                                .size(
                                    ethEdge.direction === 'UP' ? ethBayes.fairProbUp : ethBayes.fairProbDown,
                                    ethEdge.direction === 'UP' ? (ethData.bestAsk ?? ethData.priceUp) : (ethData.priceDown ?? (1 - ethData.priceUp)),
                                    this.paperBalance
                                );

                            if (ethStake.status === 'ok' && ethStake.stakeAmount > 0) {
                                // net_edge_pct stored BEFORE trend penalty (raw edge at decision time)
                                const ethUpCtx = getUpContext();
                                insertPaperTrade.run(
                                    new Date().toISOString(),
                                    ethSlug,
                                    ethEdge.direction,
                                    ethEdge.direction === 'UP' ? ethData.priceUp : ethData.priceDown,
                                    ethStake.stakeAmount,
                                    ethStake.shares,
                                    ethStake.kellyFull,
                                    ethEdge.netEdge * 100,
                                    this.paperBalance,
                                    ethTier,
                                    ethUpCtx.had60s,
                                    ethUpCtx.had300s,
                                    ethUpCtx.maxEdge,
                                    ethUpCtx.secsAgo,
                                    ethData.secondsRemaining
                                );

                                console.log(
                                    `[ETH Paper] 💰 ${ethEdge.direction} @${((ethEdge.direction==='UP'?ethData.priceUp:ethData.priceDown)*100).toFixed(1)}¢ ` +
                                    `$${ethStake.stakeAmount.toFixed(2)} edge:${(ethEdge.netEdge*100).toFixed(1)}% ` +
                                    `dist:${(ethDistFromK*100).toFixed(3)}% obs:${ethBayes.observations}`
                                );

                                this.ethSignalCount++;
                                this.sessionEthTrades++;
                                await sendTelegram(
                                    `Ξ <b>[${ethTier}] ${ethEdge.direction} | ${ethData.secondsRemaining}s | edge: ${(ethEdge.netEdge*100).toFixed(1)}%</b>\n` +
                                    `💰 $${ethStake.stakeAmount.toFixed(2)} stake  Balance: $${this.paperBalance.toFixed(2)}\n\n` +
                                    `Fair: ${((ethEdge.direction==='UP'?ethBayes.fairProbUp:ethBayes.fairProbDown)*100).toFixed(1)}%  Market: ${((ethEdge.direction==='UP'?ethData.priceUp:ethData.priceDown)*100).toFixed(1)}%  Obs: ${ethBayes.observations}\n` +
                                    `⛓ CL: $${ethCL.toFixed(2)}\n` +
                                    `Spread: ${ethData.spread ? (ethData.spread*100).toFixed(1)+'%' : 'N/A'}  Compression: ${ethSpreadResult.compressionConfirmed ? '✅' : '⏸'} Q${ethSpreadResult.qualityScore}/5\n` +
                                    `Dist from K: ${ethDistFromK >= 0 ? '+' : ''}${(ethDistFromK*100).toFixed(3)}%\n\n` +
                                    `📍 ${ethSlug}  #${this.sessionEthTrades}/${this.ethSignalCount}\n` +
                                    `🟡 PAPER TRADE`
                                );

                                db.prepare('UPDATE signals SET alert_sent = 1 WHERE id = ?').run(ethSignalRow.lastInsertRowid);
                            } else {
                                const ethFairProbWin = ethEdge.direction === 'UP' ? ethBayes.fairProbUp : ethBayes.fairProbDown;
                                const ethEntryPrice  = ethEdge.direction === 'UP' ? (ethData.bestAsk ?? ethData.priceUp) : (ethData.priceDown ?? (1 - ethData.priceUp));
                                console.warn(`[Paper] ⚠ ETH Kelly skip — status:${ethStake.status} stake:${ethStake.stakeAmount} fairProbWin:${ethFairProbWin} entryPrice:${ethEntryPrice} balance:${this.paperBalance}`);
                            }
                            } // edge >= 10%: trade and alert executed
                            // else: edge < 10%, skip trade but continue to BTC processing
                            } // ethKellyPrecheck > 0
                            } // end race/cap guard else
                        }
                    }
                    } // end else (ethBayes valid)
                }
            }

            // Fallback: ETH didn't roll this tick — send BTC-only rollover message
            // Capture + clear synchronously before await to prevent duplicate sends
            const _pendingFallback = this.pendingRolloverTg;
            this.pendingRolloverTg = null;
            if (_pendingFallback) {
                const p = _pendingFallback;
                const btcLine = p.btcOutcome
                    ? `₿ BTC ${p.btcOutcome}  K=$${p.btcK?.toFixed(2) ?? '?'} → $${p.btcFinal?.toFixed(2) ?? '?'}  P&L: ${p.btcPnl >= 0 ? '+' : ''}$${p.btcPnl.toFixed(2)}`
                    : `₿ BTC (session start)`;
                const fbWins   = p.btcWins || 0;
                const fbLosses = p.btcLosses || 0;
                const fbBadge  = fbWins === 0 && fbLosses === 0 ? ''
                    : fbWins > fbLosses  ? (fbWins   === 1 ? ' (WIN)'  : ` (WIN X${fbWins})`)
                    : fbLosses > fbWins  ? (fbLosses  === 1 ? ' (LOSS)' : ` (LOSS X${fbLosses})`)
                    : ` (WIN X${fbWins} / LOSS X${fbLosses})`;
                const sessDelta = this.paperBalance - this.sessionStartBalance;
                await sendTelegram(
                    `💰 <b>$${this.paperBalance.toFixed(2)}  ${sessDelta >= 0 ? '+$' : '-$'}${Math.abs(sessDelta).toFixed(2)} session${fbBadge}</b>\n` +
                    `${btcLine}\n\n` +
                    `₿ CL=$${rtdsState.chainlinkPrice?.toFixed(2) ?? '?'}  BN=$${rtdsState.binancePrice?.toFixed(2) ?? '?'}\n` +
                    `🔄 ${p.slug}`
                );
            }

            // ── Bayesian estimate (uses Chainlink as primary price) ────────
            // K is set from Chainlink price — the actual resolution reference
            const chainlinkPrice = rtdsState.chainlinkPrice;
            const binancePrice   = rtdsState.binancePrice;

            if (!chainlinkPrice) {
                console.log(`[Engine] Skipping tick ${this.tickCount}: no BTC Chainlink price`);
                return;
            }

            const bayesResult = this.bayesian.estimate(
                chainlinkPrice,           // S: use Chainlink as the modeled price
                secsLeft,
                rtdsState.binanceVelocity, // velocity from faster Binance feed
                windowTs,
                rtdsState.chainlinkAge
            );
            if (!bayesResult) {
                console.log(`[Engine] Skipped tick ${this.tickCount}: BTC stale K (age ${rtdsState.chainlinkAge}ms)`);
                return;
            }

            // ── Edge evaluation ────────────────────────────────────────────
            const edgeResult = this.edge.evaluate(bayesResult, polyData);

            // ── Orderbook imbalance (data collection only — no trading logic) ──
            const imbalanceRatio = (polyData.bidDepth && polyData.askDepth && polyData.askDepth > 0)
                ? polyData.bidDepth / polyData.askDepth
                : null;
            const imbalanceDelta = (polyData.bidDepth && polyData.askDepth)
                ? (polyData.bidDepth - polyData.askDepth) / (polyData.bidDepth + polyData.askDepth)
                : null;
            const imbalanceBucket = imbalanceRatio === null ? null
                : imbalanceRatio >= 2.0  ? 'bid_very_thin'
                : imbalanceRatio >= 1.3  ? 'bid_thin'
                : imbalanceRatio <= 0.5  ? 'ask_very_thin'
                : imbalanceRatio <= 0.77 ? 'ask_thin'
                : 'balanced';
            const wouldTradeDownImbalance = imbalanceRatio !== null && imbalanceRatio >= 2.0 ? 1 : 0;
            const wouldTradeUpImbalance   = imbalanceRatio !== null && imbalanceRatio <= 0.5 ? 1 : 0;

            // ── Model 3: BTC Spread Compression (observe-only, no gating yet) ──
            const spreadResult = this.spreadModel.evaluate(polyData.spread, windowTs);

            // ── Console output ─────────────────────────────────────────────
            // Only print every 5 ticks to avoid flooding (RTDS is fast)
            // Suppress for 2s after rollover — backlogged ticks flush all at once
            const msSinceRollover = this.lastRolloverTime ? Date.now() - this.lastRolloverTime : 9999;
            if (this.tickCount % 5 === 0 && msSinceRollover > 2000) {
                const lagStr = rtdsState.lagDetected
                    ? ` ⚡LAG:${rtdsState.binanceGapPct >= 0 ? '+' : ''}${rtdsState.binanceGapPct.toFixed(2)}%→${rtdsState.lagDirection}`
                    : '';
                const tradeStr = edgeResult.tradeable ? ' 🎯 SIGNAL' : '';
                const comprStr = spreadResult.compressionConfirmed ? ` 📉COMP(Q${spreadResult.qualityScore})` : '';

                console.log(
                    `[${new Date().toLocaleTimeString()}] ` +
                    `CL=$${chainlinkPrice.toFixed(2)} ` +
                    `BN=$${binancePrice?.toFixed(2) ?? '?'} ` +
                    `K=$${bayesResult.openingPrice?.toFixed(2) ?? '?'} | ` +
                    `Poly=${(polyData.priceUp * 100).toFixed(1)}% ` +
                    `Fair=${(bayesResult.fairProbUp * 100).toFixed(1)}% ` +
                    `Edge=${(edgeResult.netEdge * 100).toFixed(2)}% ` +
                    `Spd=${polyData.spread ? (polyData.spread * 100).toFixed(1) + '%' : 'N/A'} ` +
                    `${secsLeft}s` +
                    lagStr + tradeStr + comprStr
                );
            }

            // ── Log to database ────────────────────────────────────────────
            const failedFilters = edgeResult.filters
                .filter(f => !f.pass).map(f => f.name).join(',');
            const nearThreshold = (
                !edgeResult.tradeable &&
                edgeResult.netEdge >= (this.edge.minNetEdge - 0.03) &&
                edgeResult.netEdge < this.edge.minNetEdge
            ) ? 1 : 0;

            // ── Suppression filter booleans (computed here for DB logging) ──
            const sigNow         = Date.now();
            const sameWindow     = this.lastSignalWindow === polyData.slug;
            const sameDir        = this.lastSignalDir === edgeResult.direction;
            const withinCooldown = (sigNow - this.lastSignalTime) < this.signalCooldownMs;
            const windowCooldownMs = edgeResult?.netEdge >= 0.40 ? 3000 : 90000; // Tier A: 3s, others: 90s
            const windowLocked   = sameWindow && (sigNow - this.lastSignalTime) < windowCooldownMs;
            const warmedUp       = bayesResult.observations >= this.minObservationsBeforeSignal;
            let trendBlocked = false;
            let trendPenalty = 0;
            if (this.trendFilterEnabled && this.recentResolutions.length >= 2) {
                const last2   = this.recentResolutions.slice(-2);
                const allUp   = last2.every(r => r === 1);
                const allDown = last2.every(r => r === 0);
                if (allUp   && edgeResult.direction === 'DOWN') trendPenalty = 0.05;
                if (allDown && edgeResult.direction === 'UP')   trendPenalty = 0.05;
                const effectiveEdge = edgeResult.netEdge - trendPenalty;
                if (trendPenalty > 0 && effectiveEdge < this.edge.minNetEdge) {
                    trendBlocked = true;
                }
            }
            const bnGap      = rtdsState.binanceGapPct ?? 0;
            const gapBlocked = (
                (edgeResult.direction === 'DOWN' && bnGap > 0.05)  ||
                (edgeResult.direction === 'UP'   && bnGap < -0.05)
            );
            const distFromK  = bayesResult.openingPrice
                ? (chainlinkPrice - bayesResult.openingPrice) / bayesResult.openingPrice
                : 0;
            const distBlocked = (
                (edgeResult.direction === 'DOWN' && distFromK > 0.0005) ||
                (edgeResult.direction === 'UP'   && distFromK < -0.0005)
            );
            const clAgeBlocked = !!(rtdsState.chainlinkAge && rtdsState.chainlinkAge > 300000);
            const suppressionFlags = [
                !warmedUp    ? 'warmup'      : null,
                windowLocked ? 'win_lock'    : null,
                trendBlocked ? 'trend'       : null,
                gapBlocked   ? 'gap'         : null,
                distBlocked  ? 'dist_from_k' : null,
                clAgeBlocked ? 'cl_stale'    : null
            ].filter(Boolean).join(',') || 'none';

            const btcSignalRow = insertSignal.run(
                new Date().toISOString(), polyData.slug, secsLeft,
                binancePrice, chainlinkPrice,
                rtdsState.binanceGapPct, rtdsState.lagDetected ? 1 : 0,
                rtdsState.lagDirection, rtdsState.binanceVelocity,
                rtdsState.chainlinkAge,
                polyData.priceUp, polyData.priceDown,
                polyData.spread, polyData.bidDepth, polyData.askDepth, polyData.liquidity,
                bayesResult.openingPrice, bayesResult.fairProbUp,
                bayesResult.distanceFromStrike, bayesResult.volatility,
                bayesResult.drift, bayesResult.observations,
                edgeResult.rawEdge, edgeResult.netEdge, edgeResult.costs.total,
                edgeResult.direction, edgeResult.tradeable ? 1 : 0,
                failedFilters,
                0,  // alert_sent (updated by rowid after Telegram send)
                isPostVacuum ? 1 : 0,
                isPostVacuum ? ((Date.now() - this.lastVacuumTime) / 1000) : null,
                suppressionFlags,
                imbalanceRatio, imbalanceDelta, imbalanceBucket,
                wouldTradeDownImbalance, wouldTradeUpImbalance, nearThreshold,
                edgeResult.netEdge * 100,
                (edgeResult.netEdge - trendPenalty) * 100,
                trendPenalty,
                trendBlocked ? 1 : 0,
                polyData.bestAsk ?? null,
                polyData.downBestAsk ?? null,
                (polyData.upRawMid != null && polyData.downRawMid != null)
                    ? polyData.upRawMid + polyData.downRawMid : null,
                polyData.downSpread ?? null
            );

            upsertWindow.run(
                polyData.slug, windowTs,
                chainlinkPrice, binancePrice,
                edgeResult.tradeable ? 1 : 0,
                edgeResult.netEdge,
                rtdsState.lagDetected ? 1 : 0
            );

            // ── Log every UP signal with sufficient edge for diagnostics ────────
            // Captures UP signals regardless of tradeable flag, because tradeable UP
            // signals have never occurred — the edge filter itself is the gating factor.
            // suppression_reason = 'no_edge' when netEdge < minNetEdge (the common case).
            if (edgeResult.direction === 'UP' && secsLeft >= 15) {
                const upReason = !warmedUp    ? 'warmup'
                    : windowLocked ? 'win_lock'
                    : trendBlocked ? 'trend'
                    : gapBlocked   ? 'gap'
                    : distBlocked  ? 'dist_from_k'
                    : clAgeBlocked ? 'cl_stale'
                    : !edgeResult.tradeable ? 'no_edge'
                    : 'none';
                if (upReason !== 'none') {
                    insertUpDiag.run(
                        new Date().toISOString(), polyData.slug, secsLeft,
                        bayesResult.fairProbUp, polyData.priceUp,
                        edgeResult.netEdge, upReason, distFromK, bnGap
                    );
                }
            }

            // ── Fire signal alert (with cooldown + warmup gate) ─────────────
            if (edgeResult.tradeable) {
                // Suppress if: not warmed up, cooldown active, trend blocked, or gap blocked
                const isTierA   = edgeResult.netEdge >= 0.40;
                const suppressed = !warmedUp ||
                    windowLocked ||                          // tier-aware per-window lockout (3s Tier A, 90s others)
                    (!isTierA && sameWindow && sameDir && withinCooldown) || // 30s cooldown — Tier A bypasses
                    trendBlocked ||
                    gapBlocked ||
                    distBlocked ||                           // CL too far from K
                    clAgeBlocked;                            // CL stale mid-window (>5 min)

                // Log suppression reason every 5th suppressed signal (avoid spam)
                if (suppressed && this.tickCount % 20 === 0) {
                    const reason = !warmedUp    ? `warmup(${bayesResult.observations}/${this.minObservationsBeforeSignal})`
                        : windowLocked ? `window-locked(${Math.round((windowCooldownMs-(sigNow-this.lastSignalTime))/1000)}s left)`
                        : trendBlocked ? `trend(${this.recentResolutions.map(r=>r?'↑':'↓').join('')})`
                        : gapBlocked   ? `gap(BN${bnGap >= 0?'+':''}${bnGap.toFixed(2)}%)`
                        : distBlocked  ? `dist-from-K(${(distFromK*100).toFixed(3)}%)`
                        : clAgeBlocked ? `cl-stale(${((rtdsState.chainlinkAge??0)/1000).toFixed(0)}s)`
                        : 'cooldown';
                    console.log(`[Engine] ⏸ Signal suppressed: ${reason} | ${edgeResult.direction} edge=${(edgeResult.netEdge*100).toFixed(1)}%`);
                }

                if (!suppressed) {
                    // Race condition guard: re-check cooldown + claim slot synchronously
                    const nowGuard = Date.now();
                    const raceBlocked = this.lastSignalWindow === polyData.slug &&
                        (nowGuard - this.lastSignalTime) < windowCooldownMs;
                    const capBlocked = this.btcWindowTradeCount >= 7;
                    if (raceBlocked || capBlocked) {
                        if (capBlocked) console.log(`[Engine] BTC trade skipped: window cap (7)`);
                    } else {
                    // ── Tiered Kelly position sizing ───────────────────
                    // Entry price = ask price for the direction we're buying
                    const fairProbWin  = edgeResult.direction === 'UP'
                        ? bayesResult.fairProbUp
                        : bayesResult.fairProbDown;
                    const entryPrice   = edgeResult.direction === 'UP'
                        ? (polyData.bestAsk ?? polyData.priceUp)
                        : (polyData.priceDown ?? (1 - polyData.priceUp));

                    // Kelly pre-check: verify positive EV before claiming the slot.
                    // Prevents locking the window when entry price is wrong
                    // (e.g., empty UP book giving a fake edge that Kelly correctly rejects).
                    const btcKellyB = entryPrice > 0 && entryPrice < 1 ? (1 - entryPrice) / entryPrice : 0;
                    const btcKellyPrecheck = btcKellyB * fairProbWin - (1 - fairProbWin);
                    if (btcKellyPrecheck <= 0) {
                        console.warn(`[Paper] BTC Kelly precheck fail — entryPrice:${entryPrice.toFixed(4)} fairProbWin:${fairProbWin.toFixed(4)} (likely empty book)`);
                    } else {

                    this.signalCount++;
                    if (rtdsState.lagDetected) this.lagSignalCount++;
                    this.btcWindowTradeCount++;

                    // Update cooldown state — set BEFORE any await
                    this.lastSignalTime   = nowGuard;
                    this.lastSignalDir    = edgeResult.direction;
                    this.lastSignalWindow = polyData.slug;

                    // Tier caps: A=40%+(1%), B=30%(0.5%), C=20%(0.25%), D=10%(0.1%)
                    const btcTier = edgeResult.netEdge >= 0.40 ? 'A'
                        : edgeResult.netEdge >= 0.30 ? 'B'
                        : edgeResult.netEdge >= 0.20 ? 'C'
                        : edgeResult.netEdge >= 0.10 ? 'D' : null;
                    if (btcTier) {
                    const btcMaxStake = { A: 0.020, B: 0.005, C: 0.0025, D: 0.001 }[btcTier];
                    const kellyResult = new KellySizer({ maxStakePct: btcMaxStake, minStakePct: 0 })
                        .size(fairProbWin, entryPrice, this.paperBalance);

                    // Record paper trade
                    if (kellyResult.status === 'ok' && kellyResult.stakeAmount > 0) {
                        // net_edge_pct stored BEFORE trend penalty (raw edge at decision time)
                        const btcUpCtx = getUpContext();
                        insertPaperTrade.run(
                            new Date().toISOString(),
                            polyData.slug,
                            edgeResult.direction,
                            entryPrice,
                            kellyResult.stakeAmount,
                            kellyResult.shares,
                            kellyResult.kellyFull,
                            edgeResult.netEdge * 100,
                            this.paperBalance,
                            btcTier,
                            btcUpCtx.had60s,
                            btcUpCtx.had300s,
                            btcUpCtx.maxEdge,
                            btcUpCtx.secsAgo,
                            secsLeft
                        );
                        this.paperTrades.push({
                            slug:      polyData.slug,
                            direction: edgeResult.direction,
                            stake:     kellyResult.stakeAmount,
                            shares:    kellyResult.shares,
                            entry:     entryPrice
                        });
                        console.log(
                            `[Paper] 💰 Trade logged: ${edgeResult.direction} ` +
                            `$${kellyResult.stakeAmount.toFixed(2)} ` +
                            `(${kellyResult.stakePercent}% of $${this.paperBalance.toFixed(2)}) ` +
                            `@ ${(entryPrice*100).toFixed(1)}¢ | Kelly: ${kellyResult.kellyFull.toFixed(1)}%`
                        );

                        const trendNote = trendBlocked ? '' : (
                            this.recentResolutions.length >= 2
                                ? `\n📈 Trend: ${this.recentResolutions.map(r=>r?'↑':'↓').join('')} (not blocked)`
                                : ''
                        );
                        const gapNote = Math.abs(bnGap) > 0.05
                            ? `\n⚡ BN gap: ${bnGap >= 0 ? '+' : ''}${bnGap.toFixed(3)}% (${bnGap > 0 ? 'UP' : 'DOWN'} momentum)`
                            : '';
                        const distNote = `\n📏 Dist from K: ${distFromK >= 0 ? '+' : ''}${(distFromK*100).toFixed(3)}%`;

                        const vacuumBadge = isPostVacuum
                            ? `\n🕳 <b>POST-VACUUM SIGNAL</b> — orderbook emptied ${((Date.now() - this.lastVacuumTime)/1000).toFixed(0)}s ago`
                            : '';

                        const lagBadge = rtdsState.lagDetected
                            ? `\n⚡ <b>LAG DETECTED:</b> Binance ${rtdsState.binanceGapPct >= 0 ? '+' : ''}${rtdsState.binanceGapPct.toFixed(3)}% ahead of Chainlink`
                            : '';

                        const warmupNote = bayesResult.observations < 20
                            ? `\n⚠️ Early signal (${bayesResult.observations} observations)`
                            : '';

                        this.sessionBtcTrades++;
                        const lagFlag = rtdsState.lagDetected ? ' ⚡' : '';
                        const alert =
                            `₿ <b>[${btcTier}] ${edgeResult.direction}${lagFlag} | ${secsLeft}s | edge: ${(edgeResult.netEdge*100).toFixed(1)}%</b>\n` +
                            `💰 $${kellyResult.stakeAmount.toFixed(2)} stake  Balance: $${this.paperBalance.toFixed(2)}\n\n` +
                            `Fair: ${(bayesResult.fairProbUp * 100).toFixed(1)}%  Market: ${(polyData.priceUp * 100).toFixed(1)}%  Obs: ${bayesResult.observations}\n` +
                            `⛓ CL: $${chainlinkPrice.toFixed(2)}  BN: $${binancePrice?.toFixed(2) ?? 'N/A'}\n` +
                            `Spread: ${polyData.spread ? (polyData.spread * 100).toFixed(1) + '%' : 'N/A'}  Compression: ${spreadResult.compressionConfirmed ? '✅' : '⏸'} Q${spreadResult.qualityScore}/5\n` +
                            `${distNote.trim()}${trendNote}${gapNote}${vacuumBadge}${warmupNote}\n\n` +
                            `📍 ${polyData.slug}  #${this.sessionBtcTrades}/${this.signalCount}\n` +
                            `🟡 PAPER TRADE`;

                        console.log('\n' + this.edge.summary(edgeResult) + '\n');
                        await sendTelegram(alert);
                        db.prepare('UPDATE signals SET alert_sent = 1 WHERE id = ?').run(btcSignalRow.lastInsertRowid);
                    } else {
                        console.warn(`[Paper] ⚠ BTC Kelly skip — status:${kellyResult.status} stake:${kellyResult.stakeAmount} fairProbWin:${fairProbWin} entryPrice:${entryPrice} balance:${this.paperBalance}`);
                    }
                    } // edge >= 10%: trade and alert executed
                    // else: edge < 10%, skip trade and alert, continue processing
                    } // btcKellyPrecheck > 0
                    } // end race/cap guard else
                }
                // Always count tradeable ticks for stats even if suppressed
            }

        } catch (err) {
            console.error(`[Engine] Error on tick ${this.tickCount} window=${this.lastWindowTs ?? '?'}:`, err.message, err.stack);
        }
    }

    /**
     * Write a lightweight status.json for OpenClaw to read without running node.
     * Updated every 5 minutes automatically.
     */
    _writeStatusFile() {
        try {
            const stats = db.prepare(`
                SELECT COUNT(*) as ticks, SUM(tradeable) as signals,
                       MAX(timestamp) as last_tick,
                       COUNT(DISTINCT window_slug) as windows
                FROM signals WHERE timestamp > datetime('now','-24 hours')
            `).get();

            const trades = db.prepare(`
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
                       ROUND(SUM(COALESCE(pnl,0)),2) as total_pnl,
                       ROUND((SELECT balance_after FROM paper_trades WHERE balance_after IS NOT NULL ORDER BY id DESC LIMIT 1), 2) as balance
                FROM paper_trades
            `).get();

            const density = db.prepare(`
                SELECT COUNT(*) as total_10min, SUM(tradeable) as tradeable_10min
                FROM signals
                WHERE timestamp > datetime('now','-10 minutes')
                AND window_slug NOT LIKE 'eth%'
            `).get();
            const ethDensity = db.prepare(`
                SELECT COUNT(*) as total_10min, SUM(tradeable) as tradeable_10min
                FROM signals
                WHERE timestamp > datetime('now','-10 minutes')
                AND window_slug LIKE 'eth%'
            `).get();
            const rolling20 = db.prepare(`
                SELECT ROUND(100.0 * SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as wr
                FROM (SELECT pnl FROM paper_trades WHERE resolved_up IS NOT NULL ORDER BY id DESC LIMIT 20)
            `).get();
            const rolling50 = db.prepare(`
                SELECT ROUND(SUM(pnl), 2) as pnl
                FROM (SELECT pnl FROM paper_trades WHERE resolved_up IS NOT NULL ORDER BY id DESC LIMIT 50)
            `).get();

            const status = {
                running:           true,
                updated:           new Date().toISOString(),
                last_tick:         stats.last_tick || null,
                ticks_24h:         stats.ticks || 0,
                signals_24h:       stats.signals || 0,
                windows_24h:       stats.windows || 0,
                balance:           trades.balance || 100.00,
                pnl_24h:           trades.total_pnl || 0,
                trades:            trades.total || 0,
                wins:              trades.wins || 0,
                losses:            trades.losses || 0,
                current_window:    this.lastWindowTs ? `btc-updown-5m-${this.lastWindowTs}` : null,
                rtds_connected:    this.rtds?.connected || false,
                btc_density_10min: density?.tradeable_10min || 0,
                eth_density_10min: ethDensity?.tradeable_10min || 0,
                rolling_wr_20:     rolling20?.wr || 0,
                rolling_pnl_50:    rolling50?.pnl || 0,
            };

            const statusPath = path.join(dataDir, 'status.json');
            fs.writeFileSync(statusPath, JSON.stringify(status, null, 2));

            // ── trades_summary.json — tier breakdown for Nova ─────────────────
            const tierRows = db.prepare(`
                SELECT tier,
                       COUNT(*) as trades,
                       SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losses,
                       ROUND(SUM(COALESCE(pnl,0)),2) as total_pnl,
                       ROUND(AVG(CASE WHEN pnl IS NOT NULL THEN pnl END),2) as avg_pnl
                FROM paper_trades GROUP BY tier ORDER BY tier
            `).all();
            const tradesSummary = {
                updated:  new Date().toISOString(),
                balance:  trades.balance || 100.00,
                total_trades: trades.total || 0,
                wins:     trades.wins || 0,
                losses:   trades.losses || 0,
                total_pnl: trades.total_pnl || 0,
                tiers:    tierRows
            };
            fs.writeFileSync(path.join(dataDir, 'trades_summary.json'), JSON.stringify(tradesSummary, null, 2));

            // ── signals_summary.json — suppression breakdown for Nova ─────────
            const suppRows = db.prepare(`
                SELECT suppression_flags, COUNT(*) as count
                FROM signals
                WHERE tradeable = 1
                AND timestamp > datetime('now','-24 hours')
                GROUP BY suppression_flags ORDER BY count DESC LIMIT 10
            `).all();
            const tierASig = db.prepare(`
                SELECT COUNT(*) as count FROM signals
                WHERE net_edge >= 0.40 AND timestamp > datetime('now','-24 hours')
            `).get();
            const signalsSummary = {
                updated:          new Date().toISOString(),
                ticks_24h:        stats.ticks || 0,
                tradeable_24h:    stats.signals || 0,
                tier_A_signals_24h: tierASig?.count || 0,
                suppression_breakdown: suppRows
            };
            fs.writeFileSync(path.join(dataDir, 'signals_summary.json'), JSON.stringify(signalsSummary, null, 2));

            // ── last_signal.json — most recent fired trade ────────────────────
            const lastTrade = db.prepare(`
                SELECT timestamp, window_slug, tier, direction,
                       entry_price, stake_amount, net_edge_pct, pnl
                FROM paper_trades ORDER BY id DESC LIMIT 1
            `).get();
            if (lastTrade) {
                fs.writeFileSync(path.join(dataDir, 'last_signal.json'), JSON.stringify({
                    updated: new Date().toISOString(),
                    ...lastTrade
                }, null, 2));
            }

        } catch (err) {
            console.error('[Engine] Status write failed:', err.message);
        }
    }

    stop() {
        this.running = false;
        // Write final status showing bot is stopped
        try {
            const statusPath = path.join(dataDir, 'status.json');
            const existing = JSON.parse(fs.readFileSync(statusPath, 'utf8'));
            existing.running = false;
            existing.updated = new Date().toISOString();
            fs.writeFileSync(statusPath, JSON.stringify(existing, null, 2));
        } catch(e) {}
        if (this._statusInterval) clearInterval(this._statusInterval);
        this.rtds.disconnect();
        this.control.stop();
        db.close();
        console.log(`\n[Engine] Stopped. ${this.tickCount} ticks | ${this.signalCount} signals | ${this.lagSignalCount} lag signals`);
    }
}

// ── CRASH HANDLERS — log why engine restarts ─────────────────────────────────
// Restarts seen at 8:26 PM and 10:26 PM — need to know the cause
// Check data/crash.log after any unexpected restart

process.on('uncaughtException', (err) => {
    console.error('\n[FATAL] Uncaught exception:', err.message);
    console.error(err.stack);
    try {
        const crashLog = `[${new Date().toISOString()}] CRASH: ${err.message}\n${err.stack}\n\n`;
        require('fs').appendFileSync(require('path').join(dataDir, 'crash.log'), crashLog);
    } catch(e) {}
    process.exit(1);
});

process.on('unhandledRejection', (reason) => {
    console.error('\n[FATAL] Unhandled rejection:', reason);
    try {
        const crashLog = `[${new Date().toISOString()}] REJECTION: ${reason}\n\n`;
        require('fs').appendFileSync(require('path').join(dataDir, 'crash.log'), crashLog);
    } catch(e) {}
    process.exit(1);
});

// ── ENTRY POINT ───────────────────────────────────────────────────────────────

const engine = new ArbEngine();

process.on('SIGINT', () => {
    engine.stop();
    process.exit(0);
});

engine.start().catch(err => {
    console.error('Engine failed to start:', err);
    process.exit(1);
});