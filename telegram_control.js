/**
 * telegram_control.js
 *
 * Telegram control panel for the arb engine.
 * Runs alongside arb_engine.js — listens for commands from your
 * Telegram chat and responds with live engine stats.
 *
 * COMMANDS:
 *   /status   — is the engine running, RTDS connected, current window
 *   /pnl      — paper trade P&L summary from arb_signals_v2.db
 *   /signals  — last 5 signals with direction and edge
 *   /lag      — current Binance vs Chainlink gap (the core metric)
 *   /pause    — stop evaluating new signals (keeps engine + RTDS running)
 *   /resume   — resume signal evaluation after pause
 *   /stop     — graceful shutdown of the entire engine
 *
 * HOW IT WORKS:
 *   Telegram bot uses long-polling (getUpdates) — no webhook needed.
 *   No extra server, no port forwarding. Works on your Windows machine
 *   behind any firewall as long as outbound HTTPS is allowed.
 *
 * INTEGRATION:
 *   This module exports a TelegramControl class.
 *   arb_engine.js creates an instance and passes itself as a reference,
 *   so the control panel can read engine state and call engine.pause() etc.
 */

const fetch = require('node-fetch');
const Database = require('better-sqlite3');
const path = require('path');

class TelegramControl {
    constructor(engine) {
        this.engine    = engine;  // reference to ArbEngine instance
        this.token     = process.env.TELEGRAM_BOT_TOKEN;
        this.chatId    = process.env.TELEGRAM_CHAT_ID;
        this.startTime = Date.now();
        this.offset   = 0;       // for getUpdates long-polling
        this.running  = false;
        this.paused   = false;
        this.db       = null;

        // Try to open the signals database for /pnl and /signals queries
        try {
            const dbPath = path.resolve(__dirname, 'data', 'arb_signals_v2.db');
            this.db = new Database(dbPath, { readonly: true });
        } catch (e) {
            // DB may not exist yet on first run — that's fine
        }
    }

    /**
     * Start the long-polling loop.
     * Checks for new Telegram messages every 2 seconds.
     */
    start() {
        if (!this.token || !this.chatId) {
            console.log('[Telegram] Control panel disabled — TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in .env');
            return;
        }

        this.running = true;
        console.log('[Telegram] Control panel active — listening for commands');
        this._drainAndPoll();
    }

    /**
     * On startup: silently consume all pending messages so stale commands
     * (e.g. a /stop sent while engine was offline) don't execute on restart.
     */
    async _drainAndPoll() {
        try {
            const stale = await this._getUpdates(0); // timeout=0 = no wait, just grab what's there
            if (stale.length > 0) {
                this.offset = stale[stale.length - 1].update_id + 1;
                console.log(`[Telegram] Drained ${stale.length} stale message(s) on startup (offset → ${this.offset})`);
            }
        } catch (e) {
            console.error('[Telegram] Drain error:', e.message);
        }
        this._poll();
    }

    async _poll() {
        while (this.running) {
            try {
                const updates = await this._getUpdates();
                for (const update of updates) {
                    await this._handleUpdate(update);
                    this.offset = update.update_id + 1;
                }
            } catch (err) {
                console.error('[Telegram] Poll error:', err.message);
            }
            // Wait 2 seconds between polls
            await new Promise(r => setTimeout(r, 2000));
        }
    }

    async _getUpdates(timeout = 1) {
        try {
            const url = `https://api.telegram.org/bot${this.token}/getUpdates?offset=${this.offset}&timeout=${timeout}`;
            const res  = await fetch(url, { timeout: 8000 });
            const data = await res.json();
            return data.ok ? data.result : [];
        } catch {
            return [];
        }
    }

    async _handleUpdate(update) {
        const msg  = update.message || update.edited_message;
        if (!msg || !msg.text) return;

        // Only respond to messages from your own chat ID (security)
        if (String(msg.chat.id) !== String(this.chatId)) {
            console.warn(`[Telegram] Message from unknown chat ${msg.chat.id} — ignored`);
            return;
        }

        const text = msg.text.trim().toLowerCase().split(' ')[0];
        console.log(`[Telegram] Command received: ${text}`);

        switch (text) {
            case '/status':  await this.send(this._buildStatus());   break;
            case '/pnl':     await this.send(this._buildPnl());      break;
            case '/signals': await this.send(this._buildSignals());  break;
            case '/lag':     await this.send(this._buildLag());      break;
            case '/pause':   await this._doPause();                  break;
            case '/resume':  await this._doResume();                 break;
            case '/stop':    await this._doStop();                   break;
            case '/help':    await this.send(this._buildHelp());     break;
            case '/morning': await this.send(this._buildMorningReport()); break;
            case '/mode':    await this.send(this._buildMode());   break;
            case '/lag_paper': {
                this.engine.lagMode = 'paper';
                await this.send('Lag detector set to PAPER mode');
                break;
            }
            case '/lag_live': {
                this.engine.lagMode = 'live';
                await this.send('⚠️ Lag detector set to LIVE mode');
                break;
            }
            case '/arb_paper': {
                this.engine.arbMode = 'paper';
                await this.send('Arb detector set to PAPER mode');
                break;
            }
            case '/arb_live': {
                this.engine.arbMode = 'live';
                await this.send('⚠️ Arb detector set to LIVE mode');
                break;
            }
            case '/hive': {
                if (Date.now() - this.startTime < 30000) {
                    await this.send('⏳ Engine just restarted — please wait 30 seconds before running /hive');
                    break;
                }
                const fullText = msg.text.trim();
                const isBrainstorm = fullText.toLowerCase().includes('brainstorm');
                const question = fullText
                    .replace(/^\/hive\s*/i, '')
                    .replace(/^brainstorm\s*/i, '')
                    .trim();
                if (!question) {
                    await this.send('Usage:\n/hive [question]\n/hive brainstorm [question]');
                    break;
                }
                await this.send('🔄 Running HIVE review... (~30s)');
                try {
                    const { runHive } = require('./hive_lite');
                    await runHive(question, isBrainstorm ? 'brainstorm' : 'decision');
                } catch (err) {
                    await this.send(`❌ HIVE failed: ${err.message}`);
                }
                break;
            }
            default:
                await this.send(`Unknown command: ${text}\nType /help for available commands.`);
        }
    }

    // ── COMMAND HANDLERS ─────────────────────────────────────────────────────

    _buildStatus() {
        const e    = this.engine;
        const rtds = e.rtds?.getState();
        const poly = e.lastPolyData;
        const win  = poly ? poly.slug : 'unknown';
        const secs = poly ? poly.secondsRemaining : '?';

        return [
            `📡 <b>Engine Status</b>`,
            ``,
            `Mode:      ${this.paused ? '⏸ PAUSED' : '▶️  RUNNING'}`,
            `Lag:       ${e.lagMode === 'live' ? '🔴 LIVE' : '🟡 PAPER'}  Arb: ${e.arbMode === 'live' ? '🔴 LIVE' : '🟡 PAPER'}`,
            `RTDS:      ${e.rtds?.connected ? '🟢 Connected' : '🔴 Disconnected'}`,
            `Ticks:     ${e.tickCount.toLocaleString()}`,
            `Signals:   ${e.signalCount} total | ${e.lagSignalCount} lag-based`,
            ``,
            `📍 Window: ${win}`,
            `⏱ Time left: ${secs}s`,
            ``,
            rtds ? [
                `⛓ Chainlink: $${rtds.chainlinkPrice?.toFixed(2) ?? 'pending'}`,
                `₿  Binance:  $${rtds.binancePrice?.toFixed(2) ?? 'pending'}`,
                `Gap: ${rtds.binanceGapPct >= 0 ? '+' : ''}${rtds.binanceGapPct?.toFixed(3) ?? '?'}%${rtds.lagDetected ? ' ⚡' : ''}`,
            ].join('\n') : 'Waiting for first prices...',
        ].join('\n');
    }

    _buildPnl() {
        if (!this.db) return '❌ Database not available yet — run the engine first.';

        try {
            // Re-open db in case it was created after control panel started
            if (!this.db) {
                const dbPath = path.resolve(__dirname, 'data', 'arb_signals_v2.db');
                this.db = new Database(dbPath, { readonly: true });
            }

            const stats = this.db.prepare(`
                SELECT
                    COUNT(*)                                    as total,
                    SUM(CASE WHEN tradeable=1 THEN 1 ELSE 0 END) as signals,
                    SUM(CASE WHEN lag_detected=1 THEN 1 ELSE 0 END) as lag_ticks,
                    AVG(CASE WHEN tradeable=1 THEN net_edge END) as avg_edge,
                    MAX(CASE WHEN tradeable=1 THEN net_edge END) as best_edge,
                    MIN(timestamp)                              as first_tick,
                    MAX(timestamp)                              as last_tick
                FROM signals
            `).get();

            const windows = this.db.prepare(`
                SELECT COUNT(*) as total FROM windows
            `).get();

            const firstTime = stats.first_tick
                ? new Date(stats.first_tick).toLocaleTimeString()
                : 'N/A';
            const lastTime  = stats.last_tick
                ? new Date(stats.last_tick).toLocaleTimeString()
                : 'N/A';

            return [
                `📊 <b>Paper Trading P&amp;L</b>`,
                ``,
                `Period:    ${firstTime} → ${lastTime}`,
                `Windows:   ${windows.total}`,
                `Ticks:     ${stats.total?.toLocaleString() ?? 0}`,
                `Signals:   ${stats.signals ?? 0}`,
                `Lag ticks: ${stats.lag_ticks ?? 0}`,
                ``,
                `Avg edge:  ${stats.avg_edge ? (stats.avg_edge * 100).toFixed(2) + '%' : 'N/A'}`,
                `Best edge: ${stats.best_edge ? (stats.best_edge * 100).toFixed(2) + '%' : 'N/A'}`,
                ``,
                `💡 Outcome tracking active after 2 weeks paper trading`,
            ].join('\n');

        } catch (err) {
            return `❌ DB error: ${err.message}`;
        }
    }

    _buildSignals() {
        if (!this.db) return '❌ No database yet.';

        try {
            const rows = this.db.prepare(`
                SELECT timestamp, window_slug, seconds_left, direction,
                       net_edge, binance_gap_pct, lag_detected, poly_price_up, fair_prob_up
                FROM signals
                WHERE tradeable = 1
                ORDER BY id DESC
                LIMIT 5
            `).all();

            if (!rows.length) return '📭 No tradeable signals yet.';

            const lines = ['📋 <b>Last 5 Signals</b>', ''];
            for (const r of rows) {
                const time    = new Date(r.timestamp).toLocaleTimeString();
                const lagStr  = r.lag_detected ? ' ⚡' : '';
                const edgeStr = (r.net_edge * 100).toFixed(1) + '%';
                const gapStr  = r.binance_gap_pct !== null
                    ? ` gap:${r.binance_gap_pct >= 0 ? '+' : ''}${r.binance_gap_pct.toFixed(2)}%`
                    : '';
                lines.push(
                    `${time} | <b>${r.direction}</b>${lagStr} | edge:${edgeStr}${gapStr} | ${r.seconds_left}s left`
                );
            }
            return lines.join('\n');

        } catch (err) {
            return `❌ DB error: ${err.message}`;
        }
    }

    _buildLag() {
        const rtds = this.engine.rtds?.getState();
        if (!rtds) return '⏳ Waiting for RTDS connection...';

        const gap     = rtds.binanceGapPct;
        const ageS    = rtds.chainlinkAge ? (rtds.chainlinkAge / 1000).toFixed(1) : '?';
        const thresh  = 0.4;
        const bar     = this._lagBar(gap, thresh);

        return [
            `⛓ <b>Chainlink Lag Monitor</b>`,
            ``,
            `Chainlink: $${rtds.chainlinkPrice?.toFixed(2) ?? '?'}`,
            `Binance:   $${rtds.binancePrice?.toFixed(2) ?? '?'}`,
            ``,
            `Gap: ${gap >= 0 ? '+' : ''}${gap?.toFixed(3) ?? '?'}%  ${bar}`,
            `CL age: ${ageS}s since last update`,
            ``,
            rtds.lagDetected
                ? `⚡ LAG ACTIVE — Chainlink update imminent → ${rtds.lagDirection}`
                : `✅ No lag — Chainlink is current`,
            ``,
            `Threshold: ±${thresh}% triggers lag signal`,
        ].join('\n');
    }

    _lagBar(gap, threshold) {
        // Simple ASCII bar showing gap vs threshold
        if (gap === null || gap === undefined) return '';
        const pct    = Math.min(Math.abs(gap) / threshold, 1.0);
        const filled = Math.round(pct * 10);
        const bar    = '█'.repeat(filled) + '░'.repeat(10 - filled);
        return `[${bar}] ${Math.abs(gap).toFixed(3)}% / ${threshold}%`;
    }

    async _doPause() {
        this.paused        = true;
        this.engine.paused = true;
        await this.send('⏸ <b>Engine paused.</b>\nRTDS still connected. No new signals will fire.\nSend /resume to restart.');
    }

    async _doResume() {
        this.paused        = false;
        this.engine.paused = false;
        await this.send('▶️ <b>Engine resumed.</b>\nSignal evaluation active.');
    }

    async _doStop() {
        await this.send('🛑 <b>Stopping engine...</b>\nGoodbye.');
        setTimeout(() => {
            this.engine.stop();
            process.exit(0);
        }, 1000);
    }

    _buildMorningReport() {
        const fs   = require('fs');
        const path = require('path');
        const file = path.resolve(__dirname, 'data', 'morning_report.json');
        if (!fs.existsSync(file)) return '⚠️ Morning report not found — has morning_analysis.bat run today?';
        const stat = fs.statSync(file);
        const ageHours = (Date.now() - stat.mtimeMs) / 3600000;
        if (ageHours > 12) return `⚠️ Morning report is stale (${ageHours.toFixed(1)}h old) — morning_analysis.bat may not have run`;
        try {
            const d = JSON.parse(fs.readFileSync(file, 'utf8'));
            let status = {};
            try {
                const statusPath = path.resolve(__dirname, 'data', 'status.json');
                status = JSON.parse(fs.readFileSync(statusPath, 'utf8'));
            } catch (_) {}
            const tierLine = (t) => {
                const row = (d.tiers || []).find(r => r.tier === t);
                return row ? `${row.trades} trades | WR: ${(row.winrate * 100).toFixed(0)}% | ROC: ${row.roc}` : 'no data';
            };
            const suppLines = (d.up_suppression || []).map(s => `  ${s.suppression_reason}: ${s.count}`).join('\n') || '  (none)';
            return [
                `📊 <b>Morning Arb Report</b>`,
                ``,
                `💰 Balance: $${d.balance} | P&amp;L: $${d.pnl}`,
                `📈 Win Rate: ${d.win_rate}% (${d.trades} total trades)`,
                ``,
                `<b>Tier Performance:</b>`,
                `A (40%+): ${tierLine('A')}`,
                `B (30-40%): ${tierLine('B')}`,
                `C (20-30%): ${tierLine('C')}`,
                `D (10-20%): ${tierLine('D')}`,
                ``,
                `🔍 UP signal suppression:`,
                suppLines,
                ``,
                `📉 <b>Rolling Performance:</b>`,
                `Last 20 trades WR: ${status.rolling_wr_20 ?? 'N/A'}%`,
                `Last 50 trades P&L: $${status.rolling_pnl_50 ?? 'N/A'}`,
            ].join('\n');
        } catch (e) {
            return `⚠️ Failed to parse morning report: ${e.message}`;
        }
    }

    _buildMode() {
        const e = this.engine;
        return [
            `⚙️ <b>Strategy Modes</b>`,
            ``,
            `Lag detector: ${e.lagMode === 'live' ? '🔴 LIVE' : '🟡 PAPER'}`,
            `Arb detector: ${e.arbMode === 'live' ? '🔴 LIVE' : '🟡 PAPER'}`,
            ``,
            `Toggle with:`,
            `/lag_paper  /lag_live`,
            `/arb_paper  /arb_live`,
        ].join('\n');
    }

    _buildHelp() {
        return [
            `🤖 <b>Arb Bot Commands</b>`,
            ``,
            `/status  — engine state, RTDS, current window`,
            `/mode    — show lag/arb paper/live modes`,
            `/pnl     — paper trade stats from database`,
            `/signals — last 5 tradeable signals`,
            `/lag     — live Chainlink vs Binance gap`,
            `/morning — today's morning analysis report`,
            `/pause   — stop signal evaluation`,
            `/resume  — restart signal evaluation`,
            `/stop    — shut down the engine`,
            ``,
            `<b>Mode Toggles:</b>`,
            `/lag_paper  /lag_live`,
            `/arb_paper  /arb_live`,
            ``,
            `/help    — this message`,
        ].join('\n');
    }

    // ── SEND ─────────────────────────────────────────────────────────────────

    async send(text) {
        if (!this.token || !this.chatId) return;
        try {
            await fetch(`https://api.telegram.org/bot${this.token}/sendMessage`, {
                method:  'POST',
                headers: { 'Content-Type': 'application/json' },
                body:    JSON.stringify({
                    chat_id:    this.chatId,
                    text,
                    parse_mode: 'HTML'
                }),
                timeout: 5000
            });
        } catch (err) {
            console.error('[Telegram] Send failed:', err.message);
        }
    }

    stop() {
        this.running = false;
        if (this.db) {
            try { this.db.close(); } catch(e) {}
        }
    }
}

module.exports = TelegramControl;
