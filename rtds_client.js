/**
 * rtds_client.js (v4 — dual WebSocket connections)
 *
 * ROOT CAUSE FIX:
 * The RTDS server treats subscriptions on the same topic as a replacement,
 * not an addition. Sending both 'btcusdt' and 'ethusdt' on the same
 * crypto_prices topic means ETH overwrites BTC — leaving BTC with exactly
 * one message (the one that arrived before the ETH sub fired) then silence.
 *
 * THE FIX:
 * Two independent WebSocket connections to the same RTDS server.
 *   wsBtc  — owns: crypto_prices(btcusdt) + crypto_prices_chainlink(btc/usd)
 *   wsEth  — owns: crypto_prices(ethusdt) + crypto_prices_chainlink(eth/usd)
 *
 * No topic collision possible. Both connections reconnect independently.
 * External API is unchanged — arb_engine.js still just listens for
 * 'update', 'lag_detected', 'connected', 'disconnected'.
 */

const EventEmitter = require('events');
const WebSocket    = require('ws');

const LAG_THRESHOLD     = 0.004; // 0.4% gap — BTC Chainlink lag threshold
const ETH_LAG_THRESHOLD = 0.003; // 0.3% gap — ETH threshold (ETH gap profile structurally smaller than BTC)

class RTDSClient extends EventEmitter {
    constructor() {
        super();

        // ── BTC feeds ─────────────────────────────────────────────────────────
        this.binancePrice       = null;
        this.chainlinkPrice     = null;
        this.binanceTimestamp   = null;
        this.chainlinkTimestamp = null;
        this.binanceHistory     = [];

        // ── ETH feeds ─────────────────────────────────────────────────────────
        this.ethBinancePrice       = null;
        this.ethChainlinkPrice     = null;
        this.ethBinanceTimestamp   = null;
        this.ethChainlinkTimestamp = null;
        this.ethBinanceHistory     = [];

        this.maxHistory = 30;

        // ── Two independent WebSocket connections ─────────────────────────────
        this.wsBtc  = null;  // owns BTC Binance + BTC Chainlink
        this.wsEth  = null;  // owns ETH Binance + ETH Chainlink

        this.btcConnected  = false;
        this.ethConnected  = false;
        this.connected     = false;  // true when BOTH connections are live

        this._btcPingInterval    = null;
        this._ethPingInterval    = null;
        this._ethKeepaliveTimer  = null;

        // Reconnect delays (independent per connection)
        this._btcReconnectDelay = 2000;
        this._ethReconnectDelay = 2000;
        this.maxReconnectDelay  = 30000;

        this._stopping = false;

        // First-message flags — log once per feed to confirm subscription worked
        this._firstBtcBinance   = true;
        this._firstBtcChainlink = true;
        this._firstEthBinance   = true;
        this._firstEthChainlink = true;

        this.wsUrl = 'wss://ws-live-data.polymarket.com';
    }

    // ── PUBLIC API ───────────────────────────────────────────────────────────

    connect() {
        this._connectBtc();
        // Stagger ETH connection 1s after BTC — avoids server-side rate limits
        // on simultaneous handshakes from the same IP
        setTimeout(() => this._connectEth(), 1000);
    }

    disconnect() {
        this._stopping = true;
        this._clearPing('btc');
        this._clearPing('eth');
        if (this.wsBtc) { this.wsBtc.close(); this.wsBtc = null; }
        if (this.wsEth) { this.wsEth.close(); this.wsEth = null; }
        console.log('[RTDS] Both connections closed.');
    }

    getState() {
        if (!this.chainlinkPrice) return null;
        const binanceGap = (this.binancePrice && this.chainlinkPrice)
            ? (this.binancePrice - this.chainlinkPrice) / this.chainlinkPrice
            : null;
        const ethGap = (this.ethBinancePrice && this.ethChainlinkPrice)
            ? (this.ethBinancePrice - this.ethChainlinkPrice) / this.ethChainlinkPrice
            : null;
        return {
            binancePrice:      this.binancePrice,
            chainlinkPrice:    this.chainlinkPrice,
            binanceGap:        binanceGap,
            binanceGapPct:     binanceGap !== null ? binanceGap * 100 : null,
            lagDetected:       binanceGap !== null && Math.abs(binanceGap) >= LAG_THRESHOLD,
            lagDirection:      binanceGap !== null ? (binanceGap > 0 ? 'UP' : 'DOWN') : null,
            binanceVelocity:   this._calculateVelocity(),
            chainlinkAge:      this.chainlinkTimestamp ? Date.now() - this.chainlinkTimestamp : null,
            ethBinancePrice:   this.ethBinancePrice,
            ethChainlinkPrice: this.ethChainlinkPrice,
            ethGapPct:         ethGap !== null ? ethGap * 100 : null,
            ethLagDetected:    ethGap !== null && Math.abs(ethGap) >= ETH_LAG_THRESHOLD,
            ethLagDirection:   ethGap !== null ? (ethGap > 0 ? 'UP' : 'DOWN') : null,
            ethChainlinkAge:   this.ethChainlinkTimestamp ? Date.now() - this.ethChainlinkTimestamp : null,
            ethBinanceVelocity: this._calculateEthVelocity(),
            timestamp:         new Date().toISOString()
        };
    }

    // ── BTC CONNECTION ───────────────────────────────────────────────────────

    _connectBtc() {
        if (this._stopping) return;
        console.log('[RTDS:BTC] Connecting...');

        this.wsBtc = new WebSocket(this.wsUrl);

        this.wsBtc.on('open', async () => {
            console.log('[RTDS:BTC] Connected ✅');
            this.btcConnected       = true;
            this._btcReconnectDelay = 2000;
            this._updateConnected();

            const send  = (payload) => {
                if (this.wsBtc?.readyState === WebSocket.OPEN)
                    this.wsBtc.send(JSON.stringify(payload));
            };
            const delay = (ms) => new Promise(r => setTimeout(r, ms));

            // BTC Binance — crypto_prices topic, btcusdt only
            send({ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices',           type: 'update', filters: '{"symbol":"btcusdt"}' }] });
            console.log('[RTDS:BTC] Sent: btcusdt subscription');
            await delay(500);

            // BTC Chainlink — crypto_prices_chainlink topic, btc/usd only
            send({ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices_chainlink', type: '*',      filters: '{"symbol":"btc/usd"}' }] });
            console.log('[RTDS:BTC] Sent: btc/usd subscription — waiting for feeds...');

            this._btcPingInterval = setInterval(() => {
                if (this.wsBtc?.readyState === WebSocket.OPEN) this.wsBtc.ping();
            }, 5000);
        });

        this.wsBtc.on('pong', () => {});

        this.wsBtc.on('message', (data) => {
            try {
                const msg = JSON.parse(data.toString());
                this._handleBtcMessage(msg);
            } catch (e) {}
        });

        this.wsBtc.on('close', (code) => {
            this.btcConnected = false;
            this._clearPing('btc');
            this._updateConnected();
            console.warn(`[RTDS:BTC] Disconnected (${code}). Reconnecting in ${this._btcReconnectDelay / 1000}s...`);
            if (!this._stopping) {
                setTimeout(() => this._connectBtc(), this._btcReconnectDelay);
                this._btcReconnectDelay = Math.min(this._btcReconnectDelay * 1.5, this.maxReconnectDelay);
            }
        });

        this.wsBtc.on('error', (err) => {
            console.error('[RTDS:BTC] Error:', err.message);
        });
    }

    _handleBtcMessage(msg) {
        if (msg.statusCode || !msg.topic || !msg.payload) return;
        const { topic, payload } = msg;

        if (topic === 'crypto_prices' && payload.symbol === 'btcusdt') {
            this._handleBinance(payload);
        } else if (topic === 'crypto_prices_chainlink' && payload.symbol === 'btc/usd') {
            this._handleChainlink(payload);
        }
    }

    // ── ETH CONNECTION ───────────────────────────────────────────────────────

    _connectEth() {
        if (this._stopping) return;
        console.log('[RTDS:ETH] Connecting...');

        this.wsEth = new WebSocket(this.wsUrl);

        this.wsEth.on('open', async () => {
            console.log('[RTDS:ETH] Connected ✅');
            this.ethConnected       = true;
            this._ethReconnectDelay = 2000;
            this._updateConnected();

            const send  = (payload) => {
                if (this.wsEth?.readyState === WebSocket.OPEN)
                    this.wsEth.send(JSON.stringify(payload));
            };
            const delay = (ms) => new Promise(r => setTimeout(r, ms));

            // ETH Binance — crypto_prices topic, ethusdt only
            send({ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices',           type: 'update', filters: '{"symbol":"ethusdt"}' }] });
            console.log('[RTDS:ETH] Sent: ethusdt subscription');
            await delay(500);

            // ETH Chainlink — crypto_prices_chainlink topic, eth/usd only
            send({ action: 'subscribe', subscriptions: [{ topic: 'crypto_prices_chainlink', type: '*',      filters: '{"symbol":"eth/usd"}' }] });
            console.log('[RTDS:ETH] Sent: eth/usd subscription — waiting for feeds...');

            this._ethPingInterval = setInterval(() => {
                if (this.wsEth?.readyState === WebSocket.OPEN) this.wsEth.ping();
            }, 5000);

            // Keepalive: ETH Chainlink feed sometimes goes silently dead without
            // triggering a disconnect event. If the feed hasn't updated in 60s
            // while the WebSocket is still "connected", force a reconnect.
            this._ethKeepaliveTimer = setInterval(() => {
                if (!this.ethConnected || this._stopping) return;
                const age = this.ethChainlinkTimestamp
                    ? Date.now() - this.ethChainlinkTimestamp
                    : null;
                if (age !== null && age > 60000) {
                    console.warn(`[RTDS:ETH] ⚠ Chainlink feed silent for ${(age/1000).toFixed(0)}s — forcing reconnect`);
                    this.ethConnected = false;
                    this._clearPing('eth');
                    this._updateConnected();
                    if (this.wsEth) {
                        this.wsEth.removeAllListeners();
                        this.wsEth.terminate();
                        this.wsEth = null;
                    }
                    setTimeout(() => this._connectEth(), this._ethReconnectDelay);
                    this._ethReconnectDelay = Math.min(this._ethReconnectDelay * 1.5, this.maxReconnectDelay);
                }
            }, 30000);
        });

        this.wsEth.on('pong', () => {});

        this.wsEth.on('message', (data) => {
            try {
                const msg = JSON.parse(data.toString());
                this._handleEthMessage(msg);
            } catch (e) {}
        });

        this.wsEth.on('close', (code) => {
            this.ethConnected = false;
            this._clearPing('eth');
            this._updateConnected();
            console.warn(`[RTDS:ETH] Disconnected (${code}). Reconnecting in ${this._ethReconnectDelay / 1000}s...`);
            if (!this._stopping) {
                setTimeout(() => this._connectEth(), this._ethReconnectDelay);
                this._ethReconnectDelay = Math.min(this._ethReconnectDelay * 1.5, this.maxReconnectDelay);
            }
        });

        this.wsEth.on('error', (err) => {
            console.error('[RTDS:ETH] Error:', err.message);
        });
    }

    _handleEthMessage(msg) {
        if (msg.statusCode || !msg.topic || !msg.payload) return;
        const { topic, payload } = msg;

        if (topic === 'crypto_prices' && payload.symbol === 'ethusdt') {
            this._handleEthBinance(payload);
        } else if (topic === 'crypto_prices_chainlink' && payload.symbol === 'eth/usd') {
            this._handleEthChainlink(payload);
        }
    }

    // ── FEED HANDLERS ────────────────────────────────────────────────────────

    _handleBinance(payload) {
        if (payload.value === undefined || payload.value === null) return;
        const price = parseFloat(payload.value);
        if (isNaN(price) || price <= 0) return;

        if (this._firstBtcBinance) {
            console.log(`[RTDS:BTC] ✅ Binance LIVE — first price: $${price.toFixed(2)}`);
            this._firstBtcBinance = false;
        }

        const now = Date.now();
        this.binancePrice     = price;
        this.binanceTimestamp = now;
        this.binanceHistory.push({ price, timestamp: now });
        if (this.binanceHistory.length > this.maxHistory) this.binanceHistory.shift();

        this._emitState('btc-binance');
    }

    _handleChainlink(payload) {
        const price = parseFloat(payload.value);
        if (isNaN(price) || price <= 0) return;

        if (this._firstBtcChainlink) {
            console.log(`[RTDS:BTC] ✅ Chainlink LIVE — first price: $${price.toFixed(2)}`);
            this._firstBtcChainlink = false;
        }

        const prev          = this.chainlinkPrice;
        this.chainlinkPrice = price;
        this.chainlinkTimestamp = Date.now();

        if (prev && Math.abs(price - prev) > 0.10) {
            const pct = ((price - prev) / prev * 100).toFixed(3);
            console.log(
                `[RTDS:BTC] ⛓  Chainlink: $${prev.toFixed(2)} → $${price.toFixed(2)} ` +
                `(${pct >= 0 ? '+' : ''}${pct}%)`
            );
        }

        this._emitState('btc-chainlink');
    }

    _handleEthBinance(payload) {
        if (payload.value === undefined || payload.value === null) return;
        const price = parseFloat(payload.value);
        if (isNaN(price) || price <= 0) return;

        if (this._firstEthBinance) {
            console.log(`[RTDS:ETH] ✅ Binance LIVE — first price: $${price.toFixed(2)}`);
            this._firstEthBinance = false;
        }

        const now = Date.now();
        this.ethBinancePrice     = price;
        this.ethBinanceTimestamp = now;
        this.ethBinanceHistory.push({ price, timestamp: now });
        if (this.ethBinanceHistory.length > this.maxHistory) this.ethBinanceHistory.shift();

        this._emitState('eth-binance');
    }

    _handleEthChainlink(payload) {
        const price = parseFloat(payload.value);
        if (isNaN(price) || price <= 0) return;

        if (this._firstEthChainlink) {
            console.log(`[RTDS:ETH] ✅ Chainlink LIVE — first price: $${price.toFixed(2)}`);
            this._firstEthChainlink = false;
        }

        const prev             = this.ethChainlinkPrice;
        this.ethChainlinkPrice = price;
        this.ethChainlinkTimestamp = Date.now();

        if (prev && Math.abs(price - prev) > 0.05) {
            const pct = ((price - prev) / prev * 100).toFixed(3);
            console.log(
                `[RTDS:ETH] ⚡ Chainlink: $${prev.toFixed(2)} → $${price.toFixed(2)} ` +
                `(${pct >= 0 ? '+' : ''}${pct}%)`
            );
        }

        this._emitState('eth-chainlink');
    }

    // ── STATE EMISSION ───────────────────────────────────────────────────────

    _emitState(source) {
        // Only require BTC Chainlink — it's the primary resolution source.
        // Binance gap/lag fields are null until Binance arrives, but we still tick.
        if (!this.chainlinkPrice) return;

        const binanceGap    = (this.binancePrice && this.chainlinkPrice)
            ? (this.binancePrice - this.chainlinkPrice) / this.chainlinkPrice
            : null;
        const binanceGapPct = binanceGap !== null ? binanceGap * 100 : null;
        const lagDetected   = binanceGap !== null && Math.abs(binanceGap) >= LAG_THRESHOLD;
        const lagDirection  = binanceGap !== null ? (binanceGap > 0 ? 'UP' : 'DOWN') : null;

        const ethGap        = (this.ethBinancePrice && this.ethChainlinkPrice)
            ? (this.ethBinancePrice - this.ethChainlinkPrice) / this.ethChainlinkPrice
            : null;
        const ethLagDetected  = ethGap !== null && Math.abs(ethGap) >= ETH_LAG_THRESHOLD;
        const ethLagDirection = ethGap !== null ? (ethGap > 0 ? 'UP' : 'DOWN') : null;

        const state = {
            // BTC
            binancePrice:      this.binancePrice,
            chainlinkPrice:    this.chainlinkPrice,
            binanceGap,
            binanceGapPct,
            lagDetected,
            lagDirection,
            binanceVelocity:   this._calculateVelocity(),
            chainlinkAge:      this.chainlinkTimestamp ? Date.now() - this.chainlinkTimestamp : null,
            // ETH
            ethBinancePrice:    this.ethBinancePrice,
            ethChainlinkPrice:  this.ethChainlinkPrice,
            ethGapPct:          ethGap !== null ? ethGap * 100 : null,
            ethLagDetected,
            ethLagDirection,
            ethChainlinkAge:    this.ethChainlinkTimestamp ? Date.now() - this.ethChainlinkTimestamp : null,
            ethBinanceVelocity: this._calculateEthVelocity(),
            source,
            timestamp: new Date().toISOString()
        };

        this.emit('update', state);
        if (lagDetected) this.emit('lag_detected', state);
    }

    // ── UTILITIES ────────────────────────────────────────────────────────────

    _updateConnected() {
        const wasConnected = this.connected;
        this.connected = this.btcConnected && this.ethConnected;

        if (!wasConnected && this.connected) {
            console.log('[RTDS] Both connections live ✅');
            this.emit('connected');
        } else if (wasConnected && !this.connected) {
            this.emit('disconnected');
        }
    }

    _calculateVelocity() {
        if (this.binanceHistory.length < 2) return 0;
        const w        = this.binanceHistory.slice(-5);
        const timeDiff = (w[w.length - 1].timestamp - w[0].timestamp) / 1000;
        const priceDiff = w[w.length - 1].price - w[0].price;
        return timeDiff > 0 ? priceDiff / timeDiff : 0;
    }

    _calculateEthVelocity() {
        if (this.ethBinanceHistory.length < 2) return 0;
        const w        = this.ethBinanceHistory.slice(-5);
        const timeDiff = (w[w.length - 1].timestamp - w[0].timestamp) / 1000;
        const priceDiff = w[w.length - 1].price - w[0].price;
        return timeDiff > 0 ? priceDiff / timeDiff : 0;
    }

    _clearPing(which) {
        if (which === 'btc' && this._btcPingInterval) {
            clearInterval(this._btcPingInterval);
            this._btcPingInterval = null;
        }
        if (which === 'eth') {
            if (this._ethPingInterval) {
                clearInterval(this._ethPingInterval);
                this._ethPingInterval = null;
            }
            if (this._ethKeepaliveTimer) {
                clearInterval(this._ethKeepaliveTimer);
                this._ethKeepaliveTimer = null;
            }
        }
    }
}

module.exports = RTDSClient;