/**
 * spread_model.js (v1) — Model 3: Spread Compression Detector
 *
 * WHAT THIS DOES:
 * Tracks the spread trajectory across each 5-minute window and identifies
 * moments when liquidity is genuinely arriving (spread compressing from wide
 * to tight) vs moments when the spread just happens to be tight.
 *
 * WHY THIS MATTERS:
 * DB analysis of 36 resolved trades showed that tight spread alone is NOT
 * a reliable entry signal — the worst loss (-$13.94) occurred in the window
 * with the tightest, most stable spread (0.4–4%). All 3 wins entered during
 * brief compression windows inside otherwise illiquid periods.
 *
 * The signal is not "spread is tight" — it's "spread is actively compressing."
 *
 * THE MODEL:
 *   spread_velocity = (current_spread - spread_N_seconds_ago) / N
 *   Negative velocity = compressing (good)
 *   Positive velocity = widening (bad)
 *
 *   compression_confirmed = velocity < 0
 *     AND spread dropped >= MIN_COMPRESSION_DROP in last 60s
 *     AND current spread <= MAX_TRADEABLE_SPREAD
 *     AND opening_spread >= MIN_OPENING_SPREAD (must have started wide)
 *
 * SPREAD QUALITY SCORE:
 *   Based on DB data showing 4-6% bucket has highest avg edge (14.1%)
 *   and most tradeable ticks (2,851):
 *   0-2%:  score 3 (tight but may be end-of-window noise)
 *   2-4%:  score 4
 *   4-6%:  score 5 (sweet spot)
 *   6-8%:  score 3
 *   >8%:   score 0 (blocked by existing spread filter)
 *
 * INTEGRATION:
 *   Called after EdgeCalculator.evaluate() in arb_engine.js
 *   Adds spread_compressing and spread_quality to the signal context
 *   Does NOT replace existing has_orderbook or spread filters
 */

class SpreadModel {
    constructor(config = {}) {
        // ── Compression thresholds ─────────────────────────────────────────────
        this.minCompressionDrop   = config.minCompressionDrop   ?? 0.05;  // must drop 5pp in 60s
        this.minOpeningSpread     = config.minOpeningSpread     ?? 0.15;  // must have started ≥15% wide
        this.velocityWindow       = config.velocityWindow       ?? 30;    // seconds to measure velocity over
        this.compressionWindow    = config.compressionWindow    ?? 60;    // seconds to measure total drop over

        // ── History ───────────────────────────────────────────────────────────
        this.spreadHistory        = [];   // { spread, timestamp } per tick
        this.maxHistory           = 120;  // ~2 min at 2s polling
        this.openingSpread        = null; // first spread seen this window
        this.openingWindowTs      = null;
    }

    /**
     * Record a spread observation for the current window.
     * Call this every tick after polymarket data arrives.
     *
     * @param {number|null} spread    current bid-ask spread (null = no orderbook)
     * @param {number}      windowTs  current window timestamp
     */
    observe(spread, windowTs) {
        // Reset on new window
        if (this.openingWindowTs !== windowTs) {
            this.spreadHistory   = [];
            this.openingSpread   = null;
            this.openingWindowTs = windowTs;
        }

        // Only record live orderbook ticks
        if (spread === null || spread === undefined) return;

        const now = Date.now();

        // First live spread seen this window
        if (this.openingSpread === null) {
            this.openingSpread = spread;
        }

        this.spreadHistory.push({ spread, timestamp: now });
        if (this.spreadHistory.length > this.maxHistory) {
            this.spreadHistory.shift();
        }
    }

    /**
     * Calculate spread velocity (change per second) over the last N seconds.
     * Negative = compressing. Positive = widening.
     *
     * @param {number} windowSeconds  how far back to look
     * @returns {number|null}         spread/sec or null if insufficient data
     */
    calculateVelocity(windowSeconds = this.velocityWindow) {
        if (this.spreadHistory.length < 2) return null;

        const now     = Date.now();
        const cutoff  = now - (windowSeconds * 1000);
        const inWindow = this.spreadHistory.filter(h => h.timestamp >= cutoff);

        if (inWindow.length < 2) return null;

        const oldest  = inWindow[0];
        const newest  = inWindow[inWindow.length - 1];
        const timeDiff = (newest.timestamp - oldest.timestamp) / 1000;

        if (timeDiff < 1) return null;

        return (newest.spread - oldest.spread) / timeDiff;
    }

    /**
     * Calculate total spread drop over the last N seconds.
     * Positive = it dropped (compressed). Negative = it widened.
     *
     * @param {number} windowSeconds  how far back to look
     * @returns {number|null}         total drop or null if insufficient data
     */
    calculateCompressionDrop(windowSeconds = this.compressionWindow) {
        if (this.spreadHistory.length < 2) return null;

        const now      = Date.now();
        const cutoff   = now - (windowSeconds * 1000);
        const inWindow = this.spreadHistory.filter(h => h.timestamp >= cutoff);

        if (inWindow.length < 2) return null;

        const maxSpread = Math.max(...inWindow.map(h => h.spread));
        const current   = this.spreadHistory[this.spreadHistory.length - 1].spread;

        return maxSpread - current;  // positive = compressed from peak
    }

    /**
     * Score the current spread quality.
     * Based on DB analysis: 4-6% bucket has highest avg edge and most tradeable ticks.
     *
     * @param {number|null} spread  current spread
     * @returns {number}            0-5 quality score
     */
    spreadQualityScore(spread) {
        if (spread === null || spread === undefined) return 0;
        if (spread < 0.02) return 3;   // 0-2%:  tight but may be end-of-window noise
        if (spread < 0.04) return 4;   // 2-4%:  good
        if (spread < 0.06) return 5;   // 4-6%:  sweet spot — highest edge in DB data
        if (spread < 0.08) return 3;   // 6-8%:  acceptable but wider
        return 0;                       // >8%:   blocked by existing spread filter
    }

    /**
     * MAIN METHOD — evaluate spread compression for the current tick.
     *
     * @param {number|null} currentSpread  current bid-ask spread
     * @param {number}      windowTs       current window timestamp
     * @returns {object}                   compression analysis result
     */
    evaluate(currentSpread, windowTs) {
        this.observe(currentSpread, windowTs);

        // No live orderbook
        if (currentSpread === null || currentSpread === undefined) {
            return this._result(false, null, null, null, 0, 'no_orderbook');
        }

        const velocity         = this.calculateVelocity(this.velocityWindow);
        const compressionDrop  = this.calculateCompressionDrop(this.compressionWindow);
        const qualityScore     = this.spreadQualityScore(currentSpread);

        // Need sufficient history to make a judgment
        if (velocity === null || compressionDrop === null) {
            return this._result(false, velocity, compressionDrop, qualityScore, this.spreadHistory.length, 'insufficient_history');
        }

        // Check all compression conditions
        const isCompressing    = velocity < 0;
        const droppedEnough    = compressionDrop >= this.minCompressionDrop;
        const startedWide      = this.openingSpread === null || this.openingSpread >= this.minOpeningSpread;
        const currentTradeable = currentSpread <= 0.08;  // mirrors EdgeCalculator maxSpread

        const confirmed = isCompressing && droppedEnough && startedWide && currentTradeable;

        const reason = !currentTradeable  ? `spread_too_wide(${(currentSpread*100).toFixed(1)}%)`
            : !startedWide      ? `opened_tight(${this.openingSpread ? (this.openingSpread*100).toFixed(1) : '?'}%)`
            : !droppedEnough    ? `drop_insufficient(${(compressionDrop*100).toFixed(1)}%<${(this.minCompressionDrop*100).toFixed(0)}%)`
            : !isCompressing    ? `widening(vel=${velocity >= 0 ? '+' : ''}${(velocity*100).toFixed(3)}%/s)`
            : 'compression_confirmed';

        return this._result(confirmed, velocity, compressionDrop, qualityScore, this.spreadHistory.length, reason);
    }

    _result(confirmed, velocity, compressionDrop, qualityScore, observations, reason) {
        return {
            compressionConfirmed: confirmed,
            spreadVelocity:       velocity,           // %/sec, negative = compressing
            compressionDrop:      compressionDrop,    // total drop from peak in window
            qualityScore:         qualityScore,        // 0-5, higher = better spread bucket
            observations:         observations,
            openingSpread:        this.openingSpread,
            reason,
            timestamp:            new Date().toISOString()
        };
    }

    reset() {
        this.spreadHistory   = [];
        this.openingSpread   = null;
        this.openingWindowTs = null;
        console.log('[SpreadModel] State reset for new window.');
    }
}

module.exports = SpreadModel;
