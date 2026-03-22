/**
 * bayesian.js (v2 — calibrated)
 *
 * CHANGES FROM v1:
 * 1. minVolatility raised from 0.0003 → 0.002
 *    (0.2% is realistic for BTC over a 5-minute window)
 *    The old floor was 10x too low, causing tiny price moves to produce
 *    extreme probabilities (98%) via a very small denominator.
 *
 * 2. Velocity dampening added
 *    Short bursts of velocity (observed over <30s) are dampened to 20%.
 *    Once we have 30+ observations (~60s of data), velocity gets full weight.
 *    Rationale: a 10-second price spike does not reliably predict where
 *    BTC will be in 4 minutes. Mean reversion is real.
 *
 * 3. Probability bounds tightened to 0.03–0.97
 *    Markets on Polymarket never trade at 0% or 100% until resolved.
 *    Capping prevents the model from being certain when it shouldn't be.
 *
 * THE MODEL (unchanged):
 *   P(UP) = N(d)
 *   d = [ln(S/K) + (μ_eff - σ²/2) * T] / (σ * √T)
 *
 *   S    = current BTC price
 *   K    = opening BTC price (the "price to beat")
 *   T    = time remaining as fraction of 5-minute window
 *   μ_eff = dampened drift (velocity × damping factor)
 *   σ    = realized volatility from price history
 */

class BayesianEstimator {
    constructor() {
        // ── Volatility parameters (calibrated for BTC 5-min) ──────────────────
        this.defaultVolatility = 0.0020;  // 0.20% prior — realistic for BTC 5-min
        this.minVolatility     = 0.0020;  // floor — never go below 0.20%
        this.maxVolatility     = 0.0100;  // ceiling — cap at 1.0%

        // ── Velocity dampening ────────────────────────────────────────────────
        // Observed velocity is dampened until we have enough price history
        // to trust it. Short bursts of movement are weighted much less.
        this.minObservationsForFullVelocity = 30;   // ~60s at 2s polling
        this.velocityDampeningFloor         = 0.20; // 20% weight with few observations
        this.velocityDampeningCeiling       = 0.60; // 60% max weight (never full trust)

        // ── Per-window state ──────────────────────────────────────────────────
        this.openingPrice    = null;
        this.openingWindowTs = null;
        this.priceHistory    = [];
        this.maxHistory      = 90;   // keep 90 observations (~3 min at 2s polling)
        this.lastEstimate    = null;
    }

    /**
     * Record the opening price when a new window starts.
     * K = the "price to beat" — BTC must end AT or ABOVE this for UP to resolve.
     */
    recordOpeningPrice(price, windowTs, chainlinkAgeMs) {
        if (this.openingWindowTs !== windowTs) {
            if (chainlinkAgeMs > 300000) {  // 5 minutes
                console.log(`[K-Guard] Skipped stale first tick (age ${chainlinkAgeMs}ms) — window ${windowTs}`);
                return false; // K not set
            }
            this.openingPrice    = price;
            this.openingWindowTs = windowTs;
            this.priceHistory    = [{ price, timestamp: Date.now() }];
            console.log(`[Bayesian] New window ${windowTs}. Fresh K=${price.toFixed(2)} (age ${chainlinkAgeMs}ms)`);
        }
        return true;
    }

    /**
     * Add a price observation to rolling history.
     * Called every tick — more observations = better volatility estimate.
     */
    addPriceObservation(price) {
        this.priceHistory.push({ price, timestamp: Date.now() });
        if (this.priceHistory.length > this.maxHistory) {
            this.priceHistory.shift();
        }
    }

    /**
     * Calculate realized volatility from price history.
     * Uses log returns, scaled to the 5-minute window duration.
     */
    calculateVolatility() {
        if (this.priceHistory.length < 3) return this.defaultVolatility;

        const logReturns = [];
        for (let i = 1; i < this.priceHistory.length; i++) {
            const prev = this.priceHistory[i - 1].price;
            const curr = this.priceHistory[i].price;
            if (prev > 0) logReturns.push(Math.log(curr / prev));
        }

        if (logReturns.length < 2) return this.defaultVolatility;

        const mean     = logReturns.reduce((s, r) => s + r, 0) / logReturns.length;
        const variance = logReturns.reduce((s, r) => s + Math.pow(r - mean, 2), 0) / logReturns.length;
        const stdDev   = Math.sqrt(variance);

        const timeSpanSeconds = (
            this.priceHistory[this.priceHistory.length - 1].timestamp -
            this.priceHistory[0].timestamp
        ) / 1000;

        if (timeSpanSeconds < 1) return this.defaultVolatility;

        // Scale to full 300-second window
        const scaledVol = stdDev * Math.sqrt(300 / (timeSpanSeconds / logReturns.length));

        return Math.max(this.minVolatility, Math.min(this.maxVolatility, scaledVol));
    }

    /**
     * Calculate dampened drift from velocity.
     *
     * WHY WE DAMPEN:
     * A $14 move over 10 seconds does not mean BTC will keep moving that
     * direction for another 290 seconds. Short-term velocity is noisy.
     * We apply a damping factor that increases as we accumulate more
     * price history — rewarding patience and penalizing early overconfidence.
     *
     * With 5 observations:   damping = 20%  (barely use velocity)
     * With 30 observations:  damping = 60%  (moderate trust)
     * With 60+ observations: damping = 60%  (capped — never fully trust)
     */
    calculateDampedDrift(velocityPerSec, currentPrice) {
        if (!velocityPerSec || !currentPrice || currentPrice <= 0) return 0;

        const n           = this.priceHistory.length;
        const rawDrift    = velocityPerSec / currentPrice;

        // Linear interpolation between floor and ceiling based on observations
        const dampingFactor = Math.min(
            this.velocityDampeningCeiling,
            this.velocityDampeningFloor +
            (this.velocityDampeningCeiling - this.velocityDampeningFloor) *
            (n / this.minObservationsForFullVelocity)
        );

        return rawDrift * dampingFactor;
    }

    /**
     * Cumulative Normal Distribution — Abramowitz & Stegun approximation.
     * Accurate to 7.5e-8. No library needed.
     */
    normalCDF(x) {
        const a1 =  0.254829592, a2 = -0.284496736, a3 = 1.421413741;
        const a4 = -1.453152027, a5 =  1.061405429, p  = 0.3275911;
        const sign = x < 0 ? -1 : 1;
        x = Math.abs(x) / Math.sqrt(2);
        const t = 1.0 / (1.0 + p * x);
        const y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t * Math.exp(-x*x);
        return 0.5 * (1.0 + sign * y);
    }

    /**
     * MAIN METHOD — estimate fair probability of UP this tick.
     *
     * @param {number} currentPrice      S — current BTC spot price
     * @param {number} secondsRemaining  time left in this 5-min window
     * @param {number} velocityPerSec    BTC $/sec from price fetcher
     * @param {number} windowTs          current window timestamp
     */
    estimate(currentPrice, secondsRemaining, velocityPerSec, windowTs, chainlinkAgeMs) {
        if (this.openingWindowTs !== windowTs || this.openingPrice === null) {
            if (chainlinkAgeMs === undefined || chainlinkAgeMs === null) {
                console.warn(`[Bayesian] chainlinkAgeMs undefined for window ${windowTs} — treating as fresh`);
            }
            const ok = this.recordOpeningPrice(currentPrice, windowTs, chainlinkAgeMs ?? 0);
            if (!ok || this.openingPrice === null) {
                return null; // caller will skip this tick
            }
        }
        this.addPriceObservation(currentPrice);

        const K = this.openingPrice;
        const S = currentPrice;

        // Expired window
        if (!K || K <= 0 || secondsRemaining <= 0) {
            const prob = S >= K ? 0.97 : 0.03;
            return this._result(prob, K, S, secondsRemaining, 0, 0, 'expired');
        }

        // T = fraction of 5-min window remaining (minimum 1 second)
        const T     = Math.max(1, secondsRemaining) / 300;
        const sigma = this.calculateVolatility();
        const mu    = this.calculateDampedDrift(velocityPerSec, S);

        // Price exactly at strike
        if (S === K) {
            const driftAdj = mu * T * 5;
            const prob     = Math.max(0.03, Math.min(0.97, 0.5 + driftAdj));
            return this._result(prob, K, S, secondsRemaining, sigma, mu, 'at-strike');
        }

        // LOG-NORMAL BINARY OPTION FORMULA
        const lnRatio  = Math.log(S / K);
        const driftAdj = (mu - (sigma * sigma) / 2) * T;
        const denom    = sigma * Math.sqrt(T);

        if (denom === 0) {
            const prob = S >= K ? 0.97 : 0.03;
            return this._result(prob, K, S, secondsRemaining, sigma, mu, 'zero-vol');
        }

        const d    = (lnRatio + driftAdj) / denom;
        const prob = Math.max(0.03, Math.min(0.97, this.normalCDF(d)));

        this.lastEstimate = this._result(prob, K, S, secondsRemaining, sigma, mu, 'model');
        return this.lastEstimate;
    }

    _result(prob, K, S, secondsRemaining, sigma, mu, method) {
        return {
            fairProbUp:         prob,
            fairProbDown:       1 - prob,
            openingPrice:       K,
            currentPrice:       S,
            distanceFromStrike: K > 0 ? ((S - K) / K) * 100 : 0,
            secondsRemaining,
            volatility:         sigma,
            drift:              mu,
            observations:       this.priceHistory.length,
            method,
            timestamp:          new Date().toISOString()
        };
    }

    reset() {
        this.openingPrice    = null;
        this.openingWindowTs = null;
        this.priceHistory    = [];
        this.lastEstimate    = null;
        console.log('[Bayesian] State reset for new window.');
    }
}

module.exports = BayesianEstimator;