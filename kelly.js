/**
 * kelly.js — Model 5: Position Sizing
 *
 * WHAT THIS DOES:
 * Determines how much of the bankroll to stake on each signal.
 * Without this, every signal gets the same bet regardless of
 * whether the edge is 5% or 45% — that's leaving money on the table
 * and taking unnecessary risk on weak signals simultaneously.
 *
 * THE KELLY FORMULA:
 *   f* = (b*p - q) / b
 *
 *   f* = optimal fraction of bankroll to bet
 *   b  = net odds (how much you win per $1 risked)
 *   p  = probability of winning (our fair probability)
 *   q  = probability of losing (1 - p)
 *
 * POLYMARKET BINARY MARKET TRANSLATION:
 *   You buy a share at price X (e.g. $0.03 for DOWN at 3%)
 *   If correct, it pays $1.00
 *   Net odds b = (1 - X) / X  (profit per dollar risked)
 *
 *   Example: buying DOWN at market price 0.03
 *     b = (1 - 0.03) / 0.03 = 32.33
 *     p = our fair probability of DOWN = 1 - fairProbUp
 *     f* = (32.33 * 0.47 - 0.53) / 32.33 = 0.444 = 44.4%
 *
 * WHY WE USE FRACTIONAL KELLY (0.25x):
 *   Full Kelly maximizes long-run growth but causes huge swings.
 *   Quarter Kelly (25% of full Kelly) is much smoother and still
 *   captures most of the edge. Standard practice in algo trading.
 *   We cap at 20% of bankroll per trade regardless.
 *
 * WHAT IT RETURNS:
 *   stakeFraction: 0.0 - 0.20 (fraction of bankroll to bet)
 *   stakeAmount:   dollar amount based on current bankroll
 *   kellyFull:     full Kelly fraction (for reference)
 */

class KellySizer {
    constructor(config = {}) {
        // LIVE TRADING NOTE: Default to kellyFraction = 0.25 (quarter Kelly)
        // until 200+ resolved trades validate edge estimates.
        // Do not increase without formal review of win rate stability.
        this.kellyFraction = config.kellyFraction ?? 0.25;  // quarter Kelly
        this.maxStakePct   = config.maxStakePct   ?? 0.20;  // max 20% per trade
        this.minStakePct   = config.minStakePct   ?? 0.01;  // min 1% (floor)
        this.maxStakeAbs   = config.maxStakeAbs   ?? 4.00;  // hard dollar cap (~avg ask depth)
    }

    /**
     * Calculate position size for a signal.
     *
     * @param {number} fairProbWin    our model's probability of winning
     * @param {number} entryPrice     price we'd pay per share (0.0 - 1.0)
     * @param {number} bankroll       current paper balance in dollars
     * @returns {object}              sizing recommendation
     */
    size(fairProbWin, entryPrice, bankroll) {
        // Validate inputs
        if (fairProbWin <= 0 || fairProbWin >= 1) {
            return this._result(0, 0, 0, bankroll, 'invalid_probability');
        }
        if (entryPrice <= 0 || entryPrice >= 1) {
            return this._result(0, 0, 0, bankroll, 'invalid_price');
        }
        if (bankroll <= 0) {
            return this._result(0, 0, 0, bankroll, 'bankroll_empty');
        }

        const p = fairProbWin;
        const q = 1 - p;

        // Net odds: profit per dollar risked
        // Buy at entryPrice, win (1 - entryPrice), lose entryPrice
        const b = (1 - entryPrice) / entryPrice;

        // Full Kelly fraction
        const kellyFull = (b * p - q) / b;

        // If Kelly is negative, expected value is negative — don't bet
        if (kellyFull <= 0) {
            return this._result(0, 0, kellyFull, bankroll, 'negative_kelly');
        }

        // Apply fractional Kelly
        const kellyScaled = kellyFull * this.kellyFraction;

        // Apply min/max caps
        const stakeFraction = Math.max(
            this.minStakePct,
            Math.min(this.maxStakePct, kellyScaled)
        );

        const stakeAmount = Math.min(bankroll * stakeFraction, this.maxStakeAbs);

        // Number of shares we can buy
        const shares = stakeAmount / entryPrice;

        // Potential profit if correct
        const potentialProfit = shares * (1 - entryPrice);
        const potentialLoss   = stakeAmount;

        return this._result(stakeFraction, stakeAmount, kellyFull, bankroll, 'ok', {
            shares:          Math.floor(shares * 100) / 100,  // round down to 2dp
            entryPrice,
            potentialProfit: Math.round(potentialProfit * 100) / 100,
            potentialLoss:   Math.round(potentialLoss * 100) / 100,
            expectedValue:   Math.round((fairProbWin * potentialProfit - q * potentialLoss) * 100) / 100
        });
    }

    _result(stakeFraction, stakeAmount, kellyFull, bankroll, status, extra = {}) {
        return {
            stakeFraction,                                       // 0.0 - 0.20
            stakeAmount: Math.round(stakeAmount * 100) / 100,   // in dollars
            stakePercent: Math.round(stakeFraction * 100 * 10) / 10, // as %
            kellyFull: Math.round(kellyFull * 100 * 100) / 100, // full Kelly %
            bankroll,
            status,  // 'ok' | 'negative_kelly' | 'invalid_probability' | etc
            ...extra
        };
    }
}

module.exports = KellySizer;