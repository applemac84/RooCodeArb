/**
 * edge_calculator.js (v2)
 *
 * CHANGE FROM v1:
 * Added 'has_orderbook' filter — if spread is null (empty CLOB orderbook,
 * fell back to stale Gamma price), we immediately block the trade.
 * The stale Gamma price was generating fake 30-35% edge signals near
 * end of windows when the orderbook drained. Those signals were noise.
 *
 * ENTRY CRITERIA (conservative paper trading):
 *   - Net edge    > 5%
 *   - Spread      < 8%   AND spread is not null
 *   - Seconds     45–270
 *   - Liquidity   not illiquid
 *   - Has live orderbook (spread !== null)
 */

class EdgeCalculator {
    constructor(config = {}) {
        this.takerFee      = config.takerFee      ?? 0.010;
        this.slippageCost  = config.slippageCost  ?? 0.005;
        this.executionRisk = config.executionRisk ?? 0.005;
        this.minNetEdge    = config.minNetEdge    ?? 0.20;  // 20% min — data shows 0 wins on 25 trades below 20%, 27% win rate above it
        this.maxSpread     = config.maxSpread     ?? 0.08;
        this.minSeconds    = config.minSeconds    ?? 15;  // widened from 45 — overnight data shows timing blocked 60% of missed wins
        this.maxSeconds    = config.maxSeconds    ?? 270;
    }

    calculateCosts(spread) {
        const spreadCost = (spread ?? 0) / 2;
        return this.takerFee + spreadCost + this.slippageCost + this.executionRisk;
    }

    evaluate(bayesianResult, marketData) {
        const q       = bayesianResult.fairProbUp;
        const p       = marketData.priceUp;
        const seconds = marketData.secondsRemaining;
        const source  = marketData.source ?? 'unknown';

        // Determine direction first so we can pick the correct book's spread/liquidity
        const rawEdge   = q - p;
        const direction = rawEdge >= 0 ? 'UP' : 'DOWN';

        // Use direction-appropriate orderbook data for both cost calculation and filter checks.
        // DOWN trades use the DOWN token book (direct measurement).
        // Falls back to UP book data if DOWN book unavailable.
        const spread    = direction === 'UP'
            ? marketData.spread
            : (marketData.downSpread    ?? marketData.spread);
        const liquidity = direction === 'UP'
            ? (marketData.liquidity    ?? 'unknown')
            : (marketData.downLiquidity ?? marketData.liquidity ?? 'unknown');

        const totalCost        = this.calculateCosts(spread);
        const effectiveRawEdge = Math.abs(rawEdge);
        const effectiveNetEdge = effectiveRawEdge - totalCost;

        const filters   = this._checkFilters(spread, seconds, liquidity, effectiveNetEdge, source);
        const tradeable = filters.every(f => f.pass);

        return {
            fairProbUp:    q,
            marketProbUp:  p,
            rawEdge,
            netEdge:       effectiveNetEdge,
            direction,
            costs: {
                takerFee:      this.takerFee,
                spreadCost:    (spread ?? 0) / 2,
                slippage:      this.slippageCost,
                executionRisk: this.executionRisk,
                total:         totalCost
            },
            spread,
            secondsRemaining: seconds,
            liquidity,
            tradeable,
            filters,
            openingPrice:        bayesianResult.openingPrice,
            currentBTC:          bayesianResult.currentPrice,
            distanceFromStrike:  bayesianResult.distanceFromStrike,
            timestamp: new Date().toISOString()
        };
    }

    _checkFilters(spread, seconds, liquidity, netEdge, source) {
        return [
            // NEW: require a live orderbook — null spread = stale Gamma fallback
            {
                name:     'has_orderbook',
                pass:     spread !== null && spread !== undefined,
                value:    spread !== null ? spread.toFixed(4) : 'null',
                required: 'live CLOB price',
                reason:   spread !== null
                    ? 'Live orderbook confirmed'
                    : 'No live orderbook — Gamma fallback price, not tradeable'
            },
            {
                name:     'net_edge',
                pass:     netEdge >= this.minNetEdge,
                value:    `${(netEdge * 100).toFixed(2)}%`,
                required: `>= ${(this.minNetEdge * 100).toFixed(0)}%`,
                reason:   netEdge >= this.minNetEdge
                    ? 'Edge sufficient'
                    : `Edge ${(netEdge*100).toFixed(2)}% below ${(this.minNetEdge*100).toFixed(0)}% minimum`
            },
            {
                name:     'spread',
                pass:     spread !== null && spread <= this.maxSpread,
                value:    spread !== null ? `${(spread*100).toFixed(1)}%` : 'N/A',
                required: `<= ${(this.maxSpread*100).toFixed(0)}%`,
                reason:   spread === null
                    ? 'No spread data'
                    : spread <= this.maxSpread
                    ? 'Spread acceptable'
                    : `Spread ${(spread*100).toFixed(1)}% too wide`
            },
            {
                name:     'time_remaining',
                pass:     seconds >= this.minSeconds && seconds <= this.maxSeconds,
                value:    `${seconds}s`,
                required: `${this.minSeconds}s–${this.maxSeconds}s`,
                reason:   seconds < this.minSeconds
                    ? `Too late: ${seconds}s (min ${this.minSeconds}s)`
                    : seconds > this.maxSeconds
                    ? `Too early: ${seconds}s (max ${this.maxSeconds}s)`
                    : 'Timing good'
            },
            {
                name:     'liquidity',
                pass:     liquidity !== 'illiquid',
                value:    liquidity,
                required: 'tradeable or marginal',
                reason:   liquidity !== 'illiquid'
                    ? 'Liquidity acceptable'
                    : 'Orderbook too thin'
            }
        ];
    }

    summary(e) {
        return [
            `📊 Edge Evaluation`,
            `   Fair prob:  ${(e.fairProbUp*100).toFixed(1)}% UP`,
            `   Market:     ${(e.marketProbUp*100).toFixed(1)}% UP`,
            `   Raw edge:   ${(e.rawEdge*100).toFixed(2)}%`,
            `   Costs:      ${(e.costs.total*100).toFixed(2)}%`,
            `   Net edge:   ${(e.netEdge*100).toFixed(2)}%`,
            `   Direction:  ${e.direction}`,
            `   Spread:     ${e.spread !== null ? (e.spread*100).toFixed(1)+'%' : 'N/A'}`,
            `   Time left:  ${e.secondsRemaining}s`,
            `   BTC vs K:   ${e.distanceFromStrike >= 0 ? '+' : ''}${e.distanceFromStrike.toFixed(3)}%`,
            `   Decision:   ${e.tradeable ? '✅ ENTER' : '⏸  SKIP'}`,
            ...(e.tradeable ? [] : [`   Blocked:    ${e.filters.filter(f=>!f.pass).map(f=>f.reason).join(' | ')}`])
        ].join('\n');
    }
}

module.exports = EdgeCalculator;