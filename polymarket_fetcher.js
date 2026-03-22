/**
 * polymarket_fetcher.js (v4)
 *
 * CHANGES IN THIS VERSION:
 * - Fetch BOTH token orderbooks (UP + DOWN) in parallel.
 *   Prior versions only fetched the UP token, inferring priceDown = 1 - priceUp
 *   and using UP book spread/liquidity for ALL trade directions.
 *   This meant UP direction trades always failed has_orderbook when the UP book
 *   was thin at mid-range prices (~58¢), despite genuine edge.
 *   Now: UP trades use UP book, DOWN trades use DOWN book.
 *
 * RETAINED FROM v3:
 * - CRITICAL: Fixed bid array reading (bids sorted ASCENDING, best bid = last)
 * - Liquidity quality scoring (tradeable / marginal / illiquid)
 * - Gamma fallback for stale prices
 */
 
const fetch = require('node-fetch');
 
class PolymarketFetcher {
    /**
     * @param {string} asset  'btc' | 'eth' — determines slug prefix and endpoints
     */
    constructor(asset = 'btc') {
        this.asset               = asset.toLowerCase();   // 'btc' or 'eth'
        this.cachedWindowTs      = null;
        this.cachedMarketMeta    = null;
        this.priceCache          = null;
        this.priceCacheTs        = 0;
        this.priceCacheTTL       = 1000;
        this.consecutiveFailures = 0;
        this.maxFailures         = 5;
    }
 
    /**
     * Calculate current 5-minute window from system clock.
     * No API call needed — slug is pure math.
     */
    getCurrentWindow() {
        const nowSeconds       = Math.floor(Date.now() / 1000);
        const windowTs         = Math.floor(nowSeconds / 300) * 300;
        const windowEndTs      = windowTs + 300;
        const secondsRemaining = windowEndTs - nowSeconds;
        const slug             = `${this.asset}-updown-5m-${windowTs}`;
        return { windowTs, slug, windowEndTs, secondsRemaining };
    }
 
    getNextWindow() {
        const { windowTs } = this.getCurrentWindow();
        const nextTs = windowTs + 300;
        return { windowTs: nextTs, slug: `${this.asset}-updown-5m-${nextTs}` };
    }
 
    /**
     * Fetch market metadata (token IDs) from Gamma API.
     * Cached per window — only re-fetches when window rolls over.
     *
     * NOTE: Gamma's outcomePrices are stale snapshots, not live.
     * We use them only as a fallback when CLOB orderbook is empty
     * (e.g. first/last few seconds of a window).
     */
    async fetchMarketMeta(slug, windowTs) {
        if (this.cachedWindowTs === windowTs && this.cachedMarketMeta) {
            return this.cachedMarketMeta;
        }
 
        try {
            const url      = `https://gamma-api.polymarket.com/events?slug=${slug}`;
            const response = await fetch(url, {
                headers: { 'Accept': 'application/json' },
                timeout: 8000
            });
 
            if (!response.ok) throw new Error(`Gamma API returned HTTP ${response.status}`);
 
            const events = await response.json();
            const event  = Array.isArray(events)
                ? (events.find(e => e.slug === slug) || events[0])
                : events;
 
            if (!event) throw new Error(`No event found for slug: ${slug}`);
 
            const market = event.markets?.[0] || event;
 
            let tokenIds = market.clobTokenIds;
            if (typeof tokenIds === 'string') tokenIds = JSON.parse(tokenIds);
            if (!tokenIds || tokenIds.length < 2) {
                throw new Error(`Could not parse clobTokenIds from: ${JSON.stringify(market).slice(0, 200)}`);
            }
 
            let outcomePrices = market.outcomePrices;
            if (typeof outcomePrices === 'string') outcomePrices = JSON.parse(outcomePrices);
 
            const meta = {
                slug, windowTs,
                eventId:   event.id,
                marketId:  market.id,
                tokenUp:   tokenIds[0],
                tokenDown: tokenIds[1],
                // Stale snapshot prices — fallback only, not for arb decisions
                priceUpStale:   outcomePrices ? parseFloat(outcomePrices[0]) : null,
                priceDownStale: outcomePrices ? parseFloat(outcomePrices[1]) : null,
                endDate:   market.endDate || event.endDate,
                active:    market.active,
                closed:    market.closed
            };
 
            console.log(`[Polymarket] Window:     ${slug}`);
            console.log(`[Polymarket] Token UP:   ${meta.tokenUp}`);
            console.log(`[Polymarket] Token DOWN: ${meta.tokenDown}`);
 
            this.cachedWindowTs   = windowTs;
            this.cachedMarketMeta = meta;
            return meta;
 
        } catch (error) {
            console.error(`[Polymarket] fetchMarketMeta failed for ${slug}:`, error.message);
            return this.cachedMarketMeta || null;
        }
    }
 
    /**
     * Fetch live orderbook from CLOB API.
     *
     * FIX: Polymarket sorts bids ASCENDING (lowest price first).
     *   WRONG: bids[0]              → worst bid (0.01) → fake 0.98 spread
     *   RIGHT: bids[bids.length-1]  → best bid  (0.54) → real spread
     *
     * Asks are sorted ASCENDING too, so asks[0] = best ask (correct).
     *
     * Also calculates a liquidity score:
     *   'tradeable'  — spread < 0.08, depth > 5 on both sides
     *   'marginal'   — spread 0.08–0.20, or thin depth
     *   'illiquid'   — spread > 0.20, or nearly empty book
     */
    async fetchOrderbook(tokenId) {
        try {
            const url      = `https://clob.polymarket.com/book?token_id=${tokenId}`;
            const response = await fetch(url, {
                headers: { 'Accept': 'application/json' },
                timeout: 5000
            });
 
            if (!response.ok) throw new Error(`CLOB API returned HTTP ${response.status}`);
 
            const book = await response.json();
 
            if (!book.bids?.length || !book.asks?.length) {
                throw new Error('Empty orderbook — window just opened or closing');
            }
 
            // ── THE FIX ──────────────────────────────────────────────────────
            // Polymarket bids are sorted ASCENDING (worst → best)
            // So best bid is the LAST element, not the first
            const bestBid = parseFloat(book.bids[book.bids.length - 1].price);
            // Asks are sorted ASCENDING too — best ask is the FIRST element
            const bestAsk = parseFloat(book.asks[0].price);
            // ─────────────────────────────────────────────────────────────────
 
            if (isNaN(bestBid) || isNaN(bestAsk)) {
                throw new Error(`Non-numeric bid/ask: bid=${book.bids[book.bids.length-1].price} ask=${book.asks[0].price}`);
            }
 
            if (bestBid >= bestAsk) {
                // Crossed book — shouldn't happen but handle gracefully
                console.warn(`[Polymarket] Crossed book: bid ${bestBid} >= ask ${bestAsk} — using mid only`);
            }
 
            const midPrice = (bestBid + bestAsk) / 2;
            const spread   = bestAsk - bestBid;
 
            // Liquidity quality assessment
            const bidDepth = book.bids.length;
            const askDepth = book.asks.length;
            let liquidity;
            if (spread < 0.08 && bidDepth >= 5 && askDepth >= 5) {
                liquidity = 'tradeable';
            } else if (spread < 0.20 || (bidDepth >= 2 && askDepth >= 2)) {
                liquidity = 'marginal';
            } else {
                liquidity = 'illiquid';
            }
 
            return {
                priceUp:   midPrice,
                priceDown: 1 - midPrice,
                bestBid,
                bestAsk,
                spread,
                bidDepth,
                askDepth,
                liquidity  // 'tradeable' | 'marginal' | 'illiquid'
            };
 
        } catch (error) {
            console.error(`[Polymarket] fetchOrderbook failed:`, error.message);
            return null;
        }
    }
 
    /**
     * MAIN METHOD — call this every tick.
     *
     * Returns null if no price available.
     * Returns result with liquidity='illiquid' if spread too wide to trade.
     * The arb engine should check liquidity before evaluating any trade.
     */
    async fetchMarketPrice() {
        const now = Date.now();
 
        if (this.priceCache && (now - this.priceCacheTs) < this.priceCacheTTL) {
            return this.priceCache;
        }
 
        if (this.consecutiveFailures >= this.maxFailures) {
            console.warn('[Polymarket] Circuit breaker open.');
            return this.priceCache || null;
        }
 
        try {
            const window    = this.getCurrentWindow();
            const meta      = await this.fetchMarketMeta(window.slug, window.windowTs);
            if (!meta) throw new Error('Could not get market metadata');
 
            // Fetch both token orderbooks in parallel — UP for UP trades, DOWN for DOWN trades
            const [upBook, downBook] = await Promise.all([
                this.fetchOrderbook(meta.tokenUp),
                this.fetchOrderbook(meta.tokenDown)
            ]);

            // Price: prefer direct measurement per token, fall back to inference, then stale Gamma
            // upBook.priceUp   = UP token mid price
            // downBook.priceUp = DOWN token mid price (fetchOrderbook always returns priceUp=mid)
            //
            // RELIABILITY CHECK: if a book's spread > 50%, it has no real market
            // (typical when bid≈0, ask≈1, mid≈0.5 — meaningless phantom price).
            // In that case, derive the price from the other book instead.
            const upBookReliable   = upBook   && upBook.spread   !== null && upBook.spread   <= 0.50;
            const downBookReliable = downBook && downBook.spread !== null && downBook.spread <= 0.50;
            const priceUp   = upBookReliable
                ? upBook.priceUp
                : (downBookReliable ? (1 - downBook.priceUp) : (upBook?.priceUp ?? meta.priceUpStale));
            const priceDown = downBookReliable
                ? downBook.priceUp
                : (upBookReliable ? (1 - upBook.priceUp) : (downBook?.priceUp ?? meta.priceDownStale));

            if (priceUp === null && priceDown === null) throw new Error('No price available from any source');

            const result = {
                slug:             window.slug,
                windowTs:         window.windowTs,
                secondsRemaining: window.secondsRemaining,
                tokenUp:          meta.tokenUp,
                tokenDown:        meta.tokenDown,
                priceUp,
                priceDown,
                // UP token book — used for UP direction trades
                bestBid:       upBook?.bestBid    ?? null,
                bestAsk:       upBook?.bestAsk    ?? null,
                spread:        upBook?.spread     ?? null,
                bidDepth:      upBook?.bidDepth   ?? 0,
                askDepth:      upBook?.askDepth   ?? 0,
                liquidity:     upBook?.liquidity  ?? 'illiquid',
                // Raw mids from each book independently (before reliability override)
                // These may NOT sum to 1.00 — the gap is the arb signal
                upRawMid:      upBook?.priceUp     ?? null,
                downRawMid:    downBook?.priceUp   ?? null,
                // DOWN token book — used for DOWN direction trades
                downBestAsk:   downBook?.bestAsk   ?? null,
                downSpread:    downBook?.spread    ?? null,
                downBidDepth:  downBook?.bidDepth  ?? 0,
                downAskDepth:  downBook?.askDepth  ?? 0,
                downLiquidity: downBook?.liquidity ?? 'illiquid',
                source:    (upBook || downBook) ? 'clob' : 'gamma-stale',
                timestamp: new Date().toISOString()
            };
 
            this.priceCache          = result;
            this.priceCacheTs        = now;
            this.consecutiveFailures = 0;
            return result;
 
        } catch (error) {
            this.consecutiveFailures++;
            console.error(`[Polymarket] fetchMarketPrice failed (${this.consecutiveFailures}/${this.maxFailures}):`, error.message);
            return this.priceCache ? { ...this.priceCache, stale: true } : null;
        }
    }
 
    invalidateMarketCache() {
        this.cachedWindowTs   = null;
        this.cachedMarketMeta = null;
        console.log('[Polymarket] Market cache cleared.');
    }
}
 
module.exports = PolymarketFetcher;