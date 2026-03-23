// PATCH for loosening pure arbitrage thresholds to match the reference trader's firing rate

// Pure Arbitrage Scanner for BTC
// Previous thresholds:
// mid-gap detection: > 0.02
// net-gap profitability: > 0.01 after fees
// liquidity acceptance: < 0.10 spreads

// Updated thresholds:
// mid-gap detection: > 0.015
// net-gap profitability: > 0.005 after fees
// liquidity acceptance: < 0.18 spreads

if (midGap > 0.015 && netGapProfitability > 0.005 && liquiditySpread < 0.18) {
    // Execute arbitrage scan
}

// Add ARB-RAW diagnostic logging every 15 ticks
let tickCount = 0;
const logDiagnostics = () => {
    tickCount++;
    if (tickCount % 15 === 0) {
        console.log(`Ticks: ${tickCount}`);
        console.log(`Combined mids: ${combinedMids}`);
        console.log(`Gaps: ${gaps}`);
        console.log(`Spread Percentages: ${spreadPercentages}`);
        console.log(`Liquidity Check Status: ${liquidityCheckStatus}`);
    }
};

// Call the logging function every tick
setInterval(logDiagnostics, tickDuration);