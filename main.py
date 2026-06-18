import asyncio
import json
import time
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import websockets

SPOT_PAIRS = ['btcusdt', 'btcusdc', 'btcfdusd', 'btctry', 'btcars', 'btcbrl', 'btceur', 'btcgbp']
PERP_PAIRS = ['btcusdt', 'btcusdc', 'btcbusd', 'btceur', 'btctry', 'btcbrl', 'btcjpy', 'btcaud']

VENUE_WEIGHTS = {
    "btcusdt_spot": 0.45, "btcusdc_spot": 0.20, "btcfdusd_spot": 0.15, "btctry_spot": 0.05,
    "btcars_spot": 0.03,  "btcbrl_spot": 0.04,  "btceur_spot": 0.05,  "btcgbp_spot": 0.03,
    "btcusdt_perp": 0.50, "btcusdc_perp": 0.15, "btcbusd_perp": 0.10, "btceur_perp": 0.08,
    "btctry_perp": 0.05,  "btcbrl_perp": 0.04,  "btcjpy_perp": 0.04,  "btcaud_perp": 0.04
}

live_market_matrix = {}
current_candle = {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0}
cvd_metrics = {"rolling_delta": 0.0, "last_reset": time.time()}
liquidation_metrics = {"shorts_notional": 0.0, "longs_notional": 0.0, "last_reset": time.time()}

def init_matrix():
    for pair in SPOT_PAIRS:
        live_market_matrix[f"{pair}_spot"] = {"micro_price": 0.0, "status": "OFFLINE", "timestamp": 0.0}
    for pair in PERP_PAIRS:
        live_market_matrix[f"{pair}_perp"] = {"micro_price": 0.0, "status": "OFFLINE", "timestamp": 0.0}

init_matrix()

async def stream_binance_spot():
    streams = "/".join([f"{p}@bookTicker" for p in SPOT_PAIRS])
    uri = f"wss://://binance.com{streams}"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                async for message in ws:
                    data = json.loads(message)
                    pair = data['s'].lower()
                    venue_id = f"{pair}_spot"
                    if venue_id in live_market_matrix:
                        bid, ask = float(data['b']), float(data['a'])
                        bid_sz, ask_sz = float(data['B']), float(data['A'])
                        u_price = ((bid * ask_sz) + (ask * bid_sz)) / (bid_sz + ask_sz) if (bid_sz + ask_sz) > 0 else (bid + ask) / 2
                        live_market_matrix[venue_id] = {"micro_price": u_price, "status": "ONLINE", "timestamp": time.time()}
        except Exception:
            await asyncio.sleep(2)

async def stream_binance_perp():
    streams = "/".join([f"{p}@bookTicker/{p}@aggTrade" for p in PERP_PAIRS])
    uri = f"wss://://binance.com{streams}"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                async for message in ws:
                    data = json.loads(message)
                    if "e" in data and data["e"] == "aggTrade":
                        qty, is_buyer_maker = float(data["q"]), data["m"]
                        cvd_metrics["rolling_delta"] += -qty if is_buyer_maker else qty
                        continue
                    if "s" in data:
                        pair = data['s'].lower()
                        venue_id = f"{pair}_perp"
                        if venue_id in live_market_matrix:
                            bid, ask = float(data['b']), float(data['a'])
                            bid_sz, ask_sz = float(data['B']), float(data['A'])
                            u_price = ((bid * ask_sz) + (ask * bid_sz)) / (bid_sz + ask_sz) if (bid_sz + ask_sz) > 0 else (bid + ask) / 2
                            live_market_matrix[venue_id] = {"micro_price": u_price, "status": "ONLINE", "timestamp": time.time()}
                            if pair == 'btcusdt':
                                if current_candle["open"] == 0.0:
                                    current_candle.update({"open": u_price, "high": u_price, "low": u_price, "close": u_price})
                                else:
                                    current_candle["high"] = max(current_candle["high"], u_price)
                                    current_candle["low"] = min(current_candle["low"], u_price)
                                    current_candle["close"] = u_price
        except Exception:
            await asyncio.sleep(2)

async def stream_binance_liquidations():
    uri = "wss://://binance.com!forceOrder@arr"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                async for message in ws:
                    data = json.loads(message)
                    if "o" in data and data["o"]["s"].lower() in PERP_PAIRS:
                        order = data["o"]
                        if order["S"] == "BUY":
                            liquidation_metrics["shorts_notional"] += (float(order["q"]) * float(order["p"]))
                        else:
                            liquidation_metrics["longs_notional"] += (float(order["q"]) * float(order["p"]))
        except Exception:
            await asyncio.sleep(2)

@asynccontextmanager
async def lifespan(app: FastAPI):
    spot_task = asyncio.create_task(stream_binance_spot())
    perp_task = asyncio.create_task(stream_binance_perp())
    liq_task  = asyncio.create_task(stream_binance_liquidations())
    yield
    spot_task.cancel()
    perp_task.cancel()
    liq_task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def get_dashboard():
    html_path = "dashboard.html"
    if os.path.exists(html_path):
        with open(html_path, "r", encoding="utf-8") as file:
            return HTMLResponse(content=file.read())
    return HTMLResponse(content="<h1>Error loading dashboard file.</h1>")

# --- THE STABLE HTTP DATA RECOVERY LINK ---
@app.get("/data")
async def get_market_data():
    now = time.time()
    weighted_spot_sum, total_spot_weight = 0.0, 0.0
    weighted_perp_sum, total_perp_weight = 0.0, 0.0
    weighted_all_sum, total_all_weight = 0.0, 0.0
    
    for v, node in live_market_matrix.items():
        if node["status"] == "ONLINE" and (now - node["timestamp"]) > 6.0:
            node["status"] = "OFFLINE"
        
        if node["status"] == "ONLINE" and node["micro_price"] > 0:
            weight = VENUE_WEIGHTS.get(v, 0.01)
            weighted_all_sum += (node["micro_price"] * weight)
            total_all_weight += weight
            
            if "_spot" in v:
                weighted_spot_sum += (node["micro_price"] * weight)
                total_spot_weight += weight
            else:
                weighted_perp_sum += (node["micro_price"] * weight)
                total_perp_weight += weight

    ref_price = current_candle["close"] if current_candle["close"] > 0 else 95000.0
    rv_depth = (weighted_spot_sum / total_spot_weight) if total_spot_weight > 0 else ref_price
    rv_vol   = (weighted_all_sum / total_all_weight) if total_all_weight > 0 else ref_price
    rv_oi    = (weighted_perp_sum / total_perp_weight) if total_perp_weight > 0 else ref_price
    
    perp_to_spot_spread = rv_oi - rv_depth

    payload = {
        "time": int(now),
        "candle": {
            "open": current_candle["open"] if current_candle["open"] > 0 else ref_price,
            "high": current_candle["high"] if current_candle["high"] > 0 else ref_price,
            "low": current_candle["low"] if current_candle["low"] > 0 else ref_price,
            "close": ref_price
        },
        "rv_depth": rv_depth,
        "rv_vol": rv_vol,
        "rv_oi": rv_oi,
        "perp_spot_spread": perp_to_spot_spread,
        "cvd": cvd_metrics["rolling_delta"],
        "liq_shorts": liquidation_metrics["shorts_notional"],
        "liq_longs": liquidation_metrics["longs_notional"],
        "venue_matrix": live_market_matrix
    }
    
    if now - cvd_metrics["last_reset"] > 60.0:
        cvd_metrics.update({"rolling_delta": 0.0, "last_reset": now})
    if now - liquidation_metrics["last_reset"] > 60.0:
        liquidation_metrics.update({"shorts_notional": 0.0, "longs_notional": 0.0, "last_reset": now})
        
    current_candle.update({"open": ref_price, "high": ref_price, "low": ref_price, "close": ref_price})
    return payload

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=10000)

