#!/usr/bin/env python3
"""
Binance Futures Scanner — Streamlit Dashboard
Deploy free at share.streamlit.io  (connect GitHub repo, select this file)
"""

import json, os, pathlib, threading, time, uuid, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import requests
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

# ─────────────────────────────────────────────────────────────────────────────
# Network constants  (OKX — cloud-friendly, no datacenter IP blocks)
# ─────────────────────────────────────────────────────────────────────────────
BASE         = "https://www.okx.com"
LOOKBACK_30M = 300    # OKX max 300 per request; 300 × 30m = 6.25 days, plenty for avg vol

# OKX interval strings
OKX_INTERVALS = {"30m": "30m", "5m": "5m", "15m": "15m", "1h": "1H"}

# ─────────────────────────────────────────────────────────────────────────────
# Default configuration
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG: dict = {
    "tp_pct":             2.0,
    "sl_pct":             1.0,
    "path_a_vol_min":     2.0,
    "lookback_candles":   8,
    "path_b_vol_min":     1.8,
    "path_b_gain_min":    0.8,
    "move_guard_pct":     3.5,
    "body_ratio_min":     0.40,
    "rsi_5m_min":         45,
    "resistance_tol_pct": 1.5,
    "rsi_1h_min":         40,
    "rsi_1h_max":         72,
    "loop_minutes":       3,
    "cooldown_minutes":   30,
    "watchlist": [
        "PTBUSDT","SANTOSUSDT","XRPUSDT","HEMIUSDT","OGUSDT","SIRENUSDT",
        "BANUSDT","BASUSDT","4USDT","MAGMAUSDT","XANUSDT","TRIAUSDT",
        "JELLYJELLYUSDT","STABLEUSDT","ROBOUSDT","POWERUSDT","XNYUSDT",
        "1000RATSUSDT","BEATUSDT","QUSDT","LYNUSDT","RIVERUSDT","RAVEUSDT",
        "FARTCOINUSDT","DEGOUSDT","BULLAUSDT","CFGUSDT","COMPUSDT","CCUSDT",
        "ELSAUSDT","MYXUSDT","FORMUSDT","CYSUSDT","BTRUSDT","FHEUSDT",
        "NIGHTUSDT","THEUSDT","UAIUSDT","GRASSUSDT","HUMAUSDT","AINUSDT",
        "ATHUSDT","FOLKSUSDT","BABYUSDT","AIAUSDT","SUPERUSDT","KATUSDT",
        "HUSDT","SIGNUSDT","BARDUSDT","ANIMEUSDT","BCHUSDT","PIXELUSDT",
        "ZENUSDT","DASHUSDT","DEXEUSDT","BREVUSDT","FOGOUSDT","RESOLVUSDT",
        "POLYXUSDT","FIGHTUSDT","ORCAUSDT","SKRUSDT","ZECUSDT","TRADOORUSDT",
        "KAITOUSDT","LPTUSDT","ETHFIUSDT","RPLUSDT","BTCDOMUSDT","DYDXUSDT",
        "FRAXUSDT","OGNUSDT","PARTIUSDT","ONDOUSDT","AIOUSDT","KASUSDT",
        "ARUSDT","EIGENUSDT","CHZUSDT","BIOUSDT","TRUMPUSDT","KAVAUSDT",
        "SNXUSDT","API3USDT","AVNTUSDT","ENAUSDT","BIRBUSDT","ZKCUSDT",
        "GIGGLEUSDT","KITEUSDT","PEOPLEUSDT","ATOMUSDT","PLAYUSDT","BNBUSDT",
        "SENTUSDT","HYPEUSDT","HOLOUSDT","C98USDT","CLANKERUSDT","GPSUSDT",
        "KNCUSDT","BERAUSDT","ICPUSDT","SAHARAUSDT","TRBUSDT","MONUSDT",
        "PLUMEUSDT","ENSOUSDT","JTOUSDT","AEROUSDT","LIGHTUSDT","FFUSDT",
        "XTZUSDT","SOMIUSDT","1000LUNCUSDT","COSUSDT","TAUSDT","BTCUSDT",
        "STGUSDT","VANAUSDT","MERLUSDT","JCTUSDT","OPUSDT","PIEVERSEUSDT",
        "FLOWUSDT","AXLUSDT","FUSDT","TRXUSDT","YGGUSDT","AZTECUSDT",
        "AWEUSDT","ESPUSDT","STXUSDT","LTCUSDT","DOGEUSDT","XMRUSDT",
        "VANRYUSDT","IMXUSDT","BANANAS31USDT","PROVEUSDT","ASTERUSDT",
        "OPNUSDT","ORDIUSDT","WLDUSDT","TONUSDT","INJUSDT","ETCUSDT",
        "ZKUSDT","CYBERUSDT","GUSDT","OPENUSDT","AUCTIONUSDT","CAKEUSDT",
        "AIXBTUSDT","CFXUSDT","LINEAUSDT","ZKPUSDT","JASMYUSDT","QNTUSDT",
        "MIRAUSDT","LAUSDT","MORPHOUSDT","LUNA2USDT","1000PEPEUSDT","NEARUSDT",
        "ONUSDT","1000FLOKIUSDT","MEMEUSDT","LAYERUSDT","AAVEUSDT","STRKUSDT",
        "FILUSDT","STEEMUSDT","1000BONKUSDT","SPACEUSDT","PHAUSDT","ETHUSDT",
        "TURBOUSDT","ICNTUSDT","XAUTUSDT","DOTUSDT","SOLUSDT","JUPUSDT",
        "PAXGUSDT","PENGUUSDT","ANKRUSDT","MANTRAUSDT","TNSRUSDT","WLFIUSDT",
        "PUMPUSDT","PYTHUSDT","AXSUSDT","GRTUSDT","ENSUSDT","1000SHIBUSDT",
        "PENDLEUSDT","CRVUSDT","ZILUSDT","MOODENGUSDT","TIAUSDT","ACXUSDT",
        "1INCHUSDT","ARBUSDT","ZORAUSDT","IPUSDT","HBARUSDT","ROSEUSDT",
        "GALAUSDT","COLLECTUSDT","MEUSDT","APTUSDT","SPXUSDT","AKTUSDT",
        "INXUSDT","USELESSUSDT","VIRTUALUSDT","WAXPUSDT","BOMEUSDT","SKYAIUSDT",
        "NEIROUSDT","LDOUSDT","METUSDT","EDGEUSDT","WETUSDT","VETUSDT",
        "XLMUSDT","0GUSDT","LINKUSDT","DUSKUSDT","UNIUSDT","SUIUSDT",
        "ACUUSDT","GUNUSDT","SKYUSDT","SYRUPUSDT","SANDUSDT","ALLOUSDT",
        "ZAMAUSDT","PNUTUSDT","GASUSDT","LITUSDT","ADAUSDT","AVAXUSDT",
        "NEOUSDT","POLUSDT","RENDERUSDT","WIFUSDT","FETUSDT","WUSDT",
        "REZUSDT","HANAUSDT","MOVEUSDT","MANAUSDT","ARCUSDT","MUSDT",
        "INITUSDT","ENJUSDT","DENTUSDT","ALICEUSDT","TAOUSDT","APEUSDT",
        "AGLDUSDT","ATUSDT","VVVUSDT","DEEPUSDT","ARKMUSDT","SYNUSDT",
        "BSBUSDT","TAKEUSDT","ZROUSDT","EULUSDT","SEIUSDT","BLUAIUSDT",
        "APRUSDT","BRUSDT","ALGOUSDT","SUSDT","NAORISUSDT","XPLUSDT",
        "KERNELUSDT","CUSDT","GUAUSDT","PIPPINUSDT","GWEIUSDT","CLOUSDT",
        "ARIAUSDT","NOMUSDT","ONTUSDT","STOUSDT",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Sector tags
# ─────────────────────────────────────────────────────────────────────────────
SECTORS: dict = {
    "FETUSDT":"AI","RENDERUSDT":"AI","AIXBTUSDT":"AI","GRTUSDT":"AI",
    "AGLDUSDT":"AI","AIAUSDT":"AI","AINUSDT":"AI","UAIUSDT":"AI",
    "ARKMUSDT":"AI","VIRTUALUSDT":"AI","SKYAIUSDT":"AI",
    "ZECUSDT":"Privacy","DASHUSDT":"Privacy","XMRUSDT":"Privacy",
    "DUSKUSDT":"Privacy","PHAUSDT":"Privacy","POLYXUSDT":"Privacy",
    "BTCUSDT":"BTC","BTCDOMUSDT":"BTC","ORDIUSDT":"BTC",
    "ETHUSDT":"L1","SOLUSDT":"L1","AVAXUSDT":"L1","ADAUSDT":"L1",
    "DOTUSDT":"L1","NEARUSDT":"L1","APTUSDT":"L1","SUIUSDT":"L1",
    "TONUSDT":"L1","XLMUSDT":"L1","TRXUSDT":"L1","LTCUSDT":"L1",
    "BCHUSDT":"L1","XRPUSDT":"L1","BNBUSDT":"L1","ATOMUSDT":"L1",
    "ARBUSDT":"L2","OPUSDT":"L2","STRKUSDT":"L2","ZKUSDT":"L2",
    "LINEAUSDT":"L2","ZKPUSDT":"L2","POLUSDT":"L2","IMXUSDT":"L2",
    "AAVEUSDT":"DeFi","UNIUSDT":"DeFi","CRVUSDT":"DeFi","COMPUSDT":"DeFi",
    "SNXUSDT":"DeFi","DYDXUSDT":"DeFi","PENDLEUSDT":"DeFi","AEROUSDT":"DeFi",
    "MORPHOUSDT":"DeFi","1INCHUSDT":"DeFi","CAKEUSDT":"DeFi","LDOUSDT":"DeFi",
    "DOGEUSDT":"Meme","1000PEPEUSDT":"Meme","1000SHIBUSDT":"Meme",
    "1000BONKUSDT":"Meme","1000FLOKIUSDT":"Meme","FARTCOINUSDT":"Meme",
    "MEMEUSDT":"Meme","BOMEUSDT":"Meme","TURBOUSDT":"Meme","NEIROUSDT":"Meme",
    "SANDUSDT":"Gaming","MANAUSDT":"Gaming","GALAUSDT":"Gaming",
    "AXSUSDT":"Gaming","ALICEUSDT":"Gaming","APEUSDT":"Gaming",
}

# ─────────────────────────────────────────────────────────────────────────────
# Config persistence
# ─────────────────────────────────────────────────────────────────────────────
try:
    _SCRIPT_DIR = pathlib.Path(__file__).parent.absolute()
except Exception:
    _SCRIPT_DIR = pathlib.Path.cwd()

_probe = _SCRIPT_DIR / ".write_probe"
try:
    _probe.touch(); _probe.unlink()
except OSError:
    import tempfile
    _SCRIPT_DIR = pathlib.Path(tempfile.gettempdir()) / "binance_scanner"

_SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
CONFIG_FILE = _SCRIPT_DIR / "scanner_config.json"
LOG_FILE    = _SCRIPT_DIR / "scanner_log.json"

_config_lock = threading.Lock()

def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            saved = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            cfg = dict(DEFAULT_CONFIG)
            cfg.update(saved)
            return cfg
        except Exception:
            pass
    return dict(DEFAULT_CONFIG)

def save_config(cfg: dict):
    with _config_lock:
        CONFIG_FILE.write_text(json.dumps(cfg, indent=2), encoding="utf-8")

# ─────────────────────────────────────────────────────────────────────────────
# Log persistence
# ─────────────────────────────────────────────────────────────────────────────
def load_log():
    if LOG_FILE.exists():
        try: return json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except Exception: pass
    return {"health": {"total_cycles": 0, "last_scan_at": None,
                        "last_scan_duration_s": 0.0, "total_api_errors": 0,
                        "watchlist_size": 0, "btc_regime": "—", "btc_rsi": 0.0},
            "signals": []}

def save_log(log):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(log, indent=2), encoding="utf-8")

# ─────────────────────────────────────────────────────────────────────────────
# Module-level shared state  (persists across Streamlit reruns in same process)
# ─────────────────────────────────────────────────────────────────────────────
if "_scanner_initialised" not in st.session_state:
    # Only initialise globals once per process lifetime
    import builtins
    if not getattr(builtins, "_binance_scanner_globals_set", False):
        import builtins as _b
        _b._binance_scanner_globals_set = True
        _b._bsc_cfg            = load_config()
        _b._bsc_log            = load_log()
        _b._bsc_log_lock       = threading.Lock()
        _b._bsc_running        = threading.Event()
        _b._bsc_running.set()
        _b._bsc_thread         = None
        _b._bsc_filter_counts  = {}
        _b._bsc_filter_lock    = threading.Lock()
        _b._bsc_last_error     = ""
    st.session_state["_scanner_initialised"] = True

import builtins as _b
_cfg           = _b._bsc_cfg
_log           = _b._bsc_log
_log_lock      = _b._bsc_log_lock
_scanner_running = _b._bsc_running
_filter_lock   = _b._bsc_filter_lock
_filter_counts = _b._bsc_filter_counts

# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept":          "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
_local = threading.local()

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session

def safe_get(url, params=None, _retries=4):
    for attempt in range(_retries):
        try:
            r = get_session().get(url, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 60))); continue
            if r.status_code in (418, 403, 451):
                # 403/451 = datacenter IP blocked by exchange
                # If OKX also returns these, switch hosting to Railway/Fly.io
                raise RuntimeError(
                    f"HTTP {r.status_code}: Exchange is blocking this server's IP. "
                    "Deploy on Railway (railway.app) instead — uses non-datacenter IPs."
                )
            r.raise_for_status()
            data = r.json()
            # OKX wraps results: code "0" (string) = success
            if isinstance(data, dict) and "code" in data and data["code"] != "0":
                raise RuntimeError(f"OKX API error {data['code']}: {data.get('msg', '')}")
            return data
        except requests.exceptions.ConnectionError:
            if attempt < _retries - 1: time.sleep(5); continue
            raise
    raise RuntimeError(f"Failed after {_retries} retries: {url}")

# ─────────────────────────────────────────────────────────────────────────────
# OKX data helpers
# ─────────────────────────────────────────────────────────────────────────────
def _to_okx(sym: str) -> str:
    """BTCUSDT  →  BTC-USDT-SWAP"""
    return f"{sym[:-4]}-USDT-SWAP" if sym.endswith("USDT") else sym

def _from_okx(inst_id: str) -> str:
    """BTC-USDT-SWAP  →  BTCUSDT"""
    return inst_id.replace("-USDT-SWAP", "USDT") if inst_id.endswith("-USDT-SWAP") else inst_id

def get_symbols(watchlist: list) -> list:
    """Return watchlist symbols that are live USDT linear perps on OKX."""
    active = set()
    data   = safe_get(f"{BASE}/api/v5/public/instruments", {"instType": "SWAP"})
    for s in data.get("data", []):
        inst_id = s.get("instId", "")
        if inst_id.endswith("-USDT-SWAP") and s.get("state") == "live":
            active.add(_from_okx(inst_id))
    skipped = len([s for s in watchlist if s not in active])
    if skipped:
        print(f"  [{skipped} watchlist symbol(s) not trading on OKX — skipped]")
    return [s for s in watchlist if s in active]

def get_klines(sym: str, interval: str, limit: int) -> list:
    """Fetch OHLCV candles from OKX in ascending time order.
    OKX max = 300 per request, so we paginate when limit > 300.
    """
    okx_iv  = OKX_INTERVALS.get(interval, interval)
    inst_id = _to_okx(sym)
    all_bars: list = []
    after = None          # OKX 'after' = fetch bars OLDER than this ts

    while len(all_bars) < limit:
        batch  = min(300, limit - len(all_bars))
        params = {"instId": inst_id, "bar": okx_iv, "limit": batch}
        if after:
            params["after"] = after
        data = safe_get(f"{BASE}/api/v5/market/candles", params)
        bars = data.get("data", [])
        if not bars:
            break
        all_bars.extend(bars)
        after = bars[-1][0]          # oldest timestamp in this batch → page backwards
        if len(bars) < batch:
            break

    # OKX returns newest-first; reverse to oldest-first
    all_bars.reverse()
    return [{"time":   int(b[0]),
             "open":   float(b[1]),
             "high":   float(b[2]),
             "low":    float(b[3]),
             "close":  float(b[4]),
             "volume": float(b[5])}
            for b in all_bars]

# ─────────────────────────────────────────────────────────────────────────────
# Technical helpers
# ─────────────────────────────────────────────────────────────────────────────
def calc_rsi_series(closes, period=14):
    if len(closes) < period + 2: return []
    deltas = [closes[i]-closes[i-1] for i in range(1, len(closes))]
    gains  = [max(d, 0.) for d in deltas]
    losses = [max(-d, 0.) for d in deltas]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    rsi = [100. if al == 0 else 100 - 100 / (1 + ag / al)]
    for i in range(period, len(deltas)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
        rsi.append(100. if al == 0 else 100 - 100 / (1 + ag / al))
    return rsi

def find_swing_highs(candles, neighbors=2):
    highs = [c["high"] for c in candles]
    peaks = []
    for i in range(neighbors, len(highs) - neighbors):
        if (all(highs[i] > highs[i-j] for j in range(1, neighbors+1)) and
                all(highs[i] > highs[i+j] for j in range(1, neighbors+1))):
            peaks.append(highs[i])
    return peaks

def is_near_resistance(entry, peaks, tolerance):
    return any(entry <= p <= entry * (1 + tolerance) for p in peaks)

# ─────────────────────────────────────────────────────────────────────────────
# BTC regime guard
# ─────────────────────────────────────────────────────────────────────────────
def get_market_regime(cfg: dict) -> dict:
    try:
        btc_closes = [c["close"] for c in get_klines("BTCUSDT", "1h", 19)[:-1]]
        rsi = (calc_rsi_series(btc_closes) or [55.0])[-1]
    except Exception:
        rsi = 55.0
    base_vol = cfg["path_b_vol_min"]
    base_rsi = cfg["rsi_1h_max"]
    if rsi > 80:
        return {"btc_rsi": rsi, "label": "Overbought", "path_b_vol": max(base_vol, 3.0), "rsi_max": min(base_rsi, 68)}
    elif rsi > 70:
        return {"btc_rsi": rsi, "label": "Extended",   "path_b_vol": max(base_vol, 2.2), "rsi_max": min(base_rsi, 72)}
    elif rsi < 45:
        return {"btc_rsi": rsi, "label": "Weak BTC",   "path_b_vol": max(base_vol, 2.5), "rsi_max": min(base_rsi, 72)}
    else:
        return {"btc_rsi": rsi, "label": "Normal",     "path_b_vol": base_vol,            "rsi_max": base_rsi}

# ─────────────────────────────────────────────────────────────────────────────
# Core filter logic
# ─────────────────────────────────────────────────────────────────────────────
def _reset_filter_counts():
    global _filter_counts
    counts = {"checked": 0, "f1_momentum": 0, "f2_body": 0,
              "f4_rsi5m": 0, "f5_res5m": 0, "f6_res15m": 0, "f7_rsi1h": 0,
              "passed": 0, "errors": 0}
    with _filter_lock:
        _filter_counts.clear()
        _filter_counts.update(counts)
    _b._bsc_filter_counts = _filter_counts

def process(sym, cfg: dict, path_b_vol: float, rsi_max: float):
    try:
        with _filter_lock: _filter_counts["checked"] = _filter_counts.get("checked", 0) + 1

        raw_30m   = get_klines(sym, "30m", LOOKBACK_30M)
        completed = raw_30m[:-1]
        lb        = max(1, int(cfg["lookback_candles"]))
        hist_30m  = completed[:-lb]
        avg_vol   = sum(x["volume"] for x in hist_30m) / len(hist_30m) if hist_30m else 0

        ref_candle = None
        ratio      = 0.0
        for cand in completed[-lb:]:
            if not (cand["close"] > cand["open"]): continue
            r = cand["volume"] / avg_vol if avg_vol else 0
            if r >= cfg["path_a_vol_min"] and r > ratio:
                ref_candle = cand
                ratio      = r

        last3 = completed[-3:]
        if all(c["close"] > c["open"] for c in last3):
            avg3     = sum(c["volume"] for c in last3) / 3
            r3       = avg3 / avg_vol if avg_vol else 0
            gain_pct = (last3[-1]["close"] - last3[0]["open"]) / last3[0]["open"] * 100
            if r3 >= path_b_vol and gain_pct >= cfg["path_b_gain_min"]:
                if ref_candle is None or r3 > ratio:
                    ref_candle = completed[-1]
                    ratio      = r3

        if ref_candle is None:
            with _filter_lock: _filter_counts["f1_momentum"] = _filter_counts.get("f1_momentum", 0) + 1
            return None

        cr   = ref_candle["high"] - ref_candle["low"]
        body = ref_candle["close"] - ref_candle["open"]
        bp   = (body / cr) if cr > 0 else 1.0
        if cr > 0 and bp < cfg["body_ratio_min"]:
            with _filter_lock: _filter_counts["f2_body"] = _filter_counts.get("f2_body", 0) + 1
            return None

        m5            = get_klines(sym, "5m", 32)[:-1]
        current_price = m5[-1]["close"]

        if current_price > ref_candle["close"] * (1 + cfg["move_guard_pct"] / 100):
            with _filter_lock: _filter_counts["f1_momentum"] = _filter_counts.get("f1_momentum", 0) + 1
            return None

        rsi5 = (calc_rsi_series([c["close"] for c in m5]) or [0])[-1]
        if rsi5 < cfg["rsi_5m_min"]:
            with _filter_lock: _filter_counts["f4_rsi5m"] = _filter_counts.get("f4_rsi5m", 0) + 1
            return None

        entry = round(current_price, 6)
        tol   = cfg["resistance_tol_pct"] / 100

        peaks_5m = find_swing_highs(m5[-25:], neighbors=2)
        if is_near_resistance(entry, peaks_5m, tol):
            with _filter_lock: _filter_counts["f5_res5m"] = _filter_counts.get("f5_res5m", 0) + 1
            return None

        m15       = get_klines(sym, "15m", 32)[:-1]
        peaks_15m = find_swing_highs(m15[-25:], neighbors=2)
        if is_near_resistance(entry, peaks_15m, tol):
            with _filter_lock: _filter_counts["f6_res15m"] = _filter_counts.get("f6_res15m", 0) + 1
            return None

        rsi1h = (calc_rsi_series([c["close"] for c in get_klines(sym, "1h", 19)[:-1]]) or [0])[-1]
        if not (cfg["rsi_1h_min"] <= rsi1h <= rsi_max):
            with _filter_lock: _filter_counts["f7_rsi1h"] = _filter_counts.get("f7_rsi1h", 0) + 1
            return None

        tp  = round(entry * (1 + cfg["tp_pct"] / 100), 6)
        sl  = round(entry * (1 - cfg["sl_pct"] / 100), 6)
        sec = SECTORS.get(sym, "Other")
        with _filter_lock: _filter_counts["passed"] = _filter_counts.get("passed", 0) + 1

        return {
            "id":          str(uuid.uuid4())[:8],
            "timestamp":   datetime.now(timezone.utc).isoformat(),
            "symbol":      sym,
            "entry":       entry,
            "tp":          tp,
            "sl":          sl,
            "ratio":       round(ratio, 2),
            "sector":      sec,
            "status":      "open",
            "close_price": None,
            "close_time":  None,
        }
    except Exception:
        with _filter_lock: _filter_counts["errors"] = _filter_counts.get("errors", 0) + 1
        return "error"

def scan(cfg: dict):
    _reset_filter_counts()
    regime  = get_market_regime(cfg)
    symbols = get_symbols(cfg["watchlist"])
    results = []
    with ThreadPoolExecutor(max_workers=8) as exe:
        futs = [exe.submit(process, s, cfg, regime["path_b_vol"], regime["rsi_max"])
                for s in symbols]
        for f in as_completed(futs):
            r = f.result()
            if r and r != "error":
                results.append(r)
    return sorted(results, key=lambda x: x["ratio"], reverse=True), _filter_counts.get("errors", 0), regime

def update_open_signals(signals):
    for sig in signals:
        if sig["status"] != "open": continue
        try:
            sig_ts_ms = int(datetime.fromisoformat(sig["timestamp"]).timestamp() * 1000)
            candles   = get_klines(sig["symbol"], "5m", 200)
            post      = [c for c in candles if c["time"] >= sig_ts_ms]
            tp_time = sl_time = None
            for c in post:
                if tp_time is None and c["high"] >= sig["tp"]: tp_time = c["time"]
                if sl_time is None and c["low"]  <= sig["sl"]: sl_time = c["time"]
            if tp_time is not None or sl_time is not None:
                if tp_time is not None and (sl_time is None or tp_time <= sl_time):
                    sig.update(status="tp_hit", close_price=sig["tp"],
                               close_time=datetime.fromtimestamp(tp_time / 1000, tz=timezone.utc).isoformat())
                else:
                    sig.update(status="sl_hit", close_price=sig["sl"],
                               close_time=datetime.fromtimestamp(sl_time / 1000, tz=timezone.utc).isoformat())
        except Exception:
            pass
    return signals

# ─────────────────────────────────────────────────────────────────────────────
# Background scanner thread
# ─────────────────────────────────────────────────────────────────────────────
def _bg_loop():
    while True:
        if not _scanner_running.is_set():
            time.sleep(2)
            continue
        with _config_lock:
            cfg = dict(_b._bsc_cfg)
        t0 = time.time()
        try:
            with _log_lock:
                _b._bsc_log["signals"] = update_open_signals(_b._bsc_log["signals"])
            new_sigs, errors, regime = scan(cfg)
            cutoff      = datetime.now(timezone.utc) - timedelta(minutes=cfg["cooldown_minutes"])
            with _log_lock:
                cooled  = {s["symbol"] for s in _b._bsc_log["signals"]
                           if datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")) >= cutoff}
                active  = {s["symbol"] for s in _b._bsc_log["signals"] if s["status"] == "open"}
                skip    = cooled | active
                for sig in new_sigs:
                    if sig["symbol"] not in skip:
                        _b._bsc_log["signals"].append(sig)
                        skip.add(sig["symbol"])
                elapsed = time.time() - t0
                _b._bsc_log["health"].update(
                    total_cycles         = _b._bsc_log["health"].get("total_cycles", 0) + 1,
                    last_scan_at         = datetime.now(timezone.utc).isoformat(),
                    last_scan_duration_s = round(elapsed, 1),
                    total_api_errors     = _b._bsc_log["health"].get("total_api_errors", 0) + errors,
                    watchlist_size       = len(cfg["watchlist"]),
                    btc_regime           = regime["label"],
                    btc_rsi              = round(regime["btc_rsi"], 1),
                )
                save_log(_b._bsc_log)
            _b._bsc_last_error = ""
        except Exception as e:
            _b._bsc_last_error = str(e)
        elapsed = time.time() - t0
        time.sleep(max(0, cfg["loop_minutes"] * 60 - elapsed))

def _ensure_scanner():
    if _b._bsc_thread is None or not _b._bsc_thread.is_alive():
        t = threading.Thread(target=_bg_loop, daemon=True, name="binance-scanner")
        t.start()
        _b._bsc_thread = t

# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# STREAMLIT UI
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OKX Futures Scanner",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Start background scanner (safe to call on every rerun — checks is_alive())
_ensure_scanner()

# ── Snapshot shared state for this render ────────────────────────────────────
with _log_lock:
    _snap_log = json.loads(json.dumps(_b._bsc_log))   # deep copy
with _config_lock:
    _snap_cfg = dict(_b._bsc_cfg)

health  = _snap_log.get("health", {})
signals = _snap_log.get("signals", [])

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — Configuration
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Configuration")

    # ── Scanner control ───────────────────────────────────────────────────────
    running = _scanner_running.is_set()
    btn_label = "⏹ Stop Scanner" if running else "▶️ Start Scanner"
    btn_color = "primary" if running else "secondary"
    if st.button(btn_label, use_container_width=True, type=btn_color):
        if running:
            _scanner_running.clear()
        else:
            _scanner_running.set()
        st.rerun()

    status_color = "🟢" if running else "🔴"
    st.caption(f"{status_color}  Scanner is {'running' if running else 'stopped'}")

    st.divider()

    # ── Trade management ──────────────────────────────────────────────────────
    st.markdown("**📊 Trade Settings**")
    c1, c2 = st.columns(2)
    new_tp = c1.number_input("TP %", min_value=0.1, max_value=20.0, step=0.1,
                              value=float(_snap_cfg["tp_pct"]), key="cfg_tp")
    new_sl = c2.number_input("SL %", min_value=0.1, max_value=20.0, step=0.1,
                              value=float(_snap_cfg["sl_pct"]), key="cfg_sl")

    st.divider()

    # ── F1 Momentum ───────────────────────────────────────────────────────────
    st.markdown("**🔥 F1 — Momentum**")
    new_pa_vol = st.number_input("Path A vol min (×avg)", 0.5, 10.0, step=0.1,
                                  value=float(_snap_cfg["path_a_vol_min"]), key="cfg_pa_vol")
    new_lb = st.number_input("Lookback candles (30m)", 1, 48, step=1,
                               value=int(_snap_cfg["lookback_candles"]), key="cfg_lb")
    new_pb_vol = st.number_input("Path B vol min (×avg)", 0.5, 10.0, step=0.1,
                                  value=float(_snap_cfg["path_b_vol_min"]), key="cfg_pb_vol")
    new_pb_gain = st.number_input("Path B gain min (%)", 0.0, 10.0, step=0.1,
                                   value=float(_snap_cfg["path_b_gain_min"]), key="cfg_pb_gain")
    new_mg = st.number_input("Move guard (%)", 0.5, 20.0, step=0.1,
                              value=float(_snap_cfg["move_guard_pct"]), key="cfg_mg")

    st.divider()

    # ── F2 Body quality ───────────────────────────────────────────────────────
    st.markdown("**📐 F2 — Body Quality**")
    new_body = st.number_input("Body ratio min (0–1)", 0.0, 1.0, step=0.05,
                                value=float(_snap_cfg["body_ratio_min"]), key="cfg_body")

    st.divider()

    # ── RSI ───────────────────────────────────────────────────────────────────
    st.markdown("**📈 F4/F7 — RSI**")
    new_rsi5_min = st.number_input("5m RSI min", 0, 100, step=1,
                                    value=int(_snap_cfg["rsi_5m_min"]), key="cfg_rsi5")
    c3, c4 = st.columns(2)
    new_rsi1h_min = c3.number_input("1h RSI min", 0, 100, step=1,
                                     value=int(_snap_cfg["rsi_1h_min"]), key="cfg_rsi1h_min")
    new_rsi1h_max = c4.number_input("1h RSI max", 0, 100, step=1,
                                     value=int(_snap_cfg["rsi_1h_max"]), key="cfg_rsi1h_max")

    st.divider()

    # ── Resistance ────────────────────────────────────────────────────────────
    st.markdown("**🚧 F5/F6 — Resistance**")
    new_res_tol = st.number_input("Tolerance % above entry", 0.1, 10.0, step=0.1,
                                   value=float(_snap_cfg["resistance_tol_pct"]), key="cfg_res")

    st.divider()

    # ── Execution ─────────────────────────────────────────────────────────────
    st.markdown("**⏱ Execution**")
    c5, c6 = st.columns(2)
    new_loop = c5.number_input("Loop (min)", 1, 60, step=1,
                                value=int(_snap_cfg["loop_minutes"]), key="cfg_loop")
    new_cool = c6.number_input("Cooldown (min)", 1, 120, step=1,
                                value=int(_snap_cfg["cooldown_minutes"]), key="cfg_cool")

    st.divider()

    # ── Watchlist ─────────────────────────────────────────────────────────────
    st.markdown("**📋 Watchlist** (one symbol per line)")
    wl_text = st.text_area(
        label="watchlist",
        value="\n".join(_snap_cfg["watchlist"]),
        height=180,
        label_visibility="collapsed",
        key="cfg_wl",
    )

    st.divider()

    # ── Data management ───────────────────────────────────────────────────────
    st.markdown("**🗑 Clear Signal History**")

    if st.button("⚡ Flush All Data", use_container_width=True, type="secondary",
                 help="Remove every signal from history and reset health counters"):
        with _log_lock:
            _b._bsc_log["signals"] = []
            _b._bsc_log["health"] = {
                "total_cycles": 0, "last_scan_at": None,
                "last_scan_duration_s": 0.0, "total_api_errors": 0,
                "watchlist_size": 0, "btc_regime": "—", "btc_rsi": 0.0,
            }
            save_log(_b._bsc_log)
        st.success("✅ All data flushed")
        st.rerun()

    c_day, c_week = st.columns(2)
    if c_day.button("📅 Clear 24h", use_container_width=True,
                    help="Remove signals from the last 24 hours"):
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [
                s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(
                    s["timestamp"].replace("Z", "+00:00")) < cutoff
            ]
            removed = before - len(_b._bsc_log["signals"])
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {removed} signal(s) from last 24 h")
        st.rerun()

    if c_week.button("📆 Clear 7d", use_container_width=True,
                     help="Remove signals from the last 7 days"):
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        with _log_lock:
            before = len(_b._bsc_log["signals"])
            _b._bsc_log["signals"] = [
                s for s in _b._bsc_log["signals"]
                if datetime.fromisoformat(
                    s["timestamp"].replace("Z", "+00:00")) < cutoff
            ]
            removed = before - len(_b._bsc_log["signals"])
            save_log(_b._bsc_log)
        st.success(f"✅ Removed {removed} signal(s) from last 7 days")
        st.rerun()

    st.divider()

    # ── Save button ───────────────────────────────────────────────────────────
    if st.button("💾 Save & Apply", use_container_width=True, type="primary"):
        new_wl = [s.strip().upper() for s in wl_text.splitlines() if s.strip()]
        new_cfg = {
            "tp_pct":             new_tp,
            "sl_pct":             new_sl,
            "path_a_vol_min":     new_pa_vol,
            "lookback_candles":   int(new_lb),
            "path_b_vol_min":     new_pb_vol,
            "path_b_gain_min":    new_pb_gain,
            "move_guard_pct":     new_mg,
            "body_ratio_min":     new_body,
            "rsi_5m_min":         int(new_rsi5_min),
            "resistance_tol_pct": new_res_tol,
            "rsi_1h_min":         int(new_rsi1h_min),
            "rsi_1h_max":         int(new_rsi1h_max),
            "loop_minutes":       int(new_loop),
            "cooldown_minutes":   int(new_cool),
            "watchlist":          new_wl,
        }
        with _config_lock:
            _b._bsc_cfg.clear()
            _b._bsc_cfg.update(new_cfg)
        save_config(new_cfg)
        st.success(f"✅ Config saved — {len(new_wl)} coins in watchlist")
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN AREA
# ─────────────────────────────────────────────────────────────────────────────

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🔍 OKX Futures Scanner")

last_scan = health.get("last_scan_at", "never")
if last_scan and last_scan != "never":
    try:
        ts  = datetime.fromisoformat(last_scan.replace("Z", "+00:00"))
        ago = int((datetime.now(timezone.utc) - ts).total_seconds() / 60)
        last_scan = f"{ago}m ago"
    except Exception:
        pass

col_h1, col_h2 = st.columns([3, 1])
col_h1.caption(f"Last scan: {last_scan}   |   Auto-refreshes every 30 s")
if col_h2.button("🔄 Refresh now", key="manual_refresh"):
    st.rerun()

# ── BTC Regime banner ─────────────────────────────────────────────────────────
regime_label = health.get("btc_regime", "—")
btc_rsi      = health.get("btc_rsi", 0.0)
regime_styles = {
    "Normal":     ("🟢", "#1a3d2b", "#3fb950"),
    "Extended":   ("🟠", "#3d2e0d", "#d29922"),
    "Overbought": ("🔴", "#3d0d0d", "#f85149"),
    "Weak BTC":   ("🔵", "#0d2040", "#58a6ff"),
}
icon, bg, fg = regime_styles.get(regime_label, ("⚪", "#1c2128", "#8b949e"))
st.markdown(
    f"<div style='background:{bg};border:1px solid {fg};border-radius:6px;"
    f"padding:10px 16px;margin-bottom:12px;font-size:15px'>"
    f"<b style='color:{fg}'>{icon} BTC Regime: {regime_label}</b>"
    f"  &nbsp;|&nbsp;  1h RSI: <b style='color:{fg}'>{btc_rsi}</b>"
    f"</div>",
    unsafe_allow_html=True,
)

# ── Health metrics ────────────────────────────────────────────────────────────
open_count = sum(1 for s in signals if s["status"] == "open")
tp_count   = sum(1 for s in signals if s["status"] == "tp_hit")
sl_count   = sum(1 for s in signals if s["status"] == "sl_hit")

m1, m2, m3, m4, m5_col, m6 = st.columns(6)
m1.metric("Total Cycles",    health.get("total_cycles", 0))
m2.metric("Last Scan",       f"{health.get('last_scan_duration_s', 0)}s")
m3.metric("API Errors",      health.get("total_api_errors", 0))
m4.metric("Open Trades",     open_count)
m5_col.metric("TP Hit",      tp_count)
m6.metric("SL Hit",          sl_count)

# ── Last scanner error (if any) ───────────────────────────────────────────────
if getattr(_b, "_bsc_last_error", ""):
    st.warning(f"⚠️ Last scanner error: {_b._bsc_last_error}")

st.divider()

# ── Sector filter ─────────────────────────────────────────────────────────────
all_sectors = ["All", "BTC", "L1", "L2", "DeFi", "AI", "Privacy", "Meme", "Gaming", "Other"]
if "sector_filter" not in st.session_state:
    st.session_state["sector_filter"] = "All"

sector_cols = st.columns(len(all_sectors))
for i, sec in enumerate(all_sectors):
    active = st.session_state["sector_filter"] == sec
    btn_type = "primary" if active else "secondary"
    if sector_cols[i].button(sec, key=f"sec_{sec}", type=btn_type, use_container_width=True):
        st.session_state["sector_filter"] = sec
        st.rerun()

selected_sector = st.session_state["sector_filter"]

# ── Signals table ─────────────────────────────────────────────────────────────
filtered = signals if selected_sector == "All" else [s for s in signals if s.get("sector") == selected_sector]
filtered_sorted = sorted(filtered, key=lambda x: x.get("timestamp", ""), reverse=True)

st.markdown(f"### Signals ({len(filtered_sorted)} shown)")

if filtered_sorted:
    rows = []
    for s in filtered_sorted:
        status = s.get("status", "open")
        status_icon = {"open": "🔵 Open", "tp_hit": "✅ TP Hit", "sl_hit": "❌ SL Hit"}.get(status, status)
        ts = s.get("timestamp", "")
        try:
            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            ts_str = ts_dt.strftime("%m/%d %H:%M")
        except Exception:
            ts_str = ts[:16] if ts else "—"
        rows.append({
            "Time":    ts_str,
            "Symbol":  s.get("symbol", ""),
            "Sector":  s.get("sector", "Other"),
            "Entry":   s.get("entry", ""),
            "TP":      s.get("tp", ""),
            "SL":      s.get("sl", ""),
            "Ratio":   f"{s.get('ratio', 0)}×",
            "Status":  status_icon,
            "Close $": s.get("close_price") or "—",
        })
    st.dataframe(rows, use_container_width=True, hide_index=True,
                 column_config={
                     "TP":    st.column_config.NumberColumn(format="%.6f"),
                     "SL":    st.column_config.NumberColumn(format="%.6f"),
                     "Entry": st.column_config.NumberColumn(format="%.6f"),
                 })
else:
    st.info("No signals yet. The scanner runs every few minutes — check back soon.")

# ── Charts ────────────────────────────────────────────────────────────────────
if signals:
    st.divider()
    ch1, ch2 = st.columns(2)

    # Pie: sector distribution
    sec_counts: dict = {}
    for s in signals:
        sec = s.get("sector", "Other")
        sec_counts[sec] = sec_counts.get(sec, 0) + 1
    fig_pie = go.Figure(go.Pie(
        labels=list(sec_counts.keys()),
        values=list(sec_counts.values()),
        hole=0.4,
        marker=dict(colors=px.colors.qualitative.Dark24),
    ))
    fig_pie.update_layout(
        title="Signals by Sector",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e6edf3"),
        margin=dict(t=40, b=10, l=10, r=10),
        legend=dict(font=dict(size=11)),
    )
    ch1.plotly_chart(fig_pie, use_container_width=True)

    # Bar: outcome breakdown
    outcome_data = {
        "Open":   sum(1 for s in signals if s["status"] == "open"),
        "TP Hit": sum(1 for s in signals if s["status"] == "tp_hit"),
        "SL Hit": sum(1 for s in signals if s["status"] == "sl_hit"),
    }
    fig_bar = go.Figure(go.Bar(
        x=list(outcome_data.keys()),
        y=list(outcome_data.values()),
        marker_color=["#58a6ff", "#3fb950", "#f85149"],
        text=list(outcome_data.values()),
        textposition="outside",
    ))
    fig_bar.update_layout(
        title="Signal Outcomes",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e6edf3"),
        yaxis=dict(gridcolor="#21262d"),
        margin=dict(t=40, b=10, l=10, r=10),
    )
    ch2.plotly_chart(fig_bar, use_container_width=True)

    # Timeline: signals per day
    if len(signals) > 1:
        from collections import Counter
        day_counts: Counter = Counter()
        for s in signals:
            try:
                d = datetime.fromisoformat(s["timestamp"].replace("Z", "+00:00")).strftime("%m/%d")
                day_counts[d] += 1
            except Exception:
                pass
        if day_counts:
            days   = sorted(day_counts.keys())
            counts = [day_counts[d] for d in days]
            fig_line = go.Figure(go.Bar(
                x=days, y=counts,
                marker_color="#d29922",
                text=counts, textposition="outside",
            ))
            fig_line.update_layout(
                title="Signals Per Day",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e6edf3"),
                yaxis=dict(gridcolor="#21262d"),
                margin=dict(t=40, b=10, l=10, r=10),
            )
            st.plotly_chart(fig_line, use_container_width=True)

# ── Filter funnel (collapsible) ───────────────────────────────────────────────
fc = dict(_filter_counts)
if fc.get("checked", 0) > 0:
    with st.expander("🔬 Last scan filter funnel"):
        checked = fc.get("checked", 0)
        funnel_data = [
            ("Checked",        checked),
            ("After F1 Mom.",  checked - fc.get("f1_momentum", 0)),
            ("After F2 Body",  checked - fc.get("f1_momentum", 0) - fc.get("f2_body", 0)),
            ("After F4 5mRSI", checked - fc.get("f1_momentum", 0) - fc.get("f2_body", 0) - fc.get("f4_rsi5m", 0)),
            ("After F5 5mRes", checked - fc.get("f1_momentum", 0) - fc.get("f2_body", 0) - fc.get("f4_rsi5m", 0) - fc.get("f5_res5m", 0)),
            ("After F6 15mR.", checked - fc.get("f1_momentum", 0) - fc.get("f2_body", 0) - fc.get("f4_rsi5m", 0) - fc.get("f5_res5m", 0) - fc.get("f6_res15m", 0)),
            ("Passed F7 1hR.", fc.get("passed", 0)),
        ]
        fig_funnel = go.Figure(go.Funnel(
            y=[d[0] for d in funnel_data],
            x=[d[1] for d in funnel_data],
            marker=dict(color=["#58a6ff","#79c0ff","#a5d6ff","#cae8ff","#3fb950","#56d364","#d29922"]),
            textinfo="value+percent initial",
        ))
        fig_funnel.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#e6edf3"),
            margin=dict(t=10, b=10, l=10, r=10),
            height=320,
        )
        st.plotly_chart(fig_funnel, use_container_width=True)
        st.caption(f"Errors this cycle: {fc.get('errors', 0)}")

# ─────────────────────────────────────────────────────────────────────────────
# Auto-refresh every 30 seconds
# ─────────────────────────────────────────────────────────────────────────────
time.sleep(30)
st.rerun()
