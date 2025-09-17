#!/usr/bin/env python3
"""
Gap‑Up Fade Engine (AliceBlue pya3 → Algomojo) — Today‑only fast path
---------------------------------------------------------------------

What it does
- Loads your daily master.csv (Symbol, Band, Margin, Yesterday Close) from a GitHub raw URL (or local fallback)
- At **09:16 IST**, fetches **today’s 1‑minute** OHLCV for all **Tradable** stocks (Band≥10 & Margin==5)
- Detects **gap‑up 2–7%** using YesterdayClose from the master
- Allocates **TOTAL_CAPITAL** equally across the gap‑ups
- Places **SELL MARKET** tranches **every minute 09:16–09:25** until each symbol’s exit qty is filled
- Optional first‑minute weighting (negative/large body gets more size)

Why it’s fast
- Uses only **today’s** data (no 3‑day history)
- Parallel instrument lookup + 1‑min fetch via ThreadPoolExecutor

Prereqs
  pip install pandas requests urllib3 pya3 pytz

Env configuration (examples)
  ALICE_USER_ID=xxxx
  ALICE_API_KEY=xxxx
  GITHUB_MASTER_CSV_URL=https://raw.githubusercontent.com/<you>/<repo>/main/master.csv
  # Algomojo direct order endpoint (or leave empty to send to WEBHOOK_URL)
  ALGOMOJO_URL=https://api.algomojo.com/order
  ALGOMOJO_API_KEY=xxxx
  ALGOMOJO_SECRET=xxxx
  ALGOMOJO_BROKER=AB
  # Optional
  TOTAL_CAPITAL=100000
  TRANCHE_COUNT=10
  BAND_MIN=10
  MARGIN_REQUIRED=5
  GAP_MIN_PCT=2.0
  GAP_MAX_PCT=7.0
  USE_WEIGHTING=1
  BODY_SMALL_PCT=0.30
  BODY_LARGE_PCT=0.80
  ALICE_WORKERS=12
  DRY_RUN=1

Notes
- If ALGOMOJO_URL is empty but WEBHOOK_URL is set, we’ll POST a simple payload there (for your existing bridge).
- Watchlists aren’t required: this script submits explicit `quantity` in each order.
"""
from __future__ import annotations
import os
import sys
import math
import json
import time
import logging
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Dict, Optional, Tuple, List

import pandas as pd
import numpy as np
import urllib3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pya3 import Aliceblue
import pytz

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("gap_up_fade_today")

# -------------------- Time helpers --------------------
IST = pytz.timezone("Asia/Kolkata")
NOW = lambda: datetime.now(IST)
MARKET_OPEN = lambda: NOW().replace(hour=9, minute=15, second=0, microsecond=0)
FIRST_MINUTE_CLOSE = lambda: MARKET_OPEN() + timedelta(minutes=1)  # 09:16:00
LAST_ENTRY_MINUTE = lambda: MARKET_OPEN() + timedelta(minutes=10)  # 09:25:00

# -------------------- Config --------------------
@dataclass
class Config:
    alice_user_id: str = os.getenv("ALICE_USER_ID", "")
    alice_api_key: str = os.getenv("ALICE_API_KEY", "")

    github_master_csv_url: str = os.getenv("GITHUB_MASTER_CSV_URL", "")
    master_local_path: str = os.getenv("MASTER_LOCAL_PATH", "master.csv")

    total_capital: float = float(os.getenv("TOTAL_CAPITAL", 100000))
    tranche_count: int = int(os.getenv("TRANCHE_COUNT", 10))

    band_min: float = float(os.getenv("BAND_MIN", 10))
    margin_required: float = float(os.getenv("MARGIN_REQUIRED", 5))
    gap_min_pct: float = float(os.getenv("GAP_MIN_PCT", 2.0))
    gap_max_pct: float = float(os.getenv("GAP_MAX_PCT", 7.0))

    use_weighting: bool = bool(int(os.getenv("USE_WEIGHTING", "1")))
    body_small_pct: float = float(os.getenv("BODY_SMALL_PCT", 0.30))
    body_large_pct: float = float(os.getenv("BODY_LARGE_PCT", 0.80))

    alice_workers: int = int(os.getenv("ALICE_WORKERS", 12))

    algomojo_url: str = os.getenv("ALGOMOJO_URL", "")
    algomojo_api_key: str = os.getenv("ALGOMOJO_API_KEY", "")
    algomojo_secret: str = os.getenv("ALGOMOJO_SECRET", "")
    algomojo_broker: str = os.getenv("ALGOMOJO_BROKER", "AB")

    webhook_url: str = os.getenv("WEBHOOK_URL", "")  # optional fallback

    dry_run: bool = bool(int(os.getenv("DRY_RUN", "1")))

# -------------------- Clients --------------------
class Alice:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.client = Aliceblue(user_id=str(cfg.alice_user_id), api_key=cfg.alice_api_key)
        _ = self.client.get_session_id()
        self.inst_cache: Dict[Tuple[str,str], dict] = {}

    def instrument(self, symbol: str, exchange: str = "NSE"):
        key = (exchange, symbol)
        if key in self.inst_cache:
            return self.inst_cache[key]
        try:
            inst = self.client.get_instrument_by_symbol(symbol=symbol, exchange=exchange)
            self.inst_cache[key] = inst
            return inst
        except Exception:
            matches = self.client.search_instruments(exchange, symbol)
            if matches:
                self.inst_cache[key] = matches[0]
                return matches[0]
            raise

    def today_1min(self, symbol: str, exchange: str = "NSE") -> pd.DataFrame:
        inst = self.instrument(symbol, exchange)
        # pya3 expects naive datetimes (date only). We'll pass today for from/to.
        today = datetime.strptime(NOW().strftime("%d-%m-%Y"), "%d-%m-%Y")
        df = self.client.get_historical(inst, from_date=today, to_date=today, interval="1", indices=False)
        if isinstance(df, dict) and "error" in df:
            raise RuntimeError(df["error"])
        if df is None:
            return pd.DataFrame()
        # Normalize
        if "datetime" in df.columns:
            df = df.rename(columns={"datetime": "ts"})
        elif "time" in df.columns:
            df = df.rename(columns={"time": "ts"})
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        df = df.dropna(subset=["ts"]).reset_index(drop=True)
        return df

# -------------------- IO helpers --------------------
http = urllib3.PoolManager()

def load_master(cfg: Config) -> pd.DataFrame:
    if cfg.github_master_csv_url:
        log.info("Loading master from GitHub raw URL…")
        r = http.request("GET", cfg.github_master_csv_url, timeout=urllib3.Timeout(connect=3.0, read=5.0))
        if r.status != 200:
            raise RuntimeError(f"Master fetch failed: HTTP {r.status}")
        df = pd.read_csv(pd.compat.StringIO(r.data.decode("utf-8")))
    else:
        log.info("Loading master from local path…")
        df = pd.read_csv(cfg.master_local_path)

    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    # Normalize essential columns
    rename = {}
    for c in df.columns:
        if c in {"symbol", "stock", "tradingsymbol"}:
            rename[c] = "symbol"
        if "yesterday" in c or c in {"yclose", "prev_close", "previous_close", "close_prev"}:
            rename[c] = "yclose"
        if "band" in c:
            rename[c] = "band"
        if "margin" in c:
            rename[c] = "margin"
    df = df.rename(columns=rename)

    need = {"symbol", "yclose", "band", "margin"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"master.csv missing columns {missing}. Expected at least {need}")

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df = df[(df["band"] >= cfg.band_min) & (df["margin"] == cfg.margin_required)].copy()
    return df["symbol"].to_frame().join(df[["yclose", "band", "margin"]])

# -------------------- Gap logic --------------------
def detect_gap_up(open_915: float, yclose: float, cfg: Config) -> Tuple[bool, float]:
    if not (open_915 and yclose and yclose > 0):
        return False, 0.0
    gap_pct = (open_915 - yclose) / yclose * 100.0
    ok = cfg.gap_min_pct <= gap_pct <= cfg.gap_max_pct
    return ok, gap_pct

# -------------------- Weighting --------------------
def first_minute_metrics(df1: pd.DataFrame) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    # 09:15 candle is the first minute after open in NSE
    if df1.empty:
        return None, None, None
    row = df1.loc[df1["ts"].dt.time == dtime(9,15)]
    if row.empty:
        # fallback to nearest ≥ 09:15
        row = df1.loc[df1["ts"].dt.time >= dtime(9,15)].head(1)
        if row.empty:
            return None, None, None
    r = row.iloc[0]
    o, c = float(r["open"]), float(r["close"])
    body_pct = abs(c - o) / o * 100.0
    return o, c, body_pct


def compute_weight(is_negative: bool, body_pct: float, cfg: Config) -> float:
    if not cfg.use_weighting:
        return 1.0
    # α(sign)
    alpha = 1.0 if is_negative else 0.4
    # β(strength)
    if body_pct is None:
        beta = 1.0
    elif body_pct < cfg.body_small_pct:
        beta = 0.70
    elif body_pct > cfg.body_large_pct:
        beta = 1.50
    else:
        beta = 1.00
    # γ(weekday) — Wed underweight
    weekday = NOW().strftime("%A")
    gamma = {"Thursday": 1.30, "Tuesday": 1.15, "Monday": 1.05, "Friday": 1.05, "Wednesday": 0.50}.get(weekday, 1.0)
    w = max(0.25, min(1.75, alpha * beta * gamma))
    return w

# -------------------- Algomojo --------------------
class AM:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.http = urllib3.PoolManager()

    def place_short(self, symbol: str, qty: int) -> Dict:
        if qty <= 0:
            return {"ok": False, "error": "qty<=0"}
        if self.cfg.dry_run:
            log.info(f"[DRY] SELL {symbol} x{qty}")
            return {"ok": True, "dry_run": True}
        if self.cfg.algomojo_url:
            payload = {
                "api_key": self.cfg.algomojo_api_key,
                "secret": self.cfg.algomojo_secret,
                "broker": self.cfg.algomojo_broker,
                "symbol": symbol,
                "transaction_type": "SELL",
                "order_type": "MARKET",
                "product": "MIS",
                "quantity": int(qty),
                "variety": "NORMAL",
            }
            try:
                r = requests.post(self.cfg.algomojo_url, json=payload, timeout=5)
                return {"ok": r.status_code in (200,201), "status": r.status_code, "text": r.text}
            except Exception as e:
                return {"ok": False, "error": str(e)}
        elif self.cfg.webhook_url:
            payload = {"stocks": symbol, "alert_name": "SHORT", "quantity": int(qty)}
            r = self.http.request("POST", self.cfg.webhook_url, headers={"Content-Type": "application/json"}, body=json.dumps(payload).encode("utf-8"))
            return {"ok": r.status==200, "status": r.status}
        else:
            return {"ok": False, "error": "No ALGOMOJO_URL or WEBHOOK_URL configured"}

# -------------------- Engine --------------------
@dataclass
class Plan:
    symbol: str
    yclose: float
    margin: float
    open_915: Optional[float] = None
    first_close: Optional[float] = None
    first_body_pct: Optional[float] = None
    gap_pct: Optional[float] = None
    weight: float = 1.0
    base_exit_qty: int = 0
    entry_tranche: int = 0
    filled: int = 0

    def remaining(self) -> int:
        return max(0, self.base_exit_qty - self.filled)


def main():
    cfg = Config()
    if not cfg.alice_user_id or not cfg.alice_api_key:
        log.error("Set ALICE_USER_ID and ALICE_API_KEY")
        sys.exit(2)

    # Wait until 09:16:05 IST to ensure first candle is closed
    while NOW() < FIRST_MINUTE_CLOSE() + timedelta(seconds=5):
        time.sleep(0.5)

    # Load & filter tradables
    master = load_master(cfg)
    if master.empty:
        log.error("No Tradables (Band/Margin filter). Exiting.")
        return

    alice = Alice(cfg)

    # Parallel fetch of today 1min for all tradables
    def fetch_one(sym: str):
        try:
            df1 = alice.today_1min(sym)
            o, c, body = first_minute_metrics(df1)
            return sym, o, c, body
        except Exception as e:
            log.warning(f"{sym}: fetch failed: {e}")
            return sym, None, None, None

    symbols = master["symbol"].tolist()
    results: Dict[str, Tuple[Optional[float], Optional[float], Optional[float]]] = {}
    with ThreadPoolExecutor(max_workers=cfg.alice_workers) as ex:
        futs = {ex.submit(fetch_one, s): s for s in symbols}
        for f in as_completed(futs):
            s, o, c, b = f.result()
            results[s] = (o, c, b)

    # Build plans & detect gap-ups
    plans: List[Plan] = []
    for _, r in master.iterrows():
        s = r.symbol
        y = float(r.yclose)
        margin = float(r.margin)
        o, c, b = results.get(s, (None, None, None))
        ok, gap = detect_gap_up(o, y, cfg)
        if not ok:
            continue
        w = compute_weight(is_negative=(c is not None and o is not None and c < o), body_pct=b, cfg=cfg)
        plans.append(Plan(symbol=s, yclose=y, margin=margin, open_915=o, first_close=c, first_body_pct=b, gap_pct=gap, weight=w))

    if not plans:
        log.info("No gap‑up 2–7% symbols today. Exiting.")
        return

    # Capital allocation
    capital_per = cfg.total_capital / len(plans)
    for p in plans:
        ref_price = p.open_915 or p.yclose
        exit_qty = math.floor((capital_per * p.margin) / ref_price)
        p.base_exit_qty = max(1, exit_qty)
        # Weighted tranche per minute
        base_tranche = max(1, round(p.base_exit_qty / cfg.tranche_count))
        p.entry_tranche = max(1, min(base_tranche * (1 if not cfg.use_weighting else round(p.weight, 2)), p.base_exit_qty))

    log.info(f"Gap‑ups: {len(plans)} symbols. Starting entries 09:16–09:25…")
    am = AM(cfg)

    # Minute loop 09:16 .. 09:25
    while NOW() <= LAST_ENTRY_MINUTE():
        for p in plans:
            rem = p.remaining()
            if rem <= 0:
                continue
            q = int(min(p.entry_tranche, rem))
            resp = am.place_short(p.symbol, q)
            if resp.get("ok"):
                p.filled += q
                log.info(f"SELL {p.symbol} x{q} (filled {p.filled}/{p.base_exit_qty})")
            else:
                log.warning(f"Order failed {p.symbol}: {resp}")
        # sleep to next minute boundary
        now = NOW()
        to_next = 60 - now.second
        time.sleep(max(0.5, to_next))

    log.info("Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted by user.")
