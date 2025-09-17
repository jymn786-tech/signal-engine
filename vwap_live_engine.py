#!/usr/bin/env python3
"""
Gap-Up Fade Engine (AliceBlue pya3 → Algomojo)
----------------------------------------------

- Loads master.csv from same repo directory (fallback: GITHUB_MASTER_CSV_URL)
- At 09:16 IST: fetches today’s 1-min OHLCV for Tradables
- Detects gap-up (2–7%) using YesterdayClose
- Allocates TOTAL_CAPITAL equally across gap-ups
- Places SELL MARKET tranches each minute (09:16–09:25)
"""

import os, sys, math, json, time, logging
from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
from typing import List
import urllib3, pytz, requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pya3 import Aliceblue

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("gap_fade")

# -------------------- Time helpers --------------------
IST = pytz.timezone("Asia/Kolkata")
NOW = lambda: datetime.now(IST)
MARKET_OPEN = lambda: NOW().replace(hour=9, minute=15, second=0, microsecond=0)
LAST_ENTRY_MINUTE = lambda: MARKET_OPEN() + timedelta(minutes=10)

# -------------------- Config --------------------
@dataclass
class Config:
    alice_user_id: str = os.getenv("ALICE_USER_ID", "")
    alice_api_key: str = os.getenv("ALICE_API_KEY", "")

    master_local_path: str = os.getenv("MASTER_LOCAL_PATH", "master.csv")
    github_master_csv_url: str = os.getenv("GITHUB_MASTER_CSV_URL", "")

    total_capital: float = float(os.getenv("TOTAL_CAPITAL", 100000))
    tranche_count: int = int(os.getenv("TRANCHE_COUNT", 10))
    band_min: float = float(os.getenv("BAND_MIN", 10))
    margin_required: float = float(os.getenv("MARGIN_REQUIRED", 5))
    gap_min_pct: float = float(os.getenv("GAP_MIN_PCT", 2.0))
    gap_max_pct: float = float(os.getenv("GAP_MAX_PCT", 7.0))

    alice_workers: int = int(os.getenv("ALICE_WORKERS", 12))

    algomojo_url: str = os.getenv("ALGOMOJO_URL", "")
    algomojo_api_key: str = os.getenv("ALGOMOJO_API_KEY", "")
    algomojo_secret: str = os.getenv("ALGOMOJO_SECRET", "")
    algomojo_broker: str = os.getenv("ALGOMOJO_BROKER", "AB")
    webhook_url: str = os.getenv("WEBHOOK_URL", "")

    dry_run: bool = bool(int(os.getenv("DRY_RUN", "1")))

http = urllib3.PoolManager()

# -------------------- Load master.csv --------------------
def load_master(cfg: Config) -> pd.DataFrame:
    if os.path.isfile(cfg.master_local_path):
        log.info(f"Loading master.csv locally ({cfg.master_local_path})")
        df = pd.read_csv(cfg.master_local_path)
    elif cfg.github_master_csv_url:
        log.info("Local master.csv not found. Fetching from GitHub…")
        r = http.request("GET", cfg.github_master_csv_url,
                         timeout=urllib3.Timeout(connect=3, read=5))
        if r.status != 200:
            raise RuntimeError(f"Master fetch failed: HTTP {r.status}")
        from io import StringIO
        df = pd.read_csv(StringIO(r.data.decode("utf-8")))
    else:
        raise FileNotFoundError("No master.csv found")

    # Normalize headers
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    rename = {}
    for c in df.columns:
        if c in {"symbol", "stock", "tradingsymbol"}: rename[c] = "symbol"
        if "close" in c: rename[c] = "yclose"
        if "band" in c: rename[c] = "band"
        if "margin" in c: rename[c] = "margin"
    df = df.rename(columns=rename)

    # Handle "No Band"
    df["band"] = df["band"].apply(
        lambda x: 999 if str(x).strip().lower() == "no band" else x
    )
    df["band"] = pd.to_numeric(df["band"], errors="coerce")

    # Ensure required columns
    need = {"symbol", "yclose", "band", "margin"}
    if not need.issubset(df.columns):
        raise ValueError(f"master.csv missing {need}")

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df = df[(df["band"] >= cfg.band_min) & (df["margin"] == cfg.margin_required)]
    return df[["symbol", "yclose", "band", "margin"]]


# -------------------- AliceBlue helpers --------------------
def alice_connect(cfg: Config) -> Aliceblue:
    alice = Aliceblue(user_id=cfg.alice_user_id, api_key=cfg.alice_api_key)
    _ = alice.get_session_id()
    return alice

def fetch_today_ohlc(alice: Aliceblue, symbol: str):
    """
    Fetch today's 1-minute OHLCV for a given NSE symbol.
    Uses positional arguments for get_historical (same as your VWAP script).
    """
    try:
        instr = alice.get_instrument_by_symbol(symbol=symbol, exchange="NSE")

        # Today’s date as datetime
        today_str = NOW().strftime("%d-%m-%Y")
        from_date = datetime.strptime(today_str, "%d-%m-%Y")
        to_date   = datetime.strptime(today_str, "%d-%m-%Y")

        # Call in positional style (no keyword args!)
        df = alice.get_historical(instr, from_date, to_date, "1", indices=False)

        # Handle error conditions
        if isinstance(df, dict) and "error" in df:
            log.error(f"{symbol}: {df['error']}")
            return None
        if df is None or getattr(df, "empty", True):
            log.warning(f"{symbol}: no data returned")
            return None

        return df

    except Exception as e:
        log.error(f"Fetch fail {symbol}: {e}")
        return None


# -------------------- Core logic --------------------
def detect_gapups(cfg: Config, alice: Aliceblue, master: pd.DataFrame) -> List[dict]:
    gapups = []
    with ThreadPoolExecutor(max_workers=cfg.alice_workers) as exe:
        futs = {exe.submit(fetch_today_ohlc, alice, row.symbol): row
                for row in master.itertuples()}
        for fut in as_completed(futs):
            row = futs[fut]
            hist = fut.result()
            if hist is None: continue
            try:
                first_candle = hist.iloc[0]
                openp = float(first_candle["open"])
                yclose = row.yclose
                gap_pct = (openp - yclose) / yclose * 100
                if cfg.gap_min_pct <= gap_pct <= cfg.gap_max_pct:
                    gapups.append({
                        "symbol": row.symbol,
                        "yclose": yclose,
                        "open": openp,
                        "gap_pct": gap_pct,
                        "margin": row.margin,
                    })
            except Exception as e:
                log.error(f"Error analyzing {row.symbol}: {e}")
    return gapups

def allocate_and_trade(cfg: Config, gapups: List[dict]):
    if not gapups:
        log.info("No gap-ups found today.")
        return

    cap_per = cfg.total_capital / len(gapups)
    for g in gapups:
        exit_qty = math.floor((cap_per * g["margin"]) / g["open"])
        if exit_qty <= 0: continue
        tranche = max(1, round(exit_qty / cfg.tranche_count))
        log.info(f"{g['symbol']}: gap {g['gap_pct']:.2f}% | exit={exit_qty} | tranche={tranche}")

        # Fire tranches (09:16–09:25)
        for i in range(cfg.tranche_count):
            tstamp = MARKET_OPEN() + timedelta(minutes=i+1)
            if NOW() < tstamp:
                sleep_s = (tstamp - NOW()).total_seconds()
                time.sleep(max(0, sleep_s))
            place_order(cfg, g["symbol"], tranche)

# -------------------- Order sending --------------------
def place_order(cfg: Config, symbol: str, qty: int):
    payload = {
        "symbol": symbol,
        "qty": qty,
        "side": "SELL",
        "type": "MARKET",
        "product": "MIS",
        "broker": cfg.algomojo_broker,
        "api_key": cfg.algomojo_api_key,
        "secret": cfg.algomojo_secret,
    }
    if cfg.dry_run:
        log.info(f"[DRY] Order {payload}")
        return

    try:
        if cfg.algomojo_url:
            r = requests.post(cfg.algomojo_url, json=payload, timeout=5)
            log.info(f"Order {symbol} resp {r.status_code}")
        elif cfg.webhook_url:
            r = requests.post(cfg.webhook_url, json=payload, timeout=5)
            log.info(f"Webhook {symbol} resp {r.status_code}")
        else:
            log.warning("No Algomojo or webhook URL set.")
    except Exception as e:
        log.error(f"Order send fail {symbol}: {e}")

# -------------------- Main --------------------
def main():
    cfg = Config()
    if not cfg.alice_user_id or not cfg.alice_api_key:
        log.error("Missing ALICE_USER_ID / ALICE_API_KEY")
        sys.exit(1)

    # Wait until 09:16 IST
    while NOW() < MARKET_OPEN() + timedelta(minutes=1):
        time.sleep(1)

    master = load_master(cfg)
    alice = alice_connect(cfg)
    gapups = detect_gapups(cfg, alice, master)
    allocate_and_trade(cfg, gapups)

if __name__ == "__main__":
    main()
