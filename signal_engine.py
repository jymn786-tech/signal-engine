#!/usr/bin/env python3
# Stateless analytics from full-minute data + file-based dedup (cached by GitHub Actions)
# Uses repo-root NSE.csv (columns: Symbol, Token) for instrument resolution.

import os
import json
import sys
import urllib3
import pandas as pd
from types import SimpleNamespace
from datetime import datetime, time, timezone, timedelta

from pya3 import Aliceblue

# ===== Required env =====
REQUIRED_VARS = ["ALICE_USER_ID", "ALICE_API_KEY", "SYMBOL_LIST", "WEBHOOK_URL"]
missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if missing:
    print(f"❌ Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

ALICE_USER_ID = os.getenv("ALICE_USER_ID")
ALICE_API_KEY = os.getenv("ALICE_API_KEY")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOL_LIST", "").split(",") if s.strip()]
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Repo-root contract master
CONTRACT_MASTER_DIR = os.getenv("CONTRACT_MASTER_DIR", ".")
NSE_CSV_PATH = os.path.join(CONTRACT_MASTER_DIR, "NSE.csv")
LOCAL_MASTERS = {}  # exchange -> DataFrame with columns: symbol, token

# File-based dedup (cached by Actions)
DEDUP_DIR = os.getenv("DEDUP_DIR", "state")
os.makedirs(DEDUP_DIR, exist_ok=True)

# HTTP + AliceBlue
http = urllib3.PoolManager()
alice = Aliceblue(user_id=str(ALICE_USER_ID), api_key=ALICE_API_KEY)
_ = alice.get_session_id()

# Timezones
tz_utc = timezone.utc
tz_ist = timezone(timedelta(hours=5, minutes=30))

# Tunables
SURGE_PCT = float(os.getenv("SURGE_PCT", "0.02"))  # 2% in production; drop to 0.002 for testing
SURGE_CUTOFF = time(9, 30, 50)   # surge must occur <= 09:30:50 IST
DROP_CUTOFF  = time(12, 0, 59)   # drop must occur <= 12:00:59 IST

# ---------- Local master loader ----------
def load_local_masters():
    """Load repo-root NSE.csv (expects columns: Symbol, Token)."""
    if not os.path.isfile(NSE_CSV_PATH):
        print(f"ℹ️ Local NSE master not found at {NSE_CSV_PATH}.")
        return
    try:
        df = pd.read_csv(NSE_CSV_PATH)
        assert "Symbol" in df.columns and "Token" in df.columns, "NSE.csv must have columns: Symbol, Token"
        df = df.rename(columns={"Symbol": "symbol", "Token": "token"})
        df["symbol"] = df["symbol"].astype(str).str.strip()
        df["token"] = df["token"].astype(str).str.strip()
        LOCAL_MASTERS["NSE"] = df[["symbol", "token"]]
        print(f"📄 Loaded local master for NSE: {NSE_CSV_PATH} ({len(df)} rows)")
    except Exception as e:
        print(f"⚠️ Failed to read {NSE_CSV_PATH}: {e}")

# ---------- Instrument lookup (local CSV first) ----------
def instrument_for_symbol(sym: str, exchange: str = "NSE"):
    # 1) Local CSV
    df = LOCAL_MASTERS.get(exchange)
    if df is not None:
        row = df.loc[df["symbol"] == sym]
        if not row.empty:
            token = row.iloc[0]["token"]
            return SimpleNamespace(exchange=exchange, symbol=sym, token=str(token))
    # 2) pya3 cache
    try:
        return alice.get_instrument_by_symbol(symbol=sym, exchange=exchange)
    except Exception:
        pass
    # 3) search
    try:
        matches = alice.search_instruments(exchange, sym)
        if matches:
            sel = matches[0]
            if getattr(sel, "symbol", None) != sym:
                print(f"🔎 Fallback matched {getattr(sel, 'symbol', 'UNKNOWN')} for {sym} in {exchange}")
            return sel
    except Exception as e:
        print(f"❌ search_instruments failed for {exchange}:{sym}: {e}")
    raise RuntimeError(f"Instrument not found for {exchange}:{sym}. Ensure it's present in NSE.csv.")

# ---------- Dedup helpers ----------
def dedup_path(date_key: str) -> str:
    return os.path.join(DEDUP_DIR, f"signals_{date_key}.json")

def load_dedup(date_key: str) -> set:
    path = dedup_path(date_key)
    if not os.path.isfile(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            arr = json.load(f)
        return set(arr) if isinstance(arr, list) else set()
    except Exception as e:
        print(f"⚠️ Could not read dedup file {path}: {e}")
        return set()

def save_dedup(date_key: str, sent: set) -> None:
    path = dedup_path(date_key)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted(list(sent)), f, separators=(",", ":"))
    except Exception as e:
        print(f"⚠️ Could not write dedup file {path}: {e}")

# ---------- Core analytics ----------
def normalize_hist_df(hist: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df has columns: datetime, open, high, low, close, volume
    Many Aliceblue builds return 'time' instead of 'datetime'; handle both.
    """
    cols = {c.lower(): c for c in hist.columns}
    # rename time column to 'datetime' if needed
    if "datetime" not in cols and "time" in cols:
        hist = hist.rename(columns={cols["time"]: "datetime"})
    # Standardize case
    for name in ["open", "high", "low", "close", "volume"]:
        if name not in hist.columns and name in cols:
            hist = hist.rename(columns={cols[name]: name})
    return hist

def analyze_day(hist: pd.DataFrame, surge_pct: float):
    """
    Returns:
      daily_open (float),
      surge_hit_time (Timestamp or None),
      drop_hit_time  (Timestamp or None),
      surge (bool),
      drop_after_surge (bool)
    """
    if hist.empty or hist.isnull().values.any():
        return None, None, None, False, False

    # Normalize + parse timestamps (assume IST-naive strings from API)
    hist = normalize_hist_df(hist)
    ts = pd.to_datetime(hist["datetime"], errors="coerce")
    hist = hist.assign(ts=ts).dropna(subset=["ts"]).reset_index(drop=True)

    daily_open = float(hist.iloc[0]["open"])
    threshold = daily_open * (1.0 + surge_pct)

    # Surge window: <= 09:30:50
    surge_window = hist[hist["ts"].dt.time <= SURGE_CUTOFF]
    surge = False
    surge_hit_time = None
    if not surge_window.empty:
        breach = surge_window.loc[surge_window["high"] > threshold]
        if not breach.empty:
            surge = True
            surge_hit_time = breach.iloc[0]["ts"]

    # Drop window: after surge and <= 12:00:59
    drop_after_surge = False
    drop_hit_time = None
    if surge:
        drop_window = hist[(hist["ts"] > surge_hit_time) & (hist["ts"].dt.time <= DROP_CUTOFF)]
        if not drop_window.empty:
            hit = drop_window.loc[drop_window["low"] <= daily_open ]
            if not hit.empty:
                drop_after_surge = True
                drop_hit_time = hit.iloc[0]["ts"]

    return daily_open, surge_hit_time, drop_hit_time, surge, drop_after_surge

# ---------- Main ----------
def main():
    now_utc = datetime.now(tz_utc)
    now_ist = now_utc.astimezone(tz_ist)
    print(f"▶️ Invoked at {now_utc.isoformat()} UTC / {now_ist.isoformat()} IST")

    # Run only between 09:15 and 12:01 IST (inclusive)
    start_ist = time(9, 15)
    end_ist   = time(12, 15)
    if not (start_ist <= now_ist.time() <= end_ist):
        print(f"⏸ Outside trading window (IST): {now_ist.time()}. Skipping.")
        return
        
    load_local_masters()

    # Date-only for historical API
    today_str = now_ist.strftime("%d-%m-%Y")
    date_key = now_ist.date().isoformat()
    from_date = datetime.strptime(today_str, "%d-%m-%Y")
    to_date = datetime.strptime(today_str, "%d-%m-%Y")

    already_sent = load_dedup(date_key)
    print(f"🔔 Already sent: {len(already_sent)} symbols")

    for sym in SYMBOLS:
        try:
            instr = instrument_for_symbol(sym, exchange="NSE")
            hist = alice.get_historical(instr, from_date, to_date, "1", indices=False)

            if isinstance(hist, dict) and "error" in hist:
                print(f"❌ Error fetching data for {sym}: {hist['error']}")
                continue
            if getattr(hist, "empty", True) or hist.isnull().values.any():
                print(f"⚠️ No intraday candles for {sym} yet.")
                continue

            daily_open, surge_t, drop_t, surge, drop = analyze_day(hist, SURGE_PCT)

            # visibility log
            if daily_open is not None:
                s1 = surge_t.strftime("%H:%M:%S") if surge_t is not None else "-"
                s2 = drop_t.strftime("%H:%M:%S") if drop_t is not None else "-"
                print(f"ℹ️ {sym}: open={daily_open:.2f} surge={surge}@{s1} drop_after_surge={drop}@{s2}")

            # Decide signal (within drop window by current time)
            if not (surge and drop and (now_ist.time() <= DROP_CUTOFF)):
                continue

            # Dedup
            if sym in already_sent:
                print(f"↩️ Skipping duplicate signal for {sym}")
                continue

            payload = {
                "stocks": sym,
                "trigger_prices": f"{daily_open:.2f}",
                "triggered_at": now_ist.strftime("%I:%M %p").lower(),
                "scan_name": "Surge & Drop",
                "scan_url": "fuzzy-retest-live",
                "alert_name": "SHORT",
                "webhook_url": WEBHOOK_URL,
            }
            print(f"📡 Sending webhook for {sym}: {payload}")
            resp = http.request(
                "POST",
                WEBHOOK_URL,
                headers={"Content-Type": "application/json"},
                body=json.dumps(payload).encode("utf-8"),
            )
            print(f"🔔 Webhook resp: {resp.status}")
            if resp.status == 200:
                already_sent.add(sym)
                save_dedup(date_key, already_sent)
                print(f"✅ Signal stored for {sym}")
            else:
                print(f"⚠️ Webhook failed for {sym}: {resp.status}")

        except Exception as e:
            print(f"❌ Error for {sym}: {e}")

if __name__ == "__main__":
    main()
