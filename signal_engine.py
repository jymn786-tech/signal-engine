#!/usr/bin/env python3
# Uses local NSE.csv (repo root) for Symbol→Token resolution.
# No contract-master download needed on GitHub Actions.

import os
import json
import sys
import urllib3
from datetime import datetime, time, timezone, timedelta
from types import SimpleNamespace

import pandas as pd
from pya3 import Aliceblue

# === REQUIRED ENV VARS ===
REQUIRED_VARS = ["ALICE_USER_ID", "ALICE_API_KEY", "SYMBOL_LIST", "WEBHOOK_URL"]
missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if missing:
    print(f"❌ Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

# Upstash (support both legacy and REST names)
upstash_url   = os.getenv("UPSTASH_REDIS_URL") or os.getenv("UPSTASH_REDIS_REST_URL")
upstash_token = os.getenv("UPSTASH_REDIS_TOKEN") or os.getenv("UPSTASH_REDIS_REST_TOKEN")
if not upstash_url or not upstash_token:
    print("❌ Missing Upstash Redis URL or token (UPSTASH_REDIS_URL/UPSTASH_REDIS_TOKEN or UPSTASH_REDIS_REST_URL/UPSTASH_REDIS_REST_TOKEN)")
    sys.exit(1)

ALICE_USER_ID   = os.getenv("ALICE_USER_ID")
ALICE_API_KEY   = os.getenv("ALICE_API_KEY")
SYMBOLS         = [s.strip() for s in os.getenv("SYMBOL_LIST", "").split(",") if s.strip()]
WEBHOOK_URL     = os.getenv("WEBHOOK_URL")
UPSTASH_URL     = upstash_url.rstrip("/")
UPSTASH_TOKEN   = upstash_token

# Local contract master settings
# Your NSE.csv is at repo root; override via CONTRACT_MASTER_DIR if needed.
CONTRACT_MASTER_DIR = os.getenv("CONTRACT_MASTER_DIR", ".")
NSE_CSV_PATH        = os.path.join(CONTRACT_MASTER_DIR, "NSE.csv")
LOCAL_MASTERS       = {}  # exchange -> DataFrame with columns: symbol, token

# HTTP + AliceBlue
http  = urllib3.PoolManager()
alice = Aliceblue(user_id=str(ALICE_USER_ID), api_key=ALICE_API_KEY)
_ = alice.get_session_id()

# Timezones
tz_utc = timezone.utc
tz_ist = timezone(timedelta(hours=5, minutes=30))


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
        df["token"]  = df["token"].astype(str).str.strip()
        LOCAL_MASTERS["NSE"] = df[["symbol", "token"]]
        print(f"📄 Loaded local master for NSE: {NSE_CSV_PATH} ({len(df)} rows)")
    except Exception as e:
        print(f"⚠️ Failed to read {NSE_CSV_PATH}: {e}")


# ---------- Upstash helpers ----------
def load_states(key: str) -> dict:
    # Query param style (no body)
    url = f"{UPSTASH_URL}/get/{key}?token={UPSTASH_TOKEN}"
    r = http.request("GET", url)
    if r.status == 200:
        payload = json.loads(r.data.decode("utf-8"))
        data = payload.get("result")
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return {}
    return {}

def save_states(key: str, states: dict) -> None:
    url = f"{UPSTASH_URL}/set/{key}?token={UPSTASH_TOKEN}"
    body = json.dumps({"value": json.dumps(states)})
    http.request(
        "POST",
        url,
        headers={"Content-Type": "application/json"},
        body=body.encode("utf-8"),
    )


# ---------- Instrument lookup (local CSV first) ----------
def instrument_for_symbol(sym: str, exchange: str = "NSE"):
    """
    1) Try local CSV (Symbol/Token) -> build a lightweight instrument object.
    2) Fallback to pya3 cache lookup.
    3) Fallback to pya3 search.
    """
    # Local CSV
    df = LOCAL_MASTERS.get(exchange)
    if df is not None:
        row = df.loc[df["symbol"] == sym]
        if not row.empty:
            token = row.iloc[0]["token"]
            # get_historical accepts objects with .exchange and .token
            return SimpleNamespace(exchange=exchange, symbol=sym, token=str(token))

    # pya3 cache lookup
    try:
        return alice.get_instrument_by_symbol(symbol=sym, exchange=exchange)
    except Exception:
        pass

    # search fallback
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


# ---------- Main ----------
def main():
    now_utc = datetime.now(tz_utc)
    now_ist = now_utc.astimezone(tz_ist)
    print(f"▶️ Invoked at {now_utc.isoformat()} UTC / {now_ist.isoformat()} IST")

    # Load local contract master once
    load_local_masters()

    # Date-only range for AliceBlue historical API
    today_str = now_ist.strftime("%d-%m-%Y")
    from_date = datetime.strptime(today_str, "%d-%m-%Y")
    to_date   = datetime.strptime(today_str, "%d-%m-%Y")

    # Load today's state
    today_key = now_ist.date().isoformat()
    states = load_states(today_key)
    print(f"🔔 Loaded state for {len(states)} symbols")

    # Initialize daily_open
    to_init = [s for s in SYMBOLS if s not in states]
    if to_init:
        print(f"🔔 Initializing daily_open for: {to_init}")
        for sym in to_init:
            try:
                instr = instrument_for_symbol(sym, exchange="NSE")
                hist = alice.get_historical(instr, from_date, to_date, "1", indices=False)

                if isinstance(hist, dict) and "error" in hist:
                    print(f"❌ Error fetching data for {sym}: {hist['error']}")
                    continue
                if getattr(hist, "empty", True) or hist.isnull().values.any():
                    print(f"⚠️ No valid data for {sym} on {today_str}.")
                    continue

                op = float(hist.iloc[0]["open"])
                states[sym] = {"daily_open": op, "surge_detected": False, "signal_sent": False}
                print(f"💾 Set daily_open for {sym}: {op}")
            except Exception as e:
                print(f"❌ Init failed for {sym}: {e}")
        save_states(today_key, states)

    # Detect surge & drop
    changed_any = False
    for sym, st in list(states.items()):
        if st.get("signal_sent"):
            continue
        try:
            instr = instrument_for_symbol(sym, exchange="NSE")
            hist = alice.get_historical(instr, from_date, to_date, "1", indices=False)

            if isinstance(hist, dict) and "error" in hist:
                print(f"❌ Error fetching data for {sym}: {hist['error']}")
                continue
            if getattr(hist, "empty", True) or hist.isnull().values.any():
                print(f"⚠️ No intraday candles for {sym} yet.")
                continue

            changed = False

            # Surge before 09:30 IST
            if not st.get("surge_detected") and now_ist.time() <= time(9, 30):
                high_max = float(hist["high"].max())
                if high_max > st["daily_open"] * 1.02:
                    st["surge_detected"] = True
                    st["surge_time"] = now_ist.isoformat()
                    changed = True
                    print(f"🚀 Surge {sym}: high {high_max}")

            # Drop before 12:00:59 IST
            if st.get("surge_detected") and not st.get("signal_sent") and now_ist.time() <= time(12, 0, 59):
                low_min = float(hist["low"].min())
                if low_min <= st["daily_open"]:
                    payload = {
                        "stocks": sym,
                        "trigger_prices": str(st["daily_open"]),
                        "triggered_at": now_ist.strftime("%I:%M %p").lower(),
                        "scan_name": "Surge & Drop",
                        "scan_url": "fuzzy-retest-live",
                        "alert_name": "SHORT",
                        "webhook_url": WEBHOOK_URL,
                    }
                    print(f"📡 Sending webhook for {sym}: {payload}")
                    resp = http.request(
                        "POST", WEBHOOK_URL,
                        headers={"Content-Type": "application/json"},
                        body=json.dumps(payload).encode("utf-8"),
                    )
                    print(f"🔔 Webhook resp: {resp.status}")
                    if resp.status == 200:
                        st["signal_sent"] = True
                        changed = True
                        print(f"✅ Signal for {sym}")

            if changed:
                states[sym] = st
                changed_any = True
            else:
                print(f"💾 No change for {sym}")
        except Exception as e:
            print(f"❌ Error for {sym}: {e}")

    if changed_any:
        save_states(today_key, states)
        print(f"💾 State saved ({len(states)} symbols)")
    else:
        print("💾 No state changes")


if __name__ == "__main__":
    main()
