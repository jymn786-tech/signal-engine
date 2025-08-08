#!/usr/bin/env python3

import os
import json
import sys
import urllib3
from datetime import datetime, time, timezone, timedelta
from pya3 import Aliceblue

# === ENVIRONMENT VARIABLES ===
REQUIRED_VARS = [
    "ALICE_USER_ID",
    "ALICE_API_KEY",
    "SYMBOL_LIST",
    "WEBHOOK_URL",
]

# Load required secrets
missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing:
    print(f"❌ Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

# Read optional alternate secret names for Upstash
upstash_url = os.getenv("UPSTASH_REDIS_URL") or os.getenv("UPSTASH_REDIS_REST_URL")
upstash_token = os.getenv("UPSTASH_REDIS_TOKEN") or os.getenv("UPSTASH_REDIS_REST_TOKEN")
if not upstash_url or not upstash_token:
    print(
        "❌ Missing Upstash Redis URL or token (set UPSTASH_REDIS_URL/UPSTASH_REDIS_TOKEN or "
        "UPSTASH_REDIS_REST_URL/UPSTASH_REDIS_REST_TOKEN)"
    )
    sys.exit(1)

ALICE_USER_ID = os.getenv("ALICE_USER_ID")
ALICE_API_KEY = os.getenv("ALICE_API_KEY")
SYMBOL_LIST_RAW = os.getenv("SYMBOL_LIST")
SYMBOLS = [s.strip() for s in SYMBOL_LIST_RAW.split(",") if s.strip()]
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
UPSTASH_URL = upstash_url.rstrip("/")
UPSTASH_TOKEN = upstash_token

# Initialize HTTP client and Aliceblue
http = urllib3.PoolManager()
alice = Aliceblue(user_id=str(ALICE_USER_ID), api_key=ALICE_API_KEY)
session_id = alice.get_session_id()

# Timezone definitions
 tz_utc = timezone.utc
 tz_ist = timezone(timedelta(hours=5, minutes=30))

# --- Upstash Redis helpers ---
def load_states(key):
    url = f"{UPSTASH_URL}/get/{key}"
    r = http.request("GET", url, fields={"token": UPSTASH_TOKEN})
    if r.status == 200:
        payload = json.loads(r.data.decode("utf-8"))
        data = payload.get("result")
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return {}
    return {}


def save_states(key, states):
    url = f"{UPSTASH_URL}/set/{key}"
    body = json.dumps({"value": json.dumps(states)})
    http.request(
        "POST",
        url,
        fields={"token": UPSTASH_TOKEN},
        headers={"Content-Type": "application/json"},
        body=body.encode("utf-8")
    )


def main():
    now_utc = datetime.now(tz_utc)
    now_ist = now_utc.astimezone(tz_ist)
    print(f"▶️ Invoked at {now_utc.isoformat()} UTC / {now_ist.isoformat()} IST")

    # Prepare date-only range for AliceBlue
    today_str = now_ist.strftime("%d-%m-%Y")
    from_date = datetime.strptime(today_str, "%d-%m-%Y")
    to_date = datetime.strptime(today_str, "%d-%m-%Y")

    # Load or initialize today's state
    today_key = now_ist.date().isoformat()
    states = load_states(today_key)
    print(f"🔔 Loaded state for {len(states)} symbols")

    # Initialize daily_open for new symbols
    to_init = [s for s in SYMBOLS if s not in states]
    if to_init:
        print(f"🔔 Initializing daily_open for: {to_init}")
        for sym in to_init:
            try:
                instr = alice.get_instrument_by_symbol(symbol=sym, exchange="NSE")
                hist = alice.get_historical(
                    instr,
                    from_date,
                    to_date,
                    "1",
                    indices=False
                )
                # Handle API errors
                if isinstance(hist, dict) and "error" in hist:
                    print(f"❌ Error fetching data for {sym}: {hist['error']}")
                    continue
                # Skip if no usable data
                if hist.empty or hist.isnull().values.any():
                    print(f"⚠️ No valid data for {sym} on {today_str}.")
                    continue
                # Record today's open
                op = hist.iloc[0]["open"]
                states[sym] = {"daily_open": op, "surge_detected": False, "signal_sent": False}
                print(f"💾 Set daily_open for {sym}: {op}")
            except Exception as e:
                print(f"❌ Init failed for {sym}: {e}")
        save_states(today_key, states)

    # Main loop: detect surge & drop signals
    changed_any = False
    for sym, st in list(states.items()):
        if st.get("signal_sent"):
            continue
        try:
            instr = alice.get_instrument_by_symbol(symbol=sym, exchange="NSE")
            hist = alice.get_historical(
                instr,
                from_date,
                to_date,
                "1",
                indices=False
            )
            if isinstance(hist, dict) and "error" in hist:
                print(f"❌ Error fetching data for {sym}: {hist['error']}")
                continue
            if hist.empty or hist.isnull().values.any():
                continue

            changed = False
            # Surge detection before 9:30
            if not st["surge_detected"] and now_ist.time() <= time(9, 30):
                if hist["high"].max() > st["daily_open"] * 1.02:
                    st["surge_detected"] = True
                    st["surge_time"] = now_ist.isoformat()
                    changed = True
                    print(f"🚀 Surge {sym}: high {hist['high'].max()}")
            # Drop signal before 12:00:59
            if st["surge_detected"] and not st["signal_sent"] and now_ist.time() <= time(12, 0, 59):
                if hist["low"].min() <= st["daily_open"]:
                    payload = {
                        "stocks": sym,
                        "trigger_prices": str(st["daily_open"]),
                        "triggered_at": now_ist.strftime("%I:%M %p").lower(),
                        "scan_name": "Surge & Drop",
                        "scan_url": "fuzzy-retest-live",
                        "alert_name": "SHORT",
                        "webhook_url": WEBHOOK_URL
                    }
                    print(f"📡 Sending webhook for {sym}: {payload}")
                    resp = http.request(
                        "POST", WEBHOOK_URL,
                        headers={"Content-Type": "application/json"},
                        body=json.dumps(payload).encode("utf-8")
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

    # Persist state if anything changed
    if changed_any:
        save_states(today_key, states)
        print(f"💾 State saved ({len(states)} symbols)")
    else:
        print("💾 No state changes")


if __name__ == "__main__":
    main()
