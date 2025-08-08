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
    "UPSTASH_REDIS_URL",
    "UPSTASH_REDIS_TOKEN"
]
# Check that all required env vars are present
missing = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing:
    print(f"❌ Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

ALICE_USER_ID   = os.getenv("ALICE_USER_ID")
ALICE_API_KEY   = os.getenv("ALICE_API_KEY")
SYMBOL_LIST_RAW = os.getenv("SYMBOL_LIST")
SYMBOLS         = [s.strip() for s in SYMBOL_LIST_RAW.split(",") if s.strip()]
WEBHOOK_URL     = os.getenv("WEBHOOK_URL")
UPSTASH_URL     = os.getenv("UPSTASH_REDIS_URL").rstrip("/")
UPSTASH_TOKEN   = os.getenv("UPSTASH_REDIS_TOKEN")

# Initialize HTTP client and Aliceblue
http  = urllib3.PoolManager()
alice = Aliceblue(user_id=str(ALICE_USER_ID), api_key=ALICE_API_KEY)
session_id = alice.get_session_id()

# Timezone definitions
tz_utc = timezone.utc
tz_ist = timezone(timedelta(hours=5, minutes=30))

# --- Upstash Redis helpers ---
def load_states(key):
    """
    Fetch JSON-encoded state from Upstash Redis for the given key.
    Returns a dict or empty dict if missing.
    """
    url = f"{UPSTASH_URL}/get/{key}"
    r = http.request("GET", url, fields={"token": UPSTASH_TOKEN})
    if r.status == 200:
        payload = json.loads(r.data.decode('utf-8'))
        data = payload.get("result")
        if data:
            try:
                return json.loads(data)
            except json.JSONDecodeError:
                return {}
    return {}


def save_states(key, states):
    """
    Save a Python dict as JSON into Upstash Redis under the given key.
    """
    url = f"{UPSTASH_URL}/set/{key}"
    body = json.dumps({"value": json.dumps(states)})
    http.request(
        "POST",
        url,
        fields={"token": UPSTASH_TOKEN},
        headers={"Content-Type": "application/json"},
        body=body.encode('utf-8')
    )


def main():
    # Compute current UTC and IST times
    now_utc = datetime.now(tz_utc)
    now_ist = now_utc.astimezone(tz_ist)
    print(f"▶️ Invoked at {now_utc.isoformat()} UTC / {now_ist.isoformat()} IST")

    # Gate: only run Mon–Fri 09:15–15:00 IST
    if now_ist.weekday() > 4 or not (time(9, 15) <= now_ist.time() <= time(15, 0)):
        print(f"⏸ Outside market hours: {now_ist.time()}")
        return

    # Use date key for storing state
    today_key = now_ist.date().isoformat()

    # Load persisted state (dict of symbol → state dict)
    states = load_states(today_key)
    print(f"🔔 Loaded state for {len(states)} symbols")

    # 1) Initialize daily_open for new symbols
    to_init = [s for s in SYMBOLS if s not in states]
    if to_init:
        print(f"🔔 Initializing daily_open for: {to_init}")
        for sym in to_init:
            try:
                instr = alice.get_instrument_by_symbol(symbol=sym, exchange="NSE")
                hist  = alice.get_historical(
                    instr,
                    now_ist.replace(hour=0, minute=0, second=0, microsecond=0),
                    now_ist,
                    "1",
                    indices=False
                )
                if not hist.empty:
                    op = hist.iloc[0]['open']
                    states[sym] = {"daily_open": op, "surge_detected": False, "signal_sent": False}
                    print(f"💾 Set daily_open for {sym}: {op}")
            except Exception as e:
                print(f"❌ Init failed for {sym}: {e}")
        # Persist initial state
        save_states(today_key, states)

    # 2) Per-minute OHLCV check for surge & drop
    changed_any = False
    for sym, st in list(states.items()):
        if st.get("signal_sent"):
            continue
        try:
            instr = alice.get_instrument_by_symbol(symbol=sym, exchange="NSE")
            hist  = alice.get_historical(
                instr,
                now_ist.replace(hour=0, minute=0, second=0, microsecond=0),
                    now_ist,
                    "1",
                    indices=False
            )
            if hist.empty:
                continue

            changed = False
            # Surge detection (>2% within first 15m)
            if not st["surge_detected"] and now_ist.time() <= time(9, 30):
                if hist['high'].max() > st['daily_open'] * 1.02:
                    st['surge_detected'] = True
                    st['surge_time'] = now_ist.isoformat()
                    changed = True
                    print(f"🚀 Surge {sym}: high {hist['high'].max()}")

            # Drop detection (≤ open before 12:00:59)
            if st['surge_detected'] and not st['signal_sent'] and now_ist.time() <= time(12, 0, 59):
                if hist['low'].min() <= st['daily_open']:
                    payload = {
                        "stocks": sym,
                        "trigger_prices": str(st['daily_open']),
                        "triggered_at": now_ist.strftime("%I:%M %p").lower(),
                        "scan_name": "Surge & Drop",
                        "scan_url": "fuzzy-retest-live",
                        "alert_name": "SHORT",
                        "webhook_url": WEBHOOK_URL
                    }
                    print(f"📡 Sending webhook for {sym}: {payload}")
                    resp = http.request(
                        "POST",
                        WEBHOOK_URL,
                        headers={"Content-Type": "application/json"},
                        body=json.dumps(payload).encode('utf-8')
                    )
                    print(f"🔔 Webhook resp: {resp.status}")
                    if resp.status == 200:
                        st['signal_sent'] = True
                        changed = True
                        print(f"✅ Signal for {sym}")

            if changed:
                states[sym] = st
                changed_any = True
            else:
                print(f"💾 No change for {sym}")

        except Exception as e:
            print(f"❌ Error for {sym}: {e}")

    # 3) Persist updated state if any changes
    if changed_any:
        save_states(today_key, states)
        print(f"💾 State saved ({len(states)} symbols)")
    else:
        print("💾 No state changes")

if __name__ == "__main__":
    main()
