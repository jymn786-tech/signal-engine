import os
import sys
import json
import urllib3
import pandas as pd
from datetime import datetime, time, timezone, timedelta

from pya3 import Aliceblue

# ===== Required env =====
REQUIRED_VARS = ["ALICE_USER_ID", "ALICE_API_KEY", "WEBHOOK_URL"]
missing = [v for v in REQUIRED_VARS if not os.getenv(v)]
if missing:
    print(f"❌ Missing environment variables: {', '.join(missing)}")
    sys.exit(1)

ALICE_USER_ID = os.getenv("ALICE_USER_ID")
ALICE_API_KEY = os.getenv("ALICE_API_KEY")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Dedup folder
DEDUP_DIR = os.getenv("DEDUP_DIR", "state")
os.makedirs(DEDUP_DIR, exist_ok=True)

# File: Continue/Reversal stocks list
COMBINED_STOCKS = "combined_stocks.csv"

# HTTP + AliceBlue
http = urllib3.PoolManager()
alice = Aliceblue(user_id=str(ALICE_USER_ID), api_key=ALICE_API_KEY)
_ = alice.get_session_id()

# Timezones
tz_utc = timezone.utc
tz_ist = timezone(timedelta(hours=5, minutes=30))

# Tunables
VWAP_THRESHOLD = float(os.getenv("VWAP_THRESHOLD", "2.0"))  # deviation % threshold
ENTRY_CUTOFF = time(12, 0, 0)  # must trigger before this time

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
    except Exception:
        return set()

def save_dedup(date_key: str, sent: set) -> None:
    path = dedup_path(date_key)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(list(sent)), f, separators=(",", ":"))

# ---------- Local master loader ----------
def load_stock_classification():
    """Load Continue/Reversal classification from combined_stocks.csv"""
    try:
        df = pd.read_csv(COMBINED_STOCKS)
        df["Symbol"] = df["Symbol"].astype(str).str.strip().str.upper()
        return dict(zip(df["Symbol"], df["Type"]))
    except Exception as e:
        print(f"❌ Failed to load {COMBINED_STOCKS}: {e}")
        sys.exit(1)

# ---------- Instrument lookup ----------
def instrument_for_symbol(sym: str, exchange: str = "NSE"):
    try:
        return alice.get_instrument_by_symbol(symbol=sym, exchange=exchange)
    except Exception:
        try:
            matches = alice.search_instruments(exchange, sym)
            if matches:
                return matches[0]
        except Exception as e:
            print(f"❌ search_instruments failed for {exchange}:{sym}: {e}")
    raise RuntimeError(f"Instrument not found for {exchange}:{sym}")

# ---------- VWAP deviation analyzer ----------
def analyze_vwap(hist: pd.DataFrame, threshold: float):
    """
    Compute VWAP deviation and check if signal triggered.
    Returns: (signal_type: str, triggered_at: datetime) or (None, None)
    """
    if hist.empty or hist.isnull().values.any():
        return None, None

    # Normalize columns
    cols = {c.lower(): c for c in hist.columns}
    if "datetime" not in hist.columns and "time" in cols:
        hist = hist.rename(columns={cols["time"]: "datetime"})

    hist["ts"] = pd.to_datetime(hist["datetime"], errors="coerce")
    hist = hist.dropna(subset=["ts"]).reset_index(drop=True)

    # Cumulative VWAP intraday
    hist["date"] = hist["ts"].dt.date
    hist["cum_volume"] = hist.groupby("date")["volume"].cumsum()
    hist["cum_vp"] = (hist["close"] * hist["volume"]).groupby(hist["date"]).cumsum()
    hist["vwap"] = hist["cum_vp"] / hist["cum_volume"]
    hist["deviation"] = (hist["close"] - hist["vwap"]) / hist["vwap"] * 100

    # Filter before cutoff
    mask = hist["ts"].dt.time < ENTRY_CUTOFF
    candidates = hist.loc[mask & (hist["deviation"].abs() >= threshold)]

    if candidates.empty:
        return None, None

    row = candidates.iloc[-1]  # latest deviation
    sig_type = "ABOVE_VWAP" if row["deviation"] > 0 else "BELOW_VWAP"
    return sig_type, row["ts"]

# ---------- Main ----------
def main():
    now_utc = datetime.now(tz_utc)
    now_ist = now_utc.astimezone(tz_ist)
    print(f"▶️ Invoked at {now_utc.isoformat()} UTC / {now_ist.isoformat()} IST")

    # Run only in trading hours
    start_ist = time(9, 15)
    end_ist   = time(15, 15)
    if not (start_ist <= now_ist.time() <= end_ist):
        print(f"⏸ Outside trading window: {now_ist.time()}. Skipping.")
        return

    # Load classification
    stock_map = load_stock_classification()
    symbols = list(stock_map.keys())

    # Date for historical API
    today_str = now_ist.strftime("%d-%m-%Y")
    date_key = now_ist.date().isoformat()
    from_date = datetime.strptime(today_str, "%d-%m-%Y")
    to_date = datetime.strptime(today_str, "%d-%m-%Y")

    already_sent = load_dedup(date_key)
    print(f"🔔 Already sent: {len(already_sent)} symbols")

    for sym in symbols:
        try:
            instr = instrument_for_symbol(sym, exchange="NSE")
            hist = alice.get_historical(instr, from_date, to_date, "1", indices=False)

            if isinstance(hist, dict) and "error" in hist:
                print(f"❌ Error fetching data for {sym}: {hist['error']}")
                continue
            if getattr(hist, "empty", True):
                print(f"⚠️ No data yet for {sym}")
                continue

            sig_type, trig_time = analyze_vwap(hist, VWAP_THRESHOLD)

            if sig_type is None:
                continue

            # Map to BUY/SHORT
            classification = stock_map.get(sym, "Unknown")
            action = None
            if classification == "Reversal":
                action = "SHORT" if sig_type == "ABOVE_VWAP" else "BUY"
            elif classification == "Continue":
                if sig_type == "ABOVE_VWAP":
                    action = "BUY"
                else:
                    continue  # skip BELOW_VWAP for Continue

            if action is None:
                continue

            # Dedup
            if sym in already_sent:
                print(f"↩️ Skipping duplicate for {sym}")
                continue

            payload = {
                "stocks": sym,
                "alert_name": action,
                "webhook_url": WEBHOOK_URL,
            }
            print(f"📡 Sending {action} signal for {sym} ({sig_type}, {classification})")
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
                print(f"✅ Stored signal for {sym}")
            else:
                print(f"⚠️ Webhook failed for {sym}: {resp.status}")

        except Exception as e:
            print(f"❌ Error for {sym}: {e}")

if __name__ == "__main__":
    main()
