#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIFTY Trend-Chain Engine → Algomojo Webhooks (Long & Short separated)
---------------------------------------------------------------------

Updated trade logic:
- MARKET_CLOSE to 15:15
- Last-hour trend uses open@window_start → close@t-1
- 30-min chaining: extend on agreement, exit on disagreement
"""

import os, sys, time, logging, pytz, requests
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
from pya3 import Aliceblue

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("nifty_trend_chain")

# -------------------- Time helpers --------------------
IST = pytz.timezone("Asia/Kolkata")

def NOW():
    return datetime.now(IST).replace(second=0, microsecond=0)

def AT(h, m):
    n = NOW()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

MARKET_OPEN  = lambda: AT(10, 0)
MARKET_CLOSE = lambda: AT(15, 15)  # ⟵ was 15:00

EVAL_SLOTS = [AT(11,30), AT(12,0), AT(12,30), AT(13,0), AT(13,30), AT(14,0)]
GRACE_SEC = int(os.getenv("GRACE_SEC", "20"))

def within_grace(now_ts, target_ts, sec=GRACE_SEC) -> bool:
    return abs((now_ts - target_ts).total_seconds()) <= sec

def market_is_open(ts=None) -> bool:
    ts = ts or NOW()
    return MARKET_OPEN() <= ts <= MARKET_CLOSE()

# -------------------- Config --------------------
@dataclass
class Config:
    alice_user_id: str = os.getenv("ALICE_USER_ID", "")
    alice_api_key: str = os.getenv("ALICE_API_KEY", "")
    nifty_symbol_spot: str = os.getenv("NIFTY_SYMBOL_SPOT", "NIFTY 50")

    # Separate webhooks
    algomojo_webhook_long: str = os.getenv("ALGOMOJO_WEBHOOK_LONG", "")
    algomojo_webhook_short: str = os.getenv("ALGOMOJO_WEBHOOK_SHORT", "")

    # Alert names
    buy_alert_name: str = os.getenv("BUY_ALERT_NAME", "BUY")
    sell_alert_name: str = os.getenv("SELL_ALERT_NAME", "SELL")
    short_alert_name: str = os.getenv("SHORT_ALERT_NAME", "SHORT")
    cover_alert_name: str = os.getenv("COVER_ALERT_NAME", "COVER")

    dry_run: bool = bool(int(os.getenv("DRY_RUN", "1")))
    poll_sec: int = int(os.getenv("POLL_SEC", "5"))

def validate_env(cfg: Config):
    missing = []
    if not cfg.alice_user_id: missing.append("ALICE_USER_ID")
    if not cfg.alice_api_key: missing.append("ALICE_API_KEY")
    if not (cfg.algomojo_webhook_long and cfg.algomojo_webhook_short) and not cfg.dry_run:
        missing.append("ALGOMOJO_WEBHOOK_LONG/SHORT (both required when DRY_RUN=0)")
    if missing:
        log.error(f"Missing environment: {', '.join(missing)}")
        sys.exit(1)

# -------------------- AliceBlue --------------------
def alice_connect(cfg: Config) -> Aliceblue:
    alice = Aliceblue(user_id=cfg.alice_user_id, api_key=cfg.alice_api_key)
    _ = alice.get_session_id()
    return alice

def fetch_today_1min_nifty(alice: Aliceblue, cfg: Config) -> pd.DataFrame:
    instr = alice.get_instrument_by_symbol(exchange="NSE", symbol=cfg.nifty_symbol_spot)
    today = NOW().strftime("%d-%m-%Y")
    from_dt = datetime.strptime(today, "%d-%m-%Y")
    to_dt   = datetime.strptime(today, "%d-%m-%Y")
    df = alice.get_historical(instr, from_dt, to_dt, "1", indices=True)
    if df is None or getattr(df, "empty", True):
        raise RuntimeError("No NIFTY spot data")
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(IST)
    df = df.set_index("datetime").sort_index()
    df = df.loc[(df.index >= MARKET_OPEN()) & (df.index <= MARKET_CLOSE())]
    return df

# -------------------- Webhook sender --------------------
def send_algomojo_signal(cfg: Config, action: str):
    if not market_is_open():
        log.info(f"[SKIP] Market closed; not sending {action}")
        return

    if action in ["BUY", "SELL"]:
        url = cfg.algomojo_webhook_long
        alert = cfg.buy_alert_name if action == "BUY" else cfg.sell_alert_name
    else:
        url = cfg.algomojo_webhook_short
        alert = cfg.short_alert_name if action == "SHORT" else cfg.cover_alert_name

    payload = {"alert_name": alert, "webhook_url": url}

    if cfg.dry_run:
        log.info(f"[DRY] Would send {action} → {url}")
        return

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            log.info(f"📤 Sent {action} signal to Algomojo")
        else:
            log.warning(f"⚠️ {action} failed: {r.status_code}")
    except Exception as e:
        log.error(f"❌ Error sending {action}: {e}")

# -------------------- Trend logic --------------------
def sign(x: float) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)

def compute_direction(df: pd.DataFrame, eval_time) -> int:
    """
    Mixed (agreement) signal at eval_time using ONLY data before eval_time:
      - Passed-day: first bar OPEN of day -> last CLOSE before eval_time
      - Last-hour: 60-min window OPEN -> last CLOSE before eval_time
    Returns: +1 (LONG), -1 (SHORT), 0 (no-trade)
    """
    df_before = df.loc[df.index < eval_time]
    if len(df_before) < 61:  # need at least 60 prior minutes + first bar
        return 0

    day_open   = float(df_before.iloc[0]["open"])
    last_close = float(df_before.iloc[-1]["close"])

    hour_open = float(df_before.iloc[-60]["open"])   # <— open at start of the 60-min window
    day_tr  = sign(last_close - day_open)
    hour_tr = sign(last_close - hour_open)

    return day_tr if (day_tr == hour_tr and day_tr != 0) else 0

def direction_to_action(direction: int) -> str:
    return "BUY" if direction == 1 else "SHORT"

def opposite_action(action: str) -> str:
    return {"BUY": "SELL", "SELL": "BUY", "SHORT": "COVER", "COVER": "SHORT"}[action]

def can_hold_full_hour(t): 
    return (t + timedelta(minutes=60)) <= MARKET_CLOSE()

class Position:
    def __init__(self, direction, entry, exit):
        self.direction = direction
        self.entry_time = entry
        self.exit_time = exit
    def __repr__(self):
        return f"<{'LONG' if self.direction==1 else 'SHORT'} {self.entry_time.time()}→{self.exit_time.time()}>"

def wait_until(ts, poll): 
    while NOW() < ts: time.sleep(poll)

# -------------------- Engine (UPDATED CHAINING LOGIC) --------------------
def run_trend_chain(alice, cfg):
    # Wait until first slot
    first_slot = EVAL_SLOTS[0]
    if NOW() < first_slot:
        log.info(f"Waiting until first slot {first_slot.time()} IST…")
        wait_until(first_slot, cfg.poll_sec)

    active: Optional[Position] = None

    for slot in EVAL_SLOTS:
        # Only consider slots that can hold a full hour into <= 15:15
        if not can_hold_full_hour(slot):
            continue

        # Wake on the slot (within grace)
        now = NOW()
        if now < slot:
            wait_until(slot, cfg.poll_sec)
        now = NOW()
        if not within_grace(now, slot):
            continue  # missed the slot

        # Build Mixed (agreement) at this slot with no look-ahead
        df = fetch_today_1min_nifty(alice, cfg)
        direction = compute_direction(df, slot)  # +1, -1, or 0

        # --- CHAINING: extend-or-exit at boundary BEFORE exiting ---
        if active is not None and within_grace(slot, active.exit_time):
            if direction == active.direction and can_hold_full_hour(slot):
                # Same direction at the next slot → extend +60m, do not exit
                active.exit_time = slot + timedelta(minutes=60)
                log.info(f"[{slot.time()}] EXTEND {'LONG' if active.direction==1 else 'SHORT'} → new exit {active.exit_time.time()}")
                continue
            else:
                # Direction changed/invalid → exit now
                exit_action = opposite_action(direction_to_action(active.direction))
                log.info(f"[{slot.time()}] EXIT {exit_action}")
                send_algomojo_signal(cfg, exit_action)
                active = None
                # do not "continue": we may also enter a fresh trade at this slot

        # --- NEW ENTRY (only if flat and Mixed valid) ---
        if active is None and direction != 0:
            action = direction_to_action(direction)
            log.info(f"[{slot.time()}] ENTRY {action}")
            send_algomojo_signal(cfg, action)
            active = Position(direction, slot, slot + timedelta(minutes=60))

# -------------------- Main --------------------
def main():
    cfg = Config()
    validate_env(cfg)
    alice = alice_connect(cfg)
    run_trend_chain(alice, cfg)

if __name__ == "__main__":
    main()
