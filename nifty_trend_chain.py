#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIFTY Trend-Chain Engine → Algomojo Webhooks (Long & Short separated)
---------------------------------------------------------------------

Same as previous version, but uses:
- ALGOMOJO_WEBHOOK_LONG for BUY/SELL
- ALGOMOJO_WEBHOOK_SHORT for SHORT/COVER
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
MARKET_CLOSE = lambda: AT(15, 0)

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
    df_before = df.loc[df.index < eval_time]
    if len(df_before) < 61:
        return 0
    day_open = float(df_before.iloc[0]["open"])
    last_close = float(df_before.iloc[-1]["close"])
    hour_start = float(df_before.iloc[-60]["close"])
    day_tr = sign(last_close - day_open)
    hour_tr = sign(last_close - hour_start)
    return day_tr if day_tr == hour_tr and day_tr != 0 else 0

def direction_to_action(direction: int) -> str:
    return "BUY" if direction == 1 else "SHORT"

def opposite_action(action: str) -> str:
    return {"BUY": "SELL", "SELL": "BUY", "SHORT": "COVER", "COVER": "SHORT"}[action]

def can_hold_full_hour(t): return (t + timedelta(minutes=60)) <= MARKET_CLOSE()

class Position:
    def __init__(self, direction, entry, exit):
        self.direction = direction
        self.entry_time = entry
        self.exit_time = exit
    def __repr__(self):
        return f"<{'LONG' if self.direction==1 else 'SHORT'} {self.entry_time.time()}→{self.exit_time.time()}>"

def wait_until(ts, poll): 
    while NOW() < ts: time.sleep(poll)

# -------------------- Engine --------------------
def run_trend_chain(alice, cfg):
    # Wait until 11:30
    first_slot = EVAL_SLOTS[0]
    if NOW() < first_slot:
        log.info(f"Waiting until first slot {first_slot.time()} IST…")
        wait_until(first_slot, cfg.poll_sec)

    active: Optional[Position] = None

    for slot in EVAL_SLOTS:
        if not can_hold_full_hour(slot):
            continue

        now = NOW()
        if now > slot and not within_grace(now, slot):
            continue
        if now < slot:
            wait_until(slot, cfg.poll_sec)

        df = fetch_today_1min_nifty(alice, cfg)
        direction = compute_direction(df, slot)
        if active is None and direction != 0 and within_grace(NOW(), slot):
            action = direction_to_action(direction)
            log.info(f"[{slot.time()}] ENTRY {action}")
            send_algomojo_signal(cfg, action)
            active = Position(direction, slot, slot + timedelta(minutes=60))

        if active and within_grace(NOW(), active.exit_time):
            exit_action = opposite_action(direction_to_action(active.direction))
            log.info(f"[{NOW().time()}] EXIT {exit_action}")
            send_algomojo_signal(cfg, exit_action)
            active = None

# -------------------- Main --------------------
def main():
    cfg = Config()
    validate_env(cfg)
    alice = alice_connect(cfg)
    run_trend_chain(alice, cfg)

if __name__ == "__main__":
    main()
