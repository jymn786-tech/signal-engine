#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIFTY Trend-Chain Engine → Algomojo Webhook
-------------------------------------------

- Start/launch any time; will wait until 11:30 IST for first evaluation
- 1-min NIFTY spot data (no look-ahead)
- Entry slots: 11:30, 12:00, 12:30, 13:00, 13:30, 14:00 IST
- Enter only if BOTH:
    * Full-day trend (10:00→T-1) and
    * Last-hour trend (T-61→T-1)
  have the SAME sign.
- Base hold = 60 min; at next slot, if same direction & valid, extend exit by +30 (so T→T+60→T+90…)
- Exit on flip/invalid/missing, or if a full hour cannot be held within session (cutoff 15:15)
- Orders via Algomojo webhook: alert_name = "BUY" / "SELL"
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
def NOW(): return datetime.now(IST).replace(second=0, microsecond=0)
def AT(h, m):
    n = NOW()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

MARKET_OPEN  = lambda: AT(10, 0)
MARKET_CLOSE = lambda: AT(15, 15)

EVAL_SLOTS = [AT(11,30), AT(12,0), AT(12,30), AT(13,0), AT(13,30), AT(14,0)]

# -------------------- Config --------------------
@dataclass
class Config:
    # AliceBlue
    alice_user_id: str = os.getenv("ALICE_USER_ID", "")
    alice_api_key: str = os.getenv("ALICE_API_KEY", "")

    # Data symbol (spot index name in AliceBlue; adjust if needed)
    nifty_symbol_spot: str = os.getenv("NIFTY_SYMBOL_SPOT", "NIFTY 50")

    # Algomojo webhook
    algomojo_webhook_url: str = os.getenv(
        "ALGOMOJO_WEBHOOK_URL",
        "https://amapi.algomojo.com/v1/webhook/ze/5835bced68e0c22a4843cc9d3c211a68/23acc0b718e2c1e9cf6c33227e952519/YG5674_ZE_36/PlaceStrategyOrder"
    )
    buy_alert_name: str = os.getenv("BUY_ALERT_NAME", "BUY")
    sell_alert_name: str = os.getenv("SELL_ALERT_NAME", "SELL")

    # Behavior
    dry_run: bool = bool(int(os.getenv("DRY_RUN", "1")))
    poll_sec: int = int(os.getenv("POLL_SEC", "5"))

# -------------------- Broker/Data helpers --------------------
def alice_connect(cfg: Config) -> Aliceblue:
    if not cfg.alice_user_id or not cfg.alice_api_key:
        log.error("Missing ALICE_USER_ID / ALICE_API_KEY")
        sys.exit(1)
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
        raise RuntimeError("No NIFTY spot data received")
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(IST, nonexistent='NaT', ambiguous='NaT')
    df = df.dropna(subset=["datetime"]).set_index("datetime").sort_index()
    df = df.loc[(df.index >= MARKET_OPEN()) & (df.index <= MARKET_CLOSE())]
    return df[["open","high","low","close","volume"]]

# -------------------- Algomojo webhook --------------------
def send_algomojo_signal(cfg: Config, side: str):
    """
    side: "BUY" or "SELL"
    Sends your exact webhook payload format with alert_name and webhook_url.
    """
    alert = cfg.buy_alert_name if side.upper() == "BUY" else cfg.sell_alert_name
    payload = {
        "alert_name": alert,
        "webhook_url": cfg.algomojo_webhook_url
    }
    if cfg.dry_run:
        log.info(f"[DRY] Algomojo {side} → {payload}")
        return

    try:
        r = requests.post(cfg.algomojo_webhook_url, json=payload, timeout=10)
        if r.status_code == 200:
            log.info(f"📤 Sent {side} signal to Algomojo")
        else:
            log.warning(f"⚠️ Algomojo signal failed. Status={r.status_code} Body={r.text[:200]}")
    except Exception as e:
        log.error(f"❌ Error sending signal: {e}")

# -------------------- Strategy core --------------------
def sign(x: float) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)

def compute_direction(df: pd.DataFrame, eval_time) -> int:
    """
    Direction at slot T based on BOTH:
      - Full-day trend: Close(T-1) - Open(10:00)
      - Last-hour trend: Close(T-1) - Close(T-61)
    Return +1 (LONG) / -1 (SHORT) / 0 (SKIP) only if BOTH agree.
    """
    df_before = df.loc[df.index < eval_time]
    if df_before.empty:
        return 0

    # Need 10:00 open and T-1 close
    try:
        day_open = float(df_before.loc[df_before.index >= MARKET_OPEN()].iloc[0]["open"])
    except Exception:
        return 0

    last_close = float(df_before.iloc[-1]["close"])

    # Need full last 60 bars before T
    if len(df_before) < 61:
        return 0
    hour_start_close = float(df_before.iloc[-60]["close"])  # close at T-61

    day_tr = sign(last_close - day_open)
    hour_tr = sign(last_close - hour_start_close)

    if day_tr == 0 or hour_tr == 0:
        return 0
    return day_tr if day_tr == hour_tr else 0

def can_hold_full_hour(start_time) -> bool:
    return (start_time + timedelta(minutes=60)) <= MARKET_CLOSE()

class Position:
    def __init__(self, direction: int, entry_time, exit_time):
        self.direction = direction  # +1 long, -1 short
        self.entry_time = entry_time
        self.exit_time = exit_time
    def __repr__(self):
        return f"<Pos {'LONG' if self.direction==1 else 'SHORT'} {self.entry_time.time()}→{self.exit_time.time()}>"

def direction_to_side(direction: int) -> str:
    return "BUY" if direction == 1 else "SELL"

def opposite(side: str) -> str:
    return "SELL" if side == "BUY" else "BUY"

# -------------------- Orchestration --------------------
def wait_until(ts, poll_sec: int):
    while NOW() < ts:
        time.sleep(poll_sec)

def run_trend_chain(alice: Aliceblue, cfg: Config):
    first_slot = EVAL_SLOTS[0]
    if NOW() < first_slot:
        log.info(f"Waiting until first evaluation slot {first_slot.time()} IST…")
        wait_until(first_slot, cfg.poll_sec)

    active: Optional[Position] = None

    for slot in EVAL_SLOTS:
        if not can_hold_full_hour(slot):
            log.info(f"[{slot.time()}] Skip: cannot hold a full hour (past 15:15 cutoff).")
            continue

        if NOW() < slot:
            wait_until(slot, cfg.poll_sec)

        df = fetch_today_1min_nifty(alice, cfg)
        direction = compute_direction(df, slot)  # +1, -1, 0

        # 1) Extension decision (if active exit equals slot+30, check whether to extend to slot+60)
        if active is not None and active.exit_time == (slot + timedelta(minutes=30)):
            if direction != 0 and direction == active.direction and can_hold_full_hour(slot):
                new_exit = slot + timedelta(minutes=60)   # extend by +30 net (T→T+90 after first extension)
                log.info(f"[{slot.time()}] Extend {active} → exit {new_exit.time()} (same direction)")
                active.exit_time = new_exit
            else:
                log.info(f"[{slot.time()}] No extension (flip/invalid/late). Keeping exit {active.exit_time.time()}.")

        # 2) Entry decision (only if no active position)
        if active is None and direction != 0:
            side = direction_to_side(direction)
            if can_hold_full_hour(slot):
                log.info(f"[{slot.time()}] ENTRY {side} (both-trend agree). Base exit { (slot + timedelta(minutes=60)).time() }")
                send_algomojo_signal(cfg, side)
                active = Position(direction=direction, entry_time=slot, exit_time=slot + timedelta(minutes=60))
            else:
                log.info(f"[{slot.time()}] Valid signal but cannot hold full hour → skip.")

        # 3) Timed exit if due
        if active is not None and NOW() >= active.exit_time:
            exit_side = opposite(direction_to_side(active.direction))
            log.info(f"[{NOW().time()}] EXIT {exit_side} (planned {active.exit_time.time()})")
            send_algomojo_signal(cfg, exit_side)
            active = None

    # Post-loop: if still holding, exit at its planned time (capped to 15:15)
    if active is not None:
        if active.exit_time > MARKET_CLOSE():
            active.exit_time = MARKET_CLOSE()
        if NOW() < active.exit_time:
            log.info(f"Waiting for final exit at {active.exit_time.time()} IST…")
            wait_until(active.exit_time, cfg.poll_sec)
        exit_side = opposite(direction_to_side(active.direction))
        log.info(f"[{NOW().time()}] FINAL EXIT {exit_side}")
        send_algomojo_signal(cfg, exit_side)

# -------------------- Main --------------------
def main():
    cfg = Config()
    alice = alice_connect(cfg)
    run_trend_chain(alice, cfg)

if __name__ == "__main__":
    main()
