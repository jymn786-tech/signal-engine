#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIFTY Trend-Chain Engine → Algomojo Webhooks (Long & Short) with robust exits
-----------------------------------------------------------------------------

- Start anytime; waits until 11:30 IST.
- Market window: 10:00–15:00 IST.
- Slots: 11:30, 12:00, 12:30, 13:00, 13:30, 14:00.
- Entry only when BOTH trends agree:
    * Full-day (10:00 → T-1) and
    * Last-hour (T-61 → T-1).
- Base hold = 60 min; at next slot, if same direction & valid, extend by +30 (chain).
- No backfill: actions only within ±GRACE_SEC of slot times.
- Robust exits: use second-precision time, wider grace, exit checks before any blocking work.
- Dual webhooks: BUY/SELL via LONG hook; SHORT/COVER via SHORT hook.
"""

import os, sys, time, logging, pytz, requests
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict
import pandas as pd
from pya3 import Aliceblue

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("nifty_trend_chain")

# -------------------- Time helpers --------------------
IST = pytz.timezone("Asia/Kolkata")

def NOW_MIN() -> datetime:
    """Minute-aligned current time (used for slot alignment/waits)."""
    return datetime.now(IST).replace(second=0, microsecond=0)

def NOW_REAL() -> datetime:
    """Second-precision current time (used for within_grace / exits)."""
    return datetime.now(IST)

def AT(h: int, m: int) -> datetime:
    n = NOW_MIN()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

MARKET_OPEN  = lambda: AT(10, 0)
MARKET_CLOSE = lambda: AT(15, 0)

def make_slots() -> list[datetime]:
    return [AT(11,30), AT(12,0), AT(12,30), AT(13,0), AT(13,30), AT(14,0)]

# Grace / heartbeat
GRACE_SEC      = int(os.getenv("GRACE_SEC", "60"))   # widened from 20 → 60
HEARTBEAT_SEC  = int(os.getenv("HEARTBEAT_SEC", "120"))
POLL_DEFAULT_S = int(os.getenv("POLL_SEC", "5"))

def within_grace(now_ts: datetime, target_ts: datetime, sec: int = GRACE_SEC) -> bool:
    return abs((now_ts - target_ts).total_seconds()) <= sec

def market_is_open(ts: Optional[datetime] = None) -> bool:
    ts = ts or NOW_REAL()
    return MARKET_OPEN() <= ts <= MARKET_CLOSE()

# -------------------- Config --------------------
@dataclass
class Config:
    # AliceBlue
    alice_user_id: str = os.getenv("ALICE_USER_ID", "")
    alice_api_key: str = os.getenv("ALICE_API_KEY", "")
    nifty_symbol_spot: str = os.getenv("NIFTY_SYMBOL_SPOT", "NIFTY 50")

    # Dual Algomojo webhooks
    algomojo_webhook_long: str = os.getenv("ALGOMOJO_WEBHOOK_LONG", "")
    algomojo_webhook_short: str = os.getenv("ALGOMOJO_WEBHOOK_SHORT", "")

    # Alert names
    buy_alert_name:   str = os.getenv("BUY_ALERT_NAME", "BUY")
    sell_alert_name:  str = os.getenv("SELL_ALERT_NAME", "SELL")
    short_alert_name: str = os.getenv("SHORT_ALERT_NAME", "SHORT")
    cover_alert_name: str = os.getenv("COVER_ALERT_NAME", "COVER")

    # Behavior
    dry_run: bool = bool(int(os.getenv("DRY_RUN", "1")))
    poll_sec: int = POLL_DEFAULT_S

def validate_env(cfg: Config):
    missing = []
    if not cfg.alice_user_id: missing.append("ALICE_USER_ID")
    if not cfg.alice_api_key: missing.append("ALICE_API_KEY")
    if not (cfg.algomojo_webhook_long and cfg.algomojo_webhook_short) and not cfg.dry_run:
        missing.append("ALGOMOJO_WEBHOOK_LONG/SHORT (both required when DRY_RUN=0)")
    if missing:
        log.error(f"Missing environment: {', '.join(missing)}")
        sys.exit(1)

# -------------------- Broker/Data helpers --------------------
def alice_connect(cfg: Config) -> Aliceblue:
    alice = Aliceblue(user_id=cfg.alice_user_id, api_key=cfg.alice_api_key)
    _ = alice.get_session_id()
    return alice

def fetch_today_1min_nifty(alice: Aliceblue, cfg: Config) -> pd.DataFrame:
    instr = alice.get_instrument_by_symbol(exchange="NSE", symbol=cfg.nifty_symbol_spot)
    today = NOW_MIN().strftime("%d-%m-%Y")
    from_dt = datetime.strptime(today, "%d-%m-%Y")
    to_dt   = datetime.strptime(today, "%d-%m-%Y")
    df = alice.get_historical(instr, from_dt, to_dt, "1", indices=True)
    if df is None or getattr(df, "empty", True):
        raise RuntimeError("No NIFTY spot data")
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(IST)
    df = df.set_index("datetime").sort_index()
    return df.loc[(df.index >= MARKET_OPEN()) & (df.index <= MARKET_CLOSE())]

# -------------------- Webhook sender --------------------
def send_algomojo_signal(cfg: Config, action: str):
    """Send BUY/SELL via long webhook; SHORT/COVER via short webhook. Suppress if outside market hours."""
    if not market_is_open():
        log.info(f"[SKIP] Market closed; not sending {action}")
        return

    if action in ("BUY", "SELL"):
        url = cfg.algomojo_webhook_long
        alert = cfg.buy_alert_name if action == "BUY" else cfg.sell_alert_name
        hook_label = "LONG"
    else:
        url = cfg.algomojo_webhook_short
        alert = cfg.short_alert_name if action == "SHORT" else cfg.cover_alert_name
        hook_label = "SHORT"

    payload = {"alert_name": alert, "webhook_url": url}

    if cfg.dry_run:
        log.info(f"[DRY] {action} via {hook_label} webhook")
        return

    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            log.info(f"📤 Sent {action} via {hook_label} webhook")
        else:
            log.warning(f"⚠️ {action} failed: {r.status_code} | {r.text[:200]}")
    except Exception as e:
        log.error(f"❌ Error sending {action}: {e}")

# -------------------- Strategy core --------------------
def sign(x: float) -> int:
    return 1 if x > 0 else (-1 if x < 0 else 0)

def compute_direction(df: pd.DataFrame, eval_time: datetime) -> int:
    """
    Direction at slot T based on BOTH:
      - Full-day trend: Close(T-1) - Open(10:00)
      - Last-hour trend: Close(T-1) - Close(T-61)
    Return +1 (LONG) / -1 (SHORT) / 0 (SKIP) only if BOTH agree and non-zero.
    """
    df_before = df.loc[df.index < eval_time]
    if len(df_before) < 61:
        return 0

    # Day open (first bar >= 10:00)
    try:
        day_open = float(df_before.loc[df_before.index >= MARKET_OPEN()].iloc[0]["open"])
    except Exception:
        return 0

    last_close = float(df_before.iloc[-1]["close"])
    hour_start_close = float(df_before.iloc[-60]["close"])  # close at T-61

    day_tr  = sign(last_close - day_open)
    hour_tr = sign(last_close - hour_start_close)
    return day_tr if (day_tr == hour_tr and day_tr != 0) else 0

def direction_to_action(direction: int) -> str:
    return "BUY" if direction == 1 else "SHORT"

def opposite_action(action: str) -> str:
    return {"BUY": "SELL", "SELL": "BUY", "SHORT": "COVER", "COVER": "SHORT"}[action]

def can_hold_full_hour(t: datetime) -> bool:
    return (t + timedelta(minutes=60)) <= MARKET_CLOSE()

class Position:
    def __init__(self, direction: int, entry: datetime, exit_: datetime):
        self.direction = direction
        self.entry_time = entry
        self.exit_time = exit_
    def __repr__(self):
        return f"<{'LONG' if self.direction==1 else 'SHORT'} {self.entry_time.time()}→{self.exit_time.time()}>"

# -------------------- Engine --------------------
def run_trend_chain(alice: Aliceblue, cfg: Config):
    # 0) Wait until 11:30 (no work earlier)
    first_slot = AT(11, 30)
    if NOW_MIN() < first_slot:
        wait_secs = int((first_slot - NOW_MIN()).total_seconds())
        log.info(f"Waiting {wait_secs}s until first slot {first_slot.time()} IST…")
        while NOW_MIN() < first_slot:
            time.sleep(min(cfg.poll_sec, 5))

    slots = make_slots()
    seen_slot: Dict[datetime, bool] = {s: False for s in slots}
    active: Optional[Position] = None
    last_heartbeat_epoch = 0

    # 1) Continuous loop until 15:00
    while NOW_MIN() <= MARKET_CLOSE():
        now_min  = NOW_MIN()
        now_real = NOW_REAL()

        # Heartbeat log
        if int(now_real.timestamp()) - last_heartbeat_epoch >= HEARTBEAT_SEC:
            log.info(f"[HB] alive | now={now_real.strftime('%H:%M:%S')} | active={active}")
            last_heartbeat_epoch = int(now_real.timestamp())

        # ---------- EXIT checks FIRST (no blocking operations before this) ----------
        if active is not None:
            # Breadcrumb when approaching exit
            secs_to_exit = (active.exit_time - now_real).total_seconds()
            if 0 <= secs_to_exit <= (GRACE_SEC + 30):
                log.debug(f"[EXIT SOON] now={now_real.strftime('%H:%M:%S')} exit={active.exit_time.strftime('%H:%M:%S')} in {secs_to_exit:.1f}s")

            if within_grace(now_real, active.exit_time):
                exit_act = opposite_action(direction_to_action(active.direction))
                log.info(f"[{now_real.strftime('%H:%M:%S')}] EXIT {exit_act} (planned {active.exit_time.strftime('%H:%M:%S')})")
                send_algomojo_signal(cfg, exit_act)
                active = None
            elif now_real > active.exit_time and market_is_open(now_real):
                exit_act = opposite_action(direction_to_action(active.direction))
                log.info(f"[{now_real.strftime('%H:%M:%S')}] LATE EXIT {exit_act} (missed {active.exit_time.strftime('%H:%M:%S')}); flattening now.")
                send_algomojo_signal(cfg, exit_act)
                active = None
            elif now_real > active.exit_time and not market_is_open(now_real):
                log.info(f"[{now_real.strftime('%H:%M:%S')}] Missed exit after close; no webhook. Intended {active.exit_time.strftime('%H:%M:%S')}")
                active = None

        # ---------- Process slots (each at most once) ----------
        for slot in slots:
            if seen_slot[slot]:
                continue
            if not can_hold_full_hour(slot):
                seen_slot[slot] = True
                continue
            if within_grace(now_real, slot):
                # Fetch data strictly before slot for decisioning
                df = fetch_today_1min_nifty(alice, cfg)
                direction = compute_direction(df, slot)

                # Extension: if current exit equals slot+30 and same direction, extend to slot+60
                if active is not None and active.exit_time == (slot + timedelta(minutes=30)):
                    if direction != 0 and direction == active.direction and can_hold_full_hour(slot):
                        new_exit = slot + timedelta(minutes=60)
                        log.info(f"[{slot.strftime('%H:%M:%S')}] EXTEND {'LONG' if active.direction==1 else 'SHORT'} → new exit {new_exit.strftime('%H:%M:%S')}")
                        active.exit_time = new_exit
                    else:
                        log.info(f"[{slot.strftime('%H:%M:%S')}] No extension (flip/invalid). Keep exit {active.exit_time.strftime('%H:%M:%S')}.")

                # Entry: only if no active and valid direction
                if active is None and direction != 0:
                    action = direction_to_action(direction)
                    log.info(f"[{slot.strftime('%H:%M:%S')}] ENTRY {action} (both-trend agree). Base exit {(slot + timedelta(minutes=60)).strftime('%H:%M:%S')}")
                    send_algomojo_signal(cfg, action)
                    active = Position(direction, slot, slot + timedelta(minutes=60))

                seen_slot[slot] = True

        # Cap exit to market close if needed
        if active is not None and active.exit_time > MARKET_CLOSE():
            active.exit_time = MARKET_CLOSE()

        time.sleep(cfg.poll_sec)

    # 2) After loop: if still holding after 15:00, just log (no webhook).
    if active is not None and not market_is_open():
        log.info(f"[{NOW_REAL().strftime('%H:%M:%S')}] Market closed with active position; no webhook (intended exit {active.exit_time.strftime('%H:%M:%S')}).")

# -------------------- Main --------------------
def main():
    cfg = Config()
    validate_env(cfg)
    alice = alice_connect(cfg)
    run_trend_chain(alice, cfg)

if __name__ == "__main__":
    main()
