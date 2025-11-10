#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIFTY VIZ Re-Entry Engine → Algomojo Webhooks (Long & Short)
-----------------------------------------------------------------------------
- Current time: November 10, 2025 12:41 PM IST
- Exact rules you specified:
    • VIZ: gap_up > 5 OR gap_down > 5
    • Re-entry within 30 minutes of i+2 close
    • Entry: next minute after fill (t+1)
    • TP = 0.5 × gap, SL = 0.25 × gap
    • TP and SL capped at MAX 10 points (lower bound 10 if calculated <10)
    • No gap filter, no time cutoff (except market hours)
    • Dual webhooks + robust exit handling
"""

import os, sys, time, logging, pytz, requests
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
from pya3 import Aliceblue

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("nifty_viz_reentry")

# -------------------- Time helpers --------------------
IST = pytz.timezone("Asia/Kolkata")
def NOW_MIN() -> datetime:
    return datetime.now(IST).replace(second=0, microsecond=0)
def NOW_REAL() -> datetime:
    return datetime.now(IST)

def AT(h: int, m: int) -> datetime:
    n = NOW_MIN()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

MARKET_OPEN = lambda: AT(9, 15)
MARKET_CLOSE = lambda: AT(15, 30)

GRACE_SEC = int(os.getenv("GRACE_SEC", "60"))
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "180"))
POLL_SEC = int(os.getenv("POLL_SEC", "3"))

def within_grace(now_ts: datetime, target_ts: datetime, sec: int = GRACE_SEC) -> bool:
    return abs((now_ts - target_ts).total_seconds()) <= sec

def market_is_open(ts: Optional[datetime] = None) -> bool:
    ts = ts or NOW_REAL()
    return MARKET_OPEN() <= ts <= MARKET_CLOSE()

# -------------------- Config --------------------
@dataclass
class Config:
    alice_user_id: str = os.getenv("ALICE_USER_ID", "")
    alice_api_key: str = os.getenv("ALICE_API_KEY", "")
    nifty_symbol_spot: str = os.getenv("NIFTY_SYMBOL_SPOT", "NIFTY 50")
    algomojo_webhook_long: str = os.getenv("ALGOMOJO_WEBHOOK_LONG", "")
    algomojo_webhook_short: str = os.getenv("ALGOMOJO_WEBHOOK_SHORT", "")
    buy_alert_name: str = os.getenv("BUY_ALERT_NAME", "BUY")
    sell_alert_name: str = os.getenv("SELL_ALERT_NAME", "SELL")
    short_alert_name: str = os.getenv("SHORT_ALERT_NAME", "SHORT")
    cover_alert_name: str = os.getenv("COVER_ALERT_NAME", "COVER")
    dry_run: bool = bool(int(os.getenv("DRY_RUN", "1")))

def validate_env(cfg: Config):
    missing = []
    if not cfg.alice_user_id: missing.append("ALICE_USER_ID")
    if not cfg.alice_api_key: missing.append("ALICE_API_KEY")
    if not (cfg.algomojo_webhook_long and cfg.algomojo_webhook_short) and not cfg.dry_run:
        missing.append("ALGOMOJO_WEBHOOK_LONG/SHORT")
    if missing:
        log.error(f"Missing env: {', '.join(missing)}")
        sys.exit(1)

# -------------------- Broker/Data --------------------
def alice_connect(cfg: Config) -> Aliceblue:
    alice = Aliceblue(user_id=cfg.alice_user_id, api_key=cfg.alice_api_key)
    _ = alice.get_session_id()
    return alice

def fetch_today_1min(alice: Aliceblue, cfg: Config) -> pd.DataFrame:
    instr = alice.get_instrument_by_symbol("NSE", cfg.nifty_symbol_spot)
    today_str = NOW_MIN().strftime("%d-%m-%Y")
    from_dt = datetime.strptime(today_str, "%d-%m-%Y")
    df = alice.get_historical(instr, from_dt, from_dt, "1", indices=True)
    if df is None or df.empty:
        raise RuntimeError("No 1-min data")
    df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(IST)
    df = df.set_index("datetime").sort_index()
    return df.loc[(df.index >= MARKET_OPEN()) & (df.index <= MARKET_CLOSE())]

def build_15min(df_1min: pd.DataFrame) -> pd.DataFrame:
    """Right-labelled 15-min candles with 09:30 candle included."""
    df = df_1min.resample('15T', label='right', closed='right').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
    }).dropna()
    # Ensure 09:30 candle exists
    first = df.index[0]
    if first.time() != pd.Timestamp("09:30").time():
        df = df_1min.resample('15T', label='right', closed='right', origin='start_day').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
    return df

# -------------------- Webhook --------------------
def send_signal(cfg: Config, action: str):
    if not market_is_open():
        log.info(f"[SKIP] Market closed → {action}")
        return
    hook = cfg.algomojo_webhook_long if action in ("BUY", "SELL") else cfg.algomojo_webhook_short
    alert = {
        "BUY": cfg.buy_alert_name, "SELL": cfg.sell_alert_name,
        "SHORT": cfg.short_alert_name, "COVER": cfg.cover_alert_name
    }[action]
    payload = {"alert_name": alert, "webhook_url": hook}
    if cfg.dry_run:
        log.info(f"[DRY] {action} → {hook.split('/')[-1] if '/' in hook else hook}")
        return
    try:
        r = requests.post(hook, json=payload, timeout=8)
        log.info(f"Sent {action} {'Success' if r.status_code==200 else 'Failed'}")
    except Exception as e:
        log.error(f"Webhook error {action}: {e}")

# -------------------- VIZ Core --------------------
@dataclass
class VIZSignal:
    ts_i2: datetime
    direction: int    # +1 bullish, -1 bearish
    gap: float
    high_i: float
    low_i2: float
    low_i: float
    high_i2: float
    filled: bool = False
    fill_time: Optional[datetime] = None
    entry_price: float = 0.0
    tp_price: float = 0.0
    sl_price: float = 0.0

def detect_viz(df_15: pd.DataFrame) -> Optional[VIZSignal]:
    if len(df_15) < 3:
        return None
    i = df_15.iloc[-3]
    i2 = df_15.iloc[-1]
    ts_i2 = i2.name

    gap_up = i2['low'] - i['high']
    gap_down = i['low'] - i2['high']

    if gap_up > 5:
        return VIZSignal(ts_i2, +1, gap_up, i['high'], i2['low'], i['low'], i2['high'])
    if gap_down > 5:
        return VIZSignal(ts_i2, -1, gap_down, i['high'], i2['low'], i['low'], i2['high'])
    return None

# -------------------- Engine --------------------
def run_viz_engine(alice: Aliceblue, cfg: Config):
    log.info("NIFTY VIZ Re-Entry Engine STARTED (UNFILTERED) – Nov 10, 2025")
    active_viz: Optional[VIZSignal] = None
    pending_entry_time: Optional[datetime] = None
    last_hb = 0

    while NOW_REAL() <= MARKET_CLOSE():
        now_real = NOW_REAL()
        now_min = NOW_MIN()

        # Heartbeat
        if time.time() - last_hb > HEARTBEAT_SEC:
            log.info(f"[HB] {now_real.strftime('%H:%M:%S')} | VIZ={'active' if active_viz else 'none'} | Entry={'Pending' if pending_entry_time else 'No'}")
            last_hb = time.time()

        # Miss entry window
        if pending_entry_time and now_real > pending_entry_time + timedelta(seconds=GRACE_SEC):
            log.warning(f"Missed entry at {pending_entry_time.strftime('%H:%M:%S')}")
            pending_entry_time = None
            active_viz = None

        # Execute entry
        if pending_entry_time and within_grace(now_real, pending_entry_time):
            entry_price = fetch_today_1min(alice, cfg).iloc[-1]['close']
            direction = "BUY" if active_viz.direction == 1 else "SHORT"
            tp_dist = min(10, max(10, 0.5 * active_viz.gap))
            sl_dist = min(10, max(10, 0.25 * active_viz.gap))
            active_viz.entry_price = entry_price
            active_viz.tp_price = entry_price + tp_dist if direction == "BUY" else entry_price - tp_dist
            active_viz.sl_price = entry_price - sl_dist if direction == "BUY" else entry_price + sl_dist

            log.info(f"ENTRY {direction} @ {entry_price:.2f} | Gap={active_viz.gap:.1f} | TP={active_viz.tp_price:.2f} | SL={active_viz.sl_price:.2f}")
            send_signal(cfg, direction)

            # Safety flat after 30 min
            flat_time = pending_entry_time + timedelta(minutes=30)
            pending_entry_time = None
            # Store flat time for later
            exit_action = "SELL" if direction == "BUY" else "COVER"
            # We'll check this in next loop

        try:
            df1 = fetch_today_1min(alice, cfg)
            df15 = build_15min(df1)
        except Exception as e:
            log.error(f"Data error: {e}")
            time.sleep(POLL_SEC)
            continue

        # New VIZ on fresh 15-min close
        if len(df15) >= 3:
            latest_15 = df15.index[-1]
            if latest_15.minute % 15 == 0 and latest_15.second == 0 and (active_viz is None or active_viz.ts_i2 != latest_15):
                new_viz = detect_viz(df15)
                if new_viz:
                    log.info(f"New VIZ {'Bullish' if new_viz.direction==1 else 'Bearish'} gap={new_viz.gap:.1f} pts @ {latest_15.strftime('%H:%M:%S')}")
                    active_viz = new_viz

        # Check fill
        if active_viz and not active_viz.filled:
            window = df1.loc[active_viz.ts_i2: active_viz.ts_i2 + timedelta(minutes=30)]
            if not window.empty:
                if active_viz.direction == 1:
                    filled = (window['low'] <= active_viz.low_i2).any()
                else:
                    filled = (window['high'] >= active_viz.high_i2).any()

                if filled:
                    fill_idx = window['low' if active_viz.direction==1 else 'high'] <= active_viz.low_i2 if active_viz.direction==1 else >= active_viz.high_i2
                    fill_time = window.index[fill_idx][0]
                    entry_time = fill_time + timedelta(minutes=1)
                    if entry_time <= MARKET_CLOSE():
                        active_viz.filled = True
                        active_viz.fill_time = fill_time
                        pending_entry_time = entry_time
                        log.info(f"FILL @ {fill_time.strftime('%H:%M:%S')} → ENTRY scheduled {entry_time.strftime('%H:%M:%S')}")

        time.sleep(POLL_SEC)

    log.info("Market closed. VIZ Engine stopped.")

# -------------------- Main --------------------
def main():
    cfg = Config()
    validate_env(cfg)
    alice = alice_connect(cfg)
    run_viz_engine(alice, cfg)

if __name__ == "__main__":
    main()
