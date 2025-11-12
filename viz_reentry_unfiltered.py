#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIFTY Re-Entry Engine → Algomojo Webhooks (Long & Short)
Updated: integrates 5-min revisit logic, ATR sizing, EMA context, score threshold acceptance.
"""

import os, sys, time, logging, pytz, requests, math, csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import pandas as pd
from pya3 import Aliceblue

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("nifty_reentry_v2")

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
    score_threshold: float = float(os.getenv("SCORE_THRESHOLD", "0.65"))  # chronological acceptance threshold
    signal_log_csv: str = os.getenv("SIGNAL_LOG_CSV", "/tmp/reentry_signals_log.csv")

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
    # restrict to market hours (safety)
    return df.loc[(df.index >= MARKET_OPEN()) & (df.index <= MARKET_CLOSE())]

def build_5min_from_1min(df_1min: pd.DataFrame) -> pd.DataFrame:
    # Build 5-min candles aligned so label='right' covers previous 5 minutes
    five = df_1min.resample('5T', label='right', closed='right').agg({
        'open':'first','high':'max','low':'min','close':'last'
    }).dropna()
    # compute range and ATR(14) on 5-min
    five['range'] = five['high'] - five['low']
    # ATR on 5-min
    high = five['high']; low = five['low']; close = five['close']
    tr0 = high - low
    tr1 = (high - close.shift(1)).abs()
    tr2 = (low - close.shift(1)).abs()
    tr = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1)
    five['atr14'] = tr.rolling(14, min_periods=1).mean()
    return five

# -------------------- Webhook --------------------
def send_signal(cfg: Config, action: str, payload_extra: Optional[Dict[str,Any]] = None):
    if not market_is_open():
        log.info(f"[SKIP] Market closed → {action}")
        return
    hook = cfg.algomojo_webhook_long if action in ("BUY","COVER") else cfg.algomojo_webhook_short
    alert = {"BUY": cfg.buy_alert_name, "SELL": cfg.sell_alert_name,
             "SHORT": cfg.short_alert_name, "COVER": cfg.cover_alert_name}[action]
    payload = {"alert_name": alert, "webhook_url": hook}
    if payload_extra:
        payload.update(payload_extra)
    if cfg.dry_run:
        log.info(f"[DRY] {action} payload={payload_extra}")
        return
    try:
        r = requests.post(hook, json=payload, timeout=8)
        log.info(f"Sent {action} → {'OK' if r.status_code==200 else f'FAIL({r.status_code})'}")
    except Exception as e:
        log.error(f"Webhook error {action}: {e}")

# -------------------- Core logic (5-min / revisit rules) --------------------
@dataclass
class CandidateSignal:
    T: datetime                # 5-min candle close time
    five_open: float
    five_close: float
    five_range: float
    atr14: float
    trade_side: str            # 'long' or 'short' (inverted logic)
    R: datetime                # revisit timestamp (first minute that touched five_close)
    entry_time: datetime       # R + 1 minute
    entry_open: float
    tp_price: float
    sl_price: float
    score: float
    accepted: bool = False

# helper to percentile-rank a series (0..1)
def percentile_rank(series: pd.Series) -> pd.Series:
    if len(series) == 0:
        return series
    r = series.rank(method="average", pct=True)
    return r.clip(0, 1).fillna(0)

# append header to log if not exists
def ensure_log_file(path: str):
    if not os.path.exists(path):
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["timestamp","T","R","entry_time","trade_side","entry_open","tp_price","sl_price","score","accepted","result","exit_time","exit_price","pl_points"])

# -------------------- Engine --------------------
def run_engine(alice: Aliceblue, cfg: Config):
    log.info("NIFTY Re-Entry Engine STARTED (score threshold acceptance)")

    seen_T = set()        # track 5-min reference candle times we've already processed
    pending_entries: Dict[datetime, CandidateSignal] = {}  # entry_time -> CandidateSignal
    ensure_log_file(cfg.signal_log_csv)

    while NOW_REAL() <= MARKET_CLOSE():
        now_real = NOW_REAL()
        now_min = NOW_MIN()

        # heartbeat
        if int(time.time()) % HEARTBEAT_SEC == 0:
            log.info(f"[HB] {now_real.strftime('%H:%M:%S')} | PendingEntries={len(pending_entries)} | SeenRefs={len(seen_T)}")

        # clear expired pending entries (missed)
        for et in list(pending_entries.keys()):
            cs = pending_entries[et]
            if now_real > et + timedelta(seconds=GRACE_SEC):
                log.warning(f"Missed scheduled entry for T={cs.T} (entry_time={et}) -> dropping")
                pending_entries.pop(et, None)

        # fetch data
        try:
            df1 = fetch_today_1min(alice, cfg)
        except Exception as e:
            log.error(f"Data fetch error: {e}")
            time.sleep(POLL_SEC)
            continue

        # short-circuit if not enough data
        if df1.shape[0] < 10:
            time.sleep(POLL_SEC)
            continue

        # compute EMAs for market context (1-min EMAs)
        df1['ema20'] = df1['close'].ewm(span=20, adjust=False).mean()
        df1['ema50'] = df1['close'].ewm(span=50, adjust=False).mean()

        # build 5-min candles (up to most recent complete 5-min)
        five = build_5min_from_1min(df1)

        # identify qualifying 5-min candles (range >= 20)
        qual = five[five['range'] >= 20].copy()

        # compute percentiles for scoring using qualifying history up to now
        # We'll compute percentile ranks across qual entries
        if not qual.empty:
            # use all qual rows up to now
            range_pct_series = pd.Series(percentile_rank(qual['range'].fillna(0)), index=qual.index)
            atr_pct_series = pd.Series(percentile_rank(qual['atr14'].fillna(0)), index=qual.index)
        else:
            range_pct_series = pd.Series(dtype=float)
            atr_pct_series = pd.Series(dtype=float)

        # For each qualifying 5-min (T), if we haven't processed it, check revisit window T+5..T+20
        for T, rrow in qual.iterrows():
            if T in seen_T:
                continue  # already processed this reference candle
            # search revisit in T+5..T+20
            revisit_start = T + timedelta(minutes=5)
            revisit_end = T + timedelta(minutes=20)
            revisit_bars = df1[revisit_start:revisit_end]
            if revisit_bars.empty:
                # no revisit yet — do not mark seen_T; revisit might occur later in window
                continue
            touched = revisit_bars[(revisit_bars['low'] <= rrow['close']) & (revisit_bars['high'] >= rrow['close'])]
            if touched.empty:
                # no revisit in that window
                # if current time is past T+20 we can mark as seen to avoid re-checking forever
                if NOW_REAL() > revisit_end + timedelta(seconds=GRACE_SEC):
                    seen_T.add(T)
                continue
            # found revisit at R (first touch)
            R = touched.index[0]
            entry_time = R + timedelta(minutes=1)
            # ensure entry_time exists (we'll require the next minute)
            if entry_time not in df1.index:
                # if entry minute not available yet (we're in that minute), skip until available
                # but don't mark as seen_T yet
                continue
            # entry-minute confirmation: require entry-minute close in trade direction
            entry_open = df1.at[entry_time, 'open']
            entry_close = df1.at[entry_time, 'close']
            five_open = rrow['open']; five_close = rrow['close']; five_range = rrow['range']
            atr14 = rrow['atr14']
            trade_side = 'long' if five_close > five_open else 'short'

            if trade_side == 'long' and not (entry_close > entry_open):
                seen_T.add(T); continue
            if trade_side == 'short' and not (entry_close < entry_open):
                seen_T.add(T); continue

            # market context: EMA20 vs EMA50 at entry_time
            ema20 = df1.at[entry_time,'ema20']; ema50 = df1.at[entry_time,'ema50']
            if trade_side == 'long' and not (ema20 > ema50):
                seen_T.add(T); continue
            if trade_side == 'short' and not (ema20 < ema50):
                seen_T.add(T); continue

            # compute tp_dist using ATR clipping
            raw_dist = abs(entry_open - five_open)
            lower = 0.5 * atr14 if (atr14 is not None and not math.isnan(atr14)) else 0.0
            tp_dist = max(raw_dist, lower)
            tp_dist = min(tp_dist, 20.0)
            if tp_dist <= 0:
                seen_T.add(T); continue

            if trade_side == 'long':
                tp_price = entry_open + tp_dist
                sl_price = entry_open - 0.5 * tp_dist
            else:
                tp_price = entry_open - tp_dist
                sl_price = entry_open + 0.5 * tp_dist

            # scoring components (percentile ranks from qual up to now)
            # range_pct and atr_pct use the qual series percentile ranks we computed above
            range_pct = float(range_pct_series.get(T, 0.0))
            atr_pct = float(atr_pct_series.get(T, 0.0))
            proximity = 1.0 - min(abs(entry_open - five_close) / (five_range if five_range>0 else 1.0), 1.0)
            trend_flag = 1 if (df1.at[entry_time,'ema20'] > df1.at[entry_time,'ema50']) else 0
            # combine into score
            score = 0.4 * range_pct + 0.3 * proximity + 0.2 * atr_pct + 0.1 * trend_flag

            cs = CandidateSignal(
                T=T, five_open=five_open, five_close=five_close, five_range=five_range, atr14=atr14,
                trade_side=trade_side, R=R, entry_time=entry_time, entry_open=entry_open,
                tp_price=tp_price, sl_price=sl_price, score=score
            )

            # Chronological acceptance rule: accept immediately when score >= threshold
            if score >= cfg.score_threshold:
                cs.accepted = True
                pending_entries[entry_time] = cs
                # mark seen so we don't re-evaluate this T again
                seen_T.add(T)
                # log candidate accepted
                log.info(f"[ACCEPT] T={T} R={R} entry={entry_time} side={trade_side} score={score:.3f} tp_dist={tp_dist:.2f}")
                # We DO NOT send webhook here — we send webhook at actual entry (next-minute open) inside pending entry handler
                # Write to CSV log
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(),
                                trade_side, entry_open, tp_price, sl_price, score, True, "", "", "", ""])
            else:
                # Score too low -> don't accept; mark seen so we don't re-check T again
                seen_T.add(T)
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(),
                                trade_side, entry_open, tp_price, sl_price, score, False, "", "", "", ""])
                log.info(f"[REJECT] T={T} R={R} score={score:.3f} (threshold {cfg.score_threshold})")

        # Execute pending entries if within grace (entry at next-minute open)
        for et in list(pending_entries.keys()):
            cs = pending_entries[et]
            if now_real >= et and within_grace(now_real, et):
                # re-load 1-min to get fresh open/close etc
                try:
                    df1_latest = fetch_today_1min(alice, cfg)
                except Exception as e:
                    log.error(f"Failed fetching latest before entry: {e}")
                    continue
                # ensure entry_time row exists
                if et not in df1_latest.index:
                    log.warning(f"Entry minute {et} not present; skipping entry for T={cs.T}")
                    pending_entries.pop(et, None)
                    continue
                entry_price = float(df1_latest.at[et, 'open'])  # enter at next-minute open
                direction = "BUY" if cs.trade_side == 'long' else "SHORT"
                # recompute TP/SL from entry_price using same tp_dist clipped rule (keeps alignment with live price)
                raw_dist = abs(entry_price - cs.five_open)
                lower = 0.5 * (cs.atr14 if (cs.atr14 is not None and not math.isnan(cs.atr14)) else 0.0)
                tp_dist = max(raw_dist, lower)
                tp_dist = min(tp_dist, 20.0)
                if tp_dist <= 0:
                    log.warning(f"Degenerate tp_dist=0 for T={cs.T}, skipping")
                    pending_entries.pop(et, None)
                    continue
                if cs.trade_side == 'long':
                    tp_price = entry_price + tp_dist
                    sl_price = entry_price - 0.5 * tp_dist
                else:
                    tp_price = entry_price - tp_dist
                    sl_price = entry_price + 0.5 * tp_dist

                # attach actual entry prices and set filled
                cs.entry_open = entry_price
                cs.tp_price = tp_price
                cs.sl_price = sl_price
                cs.accepted = True

                log.info(f"ENTRY {direction} @ {entry_price:.2f} | T={cs.T} R={cs.R} | TP={tp_price:.2f} SL={sl_price:.2f} | score={cs.score:.3f}")
                send_signal(cfg, direction, payload_extra={
                    "T": cs.T.isoformat(), "R": cs.R.isoformat(), "entry_time": et.isoformat(),
                    "entry_price": entry_price, "tp": tp_price, "sl": sl_price, "score": cs.score
                })

                # log the actual entry
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([datetime.now(IST).isoformat(), cs.T.isoformat(), cs.R.isoformat(), et.isoformat(),
                                cs.trade_side, entry_price, tp_price, sl_price, cs.score, True, "ENTRY_SENT", "", "", ""])
                # remove from pending
                pending_entries.pop(et, None)

        # sleep between polls
        time.sleep(POLL_SEC)

    log.info("Market closed. Engine stopped.")

# -------------------- Main --------------------
def main():
    cfg = Config()
    validate_env(cfg)
    alice = alice_connect(cfg)
    run_engine(alice, cfg)

if __name__ == "__main__":
    main()
