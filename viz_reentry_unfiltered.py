#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NIFTY Re-Entry Engine v3 (SciPy removed) → Algomojo Webhooks (Long & Short)
- Uses pandas.Series.rank(method='average', pct=True) for percentile ranks (no SciPy)
- Streaming percentiles scoring (range_pct, atr_pct) computed up to each T
- ATR-sizing, EMA context, entry-minute confirmation
- Chronological acceptance when score >= SCORE_THRESHOLD
- Persist pending accepted signals to disk (JSONL) to survive restarts
- On startup, attempt to execute slightly-late pending entries within ALLOWED_LATE_SEC
- Recompute TP/SL at actual entry open (live behavior)
"""
import os, sys, time, logging, pytz, requests, math, csv, json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path

import pandas as pd
from pya3 import Aliceblue

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("nifty_reentry_v3_noscipy")

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
    score_threshold: float = float(os.getenv("SCORE_THRESHOLD", "0.65"))
    signal_log_csv: str = os.getenv("SIGNAL_LOG_CSV", "/tmp/reentry_signals_log.csv")
    persist_pending_path: str = os.getenv("PERSIST_PENDING_PATH", "/tmp/reentry_pending.jsonl")
    allowed_late_sec: int = int(os.getenv("ALLOWED_LATE_SEC", "30"))

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
    five = df_1min.resample('5min', label='right', closed='right').agg({
        'open':'first','high':'max','low':'min','close':'last'
    }).dropna()
    five['range'] = five['high'] - five['low']
    high = five['high']; low = five['low']; close = five['close']
    tr0 = high - low
    tr1 = (high - close.shift(1)).abs()
    tr2 = (low - close.shift(1)).abs()
    tr = pd.concat([tr0, tr1, tr2], axis=1).max(axis=1)
    five['atr14'] = tr.rolling(14, min_periods=1).mean()
    return five

# -------------------- Persistence for pending entries (JSONL) --------------------
def persist_pending_add(path: str, cs: "CandidateSignal"):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    obj = {
        "T": cs.T.isoformat(),
        "R": cs.R.isoformat(),
        "entry_time": cs.entry_time.isoformat(),
        "trade_side": cs.trade_side,
        "five_open": cs.five_open,
        "five_close": cs.five_close,
        "five_range": cs.five_range,
        "atr14": cs.atr14,
        "score": cs.score
    }
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj) + "\n")

def persist_pending_remove(path: str, entry_time_iso: str):
    p = Path(path)
    if not p.exists():
        return
    kept = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if obj.get("entry_time") != entry_time_iso:
                    kept.append(line)
            except Exception:
                continue
    with p.open("w", encoding="utf-8") as f:
        for line in kept:
            f.write(line)

def persist_pending_load(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    out = []
    if not p.exists():
        return out
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                out.append(obj)
            except Exception:
                continue
    return out

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

# -------------------- Core logic (CandidateSignal dataclass) --------------------
@dataclass
class CandidateSignal:
    T: datetime
    five_open: float
    five_close: float
    five_range: float
    atr14: float
    trade_side: str
    R: datetime
    entry_time: datetime
    entry_open: float
    tp_price: float
    sl_price: float
    score: float
    accepted: bool = False

def percentile_rank(series: pd.Series) -> pd.Series:
    if len(series) == 0:
        return series
    r = series.rank(method="average", pct=True)
    return r.clip(0,1).fillna(0)

def ensure_log_file(path: str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists():
        with p.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["timestamp","T","R","entry_time","trade_side","entry_open","tp_price","sl_price","score","accepted","result","exit_time","exit_price","pl_points","reason"])

# -------------------- Engine --------------------
def run_engine(alice: Aliceblue, cfg: Config):
    log.info("NIFTY Re-Entry Engine v3 (no SciPy) STARTED (streaming percentiles, persistence enabled)")

    seen_T = set()
    pending_entries: Dict[datetime, CandidateSignal] = {}
    ensure_log_file(cfg.signal_log_csv)

    # load persisted pending entries
    loaded = persist_pending_load(cfg.persist_pending_path)
    if loaded:
        for obj in loaded:
            try:
                et = pd.to_datetime(obj["entry_time"]).to_pydatetime()
                cs = CandidateSignal(
                    T=pd.to_datetime(obj["T"]).to_pydatetime(),
                    five_open=float(obj["five_open"]),
                    five_close=float(obj["five_close"]),
                    five_range=float(obj["five_range"]),
                    atr14=float(obj["atr14"]),
                    trade_side=obj["trade_side"],
                    R=pd.to_datetime(obj["R"]).to_pydatetime(),
                    entry_time=et,
                    entry_open=float(obj.get("entry_open", 0.0)),
                    tp_price=float(obj.get("tp_price", 0.0)),
                    sl_price=float(obj.get("sl_price", 0.0)),
                    score=float(obj.get("score", 0.0)),
                    accepted=True
                )
                pending_entries[et] = cs
            except Exception:
                continue
        log.info(f"Loaded {len(loaded)} persisted pending entries from disk")

    # on startup attempt recovery execution for slightly-late entries
    now_real = NOW_REAL()
    for et in list(pending_entries.keys()):
        cs = pending_entries[et]
        if now_real >= et and (now_real - et).total_seconds() <= cfg.allowed_late_sec:
            log.info(f"[RECOVERY-EXEC] Attempting immediate execution for late entry T={cs.T} (late by {(now_real-et).total_seconds():.1f}s)")
            try:
                df1_latest = fetch_today_1min(alice, cfg)
                if et in df1_latest.index:
                    entry_price = float(df1_latest.at[et,'open'])
                    # recompute TP/SL as per runtime logic
                    raw_dist = abs(entry_price - cs.five_open)
                    lower = 0.5 * (cs.atr14 if (cs.atr14 is not None and not math.isnan(cs.atr14)) else 0.0)
                    tp_dist = max(raw_dist, lower); tp_dist = min(tp_dist, 20.0)
                    if tp_dist <= 0:
                        log.warning(f"Degenerate tp_dist=0 for recovered entry T={cs.T}, dropping")
                        persist_pending_remove(cfg.persist_pending_path, et.isoformat())
                        pending_entries.pop(et, None)
                        continue
                    if cs.trade_side == 'long':
                        tp_price = entry_price + tp_dist; sl_price = entry_price - 0.5*tp_dist
                        direction = "BUY"
                    else:
                        tp_price = entry_price - tp_dist; sl_price = entry_price + 0.5*tp_dist
                        direction = "SHORT"
                    # send
                    send_signal(cfg, direction, payload_extra={
                        "T": cs.T.isoformat(), "R": cs.R.isoformat(), "entry_time": et.isoformat(),
                        "entry_price": entry_price, "tp": tp_price, "sl": sl_price, "score": cs.score, "recovery": True
                    })
                    log.info(f"[RECOVERY-SENT] {direction} @ {entry_price:.2f} | T={cs.T}")
                    # remove persisted
                    persist_pending_remove(cfg.persist_pending_path, et.isoformat())
                    pending_entries.pop(et, None)
                else:
                    # entry minute not in latest frame; if too late beyond allowed window drop it
                    if (now_real - et).total_seconds() > cfg.allowed_late_sec:
                        log.warning(f"Persisted entry for T={cs.T} missed on startup -> dropping")
                        persist_pending_remove(cfg.persist_pending_path, et.isoformat())
                        pending_entries.pop(et, None)
            except Exception as e:
                log.warning(f"Recovery attempt failed for {et}: {e}")

    # main loop
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
                persist_pending_remove(cfg.persist_pending_path, et.isoformat())
                pending_entries.pop(et, None)

        # fetch data
        try:
            df1 = fetch_today_1min(alice, cfg)
        except Exception as e:
            log.error(f"Data fetch error: {e}")
            time.sleep(POLL_SEC)
            continue

        if df1.shape[0] < 10:
            time.sleep(POLL_SEC)
            continue

        # compute EMAs
        df1['ema20'] = df1['close'].ewm(span=20, adjust=False).mean()
        df1['ema50'] = df1['close'].ewm(span=50, adjust=False).mean()

        # build 5-min
        five = build_5min_from_1min(df1)

        # qualifying 5-min
        qual = five[five['range'] >= 20].copy()

        # iterate qual T chronologically
        for T, rrow in qual.iterrows():
            if T in seen_T:
                continue
            revisit_start = T + timedelta(minutes=5); revisit_end = T + timedelta(minutes=20)
            revisit_bars = df1[revisit_start:revisit_end]
            if revisit_bars.empty:
                # revisit might occur later; don't mark seen yet
                if NOW_REAL() > revisit_end + timedelta(seconds=GRACE_SEC):
                    seen_T.add(T)
                continue
            touched = revisit_bars[(revisit_bars['low'] <= rrow['close']) & (revisit_bars['high'] >= rrow['close'])]
            if touched.empty:
                if NOW_REAL() > revisit_end + timedelta(seconds=GRACE_SEC):
                    seen_T.add(T)
                continue
            R = touched.index[0]
            entry_time = R + timedelta(minutes=1)
            if entry_time not in df1.index:
                # wait until entry minute exists
                continue
            entry_open = float(df1.at[entry_time,'open']); entry_close = float(df1.at[entry_time,'close'])
            five_open = rrow['open']; five_close = rrow['close']; five_range = rrow['range']; atr14 = rrow['atr14']
            trade_side = 'long' if five_close > five_open else 'short'
            # entry-minute confirmation
            if trade_side == 'long' and not (entry_close > entry_open):
                seen_T.add(T);
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f); w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(), trade_side, entry_open, "", "", 0.0, False, "REJECTED", "", "", 0.0, "entry_minute_failed"])
                continue
            if trade_side == 'short' and not (entry_close < entry_open):
                seen_T.add(T);
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f); w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(), trade_side, entry_open, "", "", 0.0, False, "REJECTED", "", "", 0.0, "entry_minute_failed"])
                continue
            # EMA context
            ema20 = float(df1.at[entry_time,'ema20']); ema50 = float(df1.at[entry_time,'ema50'])
            if trade_side=='long' and not (ema20 > ema50):
                seen_T.add(T);
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f); w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(), trade_side, entry_open, "", "", 0.0, False, "REJECTED", "", "", 0.0, "ema_failed"])
                continue
            if trade_side=='short' and not (ema20 < ema50):
                seen_T.add(T);
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f); w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(), trade_side, entry_open, "", "", 0.0, False, "REJECTED", "", "", 0.0, "ema_failed"])
                continue

            # compute streaming percentiles for this T (percentile among qual up to T) using pandas.rank
            prior = qual.loc[:T]
            range_pct = float(prior['range'].fillna(0).rank(method='average', pct=True).iloc[-1])
            atr_pct = float(prior['atr14'].fillna(0).rank(method='average', pct=True).iloc[-1])

            proximity = 1.0 - min(abs(entry_open - five_close) / (five_range if five_range>0 else 1.0), 1.0)
            trend_flag = 1 if (ema20 > ema50) else 0
            score = 0.4 * range_pct + 0.3 * proximity + 0.2 * atr_pct + 0.1 * trend_flag

            # compute tp_dist (prelim), will be recomputed at actual entry before execution
            raw_dist = abs(entry_open - five_open)
            lower = 0.5 * (atr14 if (atr14 is not None and not math.isnan(atr14)) else 0.0)
            tp_dist_prelim = max(raw_dist, lower); tp_dist_prelim = min(tp_dist_prelim, 20.0)

            cs = CandidateSignal(
                T=T, five_open=five_open, five_close=five_close, five_range=five_range, atr14=atr14,
                trade_side=trade_side, R=R, entry_time=entry_time, entry_open=entry_open,
                tp_price=0.0, sl_price=0.0, score=score, accepted=False
            )

            # chronological acceptance
            if score >= cfg.score_threshold:
                cs.accepted = True
                pending_entries[entry_time] = cs
                persist_pending_add(cfg.persist_pending_path, cs)
                seen_T.add(T)
                log.info(f"[ACCEPT] T={T} R={R} entry={entry_time} side={trade_side} score={score:.3f} tp_dist_prelim={tp_dist_prelim:.2f}")
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f); w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(), trade_side, entry_open, "", "", score, True, "", "", "", "", "accepted"])
            else:
                seen_T.add(T)
                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f); w.writerow([datetime.now(IST).isoformat(), T.isoformat(), R.isoformat(), entry_time.isoformat(), trade_side, entry_open, "", "", score, False, "", "", "", "", "rejected_score"])
                log.info(f"[REJECT] T={T} R={R} score={score:.3f} (threshold {cfg.score_threshold})")

        # Execute pending entries when due (within grace)
        for et in list(pending_entries.keys()):
            cs = pending_entries[et]
            if now_real >= et and within_grace(now_real, et):
                # fetch latest frame and ensure entry minute exists
                try:
                    df1_latest = fetch_today_1min(alice, cfg)
                except Exception as e:
                    log.error(f"Failed fetching latest before entry: {e}")
                    continue
                if et not in df1_latest.index:
                    log.warning(f"Entry minute {et} not present; skipping entry for T={cs.T}")
                    persist_pending_remove(cfg.persist_pending_path, et.isoformat())
                    pending_entries.pop(et, None)
                    continue
                entry_price = float(df1_latest.at[et, 'open'])
                # recompute TP/SL at actual entry price
                raw_dist = abs(entry_price - cs.five_open)
                lower = 0.5 * (cs.atr14 if (cs.atr14 is not None and not math.isnan(cs.atr14)) else 0.0)
                tp_dist = max(raw_dist, lower); tp_dist = min(tp_dist, 20.0)
                if tp_dist <= 0:
                    log.warning(f"Degenerate tp_dist=0 for T={cs.T}, skipping")
                    persist_pending_remove(cfg.persist_pending_path, et.isoformat())
                    pending_entries.pop(et, None)
                    continue
                if cs.trade_side == 'long':
                    tp_price = entry_price + tp_dist; sl_price = entry_price - 0.5 * tp_dist; direction = "BUY"
                else:
                    tp_price = entry_price - tp_dist; sl_price = entry_price + 0.5 * tp_dist; direction = "SHORT"

                cs.entry_open = entry_price; cs.tp_price = tp_price; cs.sl_price = sl_price; cs.accepted = True

                log.info(f"ENTRY {direction} @ {entry_price:.2f} | T={cs.T} R={cs.R} | TP={tp_price:.2f} SL={sl_price:.2f} | score={cs.score:.3f}")
                send_signal(cfg, direction, payload_extra={
                    "T": cs.T.isoformat(), "R": cs.R.isoformat(), "entry_time": et.isoformat(),
                    "entry_price": entry_price, "tp": tp_price, "sl": sl_price, "score": cs.score
                })

                with open(cfg.signal_log_csv, "a", newline="") as f:
                    w = csv.writer(f); w.writerow([datetime.now(IST).isoformat(), cs.T.isoformat(), cs.R.isoformat(), et.isoformat(), cs.trade_side, entry_price, tp_price, sl_price, cs.score, True, "ENTRY_SENT", "", "", "", "entry_sent"])
                # remove persisted and in-memory
                persist_pending_remove(cfg.persist_pending_path, et.isoformat())
                pending_entries.pop(et, None)

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
