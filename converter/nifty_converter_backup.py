#!/usr/bin/env python3
"""
nifty_ce_pe_converter_agg.py

Converter + Aggregator (single-process) for NIFTY FUT + ATM CE + ATM PE.

Features included (final, ready-to-run):
 - Robust dhanscrip.csv parsing (SEM_* column names)
 - Auto-detect FUT underlying (or override FUT_SECURITYID)
 - Select nearest ATM strike and subscribe to: FUT + ATM_CE + ATM_PE
 - Uses SYSTEM UTC time for ticks (ignores feed ts) to avoid feed offset problems
 - Per-second candles:  ~/dhan_live/candles_<SECID>.csv
     header: datetime (UTC ISO), datetime_ist, open, high, low, close, volume, raw
 - Per-minute candles: ~/dhan_live/agg_<SECID>_1min.csv
     header: datetime (UTC ISO), datetime_ist, open, high, low, close, volume
 - Writes atomic ~/dhan_live/current_subs.json on each ATM resubscribe
 - Rate-limit/backoff protections, resubscribe throttle
 - Heartbeat and flush threads, graceful SIGINT/SIGTERM shutdown
 - Safe CSV writes with optional fsync
"""
import os
import csv
import time
import json
import struct
import signal
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict, deque

# Force Python to use UTC internally for timestamp formatting
# (needed when environment/timezone may differ)
os.environ["TZ"] = "UTC"
try:
    time.tzset()
except Exception:
    # time.tzset exists on Unix; ignore on other platforms
    pass

# websocket-client import
try:
    import websocket  # pip3 install websocket-client
except Exception:
    print("Missing dependency: websocket-client. Install with: pip3 install websocket-client")
    raise SystemExit(1)

# -------------------- CONFIG (edit these) --------------------
CLIENT_ID = ""            # <<< REPLACE with your CLIENT ID
ACCESS_TOKEN = ""            # <<< REPLACE with your ACCESS TOKEN
FUT_SECURITYID = "51714"               # e.g. "53001" or None to auto-detect
AUTO_DETECT = False
UNDERLYING_SYMBOL_PREFIX = "NIFTY-"
INDEX_SECURITYID = "13"       # NIFTY INDEX (spot)

OUT_DIR = Path.home() / "dhan_live"
OUT_DIR.mkdir(parents=True, exist_ok=True)
DHAN_SCRIP = OUT_DIR / "dhanscrip.csv"
CURRENT_SUBS_PATH = OUT_DIR / "current_subs.json"

WS_URL = f"wss://api-feed.dhan.co?version=2&token={ACCESS_TOKEN}&clientId={CLIENT_ID}&authType=2"

ATM_SWITCH_THRESHOLD = 50.0   # points to trigger ATM switch
HEARTBEAT_INTERVAL = 10      # heartbeat print (s)
FLUSH_INTERVAL = 1.0         # flush loop interval (s)
RESUBSCRIBE_MIN_SEC = 10     # never resubscribe faster than this
RATE_LIMIT_COOLDOWN = 120    # cooldown if rate-limited (s)
MAX_BACKOFF = 300            # max backoff (s)
FSYNC_ON_WRITE = True
MAX_TRACKED_SECURITIES = 256
SUBSCRIBE_WINDOW_S = 60
MAX_SUB_PER_WINDOW = 8

# CSV headers
CSV_SEC_HEADER = ["datetime", "datetime_ist", "open", "high", "low", "close", "volume", "raw"]
CSV_MIN_HEADER = ["datetime", "datetime_ist", "open", "high", "low", "close", "volume"]

# IST timezone helper
IST_TZ = timezone(timedelta(hours=5, minutes=30))

# global stop event
STOP = threading.Event()

# -------------------- utilities --------------------
def now_utc_iso(ts=None):
    t = datetime.fromtimestamp(int(ts or time.time()), tz=timezone.utc)
    return t.isoformat().replace("+00:00", "Z")

def to_ist_iso(ts):
    t = datetime.fromtimestamp(int(ts), tz=IST_TZ)
    return t.isoformat()

def ensure_csv_header(path: Path, header):
    if not path.exists():
        tmp = str(path) + ".tmp"
        with open(tmp, "w", newline="") as f:
            csv.writer(f).writerow(header)
            f.flush()
            if FSYNC_ON_WRITE:
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
        os.replace(tmp, path)

def append_row(path: Path, header, row):
    ensure_csv_header(path, header)
    with open(path, "a", newline="") as f:
        csv.writer(f).writerow(row)
        f.flush()
        if FSYNC_ON_WRITE:
            try:
                os.fsync(f.fileno())
            except Exception:
                pass

# -------------------- dhanscrip parser --------------------
def parse_dhanscrip(path: Path):
    options_by_strike = defaultdict(dict)
    futures = []
    if not path.exists():
        print(f" dhanscrip.csv not found at {path}")
        return options_by_strike, futures

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        SECID_KEYS = ["SecurityId", "SEM_SMST_SECURITY_ID", "SECID"]
        SYMB_KEYS = ["TradingSymbol", "SEM_TRADING_SYMBOL", "SYMB"]
        STRIKE_KEYS = ["StrikePrice", "SEM_STRIKE_PRICE", "STRIKE"]
        EXP_KEYS = ["Expiry", "SEM_EXPIRY_DATE", "SEM_EXPIRY_CODE"]
        EXCH_KEYS = ["Exchange", "SEM_EXM_EXCH_ID", "EXCH"]

        def pick(row, keys):
            for k in keys:
                v = row.get(k)
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            return ""

        for r in reader:
            sym = pick(r, SYMB_KEYS).upper()
            secid = pick(r, SECID_KEYS)
            exch = pick(r, EXCH_KEYS)
            strike_raw = pick(r, STRIKE_KEYS)
            expiry = pick(r, EXP_KEYS)
            strike = None
            try:
                if strike_raw:
                    strike = float(strike_raw)
            except Exception:
                strike = None

            if not sym or not secid:
                continue
            # ---- EXPIRY DATE FILTER (CRITICAL) ----
            try:
                exp_date = datetime.strptime(expiry[:10], "%Y-%m-%d").date()
            except Exception:
                continue

            today_ist = datetime.now(IST_TZ).date()

            # skip expired option contracts
            if exp_date < today_ist:
                continue

            if (
                sym.startswith("NIFTY")
                and (sym.endswith("CE") or sym.endswith("PE"))
                and strike is not None
                and expiry
            ):
            
                typ = "CE" if sym.endswith("CE") else "PE"
                prev = options_by_strike[strike].get(typ)

                if prev is None:
                    options_by_strike[strike][typ] = {
                        "SecurityId": secid,
                        "TradingSymbol": sym,
                        "Exchange": exch,
                        "Expiry": expiry,
                    }
                else:
                    new_dt = None
                    prev_dt = None

                    if expiry and len(expiry) >= 10:
                        try:
                            new_dt = datetime.strptime(expiry[:10], "%Y-%m-%d").date()
                        except Exception:
                            new_dt = None

                    prev_exp = prev.get("Expiry")
                    if prev_exp and len(prev_exp) >= 10:
                        try:
                            prev_dt = datetime.strptime(prev_exp[:10], "%Y-%m-%d").date()
                        except Exception:
                            prev_dt = None

                    if new_dt and (prev_dt is None or new_dt < prev_dt):
                        options_by_strike[strike][typ] = {
                            "SecurityId": secid,
                            "TradingSymbol": sym,
                            "Exchange": exch,
                            "Expiry": expiry,
                        }  
    return options_by_strike, futures

# -------------------- safe atomic write for current_subs.json --------------------
def write_current_subs_json(underlying, atm_strike, ce_id, pe_id):
    payload = {
        "timestamp": now_utc_iso(),
        "underlying": str(underlying) if underlying is not None else None,
        "atm_strike": atm_strike,
        "ce_id": str(ce_id) if ce_id is not None else None,
        "pe_id": str(pe_id) if pe_id is not None else None,
    }
    tmp = str(CURRENT_SUBS_PATH) + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(payload, f)
            f.flush()
            if FSYNC_ON_WRITE:
                os.fsync(f.fileno())
        os.replace(tmp, CURRENT_SUBS_PATH)
        print(f" Wrote current_subs.json -> underlying={payload['underlying']} atm={payload['atm_strike']} ce={payload['ce_id']} pe={payload['pe_id']}")
    except Exception as e:
        print("[error] failed to write current_subs.json:", e)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except:
            pass

# -------------------- CandleMaker --------------------
class CandleMaker:
    def __init__(self):
        self.lock = threading.Lock()
        self.sec_state = {}   # secid -> current second bucket
        self.min_state = {}   # secid -> {minute_ts: agg}
        self.last_seen = {}

    def feed_tick(self, secid: str, price: float, vol: int, ts: int, raw: bytes):
        """
        ts must be epoch seconds (we use system time)
        """
        with self.lock:
            now = int(time.time())
            self.last_seen[secid] = now
            ts_int = int(ts)
            s = self.sec_state.get(secid)
            if not s or s["ts"] != ts_int:
                if s:
                    self._flush_second(secid, s)
                self.sec_state[secid] = {"ts": ts_int, "o": price, "h": price, "l": price, "c": price, "v": vol or 0, "raw": raw.hex()}
            else:
                s["h"] = max(s["h"], price)
                s["l"] = min(s["l"], price)
                s["c"] = price
                s["v"] = s.get("v", 0) + (vol or 0)
                s["raw"] = raw.hex()

    def _flush_second(self, secid, s):
        ts = int(s["ts"])
        dt_utc = now_utc_iso(ts)
        dt_ist = to_ist_iso(ts)
        sec_path = OUT_DIR / f"candles_{secid}.csv"
        append_row(sec_path, CSV_SEC_HEADER, [dt_utc, dt_ist, s["o"], s["h"], s["l"], s["c"], s["v"], s["raw"]])
        self._ingest_minute(secid, s)

    def _ingest_minute(self, secid, s_second):
        minute_ts = (int(s_second["ts"]) // 60) * 60
        ms = self.min_state.setdefault(secid, {})
        m = ms.get(minute_ts)
        if not m:
            ms[minute_ts] = {"o": s_second["o"], "h": s_second["h"], "l": s_second["l"], "c": s_second["c"], "v": s_second["v"]}
        else:
            m["h"] = max(m["h"], s_second["h"])
            m["l"] = min(m["l"], s_second["l"])
            m["c"] = s_second["c"]
            m["v"] = m.get("v", 0) + s_second["v"]

    def flush_old(self):
        with self.lock:
            now = int(time.time())
            # flush seconds older than 2s
            for secid, s in list(self.sec_state.items()):
                if now - int(s["ts"]) > 2:
                    try:
                        self._flush_second(secid, s)
                    except Exception:
                        pass
                    self.sec_state.pop(secid, None)
            # flush complete minutes
            current_minute = (now // 60) * 60
            for secid, ms in list(self.min_state.items()):
                for minute_ts in sorted(list(ms.keys())):
                    if minute_ts < current_minute:
                        m = ms.pop(minute_ts)
                        dt_utc = now_utc_iso(minute_ts)
                        dt_ist = to_ist_iso(minute_ts)
                        agg_path = OUT_DIR / f"agg_{secid}_1min.csv"
                        append_row(agg_path, CSV_MIN_HEADER, [dt_utc, dt_ist, m["o"], m["h"], m["l"], m["c"], m["v"]])
                if not ms:
                    self.min_state.pop(secid, None)

    def get_last_trade_age(self, secid):
        t = self.last_seen.get(secid)
        return None if not t else int(time.time()) - t

    def switch_active_secids(self, new_secids):
        """
        Flush state for secids that are no longer active and drop them.
        """
        with self.lock:
            for secid in list(self.sec_state.keys()):
                if secid not in new_secids:
                    try:
                        self._flush_second(secid, self.sec_state.pop(secid))
                    except Exception:
                        pass
            for secid in list(self.min_state.keys()):
                if secid not in new_secids:
                    for minute_ts, m in self.min_state[secid].items():
                        dt_utc = now_utc_iso(minute_ts)
                        dt_ist = to_ist_iso(minute_ts)
                        append_row(OUT_DIR / f"agg_{secid}_1min.csv", CSV_MIN_HEADER, [dt_utc, dt_ist, m["o"], m["h"], m["l"], m["c"], m["v"]])
                    self.min_state.pop(secid, None)
            for secid in list(self.last_seen.keys()):
                if secid not in new_secids:
                    self.last_seen.pop(secid, None)


candle_maker = CandleMaker()

# -------------------- AutoSubManager (WS + ATM switching) --------------------
class AutoSubManager:
    def __init__(self, ws_url):
        self.ws_url = ws_url
        self.ws = None
        self.lock = threading.Lock()
        self.option_map, self.futures = parse_dhanscrip(DHAN_SCRIP)
        self.strikes = sorted(self.option_map.keys())
        self.underlying_secid = str(FUT_SECURITYID) if FUT_SECURITYID else None
        self.atm_strike = None
        self.subscribed = set()
        self.last_resubscribe = 0
        self.last_pong = time.time()
        self.last_rate_limit = 0
        self.subscribe_attempts = deque()
        # auto-detect underlying if not set
        if not self.underlying_secid and AUTO_DETECT and self.futures:
            for f in self.futures:
                sym = f.get("TradingSymbol", "").upper()
                if sym.startswith(UNDERLYING_SYMBOL_PREFIX) and not (sym.endswith("CE") or sym.endswith("PE")):
                    self.underlying_secid = f.get("SecurityId")
                    break
            if not self.underlying_secid and self.futures:
                self.underlying_secid = self.futures[0].get("SecurityId")
        print("Using underlying SecurityId:", self.underlying_secid)

    def build_subscribe_payload(self, secids):
        inst = []

        for s in secids:
            if str(s) == INDEX_SECURITYID:
                inst.append({
                    "ExchangeSegment": "IDX_I",
                    "SecurityId": str(s)
                })
            else:
                inst.append({
                    "ExchangeSegment": "NSE_FNO",
                    "SecurityId": str(s)
                })

        return {
            "RequestCode": 15,
            "InstrumentCount": len(inst),
            "InstrumentList": inst
        }


    def start(self):
        print(" Converter+Aggregator starting")
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        threading.Thread(target=self._flush_loop, daemon=True).start()
        initial = set([self.underlying_secid]) if self.underlying_secid else set()
        with self.lock:
            self.subscribed = initial.copy()
        backoff = 1
        while not STOP.is_set():
            if self.last_rate_limit and time.time() - self.last_rate_limit < RATE_LIMIT_COOLDOWN:
                remaining = RATE_LIMIT_COOLDOWN - (time.time() - self.last_rate_limit)
                print(f" Rate-limited: sleeping {int(remaining)}s before reconnect")
                time.sleep(remaining)
            try:
                self._run_forever(initial)
                backoff = 1
            except Exception as e:
                print("[warn] WS main loop error:", e)
                time.sleep(min(backoff, MAX_BACKOFF))
                backoff = min(backoff * 2, MAX_BACKOFF)

    def _choose_atm_pair(self, midprice):
        if not self.strikes:
            return None, None, None
        nearest = min(self.strikes, key=lambda s: abs(s - midprice))
        ce = self.option_map.get(nearest, {}).get("CE")
        pe = self.option_map.get(nearest, {}).get("PE")
        return ce, pe, nearest

    def _maybe_switch_atm(self, midprice):
        if midprice is None or not self.strikes:
            return False
        if self.atm_strike is None:
            ce, pe, strike = self._choose_atm_pair(midprice)
            if (ce and ce.get("SecurityId")) or (pe and pe.get("SecurityId")):
                self.atm_strike = strike
                self._resubscribe_for_atm(ce, pe)
                return True
            return False
        if abs(midprice - self.atm_strike) >= ATM_SWITCH_THRESHOLD:
            ce, pe, strike = self._choose_atm_pair(midprice)
            if (ce and ce.get("SecurityId")) or (pe and pe.get("SecurityId")):
                old = self.atm_strike
                self.atm_strike = strike
                print(f" ATM moved {old} -> {self.atm_strike} (underlying {midprice})  resubscribing")
                self._resubscribe_for_atm(ce, pe)
                return True
        return False

    def _resubscribe_for_atm(self, ce, pe):
        newset = set()
        if self.underlying_secid:
            newset.add(self.underlying_secid)
            newset.add(INDEX_SECURITYID)
        if ce and ce.get("SecurityId"):
            newset.add(ce["SecurityId"])
        if pe and pe.get("SecurityId"):
            newset.add(pe["SecurityId"])
        if len(newset) > MAX_TRACKED_SECURITIES:
            print(" Requested subscription exceeds MAX_TRACKED_SECURITIES. Aborting resubscribe.")
            return
        with self.lock:
            now = time.time()
            if now - self.last_resubscribe < RESUBSCRIBE_MIN_SEC:
                print(f" Skipping resubscribe (too soon). Next allowed in {int(RESUBSCRIBE_MIN_SEC - (now - self.last_resubscribe))}s")
                return
            self.subscribed = set(newset)
            self.last_resubscribe = now
        candle_maker.switch_active_secids(self.subscribed)
        ce_id = ce.get("SecurityId") if ce else None
        pe_id = pe.get("SecurityId") if pe else None
        write_current_subs_json(self.underlying_secid, self.atm_strike, ce_id, pe_id)
        print(" New subscription set:", self.subscribed)
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass

    def _run_forever(self, initial_ids):
        def on_open(ws):
            print(" WS connected")
            with self.lock:
                ids = list(self.subscribed) if self.subscribed else list(initial_ids)
            payload = self.build_subscribe_payload(ids)
            try:
                self._record_sub_attempt()
                ws.send(json.dumps(payload))
                print(" Subscribed:", ids)
            except Exception as e:
                print("[error] subscribe send failed:", e)

        def on_message(ws, msg):
            # Handle binary tick frames and text
            if isinstance(msg, (bytes, bytearray)):
                raw = bytes(msg)
                # decode blocks of 16 bytes assumed <I I f I>
                for i in range(0, len(raw), 16):
                    block = raw[i : i + 16]
                    if len(block) < 16:
                        break
                    try:
                        vol, secid, price_f, _ = struct.unpack("<IIfI", block)
                        # FIX: use system time for tick timestamp (seconds)
                        ts_raw = int(time.time())
                        secid_s = str(secid)
                        price = float(price_f)
                        if price <= 0:
                            continue
                        candle_maker.feed_tick(secid_s, price, int(vol), ts_raw, block)
                        if self.underlying_secid and secid_s == str(self.underlying_secid):
                            self._maybe_switch_atm(price)
                    except Exception:
                        # ignore malformed block
                        continue
            else:
                s = str(msg)
                if "429" in s or "Too Many Requests" in s or "Rate limit" in s:
                    print(" Rate-limited message from server detected.")
                    self.last_rate_limit = time.time()
                    try:
                        ws.close()
                    except:
                        pass
                    return
                if len(s) > 300:
                    print("[text msg]", s[:300], "...")
                else:
                    print("[text msg]", s)

        def on_pong(ws, data):
            self.last_pong = time.time()

        def on_error(ws, err):
            print("[ws error]", err)
            try:
                if "429" in str(err) or "Too Many Requests" in str(err):
                    self.last_rate_limit = time.time()
            except Exception:
                pass

        def on_close(ws, code, reason):
            print(f"[ws closed] code={code} reason={reason}")

        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_pong=on_pong,
        )
        self.ws.run_forever(ping_interval=10, ping_timeout=5)

    def _record_sub_attempt(self):
        now = time.time()
        self.subscribe_attempts.append(now)
        while self.subscribe_attempts and now - self.subscribe_attempts[0] > SUBSCRIBE_WINDOW_S:
            self.subscribe_attempts.popleft()
        if len(self.subscribe_attempts) > MAX_SUB_PER_WINDOW:
            print(" Too many subscribe attempts  consider updating Dhan IP whitelist or rotating client-id.")
            self.last_rate_limit = time.time()

    def _heartbeat_loop(self):
        while not STOP.is_set():
            with self.lock:
                ids = list(self.subscribed)
            ages = {sid: candle_maker.get_last_trade_age(sid) for sid in ids}
            last_pong_age = int(time.time() - self.last_pong) if self.last_pong else None
            paths = {sid: {"sec": str(OUT_DIR / f"candles_{sid}.csv"), "min": str(OUT_DIR / f"agg_{sid}_1min.csv")} for sid in ids}
            print(f" heartbeat  subs={ids} atm={self.atm_strike} last_pong_s={last_pong_age} ages={ages} paths={paths}")
            time.sleep(HEARTBEAT_INTERVAL)

    def _flush_loop(self):
        while not STOP.is_set():
            candle_maker.flush_old()
            time.sleep(FLUSH_INTERVAL)

# -------------------- signals --------------------
def _graceful_shutdown(signum, frame):
    print(" Received stop signal, shutting down...")
    STOP.set()

signal.signal(signal.SIGINT, _graceful_shutdown)
signal.signal(signal.SIGTERM, _graceful_shutdown)

# -------------------- main --------------------
def main():
    print("Starting Converter+Aggregator")
    print("Output dir:", OUT_DIR)
    option_map, futures = parse_dhanscrip(DHAN_SCRIP)
    if not option_map:
        print(" dhanscrip.csv empty/missing. Running monitor-only until file appears or set FUT_SECURITYID.")
    manager = AutoSubManager(WS_URL)
    try:
        manager.start()
    except KeyboardInterrupt:
        STOP.set()
    finally:
        STOP.set()
        print("Flushing and exiting...")

if __name__ == "__main__":
    main()

