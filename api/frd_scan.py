"""
api/frd_scan.py -- Vercel serverless FRD scanner.

Runs the fast volume-funnel First-Red-Day scan and returns JSON:
  { asof, count, setups:[{ticker,run_pct,days,last_date}], funnel, timing }

Funnel (same as the local frd_fast_scanner):
  Stage 1  most-recent-day volume >= 5M  AND  low-to-high range >= 5%   (cheap, grouped)
  Stage 2  most-recent-day volume > prior-day volume                    (cheap, grouped)
  Stage 3  regular-hours history + run rules on survivors (parallel)

The run rules are baked in here (find_live_run + qualifies, rule 7 removed) so
this function is self-contained -- no cross-file imports beyond _polygon.
"""
import json, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
from urllib import parse as _pp


# ==== inlined Polygon helpers (self-contained) ====
import os, json, time, datetime as dt
from urllib import request as _rq, parse as _pp, error as _er
from zoneinfo import ZoneInfo

API_KEY = os.environ.get("POLYGON_API_KEY")
BASE = "https://api.polygon.io"
ET = ZoneInfo("America/New_York")
RTH_OPEN = 9 * 60 + 30
RTH_CLOSE = 16 * 60


def _get(path, params=None, tries=4):
    """GET a Polygon endpoint, return parsed JSON (or {} on 403/404/failure).
    Mirrors the local _get's retry/skip behavior using urllib."""
    if not API_KEY:
        return {"__error__": "POLYGON_API_KEY not set"}
    params = dict(params or {})
    params["apiKey"] = API_KEY
    url = BASE + path + "?" + _pp.urlencode(params)
    for attempt in range(tries):
        try:
            with _rq.urlopen(url, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except _er.HTTPError as e:
            if e.code in (403, 404):        # outside plan window / not found -> skip
                return {}
            if e.code == 429 or e.code >= 500:
                time.sleep(1.5 * (attempt + 1))
                continue
            return {}
        except Exception:                    # noqa (network hiccup)
            if attempt == tries - 1:
                return {}
            time.sleep(1.0 * (attempt + 1))
    return {}


def to_et(t_ms):
    return dt.datetime.fromtimestamp(t_ms / 1000, tz=dt.timezone.utc).astimezone(ET)


def minute_of_day_et(t_ms):
    e = to_et(t_ms)
    return e.hour * 60 + e.minute


def grouped_daily(date_str):
    """Whole-market OHLCV for one day (all-hours). {ticker: {o,h,l,c,v}}."""
    j = _get(f"/v2/aggs/grouped/locale/us/market/stocks/{date_str}", {"adjusted": "false"})
    return {row["T"]: {"o": row.get("o"), "h": row.get("h"), "l": row.get("l"),
                       "c": row.get("c"), "v": row.get("v")}
            for row in (j.get("results", []) or [])}


def daily_bars_range(ticker, start_date, end_date):
    """Regular daily bars for a ticker over [start,end], unadjusted, ascending.
    Returns [{date, open, high, low, close, volume}]."""
    j = _get(f"/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}",
             {"adjusted": "false", "sort": "asc", "limit": 50000})
    out = []
    for b in (j.get("results", []) or []):
        d = to_et(b["t"]).date().isoformat()
        out.append({"date": d, "open": b.get("o"), "high": b.get("h"),
                    "low": b.get("l"), "close": b.get("c"), "volume": b.get("v")})
    return out


def minute_bars(ticker, date_str):
    """1-min bars for an ET date (captures full session incl. afterhours)."""
    nxt = (dt.date.fromisoformat(date_str) + dt.timedelta(days=1)).isoformat()
    j = _get(f"/v2/aggs/ticker/{ticker}/range/1/minute/{date_str}/{nxt}",
             {"adjusted": "false", "sort": "asc", "limit": 50000})
    res = j.get("results", []) or []
    return [b for b in res if to_et(b["t"]).date().isoformat() == date_str]


def ticker_details(ticker, date_str):
    j = _get(f"/v3/reference/tickers/{ticker}", {"date": date_str})
    return j.get("results", {}) or {}


def snapshot_all():
    """Full-market snapshot (one call): prev close, day OHLC, min, lastTrade."""
    j = _get("/v2/snapshot/locale/us/markets/stocks/tickers", {})
    return j.get("tickers", []) or []


def most_recent_completed_days(n=2, asof=None):
    """Last `n` completed trading days (ascending), fetching ONLY the days needed
    (walk backward, stop when we have n). Same minimal-fetch logic as the local
    fast scanner. Returns (grouped_by_date, [trading_days])."""
    if asof:
        end = dt.date.fromisoformat(asof)
        cutoff = end + dt.timedelta(days=1)
    else:
        now_et = dt.datetime.now(ET)
        end = now_et.date()
        closed = now_et.hour > 16 or (now_et.hour == 16 and now_et.minute >= 0)
        cutoff = end + dt.timedelta(days=1) if (closed and now_et.weekday() < 5) else end
    grouped, tdays = {}, []
    probe = min(end, cutoff - dt.timedelta(days=1))
    guard = 0
    while len(tdays) < n and guard < 15:
        ds = probe.isoformat()
        if ds < cutoff.isoformat():
            g = grouped_daily(ds)
            if g:
                grouped[ds] = g
                tdays.append(ds)
        probe -= dt.timedelta(days=1)
        guard += 1
    tdays.sort()
    return grouped, tdays

# ==== end helpers ====


# ---- FRD rule constants (mirror frd_live_scanner.py) ----
MIN_GREENS = 2
MIN_RUN_PCT = 1.00
MAX_SINGLE_DAY_SHARE = 0.70
SPLIT_JUMP = 3.0
LOOKBACK_DAYS = 60
COMMON_STOCK_TYPES = {"CS", "ADRC"}
LEVERAGED_NAME_PATTERNS = ["etf", "etn", "leveraged", "2x", "3x", "ultra",
                           "proshares", "direxion", "-1x", "inverse"]

MIN_RECENT_VOL = 5_000_000
MIN_RANGE = 0.05
WORKERS = 25
_details_cache = {}


def _is_green(b):      # b = (date, o, h, l, c, v)
    return b[4] is not None and b[1] is not None and b[4] > b[1]


def find_live_run(ser):
    """Consecutive rising-volume green days at the tail (>= MIN_GREENS)."""
    if len(ser) < MIN_GREENS or not _is_green(ser[-1]):
        return None
    g = len(ser) - 1
    streak = 1
    while g - 1 >= 0 and _is_green(ser[g - 1]) and (ser[g][5] or 0) > (ser[g - 1][5] or 0):
        streak += 1
        g -= 1
    if streak < MIN_GREENS:
        return None
    return ser[len(ser) - streak:]


def qualifies(tkr, run, asof):
    first_open = run[0][1]
    seq_high = max(b[2] for b in run)
    if not first_open or first_open <= 0:
        return False
    if (seq_high / first_open - 1) < MIN_RUN_PCT:
        return False
    for i in range(1, len(run)):
        pc = run[i - 1][4] or 0
        no = run[i][1] or 0
        if pc > 0 and no > 0:
            j = no / pc
            if j >= SPLIT_JUMP or j <= (1.0 / SPLIT_JUMP):
                return False
    tot = seq_high - first_open
    if tot > 0:
        big = max((b[4] - b[1]) for b in run)
        if big / tot > MAX_SINGLE_DAY_SHARE:
            return False
    # rule 7 (peak green vol >= 5M) intentionally REMOVED
    det = _details_cache.get(tkr)
    if det is None:
        det = ticker_details(tkr, run[0][0]) or {}
        _details_cache[tkr] = det
    if det.get("type") not in COMMON_STOCK_TYPES:
        return False
    nl = (det.get("name", "") or "").lower()
    if any(p in nl for p in LEVERAGED_NAME_PATTERNS):
        return False
    return True


def fast_scan(asof=None):
    if not API_KEY:
        return {"error": "POLYGON_API_KEY not set on the server"}
    grouped, tdays = most_recent_completed_days(2, asof=asof)
    if len(tdays) < 2:
        return {"error": "not enough trading days available"}
    recent, prev = tdays[-1], tdays[-2]
    g_recent, g_prev = grouped.get(recent, {}), grouped.get(prev, {})

    # Stage 1: volume floor + range gate (free from grouped data)
    s1 = []
    for tkr, row in g_recent.items():
        if (row.get("v") or 0) < MIN_RECENT_VOL:
            continue
        hi, lo = row.get("h") or 0, row.get("l") or 0
        if lo <= 0 or (hi / lo - 1.0) < MIN_RANGE:
            continue
        s1.append(tkr)
    # Stage 2: rising volume vs prior day
    s2 = [t for t in s1
          if (g_recent.get(t, {}).get("v") or 0) > ((g_prev.get(t, {}) or {}).get("v") or 0)]

    # Stage 3: regular-hours history + rules on survivors (parallel)
    start_hist = (dt.date.fromisoformat(recent)
                  - dt.timedelta(days=int(LOOKBACK_DAYS * 1.5))).isoformat()

    def verify(tkr):
        rb = daily_bars_range(tkr, start_hist, recent)
        rser = [(b["date"], b["open"], b["high"], b["low"], b["close"], b["volume"])
                for b in rb if b["open"] is not None and b["date"] <= recent]
        if len(rser) < MIN_GREENS:
            return None
        run = find_live_run(rser)
        if not run:
            return None
        try:
            if qualifies(tkr, run, recent):
                fo = run[0][1]
                sh = max(b[2] for b in run)
                return {"ticker": tkr, "run_pct": round((sh / fo - 1) * 100),
                        "days": len(run), "last_date": run[-1][0]}
        except Exception:      # noqa
            return None
        return None

    hits = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(verify, t) for t in s2]
        for f in as_completed(futs):
            r = f.result()
            if r:
                hits.append(r)
    hits.sort(key=lambda h: -h["run_pct"])
    return {"asof": recent, "count": len(hits), "setups": hits,
            "funnel": {"market": len(g_recent), "over_vol": len(s1),
                       "rising": len(s2), "qualified": len(hits)}}


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        q = _pp.parse_qs(_pp.urlparse(self.path).query)
        asof = (q.get("asof") or [None])[0]
        try:
            res = fast_scan(asof=asof)
        except Exception as e:      # noqa
            res = {"error": f"scan failed: {e}"}
        body = json.dumps(res).encode("utf-8")
        code = 200 if not res.get("error") else 500
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
