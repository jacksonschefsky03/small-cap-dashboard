"""
api/gap_scan.py -- Vercel serverless gap-up screener.

Finds stocks whose ~9:25 PREMARKET price is >= min_gap% above yesterday's close,
price >= min_price, premarket volume >= min_pm_vol. Same method as the local
gap_up_screener.py:
  1. one full-market snapshot -> prior close + today's day high/open
  2. pre-filter to names that were up big today (cheap)
  3. pull premarket 1-min for those only; gap off the last pre-9:30 print

Returns JSON: { exists, scan_time, params, hits:[{ticker,prev,pm,gap,vol}] }
Premarket 1-min pulls are parallelized so the whole thing stays within the
serverless time budget.
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


PREFILTER_GAP = 60.0     # cheap pre-filter (lower than min_gap so we never miss one)
WORKERS = 25


def premarket_last_price(ticker, date_str):
    """(last pre-9:30 1-min close, total premarket volume) for the ET date."""
    mb = minute_bars(ticker, date_str)
    pre = [b for b in mb if minute_of_day_et(b["t"]) < RTH_OPEN]
    if not pre:
        return None, 0
    pre_vol = sum(b.get("v", 0) for b in pre)
    return pre[-1].get("c"), pre_vol


def gap_scan(min_gap=100.0, min_price=3.0, min_pm_vol=2_000_000):
    if not API_KEY:
        return {"error": "POLYGON_API_KEY not set on the server"}
    today = dt.datetime.now(ET).date().isoformat()
    tks = snapshot_all()
    if not tks:
        return {"exists": True, "scan_time": _now(), "hits": [],
                "params": _params(min_gap, min_price, min_pm_vol),
                "note": "snapshot empty (market may be closed / pre-open data not posted)"}

    # pre-filter: up big today on day high/open/last vs prior close
    cands = []
    for t in tks:
        prev = (t.get("prevDay") or {}).get("c")
        if not prev or prev <= 0:
            continue
        day = t.get("day") or {}
        mn = t.get("min") or {}
        lt = (t.get("lastTrade") or {}).get("p") or 0
        best = max(day.get("h") or 0, day.get("o") or 0, day.get("c") or 0,
                   mn.get("c") or 0, mn.get("h") or 0, lt)
        if best and (best / prev - 1) * 100 >= PREFILTER_GAP:
            cands.append((t.get("ticker"), prev))

    def check(pair):
        sym, prev = pair
        try:
            pm_px, pm_vol = premarket_last_price(sym, today)
        except Exception:      # noqa
            return None
        if not pm_px:
            return None
        gap = (pm_px / prev - 1) * 100
        if gap >= min_gap and pm_px >= min_price and pm_vol >= min_pm_vol:
            return {"ticker": sym, "prev": prev, "pm": pm_px, "gap": gap, "vol": pm_vol}
        return None

    hits = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(check, p) for p in cands]
        for f in as_completed(futs):
            r = f.result()
            if r:
                hits.append(r)
    hits.sort(key=lambda h: -h["gap"])
    return {"exists": True, "scan_time": _now(),
            "params": _params(min_gap, min_price, min_pm_vol), "hits": hits}


def _now():
    return dt.datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S ET")


def _params(g, p, v):
    return {"min_gap": g, "min_price": p, "min_pm_vol": v}


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        q = _pp.parse_qs(_pp.urlparse(self.path).query)
        def f(name, d):
            try:
                return float((q.get(name) or [d])[0])
            except (TypeError, ValueError):
                return d
        try:
            res = gap_scan(min_gap=f("min_gap", 100.0),
                           min_price=f("min_price", 3.0),
                           min_pm_vol=f("min_pm_vol", 2_000_000.0))
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
