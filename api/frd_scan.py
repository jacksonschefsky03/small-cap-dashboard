"""api/frd_scan.py -- Vercel serverless FRD scanner (volume-funnel, rules baked in)."""
import json, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
from urllib import parse as _pp

import _polygon as P

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


def _is_green(b):
    return b[4] is not None and b[1] is not None and b[4] > b[1]


def find_live_run(ser):
    if len(ser) < MIN_GREENS or not _is_green(ser[-1]):
        return None
    g = len(ser) - 1; streak = 1
    while g - 1 >= 0 and _is_green(ser[g - 1]) and (ser[g][5] or 0) > (ser[g - 1][5] or 0):
        streak += 1; g -= 1
    if streak < MIN_GREENS:
        return None
    return ser[len(ser) - streak:]


def qualifies(tkr, run, asof):
    first_open = run[0][1]; seq_high = max(b[2] for b in run)
    if not first_open or first_open <= 0:
        return False
    if (seq_high / first_open - 1) < MIN_RUN_PCT:
        return False
    for i in range(1, len(run)):
        pc = run[i - 1][4] or 0; no = run[i][1] or 0
        if pc > 0 and no > 0:
            j = no / pc
            if j >= SPLIT_JUMP or j <= (1.0 / SPLIT_JUMP):
                return False
    tot = seq_high - first_open
    if tot > 0:
        big = max((b[4] - b[1]) for b in run)
        if big / tot > MAX_SINGLE_DAY_SHARE:
            return False
    det = _details_cache.get(tkr)
    if det is None:
        det = P.ticker_details(tkr, run[0][0]) or {}
        _details_cache[tkr] = det
    if det.get("type") not in COMMON_STOCK_TYPES:
        return False
    nl = (det.get("name", "") or "").lower()
    if any(p in nl for p in LEVERAGED_NAME_PATTERNS):
        return False
    return True


def fast_scan(asof=None):
    if not P.API_KEY:
        return {"error": "POLYGON_API_KEY not set on the server"}
    grouped, tdays = P.most_recent_completed_days(2, asof=asof)
    if len(tdays) < 2:
        return {"error": "not enough trading days available"}
    recent, prev = tdays[-1], tdays[-2]
    g_recent, g_prev = grouped.get(recent, {}), grouped.get(prev, {})
    s1 = []
    for tkr, row in g_recent.items():
        if (row.get("v") or 0) < MIN_RECENT_VOL:
            continue
        hi, lo = row.get("h") or 0, row.get("l") or 0
        if lo <= 0 or (hi / lo - 1.0) < MIN_RANGE:
            continue
        s1.append(tkr)
    s2 = [t for t in s1
          if (g_recent.get(t, {}).get("v") or 0) > ((g_prev.get(t, {}) or {}).get("v") or 0)]
    start_hist = (dt.date.fromisoformat(recent)
                  - dt.timedelta(days=int(LOOKBACK_DAYS * 1.5))).isoformat()

    def verify(tkr):
        rb = P.daily_bars_range(tkr, start_hist, recent)
        rser = [(b["date"], b["open"], b["high"], b["low"], b["close"], b["volume"])
                for b in rb if b["open"] is not None and b["date"] <= recent]
        if len(rser) < MIN_GREENS:
            return None
        run = find_live_run(rser)
        if not run:
            return None
        try:
            if qualifies(tkr, run, recent):
                fo = run[0][1]; sh = max(b[2] for b in run)
                return {"ticker": tkr, "run_pct": round((sh / fo - 1) * 100),
                        "days": len(run), "last_date": run[-1][0]}
        except Exception:
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
        except Exception as e:
            res = {"error": f"scan failed: {e}"}
        body = json.dumps(res).encode("utf-8")
        code = 200 if not res.get("error") else 500
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
