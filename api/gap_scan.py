"""api/gap_scan.py -- Vercel serverless gap-up screener."""
import json, datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler
from urllib import parse as _pp

import _polygon as P

PREFILTER_GAP = 60.0
WORKERS = 25


def premarket_last_price(ticker, date_str):
    mb = P.minute_bars(ticker, date_str)
    pre = [b for b in mb if P.minute_of_day_et(b["t"]) < P.RTH_OPEN]
    if not pre:
        return None, 0
    pre_vol = sum(b.get("v", 0) for b in pre)
    return pre[-1].get("c"), pre_vol


def gap_scan(min_gap=100.0, min_price=3.0, min_pm_vol=2_000_000):
    if not P.API_KEY:
        return {"error": "POLYGON_API_KEY not set on the server"}
    today = dt.datetime.now(P.ET).date().isoformat()
    tks = P.snapshot_all()
    if not tks:
        return {"exists": True, "scan_time": _now(), "hits": [],
                "params": _params(min_gap, min_price, min_pm_vol),
                "note": "snapshot empty (market may be closed / pre-open data not posted)"}
    cands = []
    for t in tks:
        prev = (t.get("prevDay") or {}).get("c")
        if not prev or prev <= 0:
            continue
        day = t.get("day") or {}; mn = t.get("min") or {}
        lt = (t.get("lastTrade") or {}).get("p") or 0
        best = max(day.get("h") or 0, day.get("o") or 0, day.get("c") or 0,
                   mn.get("c") or 0, mn.get("h") or 0, lt)
        if best and (best / prev - 1) * 100 >= PREFILTER_GAP:
            cands.append((t.get("ticker"), prev))

    def check(pair):
        sym, prev = pair
        try:
            pm_px, pm_vol = premarket_last_price(sym, today)
        except Exception:
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
    return dt.datetime.now(P.ET).strftime("%Y-%m-%d %H:%M:%S ET")


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
