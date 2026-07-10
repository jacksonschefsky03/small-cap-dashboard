"""Lightweight Polygon helpers for Vercel serverless functions (urllib, no deps)."""
import os, json, time, datetime as dt
from urllib import request as _rq, parse as _pp, error as _er
from zoneinfo import ZoneInfo

API_KEY = os.environ.get("POLYGON_API_KEY")
BASE = "https://api.polygon.io"
ET = ZoneInfo("America/New_York")
RTH_OPEN = 9 * 60 + 30
RTH_CLOSE = 16 * 60


def _get(path, params=None, tries=4):
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
            if e.code in (403, 404):
                return {}
            if e.code == 429 or e.code >= 500:
                time.sleep(1.5 * (attempt + 1)); continue
            return {}
        except Exception:
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
    j = _get(f"/v2/aggs/grouped/locale/us/market/stocks/{date_str}", {"adjusted": "false"})
    return {row["T"]: {"o": row.get("o"), "h": row.get("h"), "l": row.get("l"),
                       "c": row.get("c"), "v": row.get("v")}
            for row in (j.get("results", []) or [])}


def daily_bars_range(ticker, start_date, end_date):
    j = _get(f"/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}",
             {"adjusted": "false", "sort": "asc", "limit": 50000})
    out = []
    for b in (j.get("results", []) or []):
        d = to_et(b["t"]).date().isoformat()
        out.append({"date": d, "open": b.get("o"), "high": b.get("h"),
                    "low": b.get("l"), "close": b.get("c"), "volume": b.get("v")})
    return out


def minute_bars(ticker, date_str):
    nxt = (dt.date.fromisoformat(date_str) + dt.timedelta(days=1)).isoformat()
    j = _get(f"/v2/aggs/ticker/{ticker}/range/1/minute/{date_str}/{nxt}",
             {"adjusted": "false", "sort": "asc", "limit": 50000})
    res = j.get("results", []) or []
    return [b for b in res if to_et(b["t"]).date().isoformat() == date_str]


def ticker_details(ticker, date_str):
    j = _get(f"/v3/reference/tickers/{ticker}", {"date": date_str})
    return j.get("results", {}) or {}


def snapshot_all():
    j = _get("/v2/snapshot/locale/us/markets/stocks/tickers", {})
    return j.get("tickers", []) or []


def most_recent_completed_days(n=2, asof=None):
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
                grouped[ds] = g; tdays.append(ds)
        probe -= dt.timedelta(days=1); guard += 1
    tdays.sort()
    return grouped, tdays
