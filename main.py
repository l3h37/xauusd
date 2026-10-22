#!/usr/bin/env python3
"""
Gold Desk Bot - assistant Telegram temps réel pour trader l'XAUUSD.

Fonctions : prix live, analyse multi-timeframes (EMA/RSI/ATR), niveaux (pivots),
macro (DXY, US10Y, EURUSD...), news, calendrier économique USD, alertes de prix,
alerte de mouvement rapide, rappel avant/après publication des gros chiffres US,
calculateur de lot.

Dépendances : requests (+ tzdata). Aucune clé d'API payante.
"""
import html
import json
import logging
import os
import threading
import time
import xml.etree.ElementTree as ET
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zoneinfo import ZoneInfo

import requests

# --------------------------------------------------------------------------- #
# Configuration (variables d'environnement)
# --------------------------------------------------------------------------- #
TOKEN = os.environ["TELEGRAM_TOKEN"]
ALLOWED = set()
for _x in os.environ.get("CHAT_ID", "").replace(" ", "").split(","):
    if _x.lstrip("-").isdigit():
        ALLOWED.add(int(_x))

TZ_NAME = os.environ.get("TZ_NAME", "Europe/Paris")
TZ = ZoneInfo(TZ_NAME)
PRICE_POLL = max(10, int(os.environ.get("PRICE_POLL_SECONDS", "20")))
MOVE_PCT_DEFAULT = float(os.environ.get("MOVE_ALERT_PCT", "0.30"))  # % sur la fenêtre
MOVE_WINDOW_MIN = int(os.environ.get("MOVE_WINDOW_MIN", "15"))
MOVE_COOLDOWN_MIN = int(os.environ.get("MOVE_COOLDOWN_MIN", "15"))
EVENT_LEAD_MIN = int(os.environ.get("EVENT_LEAD_MIN", "15"))
PUSH_IMPACTS = set(os.environ.get("PUSH_IMPACTS", "high").lower().split(","))
CAL_COUNTRIES = set(os.environ.get("CAL_COUNTRIES", "US").upper().split(","))
STATE_FILE = os.environ.get("STATE_FILE", "state.json")
PORT = int(os.environ.get("PORT", "10000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("golddesk")

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) GoldDeskBot/1.0",
    "Accept": "application/json,text/xml,*/*",
})
API = f"https://api.telegram.org/bot{TOKEN}"


def http_json(url, params=None, timeout=10):
    r = S.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


# --------------------------------------------------------------------------- #
# État persistant (alertes, seuil de mouvement). Fichier local : perdu si
# l'hébergeur redémarre le conteneur (voir README).
# --------------------------------------------------------------------------- #
STATE_LOCK = threading.Lock()
STATE = {"alerts": [], "next_id": 1, "move_pct": MOVE_PCT_DEFAULT, "notified": {}}


def load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            STATE.update(json.load(f))
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("state illisible: %s", e)


def save_state():
    try:
        with STATE_LOCK, open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f)
    except Exception as e:
        log.warning("state non sauvegardé: %s", e)


# --------------------------------------------------------------------------- #
# Telegram
# --------------------------------------------------------------------------- #
def tg(method, **params):
    r = S.post(f"{API}/{method}", json=params, timeout=65)
    return r.json()


def split_text(t, n=3900):
    out = []
    while len(t) > n:
        cut = t.rfind("\n", 0, n)
        cut = cut if cut > 0 else n
        out.append(t[:cut])
        t = t[cut:].lstrip("\n")
    out.append(t)
    return out


def send(chat, text, preview=False):
    for chunk in split_text(text):
        try:
            tg("sendMessage", chat_id=chat, text=chunk, parse_mode="HTML",
               disable_web_page_preview=not preview)
        except Exception as e:
            log.warning("sendMessage: %s", e)


def broadcast(text):
    for c in ALLOWED:
        send(c, text)


# --------------------------------------------------------------------------- #
# Données de marché
# --------------------------------------------------------------------------- #
YF = "https://query1.finance.yahoo.com/v8/finance/chart/{sym}"


def yahoo_chart(sym, interval, rng):
    d = http_json(YF.format(sym=sym), {"interval": interval, "range": rng})
    res = d["chart"]["result"][0]
    meta = res["meta"]
    ts = res.get("timestamp") or []
    q = res["indicators"]["quote"][0]
    candles = []
    for i, t in enumerate(ts):
        o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
        if None in (o, h, l, c):
            continue
        candles.append({"t": t, "o": o, "h": h, "l": l, "c": c})
    return meta, candles


def get_spot():
    """Prix live XAUUSD avec 3 sources en cascade."""
    try:
        meta, _ = yahoo_chart("XAUUSD=X", "1d", "1d")
        p = meta.get("regularMarketPrice")
        if p:
            return {"price": float(p), "time": meta.get("regularMarketTime") or time.time(),
                    "src": "Yahoo Finance XAUUSD"}
    except Exception as e:
        log.warning("spot yahoo: %s", e)
    try:
        d = http_json("https://api.gold-api.com/price/XAU")
        return {"price": float(d["price"]), "time": time.time(), "src": "gold-api.com"}
    except Exception as e:
        log.warning("spot gold-api: %s", e)
    d = http_json("https://api.binance.com/api/v3/ticker/price", {"symbol": "PAXGUSDT"})
    return {"price": float(d["price"]), "time": time.time(),
            "src": "Binance PAXG (proxy, écart possible avec le spot)"}


def f2(x, nd=2):
    return f"{x:,.{nd}f}".replace(",", " ")


def sgn(x, nd=2):
    return f"{x:+,.{nd}f}".replace(",", " ")


def fmt_t(epoch_or_dt):
    if isinstance(epoch_or_dt, datetime):
        dt = epoch_or_dt
    else:
        dt = datetime.fromtimestamp(epoch_or_dt, timezone.utc)
    return dt.astimezone(TZ).strftime("%H:%M:%S")


SESS = [("Tokyo", "Asia/Tokyo", 9, 18), ("Londres", "Europe/London", 8, 16.5),
        ("New York", "America/New_York", 8, 17)]


def market_closed_now():
    n = datetime.now(timezone.utc)
    wd, h = n.weekday(), n.hour
    return wd == 5 or (wd == 4 and h >= 22) or (wd == 6 and h < 22)


def open_sessions():
    if market_closed_now():
        return []
    now = datetime.now(timezone.utc)
    names = []
    for n, tz, a, b in SESS:
        t = now.astimezone(ZoneInfo(tz))
        if t.weekday() >= 5:
            continue
        h = t.hour + t.minute / 60
        if a <= h < b:
            names.append(n)
    return names


# ---- Indicateurs ----------------------------------------------------------- #
def ema_last(vals, n):
    if len(vals) < n:
        return None
    k = 2 / (n + 1)
    e = sum(vals[:n]) / n
    for v in vals[n:]:
        e = v * k + e * (1 - k)
    return e


def rsi_last(vals, n=14):
    if len(vals) <= n:
        return None
    g = l = 0.0
    for i in range(1, n + 1):
        d = vals[i] - vals[i - 1]
        g += max(d, 0)
        l += max(-d, 0)
    ag, al = g / n, l / n
    for i in range(n + 1, len(vals)):
        d = vals[i] - vals[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def atr_last(c, n=14):
    if len(c) <= n:
        return None
    trs = [max(c[i]["h"] - c[i]["l"], abs(c[i]["h"] - c[i - 1]["c"]),
               abs(c[i]["l"] - c[i - 1]["c"])) for i in range(1, len(c))]
    a = sum(trs[:n]) / n
    for t in trs[n:]:
        a = (a * (n - 1) + t) / n
    return a


def to_h4(c1h):
    out, key = [], None
    for x in c1h:
        dt = datetime.fromtimestamp(x["t"], timezone.utc)
        k = (dt.date(), dt.hour // 4)
        if k != key:
            out.append(dict(x))
            key = k
        else:
            b = out[-1]
            b["h"] = max(b["h"], x["h"])
            b["l"] = min(b["l"], x["l"])
            b["c"] = x["c"]
    return out


def tf_summary(name, candles):
    if len(candles) < 30:
        return f"<b>{name}</b> : données insuffisantes", 0
    closes = [x["c"] for x in candles]
    p = closes[-1]
    e20, e50, e200 = ema_last(closes, 20), ema_last(closes, 50), ema_last(closes, 200)
    rsi, atr = rsi_last(closes), atr_last(candles)
    score = 0
    if e20 and p > e20:
        score += 1
    if e20 and e50 and e20 > e50:
        score += 1
    if e50 and e200 and e50 > e200:
        score += 1
    labels = {3: "🟢 haussier", 2: "🟢 plutôt haussier", 1: "🔴 plutôt baissier", 0: "🔴 baissier"}
    zone = ""
    if rsi is not None:
        zone = " (suracheté)" if rsi >= 70 else " (survendu)" if rsi <= 30 else ""
    parts = [f"<b>{name}</b> {labels[score]}"]
    ema_txt = " · ".join(f"EMA{n} {f2(v)}" for n, v in ((20, e20), (50, e50), (200, e200)) if v)
    if ema_txt:
        parts.append(ema_txt)
    tail = []
    if rsi is not None:
        tail.append(f"RSI {rsi:.0f}{zone}")
    if atr is not None:
        tail.append(f"ATR {f2(atr)}$")
    if tail:
        parts.append(" · ".join(tail))
    return "\n".join(parts), score - 1.5  # centré sur 0


def levels_block(price, daily):
    if len(daily) < 3:
        return ""
    today = datetime.now(timezone.utc).date()
    last_date = datetime.fromtimestamp(daily[-1]["t"], timezone.utc).date()
    prev = daily[-2] if last_date == today else daily[-1]
    H, L, C = prev["h"], prev["l"], prev["c"]
    P = (H + L + C) / 3
    lv = {"R3": H + 2 * (P - L), "R2": P + (H - L), "R1": 2 * P - L, "PP": P,
          "S1": 2 * P - H, "S2": P - (H - L), "S3": L - 2 * (H - P),
          "PDH": H, "PDL": L}
    wk = daily[-5:]
    lv["Haut 5j"] = max(x["h"] for x in wk)
    lv["Bas 5j"] = min(x["l"] for x in wk)
    r50 = round(price / 50) * 50
    lv[f"Rond {r50}"] = r50
    lv[f"Rond {r50 + 50}"] = r50 + 50
    lv[f"Rond {r50 - 50}"] = r50 - 50
    rows = sorted(lv.items(), key=lambda kv: kv[1], reverse=True)
    near = [kv for kv in rows if abs(kv[1] - price) <= 60]
    near = near or rows
    lines, placed = [], False
    for k, v in near:
        if not placed and v < price:
            lines.append(f"▶ <b>{f2(price)}</b>  prix")
            placed = True
        lines.append(f"   {f2(v)}  {k}")
    if not placed:
        lines.append(f"▶ <b>{f2(price)}</b>  prix")
    return "<b>Niveaux clés</b> (pivots classiques J-1, extrêmes, ronds)\n<code>" + "\n".join(lines) + "</code>"


# --------------------------------------------------------------------------- #
# Commandes : /prix
# --------------------------------------------------------------------------- #
def cmd_prix(chat, args):
    spot = get_spot()
    p = spot["price"]
    lines = [f"🥇 <b>XAUUSD</b>  <code>{f2(p)}</code>"]
    try:
        with ThreadPoolExecutor(2) as ex:
            f1 = ex.submit(yahoo_chart, "XAUUSD=X", "15m", "5d")
            f2_ = ex.submit(yahoo_chart, "XAUUSD=X", "1d", "1mo")
            _, c15 = f1.result()
            _, cd = f2_.result()
        if len(cd) >= 2:
            prev = cd[-2]["c"]
            ch = p - prev
            arrow = "🟢▲" if ch >= 0 else "🔴▼"
            lines.append(f"{arrow} {sgn(ch)} ({sgn(ch / prev * 100)}%) vs clôture précédente")
            lines.append(f"Jour : haut {f2(max(cd[-1]['h'], p))} · bas {f2(min(cd[-1]['l'], p))}")
        cutoff = time.time() - 86400
        l24 = [x for x in c15 if x["t"] >= cutoff]
        if len(l24) < 8:
            l24 = c15[-96:]
        if l24:
            hi, lo = max(x["h"] for x in l24), min(x["l"] for x in l24)
            lines.append(f"24h : haut {f2(hi)} · bas {f2(lo)} · amplitude {f2(hi - lo)}$")
    except Exception as e:
        log.warning("snapshot partiel: %s", e)
        lines.append("<i>(stats jour/24h indisponibles, mode dégradé)</i>")
    age = time.time() - spot["time"]
    lines.append(f"🕒 {fmt_t(spot['time'])} ({TZ_NAME}) · {html.escape(spot['src'])}")
    if market_closed_now():
        lines.append("🔒 Marché fermé (week-end) : prix = dernière cotation.")
    elif age > 600:
        lines.append(f"⚠️ Cotation âgée de {int(age // 60)} min : flux possiblement figé.")
    ss = open_sessions()
    if ss:
        lines.append("Sessions ouvertes : " + ", ".join(ss))
    send(chat, "\n".join(lines))


# --------------------------------------------------------------------------- #
# /analyse
# --------------------------------------------------------------------------- #
def cmd_analyse(chat, args):
    with ThreadPoolExecutor(3) as ex:
        f15 = ex.submit(yahoo_chart, "XAUUSD=X", "15m", "5d")
        f60 = ex.submit(yahoo_chart, "XAUUSD=X", "60m", "6mo")
        fd = ex.submit(yahoo_chart, "XAUUSD=X", "1d", "1y")
        _, c15 = f15.result()
        _, c60 = f60.result()
        _, cd = fd.result()
    price = get_spot()["price"]
    blocks, total = [], 0
    for name, c in (("M15", c15), ("H1", c60), ("H4 (≈)", to_h4(c60)), ("D1", cd)):
        txt, sc = tf_summary(name, c)
        blocks.append(txt)
        total += sc
    bias = ("🟢 biais haussier" if total >= 2 else "🔴 biais baissier" if total <= -2
            else "⚪ biais neutre / mixte")
    out = [f"📈 <b>Analyse XAUUSD</b> · {f2(price)}", "", *blocks, "",
           f"<b>Lecture globale :</b> {bias}", "", levels_block(price, cd), "",
           "<i>Outil d'aide à la décision, pas un conseil en investissement.</i>"]
    send(chat, "\n".join(out))


# --------------------------------------------------------------------------- #
# /macro
# --------------------------------------------------------------------------- #
MACRO = [("DXY (dollar index)", "DX-Y.NYB", "pct"), ("Rendement US 10 ans", "^TNX", "bps"),
         ("EURUSD", "EURUSD=X", "pct"), ("Argent (XAG)", "SI=F", "pct"),
         ("S&P 500", "^GSPC", "pct"), ("Pétrole WTI", "CL=F", "pct")]


def macro_one(sym):
    meta, c = yahoo_chart(sym, "1d", "5d")
    price = meta.get("regularMarketPrice") or c[-1]["c"]
    prev = c[-2]["c"] if len(c) >= 2 else meta.get("chartPreviousClose")
    return price, prev


def cmd_macro(chat, args):
    with ThreadPoolExecutor(len(MACRO)) as ex:
        futs = [(n, s, k, ex.submit(macro_one, s)) for n, s, k in MACRO]
    lines, chg = ["🌍 <b>Macro qui bouge l'or</b>", ""], {}
    for n, s, k, f in futs:
        try:
            price, prev = f.result()
            if k == "bps":
                d = (price - prev) * 100
                lines.append(f"{n} : <code>{price:.3f}%</code> ({d:+.1f} pb)")
                chg[s] = d
            else:
                d = (price - prev) / prev * 100
                lines.append(f"{n} : <code>{f2(price, 3 if 'EUR' in n else 2)}</code> ({d:+.2f}%)")
                chg[s] = d
        except Exception as e:
            log.warning("macro %s: %s", s, e)
            lines.append(f"{n} : indisponible")
    dxy, y10 = chg.get("DX-Y.NYB"), chg.get("^TNX")
    if dxy is not None and y10 is not None:
        if dxy < 0 and y10 < 0:
            lines += ["", "➡️ Dollar et rendements en baisse : contexte généralement <b>favorable</b> à l'or."]
        elif dxy > 0 and y10 > 0:
            lines += ["", "➡️ Dollar et rendements en hausse : contexte généralement <b>pesant</b> pour l'or."]
        else:
            lines += ["", "➡️ Signaux mixtes entre dollar et rendements."]
    send(chat, "\n".join(lines))


# --------------------------------------------------------------------------- #
# /news (Google News RSS, titres + liens)
# --------------------------------------------------------------------------- #
def ago(dt):
    s = int((datetime.now(timezone.utc) - dt).total_seconds())
    if s < 3600:
        return f"{max(1, s // 60)} min"
    if s < 86400:
        return f"{s // 3600} h"
    return f"{s // 86400} j"


def cmd_news(chat, args):
    q = "gold price OR XAUUSD OR Fed OR \"gold futures\" when:1d"
    r = S.get("https://news.google.com/rss/search",
              params={"q": q, "hl": "en-US", "gl": "US", "ceid": "US:en"}, timeout=10)
    r.raise_for_status()
    items = ET.fromstring(r.content).findall(".//item")[:8]
    lines = ["📰 <b>News or / macro (dernières 24h)</b>", ""]
    for it in items:
        title = html.escape(it.findtext("title") or "")
        link = html.escape(it.findtext("link") or "", quote=True)
        try:
            when = ago(parsedate_to_datetime(it.findtext("pubDate")))
        except Exception:
            when = "?"
        lines.append(f"• <a href=\"{link}\">{title}</a> <i>({when})</i>")
    send(chat, "\n".join(lines) if items else "Aucune news trouvée.")


# --------------------------------------------------------------------------- #
# Calendrier économique (financecalendar.com, API publique gratuite)
# --------------------------------------------------------------------------- #
FC_WEEK = "https://www.financecalendar.com/wp-json/fc/v1/week"
_cache = {}
_cache_lock = threading.Lock()


def norm_event(e):
    try:
        t = datetime.fromisoformat(e["time_utc"]).astimezone(timezone.utc)
    except Exception:
        return None
    if (e.get("country") or "").upper() not in CAL_COUNTRIES:
        return None
    return {"id": str(e.get("id") or f"{e.get('name')}-{e['time_utc']}"),
            "name": e.get("name", "?"), "impact": (e.get("impact") or "").lower(), "t": t,
            "consensus": e.get("consensus"), "prior": e.get("prior"),
            "actual": e.get("actual"), "status": e.get("status")}


def fetch_week(which):
    evs = {}
    for impact in ("high", "medium"):
        params = {"impact": impact, "tz": TZ_NAME}
        if which == "next":
            params["which"] = "next"
        for e in http_json(FC_WEEK, params).get("events", []):
            n = norm_event(e)
            if n:
                evs[n["id"]] = n
    return list(evs.values())


def cal_ttl():
    c = _cache.get("this")
    if c:
        now = datetime.now(timezone.utc)
        for e in c[1]:
            dt = (e["t"] - now).total_seconds()
            if -1200 < dt < 180 and not e["actual"]:
                return 90
    return 300


def get_events(force=False):
    now = time.time()
    out = []
    for which, ttl in (("this", cal_ttl()), ("next", 3600)):
        with _cache_lock:
            c = _cache.get(which)
        if c and not force and now - c[0] < ttl:
            out += c[1]
            continue
        try:
            evs = fetch_week(which)
            with _cache_lock:
                _cache[which] = (now, evs)
            out += evs
        except Exception as e:
            log.warning("calendrier %s: %s", which, e)
            if c:
                out += c[1]
    return sorted(out, key=lambda e: e["t"])


DAYS_FR = ["lun", "mar", "mer", "jeu", "ven", "sam", "dim"]


def cmd_cal(chat, args):
    evs = get_events()
    now = datetime.now(timezone.utc)
    horizon = now + timedelta(days=7)
    evs = [e for e in evs if now - timedelta(hours=2) <= e["t"] <= horizon]
    if not evs:
        send(chat, "Aucun événement USD important trouvé sur les 7 prochains jours.")
        return
    lines, day = ["🗓 <b>Calendrier USD (impact fort 🔴 / moyen 🟠)</b>", ""], None
    for e in evs:
        lt = e["t"].astimezone(TZ)
        d = f"{DAYS_FR[lt.weekday()]} {lt.strftime('%d/%m')}"
        if d != day:
            lines.append(f"\n<b>{d}</b>")
            day = d
        ic = "🔴" if e["impact"] == "high" else "🟠"
        extra = []
        if e["actual"]:
            extra.append(f"réel <b>{html.escape(str(e['actual']))}</b>")
        if e["consensus"]:
            extra.append(f"prév {html.escape(str(e['consensus']))}")
        if e["prior"]:
            extra.append(f"préc {html.escape(str(e['prior']))}")
        lines.append(f"{ic} {lt.strftime('%H:%M')} · {html.escape(e['name'])}"
                     + (" · " + " · ".join(extra) if extra else ""))
    lines.append(f"\n<i>Heures : {TZ_NAME}. Données : "
                 "<a href=\"https://www.financecalendar.com\">financecalendar.com</a></i>")
    send(chat, "\n".join(lines))


# --------------------------------------------------------------------------- #
# Alertes de prix
# --------------------------------------------------------------------------- #
def cmd_alerte(chat, args):
    if not args:
        send(chat, "Usage : <code>/alerte 4200</code> (niveau) ou <code>/alerte +15</code> / "
                   "<code>/alerte -20</code> (relatif au prix actuel).")
        return
    try:
        raw = args[0].replace(",", ".")
        cur = get_spot()["price"]
        level = cur + float(raw) if raw[0] in "+-" else float(raw)
    except Exception:
        send(chat, "Valeur invalide. Exemple : <code>/alerte 4200</code>")
        return
    direction = "above" if level > cur else "below"
    with STATE_LOCK:
        aid = STATE["next_id"]
        STATE["next_id"] += 1
        STATE["alerts"].append({"id": aid, "level": level, "dir": direction, "chat": chat})
    save_state()
    word = "atteint ou dépasse" if direction == "above" else "descend à ou sous"
    send(chat, f"✅ Alerte #{aid} : je te préviens quand l'or {word} <b>{f2(level)}</b> "
               f"(prix actuel {f2(cur)}).")


def cmd_alertes(chat, args):
    mine = [a for a in STATE["alerts"] if a["chat"] == chat]
    if not mine:
        send(chat, "Aucune alerte active. Ex : <code>/alerte 4200</code>")
        return
    lines = ["🔔 <b>Alertes actives</b>"]
    for a in mine:
        lines.append(f"#{a['id']} · {'≥' if a['dir'] == 'above' else '≤'} {f2(a['level'])}")
    lines.append("\nSupprimer : <code>/suppr 3</code> ou <code>/suppr tout</code>")
    send(chat, "\n".join(lines))


def cmd_suppr(chat, args):
    if not args:
        send(chat, "Usage : <code>/suppr 3</code> ou <code>/suppr tout</code>")
        return
    with STATE_LOCK:
        before = len(STATE["alerts"])
        if args[0].lower() in ("tout", "all"):
            STATE["alerts"] = [a for a in STATE["alerts"] if a["chat"] != chat]
        elif args[0].isdigit():
            STATE["alerts"] = [a for a in STATE["alerts"]
                               if not (a["id"] == int(args[0]) and a["chat"] == chat)]
        removed = before - len(STATE["alerts"])
    save_state()
    send(chat, f"🗑 {removed} alerte(s) supprimée(s).")


def cmd_move(chat, args):
    if not args:
        cur = STATE["move_pct"]
        send(chat, f"⚡ Alerte mouvement rapide : "
                   f"{'désactivée' if cur <= 0 else f'{cur}% en {MOVE_WINDOW_MIN} min'}.\n"
                   "Changer : <code>/move 0.4</code> ou <code>/move off</code>")
        return
    if args[0].lower() == "off":
        STATE["move_pct"] = 0.0
    else:
        try:
            STATE["move_pct"] = max(0.05, float(args[0].replace(",", ".")))
        except ValueError:
            send(chat, "Valeur invalide. Ex : <code>/move 0.4</code>")
            return
    save_state()
    cmd_move(chat, [])


# --------------------------------------------------------------------------- #
# Calculateur de lot : /lot <capital> <risque%> <stop en $>
# --------------------------------------------------------------------------- #
def cmd_lot(chat, args):
    try:
        bal, risk, sl = (float(a.replace(",", ".")) for a in args[:3])
        assert len(args) >= 3 and bal > 0 and risk > 0 and sl > 0
    except Exception:
        send(chat, "Usage : <code>/lot capital risque% stop$</code>\n"
                   "Ex : <code>/lot 1000 1 5</code> = 1000$ de capital, 1% de risque, stop à 5$ "
                   "(= 500 points de 0,01).")
        return
    risk_usd = bal * risk / 100
    lots = risk_usd / (sl * 100)  # 1 lot = 100 oz => 1$ de mouvement = 100$
    send(chat, f"🧮 Risque : <b>{f2(risk_usd)}$</b> · Stop : {f2(sl)}$\n"
               f"Taille : <b>{lots:.2f} lot</b> (≈ {lots * 100:.1f} oz)\n"
               "<i>Basé sur 1 lot = 100 oz ; vérifie la taille de contrat de ton courtier.</i>")


# --------------------------------------------------------------------------- #
# /start /aide /statut
# --------------------------------------------------------------------------- #
HELP = (
    "🥇 <b>Gold Desk Bot</b> : ton assistant XAUUSD\n\n"
    "/prix : prix live, variation, haut/bas, sessions\n"
    "/analyse : tendance M15/H1/H4/D1, RSI, ATR, niveaux clés\n"
    "/macro : DXY, rendements US, EURUSD, argent, S&amp;P, pétrole\n"
    "/news : titres des dernières 24h\n"
    "/cal : calendrier éco USD (NFP, CPI, FOMC...)\n"
    "/alerte 4200 ou /alerte +15 : alerte de prix\n"
    "/alertes · /suppr 3 · /suppr tout\n"
    "/move 0.4 · /move off : alerte mouvement rapide\n"
    "/lot 1000 1 5 : taille de position\n"
    "/statut : état du bot\n\n"
    "Push automatiques : alertes de prix, mouvements rapides, rappel "
    f"{EVENT_LEAD_MIN} min avant les gros chiffres USD et résultat dès publication."
)


def cmd_start(chat, args):
    send(chat, HELP)


def cmd_statut(chat, args):
    try:
        sp = get_spot()
        px = f"{f2(sp['price'])} ({html.escape(sp['src'])})"
    except Exception as e:
        px = f"erreur ({e})"
    mv = STATE["move_pct"]
    send(chat, f"✅ Bot en ligne\nPrix : {px}\n"
               f"Alertes actives : {len(STATE['alerts'])}\n"
               f"Mouvement rapide : {'off' if mv <= 0 else f'{mv}% / {MOVE_WINDOW_MIN} min'}\n"
               f"Fuseau : {TZ_NAME} · Poll prix : {PRICE_POLL}s")


COMMANDS = {
    "start": cmd_start, "aide": cmd_start, "help": cmd_start, "prix": cmd_prix,
    "analyse": cmd_analyse, "macro": cmd_macro, "news": cmd_news, "cal": cmd_cal,
    "calendrier": cmd_cal, "alerte": cmd_alerte, "alertes": cmd_alertes, "suppr": cmd_suppr,
    "move": cmd_move, "lot": cmd_lot, "statut": cmd_statut,
}

BOT_COMMANDS = [
    ("prix", "Prix live XAUUSD"), ("analyse", "Analyse multi-timeframes + niveaux"),
    ("macro", "DXY, rendements, EURUSD..."), ("news", "News or / macro"),
    ("cal", "Calendrier éco USD"), ("alerte", "Créer une alerte de prix"),
    ("alertes", "Lister mes alertes"), ("suppr", "Supprimer une alerte"),
    ("move", "Alerte mouvement rapide"), ("lot", "Calculer la taille de lot"),
    ("statut", "État du bot"), ("aide", "Aide"),
]


def handle(update):
    m = update.get("message") or {}
    text = (m.get("text") or "").strip()
    chat = (m.get("chat") or {}).get("id")
    if not text or chat is None or not text.startswith("/"):
        return
    cmd, *args = text.split()
    cmd = cmd[1:].split("@")[0].lower()
    if chat not in ALLOWED:
        send(chat, f"🔒 Bot privé. Ton chat id est <code>{chat}</code> : ajoute-le dans la "
                   "variable d'environnement <b>CHAT_ID</b> puis redémarre le service.")
        return
    fn = COMMANDS.get(cmd)
    if not fn:
        send(chat, "Commande inconnue. Tape /aide")
        return
    try:
        fn(chat, args)
    except Exception as e:
        log.exception("commande %s", cmd)
        send(chat, f"⚠️ Erreur sur /{cmd} : {html.escape(str(e))[:200]}\nRéessaie dans un instant.")


# --------------------------------------------------------------------------- #
# Surveillance en arrière-plan : alertes, mouvements rapides, calendrier
# --------------------------------------------------------------------------- #
def check_alerts(p):
    fired = []
    with STATE_LOCK:
        keep = []
        for a in STATE["alerts"]:
            hit = (a["dir"] == "above" and p >= a["level"]) or (a["dir"] == "below" and p <= a["level"])
            (fired if hit else keep).append(a)
        STATE["alerts"] = keep
    if fired:
        save_state()
    for a in fired:
        send(a["chat"], f"🔔 <b>Alerte #{a['id']}</b> : XAUUSD a touché <b>{f2(a['level'])}</b> "
                        f"(prix {f2(p)}).")


def check_calendar():
    now = datetime.now(timezone.utc)
    for e in get_events():
        if e["impact"] not in PUSH_IMPACTS:
            continue
        dt = (e["t"] - now).total_seconds()
        kpre, kact = f"{e['id']}:pre", f"{e['id']}:act"
        if 0 < dt <= EVENT_LEAD_MIN * 60 and kpre not in STATE["notified"]:
            STATE["notified"][kpre] = time.time()
            bits = []
            if e["consensus"]:
                bits.append(f"prév {html.escape(str(e['consensus']))}")
            if e["prior"]:
                bits.append(f"préc {html.escape(str(e['prior']))}")
            broadcast(f"⏰ <b>Dans {max(1, int(dt // 60))} min</b> : {html.escape(e['name'])} (USD, impact fort)\n"
                      + " · ".join(bits) + "\n<i>Volatilité attendue sur l'or : attention aux spreads et aux stops.</i>")
            save_state()
        if e["actual"] and -10800 < dt <= 0 and kact not in STATE["notified"]:
            STATE["notified"][kact] = time.time()
            broadcast(f"📊 <b>Publié</b> : {html.escape(e['name'])}\n"
                      f"Réel <b>{html.escape(str(e['actual']))}</b> · prév {html.escape(str(e['consensus'] or '?'))}"
                      f" · préc {html.escape(str(e['prior'] or '?'))}\n"
                      "<i>Surveille la réaction du DXY, des rendements et de l'or.</i>")
            save_state()
    cutoff = time.time() - 3 * 86400
    STATE["notified"] = {k: v for k, v in STATE["notified"].items() if v > cutoff}


def monitor():
    window, last_alert = deque(), 0.0
    while True:
        try:
            p = get_spot()["price"]
            now = time.time()
            window.append((now, p))
            while window and now - window[0][0] > MOVE_WINDOW_MIN * 60:
                window.popleft()
            check_alerts(p)
            thr = STATE["move_pct"]
            if thr > 0 and len(window) > 3 and now - last_alert > MOVE_COOLDOWN_MIN * 60:
                ref = window[0][1]
                chg = (p - ref) / ref * 100
                if abs(chg) >= thr:
                    last_alert = now
                    broadcast(f"⚡ <b>Mouvement rapide</b> {'🟢▲' if chg > 0 else '🔴▼'} "
                              f"{sgn(p - ref)}$ ({sgn(chg)}%) en {int((now - window[0][0]) // 60)} min\n"
                              f"XAUUSD <code>{f2(p)}</code>")
        except Exception as e:
            log.warning("monitor prix: %s", e)
        try:
            check_calendar()
        except Exception as e:
            log.warning("monitor calendrier: %s", e)
        time.sleep(PRICE_POLL)


# --------------------------------------------------------------------------- #
# Serveur de santé (pour le ping anti-veille) + boucle Telegram
# --------------------------------------------------------------------------- #
class Health(BaseHTTPRequestHandler):
    def _ok(self, body=True):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        if body:
            self.wfile.write(b"ok")

    def do_GET(self):
        self._ok()

    def do_HEAD(self):
        self._ok(False)

    def log_message(self, *a):
        pass


def run_health():
    ThreadingHTTPServer(("0.0.0.0", PORT), Health).serve_forever()


def poll():
    offset = None
    while True:
        try:
            params = {"timeout": 50, "allowed_updates": json.dumps(["message"])}
            if offset:
                params["offset"] = offset
            r = S.get(f"{API}/getUpdates", params=params, timeout=65)
            if r.status_code == 409:  # autre instance en cours de remplacement
                time.sleep(5)
                continue
            for u in r.json().get("result", []):
                offset = u["update_id"] + 1
                threading.Thread(target=handle, args=(u,), daemon=True).start()
        except Exception as e:
            log.warning("poll: %s", e)
            time.sleep(5)


def main():
    load_state()
    threading.Thread(target=run_health, daemon=True).start()
    try:
        tg("deleteWebhook", drop_pending_updates=False)
        tg("setMyCommands", commands=[{"command": c, "description": d} for c, d in BOT_COMMANDS])
    except Exception as e:
        log.warning("init telegram: %s", e)
    threading.Thread(target=monitor, daemon=True).start()
    if ALLOWED:
        broadcast("🟢 Gold Desk Bot démarré. Tape /prix ou /aide.")
    else:
        log.warning("CHAT_ID vide : envoie /start au bot pour obtenir ton chat id.")
    poll()


if __name__ == "__main__":
    main()
