"""
Bot Conversor de Divisas — Optimizado para Render (Web Service)
Arquitectura: PTB v20+ en hilo asyncio propio + Flask en hilo principal.
"""

import os
import re
import json
import logging
import asyncio
import threading
import time as _time_mod
from collections import defaultdict
from datetime import datetime, time as dtime
from typing import Optional

import pytz
import requests
from bs4 import BeautifulSoup
from flask import Flask, request as flask_request, jsonify, Response
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    Application,
)
from telegram.constants import ParseMode

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── Variables de entorno ───────────────────────────────────────────────────────
TELEGRAM_TOKEN       = os.environ["TELEGRAM_TOKEN"]
EXCHANGERATE_API_KEY = os.environ["EXCHANGERATE_API_KEY"]
PORT                 = int(os.environ.get("PORT", 8080))
WEBHOOK_URL          = os.environ.get("WEBHOOK_URL", "").rstrip("/")

VEN_TZ       = pytz.timezone("America/Caracas")   # UTC-4
WEBHOOK_PATH = f"/webhook/{TELEGRAM_TOKEN}"

# ── Anti-flood ────────────────────────────────────────────────────────────────
FLOOD_WINDOW   = 5 * 60   # ventana 5 minutos
FLOOD_LIMIT    = 3        # máx. usos antes de bloquear
FLOOD_BAN_SECS = 5 * 60   # duración del bloqueo

_flood_log: dict = defaultdict(list)
_flood_ban: dict = {}

# ── Caché global ──────────────────────────────────────────────────────────────
cache: dict = {
    "rates"        : {},      # ExchangeRate-API base USD
    "bcv_rate"     : None,    # VES/USD  (BCV)
    "blue_buy"     : None,    # ARS blue compra
    "blue_sell"    : None,    # ARS blue venta
    "official_buy" : None,    # ARS oficial compra
    "official_sell": None,    # ARS oficial venta
    "cup_bcc"      : None,    # CUP/USD  BCC Segmento III (oficial)
    "cup_informal" : None,    # CUP/USD  elTOQUE (mercado negro)
    "date"         : None,    # "YYYY-MM-DD"
}

# ── Metadatos de monedas ───────────────────────────────────────────────────────
# cmd -> (ISO, bandera, nombre largo)
CURRENCY_META: dict[str, tuple] = {
    "ves"  : ("VES", "🇻🇪", "Bolívar Venezolano"),
    "ars"  : ("ARS", "🇦🇷", "Peso Argentino Oficial"),
    "arsb" : ("ARS", "🇦🇷", "Peso Argentino Blue"),
    "cop"  : ("COP", "🇨🇴", "Peso Colombiano"),
    "brl"  : ("BRL", "🇧🇷", "Real Brasileño"),
    "clp"  : ("CLP", "🇨🇱", "Peso Chileno"),
    "pen"  : ("PEN", "🇵🇪", "Sol Peruano"),
    "pyg"  : ("PYG", "🇵🇾", "Guaraní Paraguayo"),
    "bob"  : ("BOB", "🇧🇴", "Boliviano"),
    "uyu"  : ("UYU", "🇺🇾", "Peso Uruguayo"),
    "usd"  : ("USD", "🇺🇸", "Dólar Estadounidense"),
    "mxn"  : ("MXN", "🇲🇽", "Peso Mexicano"),
    "cup"  : ("CUP", "🇨🇺", "Peso Cubano Oficial (BCC Seg. III)"),
    "cupb" : ("CUP", "🇨🇺", "Peso Cubano Informal (elTOQUE)"),
    "eur"  : ("EUR", "🇪🇺", "Euro"),
    "gbp"  : ("GBP", "🇬🇧", "Libra Esterlina"),
    "cny"  : ("CNY", "🇨🇳", "Yuan Chino"),
    "jpy"  : ("JPY", "🇯🇵", "Yen Japonés"),
}

REGIONS: dict[str, list] = {
    "🌎 Sudamérica": [
        ("ves",  "🇻🇪", "VES — Bolívar Venezolano"),
        ("ars",  "🇦🇷", "ARS — Peso Argentino Oficial"),
        ("arsb", "🇦🇷", "ARS — Peso Argentino Blue"),
        ("cop",  "🇨🇴", "COP — Peso Colombiano"),
        ("brl",  "🇧🇷", "BRL — Real Brasileño"),
        ("clp",  "🇨🇱", "CLP — Peso Chileno"),
        ("pen",  "🇵🇪", "PEN — Sol Peruano"),
        ("pyg",  "🇵🇾", "PYG — Guaraní Paraguayo"),
        ("bob",  "🇧🇴", "BOB — Boliviano"),
        ("uyu",  "🇺🇾", "UYU — Peso Uruguayo"),
    ],
    "🌎 Norte/Centro América": [
        ("usd",  "🇺🇸", "USD — Dólar Estadounidense"),
        ("mxn",  "🇲🇽", "MXN — Peso Mexicano"),
        ("cup",  "🇨🇺", "CUP — Peso Cubano Oficial (BCC Seg. III)"),
        ("cupb", "🇨🇺", "CUP — Peso Cubano Informal (elTOQUE)"),
    ],
    "🌍 Europa / Asia": [
        ("eur",  "🇪🇺", "EUR — Euro"),
        ("gbp",  "🇬🇧", "GBP — Libra Esterlina"),
        ("cny",  "🇨🇳", "CNY — Yuan Chino"),
        ("jpy",  "🇯🇵", "JPY — Yen Japonés"),
    ],
}

# ══════════════════════════════════════════════════════════════════════════════
# CAPA DE DATOS
# ══════════════════════════════════════════════════════════════════════════════

_SCRAPE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}


def fetch_exchangerate() -> dict:
    url  = f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/USD"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("conversion_rates", {})


def fetch_bcv_rate() -> Optional[float]:
    try:
        from pydollarvenezuela import get_dollar
        result = get_dollar("bcv")
        if result and hasattr(result, "price"):
            return float(result.price)
    except Exception as exc:
        logger.warning(f"BCV error: {exc}")
    return None


def fetch_bluelytics() -> dict:
    try:
        resp = requests.get("https://api.bluelytics.com.ar/v2/latest", timeout=15)
        resp.raise_for_status()
        data    = resp.json()
        oficial = data.get("oficial", {})
        blue    = data.get("blue", {})
        return {
            "official_buy" : oficial.get("value_buy"),
            "official_sell": oficial.get("value_sell"),
            "blue_buy"     : blue.get("value_buy"),
            "blue_sell"    : blue.get("value_sell"),
        }
    except Exception as exc:
        logger.warning(f"Bluelytics error: {exc}")
    return {}


def fetch_cup_bcc() -> Optional[float]:
    """
    Tasa USD→CUP Segmento III del BCC.
    Intento 1: API JSON pública (api.bc.gob.cu).
    Intento 2: Scraping HTML de bc.gob.cu/tasas-de-cambio.
    """
    hdrs = {**_SCRAPE_HEADERS, "Referer": "https://www.bc.gob.cu/"}

    # Intento 1 — API JSON
    try:
        resp = requests.get(
            "https://api.bc.gob.cu/v1/tasas-de-cambio/activas",
            headers={**hdrs, "Accept": "application/json"},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            items = data if isinstance(data, list) else data.get("data", [])
            for item in items:
                codigo   = str(item.get("codigoMoneda", "")).upper()
                segmento = str(item.get("segmento", "")).upper()
                if codigo == "USD" and "III" in segmento:
                    val = item.get("valor") or item.get("tasa")
                    if val:
                        return float(val)
    except Exception as exc:
        logger.warning(f"BCC API JSON error: {exc}")

    # Intento 2 — Scraping HTML
    try:
        resp = requests.get("https://www.bc.gob.cu/tasas-de-cambio", headers=hdrs, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(" ", strip=True)
            seg3 = re.search(r"Segmento\s*III(.{0,1000})", text, re.S)
            if seg3:
                block = seg3.group(1)
                m = re.search(r"USD[^0-9]{0,30}(\d{3,4}(?:\.\d+)?)\s*CUP", block, re.I)
                if m:
                    return float(m.group(1))
    except Exception as exc:
        logger.warning(f"BCC scraping error: {exc}")

    return None


def fetch_cup_informal() -> Optional[float]:
    """
    Tasa informal USD→CUP desde elTOQUE.
    La página es Next.js con SSR: los datos van embebidos en __NEXT_DATA__ y en tablas HTML.
    Se intentan 3 métodos de extracción en orden.
    """
    urls = [
        "https://eltoque.com/tasas-de-cambio-cuba",
        "https://eltoque.com/tasas-de-cambio-cuba/mercado-informal",
    ]
    for url in urls:
        try:
            resp = requests.get(url, headers=_SCRAPE_HEADERS, timeout=20)
            if resp.status_code != 200:
                logger.warning(f"elTOQUE {url} → HTTP {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # ── Método A: JSON embebido __NEXT_DATA__ ─────────────────────────
            next_data_tag = soup.find("script", id="__NEXT_DATA__")
            if next_data_tag and next_data_tag.string:
                try:
                    jdata = json.loads(next_data_tag.string)
                    jtext = json.dumps(jdata)
                    # Buscar patrones como "USD":{"value":539.5} o similar
                    for pattern in [
                        r'"USD"\s*:\s*\{[^}]*?"value"\s*:\s*([\d.]+)',
                        r'"currency"\s*:\s*"USD"[^}]*?"rate"\s*:\s*([\d.]+)',
                        r'"USD"[^}]{0,60}"([\d]{3,4}(?:\.\d+)?)"',
                    ]:
                        m = re.search(pattern, jtext)
                        if m:
                            val = float(m.group(1))
                            if 200 < val < 3000:
                                logger.info(f"elTOQUE USD informal (NEXT_DATA): {val}")
                                return val
                except Exception:
                    pass

            # ── Método B: tablas HTML ─────────────────────────────────────────
            for row in soup.find_all("tr"):
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                for i, cell in enumerate(cells):
                    if re.search(r'\b1\s*USD\b', cell, re.I):
                        for j in range(i + 1, min(i + 4, len(cells))):
                            m = re.search(r'(\d{3,4}(?:[.,]\d+)?)', cells[j])
                            if m:
                                val = float(m.group(1).replace(",", "."))
                                if 200 < val < 3000:
                                    logger.info(f"elTOQUE USD informal (tabla): {val}")
                                    return val

            # ── Método C: texto plano ─────────────────────────────────────────
            text = soup.get_text(" ")
            for pattern in [
                r'1\s+USD\b[^0-9]{0,40}(\d{3,4}(?:[.,]\d+)?)\s*CUP',
                r'USD[^0-9]{0,20}(\d{3,4}(?:[.,]\d+)?)\s*CUP',
            ]:
                m = re.search(pattern, text, re.I)
                if m:
                    val = float(m.group(1).replace(",", "."))
                    if 200 < val < 3000:
                        logger.info(f"elTOQUE USD informal (texto): {val}")
                        return val

        except Exception as exc:
            logger.warning(f"elTOQUE scraping error ({url}): {exc}")

    logger.warning("elTOQUE: no se pudo obtener tasa informal USD/CUP")
    return None


def refresh_cache():
    logger.info("🔄 Actualizando caché de tasas…")
    today = datetime.now(VEN_TZ).strftime("%Y-%m-%d")

    rates       = fetch_exchangerate()
    bcv         = fetch_bcv_rate()
    blue        = fetch_bluelytics()
    cup_bcc     = fetch_cup_bcc()
    cup_informal = fetch_cup_informal()

    cache.update({
        "rates"        : rates,
        "bcv_rate"     : bcv,
        "official_buy" : blue.get("official_buy"),
        "official_sell": blue.get("official_sell"),
        "blue_buy"     : blue.get("blue_buy"),
        "blue_sell"    : blue.get("blue_sell"),
        "cup_bcc"      : cup_bcc,
        "cup_informal" : cup_informal,
        "date"         : today,
    })
    logger.info(
        f"✅ Caché listo — {today} — {len(rates)} monedas | "
        f"BCC Seg.III={cup_bcc} | elTOQUE={cup_informal}"
    )


def ensure_cache():
    today = datetime.now(VEN_TZ).strftime("%Y-%m-%d")
    if cache["date"] != today or not cache["rates"]:
        refresh_cache()


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_MONTHS = [
    "enero","febrero","marzo","abril","mayo","junio",
    "julio","agosto","septiembre","octubre","noviembre","diciembre",
]

def date_es() -> str:
    now = datetime.now(VEN_TZ)
    return f"{now.day} de {_MONTHS[now.month-1]} de {now.year}"


def fmt(v: float) -> str:
    return f"{v:,.2f}"


def get_usd_rate(cmd: str) -> Optional[float]:
    """Devuelve cuántas unidades de `cmd` equivalen a 1 USD."""
    rates = cache["rates"]
    if cmd == "ves":
        return cache["bcv_rate"] or rates.get("VES")
    if cmd == "ars":
        s = cache["official_sell"]
        return float(s) if s is not None else rates.get("ARS")
    if cmd == "arsb":
        s = cache["blue_sell"]
        return float(s) if s is not None else None
    if cmd == "cup":
        return cache["cup_bcc"] or rates.get("CUP")
    if cmd == "cupb":
        return cache["cup_informal"]
    iso = CURRENCY_META[cmd][0]
    return rates.get(iso)


_ESC = r"\_*[]()~`>#+-=|{}.!"

def esc(text: str) -> str:
    for ch in _ESC:
        text = text.replace(ch, f"\\{ch}")
    return text


def eur_line() -> str:
    """Devuelve la línea del Euro para añadir a cualquier mensaje de tasa."""
    eur_rate = cache["rates"].get("EUR")
    if eur_rate is None:
        return ""
    # EUR es cuántos EUR por 1 USD → para mostrar 1 USD en EUR = eur_rate
    # Pero lo que queremos es: 1 EUR = X de la moneda consultada.
    # Aquí simplemente mostramos el valor del Euro en USD para referencia cruzada:
    # 1 EUR = 1/eur_rate USD → eur_rate es EUR por USD, así que 1 USD = eur_rate EUR
    # Mostramos cuántos USD vale 1 EUR, que es 1/eur_rate
    usd_per_eur = 1.0 / eur_rate if eur_rate else None
    if usd_per_eur is None:
        return ""
    return f"🇪🇺 Euro: 🇺🇸 {esc(fmt(usd_per_eur))} USD"


# ── Anti-flood ─────────────────────────────────────────────────────────────────

def check_flood(user_id: int, cmd: str) -> tuple[bool, int]:
    key = (user_id, cmd)
    now = _time_mod.time()

    if key in _flood_ban:
        expiry = _flood_ban[key]
        if now < expiry:
            return True, int(expiry - now)
        del _flood_ban[key]

    _flood_log[key] = [t for t in _flood_log[key] if now - t < FLOOD_WINDOW]
    _flood_log[key].append(now)

    if len(_flood_log[key]) > FLOOD_LIMIT:
        _flood_ban[key] = now + FLOOD_BAN_SECS
        _flood_log[key] = []
        return True, FLOOD_BAN_SECS

    return False, 0


# ══════════════════════════════════════════════════════════════════════════════
# CONSTRUCTORES DE MENSAJES
# ══════════════════════════════════════════════════════════════════════════════

def build_rate_msg(cmd: str) -> str:
    ensure_cache()
    iso, flag, name = CURRENCY_META[cmd]

    # ── /ves — redirige a @bcvpricesbot ───────────────────────────────────────
    if cmd == "ves":
        el = eur_line()
        extra = f"\n{el}" if el else ""
        return (
            f"*TASAS DEL DÍA*\n"
            f"📅 {esc(date_es())}\n"
            f"↪️💰{flag} _Bolívar Venezolano_\n"
            f"\n"
            f"🇻🇪 Las tasas BCV actualizadas las encontrarás en:\n"
            f"👉 @bcvpricesbot\n"
            f"{extra}\n"
            f"\n"
            f"`[ℹ️]` _Para convertir 'VES' a otra moneda desde aquí, utilizar:_ "
            f"/convertir \\[valor\\] ves to \\[moneda destino\\]"
        )

    # ── /usd — caso especial ──────────────────────────────────────────────────
    if cmd == "usd":
        el = eur_line()
        return (
            f"*TASAS DEL DÍA*\n"
            f"📅 {esc(date_es())}\n"
            f"↪️💰{flag} _{esc(name)}_\n"
            f"\n"
            f"🇺🇸 1 USD \\= 1\\.00 USD\n"
            f"{el}\n"
            f"\n"
            f"`[ℹ️]` _Para convertir 'USD' a otra moneda, utilizar:_ "
            f"/convertir \\[valor\\] usd to \\[moneda destino\\]"
        )

    # ── /ars y /arsb — compra/venta ───────────────────────────────────────────
    if cmd in ("ars", "arsb"):
        if cmd == "arsb":
            buy, sell = cache["blue_buy"], cache["blue_sell"]
        else:
            buy, sell = cache["official_buy"], cache["official_sell"]

        b_str = esc(fmt(float(buy)))  if buy  is not None else "N/A"
        s_str = esc(fmt(float(sell))) if sell is not None else "N/A"
        el    = eur_line()

        return (
            f"*TASAS DEL DÍA*\n"
            f"📅 {esc(date_es())}\n"
            f"↪️💰{flag} _{esc(name)}_\n"
            f"\n"
            f"🇺🇸 Dólar Compra: \\$ {b_str}\n"
            f"🇺🇸 Dólar Venta:  \\$ {s_str}\n"
            f"{el}\n"
            f"\n"
            f"`[ℹ️]` _Para convertir '{esc(iso)}' a otra moneda, utilizar:_ "
            f"/convertir \\[valor\\] {cmd} to \\[moneda destino\\]"
        )

    # ── /cup — BCC Segmento III ───────────────────────────────────────────────
    if cmd == "cup":
        rate = cache["cup_bcc"] or cache["rates"].get("CUP")
        rate_str = esc(fmt(rate)) if rate else "N/A"
        el = eur_line()
        return (
            f"*TASAS DEL DÍA*\n"
            f"📅 {esc(date_es())}\n"
            f"↪️💰{flag} _{esc(name)}_\n"
            f"\n"
            f"🇺🇸 Dólar: 🇨🇺 {rate_str} CUP\n"
            f"{el}\n"
            f"\n"
            f"`[ℹ️]` _Fuente: BCC Segmento III \\(tasa oficial\\)_\n"
            f"_Para convertir 'CUP' a otra moneda, utilizar:_ "
            f"/convertir \\[valor\\] cup to \\[moneda destino\\]"
        )

    # ── /cupb — elTOQUE mercado informal ─────────────────────────────────────
    if cmd == "cupb":
        rate = cache["cup_informal"]
        rate_str = esc(fmt(rate)) if rate else "N/A"
        el = eur_line()
        return (
            f"*TASAS DEL DÍA*\n"
            f"📅 {esc(date_es())}\n"
            f"↪️💰{flag} _{esc(name)}_\n"
            f"\n"
            f"🇺🇸 Dólar: 🇨🇺 {rate_str} CUP\n"
            f"{el}\n"
            f"\n"
            f"`[ℹ️]` _Fuente: elTOQUE \\(mercado informal\\)_\n"
            f"_Para convertir 'CUP' a otra moneda, utilizar:_ "
            f"/convertir \\[valor\\] cupb to \\[moneda destino\\]"
        )

    # ── resto de monedas ──────────────────────────────────────────────────────
    rate = get_usd_rate(cmd)
    if rate is None:
        return f"⚠️ No hay datos para *{esc(iso)}* hoy\\."

    el = eur_line()

    # Para EUR, mostramos cuánto vale en USD en lugar de la línea EUR duplicada
    if cmd == "eur":
        usd_per_eur = 1.0 / rate if rate else None
        return (
            f"*TASAS DEL DÍA*\n"
            f"📅 {esc(date_es())}\n"
            f"↪️💰{flag} _{esc(name)}_\n"
            f"\n"
            f"🇺🇸 Dólar: {flag} {esc(fmt(rate))} EUR por USD\n"
            f"🇺🇸 1 EUR \\= {esc(fmt(usd_per_eur if usd_per_eur else 0))} USD\n"
            f"\n"
            f"`[ℹ️]` _Para convertir 'EUR' a otra moneda, utilizar:_ "
            f"/convertir \\[valor\\] eur to \\[moneda destino\\]"
        )

    return (
        f"*TASAS DEL DÍA*\n"
        f"📅 {esc(date_es())}\n"
        f"↪️💰{flag} _{esc(name)}_\n"
        f"\n"
        f"🇺🇸 Dólar: {flag} {esc(fmt(rate))}\n"
        f"{el}\n"
        f"\n"
        f"`[ℹ️]` _Para convertir '{esc(iso)}' a otra moneda, utilizar:_ "
        f"/convertir \\[valor\\] {cmd} to \\[moneda destino\\]"
    )


def build_conversion_msg(cmd_from: str, cmd_to: str, amount: float) -> str:
    ensure_cache()
    rate_from = get_usd_rate(cmd_from)
    rate_to   = get_usd_rate(cmd_to)

    if rate_from is None or rate_to is None:
        missing = cmd_from if rate_from is None else cmd_to
        return f"⚠️ Sin datos para `{esc(missing)}` hoy\\."

    result           = (amount / rate_from) * rate_to
    iso_f, flag_f, _ = CURRENCY_META[cmd_from]
    iso_t, flag_t, _ = CURRENCY_META[cmd_to]

    return (
        f"*🔁 CONVERSIÓN*\n"
        f"\n"
        f"{flag_f} *{esc(iso_f)}*: {esc(fmt(amount))}\n"
        f"{flag_t} *{esc(iso_t)}*: {esc(fmt(result))}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# HANDLERS
# ══════════════════════════════════════════════════════════════════════════════

def _flood_warn_msg(seconds: int) -> str:
    mins   = seconds // 60
    secs   = seconds % 60
    tiempo = f"{mins} min {secs} seg" if secs else f"{mins} min"
    return (
        "🚫 *Calma, calma\\!*\n\n"
        f"Usaste este comando demasiadas veces seguidas\\.\n"
        f"Por favor espera *{esc(tiempo)}* antes de volver a usarlo\\."
    )


async def _flood_check_and_reply(update: Update, cmd: str) -> bool:
    user_id = update.effective_user.id if update.effective_user else 0
    blocked, secs = check_flood(user_id, cmd)
    if blocked:
        await update.effective_message.reply_text(
            _flood_warn_msg(secs), parse_mode=ParseMode.MARKDOWN_V2
        )
        return True
    return False


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _flood_check_and_reply(update, "start"):
        return
    msg = (
        "*💱 Bot Conversor de Divisas*\n\n"
        "Consulta las tasas del día con el comando de cada moneda\\.\n\n"
        "📋 Ver todas las monedas disponibles: /paises\n"
        "🔁 Convertir: `/convertir 100 usd to ves`"
    )
    await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)


async def handle_paises(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _flood_check_and_reply(update, "paises"):
        return
    lines = ["*🌐 MONEDAS DISPONIBLES*\n"]
    for region, currencies in REGIONS.items():
        lines.append(f"*{esc(region)}*")
        for cmd_k, flag, label in currencies:
            parts     = label.split(" — ", 1)
            iso_part  = esc(parts[0])
            name_part = esc(parts[1]) if len(parts) > 1 else iso_part
            lines.append(f"{flag} {name_part} \\({iso_part}\\) → `/{cmd_k}`")
        lines.append("")
    lines.append(
        "💡 _Toca cualquier comando para ver la tasa del día_\n"
        "_O usa_ /convertir \\[monto\\] \\[origen\\] to \\[destino\\]"
    )
    await update.effective_message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2
    )


async def handle_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    raw = update.effective_message.text or ""
    cmd = raw.split("@")[0].lstrip("/").lower().strip()
    if cmd not in CURRENCY_META:
        return
    if await _flood_check_and_reply(update, cmd):
        return
    msg = build_rate_msg(cmd)
    await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)


async def handle_convertir(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _flood_check_and_reply(update, "convertir"):
        return

    args = context.args
    if len(args) != 4 or args[2].lower() != "to":
        await update.effective_message.reply_text(
            "❌ Uso: `/convertir [monto] [origen] to [destino]`\n"
            "Ejemplo: `/convertir 100 usd to ves`",
            parse_mode=ParseMode.MARKDOWN_V2,
        )
        return

    raw_amount, cmd_from, _, cmd_to = args
    cmd_from = cmd_from.lower()
    cmd_to   = cmd_to.lower()

    try:
        amount = float(raw_amount.replace(",", "."))
    except ValueError:
        await update.effective_message.reply_text(
            "❌ El monto debe ser un número válido\\.", parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    for cmd_check in (cmd_from, cmd_to):
        if cmd_check not in CURRENCY_META:
            await update.effective_message.reply_text(
                f"❌ Moneda desconocida: `{esc(cmd_check)}`\n"
                "Usa /paises para ver las disponibles\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

    msg = build_conversion_msg(cmd_from, cmd_to, amount)
    await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)


# ══════════════════════════════════════════════════════════════════════════════
# JOB — 00:00 hora Venezuela (04:00 UTC)
# ══════════════════════════════════════════════════════════════════════════════

async def daily_refresh_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("⏰ Job diario — actualizando caché…")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, refresh_cache)


# ══════════════════════════════════════════════════════════════════════════════
# FLASK — Health checks + Webhook receiver
# ══════════════════════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

_ptb_app:  Optional[Application]                 = None
_ptb_loop: Optional[asyncio.AbstractEventLoop]   = None


@flask_app.get("/")
def root():
    return jsonify({"status": "running", "cache_date": cache.get("date")}), 200


@flask_app.get("/health")
def health():
    return jsonify({"status": "ok", "cache_date": cache.get("date")}), 200


@flask_app.post(WEBHOOK_PATH)
def telegram_webhook():
    if _ptb_app is None or _ptb_loop is None:
        return Response("Bot no inicializado aún", status=503)
    data   = flask_request.get_json(force=True)
    update = Update.de_json(data, _ptb_app.bot)
    asyncio.run_coroutine_threadsafe(_ptb_app.process_update(update), _ptb_loop)
    return Response("ok", status=200)


# ══════════════════════════════════════════════════════════════════════════════
# HILO PTB — event loop separado del principal (Flask)
# ══════════════════════════════════════════════════════════════════════════════

def run_ptb():
    global _ptb_app, _ptb_loop

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _ptb_loop = loop

    async def _run():
        global _ptb_app

        app      = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        _ptb_app = app

        app.add_handler(CommandHandler("start",     handle_start))
        app.add_handler(CommandHandler("paises",    handle_paises))
        app.add_handler(CommandHandler("convertir", handle_convertir))
        for cmd_key in CURRENCY_META:
            app.add_handler(CommandHandler(cmd_key, handle_rate))

        # Job diario: 00:00 VEN = 04:00 UTC
        midnight_utc = dtime(hour=4, minute=0, tzinfo=pytz.utc)
        app.job_queue.run_daily(daily_refresh_job, time=midnight_utc, name="daily_refresh")

        await app.initialize()
        await app.start()

        if WEBHOOK_URL:
            full_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
            await app.bot.set_webhook(url=full_url, drop_pending_updates=True)
            logger.info(f"🔗 Webhook: {full_url}")
            while True:
                await asyncio.sleep(3600)
        else:
            logger.info("📡 Modo Polling (desarrollo local)")
            await app.updater.start_polling(drop_pending_updates=True)
            while True:
                await asyncio.sleep(3600)

    loop.run_until_complete(_run())


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    ensure_cache()   # carga inicial antes de servir tráfico

    t = threading.Thread(target=run_ptb, daemon=True, name="ptb-loop")
    t.start()

    logger.info(f"🌐 Flask en puerto {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False)


if __name__ == "__main__":
    main()
