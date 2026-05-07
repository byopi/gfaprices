"""
Bot Conversor de Divisas — Optimizado para Render (Web Service)
Arquitectura: PTB v20+ en hilo asyncio propio + Flask en hilo principal.
"""

import os
import logging
import asyncio
import threading
from collections import defaultdict
from datetime import datetime, time as dtime
from typing import Optional

import pytz
import requests
from flask import Flask, request as flask_request, jsonify, Response
from telegram import Update
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
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
# Estructura: { (user_id, cmd): [timestamp, ...] }
FLOOD_WINDOW   = 5 * 60   # ventana de 5 minutos (segundos)
FLOOD_LIMIT    = 3        # máx. usos permitidos en la ventana
FLOOD_BAN_SECS = 5 * 60   # tiempo de bloqueo tras exceder el límite

_flood_log: dict  = defaultdict(list)   # historial de timestamps por (user, cmd)
_flood_ban: dict  = {}                  # { (user_id, cmd): unix_timestamp_expiry }

# ── Caché global ──────────────────────────────────────────────────────────────
cache: dict = {
    "rates"        : {},
    "bcv_rate"     : None,
    "blue_buy"     : None,
    "blue_sell"    : None,
    "official_buy" : None,
    "official_sell": None,
    "date"         : None,
}

# ── Metadatos de monedas ───────────────────────────────────────────────────────
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
    "cup"  : ("CUP", "🇨🇺", "Peso Cubano"),
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
        ("cup",  "🇨🇺", "CUP — Peso Cubano"),
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


def refresh_cache():
    logger.info("🔄 Actualizando caché de tasas…")
    today = datetime.now(VEN_TZ).strftime("%Y-%m-%d")
    rates = fetch_exchangerate()
    bcv   = fetch_bcv_rate()
    blue  = fetch_bluelytics()

    cache.update({
        "rates"        : rates,
        "bcv_rate"     : bcv,
        "official_buy" : blue.get("official_buy"),
        "official_sell": blue.get("official_sell"),
        "blue_buy"     : blue.get("blue_buy"),
        "blue_sell"    : blue.get("blue_sell"),
        "date"         : today,
    })
    logger.info(f"✅ Caché listo — {today} — {len(rates)} monedas")


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
    rates = cache["rates"]
    if cmd == "ves":
        return cache["bcv_rate"] or rates.get("VES")
    if cmd == "ars":
        s = cache["official_sell"]
        return float(s) if s is not None else rates.get("ARS")
    if cmd == "arsb":
        s = cache["blue_sell"]
        return float(s) if s is not None else None
    iso = CURRENCY_META[cmd][0]
    return rates.get(iso)


_ESC = r"\_*[]()~`>#+-=|{}.!"

def esc(text: str) -> str:
    """Escapa caracteres especiales de MarkdownV2."""
    for ch in _ESC:
        text = text.replace(ch, f"\\{ch}")
    return text


# ── Anti-flood ─────────────────────────────────────────────────────────────────

import time as _time_mod

def check_flood(user_id: int, cmd: str) -> tuple[bool, int]:
    """
    Verifica si el usuario está haciendo flood del comando `cmd`.
    Retorna (bloqueado: bool, segundos_restantes: int).
    - Si está baneado: (True, segundos restantes del ban)
    - Si acaba de exceder: (True, FLOOD_BAN_SECS)  ← registra el ban
    - Si está OK: (False, 0)
    """
    key = (user_id, cmd)
    now = _time_mod.time()

    # ¿Sigue baneado?
    if key in _flood_ban:
        expiry = _flood_ban[key]
        if now < expiry:
            return True, int(expiry - now)
        else:
            del _flood_ban[key]   # ban expirado

    # Limpiar timestamps fuera de la ventana
    _flood_log[key] = [t for t in _flood_log[key] if now - t < FLOOD_WINDOW]

    # Registrar este uso
    _flood_log[key].append(now)

    # ¿Superó el límite?
    if len(_flood_log[key]) > FLOOD_LIMIT:
        _flood_ban[key] = now + FLOOD_BAN_SECS
        _flood_log[key] = []   # resetear historial
        return True, FLOOD_BAN_SECS

    return False, 0

# ══════════════════════════════════════════════════════════════════════════════
# CONSTRUCTORES DE MENSAJES
# ══════════════════════════════════════════════════════════════════════════════

def build_rate_msg(cmd: str) -> str:
    ensure_cache()
    iso, flag, name = CURRENCY_META[cmd]

    # ── /ves: redirige a @bcvpricesbot para la tasa del día ───────────────────
    if cmd == "ves":
        return (
            f"*TASAS DEL DÍA*\n"
            f"📅 {esc(date_es())}\n"
            f"↪️💰{flag} _Bolívar Venezolano_\n"
            f"\n"
            f"🇻🇪 Las tasas BCV actualizadas las encontrarás en:\n"
            f"👉 @bcvpricesbot\n"
            f"\n"
            f"`[ℹ️]` _Para convertir *VES* a otra moneda desde aquí, utilizar:_ "
            f"/convertir \\[valor\\] ves to \\[moneda destino\\]"
        )

    if cmd == "arsb":
        buy  = cache["blue_buy"]
        sell = cache["blue_sell"]
        rate_lines = (
            f"🇺🇸 Dólar Compra: \\$ {esc(fmt(float(buy))) if buy is not None else 'N/A'}\n"
            f"🇺🇸 Dólar Venta:  \\$ {esc(fmt(float(sell))) if sell is not None else 'N/A'}"
        )
        hint_cmd = "arsb"
    elif cmd == "ars":
        buy  = cache["official_buy"]
        sell = cache["official_sell"]
        rate_lines = (
            f"🇺🇸 Dólar Compra: \\$ {esc(fmt(float(buy))) if buy is not None else 'N/A'}\n"
            f"🇺🇸 Dólar Venta:  \\$ {esc(fmt(float(sell))) if sell is not None else 'N/A'}"
        )
        hint_cmd = "ars"
    elif cmd == "usd":
        rate_lines = "🇺🇸 1 USD \\= 1\\.00 USD"
        hint_cmd   = "usd"
    else:
        rate = get_usd_rate(cmd)
        if rate is None:
            return f"⚠️ No hay datos para *{esc(iso)}* hoy\\."
        rate_lines = f"🇺🇸 Dólar: {flag} {esc(fmt(rate))}"
        hint_cmd   = cmd

    return (
        f"*TASAS DEL DÍA*\n"
        f"📅 {esc(date_es())}\n"
        f"↪️💰{flag} _{esc(name)}_\n"
        f"\n"
        f"{rate_lines}\n"
        f"\n"
        f"`[ℹ️]` _Para convertir '{esc(iso)}' a otra moneda, utilizar:_ "
        f"/convertir \\[valor\\] {hint_cmd} to \\[moneda destino\\]"
    )


def build_conversion_msg(cmd_from: str, cmd_to: str, amount: float) -> str:
    ensure_cache()
    rate_from = get_usd_rate(cmd_from)
    rate_to   = get_usd_rate(cmd_to)

    if rate_from is None or rate_to is None:
        missing = cmd_from if rate_from is None else cmd_to
        return f"⚠️ Sin datos para `{esc(missing)}` hoy\\."

    result        = (amount / rate_from) * rate_to
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
    mins = seconds // 60
    secs = seconds % 60
    tiempo = f"{mins} min {secs} seg" if secs else f"{mins} min"
    return (
        "🚫 *Calma, calma\\!*\n\n"
        f"Usaste este comando demasiadas veces seguidas\\.\n"
        f"Por favor espera *{esc(tiempo)}* antes de volver a usarlo\\."
    )


async def _flood_check_and_reply(update: Update, cmd: str) -> bool:
    """
    Verifica flood para (user_id, cmd).
    Si está bloqueado, responde con advertencia y retorna True.
    Si no, retorna False (el handler puede continuar).
    """
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
            lines.append(
                f"{flag} {name_part} \\({iso_part}\\) → `/{cmd_k}`"
            )
        lines.append("")
    lines.append(
        "💡 _Toca cualquier comando para ver la tasa del día_\n"
        "_O usa_ /convertir \\[monto\\] \\[origen\\] to \\[destino\\]"
    )
    await update.effective_message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.MARKDOWN_V2
    )


async def handle_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler genérico para todos los comandos de moneda."""
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
                f"❌ Moneda desconocida: `{esc(cmd_check)}`\nUsa /paises para ver las disponibles\\.",
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            return

    msg = build_conversion_msg(cmd_from, cmd_to, amount)
    await update.effective_message.reply_text(msg, parse_mode=ParseMode.MARKDOWN_V2)


# ══════════════════════════════════════════════════════════════════════════════
# JOB — 00:00 hora Venezuela (04:00 UTC)
# ══════════════════════════════════════════════════════════════════════════════

async def daily_refresh_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("⏰ Job diario disparado — actualizando caché…")
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, refresh_cache)


# ══════════════════════════════════════════════════════════════════════════════
# FLASK
# ══════════════════════════════════════════════════════════════════════════════

flask_app = Flask(__name__)

_ptb_app: Optional[Application]                   = None
_ptb_loop: Optional[asyncio.AbstractEventLoop]    = None


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
    asyncio.run_coroutine_threadsafe(
        _ptb_app.process_update(update),
        _ptb_loop,
    )
    return Response("ok", status=200)


# ══════════════════════════════════════════════════════════════════════════════
# HILO PTB
# ══════════════════════════════════════════════════════════════════════════════

def run_ptb():
    """Corre PTB en su propio event loop (hilo daemon)."""
    global _ptb_app, _ptb_loop

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _ptb_loop = loop

    async def _run():
        global _ptb_app

        app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
        _ptb_app = app

        # Handlers
        app.add_handler(CommandHandler("start",     handle_start))
        app.add_handler(CommandHandler("paises",    handle_paises))
        app.add_handler(CommandHandler("convertir", handle_convertir))
        for cmd_key in CURRENCY_META:
            app.add_handler(CommandHandler(cmd_key, handle_rate))

        # Job diario: 00:00 VEN = 04:00 UTC
        midnight_utc = dtime(hour=4, minute=0, tzinfo=pytz.utc)
        app.job_queue.run_daily(
            daily_refresh_job,
            time=midnight_utc,
            name="daily_refresh",
        )

        await app.initialize()
        await app.start()

        if WEBHOOK_URL:
            full_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
            await app.bot.set_webhook(url=full_url, drop_pending_updates=True)
            logger.info(f"🔗 Webhook: {full_url}")
            # Mantener el loop vivo — Flask inyecta los updates
            while True:
                await asyncio.sleep(3600)
        else:
            logger.info("📡 Modo Polling")
            await app.updater.start_polling(drop_pending_updates=True)
            while True:
                await asyncio.sleep(3600)

    loop.run_until_complete(_run())


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # Carga inicial antes de arrancar
    ensure_cache()

    # PTB en hilo separado
    t = threading.Thread(target=run_ptb, daemon=True, name="ptb-loop")
    t.start()

    # Flask en hilo principal — Render envía tráfico aquí
    logger.info(f"🌐 Flask en puerto {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT, use_reloader=False)


if __name__ == "__main__":
    main()
