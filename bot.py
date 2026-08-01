"""
TON Meme Token Scanner Bot — Single File Version
=================================================
A Telegram bot that scans TON meme tokens by contract address.

Setup:
  1. Get a bot token from @BotFather
  2. Set the TELEGRAM_BOT_TOKEN environment variable
  3. pip install aiogram aiohttp python-dotenv
  4. python bot.py

Or deploy on Railway/Render (see README).
"""

import os
import re
import html
import time
import uuid
import asyncio
import base64
import struct
import logging
import sys

import aiohttp
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardButton,
    BufferedInputFile,
    InputMediaPhoto,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties

# ─── Configuration ───────────────────────────────────────────────────────────

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TONAPI_KEY = os.getenv("TONAPI_KEY", "")
DEBUG = os.getenv("DEBUG", "0") == "1"

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens"
TONAPI_BASE = "https://tonapi.io/v2"
GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"

# Chart timeframe presets: GeckoTerminal OHLCV params per button.
CHART_TIMEFRAMES = {
    "5m": {"timeframe": "minute", "aggregate": 5, "limit": 48, "label": "5m"},
    "1h": {"timeframe": "hour", "aggregate": 1, "limit": 48, "label": "1H"},
    "4h": {"timeframe": "hour", "aggregate": 4, "limit": 42, "label": "4H"},
    "1d": {"timeframe": "day", "aggregate": 1, "limit": 30, "label": "1D"},
}
CHART_TIMEFRAME_ORDER = ["5m", "1h", "4h", "1d"]
DEFAULT_CHART_TIMEFRAME = "1h"
REQUEST_TIMEOUT = 15

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ton-scanner-bot")

# ─── Report Cache (for expand/collapse buttons) ──────────────────────────────
# Maps a short report id -> {"report": dict, "show_info": bool, "show_holders": bool, "ts": float}
REPORT_CACHE: dict[str, dict] = {}
REPORT_CACHE_TTL = 60 * 60  # drop cached reports after 1 hour


def _cache_report(report: dict) -> str:
    """Store a report in the cache and return its lookup key."""
    _prune_report_cache()
    key = uuid.uuid4().hex[:12]
    REPORT_CACHE[key] = {
        "report": report,
        "show_info": False,
        "show_holders": False,
        "ts": time.time(),
    }
    return key


def _prune_report_cache():
    now = time.time()
    expired = [k for k, v in REPORT_CACHE.items() if now - v["ts"] > REPORT_CACHE_TTL]
    for k in expired:
        REPORT_CACHE.pop(k, None)

# ─── Address Utilities ────────────────────────────────────────────────────────

ADDR_FRIENDLY_RE = re.compile(r"^[EU]Q[A-Za-z0-9_\-]{46}$")
ADDR_RAW_RE = re.compile(r"^(0|[-1]):[0-9a-fA-F]{64}$")


def is_valid_ton_address(text: str) -> bool:
    text = text.strip()
    return bool(ADDR_FRIENDLY_RE.match(text) or ADDR_RAW_RE.match(text))


def _crc16(data: bytes) -> int:
    """CRC16-CCITT (XMODEM) — used by TON address checksums."""
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc <<= 1
            crc &= 0xFFFF
    return crc


def _raw_to_friendly(raw: str) -> str | None:
    """Convert raw TON address (0:hex) to bounceable user-friendly format (EQ...)."""
    try:
        parts = raw.split(":")
        workchain = int(parts[0])
        hash_hex = parts[1]
        addr_bytes = bytes([workchain & 0xFF]) + bytes.fromhex(hash_hex)
        tag = 0x11  # bounceable
        data = bytes([tag]) + addr_bytes
        crc = _crc16(data)
        full = data + struct.pack(">H", crc)
        return base64.urlsafe_b64encode(full).decode("ascii")
    except Exception:
        return None


# ─── DexScreener API ──────────────────────────────────────────────────────────

async def get_dex_data(session: aiohttp.ClientSession, address: str) -> list[dict] | None:
    """Fetch all DEX pairs for a token address from DexScreener."""
    url = f"{DEXSCREENER_API}/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            pairs = data.get("pairs")
            if not pairs:
                return []
            pairs.sort(key=lambda p: (p.get("liquidity") or {}).get("usd", 0), reverse=True)
            return pairs
    except (aiohttp.ClientError, TimeoutError):
        return None


# ─── TonAPI ───────────────────────────────────────────────────────────────────

def _tonapi_headers() -> dict:
    headers = {"Accept": "application/json"}
    if TONAPI_KEY:
        headers["Authorization"] = f"Bearer {TONAPI_KEY}"
    return headers


async def get_jetton_info(session: aiohttp.ClientSession, address: str) -> dict | None:
    """Get jetton metadata from TonAPI."""
    url = f"{TONAPI_BASE}/jettons/{address}"
    try:
        async with session.get(url, headers=_tonapi_headers(),
                               timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except (aiohttp.ClientError, TimeoutError):
        return None


async def get_jetton_holders(session: aiohttp.ClientSession, address: str, limit: int = 10) -> dict | None:
    """Get top jetton holders from TonAPI."""
    url = f"{TONAPI_BASE}/jettons/{address}/holders"
    params = {"limit": limit}
    try:
        async with session.get(url, headers=_tonapi_headers(), params=params,
                               timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except (aiohttp.ClientError, TimeoutError):
        return None


# ─── GeckoTerminal (OHLCV chart data) ─────────────────────────────────────────

async def get_ohlcv(session: aiohttp.ClientSession, pool_address: str, timeframe_key: str) -> list | None:
    """Fetch OHLCV candles for a TON pool from GeckoTerminal's free public API.

    Returns a list of [timestamp, open, high, low, close, volume] rows
    (ascending by time), or None on failure.
    """
    preset = CHART_TIMEFRAMES.get(timeframe_key, CHART_TIMEFRAMES[DEFAULT_CHART_TIMEFRAME])
    url = f"{GECKOTERMINAL_BASE}/networks/ton/pools/{pool_address}/ohlcv/{preset['timeframe']}"
    params = {"aggregate": preset["aggregate"], "limit": preset["limit"], "currency": "usd"}
    try:
        async with session.get(url, params=params,
                               timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            ohlcv_list = (data.get("data") or {}).get("attributes", {}).get("ohlcv_list")
            if not ohlcv_list:
                return None
            return sorted(ohlcv_list, key=lambda c: c[0])
    except (aiohttp.ClientError, TimeoutError):
        return None


def build_candlestick_chart(ohlcv: list, symbol: str, timeframe_label: str) -> bytes:
    """Render OHLCV candles into a dark-themed candlestick PNG. Returns raw PNG bytes."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from datetime import datetime, timezone

    times = [datetime.fromtimestamp(c[0], tz=timezone.utc) for c in ohlcv]
    opens = [c[1] for c in ohlcv]
    highs = [c[2] for c in ohlcv]
    lows = [c[3] for c in ohlcv]
    closes = [c[4] for c in ohlcv]

    bg = "#0e0e12"
    up_color = "#26a69a"
    down_color = "#ef5350"
    grid_color = "#2a2a33"
    text_color = "#e8e8ec"

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    n = len(ohlcv)
    body_width = 0.6
    for i in range(n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        color = up_color if c >= o else down_color
        ax.plot([i, i], [l, h], color=color, linewidth=1, solid_capstyle="round")
        bottom = min(o, c)
        height = abs(c - o)
        if height <= 0:
            height = max((h - l) * 0.01, h * 0.0005, 1e-12)
        ax.add_patch(plt.Rectangle((i - body_width / 2, bottom), body_width, height,
                                    facecolor=color, edgecolor=color, linewidth=0))

    # X-axis: show ~6 evenly spaced time labels
    tick_count = min(6, n)
    if tick_count > 1:
        tick_idx = [round(i * (n - 1) / (tick_count - 1)) for i in range(tick_count)]
    else:
        tick_idx = [0]
    tick_idx = sorted(set(tick_idx))
    fmt = "%H:%M" if timeframe_label in ("5m", "1H", "4H") else "%b %d"
    ax.set_xticks(tick_idx)
    ax.set_xticklabels([times[i].strftime(fmt) for i in tick_idx], color=text_color, fontsize=8)

    ax.set_xlim(-1, n)
    ax.tick_params(axis="y", colors=text_color, labelsize=8)
    ax.yaxis.tick_right()
    ax.grid(True, color=grid_color, linewidth=0.5, axis="both")
    for spine in ax.spines.values():
        spine.set_color(grid_color)

    last_price = closes[-1]
    ax.set_title(f"{symbol}  ·  {timeframe_label}  ·  ${last_price:.10g}".rstrip("0").rstrip("."),
                 color=text_color, fontsize=11, loc="left", pad=10)

    fig.tight_layout()
    from io import BytesIO
    buf = BytesIO()
    fig.savefig(buf, format="png", facecolor=bg)
    plt.close(fig)
    return buf.getvalue()


# ─── Scanner ───────────────────────────────────────────────────────────────────

async def scan_token(session: aiohttp.ClientSession, address: str) -> dict:
    """Scan a TON jetton — fetches data from DexScreener + TonAPI."""
    address = address.strip()

    jetton_info = await get_jetton_info(session, address)
    dex_pairs = await get_dex_data(session, address)

    # If DexScreener returned empty but TonAPI found the token, try alternative address formats
    if not dex_pairs and jetton_info:
        tonapi_addr = jetton_info.get("metadata", {}).get("address")
        if tonapi_addr:
            dex_pairs = await get_dex_data(session, tonapi_addr)
            if not dex_pairs and tonapi_addr.startswith("0:"):
                friendly = _raw_to_friendly(tonapi_addr)
                if friendly and friendly != address:
                    dex_pairs = await get_dex_data(session, friendly)

    report = {
        "address": address,
        "found": False,
        "dex_data": None,
        "jetton_info": None,
        "holders": None,
        "errors": [],
    }

    # --- Jetton metadata ---
    if jetton_info:
        report["found"] = True
        report["jetton_info"] = {
            "name": jetton_info.get("metadata", {}).get("name", "Unknown"),
            "symbol": jetton_info.get("metadata", {}).get("symbol", "???"),
            "decimals": jetton_info.get("metadata", {}).get("decimals", "9"),
            "image": jetton_info.get("metadata", {}).get("image"),
            "total_supply": jetton_info.get("total_supply"),
            "mintable": jetton_info.get("mintable"),
            "verification": jetton_info.get("verification", "none"),
            "holders_count": jetton_info.get("holders_count", 0),
        }

        total_supply = jetton_info.get("total_supply")
        holders_data = await get_jetton_holders(session, address, limit=10)
        report["holders"] = parse_holders(holders_data, total_supply)
    else:
        report["errors"].append("TonAPI: token not found or API unavailable")

    # --- DEX data ---
    if dex_pairs:
        report["found"] = True
        total_vol = sum((p.get("volume") or {}).get("h24", 0) for p in dex_pairs)
        total_liq = sum((p.get("liquidity") or {}).get("usd", 0) for p in dex_pairs)
        best = dex_pairs[0]
        dexes = list(set(p.get("dexId", "unknown") for p in dex_pairs))

        report["dex_data"] = {
            "price_usd": best.get("priceUsd"),
            "price_native": best.get("priceNative"),
            "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
            "total_liquidity_usd": total_liq,
            "volume_24h": total_vol,
            "fdv": best.get("fdv"),
            "market_cap": best.get("marketCap"),
            "price_change_5m": (best.get("priceChange") or {}).get("m5"),
            "price_change_1h": (best.get("priceChange") or {}).get("h1"),
            "price_change_6h": (best.get("priceChange") or {}).get("h6"),
            "price_change_24h": (best.get("priceChange") or {}).get("h24"),
            "txns_24h_buys": (best.get("txns", {}).get("h24") or {}).get("buys", 0),
            "txns_24h_sells": (best.get("txns", {}).get("h24") or {}).get("sells", 0),
            "pair_count": len(dex_pairs),
            "dexes": dexes,
            "dex_url": best.get("url"),
            "pair_address": best.get("pairAddress"),
            "pair_created_at": best.get("pairCreatedAt"),
            "quote_symbol": (best.get("quoteToken") or {}).get("symbol", ""),
            "info": best.get("info"),
        }
    else:
        if dex_pairs is None:
            report["errors"].append("DexScreener: API request failed")
        else:
            report["errors"].append("DexScreener: no DEX pairs found")

    return report


def parse_holders(holders_data: dict | None, total_supply: str | None) -> dict:
    """Parse holders data and compute concentration."""
    if not holders_data or not holders_data.get("addresses"):
        return {"holders": [], "top_concentration": None}

    supply = 0
    if total_supply:
        try:
            supply = int(total_supply)
        except (ValueError, TypeError):
            pass

    holders = []
    for h in holders_data["addresses"]:
        balance = 0
        try:
            balance = int(h.get("balance", "0"))
        except (ValueError, TypeError):
            pass
        owner = h.get("owner", {})
        pct = (balance / supply * 100) if supply > 0 else None
        holders.append({
            "address": owner.get("address", h.get("address", "")),
            "name": owner.get("name", ""),
            "is_scam": owner.get("is_scam", False),
            "is_wallet": owner.get("is_wallet", False),
            "balance": balance,
            "percentage": pct,
        })

    top_pct = sum(h["percentage"] for h in holders if h["percentage"] is not None) if holders else None
    return {"holders": holders, "top_concentration": top_pct}


# ─── Formatting ───────────────────────────────────────────────────────────────

def _fmt_price(price_str) -> str:
    if price_str is None:
        return "N/A"
    try:
        price = float(price_str)
    except (ValueError, TypeError):
        return "N/A"
    if price == 0:
        return "$0"
    elif price < 0.000001:
        return f"${price:.12f}"
    elif price < 0.0001:
        return f"${price:.9f}"
    elif price < 0.01:
        return f"${price:.6f}"
    elif price < 1:
        return f"${price:.4f}"
    elif price < 1000:
        return f"${price:.2f}"
    else:
        return f"${price:,.0f}"


def _fmt_usd(value) -> str:
    if value is None:
        return "N/A"
    try:
        if value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        elif value >= 1_000:
            return f"${value / 1_000:.1f}K"
        else:
            return f"${value:.2f}"
    except (ValueError, TypeError):
        return "N/A"


def _fmt_num(value) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{value:,.0f}"
    except (ValueError, TypeError):
        return "N/A"


def _fmt_pct(value) -> str:
    if value is None:
        return "N/A"
    try:
        return f"+{value:.2f}%" if value >= 0 else f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"


def _fmt_age(timestamp_ms) -> str:
    if not timestamp_ms:
        return "N/A"
    try:
        from datetime import datetime
        created = datetime.fromtimestamp(timestamp_ms / 1000)
        now = datetime.now()
        delta = now - created
        days = delta.days
        if days < 1:
            hours = delta.seconds // 3600
            if hours < 1:
                return f"{delta.seconds // 60}m"
            return f"{hours}h"
        elif days < 30:
            return f"{days}d"
        elif days < 365:
            return f"{days // 30}mo"
        else:
            return f"{days // 365}y"
    except Exception:
        return "N/A"


def _assess_risk(report: dict) -> tuple[str, list[str]]:
    """Assess risk and return (level, reasons)."""
    dex = report.get("dex_data") or {}
    info = report.get("jetton_info") or {}
    holders = report.get("holders") or {}
    risk_score = 0
    reasons = []

    liq = dex.get("liquidity_usd")
    if liq is not None:
        if liq < 10_000:
            risk_score += 3
            reasons.append("Low liquidity (<$10K)")
        elif liq < 50_000:
            risk_score += 2
            reasons.append("Moderate liquidity (<$50K)")
        elif liq < 100_000:
            risk_score += 1

    verification = info.get("verification", "none")
    if verification in ("whitelist", "approve"):
        risk_score -= 1
    elif verification == "none":
        risk_score += 1
        reasons.append("Unverified token")

    if info.get("mintable"):
        risk_score += 1
        reasons.append("Mintable supply")

    top_conc = holders.get("top_concentration")
    if top_conc is not None:
        if top_conc > 50:
            risk_score += 3
            reasons.append(f"High top-holder concentration ({top_conc:.0f}%)")
        elif top_conc > 25:
            risk_score += 2
            reasons.append(f"Moderate top-holder concentration ({top_conc:.0f}%)")
        elif top_conc > 10:
            risk_score += 1

    if dex.get("pair_created_at"):
        try:
            from datetime import datetime
            created = datetime.fromtimestamp(dex["pair_created_at"] / 1000)
            days = (datetime.now() - created).days
            if days < 1:
                risk_score += 2
                reasons.append("Very new pair (under 24h)")
            elif days < 7:
                risk_score += 1
                reasons.append("New pair (under 7d)")
        except Exception:
            pass

    level = "low" if risk_score <= 0 else ("medium" if risk_score <= 2 else "high")
    return level, reasons


def format_token_report(report: dict, show_info: bool = False, show_holders: bool = False) -> str:
    """Format a scan report as an HTML Telegram message.

    Token Info and Top Holders are collapsible: pass show_info / show_holders
    as True to render them in full, or False to render a collapsed placeholder
    line (used together with the toggle buttons from build_report_keyboard).
    """
    if not report.get("found"):
        errors = "\n".join(f"- {e}" for e in report.get("errors", []))
        return (
            "Token not found.\n\n"
            "Make sure you pasted the jetton master contract address "
            "(starts with <b>EQ</b> or <b>UQ</b>).\n\n"
            f"Details:\n{errors}"
        )

    dex = report.get("dex_data") or {}
    info = report.get("jetton_info") or {}
    holders = report.get("holders") or {}

    name = html.escape(str(info.get("name", "Unknown")))
    symbol = html.escape(str(info.get("symbol", "???")))
    address = html.escape(str(report.get("address", "")))

    risk, reasons = _assess_risk(report)
    risk_emoji = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(risk, "⚪")

    age = _fmt_age(dex.get("pair_created_at"))

    lines = []

    # ── Header ──
    tonviewer_url = f"https://tonviewer.com/{report.get('address', '')}"
    header = f'<a href="{html.escape(tonviewer_url)}"><b>{name}</b></a> ${symbol}'
    if age != "N/A":
        header += f"  ⏱ {age}"
    lines.append(header)
    lines.append(f"<code>{address}</code>")
    lines.append("")

    # ── Price ──
    price_line = f"💰 <b>{_fmt_price(dex.get('price_usd'))}</b>"
    if dex.get("price_native"):
        native = html.escape(str(dex['price_native']))
        quote_sym = html.escape(str(dex.get('quote_symbol', '')))
        price_line += f"  ({native} {quote_sym})"
    lines.append(price_line)

    changes = []
    for label, key in [("5m", "price_change_5m"), ("1h", "price_change_1h"),
                       ("6h", "price_change_6h"), ("24h", "price_change_24h")]:
        val = dex.get(key)
        if val is not None:
            arrow = "🔺" if val >= 0 else "🔻"
            changes.append(f"{label} {arrow}{_fmt_pct(val)}")
    if changes:
        lines.append("📈 " + "  ".join(html.escape(c) for c in changes))

    lines.append("")

    # ── Market data ──
    liq_str = _fmt_usd(dex.get('liquidity_usd'))
    if dex.get("total_liquidity_usd") and dex.get("total_liquidity_usd") != dex.get("liquidity_usd"):
        liq_str += f" (total {_fmt_usd(dex.get('total_liquidity_usd'))})"
    lines.append(f"💧 LIQ <b>{liq_str}</b>   🪙 VOL <b>{_fmt_usd(dex.get('volume_24h'))}</b>")
    lines.append(f"📊 MCAP <b>{_fmt_usd(dex.get('market_cap'))}</b>   FDV {_fmt_usd(dex.get('fdv'))}")

    buys = dex.get("txns_24h_buys", 0)
    sells = dex.get("txns_24h_sells", 0)
    dexes = dex.get("dexes", [])
    pair_count = dex.get("pair_count", 0)
    txn_bits = []
    if buys or sells:
        txn_bits.append(f"🟢{buys}/🔴{sells}")
    if dexes:
        dexes_safe = ", ".join(html.escape(str(d)) for d in dexes)
        txn_bits.append(f"{dexes_safe} ({pair_count} pair{'s' if pair_count != 1 else ''})")
    if txn_bits:
        lines.append("🔁 " + "  ·  ".join(txn_bits))

    if dex.get("dex_url"):
        safe_url = html.escape(str(dex["dex_url"]))
        lines.append(f'🔗 <a href="{safe_url}">DexScreener</a>')

    lines.append("")

    # ── Token info (collapsible) ──
    if show_info:
        verification = info.get("verification", "none")
        ver_label = {"whitelist": "✅ Verified (whitelist)", "approve": "✅ Verified (approved)",
                     "none": "⚠️ Unverified"}.get(verification, verification)
        info_bits = [ver_label, f"👥 {_fmt_num(info.get('holders_count'))} holders"]

        supply = info.get("total_supply")
        if supply:
            try:
                supply_int = int(supply)
                decimals = int(info.get("decimals", "9"))
                human = supply_int / (10 ** decimals)
                info_bits.append(f"🏦 Supply {_fmt_num(human)} {html.escape(str(symbol))}")
            except (ValueError, TypeError):
                pass

        info_bits.append("🔓 Mintable" if info.get("mintable") else "🔒 Fixed supply")

        lines.append("<b>📋 Token Info</b>")
        lines.append("  " + "  ·  ".join(info_bits))
    else:
        lines.append("📋 <b>Token Info</b>  ▸ tap below")
    lines.append("")

    # ── Top holders (collapsible) ──
    holder_list = holders.get("holders", [])
    if holder_list:
        if show_holders:
            top_conc = holders.get("top_concentration")
            conc_str = f" · Top 10: <b>{top_conc:.1f}%</b>" if top_conc is not None else ""
            lines.append(f"<b>👥 Top Holders</b>{conc_str}")
            for i, h in enumerate(holder_list[:5], 1):
                pct = h.get("percentage")
                pct_str = f" ({pct:.1f}%)" if pct is not None else ""
                name_str = f" [{html.escape(str(h['name']))}]" if h.get("name") else ""
                scam = " ⚠️SCAM" if h.get("is_scam") else ""
                addr = h.get("address", "")
                short = addr[:8] + "…" + addr[-4:] if len(addr) > 14 else addr
                short = html.escape(short)
                lines.append(f"  {i}. <code>{short}</code>{name_str}{scam}{pct_str}")
        else:
            lines.append("👥 <b>Top Holders</b>  ▸ tap below")
        lines.append("")

    # ── Risk ──
    risk_line = f"{risk_emoji} <b>Risk: {risk.capitalize()}</b>"
    if reasons:
        risk_line += "  —  " + " · ".join(html.escape(r) for r in reasons)
    lines.append(risk_line)

    lines.append("")
    lines.append("<i>DexScreener + TonAPI · Not financial advice</i>")
    return "\n".join(lines)


def build_report_keyboard(key: str, show_info: bool, show_holders: bool, has_chart: bool = False) -> InlineKeyboardBuilder:
    """Build the toggle buttons for a cached report's expand/collapse sections."""
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=("▾ Hide Token Info" if show_info else "▸ Show Token Info"),
            callback_data=f"tg:info:{key}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=("▾ Hide Top Holders" if show_holders else "▸ Show Top Holders"),
            callback_data=f"tg:holders:{key}",
        )
    )
    if has_chart:
        builder.row(
            InlineKeyboardButton(text="📈 Chart", callback_data=f"tg:chart:{key}")
        )
    return builder.as_markup()


def build_chart_keyboard(key: str, selected_tf: str) -> InlineKeyboardBuilder:
    """Build the timeframe switch buttons shown under a chart image."""
    builder = InlineKeyboardBuilder()
    buttons = [
        InlineKeyboardButton(
            text=(f"• {CHART_TIMEFRAMES[tf]['label']} •" if tf == selected_tf else CHART_TIMEFRAMES[tf]['label']),
            callback_data=f"tf:{tf}:{key}",
        )
        for tf in CHART_TIMEFRAME_ORDER
    ]
    builder.row(*buttons)
    return builder.as_markup()


# ─── Telegram Bot ─────────────────────────────────────────────────────────────

if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN is not set! Copy .env.example to .env and add your token.")
    sys.exit(1)

bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()


@dp.message(Command("start", "help"))
async def cmd_start(message: Message):
    await message.answer(
        "<b>TON Meme Token Scanner</b>\n\n"
        "Send me a TON jetton contract address (starts with <b>EQ</b> or <b>UQ</b>) "
        "and I'll scan it instantly.\n\n"
        "You'll get:\n"
        "- Price & market cap\n"
        "- Liquidity & volume\n"
        "- Price changes (5m, 1h, 6h, 24h)\n"
        "- Buy/sell transaction counts\n"
        "- Holder count & top holders\n"
        "- Verification status\n"
        "- Mintable supply check\n"
        "- Risk assessment\n\n"
        "Example: <code>EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOTC</code>\n\n"
        "Data: DexScreener + TonAPI | Not financial advice"
    )


@dp.message(F.text)
async def handle_address(message: Message):
    text = message.text.strip()

    if not is_valid_ton_address(text):
        await message.answer(
            "Please send a valid TON contract address.\n\n"
            "It should start with <b>EQ</b> or <b>UQ</b> "
            "(e.g. <code>EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOTC</code>)"
        )
        return

    status_msg = await message.answer("Scanning token...")

    try:
        async with aiohttp.ClientSession() as session:
            report = await scan_token(session, text)

        if not report.get("found"):
            # Nothing to expand/collapse — send the plain not-found message.
            result = format_token_report(report)
            await status_msg.edit_text(result, disable_web_page_preview=True)
            return

        key = _cache_report(report)
        has_chart = bool((report.get("dex_data") or {}).get("pair_address"))
        result = format_token_report(report, show_info=False, show_holders=False)
        keyboard = build_report_keyboard(key, show_info=False, show_holders=False, has_chart=has_chart)
        await status_msg.edit_text(result, disable_web_page_preview=True, reply_markup=keyboard)
    except Exception as e:
        logger.exception("Error scanning token")
        await status_msg.edit_text(f"Error scanning token: {html.escape(str(e))}\n\nPlease try again later.")


@dp.callback_query(F.data.startswith("tg:"))
async def handle_toggle(callback: CallbackQuery):
    """Handle Token Info / Top Holders / Chart button taps."""
    try:
        _, section, key = callback.data.split(":", 2)
    except ValueError:
        await callback.answer()
        return

    entry = REPORT_CACHE.get(key)
    if not entry:
        await callback.answer("This report has expired — please scan the token again.", show_alert=True)
        return
    entry["ts"] = time.time()  # refresh TTL on activity

    if section == "chart":
        await _send_chart(callback, key, DEFAULT_CHART_TIMEFRAME)
        return

    if section == "info":
        entry["show_info"] = not entry["show_info"]
    elif section == "holders":
        entry["show_holders"] = not entry["show_holders"]

    has_chart = bool((entry["report"].get("dex_data") or {}).get("pair_address"))
    text = format_token_report(entry["report"], show_info=entry["show_info"], show_holders=entry["show_holders"])
    keyboard = build_report_keyboard(key, entry["show_info"], entry["show_holders"], has_chart=has_chart)
    try:
        await callback.message.edit_text(text, disable_web_page_preview=True, reply_markup=keyboard)
    except Exception:
        # Editing fails if content is identical (rare) or message too old — ignore.
        pass
    await callback.answer()


async def _send_chart(callback: CallbackQuery, key: str, timeframe: str):
    """Fetch OHLCV data and send a brand-new chart photo message."""
    entry = REPORT_CACHE.get(key)
    pool_address = (entry["report"].get("dex_data") or {}).get("pair_address")
    symbol = (entry["report"].get("jetton_info") or {}).get("symbol", "???")

    if not pool_address:
        await callback.answer("No chart available for this token.", show_alert=True)
        return

    await callback.answer("Loading chart...")
    async with aiohttp.ClientSession() as session:
        ohlcv = await get_ohlcv(session, pool_address, timeframe)

    if not ohlcv:
        await callback.message.answer("Couldn't load chart data for this pool right now — try again shortly.")
        return

    png_bytes = build_candlestick_chart(ohlcv, symbol, CHART_TIMEFRAMES[timeframe]["label"])
    photo = BufferedInputFile(png_bytes, filename="chart.png")
    keyboard = build_chart_keyboard(key, timeframe)
    await callback.message.answer_photo(
        photo=photo,
        caption=f"<b>{html.escape(str(symbol))}</b> price chart",
        reply_markup=keyboard,
    )


@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe(callback: CallbackQuery):
    """Handle timeframe button taps on an existing chart message (switches in place)."""
    try:
        _, timeframe, key = callback.data.split(":", 2)
    except ValueError:
        await callback.answer()
        return

    entry = REPORT_CACHE.get(key)
    if not entry:
        await callback.answer("This report has expired — please scan the token again.", show_alert=True)
        return
    entry["ts"] = time.time()

    pool_address = (entry["report"].get("dex_data") or {}).get("pair_address")
    symbol = (entry["report"].get("jetton_info") or {}).get("symbol", "???")
    if not pool_address:
        await callback.answer("No chart available for this token.", show_alert=True)
        return
    if timeframe not in CHART_TIMEFRAMES:
        await callback.answer()
        return

    await callback.answer("Loading chart...")
    async with aiohttp.ClientSession() as session:
        ohlcv = await get_ohlcv(session, pool_address, timeframe)

    if not ohlcv:
        await callback.answer("Couldn't load chart data for that timeframe.", show_alert=True)
        return

    png_bytes = build_candlestick_chart(ohlcv, symbol, CHART_TIMEFRAMES[timeframe]["label"])
    media = InputMediaPhoto(
        media=BufferedInputFile(png_bytes, filename="chart.png"),
        caption=f"<b>{html.escape(str(symbol))}</b> price chart",
    )
    keyboard = build_chart_keyboard(key, timeframe)
    try:
        await callback.message.edit_media(media=media, reply_markup=keyboard)
    except Exception:
        logger.exception("Error switching chart timeframe")


async def main():
    logger.info("Starting TON Meme Token Scanner bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
