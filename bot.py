"""
TON Meme Token Scanner Bot — Single File Version
Persistent scan history edition.
Shows full contract address, uses Scanned By, includes Refresh button.
Adds bonding-curve display block for non-bonded TopBlast tokens.
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
import sqlite3

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

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TONAPI_KEY = os.getenv("TONAPI_KEY", "")
DEBUG = os.getenv("DEBUG", "0") == "1"
DB_PATH = os.getenv("SCAN_DB_PATH", "scan_history.db")

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens"
DEXSCREENER_SEARCH_API = "https://api.dexscreener.com/latest/dex/search"
TONAPI_BASE = "https://tonapi.io/v2"
GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"
REDOTRADE_URL = os.getenv("REDOTRADE_URL", "https://t.me/redotrade?start=XgWwrQ0D")
DTRADE_URL = os.getenv("DTRADE_URL", "https://t.me/dtrade?start=203XZgXX1X")
DEFAULT_SCANNER_LABEL = os.getenv("DEFAULT_SCANNER_LABEL", "Scanner")

CHART_TIMEFRAMES = {
    "5m": {"timeframe": "minute", "aggregate": 5, "limit": 48, "label": "5m"},
    "1h": {"timeframe": "hour", "aggregate": 1, "limit": 48, "label": "1H"},
    "4h": {"timeframe": "hour", "aggregate": 4, "limit": 42, "label": "4H"},
    "1d": {"timeframe": "day", "aggregate": 1, "limit": 30, "label": "1D"},
}
CHART_TIMEFRAME_ORDER = ["5m", "1h", "4h", "1d"]
DEFAULT_CHART_TIMEFRAME = "1h"
REQUEST_TIMEOUT = 15
REPORT_CACHE: dict[str, dict] = {}
REPORT_CACHE_TTL = 60 * 60
MAX_SCAN_HISTORY = 12

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ton-scanner-bot")

ADDR_FRIENDLY_RE = re.compile(r"^[EU]Q[A-Za-z0-9_\-]{46}$")
ADDR_RAW_RE = re.compile(r"^(0|[-1]):[0-9a-fA-F]{64}$")
TICKER_RE = re.compile(r"^\$?[A-Za-z0-9]{2,15}$")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_key TEXT NOT NULL,
            token_address TEXT,
            token_symbol TEXT,
            scanner_id INTEGER,
            scanner_name TEXT,
            scan_price TEXT,
            scan_market_cap TEXT,
            scan_ts INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_scan_token_key ON scan_history(token_key, scan_ts DESC)"
    )
    # One-time cleanup: remove any previously-stuck rows that were saved
    # without price/market cap data (e.g. from a transient DEX API hiccup).
    # These would otherwise permanently show as "N/A" in the Scanned By
    # section since get_first_scan always returns the oldest row.
    conn.execute(
        """
        DELETE FROM scan_history
        WHERE (scan_price IS NULL OR scan_price = 'N/A')
          AND (scan_market_cap IS NULL OR scan_market_cap = 'N/A')
        """
    )
    conn.commit()
    conn.close()


def _history_key(report: dict) -> str:
    address = str(report.get("address") or "").strip()
    symbol = str((report.get("jetton_info") or {}).get("symbol") or "").strip().upper()
    return address or symbol


def save_scan_history(report: dict, scanner_meta: dict | None):
    if not scanner_meta:
        return

    token_key = _history_key(report)
    if not token_key:
        return

    scan_ts = int(scanner_meta.get("scan_ts") or time.time())
    scanner_id = scanner_meta.get("scanner_id")
    scanner_name = scanner_meta.get("scanner_name")
    scan_price = scanner_meta.get("scan_price")
    scan_market_cap = scanner_meta.get("scan_market_cap")

    # Skip persisting scans where price/market cap data wasn't available yet
    # (e.g. a transient DEX API hiccup on the very first scan). Otherwise a
    # single bad scan gets permanently "stuck" as the recorded first scan,
    # showing N/A forever even once later scans have good data.
    if (not scan_price or scan_price == "N/A") and (not scan_market_cap or scan_market_cap == "N/A"):
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    count_row = conn.execute(
        "SELECT COUNT(*) AS cnt FROM scan_history WHERE token_key = ?",
        (token_key,),
    ).fetchone()
    existing_count = int(count_row["cnt"]) if count_row else 0

    if existing_count >= MAX_SCAN_HISTORY:
        oldest = conn.execute(
            """
            SELECT id FROM scan_history
            WHERE token_key = ?
            ORDER BY scan_ts ASC, id ASC
            LIMIT 1
            """,
            (token_key,),
        ).fetchone()
        if oldest:
            conn.execute("DELETE FROM scan_history WHERE id = ?", (oldest["id"],))

    conn.execute(
        """
        INSERT INTO scan_history (
            token_key, token_address, token_symbol, scanner_id, scanner_name,
            scan_price, scan_market_cap, scan_ts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            token_key,
            str(report.get("address") or ""),
            str((report.get("jetton_info") or {}).get("symbol") or ""),
            scanner_id,
            scanner_name,
            scan_price,
            scan_market_cap,
            scan_ts,
        ),
    )
    conn.commit()
    conn.close()


def get_scan_history(report: dict, limit: int = MAX_SCAN_HISTORY) -> list[dict]:
    token_key = _history_key(report)
    if not token_key:
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT scanner_id, scanner_name, scan_price, scan_market_cap, scan_ts
        FROM scan_history
        WHERE token_key = ?
        ORDER BY scan_ts DESC, id DESC
        LIMIT ?
        """,
        (token_key, limit),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_first_scan(report: dict) -> dict | None:
    token_key = _history_key(report)
    if not token_key:
        return None

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT scanner_id, scanner_name, scan_price, scan_market_cap, scan_ts
        FROM scan_history
        WHERE token_key = ?
        ORDER BY scan_ts ASC, id ASC
        LIMIT 1
        """,
        (token_key,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _cache_report(report: dict, scanner_meta: dict | None = None) -> str:
    _prune_report_cache()
    key = uuid.uuid4().hex[:12]

    if scanner_meta:
        save_scan_history(report, scanner_meta)

    REPORT_CACHE[key] = {
        "report": report,
        "show_info": False,
        "show_holders": False,
        "chart_tf": DEFAULT_CHART_TIMEFRAME,
        "has_image": bool(_safe_image_url(report)),
        "scanner_meta": scanner_meta or {},
        "scan_history": get_scan_history(report),
        "ts": time.time(),
    }
    return key


def _prune_report_cache():
    now = time.time()
    expired = [k for k, v in REPORT_CACHE.items() if now - v["ts"] > REPORT_CACHE_TTL]
    for k in expired:
        REPORT_CACHE.pop(k, None)


def is_valid_ton_address(text: str) -> bool:
    text = text.strip()
    return bool(ADDR_FRIENDLY_RE.match(text) or ADDR_RAW_RE.match(text))


def is_valid_ticker(text: str) -> bool:
    return bool(TICKER_RE.match(text.strip()))


def normalize_ticker(text: str) -> str:
    return text.strip().lstrip("$").upper()


def _crc16(data: bytes) -> int:
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
    try:
        parts = raw.split(":")
        workchain = int(parts[0])
        hash_hex = parts[1]
        addr_bytes = bytes([workchain & 0xFF]) + bytes.fromhex(hash_hex)
        tag = 0x11
        data = bytes([tag]) + addr_bytes
        crc = _crc16(data)
        full = data + struct.pack(">H", crc)
        return base64.urlsafe_b64encode(full).decode("ascii")
    except Exception:
        return None


def _safe_image_url(report: dict) -> str | None:
    image_url = (report.get("jetton_info") or {}).get("image")
    if not image_url or not isinstance(image_url, str):
        return None
    image_url = image_url.strip()
    if image_url.startswith("http://") or image_url.startswith("https://"):
        return image_url
    return None


async def get_dex_data(session: aiohttp.ClientSession, address: str) -> list[dict] | None:
    url = f"{DEXSCREENER_API}/{address}"
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            pairs = data.get("pairs")
            if not pairs:
                return []
            pairs.sort(
                key=lambda p: (p.get("liquidity") or {}).get("usd", 0),
                reverse=True,
            )
            return pairs
    except (aiohttp.ClientError, TimeoutError):
        return None


async def search_dex_pairs(session: aiohttp.ClientSession, query: str) -> list[dict] | None:
    try:
        async with session.get(
            DEXSCREENER_SEARCH_API,
            params={"q": query},
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("pairs", [])
    except (aiohttp.ClientError, TimeoutError):
        return None


def _is_ton_pair(pair: dict) -> bool:
    return str(pair.get("chainId", "")).lower() == "ton"


def _pair_symbol(pair: dict) -> str:
    return str((pair.get("baseToken") or {}).get("symbol", "")).upper()


def _pair_name(pair: dict) -> str:
    return str((pair.get("baseToken") or {}).get("name", "")).upper()


def _pair_liquidity(pair: dict) -> float:
    try:
        return float((pair.get("liquidity") or {}).get("usd", 0) or 0)
    except (ValueError, TypeError):
        return 0.0


def _pair_volume(pair: dict) -> float:
    try:
        return float((pair.get("volume") or {}).get("h24", 0) or 0)
    except (ValueError, TypeError):
        return 0.0


async def resolve_ticker_to_address(
    session: aiohttp.ClientSession, ticker_text: str
) -> tuple[str | None, str | None]:
    ticker = normalize_ticker(ticker_text)
    pairs = await search_dex_pairs(session, ticker)

    if pairs is None:
        return None, "Ticker search failed. Please try again later."
    if not pairs:
        return None, f"No results found for ${ticker}."

    ton_pairs = [p for p in pairs if _is_ton_pair(p)]
    if not ton_pairs:
        return None, f"No TON token found for ${ticker}."

    exact_symbol = [p for p in ton_pairs if _pair_symbol(p) == ticker]
    symbol_contains = [p for p in ton_pairs if ticker in _pair_symbol(p)]
    name_contains = [p for p in ton_pairs if ticker in _pair_name(p)]
    candidates = exact_symbol or symbol_contains or name_contains or ton_pairs
    candidates.sort(
        key=lambda p: (_pair_symbol(p) != ticker, -_pair_liquidity(p), -_pair_volume(p))
    )

    best = candidates[0]
    address = (best.get("baseToken") or {}).get("address")
    if not address:
        return None, f"Found TON results for ${ticker}, but no contract address was available."
    return address, None


def _tonapi_headers() -> dict:
    headers = {"Accept": "application/json"}
    if TONAPI_KEY:
        headers["Authorization"] = f"Bearer {TONAPI_KEY}"
    return headers


async def get_jetton_info(session: aiohttp.ClientSession, address: str) -> dict | None:
    url = f"{TONAPI_BASE}/jettons/{address}"
    try:
        async with session.get(
            url,
            headers=_tonapi_headers(),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except (aiohttp.ClientError, TimeoutError):
        return None


async def get_jetton_holders(
    session: aiohttp.ClientSession, address: str, limit: int = 10
) -> dict | None:
    url = f"{TONAPI_BASE}/jettons/{address}/holders"
    try:
        async with session.get(
            url,
            headers=_tonapi_headers(),
            params={"limit": limit},
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except (aiohttp.ClientError, TimeoutError):
        return None


async def get_ohlcv(
    session: aiohttp.ClientSession, pool_address: str, timeframe_key: str
) -> list | None:
    preset = CHART_TIMEFRAMES.get(timeframe_key, CHART_TIMEFRAMES[DEFAULT_CHART_TIMEFRAME])
    url = f"{GECKOTERMINAL_BASE}/networks/ton/pools/{pool_address}/ohlcv/{preset['timeframe']}"
    params = {"aggregate": preset["aggregate"], "limit": preset["limit"], "currency": "usd"}

    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            items = (data.get("data") or {}).get("attributes", {}).get("ohlcv_list")
            return sorted(items, key=lambda c: c[0]) if items else None
    except (aiohttp.ClientError, TimeoutError):
        return None


def build_candlestick_chart(ohlcv: list, symbol: str, timeframe_label: str) -> bytes:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from datetime import datetime, timezone
    from io import BytesIO

    times = [datetime.fromtimestamp(c[0], tz=timezone.utc) for c in ohlcv]
    opens = [c[1] for c in ohlcv]
    highs = [c[2] for c in ohlcv]
    lows = [c[3] for c in ohlcv]
    closes = [c[4] for c in ohlcv]

    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
    bg = "#0e0e12"
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    for i, (o, h, l, c) in enumerate(zip(opens, highs, lows, closes)):
        color = "#26a69a" if c >= o else "#ef5350"
        ax.plot([i, i], [l, h], color=color, linewidth=1)
        bottom = min(o, c)
        height = abs(c - o) or max((h - l) * 0.01, h * 0.0005, 1e-12)
        ax.add_patch(
            plt.Rectangle(
                (i - 0.3, bottom),
                0.6,
                height,
                facecolor=color,
                edgecolor=color,
                linewidth=0,
            )
        )

    tick_count = min(6, len(ohlcv))
    tick_idx = (
        [round(i * (len(ohlcv) - 1) / (tick_count - 1)) for i in range(tick_count)]
        if tick_count > 1
        else [0]
    )
    tick_idx = sorted(set(tick_idx))
    fmt = "%H:%M" if timeframe_label in ("5m", "1H", "4H") else "%b %d"

    ax.set_xticks(tick_idx)
    ax.set_xticklabels([times[i].strftime(fmt) for i in tick_idx], color="#e8e8ec", fontsize=8)
    ax.tick_params(axis="y", colors="#e8e8ec", labelsize=8)
    ax.yaxis.tick_right()
    ax.grid(True, color="#2a2a33", linewidth=0.5)

    for spine in ax.spines.values():
        spine.set_color("#2a2a33")

    ax.set_title(
        f"{symbol}  ·  {timeframe_label}  ·  ${closes[-1]:.10g}".rstrip("0").rstrip("."),
        color="#e8e8ec",
        fontsize=11,
        loc="left",
        pad=10,
    )

    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", facecolor=bg)
    plt.close(fig)
    return buf.getvalue()


def build_report_card(ohlcv: list, report: dict, timeframe_label: str) -> bytes:
    """Composite scan card: header stats bar + candlestick chart in one PNG."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from datetime import datetime, timezone
    from io import BytesIO

    info = report.get("jetton_info") or {}
    dex = report.get("dex_data") or {}

    symbol = str(info.get("symbol") or "???")
    name = str(info.get("name") or "Unknown")
    age = _fmt_age(dex.get("pair_created_at"))

    txns = dex.get("txns_24h") or {}
    txns_total = (txns.get("buys") or 0) + (txns.get("sells") or 0)

    h1 = dex.get("price_change_1h")
    h6 = dex.get("price_change_6h")
    h24 = dex.get("price_change_24h")

    try:
        headline_positive = float(h24) >= 0
    except (TypeError, ValueError):
        headline_positive = True

    bg = "#0e0e12"
    green = "#26a69a"
    red = "#ef5350"
    headline_color = green if headline_positive else red

    times = [datetime.fromtimestamp(c[0], tz=timezone.utc) for c in ohlcv]
    opens = [c[1] for c in ohlcv]
    highs = [c[2] for c in ohlcv]
    lows = [c[3] for c in ohlcv]
    closes = [c[4] for c in ohlcv]

    fig = plt.figure(figsize=(8, 5.6), dpi=150)
    fig.patch.set_facecolor(bg)
    gs = gridspec.GridSpec(3, 1, height_ratios=[0.65, 0.55, 3.3], hspace=0.12)

    # --- Header row: symbol / name / age + 24h change badge ---
    ax_head = fig.add_subplot(gs[0])
    ax_head.set_facecolor(bg)
    ax_head.axis("off")
    ax_head.text(
        0.01, 0.62, symbol, color="#e8e8ec", fontsize=19, fontweight="bold",
        va="center", ha="left", transform=ax_head.transAxes,
    )
    ax_head.text(
        0.01, 0.12, f"{name}  ·  {age}", color="#9a9aa5", fontsize=10,
        va="center", ha="left", transform=ax_head.transAxes,
    )
    ax_head.text(
        0.99, 0.5, _fmt_pct(h24), color=headline_color, fontsize=15, fontweight="bold",
        va="center", ha="right", transform=ax_head.transAxes,
        bbox=dict(
            boxstyle="round,pad=0.45",
            facecolor="#123028" if headline_positive else "#301416",
            edgecolor="none",
        ),
    )

    # --- Stats row: MCAP / LIQ / VOL / TXNS / 1H / 6H ---
    ax_stats = fig.add_subplot(gs[1])
    ax_stats.set_facecolor(bg)
    ax_stats.axis("off")

    stats = [
        ("MCAP", _fmt_usd(dex.get("market_cap")), None),
        ("LIQ", _fmt_usd(dex.get("liquidity_usd")), None),
        ("VOL", _fmt_usd(dex.get("volume_24h")), None),
        ("TXNS", f"{txns_total:,}" if txns_total else "N/A", None),
        ("1H", _fmt_pct(h1), h1),
        ("6H", _fmt_pct(h6), h6),
    ]
    n = len(stats)
    for i, (label, value, change_val) in enumerate(stats):
        x = (i + 0.5) / n
        ax_stats.text(
            x, 0.78, label, color="#7a7a85", fontsize=8.5,
            ha="center", va="center", transform=ax_stats.transAxes,
        )
        value_color = "#e8e8ec"
        if change_val is not None:
            try:
                value_color = green if float(change_val) >= 0 else red
            except (TypeError, ValueError):
                pass
        ax_stats.text(
            x, 0.22, value, color=value_color, fontsize=10.5, fontweight="bold",
            ha="center", va="center", transform=ax_stats.transAxes,
        )

    # --- Candlestick chart row (reuses the same look as build_candlestick_chart) ---
    ax = fig.add_subplot(gs[2])
    ax.set_facecolor(bg)

    for i, (o, h, l, c) in enumerate(zip(opens, highs, lows, closes)):
        color = green if c >= o else red
        ax.plot([i, i], [l, h], color=color, linewidth=1)
        bottom = min(o, c)
        height = abs(c - o) or max((h - l) * 0.01, h * 0.0005, 1e-12)
        ax.add_patch(
            plt.Rectangle(
                (i - 0.3, bottom),
                0.6,
                height,
                facecolor=color,
                edgecolor=color,
                linewidth=0,
            )
        )

    tick_count = min(6, len(ohlcv))
    tick_idx = (
        [round(i * (len(ohlcv) - 1) / (tick_count - 1)) for i in range(tick_count)]
        if tick_count > 1
        else [0]
    )
    tick_idx = sorted(set(tick_idx))
    fmt = "%H:%M" if timeframe_label in ("5m", "1H", "4H") else "%b %d"

    ax.set_xticks(tick_idx)
    ax.set_xticklabels([times[i].strftime(fmt) for i in tick_idx], color="#e8e8ec", fontsize=8)
    ax.tick_params(axis="y", colors="#e8e8ec", labelsize=8)
    ax.yaxis.tick_right()
    ax.grid(True, color="#2a2a33", linewidth=0.5)

    for spine in ax.spines.values():
        spine.set_color("#2a2a33")

    fig.subplots_adjust(left=0.02, right=0.93, top=0.96, bottom=0.06)

    buf = BytesIO()
    fig.savefig(buf, format="png", facecolor=bg)
    plt.close(fig)
    return buf.getvalue()


async def build_scan_photo(
    session: aiohttp.ClientSession, report: dict
) -> BufferedInputFile | None:
    """Fetches OHLCV for the token's pool and renders the composite scan card.
    Returns None if there's no pool/chart data, so callers can fall back to the
    token logo image."""
    dex = report.get("dex_data") or {}
    pool_address = dex.get("pair_address")
    if not pool_address:
        return None

    ohlcv = await get_ohlcv(session, pool_address, DEFAULT_CHART_TIMEFRAME)
    if not ohlcv:
        return None

    try:
        png_bytes = build_report_card(
            ohlcv, report, CHART_TIMEFRAMES[DEFAULT_CHART_TIMEFRAME]["label"]
        )
    except Exception:
        logger.exception("Error building scan report card image")
        return None

    return BufferedInputFile(png_bytes, filename="chart.png")


def parse_holders(holders_data: dict | None, total_supply: str | None) -> dict:
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
        try:
            balance = int(h.get("balance", "0"))
        except (ValueError, TypeError):
            balance = 0

        owner = h.get("owner", {})
        pct = (balance / supply * 100) if supply > 0 else None
        holders.append(
            {
                "address": owner.get("address", h.get("address", "")),
                "name": owner.get("name", ""),
                "is_scam": owner.get("is_scam", False),
                "balance": balance,
                "percentage": pct,
            }
        )

    top_pct = sum(h["percentage"] for h in holders if h["percentage"] is not None) if holders else None
    return {"holders": holders, "top_concentration": top_pct}


def _fmt_gram(value) -> str:
    if value is None:
        return "N/A"
    try:
        value = float(value)
        if value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"{value / 1_000:.1f}K"
        if value.is_integer():
            return f"{int(value)}"
        return f"{value:.2f}"
    except (ValueError, TypeError):
        return "N/A"


def _calc_bonding_curve(collected, target) -> dict | None:
    try:
        collected = float(collected)
        target = float(target)
        if target <= 0:
            return None
    except (ValueError, TypeError):
        return None

    percent = max(0.0, min((collected / target) * 100.0, 100.0))
    remaining = max(target - collected, 0.0)

    return {
        "collected_gram": collected,
        "target_gram": target,
        "remaining_gram": remaining,
        "percent": percent,
        "bonded": percent >= 100.0,
    }


def _progress_bar(percent: float | None, size: int = 12) -> str:
    try:
        percent = max(0.0, min(float(percent), 100.0))
    except (ValueError, TypeError):
        percent = 0.0

    filled = round((percent / 100.0) * size)
    return "█" * filled + "░" * (size - filled)


async def get_topblast_bonding_curve(
    session: aiohttp.ClientSession, address: str, symbol: str | None = None
) -> dict | None:
    # Placeholder hook for TopBlast non-bonded token data.
    # Add the real TopBlast request here when you have the endpoint / response format.
    # Expected return format:
    # {
    #     "collected_gram": 0,
    #     "target_gram": 1550,
    #     "remaining_gram": 1550,
    #     "percent": 0.0,
    #     "bonded": False,
    # }
    return None


async def scan_token(session: aiohttp.ClientSession, address: str) -> dict:
    jetton_info = await get_jetton_info(session, address)
    dex_pairs = await get_dex_data(session, address)

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
        "bonding_curve": None,
        "errors": [],
    }

    if jetton_info:
        report["found"] = True
        metadata = jetton_info.get("metadata", {}) or {}
        social_candidates = {
            "website": metadata.get("website") or metadata.get("websites"),
            "telegram": metadata.get("telegram") or metadata.get("social") or metadata.get("community"),
            "twitter": metadata.get("twitter") or metadata.get("x") or metadata.get("x_com"),
        }
        report["jetton_info"] = {
            "name": metadata.get("name", "Unknown"),
            "symbol": metadata.get("symbol", "???"),
            "decimals": metadata.get("decimals", "9"),
            "image": metadata.get("image"),
            "website": social_candidates.get("website"),
            "telegram": social_candidates.get("telegram"),
            "twitter": social_candidates.get("twitter"),
            "total_supply": jetton_info.get("total_supply"),
            "mintable": jetton_info.get("mintable"),
            "verification": jetton_info.get("verification", "none"),
            "holders_count": jetton_info.get("holders_count", 0),
        }
        report["holders"] = parse_holders(
            await get_jetton_holders(session, address, limit=10),
            jetton_info.get("total_supply"),
        )
    else:
        report["errors"].append("TonAPI: token not found or API unavailable")

    if dex_pairs:
        report["found"] = True
        total_vol = sum((p.get("volume") or {}).get("h24", 0) for p in dex_pairs)
        total_liq = sum((p.get("liquidity") or {}).get("usd", 0) for p in dex_pairs)
        best = dex_pairs[0]
        dex_info = best.get("info") or {}
        report["dex_data"] = {
            "price_usd": best.get("priceUsd"),
            "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
            "volume_24h": total_vol,
            "market_cap": best.get("marketCap"),
            "fdv": best.get("fdv"),
            "price_change_1h": (best.get("priceChange") or {}).get("h1"),
            "price_change_6h": (best.get("priceChange") or {}).get("h6"),
            "price_change_24h": (best.get("priceChange") or {}).get("h24"),
            "txns_24h": (best.get("txns") or {}).get("h24") or {},
            "pair_address": best.get("pairAddress"),
            "pair_created_at": best.get("pairCreatedAt"),
            "dex_url": best.get("url"),
            "total_liquidity_usd": total_liq,
            "websites": dex_info.get("websites") or [],
            "socials": dex_info.get("socials") or [],
        }
    else:
        report["errors"].append(
            "DexScreener: no DEX pairs found" if dex_pairs == [] else "DexScreener: API request failed"
        )

    try:
        symbol = (report.get("jetton_info") or {}).get("symbol")
        bonding_curve = await get_topblast_bonding_curve(session, address, symbol=symbol)
        if bonding_curve and not bonding_curve.get("bonded", False):
            report["bonding_curve"] = bonding_curve
    except Exception:
        logger.exception("Error loading TopBlast bonding curve data")

    return report


def _fmt_price(price_str) -> str:
    if price_str is None:
        return "N/A"
    try:
        price = float(price_str)
    except (ValueError, TypeError):
        return "N/A"

    if price == 0:
        return "$0"
    if price < 0.000001:
        return f"${price:.12f}"
    if price < 0.0001:
        return f"${price:.9f}"
    if price < 0.01:
        return f"${price:.6f}"
    if price < 1:
        return f"${price:.4f}"
    if price < 1000:
        return f"${price:.2f}"
    return f"${price:,.0f}"


def _fmt_usd(value) -> str:
    if value is None:
        return "N/A"
    try:
        value = float(value)
        if value >= 1_000_000:
            return f"${value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"${value / 1_000:.1f}K"
        return f"${value:.2f}"
    except (ValueError, TypeError):
        return "N/A"


def _fmt_num(value) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):,.0f}"
    except (ValueError, TypeError):
        return "N/A"


def _fmt_pct(value) -> str:
    if value is None:
        return "N/A"
    try:
        value = float(value)
        return f"+{value:.2f}%" if value >= 0 else f"{value:.2f}%"
    except (ValueError, TypeError):
        return "N/A"


def _fmt_age(timestamp_ms) -> str:
    if not timestamp_ms:
        return "N/A"
    try:
        from datetime import datetime
        created = datetime.fromtimestamp(timestamp_ms / 1000)
        delta = datetime.now() - created
        days = delta.days
        if days < 1:
            hours = delta.seconds // 3600
            return f"{hours}h" if hours >= 1 else f"{delta.seconds // 60}m"
        if days < 30:
            return f"{days}d"
        if days < 365:
            return f"{days // 30}mo"
        return f"{days // 365}y"
    except Exception:
        return "N/A"


def _fmt_scan_age(scan_ts: int | None) -> str:
    if not scan_ts:
        return "now"
    delta = max(0, int(time.time()) - int(scan_ts))
    if delta < 60:
        return f"{delta}s"
    if delta < 3600:
        return f"{delta // 60}m"
    if delta < 86400:
        return f"{delta // 3600}h"
    return f"{delta // 86400}d"


def _fmt_username(message: Message) -> str:
    user = message.from_user
    if not user:
        return DEFAULT_SCANNER_LABEL
    if user.username:
        return f"@{user.username}"
    full_name = " ".join(part for part in [user.first_name, user.last_name] if part).strip()
    return full_name or DEFAULT_SCANNER_LABEL


def _fmt_username_from_user(user) -> str:
    if not user:
        return DEFAULT_SCANNER_LABEL
    if getattr(user, "username", None):
        return f"@{user.username}"
    full_name = " ".join(
        part for part in [getattr(user, "first_name", None), getattr(user, "last_name", None)] if part
    ).strip()
    return full_name or DEFAULT_SCANNER_LABEL


def _build_scanner_meta(message: Message, report: dict) -> dict:
    dex = report.get("dex_data") or {}
    return {
        "scanner_name": _fmt_username(message),
        "scanner_id": message.from_user.id if message.from_user else None,
        "scan_price": _fmt_price(dex.get("price_usd")),
        "scan_market_cap": _fmt_usd(dex.get("market_cap")),
        "scan_ts": int(time.time()),
    }


def _build_scanner_meta_from_user(user, report: dict) -> dict:
    dex = report.get("dex_data") or {}
    return {
        "scanner_name": _fmt_username_from_user(user),
        "scanner_id": user.id if user else None,
        "scan_price": _fmt_price(dex.get("price_usd")),
        "scan_market_cap": _fmt_usd(dex.get("market_cap")),
        "scan_ts": int(time.time()),
    }


def _first_url(value) -> str | None:
    if isinstance(value, str):
        v = value.strip()
        return v or None

    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item.strip()
            if isinstance(item, dict):
                for key in ("url", "link", "value"):
                    maybe = item.get(key)
                    if isinstance(maybe, str) and maybe.strip():
                        return maybe.strip()
    return None


def _normalize_url(url: str | None) -> str | None:
    if not url:
        return None

    u = str(url).strip()
    if not u:
        return None

    if u.startswith("@"):
        return f"https://t.me/{u[1:]}"
    if u.startswith("t.me/"):
        return f"https://{u}"
    if u.startswith("x.com/") or u.startswith("twitter.com/"):
        return f"https://{u}"
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return f"https://{u}"


def _collect_links(report: dict) -> list[tuple[str, str]]:
    dex = report.get("dex_data") or {}
    info = report.get("jetton_info") or {}
    address = str(report.get("address") or "").strip()
    links = []

    ds = _normalize_url(dex.get("dex_url"))
    if ds:
        links.append(("DS", ds))

    if address:
        links.append(("TV", f"https://tonviewer.com/{address}"))
        links.append(("GT", f"https://www.geckoterminal.com/ton/tokens/{address}"))

    dex_websites = dex.get("websites") or []
    for item in dex_websites:
        if isinstance(item, dict):
            url = _normalize_url(item.get("url"))
            if url:
                links.append(("WEB", url))

    dex_socials = dex.get("socials") or []
    for item in dex_socials:
        if not isinstance(item, dict):
            continue

        platform = str(item.get("platform") or "").lower().strip()
        handle = str(item.get("handle") or "").strip()
        if not handle:
            continue

        if platform in ("twitter", "x"):
            if handle.startswith("@"):
                handle = handle[1:]
            url = _normalize_url(f"https://x.com/{handle}")
            if url:
                links.append(("X", url))
        elif platform == "telegram":
            if handle.startswith("@"):
                handle = handle[1:]
            url = _normalize_url(f"https://t.me/{handle}")
            if url:
                links.append(("TG", url))
        elif platform in ("website", "web"):
            url = _normalize_url(handle)
            if url:
                links.append(("WEB", url))

    website = _normalize_url(_first_url(info.get("website")))
    telegram = _normalize_url(_first_url(info.get("telegram")))
    twitter = _normalize_url(_first_url(info.get("twitter")))

    if website:
        links.append(("WEB", website))
    if telegram:
        links.append(("TG", telegram))
    if twitter:
        links.append(("X", twitter))

    deduped = []
    seen = set()
    for label, url in links:
        if url not in seen:
            deduped.append((label, url))
            seen.add(url)

    return deduped


def format_token_report(
    report: dict,
    show_info: bool = False,
    show_holders: bool = False,
    scan_history: list[dict] | None = None,
) -> str:
    if not report.get("found"):
        errors = "\n".join(f"- {e}" for e in report.get("errors", []))
        return f"Token not found.\n\nDetails:\n{errors}"

    dex = report.get("dex_data") or {}
    info = report.get("jetton_info") or {}
    holders = report.get("holders") or {}
    bonding = report.get("bonding_curve") or {}
    full_address = html.escape(str(report.get("address", "")))

    h1_value = dex.get("price_change_1h")
    h24_value = dex.get("price_change_24h")

    try:
        h1_positive = float(h1_value) >= 0
    except (ValueError, TypeError):
        h1_positive = True

    try:
        h24_positive = float(h24_value) >= 0
    except (ValueError, TypeError):
        h24_positive = True

    h1_emoji = "💎" if h1_positive else "🩸"
    h24_emoji = "💎" if h24_positive else "🩸"

    lines = [
        f"<b>💎 {html.escape(str(info.get('name', 'Unknown')))}</b>  <b>${html.escape(str(info.get('symbol', '???')))}</b>",
        f"<code>{full_address}</code>",
        "",
        f"├ Holders: <b>{_fmt_num(info.get('holders_count'))}</b>",
        f"└ Age: <b>{html.escape(_fmt_age(dex.get('pair_created_at')))}</b>",
        "",
        "<b>📊 Token Stats</b>",
        f"├ 💰 Price: <b>{html.escape(_fmt_price(dex.get('price_usd')))}</b>",
        f"├ 📊 MC: <b>{html.escape(_fmt_usd(dex.get('market_cap')))}</b>",
        f"├ 📈 Vol: <b>{html.escape(_fmt_usd(dex.get('volume_24h')))}</b>",
        f"├ 💧 LP: <b>{html.escape(_fmt_usd(dex.get('liquidity_usd')))}</b>",
        f"├ {h1_emoji} 1H: <b>{html.escape(_fmt_pct(h1_value))}</b>",
        f"└ {h24_emoji} 24H: <b>{html.escape(_fmt_pct(h24_value))}</b>",
    ]

    if bonding and not bonding.get("bonded", False):
        percent = bonding.get("percent")
        bar = _progress_bar(percent, size=12)

        progress_line = (
            f"├ 🚀 Progress: <code>{html.escape(bar)}</code> <b>{percent:.1f}%</b>"
            if percent is not None
            else "├ 🚀 Progress: <b>N/A</b>"
        )

        lines += [
            "",
            "<b>🧨 Bonding Curve</b>",
            progress_line,
            f"├ 💰 Collected: <b>{html.escape(_fmt_gram(bonding.get('collected_gram')))} GRAM</b>",
            f"├ 🎯 Target: <b>{html.escape(_fmt_gram(bonding.get('target_gram')))} GRAM</b>",
            f"└ ⏳ Left: <b>{html.escape(_fmt_gram(bonding.get('remaining_gram')))} GRAM</b>",
        ]

    links = _collect_links(report)
    if links:
        link_bits = [f'<a href="{html.escape(url)}">{html.escape(label)}</a>' for label, url in links]
        lines += ["", " • ".join(link_bits)]

    if show_info:
        lines += [
            "",
            "<b>ℹ️ Token Info</b>",
            f"├ Mintable: <b>{'Yes' if info.get('mintable') else 'No'}</b>",
            f"└ FDV: <b>{html.escape(_fmt_usd(dex.get('fdv')))}</b>",
        ]

    holder_list = holders.get("holders", [])
    if holder_list and show_holders:
        lines += ["", "<b>👥 Top Holders</b>"]
        for i, h in enumerate(holder_list[:5], 1):
            pct = h.get("percentage")
            pct_str = f"{pct:.2f}%" if pct is not None else "N/A"
            address = str(h.get("address", "")).strip()
            if address:
                wallet_url = f"https://tonviewer.com/{address}"
                pct_display = f'<a href="{html.escape(wallet_url)}">{html.escape(pct_str)}</a>'
            else:
                pct_display = html.escape(pct_str)
            lines.append(f"{i}. {pct_display}")

    first_scan = get_first_scan(report)
    if first_scan:
        lines += ["", "<b>🕰️ Scanned By</b>"]
        scanner_name = html.escape(str(first_scan.get("scanner_name", DEFAULT_SCANNER_LABEL)))
        scan_price = html.escape(str(first_scan.get("scan_price", "N/A")))
        scan_mc = html.escape(str(first_scan.get("scan_market_cap", "N/A")))
        scan_age = html.escape(_fmt_scan_age(first_scan.get("scan_ts")))
        lines.append(f"• {scanner_name} — <b>{scan_price}</b> | MC {scan_mc} | {scan_age} ago")

    lines += ["", "<i>DexScreener + TonAPI · Not financial advice</i>"]
    return "\n".join(lines)


def build_report_keyboard(key: str, show_info: bool, show_holders: bool, has_chart: bool = False):
    builder = InlineKeyboardBuilder()
    row = []

    if has_chart:
        row.append(InlineKeyboardButton(text="📈 Chart", callback_data=f"tg:chart:{key}"))

    row.append(InlineKeyboardButton(text="🔄 Refresh", callback_data=f"tg:refresh:{key}"))
    row.append(
        InlineKeyboardButton(
            text="ℹ️ Info" if not show_info else "✖️ Info",
            callback_data=f"tg:info:{key}",
        )
    )
    row.append(
        InlineKeyboardButton(
            text="👥 Holders" if not show_holders else "✖️ Holders",
            callback_data=f"tg:holders:{key}",
        )
    )
    builder.row(*row)

    builder.row(
        InlineKeyboardButton(text="🟣 RedoTrade", url=REDOTRADE_URL),
        InlineKeyboardButton(text="🔵 DTrade", url=DTRADE_URL),
    )
    return builder.as_markup()


def build_chart_keyboard(key: str, selected_tf: str):
    builder = InlineKeyboardBuilder()
    builder.row(
        *[
            InlineKeyboardButton(
                text=(f"• {CHART_TIMEFRAMES[tf]['label']} •" if tf == selected_tf else CHART_TIMEFRAMES[tf]["label"]),
                callback_data=f"tf:{tf}:{key}",
            )
            for tf in CHART_TIMEFRAME_ORDER
        ]
    )
    builder.row(InlineKeyboardButton(text="◂ Back", callback_data=f"tg:back:{key}"))
    return builder.as_markup()


async def _render_report_message(target_message: Message, key: str):
    entry = REPORT_CACHE.get(key)
    if not entry:
        return

    report = entry["report"]
    entry["scan_history"] = get_scan_history(report)

    text = format_token_report(
        report,
        show_info=entry["show_info"],
        show_holders=entry["show_holders"],
        scan_history=entry["scan_history"],
    )
    keyboard = build_report_keyboard(
        key,
        entry["show_info"],
        entry["show_holders"],
        has_chart=bool((report.get("dex_data") or {}).get("pair_address")),
    )

    image_url = _safe_image_url(report)
    if image_url:
        try:
            await target_message.edit_caption(caption=text, reply_markup=keyboard)
            return
        except Exception:
            pass

    try:
        await target_message.edit_text(text, disable_web_page_preview=True, reply_markup=keyboard)
    except Exception:
        pass


if not TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN is not set!")
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
        "Send a TON jetton contract address or ticker."
    )


@dp.message(F.text)
async def handle_address(message: Message):
    text = message.text.strip()
    status_msg = None

    try:
        async with aiohttp.ClientSession() as session:
            if is_valid_ton_address(text):
                lookup_value = text
                status_msg = await message.answer("Scanning token...")
            elif is_valid_ticker(text):
                status_msg = await message.answer(f"Searching ticker {html.escape(text)}...")
                resolved_address, error_text = await resolve_ticker_to_address(session, text)
                if error_text:
                    await status_msg.edit_text(error_text)
                    return
                lookup_value = resolved_address
                await status_msg.edit_text("Scanning token...")
            else:
                return

            report = await scan_token(session, lookup_value)

            if not report.get("found"):
                await status_msg.edit_text(
                    format_token_report(report),
                    disable_web_page_preview=True,
                )
                return

            scanner_meta = _build_scanner_meta(message, report)
            key = _cache_report(report, scanner_meta=scanner_meta)
            history = get_scan_history(report)

            result = format_token_report(
                report,
                show_info=False,
                show_holders=False,
                scan_history=history,
            )
            keyboard = build_report_keyboard(
                key,
                show_info=False,
                show_holders=False,
                has_chart=bool((report.get("dex_data") or {}).get("pair_address")),
            )
            chart_photo = await build_scan_photo(session, report)
            image_url = _safe_image_url(report)

            if status_msg:
                try:
                    await status_msg.delete()
                except Exception:
                    pass

            if chart_photo:
                try:
                    await message.answer_photo(
                        photo=chart_photo,
                        caption=result,
                        reply_markup=keyboard,
                    )
                except Exception:
                    logger.exception("Error sending chart card, falling back")
                    chart_photo = None

            if not chart_photo and image_url:
                try:
                    await message.answer_photo(
                        photo=image_url,
                        caption=result,
                        reply_markup=keyboard,
                    )
                except Exception:
                    await message.answer(
                        result,
                        disable_web_page_preview=True,
                        reply_markup=keyboard,
                    )
            elif not chart_photo and not image_url:
                await message.answer(
                    result,
                    disable_web_page_preview=True,
                    reply_markup=keyboard,
                )

    except Exception as e:
        logger.exception("Error scanning token")
        if status_msg:
            try:
                await status_msg.edit_text(f"Error scanning token: {html.escape(str(e))}")
                return
            except Exception:
                pass

        await message.answer(f"Error scanning token: {html.escape(str(e))}")


@dp.callback_query(F.data.startswith("tg:"))
async def handle_toggle(callback: CallbackQuery):
    try:
        _, section, key = callback.data.split(":", 2)
    except ValueError:
        await callback.answer()
        return

    entry = REPORT_CACHE.get(key)
    if not entry:
        await callback.answer("This report has expired — please scan the token again.", show_alert=True)
        return

    entry["ts"] = time.time()

    if section == "refresh":
        address = str(entry["report"].get("address") or "").strip()
        if not address:
            await callback.answer("No token address found to refresh.", show_alert=True)
            return

        await callback.answer("Refreshing price...")

        try:
            async with aiohttp.ClientSession() as session:
                fresh_report = await scan_token(session, address)

            if not fresh_report.get("found"):
                await callback.answer("Couldn't refresh token data right now.", show_alert=True)
                return

            scanner_meta = _build_scanner_meta_from_user(callback.from_user, fresh_report)
            save_scan_history(fresh_report, scanner_meta)

            entry["report"] = fresh_report
            entry["scanner_meta"] = scanner_meta
            entry["has_image"] = bool(_safe_image_url(fresh_report))
            entry["scan_history"] = get_scan_history(fresh_report)
            entry["ts"] = time.time()

            text = format_token_report(
                fresh_report,
                show_info=entry["show_info"],
                show_holders=entry["show_holders"],
                scan_history=entry["scan_history"],
            )
            keyboard = build_report_keyboard(
                key,
                entry["show_info"],
                entry["show_holders"],
                has_chart=bool((fresh_report.get("dex_data") or {}).get("pair_address")),
            )

            async with aiohttp.ClientSession() as session:
                chart_photo = await build_scan_photo(session, fresh_report)

            if chart_photo:
                try:
                    media = InputMediaPhoto(media=chart_photo, caption=text)
                    await callback.message.edit_media(media=media, reply_markup=keyboard)
                    return
                except Exception:
                    logger.exception("Error updating chart card on refresh, falling back")

            await _render_report_message(callback.message, key)
            return
        except Exception:
            logger.exception("Error refreshing token report")
            await callback.answer("Refresh failed. Try again in a moment.", show_alert=True)
            return

    if section == "chart":
        entry["chart_tf"] = entry.get("chart_tf", DEFAULT_CHART_TIMEFRAME)
        await _send_chart(callback, key, entry["chart_tf"])
        return

    if section == "back":
        entry["scan_history"] = get_scan_history(entry["report"])
        text = format_token_report(
            entry["report"],
            show_info=entry["show_info"],
            show_holders=entry["show_holders"],
            scan_history=entry["scan_history"],
        )
        keyboard = build_report_keyboard(
            key,
            entry["show_info"],
            entry["show_holders"],
            has_chart=bool((entry["report"].get("dex_data") or {}).get("pair_address")),
        )
        image_url = _safe_image_url(entry["report"])

        async with aiohttp.ClientSession() as session:
            chart_photo = await build_scan_photo(session, entry["report"])

        try:
            await callback.message.delete()
        except Exception:
            pass

        if chart_photo:
            try:
                await callback.message.answer_photo(
                    photo=chart_photo,
                    caption=text,
                    reply_markup=keyboard,
                )
                await callback.answer()
                return
            except Exception:
                logger.exception("Error sending chart card on back, falling back")
                chart_photo = None

        if image_url:
            try:
                await callback.message.answer_photo(
                    photo=image_url,
                    caption=text,
                    reply_markup=keyboard,
                )
            except Exception:
                await callback.message.answer(
                    text,
                    disable_web_page_preview=True,
                    reply_markup=keyboard,
                )
        else:
            await callback.message.answer(
                text,
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )

        await callback.answer()
        return

    if section == "info":
        entry["show_info"] = not entry["show_info"]
    elif section == "holders":
        entry["show_holders"] = not entry["show_holders"]

    await _render_report_message(callback.message, key)
    await callback.answer()


async def _send_chart(callback: CallbackQuery, key: str, timeframe: str):
    entry = REPORT_CACHE.get(key)
    if not entry:
        await callback.answer("This report has expired — please scan the token again.", show_alert=True)
        return

    pool_address = (entry["report"].get("dex_data") or {}).get("pair_address")
    symbol = (entry["report"].get("jetton_info") or {}).get("symbol", "???")

    if not pool_address:
        await callback.answer("No chart available for this token.", show_alert=True)
        return

    await callback.answer("Loading chart...")

    async with aiohttp.ClientSession() as session:
        ohlcv = await get_ohlcv(session, pool_address, timeframe)

    if not ohlcv:
        await callback.answer("Couldn't load chart data for this timeframe.", show_alert=True)
        return

    png_bytes = build_candlestick_chart(
        ohlcv,
        symbol,
        CHART_TIMEFRAMES[timeframe]["label"],
    )

    media = InputMediaPhoto(
        media=BufferedInputFile(png_bytes, filename="chart.png"),
        caption=f"<b>{html.escape(str(symbol))}</b> price chart",
    )
    keyboard = build_chart_keyboard(key, timeframe)

    try:
        await callback.message.edit_media(media=media, reply_markup=keyboard)
    except Exception:
        await callback.message.answer_photo(
            photo=BufferedInputFile(png_bytes, filename="chart.png"),
            caption=f"<b>{html.escape(str(symbol))}</b> price chart",
            reply_markup=keyboard,
        )

    entry["chart_tf"] = timeframe
    entry["ts"] = time.time()


@dp.callback_query(F.data.startswith("tf:"))
async def handle_timeframe(callback: CallbackQuery):
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
    entry["chart_tf"] = timeframe

    pool_address = (entry["report"].get("dex_data") or {}).get("pair_address")
    symbol = (entry["report"].get("jetton_info") or {}).get("symbol", "???")

    if not pool_address or timeframe not in CHART_TIMEFRAMES:
        await callback.answer()
        return

    await callback.answer("Loading chart...")

    async with aiohttp.ClientSession() as session:
        ohlcv = await get_ohlcv(session, pool_address, timeframe)

    if not ohlcv:
        await callback.answer("Couldn't load chart data for that timeframe.", show_alert=True)
        return

    png_bytes = build_candlestick_chart(
        ohlcv,
        symbol,
        CHART_TIMEFRAMES[timeframe]["label"],
    )

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
    init_db()
    logger.info("Starting TON Meme Token Scanner bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
