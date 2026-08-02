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
# Public, unauthenticated DeDust backend. This is the same backend that powers
# x1000.finance's "Uranus" memepad AND topblast.lol (both are dedust_v3_memepad
# bonding-curve launches under the hood), so any TON jetton launched through
# either platform shows up here with live bonding-curve numbers.
DEDUST_API_BASE = "https://mainnet.api.dedust.io/v4/api"
# nanoTON -> TON/GRAM
NANO = 1_000_000_000
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
        SELECT id, scanner_id, scanner_name, scan_price, scan_market_cap, scan_ts
        FROM scan_history
        WHERE token_key = ?
        ORDER BY scan_ts ASC, id ASC
        LIMIT 1
        """,
        (token_key,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def _is_missing_value(value) -> bool:
    return value is None or str(value).strip() in ("", "N/A")


def backfill_first_scan_prices(row_id: int, scan_price: str, scan_market_cap: str) -> None:
    """Patch a scan_history row that was saved before DexScreener had indexed the
    pair yet (very new tokens), so the 'Scanned By' line stops showing N/A forever."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "UPDATE scan_history SET scan_price = ?, scan_market_cap = ? WHERE id = ?",
        (scan_price, scan_market_cap, row_id),
    )
    conn.commit()
    conn.close()


def get_first_scan_resolved(report: dict) -> dict | None:
    """Like get_first_scan, but self-heals rows whose price/MC were N/A at save time
    by filling them in from the current live report (and persists the fix)."""
    first_scan = get_first_scan(report)
    if not first_scan:
        return None

    price_missing = _is_missing_value(first_scan.get("scan_price"))
    mc_missing = _is_missing_value(first_scan.get("scan_market_cap"))
    if not (price_missing or mc_missing):
        return first_scan

    dex = report.get("dex_data") or {}
    fresh_price = _fmt_price(dex.get("price_usd"))
    fresh_mc = _fmt_usd(dex.get("market_cap"))

    updated = False
    if price_missing and not _is_missing_value(fresh_price):
        first_scan["scan_price"] = fresh_price
        updated = True
    if mc_missing and not _is_missing_value(fresh_mc):
        first_scan["scan_market_cap"] = fresh_mc
        updated = True

    if updated and first_scan.get("id") is not None:
        backfill_first_scan_prices(
            first_scan["id"], first_scan.get("scan_price"), first_scan.get("scan_market_cap")
        )

    return first_scan


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


def _friendly_to_raw(friendly: str) -> str | None:
    """Decode an EQ.../UQ... friendly TON address into raw 'workchain:hex' form
    (e.g. '0:3af62d7f...'), which is what DeDust's API expects."""
    try:
        addr = friendly.strip()
        padded = addr + "=" * (-len(addr) % 4)
        raw_bytes = base64.urlsafe_b64decode(padded)
    