"""
TON Meme Token Scanner Bot — Single File Version
================================================
A Telegram bot that scans TON meme tokens by contract address.

Setup:
  1. Get a bot token from @BotFather
  2. Set the TELEGRAM_BOT_TOKEN environment variable
  3. pip install aiogram aiohttp python-dotenv matplotlib
  4. python bot.py
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

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TONAPI_KEY = os.getenv("TONAPI_KEY", "")
DEBUG = os.getenv("DEBUG", "0") == "1"

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens"
TONAPI_BASE = "https://tonapi.io/v2"
GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"

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

REPORT_CACHE: dict[str, dict] = {}
REPORT_CACHE_TTL = 60 * 60


def _cache_report(report: dict) -> str:
    _prune_report_cache()
    key = uuid.uuid4().hex[:12]
    REPORT_CACHE[key] = {
        "report": report,
        "show_info": False,
        "show_holders": False,
        "view": "report",
        "chart_tf": DEFAULT_CHART_TIMEFRAME,
        "ts": time.time(),
    }
    return key


def _prune_report_cache():
    now = time.time()
    expired = [k for k, v in REPORT_CACHE.items() if now - v["ts"] > REPORT_CACHE_TTL]
    for k in expired:
        REPORT_CACHE.pop(k, None)


ADDR_FRIENDLY_RE = re.compile(r"^[EU]Q[A-Za-z0-9_\-]{46}$")
ADDR_RAW_RE = re.compile(r"^(0|[-1]):[0-9a-fA-F]{64}$")


def is_valid_ton_address(text: str) -> bool:
    text = text.strip()
    return bool(ADDR_FRIENDLY_RE.match(text) or ADDR_RAW_RE.match(text))


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


async def get_dex_data(session: aiohttp.ClientSession, address: str) -> list[dict] | None:
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
                return 

