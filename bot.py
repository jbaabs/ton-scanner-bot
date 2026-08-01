import os
import re
import html
import time
import uuid
import asyncio
import logging

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
GECKO_BASE = "https://api.geckoterminal.com/api/v2"

CHART_TIMEFRAMES = {
    "5m": {"timeframe": "minute", "aggregate": 5, "limit": 48, "label": "5m"},
    "1h": {"timeframe": "hour", "aggregate": 1, "limit": 48, "label": "1H"},
    "4h": {"timeframe": "hour", "aggregate": 4, "limit": 42, "label": "4H"},
    "1d": {"timeframe": "day", "aggregate": 1, "limit": 30, "label": "1D"},
}
CHART_ORDER = ["5m", "1h", "4h", "1d"]
DEFAULT_TF = "1h"
REQUEST_TIMEOUT = 15

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ton-scanner-bot")

ADDR_FRIENDLY_RE = re.compile(r"^[EU]Q[A-Za-z0-9_\-]{46}$")
ADDR_RAW_RE = re.compile(r"^(0|[-1]):[0-9a-fA-F]{64}$")

REPORT_CACHE = {}
REPORT_TTL = 3600


def is_valid_ton_address(text: str) -> bool:
    text = text.strip()
    return bool(ADDR_FRIENDLY_RE.match(text) or ADDR_RAW_RE.match(text))


def prune_cache():
    now = time.time()
    dead = [k for k, v in REPORT_CACHE.items() if now - v["ts"] > REPORT_TTL]
    for k in dead:
        REPORT_CACHE.pop(k, None)


def cache_report(report: dict) -> str:
    prune_cache()
    key = uuid.uuid4().hex[:12]
    REPORT_CACHE[key] = {
        "report": report,
        "show_info": False,
        "show_holders": False,
        "chart_tf": DEFAULT_TF,
        "ts": time.time(),
    }
    return key


def tonapi_headers():
    headers = {"Accept": "application/json"}
    if TONAPI_KEY:
        headers["Authorization"] = f"Bearer {TONAPI_KEY}"
    return headers


async def get_dex_data(session, address):
    url = f"{DEXSCREENER_API}/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            pairs = data.get("pairs")
            if not pairs:
                retur
