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
    ReplyParameters,
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
TON_STREAM_WS = os.getenv("TON_STREAM_WS", "wss://tonapi.io/streaming/v2/ws")
LIVE_STREAM_ENABLED = os.getenv("GRX_LIVE_STREAM", "1") == "1"
LIVE_POOL_ADDRESSES: set[str] = set()
LIVE_POOL_LAST_EVENT: dict[str, float] = {}
LIVE_POOL_EVENT_COUNT: dict[str, int] = {}
LIVE_STREAM_RESUBSCRIBE = asyncio.Event()
LIVE_STREAM_CONNECTED = False
LIVE_SWAPS: dict[str, list[dict]] = {}
LIVE_SWAP_MAX_PER_POOL = 800
GRX_CANDLE_BOOK: dict[tuple[str, str], list[list]] = {}
GRX_CANDLE_BOOK_MAX = 500
LAUNCHPAD_TRADE_CACHE: dict[str, tuple[float, list[dict]]] = {}
LAUNCHPAD_TRADE_CACHE_TTL = 2.0

def _record_live_swap(pool: str, *, ts: int, price=None, amount_in=None, amount_out=None, side=None, source="ton"):
    if not pool: return
    b=LIVE_SWAPS.setdefault(pool,[])
    b.append({"ts":int(ts or time.time()),"price":price,"amount_in":amount_in,"amount_out":amount_out,"side":side,"source":source})
    if len(b)>LIVE_SWAP_MAX_PER_POOL: del b[:-LIVE_SWAP_MAX_PER_POOL]
    _invalidate_live_pool_caches(pool)
    _apply_swap_to_all_live_timeframes(pool, b[-1])

def _live_swap_counts(pool: str, seconds: int=30):
    cutoff=time.time()-seconds; buys=sells=0
    for s in LIVE_SWAPS.get(pool,()):
        if s.get("ts",0)<cutoff: continue
        buys += s.get("side")=="buy"; sells += s.get("side")=="sell"
    return buys,sells
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
GBOT_URL = os.getenv("GBOT_URL", "https://t.me/groypfi_bot?start=ref_5580192046")
DEFAULT_SCANNER_LABEL = os.getenv("DEFAULT_SCANNER_LABEL", "Scanner")

# Telegram custom emoji IDs from the NoNameDev pack.
CUSTOM_EMOJI = {
    "sell": "5334626372662894506",
    "buy": "5336975625284529400",
    "dexscreener": "5391144822268537893",
    "dextools": "5390808947236051353",
    "redotrade": "5190690624025696894",
    "dtrade": "5188545889156769764",
    "gbot": "5393374541655352818",
    "dedust": "5391224493911876583",
    "stonfi": "6255601767631822279",
    "percent": "5366387582802369247",
    "social": "5366198685845729399",
    "wallet": "5363914626532677595",
    "price": "5364283757496930929",
    "mcap": "5366561632057075362",
    "holders": "5364342783232481866",
    "liquidity": "5363840130324927385",
    "age": "5366367898967253063",
    "ath": "5366516161238308466",
    "uranus": "6294313725209877707",
    "gram": "6151926179138904505",
    "coingecko": "6256011039360426071",
    "groypfi": "6305307926659605381",
    "topblast": "6118187720675173301",
}

def _ce(name: str, fallback: str) -> str:
    """Render a Telegram custom emoji using a Telegram-safe emoji fallback."""
    emoji_id = CUSTOM_EMOJI.get(name)
    if not emoji_id:
        return fallback
    safe_fallbacks = {
        "sell": "🔴", "buy": "🟢", "dexscreener": "📊",
        "dextools": "📈", "redotrade": "🟣", "dtrade": "🔵",
        "gbot": "🤖", "dedust": "💎", "stonfi": "💎",
        "percent": "💯", "social": "💬", "wallet": "👛",
        "price": "💰", "mcap": "📈", "holders": "👥",
        "liquidity": "💧", "age": "🌱", "ath": "🏆", "uranus": "🪐", "gram": "💎",
        "coingecko": "🦎", "groypfi": "🟣", "topblast": "🚀",
    }
    safe = safe_fallbacks.get(name, "✨")
    return f'<tg-emoji emoji-id="{emoji_id}">{safe}</tg-emoji>'

def _linked_ce(name: str, fallback: str, url: str) -> str:
    # Keep custom-emoji and hyperlink entities separate for Telegram compatibility.
    return _ce(name, fallback)

def _short_address(address: str, left: int = 5, right: int = 5) -> str:
    address = str(address or "").strip()
    if len(address) <= left + right + 3:
        return address
    return f"{address[:left]}...{address[-right:]}"

def _custom_icon_button(text: str, url: str, emoji_name: str | None = None):
    """Use Telegram's button custom-emoji icon when supported by installed aiogram."""
    kwargs = {"text": text, "url": url}
    emoji_id = CUSTOM_EMOJI.get(emoji_name or "")
    try:
        fields = getattr(InlineKeyboardButton, "model_fields", {})
        if emoji_id and "icon_custom_emoji_id" in fields:
            kwargs["icon_custom_emoji_id"] = emoji_id
    except Exception:
        pass
    return InlineKeyboardButton(**kwargs)

CHART_TIMEFRAMES = {
    "1m": {"timeframe": "minute", "aggregate": 1, "limit": 120, "label": "1m"},
    "5m": {"timeframe": "minute", "aggregate": 5, "limit": 60, "label": "5m"},
    "15m": {"timeframe": "minute", "aggregate": 15, "limit": 60, "label": "15m"},
    "30m": {"timeframe": "minute", "aggregate": 30, "limit": 60, "label": "30m"},
    "1h": {"timeframe": "hour", "aggregate": 1, "limit": 48, "label": "1H"},
    "4h": {"timeframe": "hour", "aggregate": 4, "limit": 42, "label": "4H"},
    "1d": {"timeframe": "day", "aggregate": 1, "limit": 30, "label": "1D"},
    "4d": {"timeframe": "day", "aggregate": 4, "limit": 30, "label": "4D"},
}
CHART_TIMEFRAME_ORDER = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "4d"]
DEFAULT_CHART_TIMEFRAME = "1m"
REQUEST_TIMEOUT = 15
REPORT_CACHE: dict[str, dict] = {}
REPORT_CACHE_TTL = 60 * 60

# Lightweight hot caches: reduce repeated API/image work while keeping market data fresh.
OHLCV_CACHE: dict[tuple, tuple[float, list]] = {}
OHLCV_CACHE_TTL = 12
IMAGE_CACHE: dict[str, tuple[float, bytes]] = {}
IMAGE_CACHE_TTL = 60 * 60
CHART_POOL_CACHE: dict[str, tuple[float, str]] = {}
CHART_POOL_CACHE_TTL = 5 * 60
REFRESH_LOCKS: dict[str, asyncio.Lock] = {}
FAST_MARKET_CACHE: dict[str, tuple[float, list]] = {}
FAST_MARKET_CACHE_TTL = 2.0
JETTON_INFO_CACHE: dict[str, tuple[float, dict]] = {}
JETTON_INFO_CACHE_TTL = 10 * 60
HOLDERS_CACHE: dict[str, tuple[float, object]] = {}
HOLDERS_CACHE_TTL = 45
ATH_CACHE: dict[str, tuple[float, object]] = {}
ATH_CACHE_TTL = 15 * 60
SCAN_INFLIGHT: dict[str, asyncio.Task] = {}
TOKEN_STATE_CACHE: dict[str, tuple[float, dict]] = {}
TOKEN_STATE_TTL = max(2.0, float(os.getenv("TOKEN_STATE_TTL", "6")))
TOKEN_STATE_STALE_TTL = max(TOKEN_STATE_TTL, float(os.getenv("TOKEN_STATE_STALE_TTL", "45")))
TOKEN_STATE_LOCKS: dict[str, asyncio.Lock] = {}
PERF_ENABLED = os.getenv("GRX_PERF_LOG", "0") == "1"

def _perf_log(label: str, started: float) -> None:
    if PERF_ENABLED:
        logger.info("PERF %-24s %.0fms", label, (time.perf_counter() - started) * 1000)

def _token_state_get(address: str, ttl: float = TOKEN_STATE_TTL):
    item=TOKEN_STATE_CACHE.get(str(address or "").strip())
    if not item: return None
    ts,report=item
    return report if time.monotonic()-ts <= ttl else None

def _token_state_put(address: str, report: dict):
    key=str(address or "").strip()
    if key and report:
        TOKEN_STATE_CACHE[key]=(time.monotonic(),report)
        if len(TOKEN_STATE_CACHE)>1000:
            cutoff=time.monotonic()-TOKEN_STATE_STALE_TTL
            for k,(ts,_) in list(TOKEN_STATE_CACHE.items()):
                if ts<cutoff:
                    TOKEN_STATE_CACHE.pop(k,None); TOKEN_STATE_LOCKS.pop(k,None)
    return report

def _token_state_lock(address: str):
    key=str(address or "").strip()
    lock=TOKEN_STATE_LOCKS.get(key)
    if lock is None:
        lock=TOKEN_STATE_LOCKS[key]=asyncio.Lock()
    return lock

RENDER_SEMAPHORE = asyncio.Semaphore(3)

def _refresh_lock(key: str) -> asyncio.Lock:
    lock = REFRESH_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        REFRESH_LOCKS[key] = lock
    return lock

def _ttl_get(cache: dict, key, ttl: float):
    item = cache.get(key)
    if not item:
        return None
    ts, value = item
    if time.time() - ts > ttl:
        cache.pop(key, None)
        return None
    return value

def _ttl_put(cache: dict, key, value):
    cache[key] = (time.time(), value)
    # Keep these tiny caches bounded on long-running workers.
    if len(cache) > 600:
        oldest = sorted(cache.items(), key=lambda kv: kv[1][0])[:100]
        for old_key, _ in oldest:
            cache.pop(old_key, None)
MAX_SCAN_HISTORY = 12
DUPLICATE_SCAN_COOLDOWN = int(os.getenv("DUPLICATE_SCAN_COOLDOWN", "600"))
ALERT_CHECK_SECONDS = max(30, int(os.getenv("ALERT_CHECK_SECONDS", "60")))
PENDING_ALERT_INPUT: dict[int, dict] = {}
GRX_LOGO_B64 = "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"

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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS token_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token_key TEXT NOT NULL,
            snapshot_ts INTEGER NOT NULL,
            price_usd REAL,
            market_cap REAL,
            liquidity_usd REAL,
            volume_24h REAL,
            volume_1h REAL,
            volume_5m REAL,
            holders_count INTEGER,
            top10_pct REAL,
            buys_5m INTEGER,
            sells_5m INTEGER,
            buys_1h INTEGER,
            sells_1h INTEGER
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_snapshot_token ON token_snapshots(token_key, snapshot_ts DESC)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS token_watches (
            user_id INTEGER NOT NULL, token_address TEXT NOT NULL, token_symbol TEXT, created_ts INTEGER NOT NULL,
            PRIMARY KEY (user_id, token_address)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS token_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, token_address TEXT NOT NULL,
            token_symbol TEXT, alert_type TEXT NOT NULL, threshold REAL, baseline REAL, active INTEGER NOT NULL DEFAULT 1,
            created_ts INTEGER NOT NULL, triggered_ts INTEGER
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_active_alerts ON token_alerts(active, token_address)")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recent_chat_scans (
            chat_id INTEGER NOT NULL,
            token_key TEXT NOT NULL,
            scan_ts INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            chat_username TEXT,
            PRIMARY KEY (chat_id, token_key)
        )
        """
    )
    conn.commit()
    conn.close()



def _parse_money_target(text: str) -> float | None:
    raw = str(text or "").strip().upper().replace("$", "").replace(",", "")
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([KMB]?)", raw)
    if not m:
        return None
    value = float(m.group(1))
    return value * {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}[m.group(2)]

def _watching(user_id: int, address: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        return conn.execute("SELECT 1 FROM token_watches WHERE user_id=? AND token_address=?", (user_id, address)).fetchone() is not None

def _toggle_watch(user_id: int, address: str, symbol: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        exists = conn.execute("SELECT 1 FROM token_watches WHERE user_id=? AND token_address=?", (user_id, address)).fetchone()
        if exists:
            conn.execute("DELETE FROM token_watches WHERE user_id=? AND token_address=?", (user_id, address)); return False
        conn.execute("INSERT OR REPLACE INTO token_watches(user_id,token_address,token_symbol,created_ts) VALUES(?,?,?,?)", (user_id,address,symbol,int(time.time()))); return True

def _create_alert(user_id: int, address: str, symbol: str, alert_type: str, threshold: float | None = None, baseline: float | None = None):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("INSERT INTO token_alerts(user_id,token_address,token_symbol,alert_type,threshold,baseline,created_ts) VALUES(?,?,?,?,?,?,?)", (user_id,address,symbol,alert_type,threshold,baseline,int(time.time())))

def _money(v: float | None) -> str:
    if v is None: return "N/A"
    if v >= 1_000_000_000: return f"${v/1_000_000_000:.2f}B"
    if v >= 1_000_000: return f"${v/1_000_000:.2f}M"
    if v >= 1_000: return f"${v/1_000:.1f}K"
    if v >= 1: return f"${v:.4f}"
    return f"${v:.8g}"

def build_alert_keyboard(key: str):
    b=InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📈 Price Above",callback_data=f"al:pa:{key}"), InlineKeyboardButton(text="📉 Price Below",callback_data=f"al:pb:{key}"))
    b.row(InlineKeyboardButton(text="💰 MCap Above",callback_data=f"al:ma:{key}"), InlineKeyboardButton(text="💸 MCap Below",callback_data=f"al:mb:{key}"))
    b.row(InlineKeyboardButton(text="🚀 New ATH",callback_data=f"al:ath:{key}"))
    b.row(InlineKeyboardButton(text="◂ Back to Scan",callback_data=f"tg:back:{key}"))
    return b.as_markup()

GRX_WATERMARK_B64 = """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"""
GRX_WATERMARK_STICKER_ID = "CAACAgEAAxkBAAFRBllqcjxUiEFJQZYhlokbMbfzAAGzAnMAAuQGAAIQylFFkYAhMmNX2N89BA"
GRX_STICKER_WATERMARK_BYTES = None


async def _get_grx_sticker_watermark_bytes(bot) -> bytes | None:
    """Download the static GRX Telegram sticker once and cache it in memory."""
    global GRX_STICKER_WATERMARK_BYTES
    if GRX_STICKER_WATERMARK_BYTES:
        return GRX_STICKER_WATERMARK_BYTES
    if bot is None:
        return None
    try:
        tg_file = await bot.get_file(GRX_WATERMARK_STICKER_ID)
        buf = BytesIO()
        await bot.download_file(tg_file.file_path, destination=buf)
        GRX_STICKER_WATERMARK_BYTES = buf.getvalue()
        return GRX_STICKER_WATERMARK_BYTES
    except Exception:
        logger.exception("Could not download GRX sticker watermark")
        return None


async def alert_watcher():
    """Collect snapshots for watched/alerted tokens and evaluate active alerts."""
    await asyncio.sleep(5)
    while True:
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                alert_rows = conn.execute(
                    "SELECT * FROM token_alerts WHERE active=1 ORDER BY token_address"
                ).fetchall()
                watch_rows = conn.execute(
                    "SELECT DISTINCT token_address FROM token_watches"
                ).fetchall()

            by_addr = {}
            for row in alert_rows:
                by_addr.setdefault(row["token_address"], []).append(row)

            # One background scan per unique token. Watched tokens collect GRX
            # snapshots even when they have no active alert.
            addresses = set(by_addr)
            addresses.update(
                row["token_address"] for row in watch_rows if row["token_address"]
            )

            async with aiohttp.ClientSession() as session:
                for address in addresses:
                    try:
                        report = await scan_token(session, address)
                        _token_state_put(address, report)
                        _register_report_live_pool(report)
                        save_token_snapshot(report)
                    except Exception:
                        logger.exception("Background GRX snapshot failed for %s", address)
                        continue

                    dex = report.get("dex_data") or {}
                    price = _as_float(dex.get("price_usd"))
                    mcap = _as_float(dex.get("market_cap"))
                    ath = _as_float(dex.get("ath_market_cap"))

                    for a in by_addr.get(address, []):
                        typ = a["alert_type"]
                        target = a["threshold"]
                        hit = False
                        current = None
                        if typ == "price_above" and price is not None:
                            hit, current = price >= target, price
                        elif typ == "price_below" and price is not None:
                            hit, current = price <= target, price
                        elif typ == "mcap_above" and mcap is not None:
                            hit, current = mcap >= target, mcap
                        elif typ == "mcap_below" and mcap is not None:
                            hit, current = mcap <= target, mcap
                        elif typ == "new_ath" and mcap is not None:
                            hit, current = mcap > (a["baseline"] or ath or float("inf")), mcap

                        if hit:
                            label = {
                                "price_above": "Price crossed above",
                                "price_below": "Price crossed below",
                                "mcap_above": "Market cap crossed above",
                                "mcap_below": "Market cap crossed below",
                                "new_ath": "New ATH reached",
                            }[typ]
                            target_txt = _money(target) if target is not None else ""
                            try:
                                await bot.send_message(
                                    a["user_id"],
                                    f"🚨 <b>GRX Alert</b>\n\n"
                                    f"<b>{html.escape(a['token_symbol'] or 'Token')}</b>\n"
                                    f"{label}{(' <b>'+target_txt+'</b>') if target_txt else ''}\n"
                                    f"Current: <b>{_money(current)}</b>\n\n"
                                    f"<code>{html.escape(address)}</code>",
                                )
                            except Exception:
                                logger.exception("Could not deliver alert to %s", a["user_id"])
                            with sqlite3.connect(DB_PATH) as conn:
                                conn.execute(
                                    "UPDATE token_alerts SET active=0, triggered_ts=? WHERE id=?",
                                    (int(time.time()), a["id"]),
                                )
                    await asyncio.sleep(.25)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Background GRX collector cycle failed")
        await asyncio.sleep(ALERT_CHECK_SECONDS)

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



def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def save_token_snapshot(report: dict) -> None:
    token_key = _history_key(report)
    if not token_key:
        return

    dex = report.get("dex_data") or {}
    info = report.get("jetton_info") or {}
    holders = report.get("holders") or {}
    tx5 = dex.get("txns_5m") or {}
    tx1 = dex.get("txns_1h") or {}

    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO token_snapshots (
            token_key, snapshot_ts, price_usd, market_cap, liquidity_usd,
            volume_24h, volume_1h, volume_5m, holders_count, top10_pct,
            buys_5m, sells_5m, buys_1h, sells_1h
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            token_key,
            int(time.time()),
            _as_float(dex.get("price_usd")),
            _as_float(dex.get("market_cap")),
            _as_float(dex.get("liquidity_usd")),
            _as_float(dex.get("volume_24h")),
            _as_float(dex.get("volume_1h")),
            _as_float(dex.get("volume_5m")),
            int(info.get("holders_count") or 0),
            _as_float(holders.get("top_concentration")),
            int(tx5.get("buys") or 0),
            int(tx5.get("sells") or 0),
            int(tx1.get("buys") or 0),
            int(tx1.get("sells") or 0),
        ),
    )
    # Keep roughly the latest week of observations.
    conn.execute(
        "DELETE FROM token_snapshots WHERE snapshot_ts < ?",
        (int(time.time()) - 7 * 86400,),
    )
    conn.commit()
    conn.close()


def get_snapshot_near(report: dict, seconds_ago: int) -> dict | None:
    token_key = _history_key(report)
    if not token_key:
        return None
    target = int(time.time()) - seconds_ago
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT * FROM token_snapshots
        WHERE token_key = ? AND snapshot_ts <= ?
        ORDER BY snapshot_ts DESC
        LIMIT 1
        """,
        (token_key, target),
    ).fetchone()
    conn.close()
    return dict(row) if row else None



def get_snapshot_after(report: dict, seconds_ago: int) -> dict | None:
    """Nearest snapshot to the requested rolling-window boundary."""
    token_key = _history_key(report)
    if not token_key:
        return None
    target = int(time.time()) - seconds_ago
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT * FROM token_snapshots
        WHERE token_key = ?
        ORDER BY ABS(snapshot_ts - ?) ASC
        LIMIT 1
        """,
        (token_key, target),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_ath_mcap_since(report: dict, since_ts: int | None) -> float | None:
    token_key = _history_key(report)
    if not token_key or not since_ts:
        return None
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        """
        SELECT MAX(market_cap) FROM token_snapshots
        WHERE token_key = ? AND snapshot_ts >= ? AND market_cap IS NOT NULL
        """,
        (token_key, int(since_ts)),
    ).fetchone()
    conn.close()
    snap_max = _as_float(row[0]) if row else None
    current = _as_float((report.get("dex_data") or {}).get("market_cap"))
    vals = [v for v in (snap_max, current) if v is not None]
    return max(vals) if vals else None


def _live_trade_metrics(report: dict, seconds: int = 300) -> dict:
    """Use GRX-observed swaps only when side + usable notional are explicit.
    Never fabricate money-flow from transaction counts."""
    dex = report.get("dex_data") or {}
    pool = dex.get("chart_pair_address") or dex.get("pair_address")
    if not pool:
        return {"trades": None, "buy_pressure": None, "net_flow": None}
    cutoff = time.time() - seconds
    buys = sells = 0
    buy_value = sell_value = 0.0
    valued = 0
    for s in LIVE_SWAPS.get(pool, ()):
        if s.get("ts", 0) < cutoff:
            continue
        side = s.get("side")
        if side == "buy":
            buys += 1
        elif side == "sell":
            sells += 1
        # Only value a swap when decoder supplied an explicit price and amount.
        # amount_out/in asset identity is not guaranteed, so use notional only
        # when a positive explicit price exists and a positive amount is present.
        price = _as_float(s.get("price"))
        amount = _as_float(s.get("amount_out")) or _as_float(s.get("amount_in"))
        if side in ("buy", "sell") and price and price > 0 and amount and amount > 0:
            notional = price * amount
            if 0 < notional < 1e12:
                valued += 1
                if side == "buy":
                    buy_value += notional
                else:
                    sell_value += notional
    trades = buys + sells
    pressure = (buy_value / (buy_value + sell_value) * 100.0) if valued and (buy_value + sell_value) > 0 else None
    flow = (buy_value - sell_value) if valued else None
    return {"trades": trades if trades else None, "buy_pressure": pressure, "net_flow": flow}


def save_recent_chat_scan(message: Message, report: dict, sent_message: Message) -> None:
    token_key = _history_key(report)
    if not token_key:
        return
    chat_username = getattr(message.chat, "username", None)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO recent_chat_scans (chat_id, token_key, scan_ts, message_id, chat_username)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, token_key) DO UPDATE SET
            scan_ts = excluded.scan_ts,
            message_id = excluded.message_id,
            chat_username = excluded.chat_username
        """,
        (
            message.chat.id,
            token_key,
            int(time.time()),
            sent_message.message_id,
            chat_username,
        ),
    )
    conn.commit()
    conn.close()


def get_recent_chat_scan(chat_id: int, token_key: str) -> dict | None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT * FROM recent_chat_scans
        WHERE chat_id = ? AND token_key = ?
        LIMIT 1
        """,
        (chat_id, token_key),
    ).fetchone()
    conn.close()
    if not row:
        return None
    result = dict(row)
    if int(time.time()) - int(result["scan_ts"]) >= DUPLICATE_SCAN_COOLDOWN:
        return None
    return result


def _telegram_message_url(chat_id: int, message_id: int, chat_username: str | None = None) -> str | None:
    # IMPORTANT: private Telegram chats use a positive chat_id. In a private
    # bot chat, message.chat.username is the USER'S username, not a public chat
    # username. Building t.me/<user>/<message_id> therefore opens the user's
    # profile/Saved Messages instead of the stored bot scan. Never build a URL
    # for positive/private chat IDs.
    chat_text = str(chat_id)
    if not chat_text.startswith("-"):
        return None

    # Public groups/channels can use their public username.
    if chat_username:
        return f"https://t.me/{chat_username}/{message_id}"

    # Private supergroups/channels use Telegram's /c/ message-link format.
    if chat_text.startswith("-100"):
        return f"https://t.me/c/{chat_text[4:]}/{message_id}"
    return None


def _pct_change(current, old) -> float | None:
    current = _as_float(current)
    old = _as_float(old)
    if current is None or old in (None, 0):
        return None
    return ((current - old) / old) * 100.0


def _fmt_signal_change(value: float | None) -> str:
    if value is None:
        return "Collecting data"
    arrow = "↑" if value > 0 else ("↓" if value < 0 else "→")
    return f"{arrow} {abs(value):.1f}%"


def format_grx_stats(report: dict) -> str:
    dex = report.get("dex_data") or {}
    info = report.get("jetton_info") or {}
    holders = report.get("holders") or {}
    symbol = html.escape(str(info.get("symbol") or "???"))

    snap_5m = get_snapshot_after(report, 5 * 60)
    snap_10m = get_snapshot_after(report, 10 * 60)

    # Rolling volume: current provider 5m bucket vs the 5m bucket recorded
    # around five minutes ago. Suppress false -100% when either side is absent.
    now_v5 = _as_float(dex.get("volume_5m"))
    old_v5 = _as_float((snap_5m or {}).get("volume_5m"))
    vol_change = _pct_change(now_v5, old_v5) if now_v5 is not None and old_v5 not in (None, 0) else None

    holder_now = int(info.get("holders_count") or 0)
    old_holders = (snap_10m or {}).get("holders_count")
    holder_delta = holder_now - int(old_holders) if old_holders is not None else None

    top_now = _as_float(holders.get("top_concentration"))
    top_old = _as_float((snap_10m or {}).get("top10_pct"))
    top_delta = top_now - top_old if top_now is not None and top_old is not None else None

    liq_change = _pct_change(dex.get("liquidity_usd"), (snap_10m or {}).get("liquidity_usd"))
    price_change = _pct_change(dex.get("price_usd"), (snap_10m or {}).get("price_usd"))

    live = _live_trade_metrics(report, 5 * 60)
    tx5 = dex.get("txns_5m") or {}
    provider_buys = int(tx5.get("buys") or 0)
    provider_sells = int(tx5.get("sells") or 0)
    provider_total = provider_buys + provider_sells

    # Prefer GRX's decoded swap stream. Provider counts are a fallback for
    # trade count / pressure only; Net Flow is never guessed from counts.
    trades_5m = live.get("trades") if live.get("trades") is not None else (provider_total or None)
    buy_pressure = live.get("buy_pressure")
    if buy_pressure is None and provider_total:
        buy_pressure = provider_buys / provider_total * 100.0
    net_flow = live.get("net_flow")

    first = get_first_scan_resolved(report)
    first_mc_text = str((first or {}).get("scan_market_cap") or "N/A")

    def parse_compact_usd(value):
        if not value or value == "N/A":
            return None
        t = str(value).replace("$", "").replace(",", "").strip().upper()
        mult = 1
        if t.endswith("K"):
            mult, t = 1_000, t[:-1]
        elif t.endswith("M"):
            mult, t = 1_000_000, t[:-1]
        elif t.endswith("B"):
            mult, t = 1_000_000_000, t[:-1]
        try:
            return float(t) * mult
        except ValueError:
            return None

    current_mc = _as_float(dex.get("market_cap"))
    first_mc_value = parse_compact_usd(first_mc_text)
    performance = _pct_change(current_mc, first_mc_value)
    ath_since = get_ath_mcap_since(report, (first or {}).get("scan_ts"))
    ath_perf = _pct_change(ath_since, first_mc_value)

    def signed(v, suffix="%", decimals=1):
        if v is None:
            return "Collecting data"
        return f"{v:+.{decimals}f}{suffix}"

    lines = [
        f"<b>🔥 GRX SIGNALS — {symbol}</b>",
        "",
        "<b>MOMENTUM</b>",
        f"5m Volume        <b>{signed(vol_change)}</b>",
        f"Buy Pressure     <b>{(f'{buy_pressure:.0f}%' if buy_pressure is not None else 'Collecting data')}</b>",
        f"Net Flow         <b>{(_fmt_usd(net_flow) if net_flow is not None else 'Collecting data')}</b>",
        f"Trades 5m        <b>{(f'{trades_5m:,}' if trades_5m is not None else 'Collecting data')}</b>",
        "",
        "<b>HOLDERS</b>",
        f"Holders 10m      <b>{(f'{holder_delta:+d}' if holder_delta is not None else 'Collecting data')}</b>",
        f"Top 10           <b>{(f'{top_now:.2f}%  {top_delta:+.2f}%' if top_now is not None and top_delta is not None else (f'{top_now:.2f}%' if top_now is not None else 'Collecting data'))}</b>",
        "",
        "<b>MARKET</b>",
        f"Liquidity 10m    <b>{signed(liq_change)}</b>",
        f"Price 10m        <b>{signed(price_change)}</b>",
        "",
        "<b>CALL</b>",
        f"Performance      <b>{(_fmt_pct(performance) if performance is not None else 'N/A')}</b>",
        f"ATH Since Call   <b>{(_fmt_pct(ath_perf) if ath_perf is not None else 'Collecting data')}</b>",
        f"MC Since Call    <b>{html.escape(first_mc_text)} → {html.escape(_fmt_usd(current_mc))}</b>",
    ]
    return "\n".join(lines)


def _cache_report(report: dict, scanner_meta: dict | None = None) -> str:
    _prune_report_cache()
    key = uuid.uuid4().hex[:12]

    if scanner_meta:
        save_scan_history(report, scanner_meta)

    _token_state_put(report.get("address"), report)
    REPORT_CACHE[key] = {
        "report": report,
        "show_info": False,
        "show_holders": False,
        "show_stats": False,
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
        if len(raw_bytes) != 36:
            return None
        workchain_byte = raw_bytes[1]
        workchain = workchain_byte - 256 if workchain_byte > 127 else workchain_byte
        hash_hex = raw_bytes[2:34].hex()
        return f"{workchain}:{hash_hex}"
    except Exception:
        return None


def _to_raw_address(address: str) -> str | None:
    """Normalize either address format into raw 'workchain:hex' form."""
    address = (address or "").strip()
    if ADDR_RAW_RE.match(address):
        return address.lower() if address.startswith("0:") or address.startswith("-1:") else address
    if ADDR_FRIENDLY_RE.match(address):
        return _friendly_to_raw(address)
    return None


def _safe_image_url(report: dict) -> str | None:
    """Return the first usable token image from our metadata fallback chain."""
    candidates = [
        (report.get("jetton_info") or {}).get("image"),          # TonAPI metadata
        (report.get("dex_data") or {}).get("image_url"),        # DexScreener pair info
        report.get("gecko_image_url"),                           # GeckoTerminal token metadata
    ]
    for image_url in candidates:
        if isinstance(image_url, str):
            image_url = image_url.strip()
            if image_url.startswith(("http://", "https://")):
                return image_url
    return None


async def get_gecko_token_image(session: aiohttp.ClientSession, address: str) -> str | None:
    """Fetch a token icon from GeckoTerminal when TonAPI/DexScreener lack one."""
    url = f"{GECKOTERMINAL_BASE}/networks/ton/tokens/{address}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            attrs = ((data.get("data") or {}).get("attributes") or {})
            image_url = attrs.get("image_url")
            if isinstance(image_url, str) and image_url.strip().startswith(("http://", "https://")):
                return image_url.strip()
    except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError):
        return None
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


async def search_dex_pairs(session: aiohttp.ClientSession, query: str, retries: int = 2) -> list[dict] | None:
    last_status = None
    for attempt in range(retries + 1):
        try:
            async with session.get(
                DEXSCREENER_SEARCH_API,
                params={"q": query},
                headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0 (ton-scanner-bot)"},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                last_status = resp.status
                if resp.status == 429:
                    # Rate-limited by DexScreener - back off briefly and retry.
                    logger.warning("DexScreener search 429 for %r (attempt %s)", query, attempt + 1)
                    if attempt < retries:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    return None
                if resp.status != 200:
                    logger.warning(
                        "DexScreener search returned status %s for %r", resp.status, query
                    )
                    return None
                data = await resp.json()
                return data.get("pairs", [])
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(
                "DexScreener search error for %r (attempt %s): %s", query, attempt + 1, e
            )
            if attempt < retries:
                await asyncio.sleep(1.0 * (attempt + 1))
                continue
            return None
    logger.warning("DexScreener search exhausted retries for %r, last_status=%s", query, last_status)
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
        return None, "DexScreener is rate-limiting ticker searches right now — please wait a few seconds and try again."
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


def _resample_ohlcv(candles: list, factor: int) -> list:
    """Combine chronological OHLCV candles into larger candles."""
    if not candles or factor <= 1:
        return candles or []
    out = []
    for i in range(0, len(candles), factor):
        chunk = candles[i:i + factor]
        if len(chunk) < factor:
            continue
        try:
            ts = int(chunk[0][0])
            o = float(chunk[0][1])
            h = max(float(c[2]) for c in chunk)
            l = min(float(c[3]) for c in chunk)
            close = float(chunk[-1][4])
            vol = sum(float(c[5] or 0) for c in chunk if len(c) > 5)
            out.append([ts, o, h, l, close, vol])
        except (TypeError, ValueError, IndexError):
            continue
    return out


async def get_ohlcv(
    session: aiohttp.ClientSession,
    pool_address: str,
    timeframe_key: str,
    token_address: str | None = None,
) -> list | None:
    """Fetch chart candles with supported GeckoTerminal intervals and local resampling.

    GeckoTerminal does not support every arbitrary aggregate. In particular,
    30-minute and 4-day views are built from supported 15m and 1d candles.
    """
    cache_key = (str(pool_address or ""), str(timeframe_key), str(token_address or ""))
    cached = _ttl_get(OHLCV_CACHE, cache_key, OHLCV_CACHE_TTL)
    if cached:
        return cached

    requested = CHART_TIMEFRAMES.get(
        timeframe_key, CHART_TIMEFRAMES[DEFAULT_CHART_TIMEFRAME]
    )

    # (API timeframe, API aggregate, fetch limit, local resample factor)
    route = {
        "1m":  ("minute", 1,  60, 1),
        "5m":  ("minute", 5,  60, 1),
        "15m": ("minute", 15, 60, 1),
        "30m": ("minute", 15, 120, 2),
        "1h":  ("hour",   1,  60, 1),
        "4h":  ("hour",   4,  60, 1),
        "1d":  ("day",    1,  60, 1),
        "4d":  ("day",    1, 120, 4),
    }
    api_tf, api_agg, api_limit, factor = route.get(
        timeframe_key,
        (requested["timeframe"], requested["aggregate"], requested["limit"], 1),
    )

    url = f"{GECKOTERMINAL_BASE}/networks/ton/pools/{pool_address}/ohlcv/{api_tf}"
    params = {
        "aggregate": api_agg,
        "limit": api_limit,
        "currency": "usd",
        "token": "base",
    }

    async def _request(params_to_use):
        try:
            async with session.get(
                url,
                params=params_to_use,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                items = (
                    (data.get("data") or {})
                    .get("attributes", {})
                    .get("ohlcv_list")
                )
                return sorted(items, key=lambda c: c[0]) if items else None
        except (aiohttp.ClientError, asyncio.TimeoutError, TimeoutError):
            return None

    candles = await _request(params)

    # Some pools expose the scanned jetton as quote rather than base.
    # If base produces no candles, try quote before declaring the timeframe unavailable.
    if not candles:
        quote_params = dict(params)
        quote_params["token"] = "quote"
        candles = await _request(quote_params)

    if not candles:
        return None

    if factor > 1:
        candles = _resample_ohlcv(candles, factor)

    # Keep the visible chart clean and consistent.
    display_limit = requested.get("limit", 60)
    result = candles[-display_limit:] if candles else None
    if result:
        _ttl_put(OHLCV_CACHE, cache_key, result)
    return result


async def select_chart_pool(
    session: aiohttp.ClientSession, dex_pairs: list[dict], fallback_pool: str | None, token_address: str | None = None
) -> tuple[str | None, list | None]:
    """Choose the materially-liquid TON pool with the best usable 1m history."""
    pool_cache_key = str(token_address or fallback_pool or "")
    cached_pool = _ttl_get(CHART_POOL_CACHE, pool_cache_key, CHART_POOL_CACHE_TTL) if pool_cache_key else None
    if cached_pool:
        cached_candles = await get_ohlcv(session, cached_pool, "1m", token_address)
        if cached_candles:
            return cached_pool, cached_candles

    ton_pairs = [p for p in (dex_pairs or []) if _is_ton_pair(p) and p.get("pairAddress")]
    if not ton_pairs:
        if not fallback_pool:
            return None, None
        return fallback_pool, await get_ohlcv(session, fallback_pool, "1m", token_address)

    max_liq = max((_pair_liquidity(p) for p in ton_pairs), default=0.0)
    candidates = (
        [p for p in ton_pairs if _pair_liquidity(p) >= max_liq * 0.50]
        if max_liq > 0 else ton_pairs[:]
    )
    candidates.sort(key=lambda p: (_pair_liquidity(p), _pair_volume(p)), reverse=True)
    candidates = candidates[:5]

    async def inspect(pair):
        candles = await get_ohlcv(session, pair.get("pairAddress"), "1m", token_address)
        return pair, candles

    results = await asyncio.gather(*(inspect(p) for p in candidates), return_exceptions=True)
    best_pool, best_candles = fallback_pool, None
    best_score = (-1, -1.0, -1.0)

    for result in results:
        if isinstance(result, Exception):
            continue
        pair, candles = result
        if not candles:
            continue
        score = (len(candles), _pair_liquidity(pair), _pair_volume(pair))
        if score > best_score:
            best_score = score
            best_pool = pair.get("pairAddress")
            best_candles = candles

    if best_candles is None and fallback_pool:
        best_candles = await get_ohlcv(session, fallback_pool, "1m", token_address)
    if best_pool and best_candles and pool_cache_key:
        _ttl_put(CHART_POOL_CACHE, pool_cache_key, best_pool)
    return best_pool, best_candles



async def get_gecko_price_changes(
    session: aiohttp.ClientSession, pool_address: str
) -> dict:
    """Calculate 1H/6H/24H changes independently from GeckoTerminal hourly OHLCV.

    This deliberately does not trust DexScreener's priceChange fields. For a pool
    younger than the requested window, the period is left unavailable rather than
    presenting a partial move as a full 24H move.
    """
    url = f"{GECKOTERMINAL_BASE}/networks/ton/pools/{pool_address}/ohlcv/hour"
    params = {"aggregate": 1, "limit": 30, "currency": "usd"}
    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return {}
            data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return {}

    candles = (data.get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
    if not candles:
        return {}

    candles = sorted(candles, key=lambda c: int(c[0]))
    try:
        latest_ts = int(candles[-1][0])
        current = float(candles[-1][4])
    except (TypeError, ValueError, IndexError):
        return {}
    if current <= 0:
        return {}

    def change_for(hours: int):
        target = latest_ts - hours * 3600
        # Require actual history reaching the requested window. This prevents a
        # 2-hour-old pool from being labelled as a complete 24H move.
        if int(candles[0][0]) > target:
            return None
        eligible = [c for c in candles if int(c[0]) <= target]
        if not eligible:
            return None
        try:
            old = float(eligible[-1][4])
        except (TypeError, ValueError, IndexError):
            return None
        if old <= 0:
            return None
        return ((current / old) - 1.0) * 100.0

    result = {
        "price_change_1h": change_for(1),
        "price_change_6h": change_for(6),
        "price_change_24h": change_for(24),
        "price_change_source": "geckoterminal_ohlcv",
    }

    # Also expose the move across all available hourly history. Useful to GRX
    # Stats for young pools without falsely calling it a full 24H move.
    try:
        first_close = float(candles[0][4])
        if first_close > 0:
            result["price_change_available"] = ((current / first_close) - 1.0) * 100.0
            result["available_history_hours"] = max(
                0.0, (latest_ts - int(candles[0][0])) / 3600.0
            )
    except (TypeError, ValueError, IndexError):
        pass
    return result

async def get_gecko_ath(session: aiohttp.ClientSession, pool_address: str) -> float | None:
    """Return the highest USD trade price available for the pool (up to 1000 daily candles)."""
    url = f"{GECKOTERMINAL_BASE}/networks/ton/pools/{pool_address}/ohlcv/day"
    params = {"aggregate": 1, "limit": 1000, "currency": "usd"}
    try:
        async with session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None

    candles = (data.get("data") or {}).get("attributes", {}).get("ohlcv_list") or []
    highs = []
    for candle in candles:
        try:
            high = float(candle[2])
            if high > 0:
                highs.append(high)
        except (TypeError, ValueError, IndexError):
            continue
    return max(highs) if highs else None


def _chart_axis_price(v, _pos=None):
    """Human price labels without Matplotlib scientific-offset notation."""
    try:
        v=float(v)
    except Exception:
        return ""
    a=abs(v)
    if a >= 1000: return f"{v:,.0f}"
    if a >= 1: return f"{v:.2f}".rstrip("0").rstrip(".")
    if a >= .01: return f"{v:.4f}".rstrip("0").rstrip(".")
    if a >= .0001: return f"{v:.6f}".rstrip("0").rstrip(".")
    if a >= .000001: return f"{v:.8f}".rstrip("0").rstrip(".")
    return f"{v:.10f}".rstrip("0").rstrip(".")

def build_candlestick_chart(ohlcv: list, symbol: str, timeframe_label: str, token_icon_bytes: bytes | None = None, grx_watermark_bytes: bytes | None = None) -> bytes:
    """Standalone chart view matching the clean chart used by the main GRX dashboard."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patheffects as pe
    from datetime import datetime, timezone
    from io import BytesIO

    times=[datetime.fromtimestamp(c[0],tz=timezone.utc) for c in ohlcv]
    opens=[float(c[1]) for c in ohlcv]; highs=[float(c[2]) for c in ohlcv]
    lows=[float(c[3]) for c in ohlcv]; closes=[float(c[4]) for c in ohlcv]
    bg="#050607"; text="#ffffff"; muted="#b7c0ca"; grid="#282d33"
    green="#42c99a"; red="#f06468"
    first=closes[0]; last=closes[-1]
    move=((last-first)/first*100) if first else 0.0
    move_col=green if move>=0 else red

    fig=plt.figure(figsize=(8,5.0),dpi=150,facecolor=bg)
    title_x=.055
    if token_icon_bytes:
        try:
            from PIL import Image, ImageDraw
            icon=Image.open(BytesIO(token_icon_bytes)).convert("RGBA")
            side=min(icon.size); lx=(icon.width-side)//2; ty=(icon.height-side)//2
            icon=icon.crop((lx,ty,lx+side,ty+side)).resize((128,128))
            mask=Image.new("L",(128,128),0); ImageDraw.Draw(mask).ellipse((1,1,127,127),fill=255)
            icon.putalpha(mask)
            iax=fig.add_axes([.055,.865,.065,.065],zorder=20); iax.imshow(icon); iax.axis("off")
            title_x=.132
        except Exception:
            pass
    fig.text(title_x,.92,f"{symbol} / USD",color=text,fontsize=16,fontweight="bold",ha="left",va="center")
    fig.text(title_x,.865,timeframe_label,color=muted,fontsize=10,ha="left",va="center")
    fig.text(.945,.92,f"{move:+.2f}%",color=move_col,fontsize=14,fontweight="bold",ha="right",va="center")

    ax=fig.add_axes([.055,.12,.875,.67],facecolor=bg)
    n=len(ohlcv)
    width=max(.22,min(.54,18.0/max(n,1)))
    for i,(o,h,l,c) in enumerate(zip(opens,highs,lows,closes)):
        col=green if c>=o else red
        ax.plot([i,i],[l,h],color=col,linewidth=.62,solid_capstyle="round")
        bottom=min(o,c); height=abs(c-o) or max((h-l)*.012,abs(h)*.0004,1e-12)
        ax.add_patch(plt.Rectangle((i-width/2,bottom),width,height,facecolor=col,edgecolor=col,linewidth=.25))

    ax.set_xlim(-.8,len(ohlcv)+1.8)
    pad=max((max(highs)-min(lows))*.055,abs(last)*.009,1e-12)
    ax.set_ylim(min(lows)-pad,max(highs)+pad)
    ticks=min(5,len(ohlcv)); ids=[round(i*(len(ohlcv)-1)/(ticks-1)) for i in range(ticks)] if ticks>1 else [0]
    ids=sorted(set(ids)); fmt="%H:%M" if timeframe_label.lower() in ("1m","5m","15m","30m","1h","4h") else "%b %d"
    ax.set_xticks(ids); ax.set_xticklabels([times[i].strftime(fmt) for i in ids],color=muted,fontsize=8.5,fontweight="bold")
    ax.tick_params(axis="x",length=0,pad=9); ax.tick_params(axis="y",colors=muted,labelsize=8.5,length=0,pad=8)
    ax.yaxis.tick_right()
    from matplotlib.ticker import FuncFormatter, MaxNLocator
    ax.yaxis.set_major_formatter(FuncFormatter(_chart_axis_price))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
    ax.yaxis.get_offset_text().set_visible(False)
    ax.grid(axis="y",color=grid,linewidth=.48,alpha=.46); ax.grid(axis="x",visible=False)
    for sp in ax.spines.values(): sp.set_visible(False)
    ax.axhline(last,color=move_col,linewidth=.75,alpha=.48)
    ax.annotate(_fmt_price_compact(last),xy=(len(ohlcv)-1,last),xytext=(len(ohlcv)+.45,last),textcoords="data",
                ha="left",va="center",fontsize=8.0,color="#ffffff",
                bbox=dict(boxstyle="round,pad=.22",fc=move_col,ec="none"),clip_on=False)
    buf=BytesIO(); fig.savefig(buf,format="png",facecolor=bg); plt.close(fig); return buf.getvalue()


def build_report_card(ohlcv: list, report: dict, timeframe_label: str, token_icon_bytes: bytes | None = None, grx_watermark_bytes: bytes | None = None) -> bytes:
    """Render a cleaner trading-terminal GRX dashboard with large, icon-free stats."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patheffects as pe
    from matplotlib.patches import FancyBboxPatch
    from datetime import datetime, timezone
    from io import BytesIO

    info=report.get("jetton_info") or {}; dex=report.get("dex_data") or {}; holders=report.get("holders") or {}
    symbol=str(info.get("symbol") or "???"); age=_fmt_age(dex.get("pair_created_at"))
    txns=dex.get("txns_24h") or {}; buys=int(txns.get("buys") or 0); sells=int(txns.get("sells") or 0); total=buys+sells
    buy_pct=buys/total*100 if total else 0; sell_pct=sells/total*100 if total else 0
    h1=dex.get("price_change_1h"); h6=dex.get("price_change_6h"); h24=dex.get("price_change_24h"); top10=holders.get("top_concentration")
    def f(v):
        try:return float(v)
        except:return None

    bg="#050607"; panel="#090b0d"; cell="#0b0f13"; line="#343b43"; text="#ffffff"; muted="#b7c0ca"; green="#55e0ad"; red="#ff7478"; purple="#a968ff"
    def pc(v):
        n=f(v); return green if n is not None and n>=0 else red if n is not None else muted
    times=[datetime.fromtimestamp(c[0],tz=timezone.utc) for c in ohlcv]
    opens=[float(c[1]) for c in ohlcv]; highs=[float(c[2]) for c in ohlcv]; lows=[float(c[3]) for c in ohlcv]; closes=[float(c[4]) for c in ohlcv]
    fig=plt.figure(figsize=(8,9.05),dpi=160,facecolor=bg)
    def box(x,y,w,h,fc=panel,ec=line,lw=.65,r=.009):
        fig.add_artist(FancyBboxPatch((x,y),w,h,transform=fig.transFigure,boxstyle=f"round,pad=0.002,rounding_size={r}",facecolor=fc,edgecolor=ec,linewidth=lw,zorder=-5))

    fig.text(.965,.974,"TON INTELLIGENCE",color=muted,fontsize=7.5,fontweight="bold",ha="right",va="center")

    # DTrade-inspired chart: flat dark surface, subtle horizontal grid, thicker candles and current-price marker.
    # Token artwork + identity, DTrade-inspired but kept in GRX styling.
    title_x = .04
    if token_icon_bytes:
        try:
            from PIL import Image, ImageDraw
            icon = Image.open(BytesIO(token_icon_bytes)).convert("RGBA")
            side = min(icon.size)
            left = (icon.width - side) // 2
            top_crop = (icon.height - side) // 2
            icon = icon.crop((left, top_crop, left + side, top_crop + side)).resize((128, 128))
            mask = Image.new("L", (128, 128), 0)
            ImageDraw.Draw(mask).ellipse((1, 1, 127, 127), fill=255)
            icon.putalpha(mask)
            iax = fig.add_axes([.04, .895, .055, .055], zorder=20)
            iax.imshow(icon)
            iax.axis("off")
            title_x = .108
        except Exception:
            pass

    fig.text(title_x,.927,f"{symbol} / USD",color=text,fontsize=15,fontweight="bold",ha="left",va="center")
    fig.text(title_x,.899,f"{timeframe_label}",color=muted,fontsize=9,ha="left",va="center")
    last=closes[-1]; first_close=closes[0]; move=((last-first_close)/first_close*100) if first_close else 0
    fig.text(.96,.925,f"{move:+.2f}%",color=pc(move),fontsize=17,fontweight="bold",ha="right",va="center")
    ax=fig.add_axes([.045,.625,.90,.235],facecolor=bg)
    ax.tick_params(axis='x', pad=3)
    n=len(ohlcv)
    width=max(.22,min(.54,18.0/max(n,1)))
    for i,(o,h,l,c) in enumerate(zip(opens,highs,lows,closes)):
        col=green if c>=o else red
        ax.vlines(i,l,h,color=col,linewidth=.68,alpha=.95)
        bottom=min(o,c); height=max(abs(c-o),max(h,l)*1e-8)
        ax.add_patch(plt.Rectangle((i-width/2,bottom),width,height,facecolor=col,edgecolor=col,linewidth=.2))
    ax.set_xlim(-.8,len(ohlcv)+1.8)
    lo=min(lows); hi=max(highs); pad=(hi-lo)*.055 if hi>lo else max(hi*.012,1e-12); ax.set_ylim(lo-pad,hi+pad)
    ticks=min(5,len(ohlcv)); ids=[round(i*(len(ohlcv)-1)/(ticks-1)) for i in range(ticks)] if ticks>1 else [0]; ids=sorted(set(ids))
    fmt="%H:%M" if timeframe_label in ("1m","5m","1H","4H") else "%b %d"
    ax.set_xticks(ids); ax.set_xticklabels([times[i].strftime(fmt) for i in ids],color=muted,fontsize=8.5,fontweight="bold")
    ax.tick_params(axis="x",length=0,pad=9); ax.tick_params(axis="y",colors="#b5bbc2",labelsize=8.5,length=0,pad=8); ax.yaxis.tick_right()
    ax.set_axisbelow(False)
    from matplotlib.ticker import FuncFormatter, MaxNLocator
    ax.yaxis.set_major_formatter(FuncFormatter(_chart_axis_price))
    ax.yaxis.set_major_locator(MaxNLocator(nbins=5))
    ax.yaxis.get_offset_text().set_visible(False)
    ax.grid(axis="y",color="#24282c",linewidth=.48,alpha=.43); ax.grid(axis="x",visible=False)
    for sp in ax.spines.values(): sp.set_visible(False)
    ax.axhline(last,color=pc(move),linewidth=.7,alpha=.45)
    ax.annotate(_fmt_price_compact(last),xy=(len(ohlcv)-1,last),xytext=(len(ohlcv)+.48,last),textcoords="data",ha="left",va="center",fontsize=7.9,color="#ffffff",bbox=dict(boxstyle="round,pad=.22",fc=pc(move),ec="none"),clip_on=False)

    # Compact pulse/caller band.
    box(.025,.445,.465,.115); box(.51,.445,.465,.115)
    fig.text(.045,.535,"MARKET PULSE",color=muted,fontsize=8,fontweight="bold")
    for j,(lab,val) in enumerate((("1H",h1),("6H",h6),("24H",h24))):
        y=.507-j*.027; fig.text(.05,y,lab,color=muted,fontsize=8.5); fig.text(.465,y,_fmt_pct(val),color=pc(val),fontsize=10,fontweight="bold",ha="right")
    fig.text(.53,.535,"FIRST CALLED BY",color=muted,fontsize=8,fontweight="bold")
    first=get_first_scan_resolved(report)
    if first:
        caller=str(first.get("scanner_name") or DEFAULT_SCANNER_LABEL); then_txt=str(first.get("scan_market_cap") or "N/A")
        def usdnum(t):
            t=str(t or "").replace("$","").replace(",","").strip().upper(); m=1
            if t.endswith("K"):m,t=1000,t[:-1]
            elif t.endswith("M"):m,t=1000000,t[:-1]
            try:return float(t)*m
            except:return None
        now=f(dex.get("market_cap")); then=usdnum(then_txt); perf=_pct_change(now,then) if then else None
        fig.text(.53,.507,caller,color=text,fontsize=9.5,fontweight="bold")
        fig.text(.53,.480,"THEN",color=muted,fontsize=7.5); fig.text(.72,.480,then_txt,color=text,fontsize=9.5,fontweight="bold",ha="right")
        fig.text(.75,.480,"NOW",color=muted,fontsize=7.5); fig.text(.955,.480,_fmt_usd(now),color=text,fontsize=9.5,fontweight="bold",ha="right")
        fig.text(.53,.455,"PERFORMANCE",color=muted,fontsize=7.5); fig.text(.955,.455,_fmt_pct(perf) if perf is not None else "N/A",color=pc(perf),fontsize=9.5,fontweight="bold",ha="right")
    else: fig.text(.53,.49,"First scan",color=muted,fontsize=10)

    # Compact 3 x 2 stat grid on each side. The centre split remains aligned
    # exactly with MARKET PULSE / FIRST CALLED BY, but the previous dead space
    # is filled horizontally.
    left_stats = [
        ("PRICE", _fmt_price_compact(dex.get("price_usd")), text),
        ("MARKET CAP", _fmt_usd(dex.get("market_cap")), text),
        ("AGE", age, text),
        ("LIQUIDITY", _fmt_usd(dex.get("liquidity_usd")), text),
        ("BUYS", f"{buys:,} · {buy_pct:.0f}%", green),
        ("HOLDERS", _fmt_num(info.get("holders_count")), text),
    ]
    right_stats = [
        ("1H CHANGE", _fmt_pct(h1), pc(h1)),
        ("24H CHANGE", _fmt_pct(h24), pc(h24)),
        ("VOLUME 24H", _fmt_usd(dex.get("volume_24h")), text),
        ("ATH", _fmt_usd(dex.get("ath_market_cap")), text),
        ("SELLS", f"{sells:,} · {sell_pct:.0f}%", red),
        ("TOP 10", f"{top10:.2f}%" if top10 is not None else "N/A", text),
    ]

    # Two halves, each containing 3 columns x 2 rows.
    half_left_x, half_right_x = .025, .51
    half_w = .465
    col_gap = .007
    row_gap = .010
    grid_top = .405
    grid_bottom = .165
    card_h = (grid_top - grid_bottom - row_gap) / 2
    card_w = (half_w - 2 * col_gap) / 3

    def draw_grid(stats, x0):
        for idx, (label, val, col) in enumerate(stats):
            row = idx // 3
            column = idx % 3
            x = x0 + column * (card_w + col_gap)
            y = grid_top - (row + 1) * card_h - row * row_gap
            box(x, y, card_w, card_h, fc=cell, ec=line, lw=.72, r=.009)

            value = str(val)
            # Adaptive type keeps every value inside a fixed uniform card.
            fs = 11.2
            if len(value) > 11:
                fs = 9.9
            if len(value) > 15:
                fs = 8.8

            fig.text(x + .010, y + card_h * .68, label, color=muted,
                     fontsize=7.1, fontweight="bold", ha="left", va="center")
            fig.text(x + .010, y + card_h * .30, value, color=col,
                     fontsize=fs, fontweight="bold", ha="left", va="center")

    draw_grid(left_stats, half_left_x)
    draw_grid(right_stats, half_right_x)

    buf=BytesIO(); fig.savefig(buf,format="png",facecolor=bg,bbox_inches=None); plt.close(fig); return buf.getvalue()


async def _download_image_bytes(session: aiohttp.ClientSession, url: str | None) -> bytes | None:
    if not url:
        return None
    cached = _ttl_get(IMAGE_CACHE, url, IMAGE_CACHE_TTL)
    if cached:
        return cached
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return None
            data = await resp.read()
            if data:
                _ttl_put(IMAGE_CACHE, url, data)
            return data if data else None
    except Exception:
        return None


async def get_routed_chart_data(
    session: aiohttp.ClientSession, report: dict, timeframe_key: str
) -> tuple[str | None, list | None, str]:
    """Route chart data by token lifecycle.

    Pre-migration TopBlast/x1000 (Uranus) tokens are identified from the shared
    DeDust memepad backend.  Their native frontends do not expose a documented
    public OHLCV API, so GeckoTerminal is used only when it has indexed a usable
    pool.  Migrated tokens use GeckoTerminal directly.  DexScreener remains
    discovery/metadata/fallback, not the candle source.
    """
    dex = report.get("dex_data") or {}
    token_address = str(report.get("address") or "").strip() or None
    fallback_pool = dex.get("pair_address")
    bonding = report.get("bonding_curve") or {}
    launchpad = str(bonding.get("launchpad") or "").lower()
    pre_migration = bool(bonding) and not bonding.get("bonded", False)

    # Pre-migration launchpad tokens need their bonding-curve transaction
    # history, not an immature/empty DEX pool. Prefer that feed whenever DeDust
    # exposes the underlying curve account; fall back safely if it does not.
    if pre_migration and launchpad in {"topblast", "uranus", "groypfi"}:
        launch_candles = await _get_launchpad_ohlcv(session, report, timeframe_key)
        if launch_candles:
            pseudo_pool = f"launchpad:{token_address or launchpad}"
            return pseudo_pool, launch_candles, f"{launchpad}_ton_history"

    # For the 1m scanner card, inspect relevant TON pools so a thin/new
    # secondary pool cannot replace the established market.
    if timeframe_key == "1m":
        pool, candles = await select_chart_pool(
            session, dex.get("chart_pair_candidates") or [], fallback_pool, token_address
        )
    else:
        pool = dex.get("chart_pair_address") or fallback_pool
        candles = await get_ohlcv(session, pool, timeframe_key, token_address) if pool else None

    if candles and pool:
        candles = _merge_live_swaps_into_ohlcv(candles, pool, timeframe_key)

    if pre_migration and launchpad in {"topblast", "uranus", "groypfi"}:
        source = f"{launchpad}_via_grx" if candles else f"{launchpad}_unavailable"
    else:
        source = "grx_native" if candles else "unavailable"
    return pool, candles, source

async def build_scan_photo(
    session: aiohttp.ClientSession, report: dict, bot=None
) -> BufferedInputFile | None:
    """Build the GRX-branded scan card with chart, token icon and embedded GRX logo."""
    dex = report.get("dex_data") or {}
    pool_address = dex.get("pair_address")
    if not pool_address:
        return None

    # Main scanner card: render true 1-minute candles from the best usable pool.
    # The separate Chart button keeps its selectable timeframe state.
    main_scan_timeframe = "1m"
    chart_task = asyncio.create_task(get_routed_chart_data(session, report, main_scan_timeframe))
    icon_task = asyncio.create_task(_download_image_bytes(session, _safe_image_url(report)))
    (chart_pool, ohlcv, chart_source), token_icon = await asyncio.gather(chart_task, icon_task)
    if chart_pool:
        dex["chart_pair_address"] = chart_pool
    dex["chart_source"] = chart_source
    if not ohlcv:
        return None

    try:
        png_bytes = await _render_offloop(
            build_report_card,
            ohlcv,
            report,
            CHART_TIMEFRAMES[main_scan_timeframe]["label"],
            token_icon,
            None,
        )
    except Exception:
        logger.exception("Error building GRX scan report card image")
        return None

    # Cache the exact main dashboard image in-memory. Chart timeframe browsing can
    # then return to the scanner instantly without re-fetching OHLCV or artwork.
    report["_grx_scan_png"] = png_bytes
    return BufferedInputFile(png_bytes, filename="grx_scan.png")

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


def _progress_bar(percent: float | None, size: int = 16) -> str:
    """Slim Telegram-safe bonding bar with quarter-step visual resolution."""
    try:
        percent = max(0.0, min(float(percent), 100.0))
    except (ValueError, TypeError):
        percent = 0.0

    units = (percent / 100.0) * size
    full = int(units)
    frac = units - full
    partial = ""
    if full < size:
        if frac >= .75:
            partial = "▊"
        elif frac >= .50:
            partial = "▌"
        elif frac >= .25:
            partial = "▎"
    empty = max(0, size - full - (1 if partial else 0))
    return "━" * full + partial + "─" * empty


def _centre_html_line(line: str, target_width: int = 34) -> str:
    """Best-effort Telegram centring while preserving HTML/custom emojis."""
    visible = re.sub(r"<[^>]+>", "", line or "")
    visible = html.unescape(visible)
    # Custom emoji tags render as one glyph but have no visible fallback after
    # stripping markup, so target-width padding remains intentionally modest.
    pad = max(0, (target_width - len(visible)) // 2)
    return ("\u2007" * pad) + line



def _launchpad_trade_price_from_tx(tx: dict) -> tuple[float | None, float]:
    """Best-effort extraction of an explicit execution price from decoded tx data.
    We intentionally reject ambiguous transactions rather than invent candles."""
    blob=tx if isinstance(tx,dict) else {}
    candidates=[]
    def walk(v):
        if isinstance(v,dict):
            # Common decoded/indexer field pairs.
            token=None; quote=None
            for k,val in v.items():
                lk=str(k).lower()
                if lk in {"token_amount","jetton_amount","amount_out","tokens","jettons"}:
                    n=_num(val)
                    if n and n>0: token=n
                elif lk in {"ton_amount","gram_amount","amount_in","quote_amount","value"}:
                    n=_num(val)
                    if n and n>0: quote=n
            if token and quote:
                candidates.append((quote/token, quote))
            for x in v.values(): walk(x)
        elif isinstance(v,list):
            for x in v: walk(x)
    walk(blob)
    if not candidates: return None,0.0
    p,q=candidates[0]
    return (p if p>0 else None),q

async def _get_launchpad_account_transactions(session, account: str | None, limit: int = 100) -> list[dict]:
    if not account: return []
    cached=_ttl_get(LAUNCHPAD_TRADE_CACHE,account,LAUNCHPAD_TRADE_CACHE_TTL)
    if cached is not None: return cached
    # toncenter-style endpoint; existing TONCENTER_API_BASE/key config is reused.
    try:
        params={"address":account,"limit":min(max(limit,10),100),"archival":"true"}
        headers={}
        if TONCENTER_API_KEY:
            headers["X-API-Key"]=TONCENTER_API_KEY
        async with session.get(f"{TONCENTER_API_BASE}/getTransactions",params=params,headers=headers,
                               timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
            if resp.status != 200: return []
            payload=await resp.json()
        rows=(payload.get("result") if isinstance(payload,dict) else None) or []
        _ttl_put(LAUNCHPAD_TRADE_CACHE,account,rows)
        return rows
    except Exception:
        return []

async def _get_launchpad_ohlcv(session, report: dict, timeframe_key: str) -> list | None:
    bonding=report.get("bonding_curve") or {}
    account=bonding.get("curve_account") or bonding.get("coin_account")
    txs=await _get_launchpad_account_transactions(session,account,100)
    if not txs: return None
    step=_timeframe_seconds(timeframe_key)
    buckets={}
    for tx in reversed(txs):
        ts=int(_num(tx.get("utime") or tx.get("now") or tx.get("timestamp")) or 0)
        price,vol=_launchpad_trade_price_from_tx(tx)
        if not ts or not price: continue
        b=(ts//step)*step
        row=buckets.get(b)
        if row is None: buckets[b]=[b,price,price,price,price,vol]
        else:
            row[2]=max(row[2],price); row[3]=min(row[3],price); row[4]=price; row[5]+=vol
    rows=[buckets[k] for k in sorted(buckets)]
    return rows[-120:] or None

async def get_topblast_bonding_curve(
    session: aiohttp.ClientSession, address: str, symbol: str | None = None,
    source_hints: list[str] | None = None,
) -> dict | None:
    """Pulls live bonding-curve progress for a TopBlast / Uranus-style TON
    memepad token straight from DeDust's public backend.

    TopBlast (topblast.lol) uses Uranus-compatible contracts, and both
    topblast.lol and x1000.finance's "Uranus" launches are indexed by the same
    public, unauthenticated DeDust API under the 'dedust_v3_memepad' tag — so
    this single endpoint covers tokens launched on either platform.

    Docs page (https://groypfi.io/docs/topblast) only publishes contract
    addresses + get_wallet_address(); it doesn't document this API, but it's
    the same backend that renders the live "Bonding" % shown on topblast.lol
    and x1000.finance/tokens/<address>.
    """
    raw_address = _to_raw_address(address)
    if not raw_address:
        return None

    params = {
        "memecoin_extra_details": "true",
        "filter_by_assets": f"jetton:{raw_address}",
        "limit": 5,
        "compact": "false",
    }

    try:
        async with session.get(
            f"{DEDUST_API_BASE}/coins",
            params=params,
            headers={"Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None

    items = data.get("items") or []
    if not items:
        return None

    item = items[0]
    extra = item.get("memecoin_extra_details") or {}
    # Preserve source metadata returned by DeDust. Different memepad frontends
    # can share the same underlying DeDust curve, so classification is based on
    # explicit URLs/source text when available rather than guessing from curve data.
    # Search the complete DeDust payload, not only a handful of fields.  The
    # memepad source is not consistently returned in the same key.  Also fold
    # in token/DexScreener metadata supplied by scan_token so launchpad links
    # in website/social fields can identify a pre-migration token.
    def _flatten_source(value) -> list[str]:
        if isinstance(value, dict):
            parts = []
            for k, v in value.items():
                parts.append(str(k))
                parts.extend(_flatten_source(v))
            return parts
        if isinstance(value, (list, tuple, set)):
            parts = []
            for v in value:
                parts.extend(_flatten_source(v))
            return parts
        return [str(value or "")]

    source_blob = " ".join(
        _flatten_source(item) + _flatten_source(extra) + list(source_hints or [])
    ).lower()
    collected_nano = extra.get("curve_ton_collected")
    target_nano = extra.get("curve_ton_max")

    if collected_nano is None or target_nano is None:
        # Not a bonding-curve memepad token (e.g. a regular jetton), or DeDust
        # hasn't indexed curve data for it.
        return None

    try:
        collected = float(collected_nano) / NANO
        target = float(target_nano) / NANO
    except (TypeError, ValueError):
        return None

    bonding = _calc_bonding_curve(collected, target)
    if bonding is None:
        return None

    # Preserve any explicit curve/coin account exposed by DeDust. This lets the
    # chart router reconstruct pre-migration trades directly from TON history.
    for key in ("curve_address","curve_account","coin_address","coin_account","address"):
        value = extra.get(key) or item.get(key)
        if value:
            if "curve" in key:
                bonding["curve_account"] = str(value)
            elif key in {"coin_address","coin_account"}:
                bonding["coin_account"] = str(value)

    # migration_date is set once the token has graduated to a DeDust pool,
    # even if curve math alone rounds just under 100%.
    if extra.get("migration_date"):
        bonding["bonded"] = True

    if "topblast" in source_blob or "topblast.lol" in source_blob:
        bonding["launchpad"] = "topblast"
    elif "groyp" in source_blob:
        bonding["launchpad"] = "groypfi"
    elif (
        "uranus" in source_blob
        or "x1000.finance" in source_blob
        or "x1000" in source_blob
    ):
        bonding["launchpad"] = "uranus"
    return bonding


def _flatten_source_hint(value) -> list[str]:
    """Flatten token/social metadata into searchable launchpad source hints."""
    if isinstance(value, dict):
        parts = []
        for k, v in value.items():
            parts.append(str(k))
            parts.extend(_flatten_source_hint(v))
        return parts
    if isinstance(value, (list, tuple, set)):
        parts = []
        for v in value:
            parts.extend(_flatten_source_hint(v))
        return parts
    return [str(value or "")]



async def fast_refresh_report(session: aiohttp.ClientSession, old_report: dict) -> dict:
    """Refresh volatile market fields while preserving slower metadata/holder data."""
    address = str(old_report.get("address") or "").strip()
    if not address:
        return old_report

    # Explicit refresh gets near-live DexScreener data; tiny cache only coalesces duplicate taps.
    cached = _ttl_get(FAST_MARKET_CACHE, address, FAST_MARKET_CACHE_TTL)
    if cached is None:
        pairs = await get_dex_data(session, address)
        if not pairs:
            info = old_report.get("jetton_info") or {}
            alt = str(info.get("address") or "").strip()
            pairs = await get_dex_data(session, alt) if alt else None
        if pairs:
            _ttl_put(FAST_MARKET_CACHE, address, pairs)
    else:
        pairs = cached

    fresh = dict(old_report)
    old_dex = dict(old_report.get("dex_data") or {})

    # Pre-migration launchpad tokens may legitimately have no normal DEX pair.
    # Keep their existing market snapshot and still refresh the curve below.
    if not pairs:
        pairs = []
        best = {}
    else:
        best = pairs[0]
    active_pool = old_dex.get("chart_pair_address") or best.get("pairAddress") or old_dex.get("pair_address")
    _register_live_pool(active_pool)
    recent_buys, recent_sells = _live_swap_counts(active_pool, 30) if active_pool else (0, 0)
    total_vol = sum((p.get("volume") or {}).get("h24", 0) for p in pairs)
    total_liq = sum((p.get("liquidity") or {}).get("usd", 0) for p in pairs)
    txns = best.get("txns") or {}
    h24tx = txns.get("h24") or {}

    if best:
        old_dex.update({
            "price_usd": best.get("priceUsd") or old_dex.get("price_usd"),
            "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
            "total_liquidity_usd": total_liq,
            "volume_24h": total_vol,
            "market_cap": best.get("marketCap") or best.get("fdv") or old_dex.get("market_cap"),
            "fdv": best.get("fdv") or old_dex.get("fdv"),
            "price_change_1h": (best.get("priceChange") or {}).get("h1"),
            "price_change_6h": (best.get("priceChange") or {}).get("h6"),
            "price_change_24h": (best.get("priceChange") or {}).get("h24"),
            "txns_24h": {"buys": h24tx.get("buys", 0) + recent_buys, "sells": h24tx.get("sells", 0) + recent_sells},
            "pair_address": best.get("pairAddress") or old_dex.get("pair_address"),
            "pair_created_at": best.get("pairCreatedAt") or old_dex.get("pair_created_at"),
            "dex_id": best.get("dexId") or old_dex.get("dex_id"),
        })
    fresh["dex_data"] = old_dex

    # Manual Refresh must also refresh launchpad/bonding state. Previously the
    # fast path refreshed only DEX fields, so a TopBlast token could remain at
    # e.g. 66.7% indefinitely even while the curve was actively progressing.
    old_bonding = old_report.get("bonding_curve") or {}
    if old_bonding and not old_bonding.get("bonded", False):
        try:
            info = fresh.get("jetton_info") or {}
            symbol = info.get("symbol")
            canonical = (
                info.get("metadata", {}).get("address")
                if isinstance(info.get("metadata"), dict) else None
            )
            source_hints = []
            for value in (
                info.get("website"), info.get("telegram"), info.get("twitter"),
                old_dex.get("dex_url"), old_dex.get("websites"), old_dex.get("socials"),
                old_bonding.get("launchpad_url"), old_bonding.get("source"),
            ):
                source_hints.extend(_flatten_source_hint(value))

            live_bonding = await get_topblast_bonding_curve(
                session, canonical or address, symbol=symbol, source_hints=source_hints
            )
            if live_bonding:
                fresh["bonding_curve"] = live_bonding
        except Exception:
            logger.exception("Fast refresh: live bonding-curve update failed")

    fresh["found"] = True
    return fresh


def _register_live_pool(pool_address: str | None) -> None:
    """Register a DEX pool for low-latency TON streaming updates."""
    if not pool_address:
        return
    pool = str(pool_address).strip()
    if not pool or pool in LIVE_POOL_ADDRESSES:
        return
    LIVE_POOL_ADDRESSES.add(pool)
    # Keep the subscription bounded for long-running public bots.
    if len(LIVE_POOL_ADDRESSES) > 250:
        oldest = min(
            LIVE_POOL_ADDRESSES,
            key=lambda p: LIVE_POOL_LAST_EVENT.get(p, 0.0),
        )
        LIVE_POOL_ADDRESSES.discard(oldest)
        LIVE_POOL_LAST_EVENT.pop(oldest, None)
        LIVE_POOL_EVENT_COUNT.pop(oldest, None)
    LIVE_STREAM_RESUBSCRIBE.set()


def _invalidate_live_pool_caches(pool_address: str) -> None:
    """Immediately invalidate cached candles for a pool after on-chain activity."""
    pool = str(pool_address).strip()
    if not pool:
        return
    LIVE_POOL_LAST_EVENT[pool] = time.time()
    LIVE_POOL_EVENT_COUNT[pool] = LIVE_POOL_EVENT_COUNT.get(pool, 0) + 1

    # OHLCV cache keys contain pool address; remove every timeframe for this pool.
    for key in list(OHLCV_CACHE):
        try:
            if pool in str(key):
                OHLCV_CACHE.pop(key, None)
        except Exception:
            pass

    # Force the next market refresh to go back upstream rather than reuse a tiny cache.
    for key in list(FAST_MARKET_CACHE):
        FAST_MARKET_CACHE.pop(key, None)


async def _send_live_subscription(ws) -> None:
    addresses = list(LIVE_POOL_ADDRESSES)
    if not addresses:
        return
    await ws.send_json({
        "operation": "subscribe",
        "types": ["transactions"],
        "addresses": addresses,
        "min_finality": "pending",
        "include_address_book": False,
        "include_metadata": False,
        "id": f"grx-{int(time.time())}",
    })



def _num(v):
    try: return float(v) if v is not None else None
    except (TypeError,ValueError): return None

def _addr(v):
    if isinstance(v,str): return v
    if isinstance(v,dict): return v.get("address") or v.get("account_address") or v.get("account")
    return None

def _walk_actions(obj):
    if isinstance(obj,dict):
        for a in obj.get("actions") or []:
            if isinstance(a,dict): yield a
        for v in obj.values(): yield from _walk_actions(v)
    elif isinstance(obj,list):
        for v in obj: yield from _walk_actions(v)

def _decode_swap_action(a,context_pool=None):
    kind=str(a.get("type") or a.get("action_type") or "").lower()
    if "swap" not in kind: return None
    d=a.get("Swap") or a.get("swap") or a
    dex=str(d.get("dex") or d.get("platform") or d.get("protocol") or d.get("dex_name") or "").lower()
    if dex and not any(x in dex for x in ("dedust","ston","ston.fi")): return None
    pool=_addr(d.get("pool")) or _addr(d.get("pool_address")) or context_pool
    if not pool or pool not in LIVE_POOL_ADDRESSES: return None
    ino=d.get("in") or d.get("offer") or d.get("asset_in") or {}
    outo=d.get("out") or d.get("ask") or d.get("asset_out") or {}
    ai=_num(d.get("amount_in") or d.get("offer_amount") or (ino.get("amount") if isinstance(ino,dict) else None))
    ao=_num(d.get("amount_out") or d.get("ask_amount") or (outo.get("amount") if isinstance(outo,dict) else None))
    side=str(d.get("side") or "").lower()
    if side not in ("buy","sell"): side=None
    return {"pool":pool,"ts":int(d.get("timestamp") or a.get("timestamp") or time.time()),
            "price":_num(d.get("price") or d.get("price_usd")),"amount_in":ai,"amount_out":ao,
            "side":side,"source":"dedust" if "dedust" in dex else "stonfi" if "ston" in dex else "ton"}

def _decode_stream_swaps(payload,touched):
    context=next(iter(touched)) if len(touched)==1 else None
    return [s for s in (_decode_swap_action(a,context) for a in _walk_actions(payload)) if s]

def _timeframe_seconds(timeframe_key: str) -> int:
    return {
        "1s":1, "5s":5, "15s":15, "30s":30,
        "1m":60, "5m":300, "15m":900, "30m":1800,
        "1h":3600, "4h":14400, "1d":86400, "4d":345600,
    }.get(timeframe_key, 60)

def _seed_grx_candle_book(pool: str, timeframe_key: str, ohlcv: list | None) -> list:
    """Seed/refresh GRX history without replacing newer locally-built candles."""
    if not pool:
        return [list(r) for r in (ohlcv or [])]
    key=(pool,timeframe_key)
    incoming=[list(r[:6]) for r in (ohlcv or []) if isinstance(r,(list,tuple)) and len(r)>=5]
    current=GRX_CANDLE_BOOK.get(key, [])
    merged={}
    for row in incoming:
        try: merged[int(row[0])] = row
        except Exception: pass
    # Local candles win for equal/newer timestamps: they can contain trades
    # upstream indexers have not published yet.
    for row in current:
        try: merged[int(row[0])] = row
        except Exception: pass
    rows=[merged[k] for k in sorted(merged)]
    rows=rows[-GRX_CANDLE_BOOK_MAX:]
    GRX_CANDLE_BOOK[key]=rows
    return rows

def _apply_swap_to_book(pool: str, timeframe_key: str, swap: dict) -> None:
    """Apply one decoded on-chain trade to GRX OHLC immediately."""
    price=_num(swap.get("price"))
    if not pool or not price or price <= 0:
        return
    step=_timeframe_seconds(timeframe_key)
    ts=int(swap.get("ts") or time.time())
    bucket=(ts//step)*step
    key=(pool,timeframe_key)
    rows=GRX_CANDLE_BOOK.setdefault(key, [])
    volume=_num(swap.get("amount_in")) or 0.0

    if rows and int(rows[-1][0]) == bucket:
        row=rows[-1]
        row[2]=max(float(row[2]),price)
        row[3]=min(float(row[3]),price)
        row[4]=price
        if len(row) < 6: row.append(volume)
        else: row[5]=(_num(row[5]) or 0.0)+volume
    elif not rows or bucket > int(rows[-1][0]):
        # Open from the previous close when available; this makes transitions
        # between live candles visually continuous.
        open_price=float(rows[-1][4]) if rows else price
        rows.append([bucket,open_price,max(open_price,price),min(open_price,price),price,volume])
    else:
        # Late/out-of-order trade: update its historical bucket if retained.
        for row in reversed(rows):
            if int(row[0]) == bucket:
                row[2]=max(float(row[2]),price)
                row[3]=min(float(row[3]),price)
                row[4]=price
                if len(row) < 6: row.append(volume)
                else: row[5]=(_num(row[5]) or 0.0)+volume
                break
    if len(rows) > GRX_CANDLE_BOOK_MAX:
        del rows[:-GRX_CANDLE_BOOK_MAX]

def _merge_live_swaps_into_ohlcv(ohlcv,pool,timeframe_key):
    """Return GRX-owned candles: external history + every priced live swap seen."""
    rows=_seed_grx_candle_book(pool,timeframe_key,ohlcv)
    if not pool:
        return rows
    # Replaying is safe because rows are reseeded by timestamp and live swaps are
    # retained only briefly. Build a fresh seeded book first to avoid stale close.
    for swap in LIVE_SWAPS.get(pool,()):
        _apply_swap_to_book(pool,timeframe_key,swap)
    rows=GRX_CANDLE_BOOK.get((pool,timeframe_key), rows)
    visible = 120 if timeframe_key == "1m" else 90
    return [list(r) for r in rows[-visible:]]

def _apply_swap_to_all_live_timeframes(pool: str, swap: dict) -> None:
    # Keep every selectable chart ready before the user presses its button.
    for tf in CHART_TIMEFRAME_ORDER:
        if (pool,tf) in GRX_CANDLE_BOOK:
            _apply_swap_to_book(pool,tf,swap)

async def ton_live_stream_engine() -> None:
    """Persistent low-latency TON pool watcher.

    This does not replace GeckoTerminal historical candles. Instead it tells GRX
    exactly when a watched pool has on-chain activity so cached market/candle
    data is invalidated immediately. The next Refresh/chart interaction therefore
    requests fresh data instead of waiting for a TTL to expire.
    """
    global LIVE_STREAM_CONNECTED

    if not LIVE_STREAM_ENABLED:
        logger.info("GRX live stream disabled by GRX_LIVE_STREAM=0")
        return
    if not TONAPI_KEY:
        logger.warning(
            "GRX live stream inactive: TONAPI_KEY is not set. "
            "REST scanner will continue to work normally."
        )
        return

    headers = {"Authorization": f"Bearer {TONAPI_KEY}"}
    backoff = 1.0

    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.ws_connect(
                    TON_STREAM_WS,
                    heartbeat=15,
                    autoping=True,
                    receive_timeout=None,
                ) as ws:
                    LIVE_STREAM_CONNECTED = True
                    backoff = 1.0
                    logger.info("GRX live TON stream connected")

                    if LIVE_POOL_ADDRESSES:
                        await _send_live_subscription(ws)
                    LIVE_STREAM_RESUBSCRIBE.clear()

                    while True:
                        receive_task = asyncio.create_task(ws.receive())
                        resub_task = asyncio.create_task(LIVE_STREAM_RESUBSCRIBE.wait())
                        done, pending = await asyncio.wait(
                            {receive_task, resub_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for task in pending:
                            task.cancel()

                        if resub_task in done and resub_task.result():
                            LIVE_STREAM_RESUBSCRIBE.clear()
                            if LIVE_POOL_ADDRESSES:
                                await _send_live_subscription(ws)
                            continue

                        msg = receive_task.result()
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = msg.json()
                            except Exception:
                                continue

                            if payload.get("type") != "transactions":
                                continue

                            # The subscription is address-filtered. Transaction objects
                            # expose their account/address in different compatible
                            # implementations, so inspect the common fields.
                            touched = set()
                            for tx in payload.get("transactions") or []:
                                candidates = [
                                    tx.get("account"),
                                    tx.get("account_address"),
                                    tx.get("address"),
                                ]
                                account_obj = tx.get("account")
                                if isinstance(account_obj, dict):
                                    candidates.extend([
                                        account_obj.get("address"),
                                        account_obj.get("account_address"),
                                    ])
                                for candidate in candidates:
                                    if candidate:
                                        c = str(candidate)
                                        if c in LIVE_POOL_ADDRESSES:
                                            touched.add(c)

                            # Some compatible stream implementations omit the repeated
                            # account field when only one address matched. If so, mark
                            # recently subscribed pools dirty conservatively only when
                            # exactly one pool is watched.
                            if not touched and len(LIVE_POOL_ADDRESSES) == 1:
                                touched = set(LIVE_POOL_ADDRESSES)

                            for pool in touched:
                                _invalidate_live_pool_caches(pool)

                            for swap in _decode_stream_swaps(payload, touched):
                                _record_live_swap(swap["pool"], ts=swap["ts"], price=swap.get("price"),
                                    amount_in=swap.get("amount_in"), amount_out=swap.get("amount_out"),
                                    side=swap.get("side"), source=swap.get("source","ton"))

                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            raise ConnectionError("TON live stream disconnected")

        except asyncio.CancelledError:
            LIVE_STREAM_CONNECTED = False
            raise
        except Exception as exc:
            LIVE_STREAM_CONNECTED = False
            logger.warning("GRX live stream reconnecting after error: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)


def _register_report_live_pool(report: dict) -> None:
    dex = report.get("dex_data") or {}
    pool = (
        dex.get("chart_pair_address")
        or dex.get("pair_address")
        or dex.get("pairAddress")
    )
    _register_live_pool(pool)




def _trim_perf_caches():
    limits=((JETTON_INFO_CACHE,1000),(HOLDERS_CACHE,1000),(ATH_CACHE,1000),(IMAGE_CACHE,1200),(CHART_POOL_CACHE,1200))
    if len(GRX_CANDLE_BOOK) > 2000:
        # Drop the oldest dictionary entries by insertion order; active pools
        # are naturally recreated/reseeded on the next scan.
        for k in list(GRX_CANDLE_BOOK)[:len(GRX_CANDLE_BOOK)-2000]:
            GRX_CANDLE_BOOK.pop(k,None)
    for cache,limit in limits:
        if len(cache)>limit:
            oldest=sorted(cache.items(), key=lambda kv: kv[1][0])[:len(cache)-limit]
            for k,_ in oldest: cache.pop(k,None)

_CACHE_INFLIGHT: dict[tuple, asyncio.Task] = {}

async def _singleflight(cache_name: str, key, coro_factory):
    flight_key=(cache_name,str(key))
    task=_CACHE_INFLIGHT.get(flight_key)
    if task is not None:
        return await task
    task=asyncio.create_task(coro_factory())
    _CACHE_INFLIGHT[flight_key]=task
    try:
        return await task
    finally:
        _CACHE_INFLIGHT.pop(flight_key,None)

async def _cached_jetton_info(session, address):
    cached=_ttl_get(JETTON_INFO_CACHE,address,JETTON_INFO_CACHE_TTL)
    if cached is not None: return cached
    value=await _singleflight("jetton",address,lambda: get_jetton_info(session,address))
    if value: _ttl_put(JETTON_INFO_CACHE,address,value)
    return value

async def _cached_holders(session,address,limit=10):
    key=f"{address}:{limit}"
    cached=_ttl_get(HOLDERS_CACHE,key,HOLDERS_CACHE_TTL)
    if cached is not None: return cached
    value=await _singleflight("holders",key,lambda: get_jetton_holders(session,address,limit=limit))
    if value is not None: _ttl_put(HOLDERS_CACHE,key,value)
    return value

async def _cached_ath(session,pool):
    cached=_ttl_get(ATH_CACHE,pool,ATH_CACHE_TTL)
    if cached is not None: return cached
    value=await _singleflight("ath",pool,lambda: get_gecko_ath(session,pool))
    if value is not None: _ttl_put(ATH_CACHE,pool,value)
    return value

async def _render_offloop(func,*args):
    # Prevent a burst of users from starting too many Matplotlib jobs at once.
    started=time.perf_counter()
    async with RENDER_SEMAPHORE:
        result=await asyncio.to_thread(func,*args)
    _perf_log(f"render {getattr(func, '__name__', 'image')}",started)
    return result

async def get_token_state(session: aiohttp.ClientSession, address: str, *, force: bool=False) -> dict:
    """Shared short-lived state; duplicate concurrent requests collapse into one scan."""
    started=time.perf_counter()
    if not force:
        cached=_token_state_get(address)
        if cached is not None:
            _perf_log("token cache hit",started); return cached
    async with _token_state_lock(address):
        if not force:
            cached=_token_state_get(address)
            if cached is not None:
                _perf_log("token cache hit lock",started); return cached
        stale=_token_state_get(address,TOKEN_STATE_STALE_TTL)
        try:
            report=await scan_token(session,address)
            _token_state_put(address,report)
            _perf_log("token scan",started)
            return report
        except Exception:
            if stale is not None:
                logger.warning("Using recent cached token state after provider failure: %s",address)
                _perf_log("token stale fallback",started)
                return stale
            raise

async def scan_token(session: aiohttp.ClientSession, address: str) -> dict:
    # TonAPI metadata and DEX discovery are independent: fetch them together.
    jetton_info, dex_pairs = await asyncio.gather(
        _cached_jetton_info(session, address),
        get_dex_data(session, address),
    )

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
        holders_task = asyncio.create_task(_cached_holders(session, address, limit=10))
    else:
        report["errors"].append("TonAPI: token not found or API unavailable")

    if dex_pairs:
        report["found"] = True
        total_vol = sum((p.get("volume") or {}).get("h24", 0) for p in dex_pairs)
        total_liq = sum((p.get("liquidity") or {}).get("usd", 0) for p in dex_pairs)
        best = dex_pairs[0]
        dex_info = best.get("info") or {}

        # DexScreener does not always attach socials/websites to the highest-liquidity
        # pair. Merge metadata from every TON pair so X/Telegram/website links are
        # much less likely to disappear from the scan.
        merged_websites = []
        merged_socials = []
        seen_websites = set()
        seen_socials = set()
        for pair in dex_pairs:
            pair_info = pair.get("info") or {}
            for website in pair_info.get("websites") or []:
                marker = repr(website)
                if marker not in seen_websites:
                    merged_websites.append(website)
                    seen_websites.add(marker)
            for social in pair_info.get("socials") or []:
                marker = repr(social)
                if marker not in seen_socials:
                    merged_socials.append(social)
                    seen_socials.add(marker)

        report["dex_data"] = {
            "price_usd": best.get("priceUsd"),
            "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
            "volume_24h": total_vol,
            "volume_1h": (best.get("volume") or {}).get("h1"),
            "volume_5m": (best.get("volume") or {}).get("m5"),
            "market_cap": best.get("marketCap"),
            "fdv": best.get("fdv"),
            "price_change_1h": (best.get("priceChange") or {}).get("h1"),
            "price_change_6h": (best.get("priceChange") or {}).get("h6"),
            "price_change_24h": (best.get("priceChange") or {}).get("h24"),
            "txns_24h": (best.get("txns") or {}).get("h24") or {},
            "txns_1h": (best.get("txns") or {}).get("h1") or {},
            "txns_5m": (best.get("txns") or {}).get("m5") or {},
            "pair_address": best.get("pairAddress"),
            "chart_pair_candidates": [
                {
                    "pairAddress": p.get("pairAddress"),
                    "chainId": p.get("chainId"),
                    "liquidity": p.get("liquidity") or {},
                    "volume": p.get("volume") or {},
                }
                for p in dex_pairs[:8] if p.get("pairAddress")
            ],
            "dex_id": str(best.get("dexId") or "").lower(),
            "pair_created_at": best.get("pairCreatedAt"),
            "dex_url": best.get("url"),
            "image_url": dex_info.get("imageUrl"),
            "total_liquidity_usd": total_liq,
            "websites": merged_websites,
            "socials": merged_socials,
        }
    else:
        report["errors"].append(
            "DexScreener: no DEX pairs found" if dex_pairs == [] else "DexScreener: API request failed"
        )

    if jetton_info:
        try:
            report["holders"] = parse_holders(
                await holders_task,
                jetton_info.get("total_supply"),
            )
        except Exception:
            logger.debug("Holder lookup failed", exc_info=True)

    # Token-image fallback: TonAPI -> DexScreener -> GeckoTerminal.  Only make
    # the Gecko request when the first two providers did not supply an image.
    if not _safe_image_url(report):
        try:
            report["gecko_image_url"] = await get_gecko_token_image(session, address)
        except Exception:
            logger.debug("GeckoTerminal token image lookup failed", exc_info=True)

    # Cross-check headline percentage changes against independent GeckoTerminal
    # OHLCV. Keep DexScreener as fallback only when Gecko lacks a complete window.
    if report.get("dex_data"):
        pool_address = (report["dex_data"] or {}).get("pair_address")
        if pool_address:
            try:
                gecko_changes, ath_price = await asyncio.gather(
                    get_gecko_price_changes(session, pool_address),
                    _cached_ath(session, pool_address),
                )
                # Store the independent cross-check separately. Do NOT mutate
                # V5's original dex_data fields here: the report-card/chart path
                # expects those original values and should remain pixel/functionally
                # identical to the working V5 build.
                report["dex_data"]["gecko_price_changes"] = gecko_changes
                report["dex_data"]["price_change_source"] = (
                    gecko_changes.get("price_change_source")
                    if any(gecko_changes.get(k) is not None for k in (
                        "price_change_1h", "price_change_6h", "price_change_24h"
                    ))
                    else "dexscreener_fallback"
                )
                report["dex_data"]["ath_price"] = ath_price
                try:
                    current_price = float(report["dex_data"].get("price_usd") or 0)
                    current_mcap = float(report["dex_data"].get("market_cap") or 0)
                    if ath_price and current_price > 0 and current_mcap > 0:
                        report["dex_data"]["ath_market_cap"] = current_mcap * (ath_price / current_price)
                    else:
                        report["dex_data"]["ath_market_cap"] = None
                except (TypeError, ValueError, ZeroDivisionError):
                    report["dex_data"]["ath_market_cap"] = None
            except Exception:
                logger.exception("Error calculating GeckoTerminal price changes")

    try:
        symbol = (report.get("jetton_info") or {}).get("symbol")
        # Prefer the canonical jetton-master address TonAPI resolved (more
        # reliable than whatever format the user/ticker-search handed us).
        canonical_address = (jetton_info or {}).get("metadata", {}).get("address")
        # Feed every known website/social/DEX URL into launchpad detection.
        # This fixes TopBlast/Uranus custom emojis when DeDust exposes the curve
        # but omits a dedicated launchpad field.
        source_hints = []
        info_for_hints = report.get("jetton_info") or {}
        dex_for_hints = report.get("dex_data") or {}
        for value in (
            info_for_hints.get("website"), info_for_hints.get("telegram"),
            info_for_hints.get("twitter"), dex_for_hints.get("dex_url"),
            dex_for_hints.get("websites"), dex_for_hints.get("socials"),
        ):
            source_hints.extend(_flatten_source_hint(value))

        bonding_curve = await get_topblast_bonding_curve(
            session, canonical_address or address, symbol=symbol,
            source_hints=source_hints,
        )
        if bonding_curve and not bonding_curve.get("bonded", False):
            report["bonding_curve"] = bonding_curve
    except Exception:
        logger.exception("Error loading TopBlast bonding curve data")

    _register_report_live_pool(report)
    _trim_perf_caches()
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


def _fmt_price_compact(price_str) -> str:
    """Compact scan-card price, using subscript-zero notation for tiny values."""
    if price_str is None:
        return "N/A"
    try:
        price = float(price_str)
    except (ValueError, TypeError):
        return "N/A"
    if price == 0:
        return "$0"

    # DTrade-style tiny-price notation. Example:
    # 0.000000897 -> $0.0₆897
    if 0 < price < 0.0001:
        import math
        zero_count = max(1, int(-math.floor(math.log10(price))) - 1)
        subs = str.maketrans("0123456789", "₀₁₂₃₄₅₆₇₈₉")
        sub_count = str(zero_count).translate(subs)
        significant = price * (10 ** (zero_count + 1))
        digits = f"{significant:.3g}".replace(".", "")
        return f"$0.0{sub_count}{digits}"

    if price < 0.01:
        return f"${price:.6f}".rstrip("0").rstrip(".")
    if price < 1:
        return f"${price:.4f}".rstrip("0").rstrip(".")
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

        platform = str(item.get("platform") or item.get("type") or "").lower().strip()
        direct_url = _normalize_url(item.get("url"))
        handle = str(item.get("handle") or "").strip()

        if direct_url:
            if platform in ("twitter", "x"):
                links.append(("X", direct_url))
            elif platform == "telegram":
                links.append(("TG", direct_url))
            elif platform in ("website", "web"):
                links.append(("WEB", direct_url))
            continue

        if not handle:
            continue
        if platform in ("twitter", "x"):
            handle = handle.lstrip("@")
            links.append(("X", f"https://x.com/{handle}"))
        elif platform == "telegram":
            handle = handle.lstrip("@")
            links.append(("TG", f"https://t.me/{handle}"))
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



def _same_ton_address(a: str | None, b: str | None) -> bool:
    """Compare TON addresses even when one is friendly and the other is raw."""
    a = str(a or "").strip()
    b = str(b or "").strip()
    if not a or not b:
        return False
    if a == b:
        return True
    raw_a = _to_raw_address(a)
    raw_b = _to_raw_address(b)
    return bool(raw_a and raw_b and raw_a.lower() == raw_b.lower())


def _holder_icon_name(report: dict, holder: dict) -> str:
    """Use the DEX custom emoji when the holder is the active LP/pool contract."""
    dex = report.get("dex_data") or {}
    holder_address = str(holder.get("address") or "").strip()
    pair_address = str(dex.get("pair_address") or "").strip()
    if not _same_ton_address(holder_address, pair_address):
        return "wallet"

    dex_id = str(dex.get("dex_id") or "").lower()
    if "dedust" in dex_id:
        return "dedust"
    if "ston" in dex_id:
        return "stonfi"
    return "wallet"


def _dex_emoji(report: dict) -> str:
    """Return the custom emoji for the token's active DEX."""
    dex = report.get("dex_data") or {}
    dex_id = str(dex.get("dex_id") or "").lower()
    if "dedust" in dex_id:
        return _ce("dedust", "💎")
    if "ston" in dex_id:
        return _ce("stonfi", "💎")
    return ""


def _launchpad_emoji(report: dict) -> str:
    bonding = report.get("bonding_curve") or {}
    launchpad = str(bonding.get("launchpad") or "").lower()
    if launchpad == "uranus":
        return _ce("uranus", "🪐")
    if launchpad == "groypfi":
        return _ce("groypfi", "🟣")
    if launchpad == "topblast":
        return _ce("topblast", "🚀")
    return ""


def _coingecko_url(report: dict) -> str | None:
    """Return a real CoinGecko link only when token metadata already exposes one."""
    dex = report.get("dex_data") or {}
    info = report.get("jetton_info") or {}
    candidates = []
    for item in dex.get("websites") or []:
        if isinstance(item, dict):
            candidates.append(item.get("url"))
    candidates.extend([
        _first_url(info.get("website")),
        info.get("coingecko"),
        info.get("coin_gecko"),
    ])
    for candidate in candidates:
        url = _normalize_url(candidate)
        if url and "coingecko.com" in url.lower():
            return url
    return None

def _centred_token_title(symbol: str, name: str, suffix: str = "", width: int = 30) -> str:
    """Visually centre only the token title in Telegram's message column.

    Figure spaces are used so Telegram does not collapse/trim the padding.
    The rest of the report remains left aligned.
    """
    visible = f"{symbol} • {name}"
    # HTML entities/tags are not included in symbol/name at this point beyond
    # escaping, so this is intentionally an approximate visual width.
    plain_len = len(html.unescape(visible))
    pad = max(0, (width - plain_len) // 2)
    return (" " * pad) + f"<b>{visible}</b>{suffix}"


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
    address = str(report.get("address") or "").strip()

    name = html.escape(str(info.get("name") or "Unknown"))
    symbol = html.escape(str(info.get("symbol") or "???"))

    txns = dex.get("txns_24h") or {}
    buys = int(txns.get("buys") or 0)
    sells = int(txns.get("sells") or 0)
    total_txns = buys + sells
    buy_pct = (buys / total_txns * 100.0) if total_txns else 0.0
    sell_pct = (sells / total_txns * 100.0) if total_txns else 0.0

    launch_emoji = _launchpad_emoji(report)
    dex_emoji = _dex_emoji(report)

    # Pre-migration / still-bonding tokens should show their launchpad identity
    # (TopBlast/Uranus/etc.) rather than the underlying DeDust pool emoji.
    is_pre_migration = bool(bonding) and not bonding.get("bonded", False)
    if is_pre_migration and launch_emoji:
        title_emojis = launch_emoji
    else:
        title_emojis = " ".join(x for x in (dex_emoji, launch_emoji) if x)

    title_suffix = f" {title_emojis}" if title_emojis else ""

    links = _collect_links(report)

    # Compact GeckoTerminal token link, matching the abbreviated link row.
    token_address = str(report.get("address") or "").strip()
    if token_address:
        links.insert(0, ("GT", f"https://www.geckoterminal.com/ton/tokens/{token_address}"))

    cg_url = _coingecko_url(report)
    if cg_url:
        links.append(("CG", cg_url))
    link_parts = []
    seen_labels = set()
    for label, url in links:
        safe_url = html.escape(url, quote=True)
        if label == "GT" and "GT" not in seen_labels:
            link_parts.append(f"{_ce('coingecko', '🦎')} <a href=\"{safe_url}\">GT</a>")
            seen_labels.add("GT")
        elif label == "DS" and "DS" not in seen_labels:
            link_parts.append(f"{_ce('dexscreener', '📊')} <a href=\"{safe_url}\">DS</a>")
            seen_labels.add("DS")
        elif label == "TG" and "TG" not in seen_labels:
            link_parts.append(f"{_ce('social', '💬')} <a href=\"{safe_url}\">TG</a>")
            seen_labels.add("TG")
        elif label == "X" and "X" not in seen_labels:
            link_parts.append(f"𝕏 <a href=\"{safe_url}\">X</a>")
            seen_labels.add("X")
        elif label == "TV" and "TV" not in seen_labels:
            link_parts.append(f"{_ce('wallet', '👛')} <a href=\"{safe_url}\">TV</a>")
            seen_labels.add("TV")
        elif label == "WEB" and "WEB" not in seen_labels:
            link_parts.append(f"🌐 <a href=\"{safe_url}\">Web</a>")
            seen_labels.add("WEB")
        elif label == "CG" and "CG" not in seen_labels:
            link_parts.append(f"{_ce('coingecko', '🦎')} <a href=\"{safe_url}\">CG</a>")
            seen_labels.add("CG")

    # Keep the social/link row visually centred beneath the token title.
    # Telegram HTML has no text-align support, so use the same calculated
    # monospace-style padding approach as the centred token title.
    social_line = " • ".join(link_parts)
    if social_line:
        # Estimate visible characters only; HTML/custom-emoji markup itself
        # must not affect the centring calculation.
        social_line = _centre_html_line(social_line, 34)

    lines = [
        _centred_token_title(symbol, name, title_suffix),
        *([social_line] if social_line else []),
    ]

    if bonding and not bonding.get("bonded", False):
        percent = bonding.get("percent")
        bonding_title = _centre_html_line("<b>🧨 Bonding Curve</b>", 34)
        if percent is not None:
            bonding_progress = _centre_html_line(
                f"<code>{html.escape(_progress_bar(percent, size=16))}</code>  <b>{percent:.1f}%</b>", 34
            )
        else:
            bonding_progress = _centre_html_line("<b>Progress: N/A</b>", 34)
        bonding_amounts = _centre_html_line(
            f"<b>{html.escape(_fmt_gram(bonding.get('collected_gram')))}</b> {_ce('gram', '💎')} collected  •  "
            f"<b>{html.escape(_fmt_gram(bonding.get('remaining_gram')))}</b> {_ce('gram', '💎')} left",
            34,
        )
        lines += ["", bonding_title, bonding_progress, bonding_amounts]

    if show_holders:
        holder_list = holders.get("holders") or []
        if holder_list:
            lines += ["", "<b>👥 Top Wallets</b>"]
            for i, holder in enumerate(holder_list[:6], 1):
                pct = holder.get("percentage")
                pct_text = f"{pct:.2f}%" if pct is not None else "N/A"
                wallet_address = str(holder.get("address") or "").strip()
                icon_name = _holder_icon_name(report, holder)
                icon_fallback = "💎" if icon_name in ("dedust", "stonfi") else "👛"
                if wallet_address:
                    wallet_url = f"https://tonviewer.com/{wallet_address}"
                    # The numerical holding percentage itself remains the TON Viewer hyperlink.
                    lines.append(
                        f"{i}. {_ce(icon_name, icon_fallback)} <a href=\"{html.escape(wallet_url, quote=True)}\"><b>{html.escape(pct_text)}</b></a>"
                    )
                else:
                    lines.append(f"{i}. {_ce(icon_name, icon_fallback)} <b>{html.escape(pct_text)}</b>")

    return "\n".join(lines)

def build_report_keyboard(
    key: str,
    show_info: bool,
    show_holders: bool,
    has_chart: bool = False,
    show_stats: bool = False,
):
    builder = InlineKeyboardBuilder()

    # Primary action — full-width GRX Stats row.
    builder.row(
        InlineKeyboardButton(
            text="◂ Scanner" if show_stats else "🔥 GRX Stats",
            callback_data=f"tg:stats:{key}",
        )
    )

    # Scanner controls.
    row = []
    if has_chart:
        row.append(InlineKeyboardButton(text="📈 Chart", callback_data=f"tg:chart:{key}"))
    row.append(InlineKeyboardButton(text="🔄 Refresh", callback_data=f"tg:refresh:{key}"))
    row.append(
        InlineKeyboardButton(
            text="✖ Holders" if show_holders else "👥 Holders",
            callback_data=f"tg:holders:{key}",
        )
    )
    builder.row(*row)
    builder.row(
        InlineKeyboardButton(text="🔔 Alert", callback_data=f"tg:alert:{key}"),
        InlineKeyboardButton(text="⭐ Watch", callback_data=f"tg:watch:{key}"),
    )

    # Open each trading bot directly on the token currently being scanned.
    entry = REPORT_CACHE.get(key) or {}
    report = entry.get("report") or {}
    token_ca = str(report.get("address") or "").strip()

    redotrade_url = REDOTRADE_URL
    dtrade_url = DTRADE_URL
    gbot_url = GBOT_URL

    if token_ca:
        redo_ref = REDOTRADE_URL.split("?start=", 1)[1].split("&", 1)[0] if "?start=" in REDOTRADE_URL else ""
        dtrade_ref = DTRADE_URL.split("?start=", 1)[1].split("&", 1)[0] if "?start=" in DTRADE_URL else ""
        gbot_start = GBOT_URL.split("?start=", 1)[1].split("&", 1)[0] if "?start=" in GBOT_URL else ""

        # RedoTrade Fast Buy: ?start={ref_code}-{ca}
        redotrade_url = f"https://t.me/redotrade?start={redo_ref}-{token_ca}" if redo_ref else f"https://t.me/redotrade?start={token_ca}"

        # DTrade token link: ?start={ref_code}_{ca}
        dtrade_url = f"https://t.me/dtrade?start={dtrade_ref}_{token_ca}" if dtrade_ref else f"https://t.me/dtrade?start={token_ca}"

        # GroypFi/GBot token link: ?start=ref_{telegram_id}={ca}
        gbot_url = f"https://t.me/groypfi_bot?start={gbot_start}={token_ca}" if gbot_start else f"https://t.me/groypfi_bot?start={token_ca}"

    builder.row(
        _custom_icon_button("RedoTrade", redotrade_url, "redotrade"),
        _custom_icon_button("DTrade", dtrade_url, "dtrade"),
        _custom_icon_button("GBot", gbot_url, "gbot"),
    )
    return builder.as_markup()

def build_chart_keyboard(key: str, selected_tf: str):
    builder = InlineKeyboardBuilder()

    # Two clean timeframe rows:
    # 1m  5m  15m  30m
    # 1H  4H  1D   4D
    for timeframe_row in (
        ["1m", "5m", "15m", "30m"],
        ["1h", "4h", "1d", "4d"],
    ):
        builder.row(
            *[
                InlineKeyboardButton(
                    text=(f"• {CHART_TIMEFRAMES[tf]['label']} •" if tf == selected_tf else CHART_TIMEFRAMES[tf]["label"]),
                    callback_data=f"tf:{tf}:{key}",
                )
                for tf in timeframe_row
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

    text = (
        format_grx_stats(report)
        if entry.get("show_stats")
        else format_token_report(
            report,
            show_info=False,
            show_holders=entry["show_holders"],
            scan_history=entry["scan_history"],
        )
    )
    keyboard = build_report_keyboard(
        key,
        False,
        entry["show_holders"],
        has_chart=bool((report.get("dex_data") or {}).get("pair_address")),
        show_stats=entry.get("show_stats", False),
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


@dp.message(F.text & ~F.text.startswith("/"))
async def handle_address(message: Message):
    text = message.text.strip()
    pending = PENDING_ALERT_INPUT.get(message.from_user.id) if message.from_user else None
    if pending:
        value = _parse_money_target(text)
        if value is None or value <= 0:
            await message.answer("Please enter a valid target, e.g. <b>50K</b>, <b>1.2M</b> or <b>0.000015</b>.")
            return
        _create_alert(message.from_user.id, pending["address"], pending["symbol"], pending["type"], threshold=value)
        PENDING_ALERT_INPUT.pop(message.from_user.id, None)
        label={"price_above":"Price above","price_below":"Price below","mcap_above":"MCap above","mcap_below":"MCap below"}[pending["type"]]
        shown = _money(value)

        # Clean up the temporary alert setup conversation once the alert is saved.
        # In groups, Telegram requires the bot to have permission to delete user messages.
        prompt_message_id = pending.get("prompt_message_id")
        if prompt_message_id:
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=prompt_message_id)
            except Exception:
                logger.debug("Could not delete alert prompt message", exc_info=True)
        try:
            await message.delete()
        except Exception:
            logger.debug("Could not delete alert target message", exc_info=True)

        confirmation = await message.answer(
            f"✅ <b>Alert set</b> · {html.escape(pending['symbol'])}\n{label}: <b>{shown}</b>\n\nI’ll DM you when it triggers."
        )
        await asyncio.sleep(2)
        try:
            await confirmation.delete()
        except Exception:
            logger.debug("Could not delete alert confirmation message", exc_info=True)
        return
    status_msg = None

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12), connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300)) as session:
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
                # Ignore ordinary conversation in both private chats and groups.
                # The scanner only reacts to a full TON CA or a single ticker-like
                # word (e.g. GRX6900 / $GRX6900). Commands are handled separately.
                return

            duplicate_key = str(lookup_value or "").strip()
            recent = get_recent_chat_scan(message.chat.id, duplicate_key)
            if recent:
                if status_msg:
                    try:
                        await status_msg.delete()
                    except Exception:
                        pass
                link = _telegram_message_url(
                    message.chat.id,
                    recent["message_id"],
                    recent.get("chat_username"),
                )
                ticker_label = html.escape(normalize_ticker(text) if is_valid_ticker(text) else "Token")
                if link:
                    # Public/supergroup chats have a real Telegram message URL.
                    await message.answer(
                        f"<b>{ticker_label}</b> has already been scanned recently — "
                        f'<a href="{html.escape(link, quote=True)}">see scan ↗</a>'
                    )
                else:
                    # Telegram does not provide a reliable t.me message URL for
                    # private bot chats. Reply to the stored scan instead: Telegram
                    # makes the quoted message tappable and jumps straight to it.
                    try:
                        await message.answer(
                            f"<b>{ticker_label}</b> has already been scanned recently — "
                            "tap the scan above ↗",
                            reply_parameters=ReplyParameters(
                                message_id=int(recent["message_id"]),
                                chat_id=message.chat.id,
                                allow_sending_without_reply=False,
                            ),
                        )
                    except Exception:
                        # If the stored message was deleted, don't create another
                        # broken Saved Messages-style link.
                        await message.answer(
                            f"<b>{ticker_label}</b> has already been scanned recently."
                        )
                return

            report = await scan_token(session, lookup_value)

            if not report.get("found"):
                await status_msg.edit_text(
                    format_token_report(report),
                    disable_web_page_preview=True,
                )
                return

            scanner_meta = _build_scanner_meta(message, report)
            save_token_snapshot(report)
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
                show_stats=False,
            )
            chart_photo = await build_scan_photo(session, report, bot=message.bot)
            image_url = _safe_image_url(report)

            if status_msg:
                try:
                    await status_msg.delete()
                except Exception:
                    pass

            if chart_photo:
                try:
                    sent_scan = await message.answer_photo(
                        photo=chart_photo,
                        caption=result,
                        reply_markup=keyboard,
                    )
                    save_recent_chat_scan(message, report, sent_scan)
                except Exception:
                    logger.exception("Error sending chart card, falling back")
                    chart_photo = None

            if not chart_photo and image_url:
                try:
                    sent_scan = await message.answer_photo(
                        photo=image_url,
                        caption=result,
                        reply_markup=keyboard,
                    )
                    save_recent_chat_scan(message, report, sent_scan)
                except Exception:
                    sent_scan = await message.answer(
                        result,
                        disable_web_page_preview=True,
                        reply_markup=keyboard,
                    )
                    save_recent_chat_scan(message, report, sent_scan)
            elif not chart_photo and not image_url:
                sent_scan = await message.answer(
                    result,
                    disable_web_page_preview=True,
                    reply_markup=keyboard,
                )
                save_recent_chat_scan(message, report, sent_scan)

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

        lock = _refresh_lock(key)
        if lock.locked():
            await callback.answer("Already refreshing…")
            return

        await callback.answer("Refreshing…")

        async with lock:
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=8),
                    connector=aiohttp.TCPConnector(limit=30, ttl_dns_cache=300),
                ) as session:
                    # Fast path updates only volatile market data.
                    fresh_report = await fast_refresh_report(session, entry["report"])

                    if not fresh_report.get("found"):
                        await callback.answer("Couldn't refresh token data right now.", show_alert=True)
                        return

                    # Preserve scanner identity/history and slow-changing holder/ATH metadata.
                    scanner_meta = entry.get("scanner_meta") or _build_scanner_meta_from_user(
                        callback.from_user, fresh_report
                    )
                    entry["report"] = fresh_report
                    entry["scanner_meta"] = scanner_meta
                    entry["has_image"] = bool(_safe_image_url(fresh_report))
                    entry["ts"] = time.time()

                    text_out = (
                        format_grx_stats(fresh_report)
                        if entry.get("show_stats")
                        else format_token_report(
                            fresh_report,
                            show_info=False,
                            show_holders=entry["show_holders"],
                            scan_history=entry.get("scan_history") or [],
                        )
                    )
                    keyboard = build_report_keyboard(
                        key,
                        False,
                        entry["show_holders"],
                        has_chart=bool((fresh_report.get("dex_data") or {}).get("pair_address")),
                        show_stats=entry.get("show_stats", False),
                    )

                    # Force only the live 1m dashboard candles fresh; other timeframes remain cached.
                    dex_now = fresh_report.get("dex_data") or {}
                    live_pool = dex_now.get("chart_pair_address") or dex_now.get("pair_address")
                    if live_pool:
                        for ck in list(OHLCV_CACHE):
                            if live_pool in ck and "1m" in ck:
                                OHLCV_CACHE.pop(ck, None)

                    chart_photo = await build_scan_photo(session, fresh_report, bot=callback.bot)

                if chart_photo:
                    try:
                        media = InputMediaPhoto(media=chart_photo, caption=text_out)
                        await callback.message.edit_media(media=media, reply_markup=keyboard)
                        return
                    except Exception:
                        logger.exception("Error updating fast refresh card, falling back")

                await _render_report_message(callback.message, key)
                return
            except Exception:
                logger.exception("Error in fast token refresh")
                await callback.answer("Refresh failed. Try again in a moment.", show_alert=True)
                return

    if section == "alert":
        symbol=str((entry["report"].get("jetton_info") or {}).get("symbol") or "Token")
        await callback.message.edit_reply_markup(reply_markup=build_alert_keyboard(key))
        await callback.answer(f"Set an alert for {symbol}")
        return

    if section == "watch":
        address=str(entry["report"].get("address") or "").strip(); symbol=str((entry["report"].get("jetton_info") or {}).get("symbol") or "Token")
        added=_toggle_watch(callback.from_user.id,address,symbol)
        await callback.answer(f"{'⭐ Added '+symbol+' to' if added else 'Removed '+symbol+' from'} your watchlist", show_alert=True)
        return

    if section == "chart":
        entry["chart_tf"] = entry.get("chart_tf", DEFAULT_CHART_TIMEFRAME)
        await _send_chart(callback, key, entry["chart_tf"])
        return

    if section == "back":
        entry["show_stats"] = False
        entry["scan_history"] = get_scan_history(entry["report"])
        text = format_token_report(
            entry["report"],
            show_info=False,
            show_holders=entry["show_holders"],
            scan_history=entry["scan_history"],
        )
        keyboard = build_report_keyboard(
            key, False, entry["show_holders"],
            has_chart=bool(((entry["report"].get("dex_data") or {}).get("chart_pair_address") or (entry["report"].get("dex_data") or {}).get("pair_address"))),
            show_stats=False,
        )
        cached_png = entry["report"].get("_grx_scan_png")
        chart_photo = (
            BufferedInputFile(cached_png, filename="grx_scan.png")
            if cached_png
            else None
        )

        # Only rebuild if this report predates the cached-card behaviour.
        if chart_photo is None:
            async with aiohttp.ClientSession() as session:
                chart_photo = await build_scan_photo(session, entry["report"], bot=callback.bot)

        if chart_photo:
            try:
                media = InputMediaPhoto(media=chart_photo, caption=text)
                await callback.message.edit_media(media=media, reply_markup=keyboard)
                await callback.answer()
                return
            except Exception:
                logger.exception("Error restoring cached scan card in-place")

        await callback.answer("Couldn't restore the scan card. Please refresh the token.", show_alert=True)
        return

    if section == "stats":
        entry["show_stats"] = not entry.get("show_stats", False)
        # Keep the stats view focused; holders are part of the scanner view.
        if entry["show_stats"]:
            entry["show_holders"] = False
    elif section == "holders":
        entry["show_stats"] = False
        entry["show_holders"] = not entry["show_holders"]

    await _render_report_message(callback.message, key)
    await callback.answer()


async def _send_chart(callback: CallbackQuery, key: str, timeframe: str):
    entry = REPORT_CACHE.get(key)
    if not entry:
        await callback.answer("This report has expired — please scan the token again.", show_alert=True)
        return

    pool_address = ((entry["report"].get("dex_data") or {}).get("chart_pair_address") or (entry["report"].get("dex_data") or {}).get("pair_address"))
    symbol = (entry["report"].get("jetton_info") or {}).get("symbol", "???")

    if not pool_address:
        await callback.answer("No chart available for this token.", show_alert=True)
        return

    await callback.answer("Loading chart...")

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10),
        connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300),
    ) as session:
        chart_task = asyncio.create_task(get_routed_chart_data(session, entry["report"], timeframe))
        icon_task = asyncio.create_task(_download_image_bytes(session, _safe_image_url(entry["report"])))
        (routed_pool, ohlcv, chart_source), token_icon = await asyncio.gather(chart_task, icon_task)
        if routed_pool:
            (entry["report"].get("dex_data") or {})["chart_pair_address"] = routed_pool
        (entry["report"].get("dex_data") or {})["chart_source"] = chart_source

    if not ohlcv:
        await callback.answer("Couldn't load chart data for this timeframe.", show_alert=True)
        return

    png_bytes = await _render_offloop(
        build_candlestick_chart,
        ohlcv,
        symbol,
        CHART_TIMEFRAMES[timeframe]["label"],
        token_icon,
        None,
    )

    media = InputMediaPhoto(
        media=BufferedInputFile(png_bytes, filename="chart.png"),
        caption=f"<b>{html.escape(str(symbol))} Chart</b> · {html.escape(CHART_TIMEFRAMES[timeframe]["label"])}",
    )
    keyboard = build_chart_keyboard(key, timeframe)

    try:
        await callback.message.edit_media(media=media, reply_markup=keyboard)
    except Exception:
        await callback.message.answer_photo(
            photo=BufferedInputFile(png_bytes, filename="chart.png"),
            caption=f"<b>{html.escape(str(symbol))} Chart</b> · {html.escape(CHART_TIMEFRAMES[timeframe]["label"])}",
            reply_markup=keyboard,
        )

    entry["chart_tf"] = timeframe
    entry["ts"] = time.time()


@dp.callback_query(F.data.startswith("al:"))
async def handle_alert_choice(callback: CallbackQuery):
    try: _, kind, key = callback.data.split(":",2)
    except ValueError: await callback.answer(); return
    entry=REPORT_CACHE.get(key)
    if not entry: await callback.answer("This report expired — scan again.",show_alert=True); return
    report=entry["report"]; address=str(report.get("address") or ""); symbol=str((report.get("jetton_info") or {}).get("symbol") or "Token")
    if kind=="ath":
        dex=report.get("dex_data") or {}; baseline=_as_float(dex.get("ath_market_cap")) or _as_float(dex.get("market_cap"))
        _create_alert(callback.from_user.id,address,symbol,"new_ath",baseline=baseline)
        await callback.answer(f"🚀 New ATH alert set for {symbol}",show_alert=True)
        await _render_report_message(callback.message,key); return
    typ={"pa":"price_above","pb":"price_below","ma":"mcap_above","mb":"mcap_below"}.get(kind)
    if not typ: await callback.answer(); return
    prompt="price" if kind in ("pa","pb") else "market cap"
    await callback.answer()
    prompt_message = await callback.message.answer(f"🔔 <b>{html.escape(symbol)} Alert</b>\n\nSend me the {prompt} target (e.g. <b>50K</b>, <b>1.2M</b> or <b>0.000015</b>).\n\n/cancel to stop.")
    PENDING_ALERT_INPUT[callback.from_user.id]={
        "type":typ,
        "address":address,
        "symbol":symbol,
        "key":key,
        "prompt_message_id":prompt_message.message_id,
    }

@dp.message(Command("cancel"))
async def cancel_alert_input(message: Message):
    if message.from_user: PENDING_ALERT_INPUT.pop(message.from_user.id,None)
    await message.answer("Alert setup cancelled.")

@dp.message(Command("wl", ignore_case=True))
async def show_watchlist(message: Message):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        watches = conn.execute(
            "SELECT * FROM token_watches WHERE user_id=? ORDER BY created_ts DESC",
            (message.from_user.id,),
        ).fetchall()

    lines = ["⭐ <b>Your GRX Watchlist</b>"]
    if watches:
        lines += [
            f"• <b>{html.escape(r['token_symbol'] or 'Token')}</b> · <code>{html.escape(r['token_address'])}</code>"
            for r in watches
        ]
    else:
        lines.append("No watched tokens yet.")

    await message.answer("\n".join(lines))


@dp.message(Command("al", ignore_case=True))
async def show_alert_list(message: Message):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        alerts = conn.execute(
            "SELECT * FROM token_alerts WHERE user_id=? AND active=1 ORDER BY created_ts DESC",
            (message.from_user.id,),
        ).fetchall()

    lines = ["🔔 <b>Your GRX Alerts</b>"]
    if alerts:
        labels = {
            "price_above": "Price ↑",
            "price_below": "Price ↓",
            "mcap_above": "MCap ↑",
            "mcap_below": "MCap ↓",
            "new_ath": "New ATH",
        }
        for r in alerts:
            threshold = (" " + _money(r["threshold"])) if r["threshold"] else ""
            lines.append(
                f"• <b>{html.escape(r['token_symbol'] or 'Token')}</b> · "
                f"{labels.get(r['alert_type'], r['alert_type'])}{threshold}"
            )
    else:
        lines.append("No active alerts.")

    await message.answer("\n".join(lines))

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

    pool_address = ((entry["report"].get("dex_data") or {}).get("chart_pair_address") or (entry["report"].get("dex_data") or {}).get("pair_address"))
    symbol = (entry["report"].get("jetton_info") or {}).get("symbol", "???")

    if not pool_address or timeframe not in CHART_TIMEFRAMES:
        await callback.answer()
        return

    await callback.answer("Loading chart...")

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10),
        connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300),
    ) as session:
        chart_task = asyncio.create_task(get_routed_chart_data(session, entry["report"], timeframe))
        icon_task = asyncio.create_task(_download_image_bytes(session, _safe_image_url(entry["report"])))
        (routed_pool, ohlcv, chart_source), token_icon = await asyncio.gather(chart_task, icon_task)
        if routed_pool:
            (entry["report"].get("dex_data") or {})["chart_pair_address"] = routed_pool
        (entry["report"].get("dex_data") or {})["chart_source"] = chart_source

    if not ohlcv:
        await callback.answer("Couldn't load chart data for that timeframe.", show_alert=True)
        return

    png_bytes = await _render_offloop(
        build_candlestick_chart,
        ohlcv,
        symbol,
        CHART_TIMEFRAMES[timeframe]["label"],
        token_icon,
        None,
    )

    media = InputMediaPhoto(
        media=BufferedInputFile(png_bytes, filename="chart.png"),
        caption=f"<b>{html.escape(str(symbol))} Chart</b> · {html.escape(CHART_TIMEFRAMES[timeframe]["label"])}",
    )
    keyboard = build_chart_keyboard(key, timeframe)

    try:
        await callback.message.edit_media(media=media, reply_markup=keyboard)
    except Exception:
        logger.exception("Error switching chart timeframe")


async def main():
    init_db()
    logger.info("Starting TON Meme Token Scanner bot... GRX_UI_V5_CARBON_ALERTS")
    watcher = asyncio.create_task(alert_watcher())
    live_stream = asyncio.create_task(ton_live_stream_engine())
    try:
        await dp.start_polling(bot)
    finally:
        watcher.cancel()
        live_stream.cancel()
        try: await watcher
        except asyncio.CancelledError: pass
        try: await live_stream
        except asyncio.CancelledError: pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
