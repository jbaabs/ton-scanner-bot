"""
TON Meme Token Scanner Bot — Single File Version
=================================================
A Telegram bot that scans TON meme tokens by contract address.

Setup:
  1. Get a bot token from @BotFather
  2. Set the BOT_TOKEN environment variable
  3. pip install aiogram aiohttp python-dotenv
  4. python bot.py

Or deploy on Railway/Render (see README).
"""

import os
import re
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
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties

# ─── Configuration ───────────────────────────────────────────────────────────

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
TONAPI_KEY = os.getenv("TONAPI_KEY", "")
DEBUG = os.getenv("DEBUG", "0") == "1"

DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens"
TONAPI_BASE = "https://tonapi.io/v2"
REQUEST_TIMEOUT = 15

logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ton-scanner-bot")

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
                reasons.append("Very new pair (<24h)")
            elif days < 7:
                risk_score += 1
                reasons.append("New pair (<7d)")
        except Exception:
            pass

    level = "low" if risk_score <= 0 else ("medium" if risk_score <= 2 else "high")
    return level, reasons


def format_token_report(report: dict) -> str:
    """Format a scan report as an HTML Telegram message."""
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

    name = info.get("name", "Unknown")
    symbol = info.get("symbol", "???")
    address = report.get("address", "")

    risk, reasons = _assess_risk(report)

    lines = []
    lines.append(f"<b>{name}</b> ({symbol})")
    lines.append(f"<code>{address}</code>")
    lines.append("")

    # Price
    lines.append(f"Price: <b>{_fmt_price(dex.get('price_usd'))}</b>")
    if dex.get("price_native"):
        lines.append(f"  Native: {dex['price_native']} {dex.get('quote_symbol', '')}")

    changes = []
    for label, key in [("5m", "price_change_5m"), ("1h", "price_change_1h"),
                       ("6h", "price_change_6h"), ("24h", "price_change_24h")]:
        val = dex.get(key)
        if val is not None:
            changes.append(f"{label}: {_fmt_pct(val)}")
    if changes:
        lines.append(f"  Change: {' | '.join(changes)}")

    lines.append("")

    # Market data
    lines.append(f"Market Cap: <b>{_fmt_usd(dex.get('market_cap'))}</b>")
    lines.append(f"FDV: {_fmt_usd(dex.get('fdv'))}")
    liq_str = f"<b>{_fmt_usd(dex.get('liquidity_usd'))}</b>"
    if dex.get("total_liquidity_usd") and dex.get("total_liquidity_usd") != dex.get("liquidity_usd"):
        liq_str += f" (total: {_fmt_usd(dex.get('total_liquidity_usd'))})"
    lines.append(f"Liquidity: {liq_str}")
    lines.append(f"Volume 24h: <b>{_fmt_usd(dex.get('volume_24h'))}</b>")

    buys = dex.get("txns_24h_buys", 0)
    sells = dex.get("txns_24h_sells", 0)
    if buys or sells:
        lines.append(f"Txns 24h: {buys} buys / {sells} sells")

    dexes = dex.get("dexes", [])
    pair_count = dex.get("pair_count", 0)
    if dexes:
        lines.append(f"DEX: {', '.join(dexes)} ({pair_count} pair{'s' if pair_count != 1 else ''})")

    age = _fmt_age(dex.get("pair_created_at"))
    if age != "N/A":
        lines.append(f"Pair age: {age}")

    if dex.get("dex_url"):
        lines.append(f'<a href="{dex["dex_url"]}">View on DexScreener</a>')

    lines.append("")

    # Token info
    lines.append("<b>Token Info</b>")
    verification = info.get("verification", "none")
    ver_label = {"whitelist": "Verified (whitelist)", "approve": "Verified (approved)",
                 "none": "Unverified"}.get(verification, verification)
    lines.append(f"Verification: {ver_label}")
    lines.append(f"Holders: <b>{_fmt_num(info.get('holders_count'))}</b>")

    supply = info.get("total_supply")
    if supply:
        try:
            supply_int = int(supply)
            decimals = int(info.get("decimals", "9"))
            human = supply_int / (10 ** decimals)
            lines.append(f"Total Supply: {_fmt_num(human)} {symbol}")
        except (ValueError, TypeError):
            pass

    lines.append(f"Mintable: {'Yes' if info.get('mintable') else 'No'}")
    lines.append("")

    # Top holders
    holder_list = holders.get("holders", [])
    if holder_list:
        lines.append("<b>Top Holders</b>")
        top_conc = holders.get("top_concentration")
        if top_conc is not None:
            lines.append(f"Top 10 concentration: <b>{top_conc:.1f}%</b>")
        for i, h in enumerate(holder_list[:5], 1):
            pct = h.get("percentage")
            pct_str = f" ({pct:.1f}%)" if pct is not None else ""
            name_str = f" [{h['name']}]" if h.get("name") else ""
            scam = " [SCAM]" if h.get("is_scam") else ""
            addr = h.get("address", "")
            short = addr[:10] + "..." + addr[-4:] if len(addr) > 16 else addr
            lines.append(f"  {i}. <code>{short}</code>{name_str}{scam}{pct_str}")
        lines.append("")

    # Risk
    lines.append(f"<b>Risk Level: {risk.capitalize()}</b>")
    if reasons:
        for r in reasons:
            lines.append(f"  - {r}")

    lines.append("")
    lines.append("Data: DexScreener + TonAPI | Not financial advice")
    return "\n".join(lines)


# ─── Telegram Bot ─────────────────────────────────────────────────────────────

if not BOT_TOKEN:
    logger.error("BOT_TOKEN is not set! Copy .env.example to .env and add your token.")
    sys.exit(1)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties,
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
        result = format_token_report(report)
        await status_msg.edit_text(result, disable_web_page_preview=True)
    except Exception as e:
        logger.exception("Error scanning token")
        await status_msg.edit_text(f"Error scanning token: {e}\n\nPlease try again later.")


async def main():
    logger.info("Starting TON Meme Token Scanner bot...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
