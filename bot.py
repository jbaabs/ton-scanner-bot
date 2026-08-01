import asyncio
import logging
import re
import html
import os

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, BotCommand

from dotenv import load_dotenv

import matplotlib.pyplot as plt
import pandas as pd
import tempfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN environment variable is not set")

TONAPI_KEY = os.getenv("TONAPI_KEY")
if not TONAPI_KEY:
    logger.warning("TONAPI_KEY environment variable is not set; TonAPI calls may fail")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def is_valid_ton_address(text: str) -> bool:
    pattern = r"^(EQ|UQ)[A-Za-z0-9_-]{46}$"
    return bool(re.fullmatch(pattern, text))

async def fetch_tonapi_data(session: aiohttp.ClientSession, address: str) -> dict:
    url = f"https://tonapi.io/v2/accounts/{address}"
    headers = {"Authorization": f"Bearer {TONAPI_KEY}"} if TONAPI_KEY else {}
    async with session.get(url, headers=headers) as resp:
        if resp.status != 200:
            return {}
        return await resp.json()

async def fetch_dexscreener_data(session: aiohttp.ClientSession, address: str) -> dict:
    url = f"https://api.dexscreener.com/latest/dex/search?q={address}"
    async with session.get(url) as resp:
        if resp.status != 200:
            return {}
        data = await resp.json()
        return data.get("pairs", [{}])[0] if data.get("pairs") else {}

async def scan_token(session: aiohttp.ClientSession, address: str) -> dict:
    tonapi_data = await fetch_tonapi_data(session, address)
    dex_data = await fetch_dexscreener_data(session, address)

    balance = tonapi_data.get("balance", "0")
    balance_ton = int(balance) / 1e9 if isinstance(balance, (int, float)) else 0

    is_scam = dex_data.get("isScam", False)
    liquidity_usd = dex_data.get("liquidity", {}).get("usd", 0)
    fdv_usd = dex_data.get("fdv", 0)
    price_usd = dex_data.get("priceUsd", 0)
    price_change_24h = dex_data.get("priceChange", {}).get("h24", 0)
    base_token = dex_data.get("baseToken", {})
    quote_token = dex_data.get("quoteToken", {})
    dex_name = dex_data.get("dexId", "unknown")
    pair_age_days = dex_data.get("pairCreatedAt", 0)

    if pair_age_days:
        from datetime import datetime, timezone
        created_at_s = pair_age_days / 1000
        age_seconds = (datetime.now(timezone.utc).timestamp() - created_at_s)
        age_days = max(0, age_seconds / 86400)
    else:
        age_days = 0

    return {
        "address": address,
        "balance_ton": balance_ton,
        "is_scam": is_scam,
        "liquidity_usd": liquidity_usd,
        "fdv_usd": fdv_usd,
        "price_usd": price_usd,
        "price_change_24h": price_change_24h,
        "base_token": base_token,
        "quote_token": quote_token,
        "dex_name": dex_name,
        "age_days": age_days,
    }

def format_token_report(report: dict) -> str:
    address = report["address"]
    balance_ton = report["balance_ton"]
    is_scam = report["is_scam"]
    liquidity_usd = report["liquidity_usd"]
    fdv_usd = report["fdv_usd"]
    price_usd = report["price_usd"]
    price_change_24h = report["price_change_24h"]
    base_token = report["base_token"]
    quote_token = report["quote_token"]
    dex_name = report["dex_name"]
    age_days = report["age_days"]

    lines = []
    lines.append("🔍 <b>Token Scan Report</b>")
    lines.append("")
    lines.append("<b>Address:</b>")
    lines.append(f"```{address}```")
    lines.append("")

    if base_token and base_token.get("name"):
        name_str = f" {base_token.get('name', '')}"
    else:
        name_str = ""

    if quote_token and quote_token.get("symbol"):
        name_str += f" / {quote_token.get('symbol', '')}"

    scam = " ⚠️ <b>SCAM</b>" if is_scam else ""
    pct_str = ""
    if price_change_24h:
        sign = "+" if price_change_24h > 0 else ""
        pct_str = f" ({sign}{price_change_24h:.2f}% 24h)"

    short = f"${float(price_usd):.10f}" if float(price_usd) < 1 else f"${float(price_usd):.6f}"
    lines.append(f"<b>Price:</b> ```{short}```{name_str}{scam}{pct_str}")
    lines.append("")

    if liquidity_usd:
        lines.append(f"<b>Liquidity:</b> ${liquidity_usd:,.2f}")
    if fdv_usd:
        lines.append(f"<b>FDV:</b> ${fdv_usd:,.2f}")
    if age_days:
        lines.append(f"<b>Age:</b> {age_days:.1f} days")

    lines.append("")
    lines.append(f"<b>DEX:</b> {dex_name}")
    lines.append("")
    lines.append(f"<b>TON Balance:</b> {balance_ton:.4f} TON")
    lines.append("")
    lines.append(f"<b>Risk:</b> {'High' if is_scam or liquidity_usd < 10000 else 'Low'}")
    lines.append("")
    lines.append("<i>Data: DexScreener + TonAPI | Not financial advice</i>")

    return "\n".join(lines)

async def fetch_price_history(session: aiohttp.ClientSession, token_address: str, days: int = 7):
    url = f"https://api.geckoterminal.com/api/v1/networks/ton/tokens/{token_address}/ohlcv/day"
    params = {
        "aggregate": 1,
        "limit": days
    }
    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            return None
        data = await resp.json()

    ohlcv_list = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    if not ohlcv_list:
        return None

    df = pd.DataFrame(
        ohlcv_list,
        columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")
    return df

def create_price_chart(df, token_address: str):
    plt.figure(figsize=(12, 6))
    plt.plot(df["datetime"], df["close"], linewidth=2)
    plt.title(f"Token Chart: {token_address[:12]}...{token_address[-6:]}")
    plt.xlabel("Time")
    plt.ylabel("Price")
    plt.grid(True)
    plt.tight_layout()

    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
    plt.savefig(tmp_file.name, dpi=200)
    plt.close()
    return tmp_file.name

@dp.message(F.text)
async def handle_address(message: Message):
    text = message.text.strip()

    if not is_valid_ton_address(text):
        await message.answer(
            "Please send a valid TON contract address.\n\n"
            "It should start with EQ... or UQ... and be 48 characters long."
        )
        return

    status_msg = await message.answer("Scanning token...")

    chart_file = None

    try:
        async with aiohttp.ClientSession() as session:
            report = await scan_token(session, text)
            result = format_token_report(report)

            try:
                df = await fetch_price_history(session, text, days=7)
                if df is not None and not df.empty:
                    chart_file = create_price_chart(df, text)
            except Exception as chart_error:
                logger.exception("Chart error")

            await status_msg.edit_text(result, disable_web_page_preview=True)

            if chart_file and os.path.exists(chart_file):
                await message.answer_photo(
                    photo=open(chart_file, "rb"),
                    caption="Token chart (7D)"
                )

    except Exception as e:
        logger.exception("Error scanning token")
        await status_msg.edit_text(
            f"Error scanning token: {html.escape(str(e))}\n\nPlease try again later."
        )
    finally:
        if chart_file and os.path.exists(chart_file):
            os.remove(chart_file)

async def main():
    logger.info("Starting TON Meme Token Scanner bot...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped.")
