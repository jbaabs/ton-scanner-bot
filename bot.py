import os
import asyncio
import sqlite3
import time
import aiohttp

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# --------------------------
# ENV
# --------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "database.db"

# --------------------------
# BOT INIT
# --------------------------
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# --------------------------
# DATABASE
# --------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            token TEXT,
            timestamp REAL
        )
    """)

    conn.commit()
    conn.close()

def save_scan(token):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO scans VALUES (?, ?)", (token, time.time()))
    conn.commit()
    conn.close()

# --------------------------
# TON FILTERED API FETCH
# --------------------------
async def fetch_token_data(token: str):
    url = f"https://api.dexscreener.com/latest/dex/search?q={token}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as res:
            data = await res.json()

    pairs = data.get("pairs", [])

    # ONLY TON PAIRS
    ton_pairs = [p for p in pairs if p.get("chainId") == "ton"]

    if not ton_pairs:
        return None

    pair = ton_pairs[0]

    return {
        "price": pair.get("priceUsd", "0"),
        "liquidity": pair.get("liquidity", {}).get("usd", 0),
        "volume": pair.get("volume", {}).get("h24", 0),
        "pair": pair.get("url", "")
    }

# --------------------------
# KEYBOARD
# --------------------------
def build_keyboard(token, chart_url):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Buy", url=chart_url)
        ],
        [
            InlineKeyboardButton(text="📊 Chart", url=chart_url),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_{token}")
        ]
    ])

# --------------------------
# START
# --------------------------
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("🚀 GRX Scanner is live")

# --------------------------
# AUTO SCAN (MAIN LOGIC)
# --------------------------
@dp.message()
async def auto_scan_handler(message: types.Message):
    text = message.text.strip()

    if not text:
        return

    # Ignore commands
    if text.startswith("/"):
        return

    token = text.upper()

    save_scan(token)

    msg = await message.answer(
        f"🔍 Scanning <b>{token}</b>...\n\n"
        f"• Price: Loading...\n"
        f"• Liquidity: Loading...\n"
        f"• Volume (24h): Loading..."
    )

    data = await fetch_token_data(token)

    if not data:
        await msg.edit_text(f"❌ No TON data found for {token}")
        return

    price = data["price"]
    liquidity = f"${int(data['liquidity']):,}"
    volume = f"${int(data['volume']):,}"
    chart_url = data["pair"]

    await msg.edit_text(
        f"🚀 <b>{token}</b>\n\n"
        f"💰 Price: ${price}\n"
        f"💧 Liquidity: {liquidity}\n"
        f"📊 Volume (24h): {volume}",
        reply_markup=build_keyboard(token, chart_url)
    )

# --------------------------
# REFRESH
# --------------------------
@dp.callback_query(lambda c: c.data.startswith("refresh_"))
async def refresh_handler(callback: types.CallbackQuery):
    token = callback.data.split("_")[1]

    await callback.answer("Refreshing...")

    data = await fetch_token_data(token)

    if not data:
        await callback.message.answer(f"❌ No TON data for {token}")
        return

    price = data["price"]
    liquidity = f"${int(data['liquidity']):,}"
    volume = f"${int(data['volume']):,}"
    chart_url = data["pair"]

    await callback.message.edit_text(
        f"🔄 <b>{token} Updated</b>\n\n"
        f"💰 Price: ${price}\n"
        f"💧 Liquidity: {liquidity}\n"
        f"📊 Volume (24h): {volume}",
        reply_markup=build_keyboard(token, chart_url)
    )

# --------------------------
# MAIN
# --------------------------
async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
