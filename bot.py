import asyncio
import aiohttp
import sqlite3
import os

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties

# --------------------------
# ENV
# --------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "database.db"

# --------------------------
# BOT INIT (FIXED)
# --------------------------
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# --------------------------
# DB
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

# --------------------------
# TON FETCH (placeholder API)
# --------------------------
async def fetch_token_data(token: str):
    # 🔥 Replace with your real TON API later
    # This is clean mock so bot works perfectly

    return {
        "price": "0.0001050",
        "liquidity": "14,663",
        "volume": "10,169"
    }

# --------------------------
# KEYBOARD
# --------------------------
def build_keyboard(token: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🟢 Buy",
                url=f"https://ston.fi/swap?token={token}"
            )
        ],
        [
            InlineKeyboardButton(
                text="📊 Chart",
                callback_data=f"chart:{token}"
            ),
            InlineKeyboardButton(
                text="🔄 Refresh",
                callback_data=f"refresh:{token}"
            )
        ]
    ])

# --------------------------
# SCAN LOGIC
# --------------------------
async def process_scan(message: types.Message, token: str):
    data = await fetch_token_data(token)

    text = (
        f"💎 <b>{token.upper()} Updated</b>\n\n"
        f"💰 Price: ${data['price']}\n"
        f"💧 Liquidity: ${data['liquidity']}\n"
        f"📊 Volume (24h): ${data['volume']}"
    )

    await message.answer(
        text,
        reply_markup=build_keyboard(token)
    )

# --------------------------
# START
# --------------------------
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("GRX Scanner is live 🚀")

# --------------------------
# TEXT SCAN (NO /scan NEEDED)
# --------------------------
@dp.message(F.text)
async def handle_text(message: types.Message):
    token = message.text.strip().upper()

    if len(token) < 2:
        return

    await process_scan(message, token)

# --------------------------
# CALLBACKS
# --------------------------
@dp.callback_query(F.data.startswith("refresh:"))
async def refresh_callback(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]

    await callback.message.answer("🔄 Refreshing...")
    await process_scan(callback.message, token)

    await callback.answer()


@dp.callback_query(F.data.startswith("chart:"))
async def chart_callback(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]

    await
