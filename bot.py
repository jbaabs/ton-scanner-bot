import os
import asyncio
import sqlite3
import time

from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
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

# --------------------------
# KEYBOARD BUILDER
# --------------------------
def build_keyboard(token: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Buy", url=f"https://app.ston.fi/swap?output={token}")
        ],
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart_{token}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_{token}")
        ]
    ])

# --------------------------
# START
# --------------------------
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("GRX Scanner is live 🚀")

# --------------------------
# SCAN
# --------------------------
@dp.message(Command("scan"))
async def scan_handler(message: types.Message):
    args = message.text.split()

    if len(args) < 2:
        await message.answer("Usage: /scan <token>")
        return

    token = args[1].upper()

    # Save scan
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO scans VALUES (?, ?)", (token, time.time()))
    conn.commit()
    conn.close()

    text = f"""
<b>{token}</b> scanned ✅

• Liquidity: Loading...
• Holders: Loading...
• Price: Loading...
"""

    await message.answer(
        text,
        reply_markup=build_keyboard(token)
    )

# --------------------------
# CALLBACKS
# --------------------------
@dp.callback_query()
async def handle_callbacks(callback: types.CallbackQuery):
    data = callback.data

    if data.startswith("chart_"):
        token = data.split("_")[1]

        await callback.message.answer(f"📊 Chart for {token} coming soon")

    elif data.startswith("refresh_"):
        token = data.split("_")[1]

        await callback.message.answer(f"🔄 Refreshing {token}...")

    await callback.answer()

# --------------------------
# MAIN
# --------------------------
async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
