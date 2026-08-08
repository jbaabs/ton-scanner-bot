import asyncio
import sqlite3
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# =========================
# ENV VARIABLES
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "database.db"

# =========================
# BOT INIT (AIROGRAM 3.7+ FIX)
# =========================
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# =========================
# DATABASE SETUP
# =========================
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

# =========================
# HANDLERS
# =========================

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Bot is working 🚀")

@dp.message(Command("scan"))
async def scan_handler(message: types.Message):
    try:
        token = message.text.split(" ")[1].upper()
    except:
        await message.answer("Usage: /scan <token>")
        return

    # Save scan to DB
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO scans (token, timestamp) VALUES (?, ?)", (token, asyncio.get_event_loop().time()))
    conn.commit()
    conn.close()

    await message.answer(f"Scanned: <b>{token}</b> ✅")

# =========================
# MAIN
# =========================
async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
