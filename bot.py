import asyncio
import aiohttp
import time
import sqlite3
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode

# ENV
BOT_TOKEN = os.getenv("BOT_TOKEN")

# BOT INIT
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# ✅ HANDLERS GO HERE
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Bot is working 🚀")Dispatcher()

# ----------------------------
# DB SETUP
# ----------------------------
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
    
async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
