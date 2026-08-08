import asyncio
import os
import sqlite3
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# -----------------------
# ENV
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN is not set in environment variables")

# -----------------------
# INIT
# -----------------------
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

DB_PATH = "database.db"

# -----------------------
# DATABASE
# -----------------------
def init_db():
    print("📦 Initializing database...")
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
    print("✅ Database ready")

# -----------------------
# COMMANDS
# -----------------------
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("GRX Scanner is live 🚀")

# -----------------------
# SCAN HANDLER (ticker or CA)
# -----------------------
@dp.message()
async def scan_handler(message: types.Message):
    query = message.text.strip().upper()

    print(f"🔍 Scan requested: {query}")

    await message.answer(f"🔍 Scanning {query} on TON...")

    text = f"""
<b>{query} scanned ✅</b>

💰 Price: Loading...
💧 Liquidity: Loading...
📊 Volume: Loading...
👥 Holders: Loading...
"""

    buttons = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🟢 Buy", url="https://ston.fi")],
        [
            types.InlineKeyboardButton(text="📊 Chart", callback_data="chart"),
            types.InlineKeyboardButton(text="🔄 Refresh", callback_data="refresh")
        ]
    ])

    await message.answer(text, reply_markup=buttons)

# -----------------------
# MAIN
# -----------------------
async def main():
    print("🚀 BOT STARTING...")
    init_db()
    print("🤖 Starting polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
