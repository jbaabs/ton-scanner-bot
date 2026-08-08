import asyncio
import os
import sqlite3
import time
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# -----------------------
# ENV
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN not set")

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
    print("📦 Initializing DB...")
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
    print("✅ DB Ready")

# -----------------------
# MOCK TON DATA (NEXT STEP = REAL API)
# -----------------------
def get_token_data(token: str):
    # Placeholder for TON APIs (STON / DeDust)
    return {
        "price": "$0.0001050",
        "liquidity": "$14,663",
        "volume": "$10,169",
        "holders": "1,234",
        "buy_url": "https://ston.fi",
        "chart_url": "https://dexscreener.com/ton"
    }

# -----------------------
# BUILD MESSAGE
# -----------------------
def build_message(token, data):
    return f"""
<b>{token} scanned ✅</b>

💰 Price: {data['price']}
💧 Liquidity: {data['liquidity']}
📊 Volume: {data['volume']}
👥 Holders: {data['holders']}
"""

def build_buttons(token, data):
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🟢 Buy", url=data["buy_url"])],
        [
            types.InlineKeyboardButton(text="📊 Chart", url=data["chart_url"]),
            types.InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_{token}")
        ]
    ])

# -----------------------
# SCAN HANDLER (NO COMMANDS)
# -----------------------
@dp.message()
async def scan_handler(message: types.Message):
    text = message.text.strip()

    # Ignore commands like /start
    if text.startswith("/"):
        return

    token = text.upper()

    print(f"🔍 Scan: {token}")

    loading_msg = await message.answer(f"🔍 Scanning {token} on TON...")

    data = get_token_data(token)

    result = build_message(token, data)
    buttons = build_buttons(token, data)

    await loading_msg.edit_text(result, reply_markup=buttons)

# -----------------------
# REFRESH BUTTON
# -----------------------
@dp.callback_query()
async def refresh_handler(callback: types.CallbackQuery):
    if not callback.data.startswith("refresh_"):
        return

    token = callback.data.split("_")[1]

    print(f"🔄 Refresh: {token}")

    data = get_token_data(token)

    result = build_message(token, data)
    buttons = build_buttons(token, data)

    await callback.message.edit_text(result, reply_markup=buttons)
    await callback.answer("Updated ✅")

# -----------------------
# START
# -----------------------
@dp.message()
async def start_filter(message: types.Message):
    if message.text == "/start":
        await message.answer("GRX Scanner is live 🚀")

# -----------------------
# MAIN
# -----------------------
async def main():
    print("🚀 BOT STARTING...")
    init_db()
    print("🤖 Polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
