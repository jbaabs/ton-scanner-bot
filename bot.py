import asyncio
import aiohttp
import time
import sqlite3
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode

# ENV VARIABLES
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "database.db"

# DEBUG (remove later if you want)
print("TOKEN:", BOT_TOKEN)

# BOT INIT
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
# ------------------------
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

# ------------------------
# VALIDATORS
# ------------------------
def is_valid_ton_address(text):
    return len(text) > 40

def is_valid_ticker(text):
    return text.startswith("$")

# ------------------------
# TRENDING
# ------------------------
def get_trending_tokens():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    now = time.time()
    window = now - 86400  # 24h

    c.execute("SELECT token FROM scans WHERE timestamp > ?", (window,))
    rows = c.fetchall()

    scores = {}
    for (token,) in rows:
        scores[token] = scores.get(token, 0) + 1

    conn.close()

    sorted_tokens = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return sorted_tokens[:10]

def build_bar(score, max_score=20):
    filled = int((score / max_score) * 10)
    return "▓" * filled + "░" * (10 - filled)

# ------------------------
# SCAN HANDLER
# ------------------------
@dp.message()
async def handle_scan(message: types.Message):
    if not message.text:
        return

    text = message.text.strip()

    if not (is_valid_ton_address(text) or is_valid_ticker(text)):
        return

    status = await message.answer("🔍 Scanning...")

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:

            if is_valid_ton_address(text):
                token = text

            elif is_valid_ticker(text):
                # simple placeholder resolver
                token = text.upper()

            # SAVE SCAN (no cooldown)
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("INSERT INTO scans VALUES (?, ?)", (token, time.time()))
            conn.commit()
            conn.close()

            # 👉 THIS is where your real scan logic goes later
            await status.edit_text(f"✅ Scanned:\n<code>{token}</code>")

    except Exception as e:
        print("Scan error:", e)
        await status.edit_text("❌ Scan failed")

# ------------------------
# TRENDING COMMAND
# ------------------------
@dp.message(Command("trending"))
async def trending(message: types.Message):
    tokens = get_trending_tokens()

    if not tokens:
        await message.reply("No trending tokens yet.")
        return

    text = "🔥 <b>Trending Tokens</b>\n\n"

    for i, (token, score) in enumerate(tokens, 1):
        bar = build_bar(score)
        text += f"{i}. {token}\n{bar} {score}/20\n\n"

    await message.reply(text)

# ------------------------
# START
# ------------------------
async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
