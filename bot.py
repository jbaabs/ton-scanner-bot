import asyncio
import os
import time
import sqlite3

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ----------------------------
# ENV
# ----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "database.db"

# ----------------------------
# BOT INIT (aiogram 3.7+)
# ----------------------------
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

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


def save_scan(token: str):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute(
        "INSERT INTO scans (token, timestamp) VALUES (?, ?)",
        (token, time.time())
    )

    conn.commit()
    conn.close()


def get_trending_tokens(limit=10):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        SELECT token, COUNT(*) as count
        FROM scans
        GROUP BY token
        ORDER BY count DESC
        LIMIT ?
    """, (limit,))

    results = c.fetchall()
    conn.close()

    return results


# ----------------------------
# HANDLERS
# ----------------------------

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Bot is working 🚀")


@dp.message(Command("scan"))
async def scan_handler(message: types.Message):
    parts = message.text.split()

    if len(parts) < 2:
        await message.answer("Usage: /scan <token>")
        return

    token = parts[1].upper()

    save_scan(token)

    await message.answer(f"Scanned: <b>{token}</b> ✅")


@dp.message(Command("trending"))
async def trending_handler(message: types.Message):
    tokens = get_trending_tokens()

    if not tokens:
        await message.answer("No trending tokens yet.")
        return

    text = "🔥 <b>Trending Tokens</b>\n\n"

    for i, (token, count) in enumerate(tokens, 1):
        text += f"{i}. {token} — {count} scans\n"

    await message.answer(text)


# ----------------------------
# MAIN
# ----------------------------
async def main():
    init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
