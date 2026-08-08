import asyncio
import os
import re
import io
import random
import aiohttp
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# =========================
# SIMPLE DB (for scan price)
# =========================
DB_PATH = "scanner.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            token TEXT PRIMARY KEY,
            price REAL
        )
    """)
    conn.commit()
    conn.close()

def save_scan(token, price):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO scans (token, price) VALUES (?, ?)", (token, price))
    conn.commit()
    conn.close()

def get_scan_price(token):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT price FROM scans WHERE token=?", (token,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


# =========================
# MOCK DATA (REPLACE LATER)
# =========================
async def fetch_token_data(token: str):
    # Replace with GeckoTerminal / STON.fi later
    return {
        "price": round(random.uniform(0.01, 0.05), 6),
        "liquidity": round(random.uniform(5000, 200000), 2),
        "volume": round(random.uniform(1000, 100000), 2),
        "holders": random.randint(50, 500)
    }


# =========================
# CHART ENGINE
# =========================
def get_price_data(points=60):
    prices = []
    price = random.uniform(0.01, 0.05)

    for _ in range(points):
        price += random.uniform(-0.002, 0.002)
        prices.append(max(price, 0.001))

    return prices


def generate_chart(token: str, timeframe: str):
    points_map = {
        "1m": 30,
        "5m": 50,
        "15m": 60,
        "1h": 80,
        "4h": 100
    }

    prices = get_price_data(points_map.get(timeframe, 50))
    scan_price = get_scan_price(token)

    plt.figure(figsize=(10, 5))
    plt.plot(prices, linewidth=2)

    if scan_price:
        plt.axhline(y=scan_price, linewidth=2)  # GRX line

    plt.title(f"{token} • {timeframe}")
    plt.grid(alpha=0.2)

    buf = io.BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    plt.close()
    buf.seek(0)

    return buf


# =========================
# BUTTONS
# =========================
def main_buttons(token: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Buy", url="https://ston.fi")],
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{token}:1m"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{token}")
        ]
    ])


def chart_buttons(token: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1m", callback_data=f"chart:{token}:1m"),
            InlineKeyboardButton(text="5m", callback_data=f"chart:{token}:5m"),
            InlineKeyboardButton(text="15m", callback_data=f"chart:{token}:15m"),
        ],
        [
            InlineKeyboardButton(text="1h", callback_data=f"chart:{token}:1h"),
            InlineKeyboardButton(text="4h", callback_data=f"chart:{token}:4h"),
        ]
    ])


# =========================
# START
# =========================
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("GRX Scanner is live 🚀")


# =========================
# AUTO SCAN (NO /scan)
# =========================
@dp.message()
async def auto_scan(message: types.Message):
    text = message.text.strip()

    # Basic ticker / CA detection
    if not re.match(r"^[A-Za-z0-9]{2,20}$", text):
        return

    token = text.upper()

    msg = await message.answer(f"🔎 Scanning {token} on TON...")

    data = await fetch_token_data(token)

    save_scan(token, data["price"])

    await msg.edit_text(
        f"<b>{token} scanned ✅</b>\n\n"
        f"💰 Price: {data['price']}\n"
        f"💧 Liquidity: {data['liquidity']}\n"
        f"📊 Volume: {data['volume']}\n"
        f"👥 Holders: {data['holders']}",
        reply_markup=main_buttons(token)
    )


# =========================
# REFRESH
# =========================
@dp.callback_query(lambda c: c.data.startswith("refresh:"))
async def refresh_handler(callback: types.CallbackQuery):
    token = callback.data.split(":")[1]

    data = await fetch_token_data(token)

    await callback.message.edit_text(
        f"<b>{token} updated 🔄</b>\n\n"
        f"💰 Price: {data['price']}\n"
        f"💧 Liquidity: {data['liquidity']}\n"
        f"📊 Volume: {data['volume']}\n"
        f"👥 Holders: {data['holders']}",
        reply_markup=main_buttons(token)
    )


# =========================
# CHART HANDLER
# =========================
@dp.callback_query(lambda c: c.data.startswith("chart:"))
async def chart_handler(callback: types.CallbackQuery):
    _, token, timeframe = callback.data.split(":")

    chart = generate_chart(token, timeframe)

    await callback.message.answer_photo(
        photo=chart,
        caption=f"📊 {token} • {timeframe}",
        reply_markup=chart_buttons(token)
    )


# =========================
# RUN
# =========================
async def main():
    init_db()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
