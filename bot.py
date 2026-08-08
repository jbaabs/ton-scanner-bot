import os
import re
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode

BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# =========================
# UTIL
# =========================

def is_contract(text: str):
    return len(text) > 30

def is_ticker(text: str):
    return text.isalpha() and len(text) <= 10

def extract_token(text: str):
    text = text.strip().upper()

    if is_contract(text):
        return text
    if is_ticker(text):
        return text

    return None

def fmt(value):
    try:
        return f"{float(value):,.6f}".rstrip("0").rstrip(".")
    except:
        return value

# =========================
# DATA FETCH (GECKO)
# =========================

async def fetch_gecko(token):
    try:
        url = f"https://api.geckoterminal.com/api/v2/search/pools?query={token}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as r:
                data = await r.json()

        pool = data["data"][0]["attributes"]

        return {
            "price": pool["base_token_price_usd"],
            "liquidity": pool["reserve_in_usd"],
            "volume": pool["volume_usd"]["h24"],
            "holders": "N/A",
            "chart": pool.get("chart_url", None),
            "source": "GeckoTerminal"
        }
    except:
        return None

# =========================
# KEYBOARD
# =========================

def build_keyboard(token):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Buy", url="https://ston.fi")],
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{token}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{token}")
        ]
    ])

# =========================
# MESSAGE HANDLER (AUTO SCAN)
# =========================

@dp.message()
async def auto_scan(message: types.Message):
    token = extract_token(message.text or "")

    if not token:
        return

    msg = await message.answer(f"🔎 Scanning {token} on TON...")

    data = await fetch_gecko(token)

    if not data:
        await msg.edit_text(f"❌ No TON data for {token}")
        return

    text = (
        f"<b>{token} scanned ✅</b>\n\n"
        f"💰 Price: ${fmt(data['price'])}\n"
        f"💧 Liquidity: ${fmt(data['liquidity'])}\n"
        f"📊 Volume: ${fmt(data['volume'])}\n"
        f"👥 Holders: {data['holders']}\n\n"
        f"🛰 Source: {data['source']}"
    )

    await msg.edit_text(text, reply_markup=build_keyboard(token))

# =========================
# REFRESH BUTTON
# =========================

@dp.callback_query(lambda c: c.data.startswith("refresh:"))
async def refresh(call: types.CallbackQuery):
    token = call.data.split(":")[1]

    data = await fetch_gecko(token)

    if not data:
        await call.answer("No data", show_alert=True)
        return

    text = (
        f"<b>{token} updated 🔄</b>\n\n"
        f"💰 Price: ${fmt(data['price'])}\n"
        f"💧 Liquidity: ${fmt(data['liquidity'])}\n"
        f"📊 Volume: ${fmt(data['volume'])}\n"
        f"👥 Holders: {data['holders']}\n\n"
        f"🛰 Source: {data['source']}"
    )

    await call.message.edit_text(text, reply_markup=build_keyboard(token))
    await call.answer()

# =========================
# CHART BUTTON (FIXED)
# =========================

@dp.callback_query(lambda c: c.data.startswith("chart:"))
async def chart(call: types.CallbackQuery):
    token = call.data.split(":")[1]

    await call.answer("Loading chart...")

    # TEMP (Step 3 base)
    chart_url = f"https://www.geckoterminal.com/ton/pools"

    await call.message.answer(
        f"📊 <b>{token} Chart</b>\n\n"
        f"View chart:\n{chart_url}"
    )

# =========================
# START
# =========================

@dp.message(CommandStart())
async def start(message: types.Message):
    await message.answer("GRX Scanner is live 🚀")

# =========================
# RUN
# =========================

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
