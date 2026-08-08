import asyncio
import logging
import aiohttp
import os
from io import BytesIO
import pandas as pd
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, BufferedInputFile
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ==============================
# CONFIG
# ==============================

BOT_TOKEN = os.getenv("BOT_TOKEN")  # MUST be set in Railway/Render

logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# ==============================
# HELPERS
# ==============================

def fmt(x):
    try:
        return f"{float(x):,.6f}".rstrip("0").rstrip(".")
    except:
        return "N/A"

# ==============================
# DATA SOURCES
# ==============================

async def fetch_gecko(session, query):
    try:
        url = f"https://api.geckoterminal.com/api/v2/search?query={query}"
        async with session.get(url) as r:
            if r.status != 200:
                return None
            data = await r.json()

        pools = data.get("data", [])
        if not pools:
            return None

        p = pools[0]["attributes"]

        return {
            "price": float(p.get("base_token_price_usd", 0)),
            "liquidity": float(p.get("reserve_in_usd", 0)),
            "volume": float(p.get("volume_usd", {}).get("h24", 0)),
            "address": pools[0]["attributes"].get("address"),
            "chart": pools[0]["attributes"].get("address"),
            "source": "GeckoTerminal"
        }
    except:
        return None


async def fetch_dedust(session, query):
    try:
        url = f"https://api.dedust.io/v2/pools"
        async with session.get(url) as r:
            data = await r.json()

        for pool in data:
            if query.lower() in pool.get("assets", [{}])[0].get("symbol", "").lower():
                return {
                    "price": float(pool.get("price", 0)),
                    "liquidity": float(pool.get("tvl", 0)),
                    "volume": 0,
                    "address": pool.get("address"),
                    "source": "DeDust"
                }
    except:
        return None


async def fetch_stonfi(session, query):
    try:
        url = "https://api.ston.fi/v1/assets"
        async with session.get(url) as r:
            data = await r.json()

        for token in data.get("asset_list", []):
            if query.lower() in token.get("symbol", "").lower():
                return {
                    "price": float(token.get("dex_price_usd", 0)),
                    "liquidity": 0,
                    "volume": 0,
                    "address": token.get("contract_address"),
                    "source": "STON.fi"
                }
    except:
        return None


# MASTER FETCH
async def fetch_token_data(query):
    async with aiohttp.ClientSession() as session:

        for source in [
            fetch_dedust,
            fetch_stonfi,
            fetch_gecko,
        ]:
            data = await source(session, query)
            if data:
                return data

    return None


# ==============================
# CHART ENGINE
# ==============================

async def generate_chart(symbol):
    # Fake OHLC for now (replace later with real candles)
    df = pd.DataFrame({
        "price": [1, 1.2, 1.1, 1.4, 1.3, 1.6, 1.2]
    })

    plt.figure()
    plt.plot(df["price"])

    buf = BytesIO()
    plt.savefig(buf, format="png")
    plt.close()
    buf.seek(0)

    return buf


# ==============================
# KEYBOARD
# ==============================

def build_keyboard(address):
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [
            types.InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{address or 'none'}"),
            types.InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{address or 'none'}")
        ]
    ])


# ==============================
# HANDLERS
# ==============================

@dp.message()
async def handle_message(message: Message):
    symbol = message.text.strip()

    msg = await message.answer(f"🔍 Scanning {symbol} on TON...")

    data = await fetch_token_data(symbol)

    if not data:
        await msg.edit_text("❌ Token not found on TON")
        return

    address = data.get("address")

    text = (
        f"<b>{symbol.upper()} scanned ✅</b>\n\n"
        f"💰 Price: ${fmt(data.get('price'))}\n"
        f"💧 Liquidity: ${fmt(data.get('liquidity'))}\n"
        f"📊 Volume: ${fmt(data.get('volume'))}\n"
        f"👥 Holders: N/A\n\n"
        f"🛰 Source: {data.get('source')}"
    )

    await msg.edit_text(text, reply_markup=build_keyboard(address))


@dp.callback_query(lambda c: c.data.startswith("chart"))
async def chart_callback(callback: CallbackQuery):
    await callback.answer("Loading chart...")

    _, address = callback.data.split(":")

    chart = await generate_chart(address)

    await callback.message.answer_photo(
        BufferedInputFile(chart.read(), filename="chart.png"),
        caption="📊 Chart"
    )


@dp.callback_query(lambda c: c.data.startswith("refresh"))
async def refresh_callback(callback: CallbackQuery):
    await callback.answer("Refreshing...")

    symbol = callback.message.text.split()[0].replace("<b>", "").replace("</b>", "")

    data = await fetch_token_data(symbol)

    if not data:
        await callback.message.answer("❌ Refresh failed")
        return

    address = data.get("address")

    text = (
        f"<b>{symbol.upper()} updated 🔄</b>\n\n"
        f"💰 Price: ${fmt(data.get('price'))}\n"
        f"💧 Liquidity: ${fmt(data.get('liquidity'))}\n"
        f"📊 Volume: ${fmt(data.get('volume'))}\n"
        f"👥 Holders: N/A\n\n"
        f"🛰 Source: {data.get('source')}"
    )

    await callback.message.edit_text(text, reply_markup=build_keyboard(address))


# ==============================
# START
# ==============================

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
