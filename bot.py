import asyncio
import aiohttp
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
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
# DATA SOURCES (TON FIRST)
# =========================

async def fetch_stonfi(session, query):
    try:
        url = f"https://api.ston.fi/v1/assets/search?query={query}"
        async with session.get(url) as resp:
            data = await resp.json()

            if data.get("assets"):
                asset = data["assets"][0]

                return {
                    "price": asset.get("price_usd"),
                    "liquidity": asset.get("liquidity_usd"),
                    "volume": asset.get("volume_24h"),
                    "holders": "N/A",
                    "source": "STON.fi"
                }
    except:
        return None


async def fetch_dedust(session, query):
    try:
        url = f"https://api.dedust.io/v2/assets/{query}"
        async with session.get(url) as resp:
            data = await resp.json()

            return {
                "price": data.get("price"),
                "liquidity": data.get("liquidity"),
                "volume": data.get("volume24h"),
                "holders": "N/A",
                "source": "DeDust"
            }
    except:
        return None


async def fetch_geckoterminal(session, query):
    try:
        url = f"https://api.geckoterminal.com/api/v2/search/pools?query={query}"
        async with session.get(url) as resp:
            data = await resp.json()

            pools = data.get("data", [])
            if pools:
                pool = pools[0]["attributes"]

                return {
                    "price": pool.get("base_token_price_usd"),
                    "liquidity": pool.get("reserve_in_usd"),
                    "volume": pool.get("volume_usd", {}).get("h24"),
                    "holders": "N/A",
                    "source": "GeckoTerminal"
                }
    except:
        return None


async def fetch_dexscreener(session, query):
    try:
        url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
        async with session.get(url) as resp:
            data = await resp.json()

            pairs = data.get("pairs", [])
            if pairs:
                pair = pairs[0]

                return {
                    "price": pair.get("priceUsd"),
                    "liquidity": pair.get("liquidity", {}).get("usd"),
                    "volume": pair.get("volume", {}).get("h24"),
                    "holders": "N/A",
                    "source": "Dexscreener"
                }
    except:
        return None


async def get_token_data(query):
    async with aiohttp.ClientSession() as session:
        for source in [
            fetch_stonfi,
            fetch_dedust,
            fetch_geckoterminal,
            fetch_dexscreener
        ]:
            data = await source(session, query)

            if data and data.get("price"):
                return data

    return None


# =========================
# UI BUTTONS
# =========================

def build_keyboard(query):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Buy", url="https://app.ston.fi")
        ],
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart_{query}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_{query}")
        ]
    ])


# =========================
# START
# =========================

@dp.message(CommandStart())
async def start_handler(message: types.Message):
    await message.answer("GRX Scanner is live 🚀")


# =========================
# AUTO SCAN (NO /scan)
# =========================

@dp.message()
async def auto_scan(message: types.Message):
    query = message.text.strip()

    # ignore commands
    if query.startswith("/"):
        return

    msg = await message.answer(f"🔍 Scanning {query} on TON...")

    data = await get_token_data(query)

    if not data:
        await msg.edit_text(f"❌ No TON data for {query}")
        return

    text = f"""
<b>{query.upper()} scanned ✅</b>

💰 Price: ${data['price']}
💧 Liquidity: ${data['liquidity']}
📊 Volume: ${data['volume']}
👥 Holders: {data['holders']}

📡 Source: {data['source']}
"""

    await msg.edit_text(text, reply_markup=build_keyboard(query))


# =========================
# BUTTON HANDLERS
# =========================

@dp.callback_query(lambda c: c.data.startswith("refresh_"))
async def refresh_callback(callback: types.CallbackQuery):
    query = callback.data.split("_")[1]

    data = await get_token_data(query)

    if not data:
        await callback.answer("No data", show_alert=True)
        return

    text = f"""
<b>{query.upper()} updated 🔄</b>

💰 Price: ${data['price']}
💧 Liquidity: ${data['liquidity']}
📊 Volume: ${data['volume']}
👥 Holders: {data['holders']}

📡 Source: {data['source']}
"""

    await callback.message.edit_text(text, reply_markup=build_keyboard(query))


@dp.callback_query(lambda c: c.data.startswith("chart_"))
async def chart_callback(callback: types.CallbackQuery):
    await callback.answer("Chart engine coming in Step 4 📊")


# =========================
# RUN
# =========================

async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
