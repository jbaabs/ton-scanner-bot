import asyncio
import logging
import aiohttp
from io import BytesIO

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ==============================
# CONFIG
# ==============================

BOT_TOKEN = "8835642161:AAEX3XjrRtlQpn_BeycLhDQLao0lIhT-f3s"

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
        return f"{float(x):,.6g}"
    except:
        return "N/A"

def build_keyboard(address: str | None):
    if not address:
        return None

    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{address}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{address}")
        ]
    ])

# ==============================
# DATA SOURCES
# ==============================

async def fetch_geckoterminal(session, query):
    try:
        url = f"https://api.geckoterminal.com/api/v2/search?query={query}"
        async with session.get(url) as resp:

            if resp.status != 200:
                return None

            data = await resp.json()
            pairs = data.get("data", [])

            if not pairs:
                return None

            pair = pairs[0]["attributes"]

            return {
                "price": pair.get("base_token_price_usd"),
                "liquidity": pair.get("reserve_in_usd"),
                "volume": pair.get("volume_usd", {}).get("h24"),
                "address": pair.get("address"),
                "source": "GeckoTerminal"
            }

    except:
        return None


# FUTURE SOURCES (stubbed for now)
async def fetch_dedust(session, query):
    return None

async def fetch_stonfi(session, query):
    return None

async def fetch_x1000(session, query):
    return None

async def fetch_groypfi(session, query):
    return None


# ==============================
# MASTER FETCH
# ==============================

async def fetch_token_data(query):

    async with aiohttp.ClientSession() as session:

        sources = [
            fetch_dedust,
            fetch_stonfi,
            fetch_x1000,
            fetch_groypfi,
            fetch_geckoterminal,  # fallback
        ]

        for source in sources:
            data = await source(session, query)
            if data:
                return data

    return None


# ==============================
# MESSAGE HANDLER (NO /SCAN)
# ==============================

@dp.message()
async def handle_message(message: Message):

    query = message.text.strip()

    await message.answer(f"🔍 Scanning {query} on TON...")

    data = await fetch_token_data(query)

    if not data:
        await message.answer("❌ Token not found on TON")
        return

    address = data.get("address")  # SAFE

    text = (
        f"<b>{query.upper()} scanned ✅</b>\n\n"
        f"💰 Price: ${fmt(data.get('price'))}\n"
        f"💧 Liquidity: ${fmt(data.get('liquidity'))}\n"
        f"📊 Volume: ${fmt(data.get('volume'))}\n"
        f"👥 Holders: N/A\n\n"
        f"🛰 Source: {data.get('source')}"
    )

    await message.answer(
        text,
        reply_markup=build_keyboard(address)
    )


# ==============================
# REFRESH BUTTON
# ==============================

@dp.callback_query(lambda c: c.data.startswith("refresh:"))
async def refresh(callback: CallbackQuery):

    address = callback.data.split(":")[1]

    data = await fetch_token_data(address)

    if not data:
        await callback.answer("Failed to refresh")
        return

    text = (
        f"<b>Updated 🔄</b>\n\n"
        f"💰 Price: ${fmt(data.get('price'))}\n"
        f"💧 Liquidity: ${fmt(data.get('liquidity'))}\n"
        f"📊 Volume: ${fmt(data.get('volume'))}\n"
    )

    await callback.message.edit_text(
        text,
        reply_markup=build_keyboard(address)
    )

    await callback.answer()


# ==============================
# CHART BUTTON (FIXED)
# ==============================

@dp.callback_query(lambda c: c.data.startswith("chart:"))
async def chart(callback: CallbackQuery):

    address = callback.data.split(":")[1]

    # For now: send chart link (we'll upgrade to image engine next step)
    url = f"https://www.geckoterminal.com/ton/pools/{address}"

    await callback.message.answer(
        f"📊 Chart:\n{url}"
    )

    await callback.answer()


# ==============================
# START BOT
# ==============================

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
