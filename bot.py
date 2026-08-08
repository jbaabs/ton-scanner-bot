import asyncio
import logging
import aiohttp

from aiogram import Bot, Dispatcher
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ==============================
# CONFIG
# ==============================

BOT_TOKEN = "PASTE_YOUR_TOKEN_HERE"

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
                logging.warning(f"Gecko bad status: {resp.status}")
                return None

            # SAFE JSON parse
            try:
                data = await resp.json()
            except:
                logging.warning("Gecko returned non-JSON")
                return None

            pairs = data.get("data", [])
            if not pairs:
                return None

            pair = pairs[0].get("attributes", {})

            return {
                "price": pair.get("base_token_price_usd"),
                "liquidity": pair.get("reserve_in_usd"),
                "volume": pair.get("volume_usd", {}).get("h24"),
                "address": pair.get("address"),  # MAY BE NONE → SAFE
                "source": "GeckoTerminal"
            }

    except Exception as e:
        logging.error(f"Gecko error: {e}")
        return None


# STUB SOURCES (ready for step 4 expansion)

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
            fetch_geckoterminal  # fallback
        ]

        for source in sources:
            try:
                data = await source(session, query)

                if data:
                    logging.info(f"Data from {data.get('source')}")
                    return data

            except Exception as e:
                logging.error(f"Source failed: {e}")
                continue

    return None


# ==============================
# MESSAGE HANDLER
# ==============================

@dp.message()
async def handle_message(message: Message):

    if not message.text:
        return

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
        f"🛰 Source: {data.get('source', 'Unknown')}"
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
        f"📊 Volume: ${fmt(data.get('volume'))}"
    )

    await callback.message.edit_text(
        text,
        reply_markup=build_keyboard(address)
    )

    await callback.answer()


# ==============================
# CHART BUTTON
# ==============================

@dp.callback_query(lambda c: c.data.startswith("chart:"))
async def chart(callback: CallbackQuery):

    address = callback.data.split(":")[1]

    if not address:
        await callback.answer("No chart available")
        return

    url = f"https://www.geckoterminal.com/ton/pools/{address}"

    await callback.message.answer(f"📊 Chart:\n{url}")

    await callback.answer()


# ==============================
# START
# ==============================

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
