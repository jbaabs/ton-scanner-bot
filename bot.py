import asyncio
import logging
import aiohttp
import time
from io import BytesIO
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# =========================
# CONFIG
# =========================
BOT_TOKEN = "8835642161:AAEX3XjrRtlQpn_BeycLhDQLao0lIhT-f3s"

logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# =========================
# MEMORY STORE (scan history)
# =========================
SCAN_HISTORY = {}  # {symbol: {"first_scan": timestamp, "calls": int}}

# =========================
# HELPERS
# =========================
def fmt(value):
    try:
        return f"{float(value):,.6f}"
    except:
        return "0"

def build_keyboard(address=None):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{address or 'none'}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{address or 'none'}"),
        ]
    ])
    return kb

# =========================
# DATA FETCHING
# =========================
async def fetch_dedust(session, query):
    try:
        url = f"https://api.dedust.io/v2/assets/{query}"
        async with session.get(url) as r:
            if r.status != 200:
                return None
            data = await r.json()

            return {
                "price": data.get("price", 0),
                "liquidity": data.get("liquidity", 0),
                "volume": data.get("volume24h", 0),
                "address": query,
                "source": "DeDust"
            }
    except:
        return None

async def fetch_ston(session, query):
    try:
        url = f"https://api.ston.fi/v1/assets/{query}"
        async with session.get(url) as r:
            if r.status != 200:
                return None
            data = await r.json()

            return {
                "price": data.get("price", 0),
                "liquidity": data.get("liquidity", 0),
                "volume": data.get("volume24h", 0),
                "address": query,
                "source": "STON.fi"
            }
    except:
        return None

async def fetch_gecko(session, query):
    try:
        url = f"https://api.geckoterminal.com/api/v2/search?query={query}"
        async with session.get(url) as r:
            if r.status != 200:
                logging.warning(f"Gecko bad status: {r.status}")
                return None

            data = await r.json()
            pairs = data.get("data", [])

            if not pairs:
                return None

            pair = pairs[0]["attributes"]

            return {
                "price": pair.get("base_token_price_usd", 0),
                "liquidity": pair.get("reserve_in_usd", 0),
                "volume": pair.get("volume_usd", {}).get("h24", 0),
                "address": pair.get("address", None),
                "source": "GeckoTerminal"
            }
    except:
        return None

async def fetch_token_data(query):
    async with aiohttp.ClientSession() as session:

        # Priority order
        for source in [fetch_dedust, fetch_ston, fetch_gecko]:
            data = await source(session, query)
            if data:
                return data

    return None

# =========================
# CHART GENERATION
# =========================
async def generate_chart(symbol):
    history = SCAN_HISTORY.get(symbol, {})
    scans = history.get("calls", 1)

    x = list(range(scans))
    y = [i * 1.05 for i in x]  # placeholder growth

    plt.figure()
    plt.plot(x, y)

    # GRX scan line (first scan)
    plt.axvline(x=0)

    buf = BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()

    return buf

# =========================
# HANDLERS
# =========================
@dp.message()
async def handle_message(message: Message):
    symbol = message.text.strip()

    await message.answer(f"🔎 Scanning {symbol} on TON...")

    data = await fetch_token_data(symbol)

    if not data:
        await message.answer("❌ Token not found on TON")
        return

    # Track scans
    if symbol not in SCAN_HISTORY:
        SCAN_HISTORY[symbol] = {
            "first_scan": time.time(),
            "calls": 1
        }
    else:
        SCAN_HISTORY[symbol]["calls"] += 1

    text = (
        f"<b>{symbol.upper()} scanned ✅</b>\n\n"
        f"💰 Price: ${fmt(data.get('price'))}\n"
        f"💧 Liquidity: ${fmt(data.get('liquidity'))}\n"
        f"📊 Volume: ${fmt(data.get('volume'))}\n"
        f"👥 Holders: N/A\n\n"
        f"🛰 Source: {data.get('source')}"
    )

    address = data.get("address")

    await message.answer(
        text,
        reply_markup=build_keyboard(address)
    )

@dp.callback_query()
async def handle_callbacks(callback: CallbackQuery):
    data = callback.data

    if data.startswith("chart"):
        symbol = "unknown"

        chart = await generate_chart(symbol)
        await callback.message.answer_photo(chart, caption="📊 Chart")

    elif data.startswith("refresh"):
        await callback.message.answer("🔄 Refreshing...")

# =========================
# START
# =========================
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
