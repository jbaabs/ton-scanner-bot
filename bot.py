import asyncio
import logging
import aiohttp
import time
from io import BytesIO
import matplotlib.pyplot as plt

from aiogram import Bot, Dispatcher
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
# MEMORY STORE
# =========================
SCAN_HISTORY = {}  # symbol -> {first_scan, calls}

# =========================
# HELPERS
# =========================
def fmt(v):
    try:
        return f"{float(v):,.6f}"
    except:
        return "0"

def build_keyboard(address):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{address or 'none'}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{address or 'none'}"),
        ]
    ])

# =========================
# RESOLVER (CORE FIX)
# =========================
async def resolve_token(session, query):
    url = f"https://api.geckoterminal.com/api/v2/search?query={query}"

    try:
        async with session.get(url) as r:
            if r.status != 200:
                logging.warning(f"Gecko bad status: {r.status}")
                return None

            data = await r.json()
            pairs = data.get("data", [])

            # Filter TON only
            ton_pairs = [
                p for p in pairs
                if p["attributes"].get("network") == "ton"
            ]

            if not ton_pairs:
                return None

            # Pick best liquidity
            best = sorted(
                ton_pairs,
                key=lambda x: float(x["attributes"].get("reserve_in_usd", 0) or 0),
                reverse=True
            )[0]["attributes"]

            return {
                "address": best.get("address"),
                "price": best.get("base_token_price_usd"),
                "liquidity": best.get("reserve_in_usd"),
                "volume": best.get("volume_usd", {}).get("h24"),
                "dex": best.get("dex_id"),
                "symbol": best.get("base_token_symbol"),
                "name": best.get("base_token_name")
            }

    except Exception as e:
        logging.warning(f"Resolver error: {e}")
        return None

# =========================
# DATA FETCH
# =========================
async def fetch_token_data(query):
    async with aiohttp.ClientSession() as session:
        resolved = await resolve_token(session, query)

        if not resolved:
            return None

        dex = resolved.get("dex")

        if dex == "dedust":
            source = "DeDust"
        elif dex == "stonfi":
            source = "STON.fi"
        else:
            source = "GeckoTerminal"

        return {
            "address": resolved.get("address"),
            "price": resolved.get("price"),
            "liquidity": resolved.get("liquidity"),
            "volume": resolved.get("volume"),
            "symbol": resolved.get("symbol") or query,
            "name": resolved.get("name") or query,
            "source": source
        }

# =========================
# CHART (BASE)
# =========================
async def generate_chart(symbol):
    history = SCAN_HISTORY.get(symbol, {})
    calls = history.get("calls", 1)

    x = list(range(calls))
    y = [1 + (i * 0.05) for i in x]

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
# MESSAGE HANDLER
# =========================
@dp.message()
async def handle_message(message: Message):
    query = message.text.strip()

    await message.answer(f"🔎 Scanning <b>{query}</b> on TON...")

    data = await fetch_token_data(query)

    if not data:
        await message.answer("❌ Token not found on TON")
        return

    symbol = data.get("symbol")

    # Track scans
    if symbol not in SCAN_HISTORY:
        SCAN_HISTORY[symbol] = {
            "first_scan": time.time(),
            "calls": 1
        }
    else:
        SCAN_HISTORY[symbol]["calls"] += 1

    text = (
        f"<b>{data.get('name')} ({symbol})</b>\n\n"
        f"💰 Price: ${fmt(data.get('price'))}\n"
        f"💧 Liquidity: ${fmt(data.get('liquidity'))}\n"
        f"📊 Volume: ${fmt(data.get('volume'))}\n\n"
        f"🛰 Source: {data.get('source')}"
    )

    await message.answer(
        text,
        reply_markup=build_keyboard(data.get("address"))
    )

# =========================
# CALLBACK HANDLER
# =========================
@dp.callback_query()
async def handle_callback(callback: CallbackQuery):
    data = callback.data

    if data.startswith("chart"):
        _, address = data.split(":")
        chart = await generate_chart(address)

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
