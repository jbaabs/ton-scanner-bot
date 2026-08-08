import asyncio
import logging
import aiohttp
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO

from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

BOT_TOKEN = "8835642161:AAEX3XjrRtlQpn_BeycLhDQLao0lIhT-f3s"

logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# =========================
# SAFE FETCH TOKEN DATA (FIXED)
# =========================

async def fetch_token_data(query: str):
    url = f"https://api.geckoterminal.com/api/v2/search?query={query}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:

            # ✅ FIX: handle bad responses
            if resp.status != 200:
                return None

            content_type = resp.headers.get("Content-Type", "")
            if "application/json" not in content_type:
                return None

            data = await resp.json()

    try:
        pair = data["data"][0]["attributes"]

        return {
            "name": pair["name"],
            "price": float(pair["price_usd"]),
            "liquidity": float(pair["reserve_in_usd"]),
            "volume": float(pair["volume_usd"]["h24"]),
            "address": data["data"][0]["id"].split("_")[-1]
        }
    except:
        return None


# =========================
# FETCH OHLC (CHART DATA)
# =========================

async def fetch_ohlc(address, timeframe="5m"):
    url = f"https://api.geckoterminal.com/api/v2/networks/ton/pools/{address}/ohlcv/{timeframe}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()

    ohlc = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    if not ohlc:
        return None

    df = pd.DataFrame(ohlc, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="s")

    return df


# =========================
# GENERATE CHART IMAGE
# =========================

def generate_chart(df, scan_price=None):
    plt.style.use("dark_background")

    plt.figure(figsize=(10, 5))
    plt.plot(df["ts"], df["close"], linewidth=2)

    if scan_price:
        plt.axhline(y=scan_price, linestyle="--")

    plt.title("GRX Chart")
    plt.xlabel("Time")
    plt.ylabel("Price")

    buf = BytesIO()
    plt.savefig(buf, format="png")
    plt.close()

    buf.seek(0)
    return buf


# =========================
# FORMAT NUMBERS
# =========================

def fmt(n):
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.2f}K"
    return f"{n:.6f}"


# =========================
# KEYBOARD
# =========================

def build_keyboard(address):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Buy", url=f"https://www.geckoterminal.com/ton/pools/{address}")],
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{address}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{address}")
        ]
    ])


# =========================
# AUTO SCAN
# =========================

@dp.message()
async def handle_message(message: Message):
    symbol = message.text.strip()

    await message.answer(f"🔍 Scanning {symbol} on TON...")

    data = await fetch_token_data(symbol)

    if not data:
        await message.answer("❌ Token not found on TON")
        return

    text = (
        f"<b>{symbol.upper()} scanned ✅</b>\n\n"
        f"💰 Price: ${fmt(data['price'])}\n"
        f"💧 Liquidity: ${fmt(data['liquidity'])}\n"
        f"📊 Volume: ${fmt(data['volume'])}\n"
        f"👥 Holders: N/A\n\n"
        f"🛰 Source: GeckoTerminal"
    )

    await message.answer(text, reply_markup=build_keyboard(data["address"]))


# =========================
# CHART BUTTON
# =========================

@dp.callback_query(lambda c: c.data.startswith("chart"))
async def send_chart(callback: CallbackQuery):
    address = callback.data.split(":")[1]

    df = await fetch_ohlc(address)

    if df is None:
        await callback.message.answer("❌ No chart data available")
        return

    chart = generate_chart(df)

    await callback.message.answer_photo(chart, caption="📊 GRX Chart")


# =========================
# REFRESH BUTTON (FIXED)
# =========================

@dp.callback_query(lambda c: c.data.startswith("refresh"))
async def refresh(callback: CallbackQuery):
    address = callback.data.split(":")[1]

    url = f"https://api.geckoterminal.com/api/v2/networks/ton/pools/{address}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                await callback.message.answer("❌ Refresh failed")
                return

            data = await resp.json()

    try:
        attr = data["data"]["attributes"]

        text = (
            f"<b>Updated 🔄</b>\n\n"
            f"💰 Price: ${fmt(float(attr['price_usd']))}\n"
            f"💧 Liquidity: ${fmt(float(attr['reserve_in_usd']))}\n"
            f"📊 Volume: ${fmt(float(attr['volume_usd']['h24']))}\n"
        )

        await callback.message.answer(text)

    except:
        await callback.message.answer("❌ Refresh parsing failed")


# =========================
# RUN
# =========================

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
