import os
import re
import html
import asyncio
import aiohttp

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.client.default import DefaultBotProperties

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TONAPI_KEY = os.getenv("TONAPI_KEY", "")
DEX_API = "https://api.dexscreener.com/latest/dex/tokens"
TONAPI_BASE = "https://tonapi.io/v2"

ADDR_FRIENDLY_RE = re.compile(r"^[EU]Q[A-Za-z0-9_\-]{46}$")
ADDR_RAW_RE = re.compile(r"^(0|[-1]):[0-9a-fA-F]{64}$")

def is_valid_ton_address(text: str) -> bool:
    text = text.strip()
    return bool(ADDR_FRIENDLY_RE.match(text) or ADDR_RAW_RE.match(text))

def tonapi_headers():
    headers = {"Accept": "application/json"}
    if TONAPI_KEY:
        headers["Authorization"] = f"Bearer {TONAPI_KEY}"
    return headers

async def get_jetton_info(session, address):
    url = f"{TONAPI_BASE}/jettons/{address}"
    try:
        async with session.get(url, headers=tonapi_headers()) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except Exception:
        return None

async def get_dex_data(session, address):
    url = f"{DEX_API}/{address}"
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data.get("pairs") or []
    except Exception:
        return None

def fmt_usd(v):
    if v is None:
        return "N/A"
    try:
        v = float(v)
        if v >= 1_000_000:
            return f"${v / 1_000_000:.2f}M"
        if v >= 1_000:
            return f"${v / 1_000:.1f}K"
        return f"${v:.2f}"
    except Exception:
        return "N/A"

def fmt_price(v):
    if v is None:
        return "N/A"
    try:
        p = float(v)
        if p < 0.000001:
            return f"${p:.12f}"
        if p < 0.0001:
            return f"${p:.9f}"
        if p < 0.01:
            return f"${p:.6f}"
        if p < 1:
            return f"${p:.4f}"
        return f"${p:.2f}"
    except Exception:
        return "N/A"

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()

@dp.message(Command("start", "help"))
async def start_cmd(message: Message):
    await message.answer(
        "<b>TON Scanner</b>\n\n"
        "Send a TON contract address starting with EQ or UQ."
    )

@dp.message(F.text)
async def scan_cmd(message: Message):
    address = message.text.strip()

    if not is_valid_ton_address(address):
        await message.answer("Send a valid TON address starting with EQ or UQ.")
        return

    wait = await message.answer("Scanning...")

    try:
        async with aiohttp.ClientSession() as session:
            jetton = await get_jetton_info(session, address)
            pairs = await get_dex_data(session, address)

        if not jetton and not pairs:
            await wait.edit_text("Token not found.")
            return

        name = "Unknown"
        symbol = "???"
        holders = "N/A"
        verification = "none"
        mintable = "Unknown"

        if jetton:
            meta = jetton.get("metadata", {})
            name = meta.get("name", "Unknown")
            symbol = meta.get("symbol", "???")
            holders = jetton.get("holders_count", "N/A")
            verification = jetton.get("verification", "none")
            mintable = "Yes" if jetton.get("mintable") else "No"

        price = "N/A"
        liq = "N/A"
        vol = "N/A"
        mcap = "N/A"
        change1h = "N/A"
        dex_url = None

        if pairs:
            best = sorted(
                pairs,
                key=lambda p: (p.get("liquidity") or {}).get("usd", 0),
                reverse=True,
            )[0]
            price = fmt_price(best.get("priceUsd"))
            liq = fmt_usd((best.get("liquidity") or {}).get("usd"))
            vol = fmt_usd((best.get("volume") or {}).get("h24"))
            mcap = fmt_usd(best.get("marketCap"))
            c1 = (best.get("priceChange") or {}).get("h1")
            if c1 is not None:
                change1h = f"{float(c1):+.2f}%"
            dex_url = best.get("url")

        text = (
            f"<b>{html.escape(str(name))}</b> ${html.escape(str(symbol))}\n"
            f"<code>{html.escape(address)}</code>\n\n"
            f"💰 <b>{price}</b>\n"
            f"💧 LIQ <b>{liq}</b>\n"
            f"🪙 VOL <b>{vol}</b>\n"
            f"📊 MCAP <b>{mcap}</b>\n"
            f"📈 1h <b>{html.escape(str(change1h))}</b>\n\n"
            f"👥 Holders: <b>{html.escape(str(holders))}</b>\n"
            f"✅ Verification: <b>{html.escape(str(verification))}</b>\n"
            f"🏦 Mintable: <b>{html.escape(str(mintable))}</b>\n"
        )

        if dex_url:
            text += f'\n🔗 <a href="{html.escape(str(dex_url))}">DexScreener</a>'

        text += "\n\n<i>DexScreener + TonAPI</i>"

        await wait.edit_text(text, disable_web_page_preview=True)

    except Exception as e:
        await wait.edit_text(f"Error: {html.escape(str(e))}")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
