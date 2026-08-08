import asyncio
import aiohttp
import time
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ================= CONFIG =================
BOT_TOKEN = "8835642161:AAEX3XjrRtlQpn_BeycLhDQLao0lIhT-f3s"

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()

# ================= STORAGE =================
SCAN_HISTORY = {}

# ================= VALIDATION =================
def is_valid_ton_address(text: str):
    return text.startswith("EQ") or text.startswith("UQ")

def is_valid_ticker(text: str):
    return text.isalpha() and 2 <= len(text) <= 10

# ================= TRENDING =================
def can_count_scan(token, user_id, chat_id):
    now = time.time()
    for scan in SCAN_HISTORY.get(token, []):
        if (
            scan["user"] == user_id and
            scan["chat"] == chat_id and
            now - scan["time"] < 86400
        ):
            return False
    return True

def record_scan(token, user_id, chat_id, is_private):
    if not can_count_scan(token, user_id, chat_id):
        return

    points = 1.0 if is_private else 0.5

    SCAN_HISTORY.setdefault(token, []).append({
        "user": user_id,
        "chat": chat_id,
        "time": time.time(),
        "points": points
    })

def get_trending_score(token):
    now = time.time()
    return round(sum(
        s["points"]
        for s in SCAN_HISTORY.get(token, [])
        if now - s["time"] <= 86400
    ), 2)

# ================= DATA FETCH =================
async def fetch_token_data(query):
    async with aiohttp.ClientSession() as session:

        async def dedust():
            try:
                url = f"https://api.dedust.io/v2/assets/{query}"
                async with session.get(url) as r:
                    if r.status == 200:
                        d = await r.json()
                        return {
                            "source": "DeDust",
                            "price": d.get("price"),
                            "liquidity": d.get("liquidity"),
                            "address": query
                        }
            except:
                return None

        async def ston():
            try:
                url = f"https://api.ston.fi/v1/assets/{query}"
                async with session.get(url) as r:
                    if r.status == 200:
                        d = await r.json()
                        return {
                            "source": "STON.fi",
                            "price": d.get("price"),
                            "liquidity": d.get("liquidity"),
                            "address": query
                        }
            except:
                return None

        async def gecko():
            try:
                url = f"https://api.geckoterminal.com/api/v2/search?query={query}"
                async with session.get(url) as r:
                    if r.status != 200:
                        return None

                    d = await r.json()
                    pairs = d.get("data", [])

                    if not pairs:
                        return None

                    best = sorted(
                        pairs,
                        key=lambda x: float(x["attributes"].get("reserve_in_usd", 0) or 0),
                        reverse=True
                    )[0]["attributes"]

                    return {
                        "source": "Gecko",
                        "price": best.get("base_token_price_usd"),
                        "liquidity": best.get("reserve_in_usd"),
                        "address": best.get("address")
                    }
            except:
                return None

        results = await asyncio.gather(dedust(), ston(), gecko())
        valid = [r for r in results if r]

        if not valid:
            return None

        return max(valid, key=lambda x: float(x.get("liquidity") or 0))

# ================= UI =================
def build_keyboard(token):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Chart", callback_data=f"chart:{token}")],
        [InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{token}")]
    ])

# ================= HANDLER =================
@dp.message()
async def handle_message(message: types.Message):
    text = message.text.strip()

    # ✅ FIXED VALIDATION (this was your crash)
    if not (is_valid_ton_address(text) or is_valid_ticker(text)):
        return

    data = await fetch_token_data(text)
    if not data:
        return await message.answer("❌ Token not found")

    record_scan(
        text,
        message.from_user.id,
        message.chat.id,
        message.chat.type == "private"
    )

    score = get_trending_score(text)

    msg = f"""
<b>{text}</b>
💰 Price: {data.get('price')}
💧 Liquidity: {data.get('liquidity')}
📡 Source: {data.get('source')}

🔥 Trending Score: {score}
"""

    await message.answer(msg, reply_markup=build_keyboard(text))

# ================= CALLBACKS =================
@dp.callback_query()
async def callbacks(call: types.CallbackQuery):
    action, token = call.data.split(":")

    if action == "refresh":
        data = await fetch_token_data(token)
        if not data:
            return await call.answer("Failed to refresh", show_alert=True)

        await call.message.edit_text(
            f"<b>{token}</b>\n🔄 Refreshed\n💰 Price: {data.get('price')}"
        )

    elif action == "chart":
        await call.message.answer("📊 Chart engine reconnecting next step")

# ================= RUN =================
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
