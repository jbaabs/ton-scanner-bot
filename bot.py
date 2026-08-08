import asyncio
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# =========================
# START COMMAND
# =========================
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("GRX Scanner is live 🚀")

# =========================
# AUTO SCAN HANDLER
# =========================
@dp.message()
async def auto_scan(message: types.Message):
    text = message.text.strip()

    # basic filter
    if len(text) < 2:
        return

    token = text.upper()

    msg = await message.answer(f"🔎 Scanning {token} on TON...")

    # fake loading first (we'll replace with APIs next)
    await asyncio.sleep(1)

    caption = f"""
<b>{token} scanned ✅</b>

💰 Price: Loading...
💧 Liquidity: Loading...
📊 Volume: Loading...
👥 Holders: Loading...
"""

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🟢 Buy", url="https://ston.fi")
        ],
        [
            InlineKeyboardButton(text="📊 Chart", callback_data=f"chart_{token}"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh_{token}")
        ]
    ])

    await msg.edit_text(caption, reply_markup=keyboard)

# =========================
# BUTTON HANDLERS
# =========================
@dp.callback_query(lambda c: c.data.startswith("chart_"))
async def chart_handler(callback: types.CallbackQuery):
    token = callback.data.split("_")[1]

    await callback.message.answer(f"📊 Chart for {token} coming soon")

    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith("refresh_"))
async def refresh_handler(callback: types.CallbackQuery):
    token = callback.data.split("_")[1]

    await callback.answer("Refreshing...")

    await callback.message.edit_text(f"""
<b>{token} updated 🔄</b>

💰 Price: Loading...
💧 Liquidity: Loading...
📊 Volume: Loading...
👥 Holders: Loading...
""", reply_markup=callback.message.reply_markup)

# =========================
# RUN BOT
# =========================
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
