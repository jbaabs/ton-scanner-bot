"""
Pull every custom_emoji_id out of a Telegram custom emoji pack (an "addemoji"
link, e.g. https://t.me/addemoji/NoNameDev) and print a ready-to-paste
CUSTOM_EMOJI_MAP_JSON value for your .env file.

Run this yourself on a machine/host where TELEGRAM_BOT_TOKEN is already set
(same .env the bot uses) — it needs your real bot token to call Telegram's
getStickerSet API, so it isn't something to run from anywhere but your own
environment.

Usage:
    python tools/fetch_emoji_pack.py NoNameDev

Then map each printed emoji to one of the bot's keys (flag, money, chart,
drop, people, bars, buy, sell, swap, rocket, link) and paste the resulting
JSON into your .env as:

    CUSTOM_EMOJI_MAP_JSON={"money": "5361541227144878472", "rocket": "..."}

Notes:
- Custom emoji only render for recipients if the Telegram account that owns
  the bot has Telegram Premium (or the bot purchased a Fragment username).
  Without that, the plain fallback emoji is shown instead — the bot keeps
  working either way.
"""

import sys
import asyncio
import json
import os

from dotenv import load_dotenv
from aiogram import Bot

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")

KEY_HINTS = {
    "flag": "🚩", "money": "💰", "chart": "📈", "drop": "💧",
    "people": "👥", "bars": "📊", "buy": "🟢", "sell": "🔴",
    "swap": "🔁", "rocket": "🚀", "link": "🔗",
}


async def main(pack_name: str):
    if not BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN is not set in your environment/.env — aborting.")
        return

    bot = Bot(BOT_TOKEN)
    sticker_set = await bot.get_sticker_set(pack_name)

    if sticker_set.sticker_type != "custom_emoji":
        print(f"'{pack_name}' is not a custom emoji pack (type={sticker_set.sticker_type})")
        return

    print(f"\n{len(sticker_set.stickers)} custom emoji found in '{pack_name}':\n")
    suggested_map = {}
    for sticker in sticker_set.stickers:
        alt = sticker.emoji
        cid = sticker.custom_emoji_id
        # best-effort auto-suggestion by matching the fallback char to a known key
        suggested_key = next((k for k, v in KEY_HINTS.items() if v == alt), None)
        print(f"  {alt}  id={cid}" + (f"   (suggested key: {suggested_key})" if suggested_key else ""))
        if suggested_key and suggested_key not in suggested_map:
            suggested_map[suggested_key] = cid

    print("\nSuggested CUSTOM_EMOJI_MAP_JSON (edit/reassign keys as you like):\n")
    print(json.dumps(suggested_map))

    await bot.session.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python tools/fetch_emoji_pack.py <pack_name>")
        print("       e.g. python tools/fetch_emoji_pack.py NoNameDev")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
