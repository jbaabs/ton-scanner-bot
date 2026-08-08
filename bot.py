import asyncio
import aiohttp
import time
import sqlite3
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

# ENV VARIABLES
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "database.db"

# DEBUG (optional)
print("TOKEN:", BOT_TOKEN)

# BOT INIT (FIXED FOR AIROGRAM 3.7+)
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()

# ----------------------------
# DB SETUP
# ----------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            token TEXT,
            timestamp REAL
        )
    """)
    conn.commit()
    conn.close()
