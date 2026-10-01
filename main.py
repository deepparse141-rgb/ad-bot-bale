import asyncio
import aiohttp
from aiohttp import web
import re
import time
import json
import os
import logging
import sys
import sqlite3
import random
import hashlib
import base64
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiobale import Client, Dispatcher
from aiobale.types import Message, Chat, InfoMessage, Peer
from aiobale.filters import IsGift
from aiobale.enums import ChatType
from aiobale.methods import OpenGiftPacket
import traceback
from functools import lru_cache
from collections import defaultdict, deque
from logging.handlers import RotatingFileHandler
import signal
import inspect

# ==================== نام بات ====================
BOT_NAME = os.getenv("BOT_NAME", "هایپرسلف")
BOT_USERNAME = os.getenv("BOT_USERNAME", "Igug0877hbot")

# ==================== غیرفعال‌سازی پینگ داخلی ====================
try:
    from aiobale.network import WebSocketClient
    original_ping = WebSocketClient._ping
    async def patched_ping(self):
        try:
            await asyncio.wait_for(original_ping(self), timeout=5.0)
        except Exception:
            pass
    WebSocketClient._ping = patched_ping
except (ImportError, AttributeError):
    pass

# ==================== لاگ‌گیری ====================
LOG_FILE = 'bot.log'
MAX_LOG_SIZE = 5 * 1024 * 1024
BACKUP_COUNT = 3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

class SafeConsoleHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            stream = self.stream
            stream.write(msg + self.terminator)
            self.flush()
        except UnicodeEncodeError:
            try:
                msg = self.format(record).encode('ascii', errors='ignore').decode('ascii')
                stream = self.stream
                stream.write(msg + self.terminator)
                self.flush()
            except:
                pass
        except Exception:
            self.handleError(record)

console_handler = SafeConsoleHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logging.getLogger("aiobale").setLevel(logging.ERROR)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

def custom_aiobale_error_handler(*args, **kwargs):
    msg = args[0] if args else ""
    if any(x in msg for x in [
        "Cannot write to closing transport", "Connection lost", "Broken pipe", "EOF",
        "Unknown wire type", "list index out of range", "user_rate_limited",
        "PermissionDenied", "message to delete not found"
    ]):
        logging.getLogger("aiobale").debug(msg)
        return
    if "Unauthorized" in msg or "AUTH_KEY_UNREGISTERED" in msg:
        logger.warning(f"Session auth: {msg}")
        return
    original_error_handler = logging.getLogger("aiobale").error
    original_error_handler(*args, **kwargs)

logging.getLogger("aiobale").error = custom_aiobale_error_handler

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# ─── پایان بخش ۱ ───
# ==================== تنظیمات اصلی ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "216295883:iIMb6WoZgxE46qUCGrmqPDhjMpIkk23TmtU")
ADMIN_ID = int(os.getenv("ADMIN_ID", "1530477937"))
SUPPORT_ID = os.getenv("SUPPORT_ID", "@HyperSi")

if not BOT_TOKEN:
    logger.critical("❌ BOT_TOKEN تنظیم نشده است!")
    sys.exit(1)
if not ADMIN_ID:
    logger.critical("❌ ADMIN_ID تنظیم نشده است!")
    sys.exit(1)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_data.db")
SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

BASE_URL = f"https://tapi.bale.ai/bot{BOT_TOKEN}"

CONCURRENT_GIFT = 5
CONCURRENT_SPAM = 5
CONCURRENT_BROADCAST = 5
CONNECTOR_LIMIT = 300
POLLING_TIMEOUT = 30
UPDATE_QUEUE_SIZE = 10000
WORKER_COUNT = 100
CACHE_SIZE = 200
USER_CACHE_TTL = 300

PERSIAN_DIGITS = str.maketrans('۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩', '01234567890123456789')

NEW_USER_CACHE = {}

try:
    import orjson
    def json_dumps(data):
        return orjson.dumps(data).decode('utf-8')
    def json_loads(data):
        return orjson.loads(data)
except ImportError:
    def json_dumps(data):
        return json.dumps(data, ensure_ascii=False)
    def json_loads(data):
        return json.loads(data)

# ─── پایان بخش ۲ ───
# ==================== Connection Pool ====================
class SQLitePool:
    def __init__(self, db_path, max_connections=10):
        self.db_path = db_path
        self.max_connections = max_connections
        self._pool = asyncio.Queue(maxsize=max_connections)
        self._size = 0
        self._lock = asyncio.Lock()
        self._closed = False

    async def acquire(self):
        if self._closed:
            raise RuntimeError("Pool is closed")
        async with self._lock:
            if self._size < self.max_connections:
                conn = self._create_connection()
                self._size += 1
                return conn
        return await self._pool.get()

    def _create_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=20.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.row_factory = sqlite3.Row
        return conn

    async def release(self, conn):
        if self._closed:
            conn.close()
            return
        await self._pool.put(conn)

    async def close(self):
        self._closed = True
        while not self._pool.empty():
            conn = await self._pool.get()
            conn.close()

_pool = None

async def init_pool():
    global _pool
    _pool = SQLitePool(DB_PATH, max_connections=50)
    logger.info("Database pool initialized")

async def _run_query(query, params=(), fetchone=False, fetchall=False, commit=False):
    if _pool is None:
        await init_pool()
    conn = await _pool.acquire()
    try:
        cur = conn.execute(query, params)
        if commit:
            conn.commit()
        if fetchone:
            return cur.fetchone()
        if fetchall:
            return cur.fetchall()
        return cur
    finally:
        await _pool.release(conn)

# ==================== دیتابیس ====================
async def init_db():
    try:
        await _run_query("SELECT channel_id FROM forced_channels LIMIT 1", fetchone=True)
        logger.info("Table forced_channels exists with correct columns")
    except sqlite3.OperationalError as e:
        error_msg = str(e)
        if "no such column" in error_msg:
            logger.warning("Table forced_channels has wrong structure, recreating...")
            await _run_query("DROP TABLE IF EXISTS forced_channels", commit=True)
        elif "no such table" in error_msg:
            logger.warning("Table forced_channels does not exist, will create it.")
        else:
            raise e

    try:
        await _run_query("SELECT start_date FROM subscriptions LIMIT 1", fetchone=True)
        logger.info("Table subscriptions exists with correct columns")
    except sqlite3.OperationalError as e:
        error_msg = str(e)
        if "no such column" in error_msg:
            logger.warning("Table subscriptions has wrong structure, recreating...")
            await _run_query("DROP TABLE IF EXISTS subscriptions", commit=True)
        elif "no such table" in error_msg:
            logger.warning("Table subscriptions does not exist, will create it.")
        else:
            raise e

    await _run_query('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, data TEXT NOT NULL)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS banned (user_id INTEGER PRIMARY KEY)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS auto_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        word TEXT NOT NULL, interval_minutes INTEGER NOT NULL, UNIQUE(chat_id, user_id))''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS group_settings (group_id INTEGER PRIMARY KEY, settings TEXT NOT NULL)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS active_sessions (
        user_id INTEGER PRIMARY KEY, session_file TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS subscriptions (
        user_id INTEGER PRIMARY KEY, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
        is_active INTEGER DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS test_mode (
        user_id INTEGER PRIMARY KEY, start_time TEXT NOT NULL, end_time TEXT NOT NULL, is_active INTEGER DEFAULT 1)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS referrals (
        user_id INTEGER PRIMARY KEY, referral_code TEXT UNIQUE NOT NULL,
        total_invites INTEGER DEFAULT 0, pending_invites INTEGER DEFAULT 0,
        earned_days INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS referral_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT, referrer_id INTEGER NOT NULL, referred_id INTEGER NOT NULL,
        action_type TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS forced_channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT, channel_id INTEGER NOT NULL UNIQUE,
        invite_link TEXT NOT NULL, channel_name TEXT,
        error_message TEXT DEFAULT 'لطفاً ابتدا در کانال زیر عضو شوید.',
        is_active INTEGER DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''', commit=True)

    await _run_query('''CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)''', commit=True)
    await _run_query('''CREATE INDEX IF NOT EXISTS idx_referrals_code ON referrals(referral_code)''', commit=True)
    await _run_query('''CREATE INDEX IF NOT EXISTS idx_referral_actions_referrer ON referral_actions(referrer_id)''', commit=True)
    await _run_query('''CREATE INDEX IF NOT EXISTS idx_forced_channels_channel_id ON forced_channels(channel_id)''', commit=True)
    logger.info("Database initialized.")

# ─── پایان بخش ۳ ───
# ==================== توابع کانال اجباری ====================
async def add_forced_channel(channel_id: int, invite_link: str, channel_name: str = None, error_message: str = None):
    if error_message is None:
        error_message = "لطفاً ابتدا در کانال زیر عضو شوید."
    await _run_query("INSERT OR REPLACE INTO forced_channels (channel_id, invite_link, channel_name, error_message, is_active) VALUES (?, ?, ?, ?, 1)",
        (channel_id, invite_link, channel_name, error_message), commit=True)
    global forced_channels_cache
    forced_channels_cache = await get_active_forced_channels()

async def remove_forced_channel(channel_id: int):
    await _run_query("DELETE FROM forced_channels WHERE channel_id = ?", (channel_id,), commit=True)
    global forced_channels_cache
    forced_channels_cache = await get_active_forced_channels()

async def get_all_forced_channels():
    rows = await _run_query("SELECT channel_id, invite_link, channel_name, error_message, is_active FROM forced_channels", fetchall=True)
    return [{"channel_id": row[0], "invite_link": row[1], "channel_name": row[2], "error_message": row[3], "is_active": row[4]} for row in rows]

async def get_active_forced_channels():
    rows = await _run_query("SELECT channel_id, invite_link, channel_name, error_message FROM forced_channels WHERE is_active = 1", fetchall=True)
    return [{"channel_id": row[0], "invite_link": row[1], "channel_name": row[2], "error_message": row[3]} for row in rows]

async def toggle_forced_channel(channel_id: int, active: bool):
    await _run_query("UPDATE forced_channels SET is_active = ? WHERE channel_id = ?", (1 if active else 0, channel_id), commit=True)
    global forced_channels_cache
    forced_channels_cache = await get_active_forced_channels()

# ==================== توابع بررسی عضویت ====================
async def is_user_member_of_channel(user_id: int, channel_id: int) -> bool:
    try:
        url = f"{BASE_URL}/getChatMember"
        async with bot_session.post(url, json={"chat_id": channel_id, "user_id": user_id}) as resp:
            result = await resp.json()
            if result.get("ok"):
                status = result["result"].get("status")
                return status in ("member", "administrator", "creator")
            return False
    except Exception as e:
        logger.error(f"Error checking membership: {e}")
        return False

async def check_all_memberships(user_id: int) -> tuple:
    if not forced_channels_cache:
        return True, []
    not_member = []
    for ch in forced_channels_cache:
        if not await is_user_member_of_channel(user_id, ch["channel_id"]):
            not_member.append(ch)
    return len(not_member) == 0, not_member

# ==================== توابع دیتابیس کاربران ====================
async def db_get_user(user_id: int):
    row = await _run_query("SELECT data FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    return json_loads(row[0]) if row else None

async def db_set_user(user_id: int, data: dict):
    await _run_query("INSERT OR REPLACE INTO users (user_id, data) VALUES (?, ?)", (user_id, json_dumps(data)), commit=True)

async def db_get_all_users():
    rows = await _run_query("SELECT user_id, data FROM users", fetchall=True)
    return {row[0]: json_loads(row[1]) for row in rows}

async def db_is_banned(user_id: int) -> bool:
    row = await _run_query("SELECT 1 FROM banned WHERE user_id = ?", (user_id,), fetchone=True)
    return row is not None

async def db_add_banned(user_id: int):
    await _run_query("INSERT OR IGNORE INTO banned (user_id) VALUES (?)", (user_id,), commit=True)

async def db_remove_banned(user_id: int):
    await _run_query("DELETE FROM banned WHERE user_id = ?", (user_id,), commit=True)

async def db_get_all_banned():
    rows = await _run_query("SELECT user_id FROM banned", fetchall=True)
    return {row[0] for row in rows}

async def db_get_auto_tasks():
    rows = await _run_query("SELECT chat_id, user_id, word, interval_minutes FROM auto_tasks", fetchall=True)
    tasks = {}
    for row in rows:
        tasks[f"{row[0]}|{row[1]}"] = {"word": row[2], "interval": row[3]}
    return tasks

async def db_add_auto_task(chat_id: int, user_id: int, word: str, interval: int):
    await _run_query("INSERT OR REPLACE INTO auto_tasks (chat_id, user_id, word, interval_minutes) VALUES (?, ?, ?, ?)",
        (chat_id, user_id, word, interval), commit=True)

async def db_remove_auto_task(chat_id: int, user_id: int):
    await _run_query("DELETE FROM auto_tasks WHERE chat_id = ? AND user_id = ?", (chat_id, user_id), commit=True)

async def db_get_group_settings(group_id: int):
    row = await _run_query("SELECT settings FROM group_settings WHERE group_id = ?", (group_id,), fetchone=True)
    return json_loads(row[0]) if row else {}

async def db_set_group_settings(group_id: int, settings: dict):
    await _run_query("INSERT OR REPLACE INTO group_settings (group_id, settings) VALUES (?, ?)", (group_id, json_dumps(settings)), commit=True)

async def db_get_all_group_settings():
    rows = await _run_query("SELECT group_id, settings FROM group_settings", fetchall=True)
    return {row[0]: json_loads(row[1]) for row in rows}

async def db_add_active_session(user_id: int, session_file: str):
    await _run_query("INSERT OR REPLACE INTO active_sessions (user_id, session_file) VALUES (?, ?)", (user_id, session_file), commit=True)

async def db_remove_active_session(user_id: int):
    await _run_query("DELETE FROM active_sessions WHERE user_id = ?", (user_id,), commit=True)

async def db_get_all_active_sessions():
    rows = await _run_query("SELECT user_id, session_file FROM active_sessions", fetchall=True)
    return {row[0]: row[1] for row in rows}

# ==================== توابع تست ۳ ساعته ====================
async def activate_test_mode(user_id: int):
    now = datetime.now()
    end_time = now + timedelta(hours=3)
    await _run_query('''INSERT OR REPLACE INTO test_mode (user_id, start_time, end_time, is_active) VALUES (?, ?, ?, ?)''',
        (user_id, now.isoformat(), end_time.isoformat(), 1), commit=True)
    return end_time

async def deactivate_test_mode(user_id: int):
    await _run_query("UPDATE test_mode SET is_active = 0 WHERE user_id = ?", (user_id,), commit=True)

async def has_active_test(user_id: int) -> bool:
    row = await _run_query("SELECT end_time, is_active FROM test_mode WHERE user_id = ? AND is_active = 1", (user_id,), fetchone=True)
    if not row:
        return False
    return datetime.fromisoformat(row[0]) > datetime.now()

async def has_used_test_before(user_id: int) -> bool:
    row = await _run_query("SELECT 1 FROM test_mode WHERE user_id = ?", (user_id,), fetchone=True)
    return row is not None

async def get_test_remaining_time(user_id: int):
    row = await _run_query("SELECT end_time, is_active FROM test_mode WHERE user_id = ? AND is_active = 1", (user_id,), fetchone=True)
    if not row:
        return None, "❌ تست فعالی ندارید"
    end_time = datetime.fromisoformat(row[0])
    now = datetime.now()
    if end_time <= now:
        return None, "⏰ تست منقضی شده"
    remaining = end_time - now
    hours = remaining.seconds // 3600
    minutes = (remaining.seconds % 3600) // 60
    text = f"⏳ {hours} ساعت و {minutes} دقیقه" if hours > 0 else f"⏳ {minutes} دقیقه"
    return remaining.total_seconds(), text

async def test_mode_checker():
    try:
        while True:
            await asyncio.sleep(60)
            try:
                rows = await _run_query("SELECT user_id, end_time FROM test_mode WHERE is_active = 1", fetchall=True)
                now = datetime.now()
                for row in rows:
                    user_id = row[0]
                    end_time = datetime.fromisoformat(row[1])
                    if end_time <= now:
                        await deactivate_test_mode(user_id)
                        if user_id in sessions:
                            try:
                                if "client" in sessions[user_id]:
                                    await sessions[user_id]["client"].stop()
                                del sessions[user_id]
                            except:
                                pass
                        await db_remove_active_session(user_id)
                        try:
                            await send_bot_message(user_id, "⏰ تست ۳ ساعته شما به پایان رسید!\n\nربات به‌طور خودکار غیرفعال شد.\nبرای استفاده مجدد، اشتراک تهیه کنید.")
                        except:
                            pass
                        logger.info(f"Test mode expired for user {user_id}")
            except Exception as e:
                logger.error(f"Error in test mode checker: {e}")
    except asyncio.CancelledError:
        logger.info("Test mode checker cancelled")
        raise

# ==================== توابع اشتراک ====================
async def get_subscription(user_id: int):
    row = await _run_query("SELECT * FROM subscriptions WHERE user_id = ?", (user_id,), fetchone=True)
    return dict(row) if row else None

async def add_subscription(user_id: int, days: int):
    now = datetime.now()
    end_date = now + timedelta(days=days)
    existing = await get_subscription(user_id)
    if existing:
        old_end = datetime.fromisoformat(existing['end_date'])
        if old_end > now:
            end_date = old_end + timedelta(days=days)
    await _run_query('''INSERT OR REPLACE INTO subscriptions (user_id, start_date, end_date, is_active) VALUES (?, ?, ?, ?)''',
        (user_id, now.isoformat(), end_date.isoformat(), 1), commit=True)
    logger.info(f"Subscription added for user {user_id}: {days} days, ends at {end_date}")
    return end_date

async def has_active_subscription(user_id: int) -> bool:
    sub = await get_subscription(user_id)
    if not sub:
        return False
    return datetime.fromisoformat(sub['end_date']) > datetime.now() and sub.get('is_active', 1) == 1

async def get_remaining_time(user_id: int):
    sub = await get_subscription(user_id)
    if not sub:
        return None, "❌ اشتراکی ندارید", 0, 0, 0
    now = datetime.now()
    end_date = datetime.fromisoformat(sub['end_date'])
    if end_date <= now:
        return None, "⏰ منقضی شده", 0, 0, 0
    remaining = end_date - now
    days = remaining.days
    hours = remaining.seconds // 3600
    minutes = (remaining.seconds % 3600) // 60
    if days > 0:
        text = f"✨ {days} روز و {hours} ساعت و {minutes} دقیقه"
    elif hours > 0:
        text = f"✨ {hours} ساعت و {minutes} دقیقه"
    else:
        text = f"✨ {minutes} دقیقه"
    return remaining.total_seconds(), text, days, hours, minutes

# ==================== کش کاربران ====================
USER_CACHE = {}

async def get_user_cached(user_id: int):
    now = time.time()
    if user_id in USER_CACHE:
        data, ts = USER_CACHE[user_id]
        if now - ts < USER_CACHE_TTL:
            return data
        else:
            del USER_CACHE[user_id]
    data = await db_get_user(user_id)
    if data is not None:
        USER_CACHE[user_id] = (data, now)
        if len(USER_CACHE) > CACHE_SIZE * 1.5:
            sorted_items = sorted(USER_CACHE.items(), key=lambda x: x[1][1])
            for key, _ in sorted_items[:len(USER_CACHE)//2]:
                del USER_CACHE[key]
    return data

async def set_user_cached(user_id: int, data: dict):
    await db_set_user(user_id, data)
    USER_CACHE[user_id] = (data, time.time())

# ==================== متغیرهای سراسری ====================
banned_users = set()
auto_tasks_data = {}
group_settings = {}
users_db = {}
forced_channels_cache = []

async def load_global_data():
    global banned_users, auto_tasks_data, group_settings, users_db, forced_channels_cache
    await init_db()
    await init_pool()
    banned_users = await db_get_all_banned()
    auto_tasks_data = await db_get_auto_tasks()
    group_settings = await db_get_all_group_settings()
    users_db = await db_get_all_users()
    forced_channels_cache = await get_active_forced_channels()
    logger.info(f"Global data loaded. {len(forced_channels_cache)} forced channels loaded.")

# ─── پایان بخش ۴ ───
# ==================== متغیرهای سشن ====================
sessions = {}
bot_session = None
processed_edits = set()
auto_messages = {}
emoji_tasks = {}
warning_messages = defaultdict(lambda: deque(maxlen=10))
admin_states = {}
ad_reply_timestamps = defaultdict(lambda: deque(maxlen=5))
user_messages_cache = defaultdict(lambda: deque(maxlen=100))
gift_semaphore = asyncio.Semaphore(CONCURRENT_GIFT)
auto_seen_last_time = {}

LOGIN_SEMAPHORE = asyncio.Semaphore(10)
ACTIVE_LOGIN_TASKS = set()
BACKGROUND_TASKS = set()

def create_background_task(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task

# ==================== کیبوردها ====================
def get_main_keyboard():
    return {"inline_keyboard": [
        [{"text": "🎫 خرید اشتراک", "callback_data": "buy_subscription"}],
        [{"text": "📊 وضعیت سلف", "callback_data": "status_self"}],
        [{"text": "🚀 فعال‌سازی سلف", "callback_data": "activate_self"}],
        [{"text": "📖 راهنما", "callback_data": "help_self"}],
        [{"text": "📞 پشتیبانی", "url": f"https://ble.ir/{SUPPORT_ID.replace('@', '')}"}]
    ]}

def get_admin_panel_markup() -> dict:
    return {"inline_keyboard": [
        [{"text": "📊 آمار کاربران", "callback_data": "admin_stats_users"}],
        [{"text": "📋 لیست کاربران", "callback_data": "admin_list_users"}],
        [{"text": "🎁 اضافه کردن اشتراک", "callback_data": "admin_add_subscription"}],
        [{"text": "🚫 بن کاربر", "callback_data": "admin_ban_user"}],
        [{"text": "✅ آنبن کاربر", "callback_data": "admin_unban_user"}],
        [{"text": "➕ اضافه کردن کانال اجباری", "callback_data": "admin_add_channel"}],
        [{"text": "➖ حذف کانال اجباری", "callback_data": "admin_remove_channel"}],
        [{"text": "📋 لیست کانال‌های اجباری", "callback_data": "admin_list_channels"}],
        [{"text": "📢 همگانی", "callback_data": "admin_broadcast"}],
        [{"text": "❌ بستن", "callback_data": "admin_close"}]
    ]}

def get_back_keyboard():
    return {"inline_keyboard": [[{"text": "🔙 بازگشت", "callback_data": "back_to_main"}]]}

def get_forced_channels_keyboard(channels: list, user_id: int) -> dict:
    keyboard = []
    for ch in channels:
        display_name = ch.get("channel_name") or f"کانال {ch['channel_id']}"
        keyboard.append([{"text": f"🔗 عضویت در {display_name}", "url": ch["invite_link"]}])
    keyboard.append([{"text": "✅ بررسی عضویت", "callback_data": f"check_membership_{user_id}"}])
    return {"inline_keyboard": keyboard}

def get_settings_markup(is_bold: bool, is_blue: bool, is_italic: bool, is_gift: bool, is_reaction: bool) -> dict:
    status = lambda obj: "🟢 روشن" if obj else "🔴 خاموش"
    return {"inline_keyboard": [
        [{"text": f"🔹 ضخیم (Bold): {status(is_bold)}", "callback_data": "toggle_bold"}],
        [{"text": f"✨ کج (Italic): {status(is_italic)}", "callback_data": "toggle_italic"}],
        [{"text": f"🔵 لینک آبی: {status(is_blue)}", "callback_data": "toggle_blue"}],
        [{"text": f"🎁 هدیه قاپ: {status(is_gift)}", "callback_data": "toggle_gift"}],
        [{"text": f"👍 ری‌اکشن: {status(is_reaction)}", "callback_data": "toggle_reaction"}],
        [{"text": "🕘 تنظیمات ساعت", "callback_data": "open_clock_settings"}]
    ]}

def get_clock_settings_markup(user_id: int, user_data: dict) -> dict:
    cfg = user_data.get("clock", {}) if user_data else {}
    name_enabled = cfg.get("name_enabled", False)
    bio_enabled = cfg.get("bio_enabled", False)
    name_font = cfg.get("name_font", "normal")
    bio_font = cfg.get("bio_font", "normal")
    name_status = "🟢 فعال" if name_enabled else "🔴 غیرفعال"
    bio_status = "🟢 فعال" if bio_enabled else "🔴 غیرفعال"
    name_font_icon = "✨ فانتزی" if name_font == "fancy" else "🔤 معمولی"
    bio_font_icon = "✨ فانتزی" if bio_font == "fancy" else "🔤 معمولی"
    return {"inline_keyboard": [
        [{"text": f"📛 ساعت در نام: {name_status}", "callback_data": "clock_name_toggle"}],
        [{"text": f"📝 ساعت در بیو: {bio_status}", "callback_data": "clock_bio_toggle"}],
        [{"text": f"🅰️ فونت نام: {name_font_icon}", "callback_data": "clock_name_font"}],
        [{"text": f"🅱️ فونت بیو: {bio_font_icon}", "callback_data": "clock_bio_font"}],
        [{"text": "✏️ قالب نام", "callback_data": "clock_name_format"}, {"text": "✏️ قالب بیو", "callback_data": "clock_bio_format"}],
        [{"text": "🔙 بازگشت به پنل اصلی", "callback_data": "clock_back"}]
    ]}

# ==================== تابع نرمال‌سازی شماره ====================
def normalize_phone(phone_str: str) -> str:
    if not phone_str:
        return ""
    phone = phone_str.translate(PERSIAN_DIGITS)
    phone = re.sub(r'\D', '', phone)
    if not phone:
        return ""
    if phone.startswith('0'):
        phone = phone[1:]
    if phone.startswith('98') and len(phone) == 12:
        phone = phone[2:]
    if len(phone) == 10 and phone.startswith('9'):
        return phone
    return phone

FANCY_LETTERS = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "αв¢∂єfgнιנкℓмησρqяѕтυνωχуչABCDEFGHIJKLMNOPQRSTUVWXYZ")
FANCY_DIGITS = {'0': '𝟎', '1': '𝟏', '2': '𝟐', '3': '𝟑', '4': '𝟒', '5': '𝟓', '6': '𝟔', '7': '𝟕', '8': '𝟖', '9': '𝟗'}

def convert_to_fancy_font(text: str) -> str:
    res = text.translate(FANCY_LETTERS)
    for d, fancy_d in FANCY_DIGITS.items():
        res = res.replace(d, fancy_d)
    return res

def get_clock_time(font_type: str = "normal") -> str:
    now = datetime.now(ZoneInfo("Asia/Tehran")).strftime("%H:%M")
    if font_type == "fancy":
        return convert_to_fancy_font(now)
    return now

def contains_link(text: str) -> bool:
    return bool(re.compile(r'https?://\S+|www\.\S+').search(text))

async def get_group_setting(group_id: int, key: str, default=None):
    settings = await db_get_group_settings(group_id)
    return settings.get(key, default)

async def set_group_setting(group_id: int, key: str, value):
    settings = await db_get_group_settings(group_id)
    settings[key] = value
    await db_set_group_settings(group_id, settings)

async def get_group_warnings(group_id: int) -> dict:
    return await get_group_setting(group_id, "warnings", {})

async def add_warning(group_id: int, user_id: int):
    warnings = await get_group_warnings(group_id)
    warnings[str(user_id)] = warnings.get(str(user_id), 0) + 1
    await set_group_setting(group_id, "warnings", warnings)

async def clear_warnings(group_id: int, user_id: int):
    warnings = await get_group_warnings(group_id)
    warnings.pop(str(user_id), None)
    await set_group_setting(group_id, "warnings", warnings)

# ==================== توابع ارسال پیام ====================
async def send_bot_message(chat_id: int, text: str, reply_markup: dict = None, retries=2):
    url = f"{BASE_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    for attempt in range(retries):
        try:
            await asyncio.wait_for(bot_session.post(url, json=payload), timeout=5)
            return
        except asyncio.TimeoutError:
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
            else:
                logger.warning(f"Timeout sending message to {chat_id}")
        except Exception as e:
            error_str = str(e)
            if "InvalidArgument" in error_str:
                logger.debug(f"InvalidArgument (safe ignore): {e}")
                break
            elif attempt < retries - 1:
                await asyncio.sleep(0.3)
            else:
                logger.error(f"Error sending message: {e}")

async def edit_bot_message(chat_id: int, message_id: int, text: str, reply_markup: dict = None):
    url = f"{BASE_URL}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        await bot_session.post(url, json=payload)
    except Exception as e:
        logger.error(f"Error editing message: {e}")

async def send_copy_message(chat_id: int, original_msg: dict):
    try:
        async with asyncio.timeout(10):
            if "text" in original_msg:
                await send_bot_message(chat_id, original_msg["text"])
            elif "photo" in original_msg:
                photo = original_msg["photo"][-1]
                payload = {"chat_id": chat_id, "photo": photo["file_id"]}
                if "caption" in original_msg:
                    payload["caption"] = original_msg["caption"]
                await bot_session.post(f"{BASE_URL}/sendPhoto", json=payload)
            elif "document" in original_msg:
                doc = original_msg["document"]
                payload = {"chat_id": chat_id, "document": doc["file_id"]}
                if "caption" in original_msg:
                    payload["caption"] = original_msg["caption"]
                await bot_session.post(f"{BASE_URL}/sendDocument", json=payload)
            elif "video" in original_msg:
                video = original_msg["video"]
                payload = {"chat_id": chat_id, "video": video["file_id"]}
                if "caption" in original_msg:
                    payload["caption"] = original_msg["caption"]
                await bot_session.post(f"{BASE_URL}/sendVideo", json=payload)
            elif "audio" in original_msg:
                audio = original_msg["audio"]
                payload = {"chat_id": chat_id, "audio": audio["file_id"]}
                if "caption" in original_msg:
                    payload["caption"] = original_msg["caption"]
                await bot_session.post(f"{BASE_URL}/sendAudio", json=payload)
            elif "voice" in original_msg:
                voice = original_msg["voice"]
                await bot_session.post(f"{BASE_URL}/sendVoice", json={"chat_id": chat_id, "voice": voice["file_id"]})
    except asyncio.TimeoutError:
        logger.warning(f"Timeout sending copy to {chat_id}")
    except Exception as e:
        logger.error(f"Error copying message: {e}")

# ==================== توابع مدیریت گروه ====================
async def kick_user(chat_id: int, user_id: int, client: Client):
    try:
        await client.kick_user(chat_id, user_id)
    except Exception as e:
        logger.error(f"Error kicking user {user_id}: {e}")

async def handle_link_violation(message: Message, user_client: Client, chat_id: int, sender_id: int):
    try:
        await user_client.delete_message(message.message_id, message.date, chat_id, ChatType.GROUP)
    except Exception as e:
        logger.error(f"Error deleting message: {e}")
    await add_warning(chat_id, sender_id)
    warnings = await get_group_warnings(chat_id)
    warns = warnings.get(str(sender_id), 0)
    warn_limit = 3
    warn_text = f"⚠️ کاربر {sender_id} به دلیل ارسال لینک اخطار دریافت کرد. (تعداد: {warns}/{warn_limit})"
    warn_msg = await user_client.send_message(warn_text, chat_id, ChatType.GROUP)
    key = (chat_id, sender_id)
    warning_messages[key].append(warn_msg.message_id)
    if warns >= warn_limit:
        for msg_id in warning_messages[key]:
            if msg_id != warn_msg.message_id:
                try:
                    await user_client.delete_message(msg_id, 0, chat_id, ChatType.GROUP)
                except:
                    pass
        warning_messages[key].clear()
        await kick_user(chat_id, sender_id, user_client)
        await user_client.send_message(f"🚫 کاربر {sender_id} به دلیل تکرار ارسال لینک بن شد.", chat_id, ChatType.GROUP)
        await clear_warnings(chat_id, sender_id)
    else:
        async def delete_warning_after_delay():
            await asyncio.sleep(5)
            try:
                await user_client.delete_message(warn_msg.message_id, warn_msg.date, chat_id, ChatType.GROUP)
                if warn_msg.message_id in warning_messages[key]:
                    warning_messages[key].remove(warn_msg.message_id)
            except:
                pass
        create_background_task(delete_warning_after_delay())

async def apply_group_rules(message: Message, user_client: Client):
    chat_id = message.chat.id
    sender_id = message.sender_id
    text = message.text or ""
    if sender_id == user_client.me.id:
        return
    if contains_link(text):
        await handle_link_violation(message, user_client, chat_id, sender_id)

# ==================== بروزرسانی ساعت پروفایل ====================
async def update_user_profile(user_client: Client, user_id: int):
    try:
        if not hasattr(user_client, "me") or user_client.me is None:
            await ensure_user_id(user_client)
    except:
        return
    user_data = await get_user_cached(user_id) or {}
    settings = user_data.get("clock", {})
    name_enabled = settings.get("name_enabled", False)
    bio_enabled = settings.get("bio_enabled", False)
    if not name_enabled and not bio_enabled:
        return
    try:
        current = await asyncio.wait_for(user_client.get_me(), timeout=5)
    except Exception as e:
        logger.error(f"Error getting current profile for {user_id}: {e}")
        return
    current_first_name = getattr(current, 'first_name', '') or getattr(current, 'name', '') or ''
    current_last_name = getattr(current, 'last_name', '') or ''
    current_full_name = f"{current_first_name} {current_last_name}".strip()
    current_about = getattr(current, 'about', '') or ''
    if name_enabled:
        name_format = settings.get("name_format", "{time}")
        name_font = settings.get("name_font", "normal")
        new_name = name_format.replace("{time}", get_clock_time(name_font))
        if new_name != current_full_name:
            for attempt in range(3):
                try:
                    await asyncio.wait_for(user_client.edit_name(new_name), timeout=5)
                    break
                except Exception as e:
                    if "same user name" in str(e).lower():
                        break
                    if attempt == 2:
                        logger.error(f"Error updating name for {user_id}: {e}")
                    await asyncio.sleep(0.5)
    if bio_enabled:
        bio_format = settings.get("bio_format", "{time}")
        bio_font = settings.get("bio_font", "normal")
        new_bio = bio_format.replace("{time}", get_clock_time(bio_font))
        if new_bio != current_about:
            for attempt in range(3):
                try:
                    await asyncio.wait_for(user_client.edit_about(new_bio), timeout=5)
                    break
                except Exception as e:
                    if "same user bio" in str(e).lower():
                        break
                    if attempt == 2:
                        logger.error(f"Error updating bio for {user_id}: {e}")
                    await asyncio.sleep(0.5)

async def clock_updater():
    try:
        while True:
            await asyncio.sleep(60)
            for uid, session in list(sessions.items()):
                if session.get("step") == "connected" and "client" in session:
                    user_client = session["client"]
                    try:
                        await update_user_profile(user_client, uid)
                    except Exception as e:
                        logger.error(f"Clock update error for {uid}: {e}")
    except asyncio.CancelledError:
        logger.info("Clock updater cancelled")
        raise

# ==================== پاک‌سازی دوره‌ای ====================
async def periodic_cleanup():
    try:
        while True:
            await asyncio.sleep(1800)
            warning_messages.clear()
            ad_reply_timestamps.clear()
            auto_seen_last_time.clear()
            processed_edits.clear()
            NEW_USER_CACHE.clear()
            now = time.time()
            for uid in list(USER_CACHE.keys()):
                if now - USER_CACHE[uid][1] > USER_CACHE_TTL:
                    del USER_CACHE[uid]
            logger.debug("Periodic cleanup performed.")
    except asyncio.CancelledError:
        logger.info("Periodic cleanup cancelled")
        raise

async def clean_processed_edits():
    try:
        while True:
            await asyncio.sleep(3600)
            processed_edits.clear()
            now = time.time()
            for uid in list(USER_CACHE.keys()):
                if now - USER_CACHE[uid][1] > USER_CACHE_TTL:
                    del USER_CACHE[uid]
            logger.debug("Periodic cleanup performed.")
    except asyncio.CancelledError:
        logger.info("Clean processed edits cancelled")
        raise

async def session_health_checker():
    try:
        while True:
            await asyncio.sleep(3600)
            for uid, session in list(sessions.items()):
                if session.get("step") == "connected" and "client" in session:
                    try:
                        client = session["client"]
                        await asyncio.wait_for(client.get_me(), timeout=3)
                    except Exception as e:
                        logger.warning(f"Health check failed for {uid}: {e}")
                        if uid in sessions:
                            del sessions[uid]
                        await db_remove_active_session(uid)
                        try:
                            await send_bot_message(uid, "⚠️ سشن شما به‌دلیل مشکل فنی غیرفعال شد. لطفاً مجدداً فعال‌سازی کنید.")
                        except:
                            pass
    except asyncio.CancelledError:
        logger.info("Session health checker cancelled")
        raise

async def ensure_user_id(client: Client) -> int:
    try:
        if hasattr(client, "me") and client.me is not None:
            return client.me.id
        me = await asyncio.wait_for(client.get_me(), timeout=5.0)
        client.me = me
        return me.id
    except Exception as e:
        logger.error(f"Failed to get user_id: {e}")
        raise

# ==================== EMOJI Categories ====================
EMOJI_CATEGORIES = {
    "گربه": ["😺", "😸", "😹", "😻", "😼", "😽", "🙀", "😿", "😾", "🐱", "🐈", "🐈‍⬛"],
    "گیاه": ["🌱", "🌿", "☘️", "🍀", "🪴", "🌵", "🌲", "🌳", "🌴", "🌾", "🌺", "🌸", "🌼", "🏵️", "🌻"],
    "خنده": ["😀", "😃", "😄", "😁", "😆", "😅", "😂", "🤣", "🥲", "😊", "😍", "🥰", "😘", "😜", "🤪"],
    "قلب": ["❤️", "🧡", "💛", "💚", "💙", "💜", "🖤", "🤍", "🤎", "💔", "❤️‍🔥", "💖", "💗", "💓", "💕"],
    "غذا": ["🍎", "🍕", "🍔", "🍟", "🍩", "🍪", "🍫", "🍬", "🍭", "🥨", "🥩", "🍣", "🍜", "🍦", "🍰"],
    "فحش": ["😡", "👿", "💀", "🤬", "💢", "🗯️", "🖕", "💩", "👎", "🤮", "😤", "😠", "👊", "✊", "🔞", "💣", "🔪", "😈"],
}

# ─── پایان بخش ۵ ───
# ==================== نام دستگاه ورود بله ====================
BALE_DEVICE_MODELS = [
    "Samsung Galaxy A16", "Samsung Galaxy A17", "Samsung Galaxy A25", "Samsung Galaxy A26",
    "Samsung Galaxy A35", "Samsung Galaxy A36", "Samsung Galaxy A55", "Samsung Galaxy A56",
    "Samsung Galaxy S23 FE", "Samsung Galaxy S24 FE", "Samsung Galaxy S25 FE",
    "Samsung Galaxy S24 Ultra", "Samsung Galaxy S25 Ultra",
    "Redmi Note 13", "Redmi Note 13 Pro", "Redmi Note 14", "Redmi Note 14 Pro",
    "Redmi Note 14 Pro+", "Redmi Note 14 5G",
    "POCO X6", "POCO X6 Pro", "POCO X7", "POCO X7 Pro",
    "Xiaomi 14", "Xiaomi 14T", "Xiaomi 14T Pro", "Xiaomi 15", "Xiaomi 15 Ultra",
    "iPhone 13", "iPhone 14", "iPhone 15", "iPhone 15 Pro", "iPhone 16",
    "iPhone 16 Pro", "iPhone 16 Pro Max", "Nothing Phone (2a)",
]

def _make_bale_client(user_dp, session_file):
    kwargs = {"session_file": session_file}
    try:
        params = inspect.signature(Client).parameters
        for name in ("device_title", "deviceTitle", "device_name"):
            if name in params:
                kwargs[name] = random.choice(BALE_DEVICE_MODELS)
                logger.info("Bale device title selected: %s", kwargs[name])
                break
    except (TypeError, ValueError):
        pass
    return Client(user_dp, **kwargs)

def create_user_client(user_id: int, settings: dict, code_callback=None) -> Client:
    user_dp = Dispatcher()
    session_file = os.path.join(SESSIONS_DIR, f"session_{user_id}.bale")
    user_client = _make_bale_client(user_dp, session_file)

    if code_callback is None:
        async def default_code_callback(phone, code_type, transaction_hash):
            logger.info(f"Waiting for code for {phone}")
            return None
        user_client.phone_code_callback = default_code_callback
    else:
        user_client.phone_code_callback = code_callback

    if hasattr(user_client, '_ping'):
        original_ping_inst = user_client._ping
        async def patched_ping_inst():
            try:
                await asyncio.wait_for(original_ping_inst(), timeout=5.0)
            except Exception:
                pass
        user_client._ping = patched_ping_inst

    user_client.is_bold_active = settings.get("is_bold_active", True)
    user_client.is_italic_active = settings.get("is_italic_active", False)
    user_client.is_blue_active = settings.get("is_blue_active", False)
    user_client.is_gift_active = settings.get("is_gift_active", True)
    user_client.is_reaction_active = settings.get("is_reaction_active", True)

    async def animate_emojis(message: Message, emoji_list: list, delay: float = 0.8):
        try:
            for emoji in emoji_list:
                await message.edit_text(emoji)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Error animating emojis: {e}")

    @user_dp.message(IsGift())
    async def gift_handler(message: Message):
        if not getattr(user_client, "is_gift_active", True):
            return
        token_val = None
        try:
            if hasattr(message.content, "gift") and hasattr(message.content.gift, "token"):
                token_val = message.content.gift.token.value
        except Exception:
            token_val = None
        if not token_val:
            return
        try:
            info_msg = InfoMessage(
                peer=Peer(type=message.chat.type, id=message.chat.id),
                message_id=message.message_id,
                date=message.date,
                previous_message=None
            )
            async with gift_semaphore:
                result = await asyncio.wait_for(
                    user_client(OpenGiftPacket(message=info_msg, receiver_token=token_val, page_no={})),
                    timeout=3.0
                )
        except asyncio.TimeoutError:
            logger.debug("Gift opening timed out")
            return
        except Exception as e:
            logger.debug(f"Gift opening error (likely validation): {e}")
            return
        create_background_task(_process_gift_result(message, result))

    async def _process_gift_result(message: Message, result):
        try:
            def get_val(obj, *attrs, default=0):
                for attr in attrs:
                    if hasattr(obj, attr):
                        val = getattr(obj, attr)
                        if val is not None:
                            return val
                    elif isinstance(obj, dict) and attr in obj:
                        val = obj[attr]
                        if val is not None:
                            return val
                return default
            win_amount = get_val(result, 'amount', 'win_amount', 'winAmount')
            if win_amount == 0:
                return
            user_id = user_client.me.id
            user_data = await get_user_cached(user_id) or {}
            user_data['gift_count'] = user_data.get('gift_count', 0) + 1
            await set_user_cached(user_id, user_data)
            if getattr(user_client, "is_reaction_active", True):
                try:
                    await message.react("👍")
                except Exception as e:
                    logger.debug(f"Error reacting: {e}")
        except Exception as e:
            logger.error(f"Gift result processing error: {e}\n{traceback.format_exc()}")

    @user_dp.message()
    async def user_text_handler(message: Message):
        try:
            await _user_text_handler_impl(message)
        except Exception as e:
            logger.error(f"Unhandled error in user_text_handler: {e}")

    async def _user_text_handler_impl(message: Message):
        try:
            await ensure_user_id(user_client)
        except:
            return
        chat = message.chat
        chat_id = chat.id
        sender_id = message.sender_id
        can_edit = hasattr(message, "edit_text")
        my_id = user_client.me.id

        if sender_id == my_id:
            if not message.text:
                return
            msg_text = message.text.strip()
            key = (chat_id, my_id)
            user_messages_cache[key].append(message.message_id)

            if msg_text.startswith(".پاکسازی "):
                parts = msg_text.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    if can_edit:
                        await message.edit_text("⚠️ فرمت صحیح: `.پاکسازی [تعداد]`")
                    return
                count = int(parts[1])
                if count <= 0 or count > 200:
                    if can_edit:
                        await message.edit_text("⚠️ تعداد باید بین 1 تا 200 باشد.")
                    return
                cached = user_messages_cache.get(key, [])
                if not cached:
                    if can_edit:
                        await message.edit_text("❌ هیچ پیام قابل حذفی در کش وجود ندارد.")
                    return
                to_delete = list(cached)[-count:] if count <= len(cached) else list(cached)[:]
                total_to_delete = len(to_delete)
                for _ in to_delete:
                    cached.remove(_)
                await message.edit_text("🔄 *پاکسازی پیام‌ها*\n▱▱▱▱▱▱▱▱▱▱ 0%\n📝 0/{}".format(total_to_delete))
                deleted = 0
                for idx, msg_id in enumerate(to_delete, 1):
                    try:
                        await user_client.delete_message(msg_id, 0, chat_id, ChatType.GROUP)
                        deleted += 1
                    except Exception:
                        pass
                    if idx % 2 == 0 or idx == total_to_delete:
                        percent = int((deleted / total_to_delete) * 100)
                        bar = "▰" * (percent // 10) + "▱" * (10 - (percent // 10))
                        await message.edit_text(f"🔄 *پاکسازی پیام‌ها*\n{bar} {percent}%\n📝 {deleted}/{total_to_delete}")
                    await asyncio.sleep(0.05)
                await message.edit_text(f"✅ **پاکسازی کامل شد!**\n🗑️ {deleted} پیام از {total_to_delete} درخواستی حذف شد.")
                return

            if msg_text in [".پاکت بازکن روشن", ".پاکت ان", ".پاکت بازکن"]:
                user_client.is_gift_active = True
                user_data = await get_user_cached(my_id) or {}
                user_data["is_gift_active"] = True
                await set_user_cached(my_id, user_data)
                if can_edit:
                    await message.edit_text("✅ **تمام پاکت‌ها به‌صورت خودکار باز می‌شوند.**")
                return

            if msg_text in [".پاکت بازکن خاموش", ".پاکت اف", ".پاکت باز نکن"]:
                user_client.is_gift_active = False
                user_data = await get_user_cached(my_id) or {}
                user_data["is_gift_active"] = False
                await set_user_cached(my_id, user_data)
                if can_edit:
                    await message.edit_text("❌ **ربات دیگر به‌صورت خودکار پاکت هدیه را باز نمی‌کند.**")
                return

            if msg_text == ".آمار":
                user_data = await get_user_cached(my_id) or {}
                gift_count = user_data.get('gift_count', 0)
                clock_cfg = user_data.get('clock', {})
                name_clock = "🟢 فعال" if clock_cfg.get('name_enabled') else "🔴 غیرفعال"
                bio_clock = "🟢 فعال" if clock_cfg.get('bio_enabled') else "🔴 غیرفعال"
                bold = "🟢" if getattr(user_client, "is_bold_active", True) else "🔴"
                italic = "🟢" if getattr(user_client, "is_italic_active", False) else "🔴"
                blue = "🟢" if getattr(user_client, "is_blue_active", False) else "🔴"
                gift = "🟢" if getattr(user_client, "is_gift_active", True) else "🔴"
                reaction = "🟢" if getattr(user_client, "is_reaction_active", True) else "🔴"
                session_status = "🟢 فعال" if my_id in sessions and sessions[my_id].get('step') == 'connected' else "🔴 غیرفعال"
                auto_seen = "🟢 فعال" if user_data.get("auto_seen_enabled", False) else "🔴 غیرفعال"
                is_test = await has_active_test(my_id)
                test_status = "🧪 فعال" if is_test else "❌ غیرفعال"
                if is_test:
                    _, remaining = await get_test_remaining_time(my_id)
                    test_status += f" ({remaining})"
                stats = (
                    f"📊 **آمار {BOT_NAME}**\n\n"
                    f"🔹 **وضعیت سشن:** {session_status}\n"
                    f"🎁 **پاکت‌های باز شده:** `{gift_count}`\n"
                    f"🧪 **تست ۳ ساعته:** {test_status}\n"
                    f"⚙️ **قابلیت‌ها:**\n"
                    f"  • پررنگ: {bold}\n  • کج: {italic}\n  • لینک آبی: {blue}\n"
                    f"  • بازکن پاکت: {gift}\n  • ری‌اکشن: {reaction}\n"
                    f"  • ساعت در نام: {name_clock}\n  • ساعت در بیو: {bio_clock}\n"
                    f"  • سین خودکار: {auto_seen}\n"
                )
                if can_edit:
                    await message.edit_text(stats)
                return

            if msg_text.startswith(".تبلیغات ") and not msg_text.startswith(".تبلیغات خاموش"):
                banner = msg_text[9:].strip()
                if not banner:
                    if can_edit:
                        await message.edit_text("⚠️ لطفاً متن بنر را وارد کنید.\nمثال: `.تبلیغات کانال ما @example`")
                    return
                user_data = await get_user_cached(my_id) or {}
                if "ad_settings" not in user_data:
                    user_data["ad_settings"] = {}
                user_data["ad_settings"][str(chat_id)] = {"enabled": True, "banner": banner}
                await set_user_cached(my_id, user_data)
                ad_reply_timestamps.clear()
                if can_edit:
                    await message.edit_text(f"✅ **تبلیغات در این گروه فعال شد.**\nبنر: `{banner}`")
                return

            if msg_text == ".تبلیغات خاموش":
                user_data = await get_user_cached(my_id) or {}
                if "ad_settings" in user_data and str(chat_id) in user_data["ad_settings"]:
                    user_data["ad_settings"][str(chat_id)]["enabled"] = False
                    await set_user_cached(my_id, user_data)
                    if can_edit:
                        await message.edit_text("❌ **تبلیغات در این گروه غیرفعال شد.**")
                return

            if msg_text == ".اتو خاموش":
                key = (chat_id, my_id)
                if key in auto_messages and not auto_messages[key].done():
                    auto_messages[key].cancel()
                    del auto_messages[key]
                    await db_remove_auto_task(chat_id, my_id)
                    if can_edit:
                        await message.edit_text("🛑 ارسال خودکار متوقف شد.")
                return

            if msg_text.startswith(".اتو "):
                rest = msg_text[len(".اتو "):].strip()
                parts = rest.split(maxsplit=1)
                if len(parts) != 2:
                    if can_edit:
                        await message.edit_text("⚠️ فرمت صحیح: `.اتو [دقیقه] [متن]`")
                    return
                time_part = parts[0]
                if not time_part.isdigit():
                    if can_edit:
                        await message.edit_text("⚠️ زمان باید یک عدد باشد.")
                    return
                minutes = int(time_part)
                if minutes < 1 or minutes > 60:
                    if can_edit:
                        await message.edit_text("⚠️ زمان باید بین 1 تا 60 دقیقه باشد.")
                    return
                text_to_send = parts[1]
                if not text_to_send:
                    if can_edit:
                        await message.edit_text("⚠️ متن نمی‌تواند خالی باشد.")
                    return
                key = (chat_id, my_id)
                if key in auto_messages and not auto_messages[key].done():
                    auto_messages[key].cancel()
                await db_add_auto_task(chat_id, my_id, text_to_send, minutes)
                async def auto_sender():
                    try:
                        while True:
                            await asyncio.sleep(minutes * 60)
                            await user_client.send_message(text_to_send, chat_id, ChatType.GROUP)
                    except asyncio.CancelledError:
                        pass
                auto_messages[key] = create_background_task(auto_sender())
                if can_edit:
                    await message.edit_text(f"✅ ارسال خودکار «{text_to_send}» هر {minutes} دقیقه شروع شد.\nبرای توقف: `.اتو خاموش`")
                return

            if msg_text.startswith(".پاسخ "):
                parts = msg_text.split(maxsplit=1)
                if len(parts) < 2:
                    if can_edit:
                        await message.edit_text("⚠️ فرمت: `.پاسخ [متن] [جواب]`")
                    return
                rest = parts[1].strip()
                if ' ' not in rest:
                    if can_edit:
                        await message.edit_text("⚠️ فرمت: `.پاسخ [متن] [جواب]`")
                    return
                trigger, response = rest.split(' ', 1)
                trigger = trigger.strip()
                response = response.strip()
                if not trigger:
                    if can_edit:
                        await message.edit_text("⚠️ متن نمی‌تواند خالی باشد.")
                    return
                user_data = await get_user_cached(my_id) or {}
                if "auto_replies" not in user_data:
                    user_data["auto_replies"] = {}
                if response == "خاموش":
                    if trigger.lower() in user_data["auto_replies"]:
                        del user_data["auto_replies"][trigger.lower()]
                        await set_user_cached(my_id, user_data)
                        reply_text = f"✅ پاسخ خودکار برای «{trigger}» غیرفعال شد."
                    else:
                        reply_text = f"⚠️ هیچ پاسخ خودکاری برای «{trigger}» فعال نیست."
                else:
                    user_data["auto_replies"][trigger.lower()] = response
                    await set_user_cached(my_id, user_data)
                    reply_text = f"✅ پاسخ خودکار برای «{trigger}» با جواب «{response}» فعال شد."
                if can_edit:
                    await message.edit_text(reply_text)
                return

            if msg_text.startswith(".پاسخ حذف "):
                parts = msg_text.split(maxsplit=1)
                if len(parts) < 2:
                    if can_edit:
                        await message.edit_text("⚠️ فرمت: `.پاسخ حذف [متن]`")
                    return
                trigger = parts[1].strip()
                user_data = await get_user_cached(my_id) or {}
                auto_replies = user_data.get("auto_replies", {})
                if trigger.lower() in auto_replies:
                    del auto_replies[trigger.lower()]
                    await set_user_cached(my_id, user_data)
                    if can_edit:
                        await message.edit_text(f"✅ پاسخ خودکار برای «{trigger}» حذف شد.")
                return

            if msg_text.lower().startswith(".ایموجی "):
                parts = msg_text.split(maxsplit=1)
                if len(parts) < 2:
                    if can_edit:
                        await message.edit_text("⚠️ لطفاً نوع ایموجی را مشخص کنید.")
                    return
                category_name = parts[1].strip()
                matched_category = None
                for key_cat in EMOJI_CATEGORIES.keys():
                    if key_cat == category_name:
                        matched_category = key_cat
                        break
                if not matched_category:
                    available = "، ".join(EMOJI_CATEGORIES.keys())
                    if can_edit:
                        await message.edit_text(f"❌ دسته «{category_name}» یافت نشد.\nدسته‌های موجود: {available}")
                    return
                if chat_id in emoji_tasks and not emoji_tasks[chat_id].done():
                    emoji_tasks[chat_id].cancel()
                    try:
                        await emoji_tasks[chat_id]
                    except:
                        pass
                emoji_list = EMOJI_CATEGORIES[matched_category]
                emoji_tasks[chat_id] = create_background_task(animate_emojis(message, emoji_list, delay=0.8))
                return

            if msg_text.lower() == ".اطلاعات":
                if not hasattr(message, 'replied_to') or not message.replied_to:
                    if can_edit:
                        await message.edit_text("⚠️ لطفاً روی پیام یک کاربر ریپلای کنید.")
                    return
                replied_msg = message.replied_to
                target_user_id = getattr(replied_msg, 'sender_id', None)
                if target_user_id is None:
                    if can_edit:
                        await message.edit_text("❌ نمی‌توانم آیدی کاربر را تشخیص دهم.")
                    return
                try:
                    url = f"{BASE_URL}/getChat"
                    async with bot_session.post(url, json={"chat_id": target_user_id}) as resp:
                        result = await resp.json()
                        if result.get("ok"):
                            chat_data = result.get("result", {})
                            user_id = chat_data.get("id")
                            first_name = chat_data.get("first_name", "نامشخص")
                            last_name = chat_data.get("last_name", "")
                            full_name = f"{first_name} {last_name}".strip() or "بدون نام"
                            username = chat_data.get("username")
                            bio = chat_data.get("bio", "") or chat_data.get("about", "") or "بیوگرافی ثبت نشده"
                            info_text = (f"👤 **اطلاعات کاربر**\n\n🆔 `{user_id}`\n📝 {full_name}\n"
                                f"🔖 {f'@{username}' if username else 'ندارد'}\n📄 {bio}")
                            if can_edit:
                                await message.edit_text(info_text)
                        else:
                            error_desc = result.get("description", "خطای ناشناخته")
                            if "blocked" in error_desc.lower() or "forbidden" in error_desc.lower():
                                error_msg = "❌ کاربر ربات را بلاک کرده است."
                            else:
                                error_msg = f"❌ خطا در دریافت اطلاعات: {error_desc}"
                            if can_edit:
                                await message.edit_text(error_msg)
                except Exception as e:
                    logger.error(f"Error getting user info: {e}")
                    if can_edit:
                        await message.edit_text("❌ خطا در دریافت اطلاعات.")
                return

            if msg_text.lower() == ".clock":
                user_data = await get_user_cached(my_id) or {}
                await message.reply("🕘 **پنل مدیریت ساعت پروفایل**\n\nاز دکمه‌های زیر استفاده کنید.",
                    reply_markup=get_clock_settings_markup(my_id, user_data))
                return

            if msg_text.lower() == ".nameclock on":
                user_data = await get_user_cached(my_id) or {}
                if "clock" not in user_data:
                    user_data["clock"] = {}
                user_data["clock"]["name_enabled"] = True
                await set_user_cached(my_id, user_data)
                await message.reply("✅ نمایش ساعت در **نام** فعال شد.")
                await update_user_profile(user_client, my_id)
                return

            if msg_text.lower() == ".nameclock off":
                user_data = await get_user_cached(my_id) or {}
                if "clock" in user_data:
                    user_data["clock"]["name_enabled"] = False
                    await set_user_cached(my_id, user_data)
                    await message.reply("❌ نمایش ساعت در **نام** غیرفعال شد.")
                return

            if msg_text.lower() == ".bioclock on":
                user_data = await get_user_cached(my_id) or {}
                if "clock" not in user_data:
                    user_data["clock"] = {}
                user_data["clock"]["bio_enabled"] = True
                await set_user_cached(my_id, user_data)
                await message.reply("✅ نمایش ساعت در **بیوگرافی** فعال شد.")
                await update_user_profile(user_client, my_id)
                return

            if msg_text.lower() == ".bioclock off":
                user_data = await get_user_cached(my_id) or {}
                if "clock" in user_data:
                    user_data["clock"]["bio_enabled"] = False
                    await set_user_cached(my_id, user_data)
                    await message.reply("❌ نمایش ساعت در **بیوگرافی** غیرفعال شد.")
                return

            if msg_text.lower() in [".کیف", ".wallet"]:
                try:
                    wallet_info = await user_client.get_wallet()
                    balance = wallet_info.wallet.balance
                    if can_edit:
                        await message.edit_text(f"💰 **کیف پول شما**\n\nموجودی: `{balance}` ریال")
                except Exception as e:
                    logger.error(f"Error getting wallet: {e}")
                    if can_edit:
                        await message.edit_text("❌ خطا در دریافت کیف پول.")
                return

            GIFT_PATTERN = re.compile(r"\.(ارسال پاکت|sendgift)\s+(\d+)\s+(.*?)\s+(\d+)$")
            gift_match = GIFT_PATTERN.match(msg_text)
            if gift_match:
                amount = int(gift_match.group(2))
                text_content = gift_match.group(3).strip()
                count = int(gift_match.group(4))
                if count <= 0 or count > 50:
                    if can_edit:
                        await message.edit_text("⚠️ تعداد باید بین 1 تا 50 باشد.")
                    return
                if amount <= 0:
                    if can_edit:
                        await message.edit_text("⚠️ مبلغ باید بیشتر از صفر باشد.")
                    return
                status_msg = await message.reply(f"⏳ در حال ارسال {count} پاکت هدیه ...")
                sem = asyncio.Semaphore(CONCURRENT_GIFT)
                async def send_one():
                    async with sem:
                        try:
                            await user_client.send_gift(chat_id=message.chat.id, chat_type=message.chat.type,
                                amount=amount, message=text_content, gift_count=1)
                        except Exception as e:
                            error_str = str(e)
                            if "پاکت هدیهٔ دیگری" in error_str:
                                raise Exception("پاکت هدیهٔ دیگری در حال ارسال است، لطفاً چند دقیقه دیگر تلاش کنید.")
                            raise
                tasks = [send_one() for _ in range(count)]
                try:
                    await asyncio.gather(*tasks)
                    await status_msg.edit_text(f"✅ {count} پاکت هدیه با موفقیت ارسال شد.\nمبلغ هر پاکت: {amount} ریال\nمتن: `{text_content}`")
                except Exception as e:
                    logger.error(f"Error sending gifts: {e}")
                    await status_msg.edit_text(f"❌ خطا در ارسال: {str(e)}")
                return

            SPAM_PATTERN = re.compile(r"\.(اسپم|spam)\s+(.+?)\s+(\d+)$", re.IGNORECASE)
            spam_match = SPAM_PATTERN.match(msg_text)
            if spam_match:
                spam_text = spam_match.group(2).strip()
                count = int(spam_match.group(3))
                max_count = 50
                if count < 1 or count > max_count:
                    if can_edit:
                        await message.edit_text(f"⚠️ تعداد باید بین 1 تا {max_count} باشد.")
                    return
                if not spam_text:
                    if can_edit:
                        await message.edit_text("⚠️ متن پیام نمی‌تواند خالی باشد.")
                    return
                status_msg = await message.reply(f"🔄 *ارسال اسپم*\n▱▱▱▱▱▱▱▱▱▱ 0%\n📝 0/{count}")
                sem = asyncio.Semaphore(CONCURRENT_SPAM)
                sent = 0
                async def send_one():
                    nonlocal sent
                    async with sem:
                        try:
                            await message.answer(spam_text)
                            sent += 1
                        except Exception:
                            pass
                tasks = [send_one() for _ in range(count)]
                for i, task in enumerate(asyncio.as_completed(tasks), 1):
                    await task
                    if i % 2 == 0 or i == count:
                        percent = int((sent / count) * 100)
                        bar = "▰" * (percent // 10) + "▱" * (10 - (percent // 10))
                        await status_msg.edit_text(f"🔄 *ارسال اسپم*\n{bar} {percent}%\n📝 {sent}/{count}")
                    await asyncio.sleep(0.05)
                await status_msg.edit_text(f"✅ **اسپم با موفقیت ارسال شد!**\n📨 {sent} پیام از {count} درخواستی ارسال گردید.")
                await asyncio.sleep(3)
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                return

            if msg_text.startswith(".نصب ربات"):
                if chat.type != ChatType.GROUP:
                    await message.reply("❌ این دستور فقط در گروه قابل استفاده است.")
                    return
                await set_group_setting(chat_id, "installed", True)
                await set_group_setting(chat_id, "warnings", {})
                await message.edit_text("✅ ربات در این گروه نصب شد. لینک‌ها به‌صورت خودکار حذف و اخطار داده می‌شوند.")
                return

            if msg_text.startswith(".حذف ربات"):
                if chat.type != ChatType.GROUP:
                    await message.reply("❌ این دستور فقط در گروه قابل استفاده است.")
                    return
                await set_group_setting(chat_id, "installed", False)
                await message.edit_text("❌ ربات از این گروه حذف شد.")
                return

            target_user_id = None
            if hasattr(message, 'replied_to') and message.replied_to:
                target_user_id = getattr(message.replied_to, 'sender_id', None)
            else:
                parts = msg_text.split()
                if len(parts) > 1 and parts[1].isdigit():
                    target_user_id = int(parts[1])

            if msg_text.startswith(".بن"):
                if target_user_id is None:
                    await message.edit_text("⚠️ لطفاً به پیام کاربر ریپلای کنید یا آیدی را وارد کنید.")
                    return
                await kick_user(chat_id, target_user_id, user_client)
                await message.edit_text(f"🚫 کاربر {target_user_id} بن شد.")
                return

            if msg_text.lower() == ".سین خودکار":
                user_data = await get_user_cached(my_id) or {}
                user_data["auto_seen_enabled"] = True
                await set_user_cached(my_id, user_data)
                if can_edit:
                    await message.edit_text("✅ **حالت سین خودکار فعال شد.**")
                return

            if msg_text.lower() == ".سین خودکار خاموش":
                user_data = await get_user_cached(my_id) or {}
                user_data["auto_seen_enabled"] = False
                await set_user_cached(my_id, user_data)
                if can_edit:
                    await message.edit_text("❌ **حالت سین خودکار غیرفعال شد.**")
                return

            if msg_text.lower() in [".راهنما", ".help"]:
                help_text = (
                    f"🌟 **✨ راهنمای {BOT_NAME} ✨** 🌟\n\n"
                    f"📞 ── پشتیبانی ──\n   🆔 {SUPPORT_ID}\n\n"
                    f"───────────────────\n📌 **لیست کامل دستورات:**\n"
                    f"┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
                    f"🔹 `.ping` / `.پینگ` - بررسی سرعت\n"
                    f"🔹 `.id` / `.ایدی` - دریافت آیدی\n"
                    f"🔹 `.font [متن]` - فونت فانتزی\n"
                    f"🔹 `.ایموجی [نوع]` - ایموجی\n"
                    f"🔹 `.clock` - تنظیم ساعت\n"
                    f"🔹 `.nameclock on/off` - ساعت در نام\n"
                    f"🔹 `.bioclock on/off` - ساعت در بیو\n"
                    f"🔹 `.کیف` - کیف پول\n"
                    f"🔹 `.ارسال پاکت [مبلغ] [متن] [تعداد]`\n"
                    f"🔹 `.اسپم [متن] [تعداد]`\n"
                    f"🔹 `.اتو [دقیقه] [متن]` / `.اتو خاموش`\n"
                    f"🔹 `.پاسخ [متن] [جواب]` / `.پاسخ حذف [متن]`\n"
                    f"🔹 `.اطلاعات` (ریپلای)\n"
                    f"🔹 `.پاکسازی [تعداد]`\n"
                    f"🔹 `.پاکت بازکن روشن/خاموش`\n"
                    f"🔹 `.آمار`\n"
                    f"🔹 `.تبلیغات [بنر]` / `.تبلیغات خاموش`\n"
                    f"🔹 `.سین خودکار` / `.سین خودکار خاموش`\n"
                    f"🔹 `.نصب ربات` / `.حذف ربات`\n"
                    f"🔹 `.بن` (ریپلای)"
                )
                keyboard = {"inline_keyboard": [[{"text": "🔙 بازگشت به منو", "callback_data": "back_to_main"}]]}
                if can_edit:
                    await message.edit_text(help_text, reply_markup=keyboard)
                return

            if msg_text.lower() in [".ping", ".پینگ"]:
                try:
                    start_time = time.perf_counter()
                    temp_msg = await message.reply("⏳ در حال اندازه‌گیری...")
                    elapsed = (time.perf_counter() - start_time) * 1000
                    await temp_msg.edit_text(f"⏱ *ping:* `{elapsed:.0f}ms`")
                except Exception as e:
                    logger.debug(f"Ping error: {e}")
                return

            if msg_text.lower() in [".id", ".ایدی", ".آیدی"]:
                chat = message.chat
                lines = [f"🆔 **آیدی چت:** `{chat.id}`", f"👤 **آیدی شما:** `{my_id}`"]
                if hasattr(chat, 'title') and chat.title:
                    lines.append(f"📛 **عنوان چت:** {chat.title}")
                if hasattr(message, 'replied_to') and message.replied_to:
                    replied_sender = getattr(message.replied_to, 'sender_id', None)
                    if replied_sender:
                        lines.append(f"\n📌 **آیدی فرستنده:** `{replied_sender}`")
                try:
                    await message.reply("\n".join(lines))
                except Exception as e:
                    logger.error(f"Error in id command: {e}")
                return

            if msg_text.lower().startswith((".font ", ".فونت ")):
                target_text = msg_text[6:]
                fancy_text = convert_to_fancy_font(target_text)
                try:
                    if can_edit:
                        await message.edit_text(fancy_text)
                except:
                    pass
                return

            is_bold = getattr(user_client, "is_bold_active", True)
            is_italic = getattr(user_client, "is_italic_active", False)
            is_blue = getattr(user_client, "is_blue_active", False)
            if not is_bold and not is_blue and not is_italic:
                return
            new_text = msg_text
            if is_italic:
                new_text = f"_{new_text}_"
            if is_bold:
                new_text = f"*{new_text}*"
            if is_blue:
                new_text = f"[{new_text}](uid:) "
            if new_text != msg_text and can_edit and sender_id == my_id:
                try:
                    processed_edits.add(message.message_id)
                    await asyncio.wait_for(message.edit_text(new_text), timeout=3)
                except asyncio.TimeoutError:
                    processed_edits.discard(message.message_id)
                except Exception as e:
                    processed_edits.discard(message.message_id)
            return

        if chat.type == ChatType.GROUP:
            installed = await get_group_setting(chat_id, "installed", False)
            if installed:
                await apply_group_rules(message, user_client)
                if message.text and contains_link(message.text):
                    return
            user_data = await get_user_cached(my_id) or {}
            user_auto_replies = user_data.get("auto_replies", {})
            if user_auto_replies and message.text:
                trigger = message.text.strip().lower()
                if trigger in user_auto_replies:
                    try:
                        await message.reply(user_auto_replies[trigger])
                    except Exception as e:
                        logger.error(f"Error auto-replying: {e}")
            ad_settings = user_data.get("ad_settings", {}).get(str(chat_id))
            if ad_settings and ad_settings.get("enabled", False):
                banner = ad_settings.get("banner")
                if banner and sender_id != my_id:
                    key = (chat_id, sender_id)
                    now = time.time()
                    timestamps = ad_reply_timestamps.get(key, [])
                    if not isinstance(timestamps, deque):
                        timestamps = deque(timestamps, maxlen=5)
                    timestamps = [t for t in timestamps if now - t < 300]
                    if len(timestamps) < 2:
                        try:
                            await message.reply(banner)
                            timestamps.append(now)
                            ad_reply_timestamps[key] = deque(timestamps, maxlen=5)
                        except Exception as e:
                            logger.error(f"Error advertising: {e}")

        if chat.type in (ChatType.GROUP, ChatType.CHANNEL):
            user_data = await get_user_cached(my_id) or {}
            if user_data.get("auto_seen_enabled", False):
                now = time.time()
                last = auto_seen_last_time.get(chat_id, 0)
                if now - last > 5:
                    try:
                        await user_client.seen_chat(chat_id, chat.type)
                        auto_seen_last_time[chat_id] = now
                    except Exception as e:
                        if "user_rate_limited" in str(e):
                            logger.debug(f"Auto-seen rate limited for chat {chat_id}")
                        else:
                            logger.error(f"Error in auto-seen for chat {chat_id}: {e}")

    return user_client

# ==================== اجرای پایدار کلاینت ====================
async def run_client_forever(user_client: Client, user_id: int):
    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        try:
            user_client.phone_code_callback = user_client.phone_code_callback or (lambda *a, **k: (lambda: None)())
            await user_client.start(run_in_background=False, signal_handling=False)
            await ensure_user_id(user_client)
            attempt = 0
            while True:
                await asyncio.sleep(1)
                if not hasattr(user_client, '_websocket') or user_client._websocket is None:
                    logger.info(f"Client {user_id} disconnected, reconnecting...")
                    break
        except asyncio.CancelledError:
            try:
                await user_client.stop()
            except:
                pass
            break
        except Exception as e:
            error_msg = str(e).lower()
            if any(x in error_msg for x in ["cannot write to closing transport", "unknown wire type",
                "list index out of range", "unauthorized", "auth_key_unregistered",
                "invalid session", "session revoked", "user deactivated"]):
                logger.warning(f"Session {user_id} invalid: {error_msg}")
                if user_id in sessions:
                    del sessions[user_id]
                session_file = os.path.join(SESSIONS_DIR, f"session_{user_id}.bale")
                if os.path.exists(session_file):
                    try:
                        os.remove(session_file)
                    except:
                        pass
                await db_remove_active_session(user_id)
                try:
                    await send_bot_message(user_id, "⚠️ نشست شما نامعتبر یا خراب است. لطفاً مجدداً /start را بفرستید.")
                except:
                    pass
                break
            elif any(x in error_msg for x in ["connection lost", "cannot connect to host", "broken pipe", "eof", "timeout"]):
                attempt += 1
                if attempt >= max_attempts:
                    if user_id in sessions:
                        del sessions[user_id]
                    try:
                        await user_client.stop()
                    except:
                        pass
                    await db_remove_active_session(user_id)
                    break
                wait_time = min(2 ** attempt, 30)
                try:
                    await user_client.stop()
                except:
                    pass
                await asyncio.sleep(wait_time)
                settings = await get_user_cached(user_id) or {}
                new_client = create_user_client(user_id, settings)
                if user_id in sessions:
                    sessions[user_id]["client"] = new_client
                else:
                    sessions[user_id] = {"step": "connected", "client": new_client}
                user_client = new_client
                continue
            else:
                logger.error(f"Client error for {user_id}: {error_msg}")
                attempt += 1
                if attempt >= max_attempts:
                    if user_id in sessions:
                        del sessions[user_id]
                    try:
                        await user_client.stop()
                    except:
                        pass
                    await db_remove_active_session(user_id)
                    break
                await asyncio.sleep(3)
                continue

# ─── پایان بخش ۶ ───
# ==================== تابع امن لاگین ====================
async def safe_handle_self_login(chat_id: int, phone: str, admin_states: dict):
    task = asyncio.current_task()
    ACTIVE_LOGIN_TASKS.add(task)
    try:
        async with LOGIN_SEMAPHORE:
            await handle_self_login(chat_id, phone, admin_states)
    except Exception as e:
        logger.error(f"[SAFE-LOGIN] Crash for {chat_id}: {e}", exc_info=True)
        try:
            await send_bot_message(chat_id,
                f"❌ خطا در فرآیند فعال‌سازی:\n`{str(e)[:300]}`\n\nلطفاً مجدداً تلاش کنید.")
        except:
            pass
        if chat_id in admin_states:
            del admin_states[chat_id]
        if chat_id in sessions:
            try:
                if "client" in sessions[chat_id]:
                    await sessions[chat_id]["client"].stop()
            except:
                pass
            del sessions[chat_id]
    finally:
        ACTIVE_LOGIN_TASKS.discard(task)

# ==================== تابع لاگین ====================
async def handle_self_login(chat_id: int, phone: str, admin_states: dict):
    user_client = None
    try:
        logger.info(f"[LOGIN-START] user={chat_id}, phone={phone}")
        phone_for_auth = '98' + phone if phone.startswith('9') else phone
        logger.info(f"[LOGIN] phone_for_auth={phone_for_auth}")

        try:
            user_data = await get_user_cached(chat_id) or {}
        except Exception as e:
            logger.error(f"[LOGIN] Failed to get user data: {e}")
            user_data = {}

        try:
            user_client = create_user_client(chat_id, user_data)
            logger.info(f"[LOGIN] Client created successfully")
        except Exception as e:
            logger.error(f"[LOGIN] Failed to create client: {e}", exc_info=True)
            await send_bot_message(chat_id, f"❌ خطا در ساخت کلاینت:\n`{str(e)[:300]}`")
            if chat_id in admin_states:
                del admin_states[chat_id]
            return

        auth_method = None
        for method_name in ['start_phone_auth', 'send_code_request', 'send_code', 'auth_send_code']:
            if hasattr(user_client, method_name):
                auth_method = getattr(user_client, method_name)
                logger.info(f"[LOGIN] Using method: {method_name}")
                break

        if auth_method is None:
            logger.error(f"[LOGIN] No phone auth method found!")
            await send_bot_message(chat_id, "❌ متد ارسال کد در کتابخانه aiobale یافت نشد!\nلطفاً نسخه کتابخانه را بررسی کنید.")
            if chat_id in admin_states:
                del admin_states[chat_id]
            return

        try:
            auth_res = await asyncio.wait_for(auth_method(phone_for_auth), timeout=30.0)
            logger.info(f"[LOGIN] Auth response received")
        except asyncio.TimeoutError:
            await send_bot_message(chat_id, "❌ زمان ارسال کد به پایان رسید. لطفاً دوباره تلاش کنید.")
            if chat_id in admin_states:
                del admin_states[chat_id]
            return
        except Exception as e:
            error_msg = str(e)
            logger.error(f"[LOGIN] Auth error: {error_msg}", exc_info=True)
            if any(x in error_msg.lower() for x in ["invalid", "wrong", "phone"]):
                await send_bot_message(chat_id,
                    "❌ شماره تلفن نامعتبر است.\n\n📱 لطفاً شماره موبایل خود را به یکی از فرمت‌های زیر وارد کنید:\n"
                    "• `09123456789`\n• `9123456789`\n• `۰۹۱۲۳۴۵۶۷۸۹`\n• `۹۱۲۳۴۵۶۷۸۹`")
            else:
                await send_bot_message(chat_id, f"❌ خطا در ارسال کد:\n`{error_msg[:300]}`")
            if chat_id in admin_states:
                del admin_states[chat_id]
            if chat_id in sessions:
                del sessions[chat_id]
            return

        tx_hash = getattr(auth_res, 'transaction_hash', None) or str(auth_res)
        logger.info(f"[LOGIN] tx_hash={tx_hash}")

        sessions[chat_id] = {"step": "waiting_code_for_self", "phone": phone, "tx_hash": tx_hash, "client": user_client}
        if chat_id not in admin_states:
            admin_states[chat_id] = {}
        admin_states[chat_id]["state"] = "waiting_code_for_self"
        admin_states[chat_id]["phone"] = phone

        await send_bot_message(chat_id,
            "🔑 **کد تأیید ارسال شد!**\n\n📥 لطفاً کد ۵ رقمی را که به شماره شما پیامک شده است وارد کنید:\n"
            "⚠️ کد را بدون فاصله و خط تیره وارد کنید.\nبرای انصراف /cancel را بفرستید.")
        logger.info(f"[LOGIN] Code sent for {chat_id}")
    except Exception as e:
        logger.error(f"[LOGIN] Unexpected error: {e}", exc_info=True)
        try:
            await send_bot_message(chat_id, f"❌ خطای غیرمنتظره:\n`{str(e)[:300]}`")
        except:
            pass
        if chat_id in admin_states:
            del admin_states[chat_id]
        if chat_id in sessions:
            try:
                if user_client and "client" in sessions.get(chat_id, {}):
                    await sessions[chat_id]["client"].stop()
            except:
                pass
            del sessions[chat_id]

# ==================== تأیید کد ====================
async def handle_self_code_validation(chat_id: int, code: str, admin_states: dict):
    try:
        if chat_id not in sessions:
            await send_bot_message(chat_id, "❌ نشست منقضی شده است. لطفاً دوباره تلاش کنید.")
            if chat_id in admin_states:
                del admin_states[chat_id]
            return

        user_client = sessions[chat_id]["client"]
        tx_hash = sessions[chat_id]["tx_hash"]
        code = code.strip().replace(" ", "").replace("-", "")

        if not code.isdigit() or len(code) < 4:
            await send_bot_message(chat_id, "❌ کد نامعتبر. لطفاً کد ۵ رقمی را وارد کنید.")
            return

        try:
            await asyncio.wait_for(user_client.validate_code(code, tx_hash), timeout=30.0)
        except asyncio.TimeoutError:
            await send_bot_message(chat_id, "❌ زمان تأیید کد به پایان رسید. لطفاً دوباره تلاش کنید.")
            if chat_id in sessions:
                del sessions[chat_id]
            if chat_id in admin_states:
                del admin_states[chat_id]
            return
        except Exception as e:
            error_msg = str(e)
            if "invalid" in error_msg.lower() or "wrong" in error_msg.lower():
                await send_bot_message(chat_id, "❌ کد اشتباه است. لطفاً دوباره تلاش کنید.\nبرای انصراف /cancel را بفرستید.")
                if chat_id in admin_states:
                    admin_states[chat_id]["state"] = "waiting_code_for_self"
                return
            else:
                raise e

        logger.info(f"Code validated successfully for user {chat_id}")
        session_file = os.path.join(SESSIONS_DIR, f"session_{chat_id}.bale")
        await db_add_active_session(chat_id, session_file)
        sessions[chat_id]["step"] = "connected"
        create_background_task(run_client_forever(user_client, chat_id))

        if chat_id in admin_states:
            del admin_states[chat_id]

        is_test_user = await has_active_test(chat_id)
        if is_test_user:
            _, remaining = await get_test_remaining_time(chat_id)
            welcome_text = (f"🧪 **حالت تست ۳ ساعته فعال شد!**\n\n⏰ زمان باقی‌مانده: {remaining}\n\n"
                f"✨ از تمام قابلیت‌های ربات استفاده کنید.\n⚠️ بعد از اتمام زمان، ربات خودکار قطع می‌شود.")
        else:
            welcome_text = (f"✅ **سلف {BOT_NAME} با موفقیت فعال شد!**\n\n🎉 اکنون می‌توانید از تمام قابلیت‌ها استفاده کنید.\n"
                f"📋 برای مشاهده وضعیت، از دکمه «وضعیت سلف» استفاده کنید.")

        await send_bot_message(chat_id, welcome_text, get_main_keyboard())
    except Exception as e:
        logger.error(f"Code validation error for {chat_id}: {e}", exc_info=True)
        await send_bot_message(chat_id, f"❌ خطا در تأیید کد: {str(e)[:200]}")
        if chat_id in admin_states:
            admin_states[chat_id]["state"] = "waiting_phone_for_self"
        if chat_id in sessions:
            try:
                if "client" in sessions[chat_id]:
                    await sessions[chat_id]["client"].stop()
            except:
                pass
            del sessions[chat_id]

# ─── پایان بخش ۷ ───
# ==================== Worker ====================
update_queue = asyncio.Queue(maxsize=UPDATE_QUEUE_SIZE)

async def worker():
    while True:
        try:
            update = await asyncio.wait_for(update_queue.get(), timeout=60)
            try:
                await handle_bot_update(update)
            except Exception as e:
                logger.error(f"Handle update error: {e}")
                traceback.print_exc()
            finally:
                update_queue.task_done()
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            logger.info("Worker cancelled")
            break
        except Exception as e:
            logger.error(f"Worker critical error: {e}")
            await asyncio.sleep(1)

# ==================== هندلر آپدیت‌ها ====================
async def handle_bot_update(update: dict):
    try:
        if "callback_query" in update:
            callback = update["callback_query"]
            chat_id = callback["message"]["chat"]["id"]
            message_id = callback["message"]["message_id"]
            data = callback["data"]
            user_id = callback["from"]["id"]

            # ============ چک ادمین ============
            if data.startswith("admin_") and user_id != ADMIN_ID:
                await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": "🚫 شما ادمین نیستید!", "show_alert": True})
                return

            # ============ منوی اصلی ============
            if data == "buy_subscription":
                await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": "✅ در حال ارسال اطلاعات..."})
                await edit_bot_message(chat_id, message_id,
                    f"🎫 **خرید اشتراک {BOT_NAME}**\n\n💰 **قیمت اشتراک:** 35,000 تومان\n\n"
                    f"برای خرید اشتراک، لطفاً با پشتیبانی تماس بگیرید:\n\n📞 **آیدی پشتیبانی:** {SUPPORT_ID}\n\n"
                    f"💡 پس از خرید، اشتراک شما فعال می‌شود.", get_back_keyboard())
                return

            if data == "status_self":
                has_sub = await has_active_subscription(user_id)
                is_test = await has_active_test(user_id)
                test_info = ""
                if is_test:
                    _, remaining = await get_test_remaining_time(user_id)
                    test_info = f"\n🧪 **تست ۳ ساعته:** فعال ({remaining})"
                if has_sub:
                    _, remaining_text, days, hours, minutes = await get_remaining_time(user_id)
                    session_exists = user_id in sessions and sessions[user_id].get('step') == 'connected'
                    session_status = "🟢 فعال" if session_exists else "🔴 غیرفعال"
                    status_text = (f"📊 **وضعیت سلف {BOT_NAME}**\n\n✅ **اشتراک:** فعال\n⏰ **زمان باقی‌مانده:** {remaining_text}\n"
                        f"📱 **وضعیت سشن:** {session_status}{test_info}\n\n🚀 برای فعال‌سازی سلف، از دکمه «فعال‌سازی سلف» استفاده کنید.")
                else:
                    status_text = (f"📊 **وضعیت سلف {BOT_NAME}**\n\n❌ **اشتراک:** غیرفعال{test_info}\n\n"
                        f"🎫 برای خرید اشتراک، روی دکمه «خرید اشتراک» کلیک کنید.\n📞 آیدی پشتیبانی: {SUPPORT_ID}")
                await edit_bot_message(chat_id, message_id, status_text, get_back_keyboard())
                return

            if data == "activate_self":
                all_member, not_member = await check_all_memberships(user_id)
                if not all_member:
                    text = "⚠️ **شما در همه کانال‌های اجباری عضو نیستید!**\n\n"
                    for ch in not_member:
                        display_name = ch.get("channel_name") or f"کانال {ch['channel_id']}"
                        text += f"\n🔹 [عضویت در {display_name}]({ch['invite_link']})"
                    text += "\n\nپس از عضویت، روی دکمه «بررسی عضویت» کلیک کنید."
                    await edit_bot_message(chat_id, message_id, text, get_forced_channels_keyboard(not_member, user_id))
                    return
                has_sub = await has_active_subscription(user_id)
                is_test = await has_active_test(user_id)
                if not has_sub and not is_test:
                    await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": "❌ اشتراک یا تست فعالی ندارید!", "show_alert": True})
                    await edit_bot_message(chat_id, message_id,
                        f"❌ **شما اشتراک یا تست فعالی ندارید!**\n\n🎫 برای خرید اشتراک، روی دکمه «خرید اشتراک» کلیک کنید.\n📞 آیدی پشتیبانی: {SUPPORT_ID}",
                        get_back_keyboard())
                    return
                session_exists = user_id in sessions and sessions[user_id].get('step') == 'connected'
                if session_exists:
                    keyboard = {"inline_keyboard": [
                        [{"text": "🔄 فعال‌سازی مجدد سلف", "callback_data": "reactivate_self"}],
                        [{"text": "🔙 بازگشت", "callback_data": "back_to_main"}]]}
                    await edit_bot_message(chat_id, message_id,
                        f"✅ **سلف {BOT_NAME} در حال حاضر فعال است!**\n\nاگر ربات به درستی کار نمی‌کند، می‌توانید مجدداً فعال کنید.",
                        keyboard)
                    return
                else:
                    admin_states[chat_id] = {"state": "waiting_phone_for_self"}
                    await edit_bot_message(chat_id, message_id,
                        f"🚀 **فعال‌سازی سلف {BOT_NAME}**\n\n📱 لطفاً شماره موبایل خود را وارد کنید:\n\n"
                        f"✅ **فرمت‌های مجاز:**\n• `09123456789`\n• `9123456789`\n• `۰۹۱۲۳۴۵۶۷۸۹`\n• `۹۱۲۳۴۵۶۷۸۹`\n\n"
                        f"برای انصراف /cancel را بفرستید.", None)
                    return

            if data == "reactivate_self":
                if user_id in sessions:
                    try:
                        if "client" in sessions[user_id]:
                            await sessions[user_id]["client"].stop()
                    except:
                        pass
                    del sessions[user_id]
                await db_remove_active_session(user_id)
                session_file = os.path.join(SESSIONS_DIR, f"session_{user_id}.bale")
                if os.path.exists(session_file):
                    try:
                        os.remove(session_file)
                    except:
                        pass
                admin_states[chat_id] = {"state": "waiting_phone_for_self"}
                await edit_bot_message(chat_id, message_id,
                    f"🔄 **فعال‌سازی مجدد سلف {BOT_NAME}**\n\n📱 لطفاً شماره موبایل خود را وارد کنید:\n\nبرای انصراف /cancel", None)
                return

            if data == "help_self":
                help_text = (f"🌟 **راهنمای {BOT_NAME}** 🌟\n\n📞 پشتیبانی: {SUPPORT_ID}\n\n"
                    f"📌 برای لیست کامل دستورات، در چت سلف `.راهنما` را بفرستید.")
                keyboard = {"inline_keyboard": [[{"text": "🔙 بازگشت", "callback_data": "back_to_main"}]]}
                await edit_bot_message(chat_id, message_id, help_text, keyboard)
                return

            if data == "back_to_main":
                await edit_bot_message(chat_id, message_id,
                    f"🎫 **به {BOT_NAME} خوش آمدید!**\n\nاز دکمه‌های زیر استفاده کنید:", get_main_keyboard())
                return

            # ============ پنل ادمین ============
            if data == "admin_add_subscription":
                if user_id != ADMIN_ID: return
                admin_states[chat_id] = {"state": "waiting_user_id_for_sub"}
                await edit_bot_message(chat_id, message_id,
                    f"🎁 **اضافه کردن اشتراک**\n\nلطفاً شناسه عددی کاربر را وارد کنید:\nمثال: `123456789`\n\nبرای انصراف /cancel", None)
                return

            if data == "admin_ban_user":
                if user_id != ADMIN_ID: return
                admin_states[chat_id] = {"state": "waiting_ban_user_id"}
                await edit_bot_message(chat_id, message_id, f"🚫 **بن کردن کاربر**\n\nشناسه عددی کاربر را وارد کنید:\nمثال: `123456789`\n\nبرای انصراف /cancel", None)
                return

            if data == "admin_unban_user":
                if user_id != ADMIN_ID: return
                admin_states[chat_id] = {"state": "waiting_unban_user_id"}
                await edit_bot_message(chat_id, message_id, f"✅ **آنبن کردن کاربر**\n\nشناسه عددی کاربر را وارد کنید:\nمثال: `123456789`\n\nبرای انصراف /cancel", None)
                return

            if data == "admin_stats_users":
                if user_id != ADMIN_ID: return
                total_users = len(users_db)
                banned_count = len(banned_users)
                active = total_users - banned_count
                sub_count = 0
                test_count = 0
                for uid in users_db.keys():
                    if await has_active_subscription(uid): sub_count += 1
                    if await has_active_test(uid): test_count += 1
                text = (f"📊 **آمار {BOT_NAME}**\n\n👥 کل کاربران: `{total_users}`\n✅ فعال: `{active}`\n"
                    f"🚫 بن شده: `{banned_count}`\n🎁 دارای اشتراک: `{sub_count}`\n🧪 تست ۳ ساعته: `{test_count}`")
                await edit_bot_message(chat_id, message_id, text, get_admin_panel_markup())
                return

            if data == "admin_list_users":
                if user_id != ADMIN_ID: return
                users_list = list(users_db.keys())
                if not users_list:
                    await edit_bot_message(chat_id, message_id, "❌ هیچ کاربری یافت نشد.", get_admin_panel_markup())
                    return
                page = 0
                per_page = 10
                total_pages = (len(users_list) + per_page - 1) // per_page
                start = page * per_page
                end = start + per_page
                page_users = users_list[start:end]
                text = f"📋 **لیست کاربران (صفحه {page+1}/{total_pages})**\n\n"
                for uid in page_users:
                    status = "🔴 بن" if uid in banned_users else "🟢 فعال"
                    has_sub = await has_active_subscription(uid)
                    sub_status = "✅" if has_sub else "❌"
                    is_test = await has_active_test(uid)
                    test_status = "🧪" if is_test else ""
                    text += f"🆔 `{uid}` - {status} - اشتراک: {sub_status} {test_status}\n"
                nav_buttons = []
                if page > 0:
                    nav_buttons.append({"text": "⏪ قبلی", "callback_data": f"admin_list_page_{page-1}"})
                if page < total_pages - 1:
                    nav_buttons.append({"text": "بعدی ⏩", "callback_data": f"admin_list_page_{page+1}"})
                action_buttons = []
                for uid in page_users[:5]:
                    if uid in banned_users:
                        action_buttons.append({"text": f"✅ آنبن {uid}", "callback_data": f"admin_unban_{uid}"})
                    else:
                        action_buttons.append({"text": f"🚫 بن {uid}", "callback_data": f"admin_ban_{uid}"})
                markup = {"inline_keyboard": [nav_buttons] if nav_buttons else []}
                if action_buttons:
                    for i in range(0, len(action_buttons), 2):
                        markup["inline_keyboard"].append(action_buttons[i:i+2])
                markup["inline_keyboard"].append([{"text": "🔙 بازگشت به پنل", "callback_data": "admin_panel"}])
                await edit_bot_message(chat_id, message_id, text, markup)
                return

            if data.startswith("admin_list_page_"):
                if user_id != ADMIN_ID: return
                page = int(data.split("_")[-1])
                users_list = list(users_db.keys())
                per_page = 10
                total_pages = (len(users_list) + per_page - 1) // per_page
                start = page * per_page
                end = start + per_page
                page_users = users_list[start:end]
                text = f"📋 **لیست کاربران (صفحه {page+1}/{total_pages})**\n\n"
                for uid in page_users:
                    status = "🔴 بن" if uid in banned_users else "🟢 فعال"
                    has_sub = await has_active_subscription(uid)
                    sub_status = "✅" if has_sub else "❌"
                    is_test = await has_active_test(uid)
                    test_status = "🧪" if is_test else ""
                    text += f"🆔 `{uid}` - {status} - اشتراک: {sub_status} {test_status}\n"
                nav_buttons = []
                if page > 0:
                    nav_buttons.append({"text": "⏪ قبلی", "callback_data": f"admin_list_page_{page-1}"})
                if page < total_pages - 1:
                    nav_buttons.append({"text": "بعدی ⏩", "callback_data": f"admin_list_page_{page+1}"})
                action_buttons = []
                for uid in page_users[:5]:
                    if uid in banned_users:
                        action_buttons.append({"text": f"✅ آنبن {uid}", "callback_data": f"admin_unban_{uid}"})
                    else:
                        action_buttons.append({"text": f"🚫 بن {uid}", "callback_data": f"admin_ban_{uid}"})
                markup = {"inline_keyboard": [nav_buttons] if nav_buttons else []}
                if action_buttons:
                    for i in range(0, len(action_buttons), 2):
                        markup["inline_keyboard"].append(action_buttons[i:i+2])
                markup["inline_keyboard"].append([{"text": "🔙 بازگشت به پنل", "callback_data": "admin_panel"}])
                await edit_bot_message(chat_id, message_id, text, markup)
                return

            if data.startswith("admin_ban_"):
                if user_id != ADMIN_ID: return
                target_id = int(data.split("_")[2])
                user_data = await db_get_user(target_id)
                if not user_data:
                    await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": f"❌ کاربر {target_id} یافت نشد!", "show_alert": True})
                    return
                if await db_is_banned(target_id):
                    await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": f"⚠️ کاربر {target_id} قبلاً بن شده!", "show_alert": True})
                    return
                await db_add_banned(target_id)
                banned_users.add(target_id)
                try:
                    await send_bot_message(target_id,
                        f"🚫 **شما توسط ادمین بن شدید!**\n\n❌ دیگر نمی‌توانید از ربات {BOT_NAME} استفاده کنید.\n📞 پشتیبانی: {SUPPORT_ID}")
                except: pass
                await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": f"✅ کاربر {target_id} بن شد!", "show_alert": True})
                await edit_bot_message(chat_id, message_id, "🔄 در حال به‌روزرسانی...", get_admin_panel_markup())
                return

            if data.startswith("admin_unban_"):
                if user_id != ADMIN_ID: return
                target_id = int(data.split("_")[2])
                if not await db_is_banned(target_id):
                    await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": f"⚠️ کاربر {target_id} بن نشده!", "show_alert": True})
                    return
                await db_remove_banned(target_id)
                banned_users.discard(target_id)
                try:
                    await send_bot_message(target_id,
                        f"✅ **شما توسط ادمین آنبن شدید!**\n\n🎉 دوباره می‌توانید از ربات {BOT_NAME} استفاده کنید.",
                        get_main_keyboard())
                except: pass
                await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": f"✅ کاربر {target_id} آنبن شد!", "show_alert": True})
                await edit_bot_message(chat_id, message_id, "🔄 در حال به‌روزرسانی...", get_admin_panel_markup())
                return

            if data == "admin_panel":
                if user_id != ADMIN_ID: return
                await edit_bot_message(chat_id, message_id, f"👑 **پنل مدیریت {BOT_NAME}**\n\nگزینه‌ای انتخاب کنید:", get_admin_panel_markup())
                return

            if data == "admin_broadcast":
                if user_id != ADMIN_ID: return
                admin_states[chat_id] = {"state": "waiting_broadcast"}
                await edit_bot_message(chat_id, message_id, "📢 **ارسال همگانی**\n\nپیام خود را ارسال کنید.\n(برای انصراف /cancel)", None)
                return

            if data == "admin_close":
                if user_id != ADMIN_ID: return
                await edit_bot_message(chat_id, message_id, "🔒 پنل بسته شد.", None)
                if chat_id in admin_states: del admin_states[chat_id]
                return

            if data == "admin_add_channel":
                if user_id != ADMIN_ID: return
                admin_states[chat_id] = {"state": "admin_add_channel_waiting_id"}
                await edit_bot_message(chat_id, message_id,
                    "➕ **اضافه کردن کانال اجباری**\n\nآیدی **عددی** کانال را وارد کنید:\n\nبرای انصراف /cancel", None)
                return

            if data == "admin_remove_channel":
                if user_id != ADMIN_ID: return
                admin_states[chat_id] = {"state": "admin_remove_channel_waiting_id"}
                await edit_bot_message(chat_id, message_id,
                    "➖ **حذف کانال اجباری**\n\nآیدی **عددی** کانال را وارد کنید:\nبرای انصراف /cancel", None)
                return

            if data == "admin_list_channels":
                if user_id != ADMIN_ID: return
                channels = await get_all_forced_channels()
                if not channels:
                    await edit_bot_message(chat_id, message_id, "📋 **لیست کانال‌های اجباری**\n\n❌ هیچ کانالی ثبت نشده است.", get_admin_panel_markup())
                    return
                text = "📋 **لیست کانال‌های اجباری**\n\n"
                for ch in channels:
                    status = "🟢 فعال" if ch["is_active"] else "🔴 غیرفعال"
                    display_name = ch.get("channel_name") or f"کانال {ch['channel_id']}"
                    text += f"📛 نام: {display_name}\n🆔 آیدی: `{ch['channel_id']}`\n🔗 لینک: {ch['invite_link']}\n📌 وضعیت: {status}\n\n"
                await edit_bot_message(chat_id, message_id, text, get_admin_panel_markup())
                return

            if data.startswith("check_membership_"):
                target_user_id = int(data.split("_")[2])
                if target_user_id != user_id:
                    await bot_session.post(f"{BASE_URL}/answerCallbackQuery", json={"callback_query_id": callback["id"], "text": "❌ این دکمه مخصوص شما نیست!", "show_alert": True})
                    return
                all_member, not_member = await check_all_memberships(user_id)
                if all_member:
                    await edit_bot_message(chat_id, message_id,
                        f"🎫 **به {BOT_NAME} خوش آمدید!** 🎫\n\n⚡ **قدرت مدیریت سلف!**\n\n"
                        f"📌 مراحل:\n1️⃣ خرید اشتراک\n2️⃣ وضعیت سلف\n3️⃣ فعال‌سازی سلف\n\n"
                        f"📞 پشتیبانی: {SUPPORT_ID}",
                        get_main_keyboard())
                else:
                    text = "⚠️ **شما هنوز در همه کانال‌ها عضو نشده‌اید!**\n\nلطفاً ابتدا در کانال‌های زیر عضو شوید:\n"
                    for ch in not_member:
                        display_name = ch.get("channel_name") or f"کانال {ch['channel_id']}"
                        text += f"\n🔹 [عضویت در {display_name}]({ch['invite_link']})"
                    text += "\n\nپس از عضویت، روی دکمه «بررسی عضویت» کلیک کنید."
                    await edit_bot_message(chat_id, message_id, text, get_forced_channels_keyboard(not_member, user_id))
                return

            # ============ تنظیمات سلف ============
            session = sessions.get(user_id)
            if not session or session.get("step") != "connected" or "client" not in session:
                await send_bot_message(chat_id, "❌ نشست شما فعال نیست. لطفاً /start را بفرستید.")
                return

            user_client = session["client"]
            try:
                await ensure_user_id(user_client)
            except:
                await send_bot_message(chat_id, "❌ خطا در دریافت اطلاعات کاربر. لطفاً /start را بفرستید.")
                return
            my_id = user_client.me.id

            if data == "open_clock_settings":
                user_data = await get_user_cached(my_id) or {}
                await edit_bot_message(chat_id, message_id, "🕘 **پنل مدیریت ساعت پروفایل**", get_clock_settings_markup(my_id, user_data))
                return

            if data.startswith("clock_"):
                user_data = await get_user_cached(my_id) or {}
                if "clock" not in user_data: user_data["clock"] = {}

                if data == "clock_name_toggle":
                    user_data["clock"]["name_enabled"] = not user_data["clock"].get("name_enabled", False)
                    await set_user_cached(my_id, user_data)
                    await update_user_profile(user_client, my_id)
                    await edit_bot_message(chat_id, message_id, "🕘 **پنل ساعت**", get_clock_settings_markup(my_id, user_data))
                elif data == "clock_bio_toggle":
                    user_data["clock"]["bio_enabled"] = not user_data["clock"].get("bio_enabled", False)
                    await set_user_cached(my_id, user_data)
                    await update_user_profile(user_client, my_id)
                    await edit_bot_message(chat_id, message_id, "🕘 **پنل ساعت**", get_clock_settings_markup(my_id, user_data))
                elif data == "clock_name_font":
                    current = user_data["clock"].get("name_font", "normal")
                    user_data["clock"]["name_font"] = "fancy" if current == "normal" else "normal"
                    await set_user_cached(my_id, user_data)
                    if user_data["clock"].get("name_enabled", False):
                        await update_user_profile(user_client, my_id)
                    await edit_bot_message(chat_id, message_id, "🕘 **پنل ساعت**", get_clock_settings_markup(my_id, user_data))
                elif data == "clock_bio_font":
                    current = user_data["clock"].get("bio_font", "normal")
                    user_data["clock"]["bio_font"] = "fancy" if current == "normal" else "normal"
                    await set_user_cached(my_id, user_data)
                    if user_data["clock"].get("bio_enabled", False):
                        await update_user_profile(user_client, my_id)
                    await edit_bot_message(chat_id, message_id, "🕘 **پنل ساعت**", get_clock_settings_markup(my_id, user_data))
                elif data == "clock_name_format":
                    session["awaiting_format"] = "name"
                    await send_bot_message(chat_id, "✏️ قالب جدید برای **نام** را ارسال کنید.\nمثال: `⏰ {time}`\nبرای انصراف /cancel")
                    return
                elif data == "clock_bio_format":
                    session["awaiting_format"] = "bio"
                    await send_bot_message(chat_id, "✏️ قالب جدید برای **بیو** را ارسال کنید.\nمثال: `🕒 {time}`\nبرای انصراف /cancel")
                    return
                elif data == "clock_back":
                    await edit_bot_message(chat_id, message_id, "⚙️ **پنل سلف:**", get_settings_markup(
                        getattr(user_client, "is_bold_active", True), getattr(user_client, "is_blue_active", False),
                        getattr(user_client, "is_italic_active", False), getattr(user_client, "is_gift_active", True),
                        getattr(user_client, "is_reaction_active", True)))
                return

            if data == "toggle_bold":
                user_client.is_bold_active = not getattr(user_client, "is_bold_active", True)
            elif data == "toggle_italic":
                user_client.is_italic_active = not getattr(user_client, "is_italic_active", False)
            elif data == "toggle_blue":
                user_client.is_blue_active = not getattr(user_client, "is_blue_active", False)
            elif data == "toggle_gift":
                user_client.is_gift_active = not getattr(user_client, "is_gift_active", True)
            elif data == "toggle_reaction":
                user_client.is_reaction_active = not getattr(user_client, "is_reaction_active", True)
            else:
                return

            user_data = await get_user_cached(my_id) or {}
            user_data.update({"is_bold_active": user_client.is_bold_active, "is_italic_active": user_client.is_italic_active,
                "is_blue_active": user_client.is_blue_active, "is_gift_active": user_client.is_gift_active,
                "is_reaction_active": user_client.is_reaction_active})
            await set_user_cached(my_id, user_data)
            await edit_bot_message(chat_id, message_id,
                "⚙️ **پنل مدیریت سلف‌بات:**",
                get_settings_markup(user_client.is_bold_active, user_client.is_blue_active, user_client.is_italic_active,
                    user_client.is_gift_active, user_client.is_reaction_active))
            return

        # ==================== پردازش پیام‌ها ====================
        msg = update.get("message")
        if not msg:
            return

        chat_id = msg["chat"]["id"]
        user_id = msg.get("from", {}).get("id")
        text = msg.get("text", "").strip()

        if await db_is_banned(user_id):
            await send_bot_message(chat_id, "🚫 شما توسط ادمین بن شده‌اید.")
            return

        if chat_id in admin_states:
            state = admin_states[chat_id]["state"]
            if text.lower() == "/cancel":
                del admin_states[chat_id]
                await send_bot_message(chat_id, "❌ عملیات لغو شد.", get_main_keyboard())
                return

            if state == "waiting_phone_for_self":
                phone = normalize_phone(text)
                logger.info(f"Original: '{text}' → Normalized: '{phone}'")
                if len(phone) != 10 or not phone.startswith('9'):
                    await send_bot_message(chat_id,
                        "❌ **شماره نامعتبر است!**\n\n📱 فرمت‌های مجاز:\n"
                        "• `09123456789`\n• `9123456789`\n• `۰۹۱۲۳۴۵۶۷۸۹`\n• `۹۱۲۳۴۵۶۷۸۹`")
                    return
                admin_states[chat_id]["phone"] = phone
                await send_bot_message(chat_id, "⏳ **ارسال کد...**")
                create_background_task(safe_handle_self_login(chat_id, phone, admin_states))
                return

            if state == "waiting_code_for_self":
                if text.lower() == "/cancel":
                    del admin_states[chat_id]
                    if chat_id in sessions:
                        try:
                            if "client" in sessions[chat_id]:
                                await sessions[chat_id]["client"].stop()
                        except: pass
                        del sessions[chat_id]
                    await send_bot_message(chat_id, "❌ لغو شد.", get_main_keyboard())
                    return
                create_background_task(handle_self_code_validation(chat_id, text, admin_states))
                await send_bot_message(chat_id, "⏳ **بررسی کد...**")
                return

            if state == "waiting_user_id_for_sub":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                if text.lower() == "/cancel":
                    del admin_states[chat_id]
                    await send_bot_message(chat_id, "❌ لغو شد.", get_admin_panel_markup()); return
                try:
                    target_id = int(text.strip())
                    user_data = await db_get_user(target_id)
                    if not user_data:
                        await db_set_user(target_id, {})
                    admin_states[chat_id]["target_id"] = target_id
                    admin_states[chat_id]["state"] = "waiting_days_for_sub"
                    await send_bot_message(chat_id, f"✅ کاربر `{target_id}`\n📅 تعداد روز اشتراک را وارد کنید:\nمثال: `30`")
                except ValueError:
                    await send_bot_message(chat_id, "❌ آیدی نامعتبر!")
                return

            if state == "waiting_days_for_sub":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                if text.lower() == "/cancel":
                    del admin_states[chat_id]
                    await send_bot_message(chat_id, "❌ لغو شد.", get_admin_panel_markup()); return
                try:
                    days = int(text.strip())
                    if days < 1 or days > 365:
                        await send_bot_message(chat_id, "❌ تعداد روز 1 تا 365!"); return
                    target_id = admin_states[chat_id]["target_id"]
                    end_date = await add_subscription(target_id, days)
                    try:
                        await send_bot_message(target_id,
                            f"🎉 **اشتراک {BOT_NAME} فعال شد!**\n\n📅 انقضا: {end_date.strftime('%Y-%m-%d %H:%M')}\n\n"
                            f"🚀 دکمه «فعال‌سازی سلف» رو بزن.", get_main_keyboard())
                    except: pass
                    del admin_states[chat_id]
                    await send_bot_message(chat_id,
                        f"✅ **اشتراک اضافه شد!**\n👤 کاربر: `{target_id}`\n📅 {days} روز\n📆 انقضا: {end_date.strftime('%Y-%m-%d %H:%M')}",
                        get_admin_panel_markup())
                except ValueError:
                    await send_bot_message(chat_id, "❌ عدد بفرست!")
                return

            if state == "waiting_ban_user_id":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                try:
                    target_id = int(text)
                    user_data = await db_get_user(target_id)
                    if not user_data:
                        await send_bot_message(chat_id, f"❌ کاربر `{target_id}` پیدا نشد!"); return
                    if await db_is_banned(target_id):
                        await send_bot_message(chat_id, f"⚠️ کاربر `{target_id}` قبلاً بن شده!")
                        del admin_states[chat_id]; return
                    await db_add_banned(target_id)
                    banned_users.add(target_id)
                    try:
                        await send_bot_message(target_id,
                            f"🚫 **بن شدید!**\n\n❌ دسترسی شما به {BOT_NAME} قطع شد.\n📞 {SUPPORT_ID}")
                    except: pass
                    del admin_states[chat_id]
                    await send_bot_message(chat_id, f"✅ **کاربر `{target_id}` بن شد!**", get_admin_panel_markup())
                except ValueError:
                    await send_bot_message(chat_id, "❌ آیدی نامعتبر!")
                return

            if state == "waiting_unban_user_id":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                try:
                    target_id = int(text)
                    if not await db_is_banned(target_id):
                        await send_bot_message(chat_id, f"⚠️ کاربر `{target_id}` بن نشده!")
                        del admin_states[chat_id]; return
                    await db_remove_banned(target_id)
                    banned_users.discard(target_id)
                    try:
                        await send_bot_message(target_id,
                            f"✅ **آنبن شدید!**\n\n🎉 دوباره به {BOT_NAME} خوش آمدید.",
                            get_main_keyboard())
                    except: pass
                    del admin_states[chat_id]
                    await send_bot_message(chat_id, f"✅ **کاربر `{target_id}` آنبن شد!**", get_admin_panel_markup())
                except ValueError:
                    await send_bot_message(chat_id, "❌ آیدی نامعتبر!")
                return

            if state == "waiting_broadcast":
                total_users = len(users_db)
                sem = asyncio.Semaphore(CONCURRENT_BROADCAST)
                async def send_to_user(uid):
                    if await db_is_banned(uid): return
                    async with sem:
                        await send_copy_message(uid, msg)
                tasks = [send_to_user(uid) for uid in users_db.keys()]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                success = sum(1 for r in results if not isinstance(r, Exception))
                await send_bot_message(chat_id, f"✅ پیام به {success} از {total_users} ارسال شد.")
                del admin_states[chat_id]
                await send_bot_message(chat_id, "👑 پنل مدیریت:", get_admin_panel_markup())
                return

            if state == "admin_add_channel_waiting_id":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                try:
                    channel_id = int(text)
                    admin_states[chat_id]["channel_id"] = channel_id
                    admin_states[chat_id]["state"] = "admin_add_channel_waiting_link"
                    await send_bot_message(chat_id, f"✅ آیدی: `{channel_id}`\n\n🔗 لینک دعوت:\nمثال: `https://ble.ir/join/abc123`\n\nبرای انصراف /cancel")
                except ValueError:
                    await send_bot_message(chat_id, "❌ آیدی نامعتبر!")
                return

            if state == "admin_add_channel_waiting_link":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                invite_link = text.strip()
                if not invite_link.startswith("http"):
                    await send_bot_message(chat_id, "❌ لینک باید با http شروع شود!"); return
                admin_states[chat_id]["invite_link"] = invite_link
                admin_states[chat_id]["state"] = "admin_add_channel_waiting_name"
                await send_bot_message(chat_id, f"✅ لینک: {invite_link}\n\n📛 نام کانال:\n\nبرای انصراف /cancel")
                return

            if state == "admin_add_channel_waiting_name":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                channel_name = text.strip()
                if channel_name.lower() == "/skip": channel_name = None
                channel_id = admin_states[chat_id]["channel_id"]
                invite_link = admin_states[chat_id]["invite_link"]
                await add_forced_channel(channel_id, invite_link, channel_name, None)
                del admin_states[chat_id]
                await send_bot_message(chat_id,
                    f"✅ **کانال اجباری اضافه شد!**\n\n📛 نام: {channel_name or f'کانال {channel_id}'}\n🆔 `{channel_id}`\n🔗 {invite_link}",
                    get_admin_panel_markup())
                return

            if state == "admin_remove_channel_waiting_id":
                if user_id != ADMIN_ID:
                    del admin_states[chat_id]; return
                try:
                    channel_id = int(text)
                    channels = await get_all_forced_channels()
                    if not any(ch["channel_id"] == channel_id for ch in channels):
                        await send_bot_message(chat_id, f"❌ کانال `{channel_id}` پیدا نشد!"); return
                    await remove_forced_channel(channel_id)
                    del admin_states[chat_id]
                    await send_bot_message(chat_id, f"✅ **کانال `{channel_id}` حذف شد.**", get_admin_panel_markup())
                except ValueError:
                    await send_bot_message(chat_id, "❌ آیدی نامعتبر!")
                return

        # ============ دستور /admin ============
        if text == "/admin" and user_id == ADMIN_ID:
            await send_bot_message(chat_id, f"👑 **پنل مدیریت {BOT_NAME}**", get_admin_panel_markup())
            return

        # ============ تنظیم قالب ساعت ============
        session = sessions.get(user_id) if user_id else None
        if session and session.get("awaiting_format"):
            if text.lower() == "/cancel":
                session.pop("awaiting_format", None)
                await send_bot_message(chat_id, "❌ لغو شد.")
                return
            if "{time}" not in text:
                await send_bot_message(chat_id, "⚠️ قالب باید شامل `{time}` باشه!")
                return
            user_data = await get_user_cached(user_id) or {}
            if "clock" not in user_data: user_data["clock"] = {}
            target = session["awaiting_format"]
            if target == "name":
                user_data["clock"]["name_format"] = text
                await set_user_cached(user_id, user_data)
                await send_bot_message(chat_id, f"✅ قالب نام: `{text}`")
                if user_data["clock"].get("name_enabled", False):
                    if session.get("client"): await update_user_profile(session["client"], user_id)
            elif target == "bio":
                user_data["clock"]["bio_format"] = text
                await set_user_cached(user_id, user_data)
                await send_bot_message(chat_id, f"✅ قالب بیو: `{text}`")
                if user_data["clock"].get("bio_enabled", False):
                    if session.get("client"): await update_user_profile(session["client"], user_id)
            session.pop("awaiting_format", None)
            return

        # ============ /start ============
        if text == "/start":
            user_data = await get_user_cached(user_id)
            if not user_data:
                await set_user_cached(user_id, {})
            all_member, not_member = await check_all_memberships(user_id)
            if not all_member:
                text_msg = "⚠️ **برای استفاده از ربات باید در کانال‌های زیر عضو شوید**\n\n"
                for ch in not_member:
                    display_name = ch.get("channel_name") or f"کانال {ch['channel_id']}"
                    text_msg += f"\n🔹 [عضویت در {display_name}]({ch['invite_link']})"
                text_msg += "\n\nپس از عضویت، روی دکمه «بررسی عضویت» کلیک کنید."
                await send_bot_message(chat_id, text_msg, get_forced_channels_keyboard(not_member, user_id))
                return
            await send_bot_message(chat_id,
                f"🎫 **به {BOT_NAME} خوش آمدید!** 🎫\n\n⚡ **قدرت مدیریت سلف در دستان تو!**\n\n"
                f"📌 **مراحل استفاده:**\n\n1️⃣ **خرید اشتراک**\n   با پشتیبانی تماس بگیرید.\n\n"
                f"2️⃣ **وضعیت سلف**\n   وضعیت اشتراک و سشن خود را بررسی کنید.\n\n3️⃣ **فعال‌سازی سلف**\n   پس از خرید، شماره خود را وارد کنید.\n\n"
                f"🧪 **تست ۳ ساعته**\n   تست رایگان ۳ ساعته (فقط یک بار)\n\n📞 **پشتیبانی:** {SUPPORT_ID}",
                get_main_keyboard())
            return

        # ============ سشن کاربر ============
        session = sessions.get(user_id) if user_id else None
        if not session:
            sessions[user_id] = {"step": "start"}
            session = sessions[user_id]

        if session.get("step") == "connected" and "client" in session:
            user_client = session["client"]
            if text == "/panel":
                await send_bot_message(chat_id, "⚙️ **پنل سلف:**", get_settings_markup(
                    user_client.is_bold_active, user_client.is_blue_active, user_client.is_italic_active,
                    user_client.is_gift_active, user_client.is_reaction_active))
            return

        if session["step"] == "waiting_phone":
            if not text:
                await send_bot_message(chat_id, "⚠️ شماره خالیه!"); return
            phone = normalize_phone(text)
            if len(phone) != 10 or not phone.startswith('9'):
                await send_bot_message(chat_id, "❌ شماره اشتباه! فرمت‌های مجاز:\n• `09123456789`\n• `9123456789`\n• `۰۹۱۲۳۴۵۶۷۸۹`\n• `۹۱۲۳۴۵۶۷۸۹`")
                return
            await send_bot_message(chat_id, f"⏳ سشن برای `{phone}`...")
            try:
                user_data = await get_user_cached(user_id) or {}
                user_data.setdefault("is_bold_active", True)
                user_data.setdefault("is_italic_active", False)
                user_data.setdefault("is_blue_active", False)
                user_data.setdefault("is_gift_active", True)
                user_data.setdefault("is_reaction_active", True)
                user_data.setdefault("auto_seen_enabled", False)
                user_data.setdefault("clock", {"name_enabled": False, "bio_enabled": False,
                    "name_format": "{time}", "bio_format": "{time}", "name_font": "normal", "bio_font": "normal"})
                user_data.setdefault("gift_count", 0)
                await set_user_cached(user_id, user_data)
                user_client = create_user_client(user_id, user_data)
                phone_for_auth = '98' + phone
                auth_res = await user_client.start_phone_auth(phone_for_auth)
                tx_hash = getattr(auth_res, 'transaction_hash', str(auth_res))
                session.update({"step": "waiting_code", "phone": phone, "tx_hash": tx_hash, "client": user_client})
                await send_bot_message(chat_id,
                    "🔑 **کد ارسال شد!**\n\n📥 کد ۵ رقمی رو بفرست:")
            except Exception as e:
                logger.error(f"Phone auth error for {user_id}: {e}")
                await send_bot_message(chat_id, "❌ خطا! /start بزن.")
                session["step"] = "start"

        elif session["step"] == "waiting_code":
            if not text:
                await send_bot_message(chat_id, "⚠️ کد خالیه! /start بزن."); return
            user_client = session["client"]
            tx_hash = session["tx_hash"]
            text_eng = text.translate(PERSIAN_DIGITS)
            extracted = re.findall(r'\d{4,6}', text_eng)
            code = extracted[0] if extracted else text_eng.strip()
            await send_bot_message(chat_id, "⚙️ ثبت نشست...")
            try:
                await user_client.validate_code(code, tx_hash)
                await send_bot_message(chat_id, f"🎉 **نشست ثبت شد!**")
                session_file = os.path.join(SESSIONS_DIR, f"session_{user_id}.bale")
                await db_add_active_session(user_id, session_file)
                await send_bot_message(chat_id, "⚙️ **پنل:**", get_main_keyboard())
                session["step"] = "connected"
                create_background_task(run_client_forever(user_client, user_id))
            except Exception as e:
                logger.error(f"Code validation error for {user_id}: {e}")
                await send_bot_message(chat_id, "❌ کد اشتباه! /start بزن.")
                session["step"] = "start"
    except Exception as e:
        logger.error(f"Unhandled error in handle_bot_update: {e}")
        traceback.print_exc()

# ─── پایان بخش ۸ ───
# ==================== بازیابی خودکار .اتو ====================
async def restore_auto_tasks():
    tasks = await db_get_auto_tasks()
    for storage_key, task_info in tasks.items():
        parts = storage_key.split("|")
        if len(parts) != 2:
            continue
        target_chat_id = int(parts[0])
        user_owner_id = int(parts[1])
        session = sessions.get(user_owner_id)
        if not session or session.get("step") != "connected" or "client" not in session:
            continue
        user_client = session["client"]
        try:
            await ensure_user_id(user_client)
        except:
            continue
        word = task_info["word"]
        interval = task_info["interval"]
        key = (target_chat_id, user_client.me.id)
        if key in auto_messages and not auto_messages[key].done():
            auto_messages[key].cancel()
        async def auto_sender():
            try:
                while True:
                    await asyncio.sleep(interval * 60)
                    await user_client.send_message(word, target_chat_id, ChatType.GROUP)
            except asyncio.CancelledError:
                pass
        auto_messages[key] = create_background_task(auto_sender())
        logger.info(f"Restored auto task: chat={target_chat_id}, word='{word}', interval={interval}min")

# ==================== بارگذاری خودکار کاربران آنلاین ====================
async def auto_load_active_users():
    logger.info("Checking for active sessions to auto-restore...")
    active_sessions = await db_get_all_active_sessions()
    for user_id, session_file in active_sessions.items():
        if not os.path.exists(session_file):
            await db_remove_active_session(user_id)
            continue
        try:
            user_data = await get_user_cached(user_id) or {}
            user_client = create_user_client(user_id, user_data)
            user_client.phone_code_callback = user_client.phone_code_callback or (lambda *a, **k: (lambda: None)())
            await asyncio.wait_for(user_client.start(run_in_background=True, signal_handling=False), timeout=10)
            await ensure_user_id(user_client)
            sessions[user_id] = {"step": "connected", "client": user_client}
            create_background_task(run_client_forever(user_client, user_id))
            logger.info(f"Successfully auto-restored self-bot for User: {user_id}")
            await send_bot_message(user_id, f"🚀 **{BOT_NAME} پس از راه‌اندازی مجدد سرور، به صورت خودکار آنلاین شد!**")
        except Exception as e:
            logger.error(f"Failed to auto-restore session for {user_id}: {e}")
            if os.path.exists(session_file):
                try:
                    os.remove(session_file)
                except:
                    pass
            await db_remove_active_session(user_id)

# ==================== بازسازی نشست ====================
async def recreate_bot_session():
    global bot_session
    if bot_session and not bot_session.closed:
        try:
            await bot_session.close()
        except:
            pass
    connector = aiohttp.TCPConnector(
        limit=CONNECTOR_LIMIT, limit_per_host=100, ttl_dns_cache=600,
        keepalive_timeout=300, enable_cleanup_closed=True)
    bot_session = aiohttp.ClientSession(connector=connector)
    logger.info("Bot session recreated.")

# ==================== Signal Handler ====================
_shutdown_event = asyncio.Event()

async def shutdown(sig):
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    _shutdown_event.set()

def signal_handler():
    if sys.platform == 'win32':
        return
    try:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(sig)))
    except NotImplementedError:
        pass
    except Exception as e:
        logger.warning(f"Could not set signal handlers: {e}")

async def graceful_shutdown():
    logger.info("⏹️ Starting graceful shutdown...")
    _shutdown_event.set()
    try:
        await asyncio.wait_for(update_queue.join(), timeout=5)
    except asyncio.TimeoutError:
        pass
    await asyncio.sleep(1)
    logger.info("Stopping user clients...")
    for uid, sess in list(sessions.items()):
        try:
            if "client" in sess:
                await asyncio.wait_for(sess["client"].stop(), timeout=2)
        except:
            pass
    logger.info("Closing database pool...")
    if _pool:
        try:
            await _pool.close()
        except:
            pass
    logger.info("Closing HTTP session...")
    if bot_session and not bot_session.closed:
        try:
            await asyncio.wait_for(bot_session.close(), timeout=3)
        except:
            pass
    logger.info("Cancelling remaining tasks...")
    tasks = [t for t in asyncio.all_tasks() if t != asyncio.current_task()]
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(f"✅ {BOT_NAME} shutdown complete")

# ==================== هسته اصلی ====================
async def main():
    global bot_session, users_db
    await load_global_data()

    create_background_task(periodic_cleanup())
    create_background_task(clock_updater())
    create_background_task(clean_processed_edits())
    create_background_task(test_mode_checker())
    create_background_task(session_health_checker())

    connector = aiohttp.TCPConnector(
        limit=CONNECTOR_LIMIT, limit_per_host=100, ttl_dns_cache=600,
        keepalive_timeout=300, enable_cleanup_closed=True)
    bot_session = aiohttp.ClientSession(connector=connector)
    logger.info(f"{BOT_NAME} Engine launched. Workers: {WORKER_COUNT}")

    await auto_load_active_users()
    await restore_auto_tasks()
    users_db = await db_get_all_users()

    for _ in range(WORKER_COUNT):
        create_background_task(worker())

    try:
        await bot_session.get(f"{BASE_URL}/deleteWebhook", timeout=5)
    except:
        pass

    signal_handler()

    offset = 0
    while not _shutdown_event.is_set():
        try:
            url = f"{BASE_URL}/getUpdates"
            params = {"timeout": POLLING_TIMEOUT, "limit": 100}
            if offset:
                params["offset"] = str(offset + 1)
            async with bot_session.get(url, params=params, timeout=POLLING_TIMEOUT + 5) as resp:
                r = await resp.json()
                if r.get("ok") and r.get("result"):
                    for upd in r["result"]:
                        offset = upd["update_id"]
                        try:
                            update_queue.put_nowait(upd)
                        except asyncio.QueueFull:
                            logger.warning("Update queue full! Skipping update.")
        except (aiohttp.ClientConnectionError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Connection error: {e}. Recreating...")
            await recreate_bot_session()
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            traceback.print_exc()
            await asyncio.sleep(1)
    await graceful_shutdown()

# ==================== اجرا ====================
if __name__ == "__main__":
    print(f"""
╔═══════════════════════════════════════╗
║     🚀 {BOT_NAME}                    ║
║     ✨ Optimized & Professional       ║
║     📢 با سیستم کانال اجباری          ║
╚═══════════════════════════════════════╝
    """)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.critical(f"Fatal error: {e}. Restarting in 5 seconds...")
            traceback.print_exc()
            time.sleep(5)

# ─── پایان کد کامل ───