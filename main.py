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

# ==================== تنظیمات ====================
BOT_NAME = os.getenv("BOT_NAME", "هایپرسین سلف")
BOT_USERNAME = os.getenv("BOT_USERNAME", "YourBotUsername")
BOT_TOKEN = os.getenv("BOT_TOKEN", "216295883:iIMb6WoZgxE46qUCGrmqPDhjMpIkk23TmtU")
OWNER_ID = int(os.getenv("OWNER_ID", "1530477937"))
SUPPORT_ID = os.getenv("SUPPORT_ID", "@YourSupport")

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

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "bot_data.db")
SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

BASE_URL = f"https://tapi.bale.ai/bot{BOT_TOKEN}"

CONCURRENT_GIFT = 5
CONCURRENT_SPAM = 5
CONNECTOR_LIMIT = 300
POLLING_TIMEOUT = 30
UPDATE_QUEUE_SIZE = 10000
WORKER_COUNT = 100
CACHE_SIZE = 200
USER_CACHE_TTL = 300

PERSIAN_DIGITS = str.maketrans('۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩', '01234567890123456789')

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
    await _run_query('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, data TEXT NOT NULL)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS auto_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        word TEXT NOT NULL, interval_minutes INTEGER NOT NULL, UNIQUE(chat_id, user_id))''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS group_settings (group_id INTEGER PRIMARY KEY, settings TEXT NOT NULL)''', commit=True)
    await _run_query('''CREATE TABLE IF NOT EXISTS active_sessions (
        user_id INTEGER PRIMARY KEY, session_file TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''', commit=True)
    await _run_query('''CREATE INDEX IF NOT EXISTS idx_users_user_id ON users(user_id)''', commit=True)
    logger.info("Database initialized.")

# ==================== توابع دیتابیس ====================
async def db_get_user(user_id: int):
    row = await _run_query("SELECT data FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    return json_loads(row[0]) if row else None

async def db_set_user(user_id: int, data: dict):
    await _run_query("INSERT OR REPLACE INTO users (user_id, data) VALUES (?, ?)", (user_id, json_dumps(data)), commit=True)

async def db_get_all_users():
    rows = await _run_query("SELECT user_id, data FROM users", fetchall=True)
    return {row[0]: json_loads(row[1]) for row in rows}

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

async def db_add_active_session(user_id: int, session_file: str):
    await _run_query("INSERT OR REPLACE INTO active_sessions (user_id, session_file) VALUES (?, ?)", (user_id, session_file), commit=True)

async def db_remove_active_session(user_id: int):
    await _run_query("DELETE FROM active_sessions WHERE user_id = ?", (user_id,), commit=True)

async def db_get_all_active_sessions():
    rows = await _run_query("SELECT user_id, session_file FROM active_sessions", fetchall=True)
    return {row[0]: row[1] for row in rows}

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

# ==================== متغیرها ====================
sessions = {}
bot_session = None
processed_edits = set()
auto_messages = {}
emoji_tasks = {}
warning_messages = defaultdict(lambda: deque(maxlen=10))
ad_reply_timestamps = defaultdict(lambda: deque(maxlen=5))
user_messages_cache = defaultdict(lambda: deque(maxlen=100))
gift_semaphore = asyncio.Semaphore(CONCURRENT_GIFT)
auto_seen_last_time = {}

BACKGROUND_TASKS = set()

def create_background_task(coro):
    task = asyncio.create_task(coro)
    BACKGROUND_TASKS.add(task)
    task.add_done_callback(BACKGROUND_TASKS.discard)
    return task

# ==================== کیبوردها ====================
def get_main_keyboard():
    return {"inline_keyboard": [
        [{"text": "📊 وضعیت سلف", "callback_data": "status_self"}],
        [{"text": "🚀 فعال‌سازی سلف", "callback_data": "activate_self"}],
        [{"text": "📖 راهنما", "callback_data": "help_self"}],
        [{"text": "📞 پشتیبانی", "url": f"https://ble.ir/{SUPPORT_ID.replace('@', '')}"}]
    ]}

def get_back_keyboard():
    return {"inline_keyboard": [[{"text": "🔙 بازگشت", "callback_data": "back_to_main"}]]}

def get_settings_markup(is_bold, is_blue, is_italic, is_gift, is_reaction):
    status = lambda obj: "🟢 روشن" if obj else "🔴 خاموش"
    return {"inline_keyboard": [
        [{"text": f"🔹 ضخیم (Bold): {status(is_bold)}", "callback_data": "toggle_bold"}],
        [{"text": f"✨ کج (Italic): {status(is_italic)}", "callback_data": "toggle_italic"}],
        [{"text": f"🔵 لینک آبی: {status(is_blue)}", "callback_data": "toggle_blue"}],
        [{"text": f"🎁 هدیه قاپ: {status(is_gift)}", "callback_data": "toggle_gift"}],
        [{"text": f"👍 ری‌اکشن: {status(is_reaction)}", "callback_data": "toggle_reaction"}],
        [{"text": "🕘 تنظیمات ساعت", "callback_data": "open_clock_settings"}]
    ]}

def get_clock_settings_markup(user_id, user_data):
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
        [{"text": "🔙 بازگشت", "callback_data": "clock_back"}]
    ]}

# ==================== نرمال‌سازی شماره ====================
def normalize_phone(phone_str):
    if not phone_str: return ""
    phone = phone_str.translate(PERSIAN_DIGITS)
    phone = re.sub(r'\D', '', phone)
    if not phone: return ""
    if phone.startswith('0'): phone = phone[1:]
    if phone.startswith('98') and len(phone) == 12: phone = phone[2:]
    if len(phone) == 10 and phone.startswith('9'): return phone
    return phone

FANCY_LETTERS = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "αв¢∂єfgнιנкℓмησρqяѕтυνωχуչABCDEFGHIJKLMNOPQRSTUVWXYZ")
FANCY_DIGITS = {'0': '𝟎', '1': '𝟏', '2': '𝟐', '3': '𝟑', '4': '𝟒', '5': '𝟓', '6': '𝟔', '7': '𝟕', '8': '𝟖', '9': '𝟗'}

def convert_to_fancy_font(text):
    res = text.translate(FANCY_LETTERS)
    for d, fancy_d in FANCY_DIGITS.items():
        res = res.replace(d, fancy_d)
    return res

def get_clock_time(font_type="normal"):
    now = datetime.now(ZoneInfo("Asia/Tehran")).strftime("%H:%M")
    if font_type == "fancy":
        return convert_to_fancy_font(now)
    return now

def contains_link(text):
    return bool(re.compile(r'https?://\S+|www\.\S+').search(text))

async def get_group_setting(group_id, key, default=None):
    settings = await db_get_group_settings(group_id)
    return settings.get(key, default)

async def set_group_setting(group_id, key, value):
    settings = await db_get_group_settings(group_id)
    settings[key] = value
    await db_set_group_settings(group_id, settings)

async def get_group_warnings(group_id):
    return await get_group_setting(group_id, "warnings", {})

async def add_warning(group_id, user_id):
    warnings = await get_group_warnings(group_id)
    warnings[str(user_id)] = warnings.get(str(user_id), 0) + 1
    await set_group_setting(group_id, "warnings", warnings)

async def clear_warnings(group_id, user_id):
    warnings = await get_group_warnings(group_id)
    warnings.pop(str(user_id), None)
    await set_group_setting(group_id, "warnings", warnings)

# ==================== ارسال پیام ====================
async def send_bot_message(chat_id, text, reply_markup=None, retries=2):
    url = f"{BASE_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup: payload["reply_markup"] = reply_markup
    for attempt in range(retries):
        try:
            await asyncio.wait_for(bot_session.post(url, json=payload), timeout=5)
            return
        except asyncio.TimeoutError:
            if attempt < retries - 1: await asyncio.sleep(0.5 * (attempt + 1))
        except Exception as e:
            if attempt < retries - 1: await asyncio.sleep(0.3)

async def edit_bot_message(chat_id, message_id, text, reply_markup=None):
    url = f"{BASE_URL}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup: payload["reply_markup"] = reply_markup
    try:
        await bot_session.post(url, json=payload)
    except: pass

# ==================== مدیریت گروه ====================
async def kick_user(chat_id, user_id, client):
    try:
        await client.kick_user(chat_id, user_id)
    except: pass

async def handle_link_violation(message, user_client, chat_id, sender_id):
    try:
        await user_client.delete_message(message.message_id, message.date, chat_id, ChatType.GROUP)
    except: pass
    await add_warning(chat_id, sender_id)
    warnings = await get_group_warnings(chat_id)
    warns = warnings.get(str(sender_id), 0)
    if warns >= 3:
        await kick_user(chat_id, sender_id, user_client)
        await clear_warnings(chat_id, sender_id)

async def apply_group_rules(message, user_client):
    chat_id = message.chat.id
    sender_id = message.sender_id
    text = message.text or ""
    if sender_id == user_client.me.id: return
    if contains_link(text):
        await handle_link_violation(message, user_client, chat_id, sender_id)

# ==================== بروزرسانی ساعت ====================
async def update_user_profile(user_client, user_id):
    try:
        if not hasattr(user_client, "me") or user_client.me is None:
            await ensure_user_id(user_client)
    except: return
    user_data = await get_user_cached(user_id) or {}
    settings = user_data.get("clock", {})
    name_enabled = settings.get("name_enabled", False)
    bio_enabled = settings.get("bio_enabled", False)
    if not name_enabled and not bio_enabled: return
    try:
        current = await asyncio.wait_for(user_client.get_me(), timeout=5)
    except: return
    if name_enabled:
        name_format = settings.get("name_format", "{time}")
        name_font = settings.get("name_font", "normal")
        new_name = name_format.replace("{time}", get_clock_time(name_font))
        try:
            await asyncio.wait_for(user_client.edit_name(new_name), timeout=5)
        except: pass
    if bio_enabled:
        bio_format = settings.get("bio_format", "{time}")
        bio_font = settings.get("bio_font", "normal")
        new_bio = bio_format.replace("{time}", get_clock_time(bio_font))
        try:
            await asyncio.wait_for(user_client.edit_about(new_bio), timeout=5)
        except: pass

async def clock_updater():
    try:
        while True:
            await asyncio.sleep(60)
            for uid, session in list(sessions.items()):
                if session.get("step") == "connected" and "client" in session:
                    try:
                        await update_user_profile(session["client"], uid)
                    except: pass
    except asyncio.CancelledError:
        raise

# ==================== پاک‌سازی ====================
async def periodic_cleanup():
    try:
        while True:
            await asyncio.sleep(1800)
            warning_messages.clear()
            ad_reply_timestamps.clear()
            auto_seen_last_time.clear()
            processed_edits.clear()
            now = time.time()
            for uid in list(USER_CACHE.keys()):
                if now - USER_CACHE[uid][1] > USER_CACHE_TTL:
                    del USER_CACHE[uid]
    except asyncio.CancelledError:
        raise

async def session_health_checker():
    try:
        while True:
            await asyncio.sleep(3600)
            for uid, session in list(sessions.items()):
                if session.get("step") == "connected" and "client" in session:
                    try:
                        await asyncio.wait_for(session["client"].get_me(), timeout=3)
                    except:
                        del sessions[uid]
                        await db_remove_active_session(uid)
    except asyncio.CancelledError:
        raise

async def ensure_user_id(client):
    try:
        if hasattr(client, "me") and client.me is not None:
            return client.me.id
        me = await asyncio.wait_for(client.get_me(), timeout=5.0)
        client.me = me
        return me.id
    except Exception as e:
        raise

# ==================== EMOJI ====================
EMOJI_CATEGORIES = {
    "گربه": ["😺", "😸", "😹", "😻", "😼", "😽", "🙀", "😿", "😾", "🐱", "🐈", "🐈‍⬛"],
    "گیاه": ["🌱", "🌿", "☘️", "🍀", "🪴", "🌵", "🌲", "🌳", "🌴", "🌾", "🌺", "🌸", "🌼", "🏵️", "🌻"],
    "خنده": ["😀", "😃", "😄", "😁", "😆", "😅", "😂", "🤣", "🥲", "😊", "😍", "🥰", "😘", "😜", "🤪"],
    "قلب": ["❤️", "🧡", "💛", "💚", "💙", "💜", "🖤", "🤍", "🤎", "💔", "❤️‍🔥", "💖", "💗", "💓", "💕"],
    "غذا": ["🍎", "🍕", "🍔", "🍟", "🍩", "🍪", "🍫", "🍬", "🍭", "🥨", "🥩", "🍣", "🍜", "🍦", "🍰"],
    "فحش": ["😡", "👿", "💀", "🤬", "💢", "🗯️", "🖕", "💩", "👎", "🤮", "😤", "😠", "👊", "✊", "🔞", "💣", "🔪", "😈"],
}

# ==================== ساخت کلاینت ====================
BALE_DEVICE_MODELS = [
    "Samsung Galaxy A16", "Samsung Galaxy A17", "Samsung Galaxy A25",
    "Samsung Galaxy A35", "Samsung Galaxy A55", "Samsung Galaxy S24 Ultra",
    "Redmi Note 13 Pro", "Redmi Note 14 Pro", "POCO X6 Pro", "POCO X7 Pro",
    "Xiaomi 14T Pro", "iPhone 15 Pro", "iPhone 16 Pro", "Nothing Phone (2a)",
]

def _make_bale_client(user_dp, session_file):
    kwargs = {"session_file": session_file}
    try:
        params = inspect.signature(Client).parameters
        for name in ("device_title", "deviceTitle", "device_name"):
            if name in params:
                kwargs[name] = random.choice(BALE_DEVICE_MODELS)
                break
    except: pass
    return Client(user_dp, **kwargs)

def create_user_client(user_id, settings, code_callback=None):
    user_dp = Dispatcher()
    session_file = os.path.join(SESSIONS_DIR, f"session_{user_id}.bale")
    user_client = _make_bale_client(user_dp, session_file)

    if code_callback is None:
        async def default_code_callback(phone, code_type, transaction_hash):
            return None
        user_client.phone_code_callback = default_code_callback
    else:
        user_client.phone_code_callback = code_callback

    if hasattr(user_client, '_ping'):
        original_ping_inst = user_client._ping
        async def patched_ping_inst():
            try:
                await asyncio.wait_for(original_ping_inst(), timeout=5.0)
            except: pass
        user_client._ping = patched_ping_inst

    user_client.is_bold_active = settings.get("is_bold_active", True)
    user_client.is_italic_active = settings.get("is_italic_active", False)
    user_client.is_blue_active = settings.get("is_blue_active", False)
    user_client.is_gift_active = settings.get("is_gift_active", True)
    user_client.is_reaction_active = settings.get("is_reaction_active", True)

    async def animate_emojis(message, emoji_list, delay=0.8):
        try:
            for emoji in emoji_list:
                await message.edit_text(emoji)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise
        except: pass

    @user_dp.message(IsGift())
    async def gift_handler(message):
        if not getattr(user_client, "is_gift_active", True): return
        token_val = None
        try:
            if hasattr(message.content, "gift") and hasattr(message.content.gift, "token"):
                token_val = message.content.gift.token.value
        except: token_val = None
        if not token_val: return
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
        except: return
        create_background_task(_process_gift_result(message, result))

    async def _process_gift_result(message, result):
        try:
            def get_val(obj, *attrs, default=0):
                for attr in attrs:
                    if hasattr(obj, attr):
                        val = getattr(obj, attr)
                        if val is not None: return val
                    elif isinstance(obj, dict) and attr in obj:
                        val = obj[attr]
                        if val is not None: return val
                return default
            win_amount = get_val(result, 'amount', 'win_amount', 'winAmount')
            if win_amount == 0: return
            user_id = user_client.me.id
            user_data = await get_user_cached(user_id) or {}
            user_data['gift_count'] = user_data.get('gift_count', 0) + 1
            await set_user_cached(user_id, user_data)
            if getattr(user_client, "is_reaction_active", True):
                try:
                    await message.react("👍")
                except: pass
        except: pass

    @user_dp.message()
    async def user_text_handler(message):
        try:
            await _user_text_handler_impl(message)
        except: pass

    async def _user_text_handler_impl(message):
        try:
            await ensure_user_id(user_client)
        except: return
        chat = message.chat
        chat_id = chat.id
        sender_id = message.sender_id
        can_edit = hasattr(message, "edit_text")
        my_id = user_client.me.id

        if sender_id == my_id:
            if not message.text: return
            msg_text = message.text.strip()
            key = (chat_id, my_id)
            user_messages_cache[key].append(message.message_id)

            if msg_text.startswith(".پاکسازی "):
                parts = msg_text.split()
                if len(parts) != 2 or not parts[1].isdigit():
                    if can_edit: await message.edit_text("⚠️ فرمت: `.پاکسازی [تعداد]`")
                    return
                count = int(parts[1])
                if count <= 0 or count > 200:
                    if can_edit: await message.edit_text("⚠️ تعداد بین 1 تا 200!")
                    return
                cached = user_messages_cache.get(key, [])
                if not cached:
                    if can_edit: await message.edit_text("❌ پیامی نیست!")
                    return
                to_delete = list(cached)[-count:] if count <= len(cached) else list(cached)[:]
                for _ in to_delete: cached.remove(_)
                deleted = 0
                for msg_id in to_delete:
                    try:
                        await user_client.delete_message(msg_id, 0, chat_id, ChatType.GROUP)
                        deleted += 1
                    except: pass
                    await asyncio.sleep(0.05)
                await message.edit_text(f"✅ **{deleted} پیام حذف شد!**")
                return

            if msg_text in [".پاکت بازکن روشن", ".پاکت ان", ".پاکت بازکن"]:
                user_client.is_gift_active = True
                user_data = await get_user_cached(my_id) or {}
                user_data["is_gift_active"] = True
                await set_user_cached(my_id, user_data)
                if can_edit: await message.edit_text("✅ **پاکت خودکار فعال!**")
                return

            if msg_text in [".پاکت بازکن خاموش", ".پاکت اف", ".پاکت باز نکن"]:
                user_client.is_gift_active = False
                user_data = await get_user_cached(my_id) or {}
                user_data["is_gift_active"] = False
                await set_user_cached(my_id, user_data)
                if can_edit: await message.edit_text("❌ **پاکت خودکار خاموش!**")
                return

            if msg_text == ".آمار":
                user_data = await get_user_cached(my_id) or {}
                gift_count = user_data.get('gift_count', 0)
                session_status = "🟢 فعال" if my_id in sessions and sessions[my_id].get('step') == 'connected' else "🔴 غیرفعال"
                auto_seen = "🟢" if user_data.get("auto_seen_enabled", False) else "🔴"
                stats = (
                    f"📊 **آمار {BOT_NAME}**\n\n"
                    f"🔹 سشن: {session_status}\n"
                    f"🎁 پاکت‌ها: `{gift_count}`\n"
                    f"👁️ سین خودکار: {auto_seen}\n"
                )
                if can_edit: await message.edit_text(stats)
                return

            if msg_text.startswith(".تبلیغات ") and not msg_text.startswith(".تبلیغات خاموش"):
                banner = msg_text[9:].strip()
                if not banner:
                    if can_edit: await message.edit_text("⚠️ متن بنر رو بده!")
                    return
                user_data = await get_user_cached(my_id) or {}
                if "ad_settings" not in user_data: user_data["ad_settings"] = {}
                user_data["ad_settings"][str(chat_id)] = {"enabled": True, "banner": banner}
                await set_user_cached(my_id, user_data)
                ad_reply_timestamps.clear()
                if can_edit: await message.edit_text(f"✅ **تبلیغات فعال!**\n`{banner}`")
                return

            if msg_text == ".تبلیغات خاموش":
                user_data = await get_user_cached(my_id) or {}
                if "ad_settings" in user_data and str(chat_id) in user_data["ad_settings"]:
                    user_data["ad_settings"][str(chat_id)]["enabled"] = False
                    await set_user_cached(my_id, user_data)
                    if can_edit: await message.edit_text("❌ **تبلیغات خاموش!**")
                return

            if msg_text == ".اتو خاموش":
                key = (chat_id, my_id)
                if key in auto_messages and not auto_messages[key].done():
                    auto_messages[key].cancel()
                    del auto_messages[key]
                    await db_remove_auto_task(chat_id, my_id)
                    if can_edit: await message.edit_text("🛑 **اتو خاموش شد!**")
                return

            if msg_text.startswith(".اتو "):
                rest = msg_text[len(".اتو "):].strip()
                parts = rest.split(maxsplit=1)
                if len(parts) != 2:
                    if can_edit: await message.edit_text("⚠️ فرمت: `.اتو [دقیقه] [متن]`")
                    return
                time_part = parts[0]
                if not time_part.isdigit():
                    if can_edit: await message.edit_text("⚠️ دقیقه باید عدد باشه!")
                    return
                minutes = int(time_part)
                if minutes < 1 or minutes > 60:
                    if can_edit: await message.edit_text("⚠️ 1 تا 60 دقیقه!")
                    return
                text_to_send = parts[1]
                if not text_to_send:
                    if can_edit: await message.edit_text("⚠️ متن خالیه!")
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
                if can_edit: await message.edit_text(f"✅ **اتو فعال: هر {minutes} دقیقه**")
                return

            if msg_text.startswith(".پاسخ "):
                parts = msg_text.split(maxsplit=1)
                if len(parts) < 2:
                    if can_edit: await message.edit_text("⚠️ فرمت: `.پاسخ [متن] [جواب]`")
                    return
                rest = parts[1].strip()
                if ' ' not in rest:
                    if can_edit: await message.edit_text("⚠️ فرمت: `.پاسخ [متن] [جواب]`")
                    return
                trigger, response = rest.split(' ', 1)
                trigger, response = trigger.strip(), response.strip()
                user_data = await get_user_cached(my_id) or {}
                if "auto_replies" not in user_data: user_data["auto_replies"] = {}
                if response == "خاموش":
                    if trigger.lower() in user_data["auto_replies"]:
                        del user_data["auto_replies"][trigger.lower()]
                        await set_user_cached(my_id, user_data)
                        if can_edit: await message.edit_text(f"✅ **حذف شد!**")
                else:
                    user_data["auto_replies"][trigger.lower()] = response
                    await set_user_cached(my_id, user_data)
                    if can_edit: await message.edit_text(f"✅ **پاسخ فعال!**")
                return

            if msg_text.lower().startswith(".ایموجی "):
                parts = msg_text.split(maxsplit=1)
                if len(parts) < 2:
                    if can_edit: await message.edit_text("⚠️ فرمت: `.ایموجی [نوع]`")
                    return
                category_name = parts[1].strip()
                matched = None
                for k in EMOJI_CATEGORIES.keys():
                    if k == category_name:
                        matched = k
                        break
                if not matched:
                    available = "، ".join(EMOJI_CATEGORIES.keys())
                    if can_edit: await message.edit_text(f"❌ دسته موجود نیست!\n{available}")
                    return
                if chat_id in emoji_tasks and not emoji_tasks[chat_id].done():
                    emoji_tasks[chat_id].cancel()
                emoji_tasks[chat_id] = create_background_task(animate_emojis(message, EMOJI_CATEGORIES[matched], 0.8))
                return

            if msg_text.lower() == ".اطلاعات":
                if not hasattr(message, 'replied_to') or not message.replied_to:
                    if can_edit: await message.edit_text("⚠️ روی یه پیام ریپلای کن!")
                    return
                target_user_id = getattr(message.replied_to, 'sender_id', None)
                if target_user_id is None: return
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
                            info_text = (f"👤 **اطلاعات**\n\n🆔 `{user_id}`\n📝 {full_name}\n"
                                f"🔖 {f'@{username}' if username else 'ندارد'}\n📄 {bio}")
                            if can_edit: await message.edit_text(info_text)
                except: pass
                return

            if msg_text.lower() == ".clock":
                user_data = await get_user_cached(my_id) or {}
                await message.reply("🕘 **پنل ساعت**", reply_markup=get_clock_settings_markup(my_id, user_data))
                return

            if msg_text.lower() == ".nameclock on":
                user_data = await get_user_cached(my_id) or {}
                if "clock" not in user_data: user_data["clock"] = {}
                user_data["clock"]["name_enabled"] = True
                await set_user_cached(my_id, user_data)
                await message.reply("✅ **ساعت در نام فعال!**")
                await update_user_profile(user_client, my_id)
                return

            if msg_text.lower() == ".nameclock off":
                user_data = await get_user_cached(my_id) or {}
                if "clock" in user_data:
                    user_data["clock"]["name_enabled"] = False
                    await set_user_cached(my_id, user_data)
                    await message.reply("❌ **خاموش شد!**")
                return

            if msg_text.lower() == ".bioclock on":
                user_data = await get_user_cached(my_id) or {}
                if "clock" not in user_data: user_data["clock"] = {}
                user_data["clock"]["bio_enabled"] = True
                await set_user_cached(my_id, user_data)
                await message.reply("✅ **ساعت در بیو فعال!**")
                await update_user_profile(user_client, my_id)
                return

            if msg_text.lower() == ".bioclock off":
                user_data = await get_user_cached(my_id) or {}
                if "clock" in user_data:
                    user_data["clock"]["bio_enabled"] = False
                    await set_user_cached(my_id, user_data)
                    await message.reply("❌ **خاموش شد!**")
                return

            if msg_text.lower() in [".کیف", ".wallet"]:
                try:
                    wallet_info = await user_client.get_wallet()
                    balance = wallet_info.wallet.balance
                    if can_edit: await message.edit_text(f"💰 **کیف پول**\n\nموجودی: `{balance}` ریال")
                except:
                    if can_edit: await message.edit_text("❌ خطا!")
                return

            GIFT_PATTERN = re.compile(r"\.(ارسال پاکت|sendgift)\s+(\d+)\s+(.*?)\s+(\d+)$")
            gift_match = GIFT_PATTERN.match(msg_text)
            if gift_match:
                amount = int(gift_match.group(2))
                text_content = gift_match.group(3).strip()
                count = int(gift_match.group(4))
                if count <= 0 or count > 50:
                    if can_edit: await message.edit_text("⚠️ تعداد 1-50!")
                    return
                if amount <= 0:
                    if can_edit: await message.edit_text("⚠️ مبلغ > 0!")
                    return
                status_msg = await message.reply(f"⏳ در حال ارسال {count} پاکت...")
                sem = asyncio.Semaphore(CONCURRENT_GIFT)
                async def send_one():
                    async with sem:
                        try:
                            await user_client.send_gift(chat_id=message.chat.id, chat_type=message.chat.type,
                                amount=amount, message=text_content, gift_count=1)
                        except Exception as e:
                            raise
                tasks = [send_one() for _ in range(count)]
                try:
                    await asyncio.gather(*tasks)
                    await status_msg.edit_text(f"✅ **{count} پاکت ارسال شد!**")
                except Exception as e:
                    await status_msg.edit_text(f"❌ خطا: {str(e)[:100]}")
                return

            SPAM_PATTERN = re.compile(r"\.(اسپم|spam)\s+(.+?)\s+(\d+)$", re.IGNORECASE)
            spam_match = SPAM_PATTERN.match(msg_text)
            if spam_match:
                spam_text = spam_match.group(2).strip()
                count = int(spam_match.group(3))
                if count < 1 or count > 50:
                    if can_edit: await message.edit_text("⚠️ تعداد 1-50!")
                    return
                if not spam_text:
                    if can_edit: await message.edit_text("⚠️ متن خالیه!")
                    return
                status_msg = await message.reply(f"🔄 **ارسال {count} اسپم...**")
                sem = asyncio.Semaphore(CONCURRENT_SPAM)
                sent = 0
                async def send_one():
                    nonlocal sent
                    async with sem:
                        try:
                            await message.answer(spam_text)
                            sent += 1
                        except: pass
                tasks = [send_one() for _ in range(count)]
                await asyncio.gather(*tasks)
                await status_msg.edit_text(f"✅ **{sent} پیام ارسال شد!**")
                return

            if msg_text.startswith(".نصب ربات"):
                if chat.type != ChatType.GROUP:
                    await message.reply("❌ فقط در گروه!")
                    return
                await set_group_setting(chat_id, "installed", True)
                await set_group_setting(chat_id, "warnings", {})
                await message.edit_text("✅ **نصب شد!**")
                return

            if msg_text.startswith(".حذف ربات"):
                if chat.type != ChatType.GROUP:
                    await message.reply("❌ فقط در گروه!")
                    return
                await set_group_setting(chat_id, "installed", False)
                await message.edit_text("❌ **حذف شد!**")
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
                    if can_edit: await message.edit_text("⚠️ ریپلای یا آیدی!")
                    return
                await kick_user(chat_id, target_user_id, user_client)
                if can_edit: await message.edit_text(f"🚫 بن شد!")
                return

            if msg_text.lower() == ".سین خودکار":
                user_data = await get_user_cached(my_id) or {}
                user_data["auto_seen_enabled"] = True
                await set_user_cached(my_id, user_data)
                if can_edit: await message.edit_text("✅ **سین خودکار فعال!**")
                return

            if msg_text.lower() == ".سین خودکار خاموش":
                user_data = await get_user_cached(my_id) or {}
                user_data["auto_seen_enabled"] = False
                await set_user_cached(my_id, user_data)
                if can_edit: await message.edit_text("❌ **خاموش شد!**")
                return

            if msg_text.lower() in [".راهنما", ".help"]:
                help_text = (
                    f"🌟 **راهنمای {BOT_NAME}** 🌟\n\n"
                    f"📞 پشتیبانی: {SUPPORT_ID}\n\n"
                    f"📌 **دستورات:**\n\n"
                    f"🔹 `.ping` - تست سرعت\n"
                    f"🔹 `.id` - آیدی چت\n"
                    f"🔹 `.font [متن]` - فونت فانتزی\n"
                    f"🔹 `.ایموجی [نوع]` - ایموجی\n"
                    f"🔹 `.clock` - تنظیم ساعت\n"
                    f"🔹 `.nameclock on/off` - ساعت در نام\n"
                    f"🔹 `.bioclock on/off` - ساعت در بیو\n"
                    f"🔹 `.کیف` - کیف پول\n"
                    f"🔹 `.ارسال پاکت [مبلغ] [متن] [تعداد]`\n"
                    f"🔹 `.اسپم [متن] [تعداد]`\n"
                    f"🔹 `.اتو [دقیقه] [متن]`\n"
                    f"🔹 `.اتو خاموش`\n"
                    f"🔹 `.پاسخ [متن] [جواب]`\n"
                    f"🔹 `.اطلاعات` (ریپلای)\n"
                    f"🔹 `.پاکسازی [تعداد]`\n"
                    f"🔹 `.پاکت بازکن روشن/خاموش`\n"
                    f"🔹 `.آمار`\n"
                    f"🔹 `.تبلیغات [بنر]`\n"
                    f"🔹 `.تبلیغات خاموش`\n"
                    f"🔹 `.سین خودکار`\n"
                    f"🔹 `.سین خودکار خاموش`\n"
                    f"🔹 `.نصب ربات` / `.حذف ربات`\n"
                    f"🔹 `.بن` (ریپلای)"
                )
                keyboard = {"inline_keyboard": [[{"text": "🔙 بازگشت", "callback_data": "back_to_main"}]]}
                if can_edit: await message.edit_text(help_text, reply_markup=keyboard)
                return

            if msg_text.lower() in [".ping", ".پینگ"]:
                try:
                    start = time.perf_counter()
                    temp = await message.reply("⏳ ...")
                    elapsed = (time.perf_counter() - start) * 1000
                    await temp.edit_text(f"⏱ `{elapsed:.0f}ms`")
                except: pass
                return

            if msg_text.lower() in [".id", ".ایدی", ".آیدی"]:
                chat = message.chat
                lines = [f"🆔 **چت:** `{chat.id}`", f"👤 **شما:** `{my_id}`"]
                if hasattr(chat, 'title') and chat.title:
                    lines.append(f"📛 **عنوان:** {chat.title}")
                try: await message.reply("\n".join(lines))
                except: pass
                return

            if msg_text.lower().startswith((".font ", ".فونت ")):
                target_text = msg_text[6:]
                fancy_text = convert_to_fancy_font(target_text)
                try:
                    if can_edit: await message.edit_text(fancy_text)
                except: pass
                return

            is_bold = getattr(user_client, "is_bold_active", True)
            is_italic = getattr(user_client, "is_italic_active", False)
            is_blue = getattr(user_client, "is_blue_active", False)
            if not is_bold and not is_blue and not is_italic: return
            new_text = msg_text
            if is_italic: new_text = f"_{new_text}_"
            if is_bold: new_text = f"*{new_text}*"
            if is_blue: new_text = f"[{new_text}](uid:) "
            if new_text != msg_text and can_edit and sender_id == my_id:
                try:
                    processed_edits.add(message.message_id)
                    await asyncio.wait_for(message.edit_text(new_text), timeout=3)
                except:
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
                    try: await message.reply(user_auto_replies[trigger])
                    except: pass
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
                        except: pass

        if chat.type in (ChatType.GROUP, ChatType.CHANNEL):
            user_data = await get_user_cached(my_id) or {}
            if user_data.get("auto_seen_enabled", False):
                now = time.time()
                last = auto_seen_last_time.get(chat_id, 0)
                if now - last > 5:
                    try:
                        await user_client.seen_chat(chat_id, chat.type)
                        auto_seen_last_time[chat_id] = now
                    except: pass

    return user_client

# ==================== اجرای کلاینت ====================
async def run_client_forever(user_client, user_id):
    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        try:
            user_client.phone_code_callback = user_client.phone_code_callback or (lambda *a, **k: None)
            await user_client.start(run_in_background=False, signal_handling=False)
            await ensure_user_id(user_client)
            attempt = 0
            while True:
                await asyncio.sleep(1)
                if not hasattr(user_client, '_websocket') or user_client._websocket is None:
                    break
        except asyncio.CancelledError:
            try: await user_client.stop()
            except: pass
            break
        except Exception as e:
            error_msg = str(e).lower()
            if any(x in error_msg for x in ["cannot write to closing transport", "unknown wire type",
                "list index out of range", "unauthorized", "auth_key_unregistered",
                "invalid session", "session revoked", "user deactivated"]):
                if user_id in sessions: del sessions[user_id]
                session_file = os.path.join(SESSIONS_DIR, f"session_{user_id}.bale")
                if os.path.exists(session_file):
                    try: os.remove(session_file)
                    except: pass
                await db_remove_active_session(user_id)
                try:
                    await send_bot_message(user_id, "⚠️ نشست نامعتبر! /start بزن.")
                except: pass
                break
            elif any(x in error_msg for x in ["connection lost", "cannot connect to host", "broken pipe", "eof", "timeout"]):
                attempt += 1
                if attempt >= max_attempts:
                    if user_id in sessions: del sessions[user_id]
                    try: await user_client.stop()
                    except: pass
                    await db_remove_active_session(user_id)
                    break
                await asyncio.sleep(min(2 ** attempt, 30))
                settings = await get_user_cached(user_id) or {}
                new_client = create_user_client(user_id, settings)
                if user_id in sessions:
                    sessions[user_id]["client"] = new_client
                else:
                    sessions[user_id] = {"step": "connected", "client": new_client}
                user_client = new_client
                continue
            else:
                attempt += 1
                if attempt >= max_attempts:
                    if user_id in sessions: del sessions[user_id]
                    try: await user_client.stop()
                    except: pass
                    await db_remove_active_session(user_id)
                    break
                await asyncio.sleep(3)
                continue

# ==================== لاگین ====================
async def safe_handle_self_login(chat_id, phone, admin_states):
    try:
        await handle_self_login(chat_id, phone, admin_states)
    except Exception as e:
        try:
            await send_bot_message(chat_id, f"❌ خطا:\n`{str(e)[:300]}`")
        except: pass
        if chat_id in admin_states: del admin_states[chat_id]
        if chat_id in sessions:
            try:
                if "client" in sessions[chat_id]:
                    await sessions[chat_id]["client"].stop()
            except: pass
            del sessions[chat_id]

async def handle_self_login(chat_id, phone, admin_states):
    user_client = None
    try:
        phone_for_auth = '98' + phone if phone.startswith('9') else phone
        try:
            user_data = await get_user_cached(chat_id) or {}
        except: user_data = {}
        try:
            user_client = create_user_client(chat_id, user_data)
        except Exception as e:
            await send_bot_message(chat_id, f"❌ خطا: `{str(e)[:300]}`")
            if chat_id in admin_states: del admin_states[chat_id]
            return

        auth_method = None
        for method_name in ['start_phone_auth', 'send_code_request', 'send_code', 'auth_send_code']:
            if hasattr(user_client, method_name):
                auth_method = getattr(user_client, method_name)
                break

        if auth_method is None:
            await send_bot_message(chat_id, "❌ متد ارسال کد یافت نشد!")
            if chat_id in admin_states: del admin_states[chat_id]
            return

        try:
            auth_res = await asyncio.wait_for(auth_method(phone_for_auth), timeout=30.0)
        except asyncio.TimeoutError:
            await send_bot_message(chat_id, "❌ زمان تموم شد!")
            if chat_id in admin_states: del admin_states[chat_id]
            return
        except Exception as e:
            await send_bot_message(chat_id, f"❌ خطا: `{str(e)[:200]}`")
            if chat_id in admin_states: del admin_states[chat_id]
            if chat_id in sessions: del sessions[chat_id]
            return

        tx_hash = getattr(auth_res, 'transaction_hash', None) or str(auth_res)
        sessions[chat_id] = {"step": "waiting_code_for_self", "phone": phone, "tx_hash": tx_hash, "client": user_client}
        if chat_id not in admin_states: admin_states[chat_id] = {}
        admin_states[chat_id]["state"] = "waiting_code_for_self"

        await send_bot_message(chat_id,
            "🔑 **کد تأیید ارسال شد!**\n\n📥 کد ۵ رقمی رو بفرست:\nبرای انصراف /cancel")
    except Exception as e:
        try: await send_bot_message(chat_id, f"❌ خطا: `{str(e)[:300]}`")
        except: pass
        if chat_id in admin_states: del admin_states[chat_id]
        if chat_id in sessions:
            try:
                if user_client and "client" in sessions.get(chat_id, {}):
                    await sessions[chat_id]["client"].stop()
            except: pass
            del sessions[chat_id]

async def handle_self_code_validation(chat_id, code, admin_states):
    try:
        if chat_id not in sessions:
            await send_bot_message(chat_id, "❌ نشست منقضی! مجدد تلاش کن.")
            if chat_id in admin_states: del admin_states[chat_id]
            return
        user_client = sessions[chat_id]["client"]
        tx_hash = sessions[chat_id]["tx_hash"]
        code = code.strip().replace(" ", "").replace("-", "")
        if not code.isdigit() or len(code) < 4:
            await send_bot_message(chat_id, "❌ کد نامعتبر!")
            return
        try:
            await asyncio.wait_for(user_client.validate_code(code, tx_hash), timeout=30.0)
        except asyncio.TimeoutError:
            await send_bot_message(chat_id, "❌ زمان تموم شد!")
            if chat_id in sessions: del sessions[chat_id]
            if chat_id in admin_states: del admin_states[chat_id]
            return
        except Exception as e:
            error_msg = str(e)
            if "invalid" in error_msg.lower() or "wrong" in error_msg.lower():
                await send_bot_message(chat_id, "❌ کد اشتباه! مجدد بفرست:")
                if chat_id in admin_states:
                    admin_states[chat_id]["state"] = "waiting_code_for_self"
                return
            else:
                raise e

        session_file = os.path.join(SESSIONS_DIR, f"session_{chat_id}.bale")
        await db_add_active_session(chat_id, session_file)
        sessions[chat_id]["step"] = "connected"
        create_background_task(run_client_forever(user_client, chat_id))

        if chat_id in admin_states: del admin_states[chat_id]

        welcome_text = (f"✅ **سلف {BOT_NAME} فعال شد!**\n\n🎉 از تمام قابلیت‌ها استفاده کن.\n"
            f"📋 برای وضعیت، دکمه «وضعیت سلف» رو بزن.")

        await send_bot_message(chat_id, welcome_text, get_main_keyboard())
    except Exception as e:
        await send_bot_message(chat_id, f"❌ خطا: {str(e)[:200]}")
        if chat_id in admin_states:
            admin_states[chat_id]["state"] = "waiting_phone_for_self"
        if chat_id in sessions:
            try:
                if "client" in sessions[chat_id]:
                    await sessions[chat_id]["client"].stop()
            except: pass
            del sessions[chat_id]

# ==================== Worker ====================
update_queue = asyncio.Queue(maxsize=UPDATE_QUEUE_SIZE)

async def worker():
    while True:
        try:
            update = await asyncio.wait_for(update_queue.get(), timeout=60)
            try:
                await handle_bot_update(update)
            except Exception as e:
                logger.error(f"Handle error: {e}")
            finally:
                update_queue.task_done()
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
        except Exception as e:
            await asyncio.sleep(1)

# ==================== هندلر ====================
async def handle_bot_update(update):
    try:
        if "callback_query" in update:
            callback = update["callback_query"]
            chat_id = callback["message"]["chat"]["id"]
            message_id = callback["message"]["message_id"]
            data = callback["data"]
            user_id = callback["from"]["id"]

            if data == "status_self":
                session_exists = user_id in sessions and sessions[user_id].get('step') == 'connected'
                session_status = "🟢 فعال" if session_exists else "🔴 غیرفعال"
                status_text = (f"📊 **وضعیت سلف {BOT_NAME}**\n\n"
                    f"📱 **وضعیت سشن:** {session_status}\n\n"
                    f"🚀 برای فعال‌سازی سلف، از دکمه «فعال‌سازی سلف» استفاده کن.")
                await edit_bot_message(chat_id, message_id, status_text, get_back_keyboard())
                return

            if data == "activate_self":
                session_exists = user_id in sessions and sessions[user_id].get('step') == 'connected'
                if session_exists:
                    keyboard = {"inline_keyboard": [
                        [{"text": "🔄 فعال‌سازی مجدد", "callback_data": "reactivate_self"}],
                        [{"text": "🔙 بازگشت", "callback_data": "back_to_main"}]]}
                    await edit_bot_message(chat_id, message_id,
                        f"✅ **سلف فعاله!**\n\nاگه مشکل داری، مجدد فعال کن.", keyboard)
                    return
                else:
                    admin_states[chat_id] = {"state": "waiting_phone_for_self"}
                    await edit_bot_message(chat_id, message_id,
                        f"🚀 **فعال‌سازی سلف**\n\n📱 شماره موبایلت رو بفرست:\n\n"
                        f"✅ فرمت‌های مجاز:\n• `09123456789`\n• `9123456789`\n"
                        f"• `۰۹۱۲۳۴۵۶۷۸۹`\n• `۹۱۲۳۴۵۶۷۸۹`\n\n"
                        f"برای انصراف /cancel", None)
                    return

            if data == "reactivate_self":
                if user_id in sessions:
                    try:
                        if "client" in sessions[user_id]:
                            await sessions[user_id]["client"].stop()
                    except: pass
                    del sessions[user_id]
                await db_remove_active_session(user_id)
                session_file = os.path.join(SESSIONS_DIR, f"session_{user_id}.bale")
                if os.path.exists(session_file):
                    try: os.remove(session_file)
                    except: pass
                admin_states[chat_id] = {"state": "waiting_phone_for_self"}
                await edit_bot_message(chat_id, message_id,
                    f"🔄 **فعال‌سازی مجدد**\n\n📱 شماره موبایلت رو بفرست:\n\n"
                    f"برای انصراف /cancel", None)
                return

            if data == "help_self":
                help_text = (f"🌟 **راهنمای {BOT_NAME}** 🌟\n\n📞 پشتیبانی: {SUPPORT_ID}\n\n"
                    f"📌 برای دستورات کامل، تو چت سلف `.راهنما` بفرست.")
                keyboard = {"inline_keyboard": [[{"text": "🔙 بازگشت", "callback_data": "back_to_main"}]]}
                await edit_bot_message(chat_id, message_id, help_text, keyboard)
                return

            if data == "back_to_main":
                await edit_bot_message(chat_id, message_id,
                    f"🎫 **{BOT_NAME}**\n\nاز دکمه‌ها استفاده کن:", get_main_keyboard())
                return

            session = sessions.get(user_id)
            if not session or session.get("step") != "connected" or "client" not in session:
                await send_bot_message(chat_id, "❌ نشست فعال نیست! /start بزن.")
                return

            user_client = session["client"]
            try:
                await ensure_user_id(user_client)
            except:
                await send_bot_message(chat_id, "❌ خطا! /start بزن.")
                return
            my_id = user_client.me.id

            if data == "open_clock_settings":
                user_data = await get_user_cached(my_id) or {}
                await edit_bot_message(chat_id, message_id, "🕘 **پنل ساعت**", get_clock_settings_markup(my_id, user_data))
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
                    await send_bot_message(chat_id, "✏️ قالب نام رو بفرست (باید شامل `{time}` باشه):")
                    return
                elif data == "clock_bio_format":
                    session["awaiting_format"] = "bio"
                    await send_bot_message(chat_id, "✏️ قالب بیو رو بفرست (باید شامل `{time}` باشه):")
                    return
                elif data == "clock_back":
                    await edit_bot_message(chat_id, message_id, "⚙️ **پنل سلف:**", get_settings_markup(
                        getattr(user_client, "is_bold_active", True),
                        getattr(user_client, "is_blue_active", False),
                        getattr(user_client, "is_italic_active", False),
                        getattr(user_client, "is_gift_active", True),
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
                "⚙️ **پنل سلف:**",
                get_settings_markup(user_client.is_bold_active, user_client.is_blue_active,
                    user_client.is_italic_active, user_client.is_gift_active, user_client.is_reaction_active))
            return

        msg = update.get("message")
        if not msg: return

        chat_id = msg["chat"]["id"]
        user_id = msg.get("from", {}).get("id")
        text = msg.get("text", "").strip()

        if chat_id in admin_states:
            state = admin_states[chat_id]["state"]
            if text.lower() == "/cancel":
                del admin_states[chat_id]
                await send_bot_message(chat_id, "❌ لغو شد.", get_main_keyboard())
                return

            if state == "waiting_phone_for_self":
                phone = normalize_phone(text)
                if len(phone) != 10 or not phone.startswith('9'):
                    await send_bot_message(chat_id,
                        "❌ **شماره نامعتبر!**\n\n📱 فرمت‌های مجاز:\n"
                        "• `09123456789`\n• `9123456789`\n"
                        "• `۰۹۱۲۳۴۵۶۷۸۹`\n• `۹۱۲۳۴۵۶۷۸۹`")
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

        if text == "/start":
            user_data = await get_user_cached(user_id)
            if not user_data:
                await set_user_cached(user_id, {})
            await send_bot_message(chat_id,
                f"🎫 **{BOT_NAME}** 🎫\n\n⚡ **قدرت مدیریت سلف!**\n\n"
                f"📌 مراحل:\n1️⃣ فعال‌سازی سلف\n2️⃣ شماره\n3️⃣ کد تأیید\n\n"
                f"📞 پشتیبانی: {SUPPORT_ID}",
                get_main_keyboard())
            return

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
                    if session.get("client"):
                        await update_user_profile(session["client"], user_id)
            elif target == "bio":
                user_data["clock"]["bio_format"] = text
                await set_user_cached(user_id, user_data)
                await send_bot_message(chat_id, f"✅ قالب بیو: `{text}`")
                if user_data["clock"].get("bio_enabled", False):
                    if session.get("client"):
                        await update_user_profile(session["client"], user_id)
            session.pop("awaiting_format", None)
            return

        if not session:
            sessions[user_id] = {"step": "start"}
            session = sessions[user_id]

        if session.get("step") == "connected" and "client" in session:
            user_client = session["client"]
            if text == "/panel":
                await send_bot_message(chat_id, "⚙️ **پنل سلف:**", get_settings_markup(
                    user_client.is_bold_active, user_client.is_blue_active,
                    user_client.is_italic_active, user_client.is_gift_active, user_client.is_reaction_active))
            return

        if session["step"] == "waiting_phone":
            if not text:
                await send_bot_message(chat_id, "⚠️ شماره خالیه!")
                return
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
                    "🔑 **کد ارسال شد!**\n\n📥 کد رو بفرست:")
            except Exception as e:
                await send_bot_message(chat_id, "❌ خطا! /start بزن.")
                session["step"] = "start"

        elif session["step"] == "waiting_code":
            if not text:
                await send_bot_message(chat_id, "⚠️ کد خالیه!")
                return
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
                await send_bot_message(chat_id, "❌ کد اشتباه! /start بزن.")
                session["step"] = "start"
    except Exception as e:
        logger.error(f"Handle error: {e}")

# ==================== بازیابی .اتو ====================
async def restore_auto_tasks():
    tasks = await db_get_auto_tasks()
    for storage_key, task_info in tasks.items():
        parts = storage_key.split("|")
        if len(parts) != 2: continue
        target_chat_id = int(parts[0])
        user_owner_id = int(parts[1])
        session = sessions.get(user_owner_id)
        if not session or session.get("step") != "connected" or "client" not in session: continue
        user_client = session["client"]
        try: await ensure_user_id(user_client)
        except: continue
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

# ==================== بارگذاری خودکار ====================
async def auto_load_active_users():
    active_sessions = await db_get_all_active_sessions()
    for user_id, session_file in active_sessions.items():
        if not os.path.exists(session_file):
            await db_remove_active_session(user_id)
            continue
        try:
            user_data = await get_user_cached(user_id) or {}
            user_client = create_user_client(user_id, user_data)
            user_client.phone_code_callback = user_client.phone_code_callback or (lambda *a, **k: None)
            await asyncio.wait_for(user_client.start(run_in_background=True, signal_handling=False), timeout=10)
            await ensure_user_id(user_client)
            sessions[user_id] = {"step": "connected", "client": user_client}
            create_background_task(run_client_forever(user_client, user_id))
            await send_bot_message(user_id, f"🚀 **{BOT_NAME} آنلاین شد!**")
        except:
            if os.path.exists(session_file):
                try: os.remove(session_file)
                except: pass
            await db_remove_active_session(user_id)

# ==================== بازسازی ====================
async def recreate_bot_session():
    global bot_session
    if bot_session and not bot_session.closed:
        try: await bot_session.close()
        except: pass
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, limit_per_host=100, ttl_dns_cache=600,
        keepalive_timeout=300, enable_cleanup_closed=True)
    bot_session = aiohttp.ClientSession(connector=connector)

# ==================== Signal ====================
_shutdown_event = asyncio.Event()

async def graceful_shutdown():
    _shutdown_event.set()
    try:
        await asyncio.wait_for(update_queue.join(), timeout=5)
    except: pass
    for uid, sess in list(sessions.items()):
        try:
            if "client" in sess:
                await asyncio.wait_for(sess["client"].stop(), timeout=2)
        except: pass
    if _pool:
        try: await _pool.close()
        except: pass
    if bot_session and not bot_session.closed:
        try: await asyncio.wait_for(bot_session.close(), timeout=3)
        except: pass
    tasks = [t for t in asyncio.all_tasks() if t != asyncio.current_task()]
    for task in tasks: task.cancel()
    if tasks: await asyncio.gather(*tasks, return_exceptions=True)

# ==================== اصلی ====================
async def main():
    global bot_session, users_db
    await init_db()
    await init_pool()

    create_background_task(periodic_cleanup())
    create_background_task(clock_updater())
    create_background_task(session_health_checker())

    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, limit_per_host=100, ttl_dns_cache=600,
        keepalive_timeout=300, enable_cleanup_closed=True)
    bot_session = aiohttp.ClientSession(connector=connector)
    logger.info(f"{BOT_NAME} launched. Workers: {WORKER_COUNT}")

    await auto_load_active_users()
    await restore_auto_tasks()

    for _ in range(WORKER_COUNT):
        create_background_task(worker())

    try:
        await bot_session.get(f"{BASE_URL}/deleteWebhook", timeout=5)
    except: pass

    offset = 0
    while not _shutdown_event.is_set():
        try:
            url = f"{BASE_URL}/getUpdates"
            params = {"timeout": POLLING_TIMEOUT, "limit": 100}
            if offset: params["offset"] = str(offset + 1)
            async with bot_session.get(url, params=params, timeout=POLLING_TIMEOUT + 5) as resp:
                r = await resp.json()
                if r.get("ok") and r.get("result"):
                    for upd in r["result"]:
                        offset = upd["update_id"]
                        try:
                            update_queue.put_nowait(upd)
                        except asyncio.QueueFull:
                            pass
        except (aiohttp.ClientConnectionError, aiohttp.ClientError, asyncio.TimeoutError):
            await recreate_bot_session()
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Error: {e}")
            await asyncio.sleep(1)
    await graceful_shutdown()

# ==================== اجرا ====================
if __name__ == "__main__":
    print(f"""
╔═══════════════════════════════════════╗
║     🚀 {BOT_NAME}                    ║
║     ✨ سلف‌بات سبک                    ║
╚═══════════════════════════════════════╝
    """)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.critical(f"Fatal: {e}. Restarting in 5s...")
            time.sleep(5)