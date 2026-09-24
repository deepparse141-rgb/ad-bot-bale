# -*- coding: utf-8 -*-
"""
🚀 ربات سلف‌بات بله — نسخه سریع
📱 ورود فوری با شماره و کد
📢 تبلیغ خودکار
🎁 پاکت خودکار
"""

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
import traceback
from datetime import datetime, timedelta
from collections import defaultdict, deque
from aiobale import Client, Dispatcher
from aiobale.types import Message, InfoMessage, Peer
from aiobale.filters import IsGift
from aiobale.enums import ChatType
from aiobale.methods import OpenGiftPacket

# ==================== تنظیمات ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "1566501587:CKigTRWfyH0SlxhFpvl_ou_jKJc9NGKO-vE")
SUPPORT_ID = os.getenv("SUPPORT_ID", "@Idnuedobot")

AD_TEXT = """🚀 کانالتو بترکون! با این ربات خفن
⚡ عضوگیر + سین‌زن حرفه‌ای بله
╭┈┈┈┈┈┈┈┈┈┈┈┈┈┈╮
┊  🤖   ┊@Idnuedobot
╰┈┈┈┈┈┈┈┈┈┈┈┈┈┈╯"""

AD_INTERVAL = 15
GIFT_SPEED = 0.3
PORT = int(os.getenv("PORT", "8080"))

SESSIONS_DIR = "sessions"
os.makedirs(SESSIONS_DIR, exist_ok=True)

BASE_URL = f"https://tapi.bale.ai/bot{BOT_TOKEN}"
PERSIAN_DIGITS = str.maketrans('۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩', '01234567890123456789')

# ==================== لاگ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ==================== متغیرهای سراسری ====================
user_sessions = {}          # {chat_id: {state, phone, client, code_future}}
active_client = None        # کلاینت فعال
my_groups = []              # لیست گروه‌ها
ad_task = None              # تسک تبلیغ
bot_session = None          # aiohttp session
gift_semaphore = asyncio.Semaphore(20)

settings = {
    "gift_enabled": True,
    "ad_enabled": False,
    "ad_interval": AD_INTERVAL,
}

# ==================== توابع کمکی ====================
def normalize_phone(phone_str):
    """نرمال‌سازی شماره"""
    if not phone_str: return ""
    phone = phone_str.translate(PERSIAN_DIGITS)
    phone = re.sub(r'\D', '', phone)
    if phone.startswith('0'): phone = phone[1:]
    if phone.startswith('98') and len(phone) == 12: phone = phone[2:]
    if len(phone) == 10 and phone.startswith('9'): return phone
    return ""

async def send_msg(chat_id, text, markup=None):
    """ارسال پیام"""
    try:
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if markup: payload["reply_markup"] = markup
        async with bot_session.post(f"{BASE_URL}/sendMessage", json=payload, timeout=10) as r:
            return await r.json()
    except Exception as e:
        logger.error(f"send_msg: {e}")

# ==================== کیبوردها ====================
def main_menu():
    return {"inline_keyboard": [
        [{"text": "🚀 فعال‌سازی سلف", "callback_data": "activate_self"}],
        [{"text": "📊 وضعیت", "callback_data": "status"}],
        [{"text": "📖 راهنما", "callback_data": "help"}],
    ]}

# ==================== پاکت خودکار ====================
async def open_gift(message, token):
    async with gift_semaphore:
        try:
            info = InfoMessage(
                peer=Peer(type=message.chat.type, id=message.chat.id),
                message_id=message.message_id,
                date=message.date,
                previous_message=None
            )
            await asyncio.wait_for(
                active_client(OpenGiftPacket(message=info, receiver_token=token, page_no={})),
                timeout=2.0
            )
            logger.info("🎁 پاکت باز شد!")
            try: await message.react("👍")
            except: pass
        except Exception as e:
            logger.debug(f"gift: {e}")

# ==================== تبلیغ ====================
async def ad_loop():
    logger.info(f"📢 تبلیغ شروع (هر {settings['ad_interval']}s)")
    while settings['ad_enabled'] and active_client:
        try:
            if not my_groups:
                await asyncio.sleep(5); continue
            logger.info(f"📢 ارسال به {len(my_groups)} گروه...")
            sem = asyncio.Semaphore(5)
            async def send_one(g):
                async with sem:
                    try:
                        await active_client.send_message(AD_TEXT, g['id'], ChatType.GROUP)
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        logger.debug(f"{g.get('title','?')}: {e}")
            await asyncio.gather(*[send_one(g) for g in my_groups], return_exceptions=True)
            logger.info("✅ ارسال شد")
            await asyncio.sleep(settings['ad_interval'])
        except asyncio.CancelledError: break
        except Exception as e:
            logger.error(f"ad_loop: {e}")
            await asyncio.sleep(5)

# ==================== گرفتن گروه‌ها ====================
async def refresh_groups():
    global my_groups
    try:
        if not active_client: return
        dialogs = await active_client.get_dialogs()
        groups = []
        for d in dialogs:
            try:
                chat = d.chat if hasattr(d, 'chat') else d
                ct = getattr(chat, 'type', None)
                if ct in (ChatType.GROUP, ChatType.SUPERGROUP):
                    groups.append({'id': chat.id, 'title': getattr(chat, 'title', '?')})
            except: continue
        my_groups = groups
        logger.info(f"👥 {len(my_groups)} گروه")
    except Exception as e:
        logger.error(f"refresh_groups: {e}")

# ==================== ساخت کلاینت ====================
def make_client(session_file, chat_id):
    """ساخت کلاینت با هندلرها"""
    global active_client
    dp = Dispatcher()
    client = Client(dp, session_file=session_file)
    active_client = client
    
    # هندلر پاکت
    @dp.message(IsGift())
    async def gift_handler(message: Message):
        if not settings['gift_enabled']: return
        token = None
        try:
            if hasattr(message.content, "gift") and hasattr(message.content.gift, "token"):
                token = message.content.gift.token.value
        except: pass
        if token:
            asyncio.create_task(open_gift(message, token))
    
    # هندلر پیام‌های خودت
    @dp.message()
    async def msg_handler(message: Message):
        try:
            text = (message.text or "").strip()
            sender_id = getattr(message, 'sender_id', None)
            if not client.me or sender_id != client.me.id: return
            if not text.startswith("."): return
            
            # راهنما
            if text in [".راهنما", ".help"]:
                help_text = (
                    "📖 **راهنما**\n\n"
                    "🔹 `.وضعیت` — وضعیت ربات\n"
                    "🔹 `.گروه‌ها` — لیست گروه‌ها\n\n"
                    "📢 **تبلیغ:**\n"
                    "🔹 `.تبلیغ` — شروع\n"
                    "🔹 `.تبلیغ خاموش` — توقف\n"
                    "🔹 `.فاصله 30` — تغییر فاصله\n\n"
                    "🎁 **پاکت:**\n"
                    "🔹 `.پاکت روشن` / `.پاکت خاموش`\n\n"
                    "🛑 `.توقف` — خروج"
                )
                try: await message.reply(help_text)
                except: pass
                return
            
            # وضعیت
            if text == ".وضعیت":
                s = (
                    f"📊 **وضعیت**\n\n"
                    f"📢 تبلیغ: {'🟢' if settings['ad_enabled'] else '🔴'}\n"
                    f"⏰ فاصله: {settings['ad_interval']}s\n"
                    f"🎁 پاکت: {'🟢' if settings['gift_enabled'] else '🔴'}\n"
                    f"👥 گروه‌ها: {len(my_groups)}"
                )
                try: await message.reply(s)
                except: pass
                return
            
            # گروه‌ها
            if text == ".گروه‌ها":
                if not my_groups:
                    try: await message.reply("❌ گروهی نیست!")
                    except: pass
                    return
                t = f"👥 **{len(my_groups)} گروه:**\n\n"
                for i, g in enumerate(my_groups[:20], 1):
                    t += f"{i}. {g.get('title','?')}\n"
                try: await message.reply(t)
                except: pass
                return
            
            # تبلیغ
            if text == ".تبلیغ":
                if settings['ad_enabled']:
                    try: await message.reply("⚠️ قبلاً روشنه!")
                    except: pass
                    return
                settings['ad_enabled'] = True
                global ad_task
                if ad_task is None or ad_task.done():
                    ad_task = asyncio.create_task(ad_loop())
                try: await message.reply(f"✅ تبلیغ روشن!\n👥 {len(my_groups)} گروه")
                except: pass
                return
            
            if text == ".تبلیغ خاموش":
                settings['ad_enabled'] = False
                try: await message.reply("🛑 تبلیغ خاموش شد")
                except: pass
                return
            
            # فاصله
            if text.startswith(".فاصله "):
                try:
                    sec = int(text.split()[1])
                    if sec < 5:
                        try: await message.reply("⚠️ حداقل ۵ ثانیه!")
                        except: pass
                        return
                    settings['ad_interval'] = sec
                    try: await message.reply(f"✅ فاصله = {sec}s")
                    except: pass
                except:
                    try: await message.reply("⚠️ فرمت: `.فاصله 30`")
                    except: pass
                return
            
            # پاکت
            if text == ".پاکت روشن":
                settings['gift_enabled'] = True
                try: await message.reply("✅ پاکت روشن")
                except: pass
                return
            
            if text == ".پاکت خاموش":
                settings['gift_enabled'] = False
                try: await message.reply("🛑 پاکت خاموش")
                except: pass
                return
            
            # توقف
            if text == ".توقف":
                try: await message.reply("🛑 خروج...")
                except: pass
                await asyncio.sleep(1)
                os._exit(0)
        
        except Exception as e:
            logger.error(f"msg_handler: {e}")
    
    return client

# ==================== وب سرور ====================
async def health(request):
    return web.Response(text="Bot is running!")

async def start_web():
    app = web.Application()
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    app.router.add_get('/ping', health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"🌐 Web server on port {PORT}")

# ==================== لاگین سریع ====================
async def do_login(chat_id, phone):
    """شروع لاگین"""
    try:
        await send_msg(chat_id, "⏳ **در حال ارسال کد...**\n\nچند ثانیه صبر کن")
        
        session_file = os.path.join(SESSIONS_DIR, f"session_{chat_id}.bale")
        
        # پاک کردن سشن قبلی اگه هست
        if os.path.exists(session_file):
            try: os.remove(session_file)
            except: pass
        
        client = make_client(session_file, chat_id)
        
        # callback برای کد
        code_future = asyncio.get_event_loop().create_future()
        
        async def code_callback(phone_arg, code_type, tx_hash):
            logger.info(f"📨 callback کد برای {phone_arg}")
            return await code_future
        
        client.phone_code_callback = code_callback
        
        # ذخیره در session
        user_sessions[chat_id] = {
            "state": "waiting_code",
            "phone": phone,
            "client": client,
            "code_future": code_future,
        }
        
        # ارسال درخواست کد
        phone_for_auth = '98' + phone
        logger.info(f"📤 درخواست کد برای {phone_for_auth}")
        
        await asyncio.wait_for(client.start_phone_auth(phone_for_auth), timeout=30.0)
        
        logger.info("✅ درخواست کد ارسال شد")
        await send_msg(chat_id, "🔑 **کد SMS رو بفرست:**\n\n(۵ رقم)")
        
    except asyncio.TimeoutError:
        await send_msg(chat_id, "❌ زمان ارسال کد تموم شد!\nدوباره `/start` بزن")
        user_sessions.pop(chat_id, None)
    except Exception as e:
        logger.error(f"do_login: {e}\n{traceback.format_exc()}")
        await send_msg(chat_id, f"❌ خطا:\n`{str(e)[:200]}`")
        user_sessions.pop(chat_id, None)

# ==================== تایید کد ====================
async def verify_code(chat_id, code):
    """تایید کد و راه‌اندازی"""
    try:
        sess = user_sessions.get(chat_id)
        if not sess:
            await send_msg(chat_id, "❌ سشن منقضی شد!\n`/start` بزن")
            return
        
        client = sess["client"]
        code_future = sess["code_future"]
        
        # پاک کردن فاصله‌های کد
        code = code.strip().replace(" ", "").replace("-", "")
        if not code.isdigit() or len(code) < 4:
            await send_msg(chat_id, "❌ کد نامعتبر! ۵ رقم بفرست")
            return
        
        # ارسال کد به callback
        if not code_future.done():
            code_future.set_result(code)
        
        await send_msg(chat_id, "✅ **کد دریافت شد!**\n\n⏳ در حال بررسی...")
        
        # صبر کن تا کلاینت validate کنه
        # (validate_code خودش داخل start_phone_auth فراخوانی می‌شه)
        
        # شروع کلاینت در پس‌زمینه
        async def start_client():
            try:
                await client.start(run_in_background=False, signal_handling=False)
                # اگه رسیدیم اینجا، یعنی موفق
                logger.info(f"✅ سشن {chat_id} فعال شد")
                await send_msg(chat_id, "🎉 **ورود موفق!**\n\nحالا می‌تونی از دستورات استفاده کنی")
                await refresh_groups()
                await send_msg(chat_id, f"👥 {len(my_groups)} گروه پیدا شد\n\nاز منو استفاده کن:", main_menu())
            except Exception as e:
                logger.error(f"start_client: {e}\n{traceback.format_exc()}")
                await send_msg(chat_id, f"❌ خطا در اتصال:\n`{str(e)[:200]}`")
                user_sessions.pop(chat_id, None)
        
        asyncio.create_task(start_client())
        user_sessions[chat_id]["state"] = "starting"
        
    except Exception as e:
        logger.error(f"verify_code: {e}\n{traceback.format_exc()}")
        await send_msg(chat_id, f"❌ خطا:\n`{str(e)[:200]}`")

# ==================== Handle Update ====================
async def handle_update(update):
    try:
        if "callback_query" in update:
            cb = update["callback_query"]
            chat_id = cb["message"]["chat"]["id"]
            data = cb["data"]
            
            if data == "activate_self":
                user_sessions[chat_id] = {"state": "waiting_phone"}
                await send_msg(chat_id, "📱 **شماره موبایلت رو بفرست:**\n\nمثال: `09123456789`")
            
            elif data == "status":
                s = (f"📊 **وضعیت**\n\n"
                     f"📢 تبلیغ: {'🟢' if settings['ad_enabled'] else '🔴'}\n"
                     f"🎁 پاکت: {'🟢' if settings['gift_enabled'] else '🔴'}\n"
                     f"👥 گروه‌ها: {len(my_groups)}\n"
                     f"🔌 اتصال: {'✅' if active_client else '❌'}")
                await send_msg(chat_id, s, main_menu())
            
            elif data == "help":
                await send_msg(chat_id, "📖 توی گروه `.راهنما` بزن", main_menu())
        
        if "message" in update:
            msg = update["message"]
            chat_id = msg["chat"]["id"]
            text = (msg.get("text") or "").strip()
            
            if text == "/start":
                await send_msg(chat_id, "🎫 **ربات سلف‌بات بله**\n\nاز منو انتخاب کن:", main_menu())
                return
            
            sess = user_sessions.get(chat_id)
            
            # مرحله شماره
            if sess and sess.get("state") == "waiting_phone":
                phone = normalize_phone(text)
                if not phone:
                    await send_msg(chat_id, "❌ شماره نامعتبر!\nمثال: `09123456789`")
                    return
                await do_login(chat_id, phone)
                return
            
            # مرحله کد
            if sess and sess.get("state") == "waiting_code":
                await verify_code(chat_id, text)
                return
    
    except Exception as e:
        logger.error(f"handle_update: {e}\n{traceback.format_exc()}")

# ==================== Main ====================
async def main():
    global bot_session
    
    logger.info("🚀 شروع ربات")
    
    await start_web()
    
    connector = aiohttp.TCPConnector(limit=100)
    bot_session = aiohttp.ClientSession(connector=connector)
    
    logger.info("📡 شروع polling")
    offset = 0
    while True:
        try:
            url = f"{BASE_URL}/getUpdates"
            params = {"timeout": 30, "limit": 100}
            if offset: params["offset"] = offset + 1
            async with bot_session.get(url, params=params, timeout=35) as r:
                data = await r.json()
                if data.get("ok") and data.get("result"):
                    for upd in data["result"]:
                        offset = upd["update_id"]
                        asyncio.create_task(handle_update(upd))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"polling: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════╗
║  🚀 ربات سلف‌بات بله                 ║
║  ⚡ نسخه سریع                        ║
╚══════════════════════════════════════╝
    """)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑")
    except Exception as e:
        logger.critical(f"Fatal: {e}")