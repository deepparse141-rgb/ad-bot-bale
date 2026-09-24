# -*- coding: utf-8 -*-
"""
🚀 ربات سلف‌بات بله + تبلیغ + پاکت
👤 ورود با شماره + کد
📢 تبلیغ هر ۱۵ ثانیه
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
import signal
from datetime import datetime, timedelta
from collections import defaultdict, deque
from aiobale import Client, Dispatcher
from aiobale.types import Message, InfoMessage, Peer
from aiobale.filters import IsGift
from aiobale.enums import ChatType
from aiobale.methods import OpenGiftPacket

# ==================== تنظیمات ====================
BOT_TOKEN = "1566501587:CKigTRWfyH0SlxhFpvl_ou_jKJc9NGKO-vE"
ADMIN_ID = 0  # بعداً پر کن
SUPPORT_ID = "@Idnuedobot"

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
sessions = {}
bot_session = None
gift_semaphore = asyncio.Semaphore(20)
ad_task = None
my_groups = []
settings = {
    "gift_enabled": True,
    "ad_enabled": False,
    "ad_interval": AD_INTERVAL,
    "gift_speed": GIFT_SPEED,
}

# ==================== توابع کمکی ====================
def normalize_phone(phone_str):
    if not phone_str: return ""
    phone = phone_str.translate(PERSIAN_DIGITS)
    phone = re.sub(r'\D', '', phone)
    if phone.startswith('0'): phone = phone[1:]
    if phone.startswith('98') and len(phone) == 12: phone = phone[2:]
    if len(phone) == 10 and phone.startswith('9'): return phone
    return phone

async def send_bot_message(chat_id, text, reply_markup=None):
    try:
        url = f"{BASE_URL}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup: payload["reply_markup"] = reply_markup
        async with bot_session.post(url, json=payload, timeout=10) as r:
            return await r.json()
    except Exception as e:
        logger.error(f"Send error: {e}")
        return None

async def edit_bot_message(chat_id, message_id, text, reply_markup=None):
    try:
        url = f"{BASE_URL}/editMessageText"
        payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "Markdown"}
        if reply_markup: payload["reply_markup"] = reply_markup
        async with bot_session.post(url, json=payload, timeout=10) as r:
            return await r.json()
    except Exception as e:
        logger.error(f"Edit error: {e}")
        return None

# ==================== کیبوردها ====================
def main_menu():
    return {"inline_keyboard": [
        [{"text": "🚀 فعال‌سازی سلف", "callback_data": "activate_self"}],
        [{"text": "📊 وضعیت", "callback_data": "status"}],
        [{"text": "📖 راهنما", "callback_data": "help"}],
        [{"text": "📞 پشتیبانی", "url": f"https://ble.ir/{SUPPORT_ID.replace('@','')}"}]
    ]}

def self_menu():
    return {"inline_keyboard": [
        [{"text": f"{'🟢' if settings['gift_enabled'] else '🔴'} پاکت خودکار", "callback_data": "toggle_gift"}],
        [{"text": f"{'🟢' if settings['ad_enabled'] else '🔴'} تبلیغ خودکار", "callback_data": "toggle_ad"}],
        [{"text": f"⏰ فاصله: {settings['ad_interval']}s", "callback_data": "set_interval"}],
        [{"text": "👥 لیست گروه‌ها", "callback_data": "list_groups"}],
        [{"text": "🔙 بازگشت", "callback_data": "back"}]
    ]}

# ==================== پاکت خودکار ====================
async def open_gift_fast(message, token):
    async with gift_semaphore:
        try:
            info_msg = InfoMessage(
                peer=Peer(type=message.chat.type, id=message.chat.id),
                message_id=message.message_id,
                date=message.date,
                previous_message=None
            )
            await asyncio.wait_for(
                sessions["client"](OpenGiftPacket(
                    message=info_msg,
                    receiver_token=token,
                    page_no={}
                )),
                timeout=2.0
            )
            logger.info("🎁 پاکت باز شد!")
            try: await message.react("👍")
            except: pass
        except Exception as e:
            logger.debug(f"پاکت: {e}")

# ==================== تبلیغ ====================
async def ad_loop():
    global my_groups
    logger.info(f"📢 تبلیغ شروع (هر {settings['ad_interval']}s)")
    
    while settings['ad_enabled']:
        try:
            if not my_groups:
                await asyncio.sleep(settings['ad_interval'])
                continue
            
            client = sessions.get("client")
            if not client: break
            
            logger.info(f"📢 ارسال به {len(my_groups)} گروه...")
            sem = asyncio.Semaphore(5)
            
            async def send_ad(g):
                async with sem:
                    try:
                        await client.send_message(AD_TEXT, g['id'], ChatType.GROUP)
                        await asyncio.sleep(0.1)
                    except Exception as e:
                        logger.debug(f"خطا {g.get('title','?')}: {e}")
            
            await asyncio.gather(*[send_ad(g) for g in my_groups], return_exceptions=True)
            logger.info(f"✅ ارسال شد")
            await asyncio.sleep(settings['ad_interval'])
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"خطا تبلیغ: {e}")
            await asyncio.sleep(5)

# ==================== گرفتن گروه‌ها ====================
async def refresh_groups():
    global my_groups
    try:
        client = sessions.get("client")
        if not client: return
        dialogs = await client.get_dialogs()
        groups = []
        for d in dialogs:
            try:
                chat = d.chat if hasattr(d, 'chat') else d
                ct = getattr(chat, 'type', None)
                if ct in (ChatType.GROUP, ChatType.SUPERGROUP):
                    groups.append({'id': chat.id, 'title': getattr(chat, 'title', 'بدون نام')})
            except: continue
        my_groups = groups
        logger.info(f"👥 {len(my_groups)} گروه")
    except Exception as e:
        logger.error(f"گروه‌ها: {e}")

# ==================== ساخت کلاینت ====================
def create_client(session_file):
    dp = Dispatcher()
    client = Client(dp, session_file=session_file)
    
    @dp.message(IsGift())
    async def gift_handler(message: Message):
        if not settings['gift_enabled']: return
        token = None
        try:
            if hasattr(message.content, "gift") and hasattr(message.content.gift, "token"):
                token = message.content.gift.token.value
        except: pass
        if token:
            asyncio.create_task(open_gift_fast(message, token))
    
    @dp.message()
    async def msg_handler(message: Message):
        try:
            text = (message.text or "").strip()
            chat_id = message.chat.id
            sender_id = getattr(message, 'sender_id', None)
            
            # فقط پیام‌های خودت
            client = sessions.get("client")
            if not client or not hasattr(client, 'me'): return
            if sender_id != client.me.id: return
            
            if not text.startswith("."): return
            
            # راهنما
            if text in [".راهنما", ".help"]:
                help_text = (
                    "📖 **راهنما**\n\n"
                    "🔹 `.راهنما` → همین پیام\n"
                    "🔹 `.وضعیت` → وضعیت\n"
                    "🔹 `.گروه‌ها` → لیست گروه‌ها\n\n"
                    "📢 **تبلیغ:**\n"
                    "🔹 `.تبلیغ` → شروع\n"
                    "🔹 `.تبلیغ خاموش` → توقف\n"
                    "🔹 `.فاصله 30` → تغییر فاصله\n\n"
                    "🎁 **پاکت:**\n"
                    "🔹 `.پاکت روشن` / `.پاکت خاموش`\n\n"
                    "🛑 `.توقف` → خروج"
                )
                try: await message.reply(help_text)
                except: pass
                return
            
            # وضعیت
            if text == ".وضعیت":
                status = (
                    f"📊 **وضعیت**\n\n"
                    f"📢 تبلیغ: {'🟢 روشن' if settings['ad_enabled'] else '🔴 خاموش'}\n"
                    f"⏰ فاصله: {settings['ad_interval']}s\n"
                    f"🎁 پاکت: {'🟢 روشن' if settings['gift_enabled'] else '🔴 خاموش'}\n"
                    f"👥 گروه‌ها: {len(my_groups)}"
                )
                try: await message.reply(status)
                except: pass
                return
            
            # لیست گروه‌ها
            if text == ".گروه‌ها":
                if not my_groups:
                    try: await message.reply("❌ گروهی نیست!")
                    except: pass
                    return
                txt = f"👥 **{len(my_groups)} گروه:**\n\n"
                for i, g in enumerate(my_groups[:20], 1):
                    txt += f"{i}. {g.get('title','?')}\n"
                try: await message.reply(txt)
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
                try: await message.reply(f"✅ تبلیغ روشن شد!\n👥 {len(my_groups)} گروه\n⏰ هر {settings['ad_interval']}s")
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
                try: await message.reply("✅ پاکت روشن شد")
                except: pass
                return
            
            if text == ".پاکت خاموش":
                settings['gift_enabled'] = False
                try: await message.reply("🛑 پاکت خاموش شد")
                except: pass
                return
            
            # توقف
            if text == ".توقف":
                try: await message.reply("🛑 خروج...")
                except: pass
                await asyncio.sleep(1)
                os._exit(0)
                
        except Exception as e:
            logger.error(f"Handler: {e}")
    
    return client

# ==================== وب سرور (keep-alive) ====================
async def health(request):
    return web.Response(text="Bot is running!")

async def ping(request):
    return web.Response(text="pong")

async def start_web():
    app = web.Application()
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    app.router.add_get('/ping', ping)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"🌐 Web server on port {PORT}")

# ==================== لاگین ====================
async def do_login(chat_id, phone):
    try:
        await send_bot_message(chat_id, "⏳ در حال ارسال کد...")
        
        session_file = os.path.join(SESSIONS_DIR, "my_session.bale")
        client = create_client(session_file)
        
        # callback برای کد
        code_future = asyncio.get_event_loop().create_future()
        
        async def code_callback(phone, code_type, tx_hash):
            await send_bot_message(chat_id, "🔑 کد رو از SMS بخون و تو ربات بفرست")
            # منتظر کد از کاربر
            while True:
                if "code" in sessions:
                    c = sessions.pop("code")
                    return c
                await asyncio.sleep(1)
        
        client.phone_code_callback = code_callback
        
        phone_for_auth = '98' + phone
        await client.start_phone_auth(phone_for_auth)
        
        sessions["client"] = client
        sessions["step"] = "waiting_code"
        sessions["phone"] = phone
        
        await send_bot_message(chat_id, "🔑 **کد SMS رو بفرست:**")
    except Exception as e:
        logger.error(f"Login: {e}")
        await send_bot_message(chat_id, f"❌ خطا: {e}")
        sessions.pop("client", None)

# ==================== handle update ====================
async def handle_update(update):
    try:
        # callback
        if "callback_query" in update:
            cb = update["callback_query"]
            chat_id = cb["message"]["chat"]["id"]
            msg_id = cb["message"]["message_id"]
            data = cb["data"]
            
            if data == "activate_self":
                sessions["admin_states"] = {"state": "waiting_phone", "chat_id": chat_id}
                await edit_bot_message(chat_id, msg_id, "📱 **شماره موبایلت رو بفرست:**\n\nمثال: `09123456789`")
            
            elif data == "status":
                status = (
                    f"📊 **وضعیت**\n\n"
                    f"📢 تبلیغ: {'🟢' if settings['ad_enabled'] else '🔴'}\n"
                    f"⏰ فاصله: {settings['ad_interval']}s\n"
                    f"🎁 پاکت: {'🟢' if settings['gift_enabled'] else '🔴'}\n"
                    f"👥 گروه‌ها: {len(my_groups)}\n"
                    f"🔌 اتصال: {'✅' if 'client' in sessions else '❌'}"
                )
                await edit_bot_message(chat_id, msg_id, status, self_menu())
            
            elif data == "help":
                help_text = (
                    "📖 **راهنمای ربات**\n\n"
                    "🔹 ورود با شماره و کد\n"
                    "🔹 تبلیغ خودکار در گروه‌ها\n"
                    "🔹 پاکت خودکار\n\n"
                    "💡 تو گروه، دستور `.راهنما` رو بزن"
                )
                await edit_bot_message(chat_id, msg_id, help_text, {"inline_keyboard": [[{"text": "🔙 بازگشت", "callback_data": "back"}]]})
            
            elif data == "back":
                await edit_bot_message(chat_id, msg_id, "🏠 منو:", main_menu())
            
            elif data == "toggle_gift":
                settings['gift_enabled'] = not settings['gift_enabled']
                await edit_bot_message(chat_id, msg_id, "⚙️ تنظیمات:", self_menu())
            
            elif data == "toggle_ad":
                settings['ad_enabled'] = not settings['ad_enabled']
                global ad_task
                if settings['ad_enabled'] and (ad_task is None or ad_task.done()):
                    ad_task = asyncio.create_task(ad_loop())
                await edit_bot_message(chat_id, msg_id, "⚙️ تنظیمات:", self_menu())
            
            elif data == "set_interval":
                await edit_bot_message(chat_id, msg_id, "⏰ فاصله رو تو گروه با `.فاصله 30` تنظیم کن")
            
            elif data == "list_groups":
                if not my_groups:
                    await edit_bot_message(chat_id, msg_id, "❌ گروهی نیست", self_menu())
                else:
                    txt = f"👥 **{len(my_groups)} گروه:**\n\n"
                    for i, g in enumerate(my_groups[:20], 1):
                        txt += f"{i}. {g.get('title','?')}\n"
                    await edit_bot_message(chat_id, msg_id, txt, self_menu())
        
        # message
        if "message" in update:
            msg = update["message"]
            chat_id = msg["chat"]["id"]
            text = (msg.get("text") or "").strip()
            user_id = msg.get("from", {}).get("id")
            
            # /start
            if text == "/start":
                await send_bot_message(chat_id, "🎫 **به ربات سلف‌بات خوش آمدی!**\n\nاز منو انتخاب کن:", main_menu())
                return
            
            # اگه منتظر شماره
            admin = sessions.get("admin_states", {})
            if admin.get("state") == "waiting_phone":
                phone = normalize_phone(text)
                if len(phone) != 10:
                    await send_bot_message(chat_id, "❌ شماره نامعتبر! مثال: `09123456789`")
                    return
                admin["state"] = "waiting_code"
                sessions["admin_states"] = admin
                asyncio.create_task(do_login(chat_id, phone))
                return
            
            # اگه کد اومد
            if sessions.get("step") == "waiting_code":
                sessions["code"] = text
                await send_bot_message(chat_id, "⏳ بررسی کد...")
                # کد رو به callback بده
                return
    
    except Exception as e:
        logger.error(f"Update: {e}\n{traceback.format_exc()}")

# ==================== Main ====================
async def main():
    global bot_session
    
    logger.info("🚀 شروع ربات")
    
    # Web
    await start_web()
    
    # HTTP session
    connector = aiohttp.TCPConnector(limit=100)
    bot_session = aiohttp.ClientSession(connector=connector)
    
    # Polling
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
        except Exception as e:
            logger.error(f"Polling: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    print("""
╔══════════════════════════════════════╗
║  🚀 ربات سلف‌بات بله                 ║
║  📢 تبلیغ + 🎁 پاکت                  ║
╚══════════════════════════════════════╝
    """)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 خداحافظ!")
    except Exception as e:
        logger.critical(f"Fatal: {e}\n{traceback.format_exc()}")
