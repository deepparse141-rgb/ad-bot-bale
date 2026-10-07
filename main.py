import requests
import json
import os
import time
import threading
from datetime import datetime
from flask import Flask, jsonify

# ═══════════════════════════════════
# ⚙️ تنظیمات
# ═══════════════════════════════════
TOKEN = "199184758:_XBQYSRDU16pFUE-V_3Umzmgkfq65Cno0E0"
BASE_URL = f"https://tapi.bale.ai/bot{TOKEN}"
OWNER_ID = "1530477937"

REWARD_COINS = 25
REQUIRED_COUNT = 50
BOT_TO_ADVERTISE = "@Idnuedobot"

os.makedirs("data", exist_ok=True)
DB_FILE = "data/hyperad.json"

# ═══════════════════════════════════
# 📦 دیتابیس
# ═══════════════════════════════════
def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {
        "users": {},
        "pending": {},
        "approved": {},
        "stats": {"total_claims": 0, "total_approved": 0}
    }

def save_db(db):
    try:
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False, indent=2)
    except:
        pass

# ═══════════════════════════════════
# 📡 API بله
# ═══════════════════════════════════
def api(method, **params):
    try:
        r = requests.post(f"{BASE_URL}/{method}", json=params, timeout=15)
        return r.json()
    except Exception as e:
        print(f"❌ خطا API: {e}")
        return {"ok": False}

def send_message(chat_id, text, reply_markup=None, reply_to_message_id=None):
    data = {"chat_id": chat_id, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    if reply_to_message_id:
        data["reply_to_message_id"] = reply_to_message_id
    res = api("sendMessage", **data)
    if not res.get("ok"):
        print(f"❌ خطا ارسال: {res}")
    return res

def forward_message(chat_id, from_chat_id, message_id):
    return api("forwardMessage", chat_id=chat_id, from_chat_id=from_chat_id, message_id=message_id)

def edit_message(chat_id, message_id, text, reply_markup=None):
    data = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    return api("editMessageText", **data)

def answer_callback(callback_id, text=None, alert=False):
    data = {"callback_query_id": callback_id}
    if text:
        data["text"] = text
    data["show_alert"] = alert
    return api("answerCallbackQuery", **data)

# ═══════════════════════════════════
# 🎮 کیبوردها
# ═══════════════════════════════════
def main_keyboard(user_id=None):
    rows = [
        [{"text": "🎁 دریافت ۲۵ سکه 🤩", "callback_data": "get_coin"}],
        [{"text": "📖 راهنما", "callback_data": "help"}]
    ]
    if str(user_id) == OWNER_ID:
        rows.append([{"text": "👑 پنل مالک", "callback_data": "owner_panel"}])
    return {"inline_keyboard": rows}

def owner_keyboard():
    return {"inline_keyboard": [
        [
            {"text": "📊 آمار کل", "callback_data": "stats"},
            {"text": "📋 در انتظارها", "callback_data": "pending_list"}
        ],
        [{"text": "🔙 بازگشت", "callback_data": "back_main"}]
    ]}

def send_photo_keyboard():
    return {"inline_keyboard": [[
        {"text": "📸 فرستادن عکس", "callback_data": "send_photo"}
    ]]}

def admin_approve_keyboard(user_id):
    return {"inline_keyboard": [
        [
            {"text": "✅ تایید", "callback_data": f"approve_{user_id}"},
            {"text": "❌ رد", "callback_data": f"reject_{user_id}"}
        ]
    ]}

def cancel_keyboard():
    return {"inline_keyboard": [[
        {"text": "🔙 لغو", "callback_data": "cancel"}
    ]]}

# ═══════════════════════════════════
# 🎯 حالت‌ها
# ═══════════════════════════════════
user_states = {}
db = load_db()

# ═══════════════════════════════════
# 🌐 Flask
# ═══════════════════════════════════
app = Flask(__name__)

@app.route("/")
def home():
    return "🤖 Hyper Ad Bot"

@app.route("/ping")
def ping():
    return "pong ✅"

@app.route("/health")
def health():
    return jsonify({
        "status": "online",
        "users": len(db["users"]),
        "pending": len(db["pending"]),
        "approved": len(db["approved"])
    })

# ═══════════════════════════════════
# 🎯 پردازش پیام
# ═══════════════════════════════════
def handle_message(msg):
    global user_states, db
    try:
        chat_id = msg["chat"]["id"]
        chat_type = msg["chat"]["type"]

        if chat_type != "private":
            return

        user_id = str(msg["from"]["id"])
        text = msg.get("text", "").strip()
        name = msg["from"].get("first_name", "کاربر")
        username = msg["from"].get("username", "")

        print(f"📩 پیام از {user_id}: {text}")

        db = load_db()

        if user_id not in db["users"]:
            db["users"][user_id] = {
                "name": name,
                "username": username,
                "coins": 0,
                "claims": 0,
                "joined": str(datetime.now())
            }
            save_db(db)

        if text == "/start":
            user_states.pop(user_id, None)
            send_message(chat_id,
                f"👋 سلام {name} جان! ❤️\n\n"
                f"🎉 به **هایپر تبلیغ** خوش اومدی!\n\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"💰 **{REWARD_COINS} سکه رایگان**\n"
                f"فقط با یه کار ساده!\n\n"
                f"📌 **چطوری؟**\n"
                f"دکمه «🎁 دریافت {REWARD_COINS} سکه 🤩» رو بزن\n"
                f"و مراحل رو انجام بده!\n"
                f"━━━━━━━━━━━━━━━━━\n\n"
                f"🚀 آماده‌ای؟",
                main_keyboard(user_id))
            return

    except Exception as e:
        print(f"❌ خطا handle_message: {e}")

# ═══════════════════════════════════
# 🎯 پردازش عکس
# ═══════════════════════════════════
def handle_photo(msg):
    global user_states, db
    try:
        chat_id = msg["chat"]["id"]
        user_id = str(msg["from"]["id"])
        name = msg["from"].get("first_name", "کاربر")
        username = msg["from"].get("username", "")
        message_id = msg["message_id"]

        print(f"📸 عکس از {user_id}")

        db = load_db()

        # اگه کاربر تو حالت انتظار عکس نیست
        if user_id not in user_states or user_states[user_id].get("step") != "waiting_photo":
            # اگه کاربر تو pending یا approved هست → عکس آلبوم رو فوروارد کن
            if user_id in db.get("approved", {}) or user_id in db.get("pending", {}):
                fwd = forward_message(OWNER_ID, chat_id, message_id)
                if fwd.get("ok"):
                    print(f"📤 عکس آلبوم فوروارد شد: {message_id}")
                return
            
            send_message(chat_id,
                "⚠️ **داش چیکار می‌کنی؟**\n\n"
                "اول دکمه «🎁 دریافت سکه 🤩» رو بزن،\n"
                "بعد عکس بفرست!",
                main_keyboard(user_id))
            return

        # ─── عکس اول: ذخیره + فوروارد + پیام تایید ───
        db.setdefault("pending", {})[user_id] = {
            "name": name,
            "username": username,
            "photo_id": message_id,
            "chat_id": chat_id,
            "time": str(datetime.now())
        }
        db["stats"]["total_claims"] += 1
        save_db(db)

        user_states.pop(user_id, None)

        send_message(chat_id,
            "⏳ **در حال بررسی توسط ادمین...**\n\n"
            "📸 عکست دریافت شد!\n"
            "🔍 ادمین داره بررسی می‌کنه",
            main_keyboard(user_id))

        fwd = forward_message(OWNER_ID, chat_id, message_id)

        if fwd.get("ok"):
            info_text = (
                f"📸 **درخواست جدید!**\n\n"
                f"👤 **نام:** {name}\n"
                f"🆔 **آیدی:** `{user_id}`\n"
                f"📱 **یوزرنیم:** @{username if username else 'ندارد'}\n"
                f"🎁 **جایزه:** {REWARD_COINS} سکه\n\n"
                f"📌 **تایید یا رد کن:**"
            )
            send_message(OWNER_ID, info_text, admin_approve_keyboard(user_id))
        else:
            print(f"❌ خطا فوروارد: {fwd}")

    except Exception as e:
        print(f"❌ خطا handle_photo: {e}")

# ═══════════════════════════════════
# 🎯 پردازش Callback
# ═══════════════════════════════════
def handle_callback(callback):
    global user_states, db
    try:
        callback_id = callback["id"]
        data = callback["data"]
        user_id = str(callback["from"]["id"])
        name = callback["from"].get("first_name", "کاربر")
        message = callback.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")

        print(f"🔘 کلیک: {data} از {user_id}")

        db = load_db()

        # ─── دریافت سکه ───
        if data == "get_coin":
            answer_callback(callback_id)
            user_states[user_id] = {"step": "waiting_photo"}
            
            # پیام اول: متن تبلیغ
            first_msg = send_message(chat_id,
                f"🔥 **داش یه ربات پیدا کردم**\n"
                f"👥 عضو بگیر و سین‌زن، خیلی خفنه!\n"
                f"💰 اونم کاملاً رایگان!\n\n"
                f"🚀 **کانالتو بترکون!**\n"
                f"با این ربات خفن 👇\n\n"
                f"⚡ عضوگیر + سین‌زن حرفه‌ای بله\n\n"
                f"╭┈┈┈┈┈┈┈┈┈┈┈┈┈┈╮\n"
                f"┊  🤖  ┊ {BOT_TO_ADVERTISE}  ┊\n"
                f"╰┈┈┈┈┈┈┈┈┈┈┈┈┈┈╯"
            )
            
            # پیام دوم: ریپلای با شرط
            if first_msg and first_msg.get("ok"):
                first_msg_id = first_msg["result"]["message_id"]
                
                send_message(chat_id,
                    f"📌 **شرط دریافت سکه:**\n"
                    f"1️⃣ پیام بالا رو برای **{REQUIRED_COUNT} نفر** بفرست\n"
                    f"2️⃣ ازش **عکس (اسکرین‌شات)** بگیر\n"
                    f"3️⃣ اینجا بفرست تا سکه بگیری!",
                    send_photo_keyboard(),
                    reply_to_message_id=first_msg_id
                )
            return

        # ─── راهنما ───
        if data == "help":
            answer_callback(callback_id)
            send_message(chat_id,
                "📖 **راهنمای هایپر تبلیغ**\n\n"
                "━━━━━━━━━━━━━━━━━\n"
                f"🎁 **دریافت {REWARD_COINS} سکه**\n\n"
                f"1️⃣ دکمه «🎁 دریافت {REWARD_COINS} سکه 🤩»\n"
                f"2️⃣ پیام رو برای {REQUIRED_COUNT} نفر بفرست\n"
                f"3️⃣ عکس (اسکرین‌شات) بگیر\n"
                f"4️⃣ دکمه «📸 فرستادن عکس»\n"
                f"5️⃣ عکس رو بفرست\n"
                f"6️⃣ منتظر تایید ادمین باش\n"
                f"7️⃣ سکه‌هات اضافه میشه ✅\n"
                f"━━━━━━━━━━━━━━━━━\n\n"
                f"⚠️ **نکات:**\n"
                f"• هر کاربر فقط یک بار\n"
                f"• عکس باید واضح باشه\n"
                f"• تقلب = بن ❌",
                main_keyboard(user_id))
            return

        # ─── پنل مالک ───
        if data == "owner_panel" and user_id == OWNER_ID:
            answer_callback(callback_id)
            send_message(chat_id, "👑 **پنل مالک**", owner_keyboard())
            return

        if data == "stats" and user_id == OWNER_ID:
            answer_callback(callback_id)
            send_message(chat_id,
                f"📊 **آمار کل**\n\n"
                f"👥 کاربران: {len(db['users'])}\n"
                f"⏳ در انتظار: {len(db['pending'])}\n"
                f"✅ تایید شده: {len(db['approved'])}\n"
                f"🎁 کل دریافت: {db['stats']['total_claims']}",
                owner_keyboard())
            return

        if data == "pending_list" and user_id == OWNER_ID:
            answer_callback(callback_id)
            pending = db.get("pending", {})
            if not pending:
                send_message(chat_id, "✅ هیچ درخواستی نیست!", owner_keyboard())
                return
            msg_text = f"⏳ **در انتظار ({len(pending)}):**\n\n"
            for uid in list(pending.keys()):
                msg_text += f"🆔 `{uid}`\n"
            send_message(chat_id, msg_text, owner_keyboard())
            return

        if data == "back_main":
            answer_callback(callback_id)
            send_message(chat_id, "🏠 منوی اصلی:", main_keyboard(user_id))
            return

        # ─── لغو ───
        if data == "cancel":
            answer_callback(callback_id, "❌ لغو شد")
            user_states.pop(user_id, None)
            send_message(chat_id, "🏠 منوی اصلی:", main_keyboard(user_id))
            return

        # ─── فرستادن عکس ───
        if data == "send_photo":
            answer_callback(callback_id, "📸 منتظر عکستم...")
            send_message(chat_id,
                "📸 **حالا عکس رو بفرست!**\n\n"
                f"⚠️ باید واضح باشه که برای {REQUIRED_COUNT} نفر فرستادی",
                cancel_keyboard())
            user_states[user_id] = {"step": "waiting_photo"}
            return

        # ─── تایید ───
        if data.startswith("approve_") and user_id == OWNER_ID:
            target_id = data.replace("approve_", "")
            answer_callback(callback_id, "✅ تایید شد!", alert=True)

            if target_id in db.get("pending", {}):
                pending_data = db["pending"].pop(target_id)
                target_chat = pending_data.get("chat_id")
                
                if str(target_id) not in db["users"]:
                    db["users"][str(target_id)] = {"name": pending_data.get("name", ""), "coins": 0}
                
                db["users"][str(target_id)]["coins"] = db["users"][str(target_id)].get("coins", 0) + REWARD_COINS
                db["users"][str(target_id)]["claims"] = db["users"][str(target_id)].get("claims", 0) + 1
                db.setdefault("approved", {})[str(target_id)] = pending_data
                db["stats"]["total_approved"] += 1
                save_db(db)

                send_message(target_chat,
                    f"🎉 **سکه برای شما انتقال داده شد!**\n\n"
                    f"💰 **{REWARD_COINS} سکه** به حساب شما اضافه شد!",
                    main_keyboard(target_id))

                edit_message(chat_id, message_id,
                    f"✅ **تایید شد!**\n\n"
                    f"🆔 کاربر: `{target_id}`\n"
                    f"💰 جایزه: {REWARD_COINS} سکه")
            return

        # ─── رد ───
        if data.startswith("reject_") and user_id == OWNER_ID:
            target_id = data.replace("reject_", "")
            answer_callback(callback_id, "❌ رد شد!", alert=True)

            if target_id in db.get("pending", {}):
                pending_data = db["pending"].pop(target_id)
                target_chat = pending_data.get("chat_id")
                save_db(db)

                send_message(target_chat,
                    f"❌ **درخواستت رد شد!**\n\n"
                    f"💡 می‌تونی دوباره تلاش کنی!",
                    main_keyboard(target_id))

                edit_message(chat_id, message_id,
                    f"❌ **رد شد!**\n\n"
                    f"🆔 کاربر: `{target_id}`")
            return

    except Exception as e:
        print(f"❌ خطا callback: {e}")

# ═══════════════════════════════════
# 🚀 حلقه اصلی (Thread)
# ═══════════════════════════════════
def bot_loop():
    global db
    last_update_id = 0
    
    print("🤖 ربات شروع شد!")
    
    try:
        me = requests.get(f"{BASE_URL}/getMe", timeout=10).json()
        if me.get("ok"):
            print(f"✅ متصل شد به: @{me['result'].get('username', '?')}")
        else:
            print(f"❌ خطا: {me}")
            return
    except Exception as e:
        print(f"❌ خطا تست: {e}")
        return
    
    while True:
        try:
            r = requests.get(
                f"{BASE_URL}/getUpdates",
                params={"offset": last_update_id + 1, "timeout": 25},
                timeout=30
            )
            data = r.json()
            
            if data.get("ok") and data.get("result"):
                for update in data["result"]:
                    last_update_id = update["update_id"]
                    try:
                        if "message" in update:
                            msg = update["message"]
                            if "photo" in msg:
                                handle_photo(msg)
                            else:
                                handle_message(msg)
                        elif "callback_query" in update:
                            handle_callback(update["callback_query"])
                    except Exception as e:
                        print(f"❌ خطا پردازش: {e}")
                        
        except Exception as e:
            print(f"❌ خطا حلقه: {e}")
            time.sleep(3)

# ═══════════════════════════════════
# 🚀 اجرا (Railway)
# ═══════════════════════════════════
if __name__ == "__main__":
    print("═" * 40)
    print("🤖 هایپر تبلیغ")
    print(f"💰 جایزه: {REWARD_COINS} سکه")
    print(f"👥 تعداد لازم: {REQUIRED_COUNT} نفر")
    print("═" * 40)

    # اجرای bot_loop در thread جدا
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()

    # اجرای Flask (keep-alive)
    port = int(os.getenv("PORT", 5000))
    print(f"🌐 Web server on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)