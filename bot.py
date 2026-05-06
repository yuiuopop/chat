import os
import asyncio
import threading
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import requests
from flask import Flask
from dotenv import load_dotenv

# Pyrogram sync import needs a current event loop on Python 3.10+
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from pyrogram import Client, filters, idle, enums
from pyrogram.types import Message
from pyrogram.errors import RPCError, SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

load_dotenv()

# -----------------------------
# Config
# -----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN:
    print("WARNING: Missing BOT_TOKEN in .env. Admin features will be limited until set.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("userbot_v2")

# -----------------------------
# DB (SQLite/PostgreSQL)
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = "userbot_v2.db"

@contextmanager
def db_conn():
    if DATABASE_URL:
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
    else:
        conn = sqlite3.connect(DB_PATH)
    
    try:
        yield conn
        if not DATABASE_URL:
            conn.commit()
    finally:
        conn.close()

def get_placeholder():
    return "%s" if DATABASE_URL else "?"

def init_db():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
    logger.info("DB initialized")

def get_setting(key, default=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT value FROM settings WHERE key = {p}", (key,))
        row = c.fetchone()
        return row[0] if row else default

def set_setting(key, value):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                """
                INSERT INTO settings (key, value) VALUES (%s, %s)
                ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, str(value))
            )
        else:
            c.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value))
            )

# -----------------------------
# Global State
# -----------------------------
bot = telebot.TeleBot(BOT_TOKEN)
userbot = None

admin_states = {}
login_data = {} # Temporary storage for login steps

# -----------------------------
# UI Helpers
# -----------------------------
def get_dashboard_text():
    is_online = userbot and userbot.is_connected
    status_emoji = "🟢" if is_online else "🔴"
    status_text = "ONLINE" if is_online else "OFFLINE"
    
    text = f"┏━━━━━━━ ⚡ SYSTEM CONSOLE ⚡ ━━━━━━━┓\n"
    text += f"┃\n"
    text += f"┃  🤖 STATUS : {status_emoji} {status_text}\n"
    text += f"┃  👤 USER   : {('@' + userbot.me.username) if (is_online and userbot.me) else 'Not Connected'}\n"
    text += f"┃\n"
    text += f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n\n"
    
    if is_online:
        text += "✅ Your userbot is active and monitoring.\n"
        text += "Use the buttons below to manage your account."
    else:
        text += "❌ Your userbot is currently disconnected.\n"
        text += "Please click the **Connect** button to start."
    
    return text

def get_dashboard_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    is_online = userbot and userbot.is_connected
    
    if is_online:
        markup.add(InlineKeyboardButton("👤 User Account", callback_data="user_acc_main"))
        markup.add(InlineKeyboardButton("🔄 Refresh Status", callback_data="dash_refresh"))
        markup.add(InlineKeyboardButton("🔴 Disconnect", callback_data="user_disconnect_confirm"))
    else:
        markup.add(InlineKeyboardButton("🔌 Connect Userbot", callback_data="user_connect_start"))
        markup.add(InlineKeyboardButton("🔄 Refresh", callback_data="dash_refresh"))
    
    return markup

def user_account_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("👥 Groups", callback_data="user_acc_list_groups_0"),
        InlineKeyboardButton("📢 Channels", callback_data="user_acc_list_channels_0")
    )
    markup.add(
        InlineKeyboardButton("👤 Private", callback_data="user_acc_list_private_0"),
        InlineKeyboardButton("🤖 Bots", callback_data="user_acc_list_bots_0")
    )
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup

# -----------------------------
# Userbot Logic
# -----------------------------
async def start_userbot():
    global userbot
    api_id = get_setting("api_id")
    api_hash = get_setting("api_hash")
    session_string = get_setting("session_string")
    
    if not (api_id and api_hash and session_string):
        return False, "Missing credentials"
    
    try:
        if userbot:
            try: await userbot.stop()
            except: pass
            
        userbot = Client(
            name="userbot_session",
            api_id=int(api_id),
            api_hash=api_hash,
            session_string=session_string,
            in_memory=True
        )
        await userbot.start()
        # Set a heartbeat/me check
        await userbot.get_me()
        return True, "Userbot started successfully"
    except Exception as e:
        logger.error(f"Userbot start failed: {e}")
        return False, str(e)

# -----------------------------
# Bot Handlers
# -----------------------------
@bot.message_handler(commands=['start', 'dash'])
def cmd_start(message):
    if message.from_user.id != ADMIN_ID:
        return
    bot.send_message(
        message.chat.id,
        get_dashboard_text(),
        reply_markup=get_dashboard_markup(),
        parse_mode="Markdown"
    )

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    global userbot
    uid = call.from_user.id
    if uid != ADMIN_ID:
        return

    data = call.data
    
    if data == "dash_refresh" or data == "dash_main":
        bot.edit_message_text(
            get_dashboard_text(),
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_dashboard_markup(),
            parse_mode="Markdown"
        )
        bot.answer_callback_query(call.id, "Dashboard updated")

    elif data == "user_connect_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_api_id"
        bot.send_message(call.message.chat.id, "Step 1: Please send your **API ID**.\n(Get it from my.telegram.org)", parse_mode="Markdown")

    elif data == "user_disconnect_confirm":
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Yes, Disconnect", callback_data="user_disconnect_do"))
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data="dash_main"))
        bot.edit_message_text("⚠️ Are you sure you want to disconnect the userbot?", call.message.chat.id, call.message.message_id, reply_markup=markup)

    elif data == "user_disconnect_do":
        if userbot:
            async def stop_ub():
                await userbot.stop()
            asyncio.run_coroutine_threadsafe(stop_ub(), loop)
        bot.answer_callback_query(call.id, "Userbot disconnected")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "dash_main", 'message': call.message, 'id': call.id}))

    elif data == "user_acc_main":
        bot.edit_message_text(
            "👤 **User Account Dashboard**\n\nBrowse and inspect the chats in your account:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=user_account_markup(),
            parse_mode="Markdown"
        )

    elif data.startswith("user_acc_list_"):
        # user_acc_list_{category}_{page}
        parts = data.split("_")
        category = parts[3]
        page = int(parts[4])
        bot.answer_callback_query(call.id, f"Loading {category}...")
        
        async def run_list():
            if not userbot:
                bot.send_message(call.message.chat.id, "❌ Userbot not running.")
                return
            
            # Fetch dialogs
            all_dialogs = []
            async for dialog in userbot.get_dialogs():
                c = dialog.chat
                if category == "groups" and c.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                    all_dialogs.append(c)
                elif category == "channels" and c.type == enums.ChatType.CHANNEL:
                    all_dialogs.append(c)
                elif category == "bots" and c.type == enums.ChatType.BOT:
                    all_dialogs.append(c)
                elif category == "private" and c.type == enums.ChatType.PRIVATE:
                    all_dialogs.append(c)
            
            # Pagination
            page_size = 8
            start = page * page_size
            end = start + page_size
            page_items = all_dialogs[start:end]
            
            markup = InlineKeyboardMarkup(row_width=1)
            for chat in page_items:
                title = chat.title or chat.first_name or str(chat.id)
                markup.add(InlineKeyboardButton(f"👁 {title}", callback_data=f"user_acc_view_{chat.id}"))
            
            # Nav buttons
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"user_acc_list_{category}_{page-1}"))
            if end < len(all_dialogs):
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"user_acc_list_{category}_{page+1}"))
            if nav:
                markup.add(*nav)
            
            markup.add(InlineKeyboardButton("🔙 Back to Categories", callback_data="user_acc_main"))
            
            msg = f"👤 **Account Browser:** {category.capitalize()}\nPage {page + 1} | Total: {len(all_dialogs)}"
            bot.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        asyncio.run_coroutine_threadsafe(run_list(), loop)

    elif data.startswith("user_acc_view_"):
        chat_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "Fetching details...")
        
        async def run_view():
            if not userbot: return
            try:
                chat = await userbot.get_chat(chat_id)
                msg_count = await userbot.get_chat_history_count(chat_id)
                title = chat.title or chat.first_name or "Unknown"
                
                info = f"📋 **Chat Details:**\n\n"
                info += f"🏷 **Title:** `{title}`\n"
                info += f"🆔 **ID:** `{chat.id}`\n"
                info += f"📂 **Type:** `{chat.type.value if hasattr(chat.type, 'value') else chat.type}`\n"
                info += f"💬 **Messages:** `{msg_count}`\n"
                if hasattr(chat, 'members_count') and chat.members_count:
                    info += f"👥 **Members:** `{chat.members_count}`\n"
                if chat.username:
                    info += f"🔗 **Username:** @{chat.username}\n"
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🔙 Back to List", callback_data=f"user_acc_main"))
                
                bot.edit_message_text(info, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

            except Exception as e:
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")

        asyncio.run_coroutine_threadsafe(run_view(), loop)

@bot.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and admin_states.get(m.from_user.id))
def handle_state_inputs(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    text = message.text.strip()
    
    if state == "awaiting_api_id":
        if not text.isdigit():
            bot.reply_to(message, "Invalid API ID. Please send a numeric ID.")
            return
        login_data[uid] = {"api_id": int(text)}
        admin_states[uid] = "awaiting_api_hash"
        bot.send_message(message.chat.id, "Step 2: Please send your **API HASH**.", parse_mode="Markdown")

    elif state == "awaiting_api_hash":
        if len(text) < 10:
            bot.reply_to(message, "Invalid API HASH.")
            return
        login_data[uid]["api_hash"] = text
        admin_states[uid] = "awaiting_phone"
        bot.send_message(message.chat.id, "Step 3: Please send your **Phone Number** (with country code).\nExample: `+1234567890`", parse_mode="Markdown")

    elif state == "awaiting_phone":
        login_data[uid]["phone"] = text
        
        # Now start the login process with Pyrogram
        bot.send_message(message.chat.id, "⏳ Sending OTP...")
        
        async def send_otp_task():
            try:
                temp_client = Client(
                    name="temp_login",
                    api_id=login_data[uid]["api_id"],
                    api_hash=login_data[uid]["api_hash"],
                    in_memory=True
                )
                await temp_client.connect()
                code_info = await temp_client.send_code(login_data[uid]["phone"])
                login_data[uid]["client"] = temp_client
                login_data[uid]["phone_code_hash"] = code_info.phone_code_hash
                
                admin_states[uid] = "awaiting_otp"
                bot.send_message(message.chat.id, "Step 4: Please send the **OTP** you received.", parse_mode="Markdown")
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Error: {e}")
                admin_states.pop(uid, None)
        
        asyncio.run_coroutine_threadsafe(send_otp_task(), loop)

    elif state == "awaiting_otp":
        otp = text.replace(" ", "")
        
        async def verify_otp_task():
            client = login_data[uid].get("client")
            try:
                await client.sign_in(
                    phone_number=login_data[uid]["phone"],
                    phone_code_hash=login_data[uid]["phone_code_hash"],
                    phone_code=otp
                )
                # Success!
                await complete_login(uid, client, message.chat.id)
            except SessionPasswordNeeded:
                admin_states[uid] = "awaiting_password"
                bot.send_message(message.chat.id, "Step 5: Your account has **2FA enabled**. Please send your password.", parse_mode="Markdown")
            except (PhoneCodeInvalid, PhoneCodeExpired):
                bot.send_message(message.chat.id, "❌ Invalid or expired OTP. Please try /start again.")
                admin_states.pop(uid, None)
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Error: {e}")
                admin_states.pop(uid, None)

        asyncio.run_coroutine_threadsafe(verify_otp_task(), loop)

    elif state == "awaiting_password":
        password = text
        
        async def verify_password_task():
            client = login_data[uid].get("client")
            try:
                await client.check_password(password)
                await complete_login(uid, client, message.chat.id)
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Password error: {e}")
                admin_states.pop(uid, None)

        asyncio.run_coroutine_threadsafe(verify_password_task(), loop)

async def complete_login(uid, client, chat_id):
    session_string = await client.export_session_string()
    set_setting("api_id", login_data[uid]["api_id"])
    set_setting("api_hash", login_data[uid]["api_hash"])
    set_setting("session_string", session_string)
    
    admin_states.pop(uid, None)
    login_data.pop(uid, None)
    
    bot.send_message(chat_id, "✅ **Userbot Connected Successfully!**", parse_mode="Markdown")
    
    # Restart the global userbot with new session
    ok, msg = await start_userbot()
    if ok:
        bot.send_message(chat_id, get_dashboard_text(), reply_markup=get_dashboard_markup(), parse_mode="Markdown")
    else:
        bot.send_message(chat_id, f"❌ Failed to start userbot after login: {msg}")

# -----------------------------
# Health + keepalive
# -----------------------------
def keep_alive_worker():
    """
    Periodically ping this service URL to reduce idle/sleep risk on Render.
    Auto-detects URL from environment variables.
    """
    def detect_public_url() -> str:
        # 1) Render
        url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
        if url: return url.rstrip("/")
        
        # 2) Railway
        url = os.getenv("RAILWAY_STATIC_URL", "").strip()
        if url:
            if url.startswith("http"): return url.rstrip("/")
            return f"https://{url}".rstrip("/")
        
        # 3) Generic
        url = os.getenv("WEB_URL", "").strip()
        if url: return url.rstrip("/")
        
        return ""

    detected_url = ""
    while True:
        try:
            url = detect_public_url()
            if url:
                if url != detected_url:
                    detected_url = url
                    logger.info(f"KEEP_ALIVE: Detected public URL: {detected_url}")
                requests.get(url, timeout=12)
            else:
                logger.info("KEEP_ALIVE: Public URL not detected yet; skipping ping cycle.")
        except Exception as e:
            logger.warning(f"KEEP_ALIVE: Ping failed: {e}")
        finally:
            time.sleep(600) # 10 minutes

app = Flask(__name__)
@app.route("/")
def health():
    return "Userbot v2 Running", 200

def run_web():
    app.run(host="0.0.0.0", port=PORT)

# -----------------------------
# Main Loop
# -----------------------------
async def main():
    init_db()
    
    # Start Keep Alive pinger
    threading.Thread(target=keep_alive_worker, daemon=True).start()

    # Try to start existing session
    ok, msg = await start_userbot()
    if ok:
        logger.info("Userbot started from existing session")
    else:
        logger.warning(f"No existing session or start failed: {msg}")

    # Start telebot polling in thread
    threading.Thread(target=lambda: bot.infinity_polling(), daemon=True).start()
    logger.info("Admin bot polling started")
    
    # Keep alive Flask
    threading.Thread(target=run_web, daemon=True).start()
    
    await idle()

if __name__ == "__main__":
    loop.run_until_complete(main())
