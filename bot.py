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
        
        if DATABASE_URL:
            # PostgreSQL
            c.execute("""
                CREATE TABLE IF NOT EXISTS target_pairs (
                    id SERIAL PRIMARY KEY,
                    source_id BIGINT,
                    target_id BIGINT,
                    source_title TEXT,
                    target_title TEXT,
                    UNIQUE(source_id, target_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id SERIAL PRIMARY KEY,
                    pair_id INTEGER,
                    source_message_id BIGINT,
                    media_type TEXT,
                    caption TEXT,
                    released INTEGER DEFAULT 0,
                    UNIQUE(pair_id, source_message_id)
                )
            """)
        else:
            # SQLite
            c.execute("""
                CREATE TABLE IF NOT EXISTS target_pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id BIGINT,
                    target_id BIGINT,
                    source_title TEXT,
                    target_title TEXT,
                    UNIQUE(source_id, target_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id INTEGER,
                    source_message_id BIGINT,
                    media_type TEXT,
                    caption TEXT,
                    released INTEGER DEFAULT 0,
                    UNIQUE(pair_id, source_message_id)
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

def add_target_pair(sid, tid, s_title, t_title):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO target_pairs (source_id, target_id, source_title, target_title) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (sid, tid, s_title, t_title)
        )

def get_target_pairs():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, source_id, target_id, source_title, target_title FROM target_pairs")
        return c.fetchall()

def get_pair_stats(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*), SUM(CASE WHEN released = 0 THEN 1 ELSE 0 END) FROM collected_media WHERE pair_id = ?", (pair_id,))
        row = c.fetchone()
        return {"total": row[0] or 0, "pending": row[1] or 0}

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
    status = "🟢 ACTIVE" if is_online else "🔴 OFFLINE"
    
    text = f"✨ **SYSTEM CONSOLE**\n"
    text += f"Status: `{status}`\n"
    if is_online and userbot.me:
        name = userbot.me.first_name or "User"
        text += f"Account: `{name}`\n"
    
    text += "\n_Manage your automation pairs below:_"
    return text

def get_dashboard_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    is_online = userbot and userbot.is_connected
    
    if is_online:
        markup.add(InlineKeyboardButton("🎯 Target Pairs", callback_data="pairs_main"))
        markup.add(InlineKeyboardButton("👤 User Account", callback_data="user_acc_main"))
    else:
        markup.add(InlineKeyboardButton("🔌 Connect Userbot", callback_data="user_connect_start"))
    
    return markup

def pairs_list_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    pairs = get_target_pairs()
    for pid, sid, tid, s_title, t_title in pairs:
        stats = get_pair_stats(pid)
        btn_text = f"📁 {s_title} ➔ {t_title} ({stats['pending']})"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"pair_view_{pid}"))
    
    markup.add(InlineKeyboardButton("➕ Add New Pair", callback_data="pair_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

def pair_view_markup(pair_id):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("📥 Collect", callback_data=f"pair_collect_{pair_id}"),
        InlineKeyboardButton("🚀 Release", callback_data=f"pair_release_{pair_id}")
    )
    markup.add(InlineKeyboardButton("🗑 Delete Pair", callback_data=f"pair_delete_confirm_{pair_id}"))
    markup.add(InlineKeyboardButton("🔙 Back to Pairs", callback_data="pairs_main"))
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
    
    if data == "dash_main":
        bot.edit_message_text(get_dashboard_text(), call.message.chat.id, call.message.message_id, reply_markup=get_dashboard_markup(), parse_mode="Markdown")

    elif data == "pairs_main":
        bot.edit_message_text("🎯 **Target Pairs**\nSelect a pair to manage collection or release:", call.message.chat.id, call.message.message_id, reply_markup=pairs_list_markup(), parse_mode="Markdown")

    elif data == "pair_add_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_pair_source"
        bot.send_message(call.message.chat.id, "📍 Step 1: Send the **Source Chat ID** (where content comes from).")

    elif data.startswith("pair_view_"):
        pid = int(data.split("_")[-1])
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT source_id, target_id, source_title, target_title FROM target_pairs WHERE id = ?", (pid,))
            row = c.fetchone()
        
        if not row:
            bot.answer_callback_query(call.id, "Pair not found.")
            return
            
        stats = get_pair_stats(pid)
        text = (
            f"📁 **Pair Details**\n\n"
            f"Source: `{row[2]}` (`{row[0]}`)\n"
            f"Target: `{row[3]}` (`{row[1]}`)\n\n"
            f"📊 Collected: `{stats['total']}`\n"
            f"📥 Pending: `{stats['pending']}`"
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=pair_view_markup(pid), parse_mode="Markdown")

    elif data.startswith("pair_collect_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "🚀 Starting Collection...")
        asyncio.run_coroutine_threadsafe(run_collection(call.message.chat.id, pid), loop)

    elif data.startswith("pair_release_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "🚀 Starting Release...")
        asyncio.run_coroutine_threadsafe(run_release(call.message.chat.id, pid), loop)

    elif data.startswith("pair_delete_confirm_"):
        pid = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Confirm Delete", callback_data=f"pair_delete_do_{pid}"))
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"pair_view_{pid}"))
        bot.edit_message_text("⚠️ Delete this pair and all its collected media history?", call.message.chat.id, call.message.message_id, reply_markup=markup)

    elif data.startswith("pair_delete_do_"):
        pid = int(data.split("_")[-1])
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM target_pairs WHERE id = ?", (pid,))
            c.execute("DELETE FROM collected_media WHERE pair_id = ?", (pid,))
        bot.answer_callback_query(call.id, "Pair Deleted")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "pairs_main", 'message': call.message, 'id': call.id}))

    elif data == "user_connect_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_api_id"
        bot.send_message(call.message.chat.id, "Step 1: Please send your **API ID**.\n(Get it from my.telegram.org)", parse_mode="Markdown")

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

@bot.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and admin_states.get(m.from_user.id))
def handle_state_inputs(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    text = message.text.strip()
    
    # --- Login Flow ---
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
        bot.send_message(message.chat.id, "⏳ Sending OTP...")
        async def send_otp_task():
            try:
                temp_client = Client(name="temp_login", api_id=login_data[uid]["api_id"], api_hash=login_data[uid]["api_hash"], in_memory=True)
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
                await client.sign_in(phone_number=login_data[uid]["phone"], phone_code_hash=login_data[uid]["phone_code_hash"], phone_code=otp)
                await complete_login(uid, client, message.chat.id)
            except SessionPasswordNeeded:
                admin_states[uid] = "awaiting_password"
                bot.send_message(message.chat.id, "Step 5: Your account has **2FA enabled**. Please send your password.", parse_mode="Markdown")
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Error: {e}")
                admin_states.pop(uid, None)
        asyncio.run_coroutine_threadsafe(verify_otp_task(), loop)

    elif state == "awaiting_password":
        async def verify_password_task():
            client = login_data[uid].get("client")
            try:
                await client.check_password(text)
                await complete_login(uid, client, message.chat.id)
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Password error: {e}")
                admin_states.pop(uid, None)
        asyncio.run_coroutine_threadsafe(verify_password_task(), loop)

    # --- Target Pair Flow ---
    elif state == "awaiting_pair_source":
        try:
            sid = int(text)
            login_data[uid] = {"source_id": sid}
            admin_states[uid] = "awaiting_pair_target"
            bot.send_message(message.chat.id, "🎯 Step 2: Send the **Target Chat ID** (where content goes).")
        except ValueError:
            bot.reply_to(message, "Invalid ID. Please send a numeric Chat ID.")

    elif state == "awaiting_pair_target":
        try:
            tid = int(text)
            sid = login_data[uid]["source_id"]
            bot.send_message(message.chat.id, "⏳ Resolving chat titles...")
            
            async def resolve_pair():
                try:
                    s_chat = await userbot.get_chat(sid)
                    t_chat = await userbot.get_chat(tid)
                    s_title = s_chat.title or s_chat.first_name or str(sid)
                    t_title = t_chat.title or t_chat.first_name or str(tid)
                    
                    add_target_pair(sid, tid, s_title, t_title)
                    bot.send_message(message.chat.id, f"✅ **Pair Added!**\n`{s_title}` ➔ `{t_title}`", parse_mode="Markdown")
                    admin_states.pop(uid, None)
                    bot.send_message(message.chat.id, "🎯 **Target Pairs**", reply_markup=pairs_list_markup())
                except Exception as e:
                    bot.send_message(message.chat.id, f"❌ Resolution Error: {e}\nEnsure your userbot is in both chats.")
                    admin_states.pop(uid, None)

            asyncio.run_coroutine_threadsafe(resolve_pair(), loop)
        except ValueError:
            bot.reply_to(message, "Invalid ID. Please send a numeric Chat ID.")

async def resolve_target_id(client: Client, target_ref: str):
    try:
        return await client.get_chat(target_ref)
    except Exception:
        try:
            if str(target_ref).lstrip("-").isdigit():
                return await client.get_chat(int(target_ref))
        except Exception: pass
        async for dialog in client.get_dialogs(limit=50):
            if str(dialog.chat.id) == str(target_ref) or dialog.chat.username == str(target_ref).replace("@", ""):
                return dialog.chat
    raise ValueError(f"Could not find chat: {target_ref}")

async def run_collection(admin_chat_id, pair_id):
    if not userbot: return
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT source_id, source_title FROM target_pairs WHERE id = ?", (pair_id,))
        row = c.fetchone()
    
    if not row: return
    sid, title = row
    collected = 0
    scanned = 0
    status_msg = bot.send_message(admin_chat_id, f"📥 Collecting from `{title}`...")
    
    try:
        async for m in userbot.get_chat_history(sid, limit=300):
            scanned += 1
            if m.media:
                media_type = m.media.value
                with db_conn() as conn:
                    c = conn.cursor()
                    c.execute(
                        "INSERT OR IGNORE INTO collected_media (pair_id, source_message_id, media_type, caption) VALUES (?, ?, ?, ?)",
                        (pair_id, m.id, media_type, m.caption or "")
                    )
                    if c.rowcount > 0: collected += 1
            if scanned % 100 == 0:
                try: bot.edit_message_text(f"📥 Collecting from `{title}`...\nScanned: `{scanned}`\nCollected: `{collected}`", admin_chat_id, status_msg.message_id)
                except: pass
        bot.send_message(admin_chat_id, f"✅ Collection Done: `{title}`\nNew items: `{collected}`")
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Collection Error: {e}")

async def run_release(admin_chat_id, pair_id):
    if not userbot: return
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT source_id, target_id FROM target_pairs WHERE id = ?", (pair_id,))
        row = c.fetchone()
    
    if not row: return
    sid, tid_ref = row
    
    try:
        target_chat = await resolve_target_id(userbot, tid_ref)
        target_id = target_chat.id
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Target Error: {e}")
        return

    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, source_message_id FROM collected_media WHERE pair_id = ? AND released = 0", (pair_id,))
        items = c.fetchall()
    
    if not items:
        bot.send_message(admin_chat_id, "No pending items to release.")
        return

    sent = 0
    status_msg = bot.send_message(admin_chat_id, f"🚀 Releasing `{len(items)}` items...")
    
    for row_id, smid in items:
        try:
            try: await userbot.copy_message(target_id, sid, smid)
            except: await userbot.forward_messages(target_id, sid, smid)
            
            with db_conn() as conn:
                c = conn.cursor()
                c.execute("UPDATE collected_media SET released = 1 WHERE id = ?", (row_id,))
            sent += 1
            if sent % 5 == 0:
                try: bot.edit_message_text(f"🚀 Releasing...\nSent: `{sent}/{len(items)}`", admin_chat_id, status_msg.message_id)
                except: pass
            await asyncio.sleep(0.8)
        except Exception as e:
            logger.error(f"Release error: {e}")
            
    bot.send_message(admin_chat_id, f"✅ Release Complete: Sent `{sent}` items.")

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
