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
                    is_monitoring INTEGER DEFAULT 0,
                    is_live INTEGER DEFAULT 0,
                    UNIQUE(source_id, target_id)
                )
            """)
            # Migration
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_monitoring INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_live INTEGER DEFAULT 0")
            except: pass
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
            # Migration check: Ensure pair_id exists if table was old
            try:
                c.execute("ALTER TABLE collected_media ADD COLUMN pair_id INTEGER")
            except: pass 
            try:
                c.execute("ALTER TABLE collected_media ADD COLUMN source_message_id BIGINT")
            except: pass
        else:
            # SQLite
            c.execute("""
                CREATE TABLE IF NOT EXISTS target_pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id BIGINT,
                    target_id BIGINT,
                    source_title TEXT,
                    target_title TEXT,
                    is_monitoring INTEGER DEFAULT 0,
                    is_live INTEGER DEFAULT 0,
                    UNIQUE(source_id, target_id)
                )
            """)
            # Migration check
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_monitoring INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_live INTEGER DEFAULT 0")
            except: pass
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
            # Migration check
            try:
                c.execute("ALTER TABLE collected_media ADD COLUMN pair_id INTEGER")
            except: pass
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
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                "INSERT INTO target_pairs (source_id, target_id, source_title, target_title) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (sid, tid, s_title, t_title)
            )
        else:
            c.execute(
                "INSERT INTO target_pairs (source_id, target_id, source_title, target_title) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (sid, tid, s_title, t_title)
            )

def get_target_pairs():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live FROM target_pairs")
        return c.fetchall()

def get_target_pair(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live FROM target_pairs WHERE id = {p}", (pair_id,))
        return c.fetchone()

def get_pair_stats(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT COUNT(*), SUM(CASE WHEN released = 0 THEN 1 ELSE 0 END) FROM collected_media WHERE pair_id = {p}", (pair_id,))
        row = c.fetchone()
        return {"total": row[0] or 0, "pending": row[1] or 0}

# -----------------------------
# Global State
# -----------------------------
bot = telebot.TeleBot(BOT_TOKEN)
userbot = None

admin_states = {}
login_data = {} # Temporary storage for login steps
running_tasks = {} # Track long-running tasks for cancellation: { "hist_1": True, "coll_1": True }

def stop_task(task_key):
    if task_key in running_tasks:
        running_tasks[task_key] = False
        return True
    return False

def is_task_running(task_key):
    return running_tasks.get(task_key, False)

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
    for pid, sid, tid, s_title, t_title, is_mon, is_live in pairs:
        stats = get_pair_stats(pid)
        mon_status = "👁️" if is_mon else ""
        live_status = "⚡" if is_live else ""
        btn_text = f"📁 {s_title} ➔ {t_title} {mon_status}{live_status} ({stats['pending']})"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"pair_view_{pid}"))
    
    markup.add(InlineKeyboardButton("➕ Add New Pair", callback_data="pair_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

def pair_view_markup(pair_id):
    pair = get_target_pair(pair_id)
    if not pair: return InlineKeyboardMarkup()
    
    pid, sid, tid, s_title, t_title, is_mon, is_live = pair
    markup = InlineKeyboardMarkup(row_width=2)
    
    mon_btn = "🛑 Stop Monitor" if is_mon else "👁️ Monitor"
    live_btn = "🛑 Stop Live" if is_live else "⚡ Live Forward"
    
    markup.add(
        InlineKeyboardButton(mon_btn, callback_data=f"pair_toggle_mon_{pair_id}"),
        InlineKeyboardButton(live_btn, callback_data=f"pair_toggle_live_{pair_id}")
    )
    
    # Check if a manual task is running
    is_hist = is_task_running(f"hist_{pair_id}")
    is_coll = is_task_running(f"coll_{pair_id}")
    is_rel = is_task_running(f"rel_{pair_id}")
    
    if is_hist: markup.add(InlineKeyboardButton("🛑 Stop History Scrape", callback_data=f"pair_stop_task_hist_{pair_id}"))
    else: markup.add(InlineKeyboardButton("📜 History Scraper", callback_data=f"pair_hist_menu_{pair_id}"))
    
    if is_coll: markup.add(InlineKeyboardButton("🛑 Stop Collection", callback_data=f"pair_stop_task_coll_{pair_id}"))
    else: markup.add(InlineKeyboardButton("📥 Collect Now", callback_data=f"pair_collect_{pair_id}"))
    
    if is_rel: markup.add(InlineKeyboardButton("🛑 Stop Release", callback_data=f"pair_stop_task_rel_{pair_id}"))
    else: markup.add(InlineKeyboardButton("🚀 Release Now", callback_data=f"pair_release_{pair_id}"))

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

async def get_chat_selection_markup(prefix, page=0):
    markup = InlineKeyboardMarkup(row_width=1)
    if not userbot or not userbot.is_connected:
        return None
    
    chats = []
    async for dialog in userbot.get_dialogs():
        if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.CHANNEL]:
            chats.append(dialog.chat)
    
    # Pagination (10 per page)
    start = page * 10
    end = start + 10
    for chat in chats[start:end]:
        title = chat.title or "Untitled"
        markup.add(InlineKeyboardButton(f"{title}", callback_data=f"{prefix}_{chat.id}"))
    
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{prefix}_page_{page-1}"))
    if end < len(chats): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_page_{page+1}"))
    if nav: markup.add(*nav)
    
    markup.add(InlineKeyboardButton("🔙 Cancel", callback_data="pairs_main"))
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
        # Register automation handlers
        await setup_automation_handlers(userbot)
        # Set a heartbeat/me check
        await userbot.get_me()
        return True, "Userbot started successfully"
    except Exception as e:
        logger.error(f"Userbot start failed: {e}")
        return False, str(e)

async def setup_automation_handlers(client: Client):
    @client.on_message()
    async def auto_handler(c, m):
        # Fetch active pairs
        pairs = get_target_pairs()
        for pid, sid, tid, s_title, t_title, is_mon, is_live in pairs:
            # We match numeric IDs
            if str(m.chat.id) == str(sid):
                # 1) Monitor: Save to DB if monitoring is ON
                if is_mon:
                    if m.media:
                        m_type = m.media.value
                        with db_conn() as conn:
                            db_c = conn.cursor()
                            p = get_placeholder()
                            if DATABASE_URL:
                                db_c.execute("INSERT INTO collected_media (pair_id, source_message_id, media_type, caption) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (pid, m.id, m_type, m.caption or ""))
                            else:
                                db_c.execute("INSERT OR IGNORE INTO collected_media (pair_id, source_message_id, media_type, caption) VALUES (?, ?, ?, ?)", (pid, m.id, m_type, m.caption or ""))
                
                # 2) Live Forward: Copy message to target if live is ON
                if is_live:
                    try:
                        await m.copy(tid)
                    except Exception as e:
                        logger.error(f"Live Forward Error for Pair {pid}: {e}")

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

@bot.message_handler(commands=["list"])
def cmd_list(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    if not userbot or not userbot.is_connected:
        bot.send_message(message.chat.id, "❌ Userbot is not connected. Use /start to connect first.")
        return

    status_msg = bot.send_message(message.chat.id, "🔍 Fetching your chats...")
    
    async def fetch_and_list():
        try:
            text = "📋 **Your Groups & Channels**\n\n"
            async for dialog in userbot.get_dialogs(limit=50):
                if dialog.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP, enums.ChatType.CHANNEL]:
                    chat_type = "📢 Channel" if dialog.chat.type == enums.ChatType.CHANNEL else "👥 Group"
                    title = dialog.chat.title or "Untitled"
                    text += f"{chat_type}: `{title}`\nID: `{dialog.chat.id}`\n\n"
            
            if len(text) > 4000:
                parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for p in parts: bot.send_message(message.chat.id, p, parse_mode="Markdown")
            else:
                bot.send_message(message.chat.id, text, parse_mode="Markdown")
            
            try: bot.delete_message(message.chat.id, status_msg.message_id)
            except: pass
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Error fetching chats: {e}")

    asyncio.run_coroutine_threadsafe(fetch_and_list(), loop)

@bot.message_handler(commands=["logout"])
def cmd_logout(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Yes, Logout", callback_data="user_logout_do"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="dash_main"))
    bot.send_message(message.chat.id, "⚠️ **Logout Confirmation**\n\nThis will stop the userbot and delete the session from the database. Are you sure?", reply_markup=markup, parse_mode="Markdown")

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
        async def show_src_list():
            markup = await get_chat_selection_markup("sel_src", 0)
            if markup:
                bot.edit_message_text("🎯 **Select Source Chat**\nChoose the group or channel to collect from:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            else:
                bot.send_message(call.message.chat.id, "❌ Userbot not connected.")
        asyncio.run_coroutine_threadsafe(show_src_list(), loop)

    elif data.startswith("sel_src_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_src_list():
                markup = await get_chat_selection_markup("sel_src", page)
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_src_list(), loop)
        else:
            sid = int(parts[2])
            login_data[uid] = {"source_id": sid}
            async def show_tgt_list():
                markup = await get_chat_selection_markup("sel_tgt", 0)
                bot.edit_message_text("🎯 **Select Target Chat**\nChoose the group or channel to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            asyncio.run_coroutine_threadsafe(show_tgt_list(), loop)

    elif data.startswith("sel_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_tgt_list():
                markup = await get_chat_selection_markup("sel_tgt", page)
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_tgt_list(), loop)
        else:
            tid = int(parts[2])
            sid = login_data[uid]["source_id"]
            bot.edit_message_text("⏳ Resolving pair...", call.message.chat.id, call.message.message_id)
            
            async def finalize_pair():
                try:
                    s_chat = await userbot.get_chat(sid)
                    t_chat = await userbot.get_chat(tid)
                    s_title = s_chat.title or s_chat.first_name or str(sid)
                    t_title = t_chat.title or t_chat.first_name or str(tid)
                    add_target_pair(sid, tid, s_title, t_title)
                    bot.send_message(call.message.chat.id, f"✅ **Pair Added!**\n`{s_title}` ➔ `{t_title}`", parse_mode="Markdown")
                    bot.send_message(call.message.chat.id, "🎯 **Target Pairs**", reply_markup=pairs_list_markup())
                except Exception as e:
                    bot.send_message(call.message.chat.id, f"❌ Pair error: {e}")
            asyncio.run_coroutine_threadsafe(finalize_pair(), loop)

    elif data.startswith("pair_view_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        
        if not row:
            bot.answer_callback_query(call.id, "Pair not found.")
            return
            
        pid, sid, tid, s_title, t_title, is_mon, is_live = row
        stats = get_pair_stats(pid)
        
        mon_status = "🟢 Monitoring" if is_mon else "⚪️ Idle"
        live_status = "🟢 Live Forwarding" if is_live else "⚪️ Idle"
        
        text = (
            f"📁 **Pair Management**\n\n"
            f"Source: `{s_title}` (`{sid}`)\n"
            f"Target: `{t_title}` (`{tid}`)\n\n"
            f"📊 Collected: `{stats['total']}`\n"
            f"📥 Pending: `{stats['pending']}`\n\n"
            f"🤖 **Automation Status:**\n"
            f"Monitor: `{mon_status}`\n"
            f"Live: `{live_status}`"
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=pair_view_markup(pid), parse_mode="Markdown")

    elif data.startswith("pair_toggle_mon_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        new_val = 0 if row[5] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_monitoring = {p} WHERE id = {p}", (new_val, pid))
        
        if new_val == 1:
            bot.send_message(call.message.chat.id, "👁️ Monitoring Started! Initializing full history scan in background...")
            asyncio.run_coroutine_threadsafe(run_collection(call.message.chat.id, pid, limit=None), loop)
        
        bot.answer_callback_query(call.id, f"Monitor {'Started' if new_val else 'Stopped'}")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': f"pair_view_{pid}", 'message': call.message, 'id': call.id}))

    elif data.startswith("pair_toggle_live_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        new_val = 0 if row[6] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_live = {p} WHERE id = {p}", (new_val, pid))
        bot.answer_callback_query(call.id, f"Live Forward {'Started' if new_val else 'Stopped'}")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': f"pair_view_{pid}", 'message': call.message, 'id': call.id}))

    elif data.startswith("pair_hist_menu_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        admin_states[uid] = f"hist_setup_count_{pid}"
        bot.send_message(call.message.chat.id, "📜 **History Scraper**\n\nHow many messages would you like to scrape from this chat?\n(Send a number, e.g., `500`)")

    elif data.startswith("pair_stop_task_"):
        parts = data.split("_")
        type_str = parts[3]
        pid = int(parts[4])
        task_key = f"{type_str}_{pid}"
        if stop_task(task_key):
            bot.answer_callback_query(call.id, f"Stopping {type_str}...")
        else:
            bot.answer_callback_query(call.id, "No active task found.")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': f"pair_view_{pid}", 'message': call.message, 'id': call.id}))

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
            p = get_placeholder()
            c.execute(f"DELETE FROM target_pairs WHERE id = {p}", (pid,))
            c.execute(f"DELETE FROM collected_media WHERE pair_id = {p}", (pid,))
        bot.answer_callback_query(call.id, "Pair Deleted")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "pairs_main", 'message': call.message, 'id': call.id}))

    elif data == "user_logout_do":
        if userbot:
            async def stop_ub():
                try: await userbot.stop()
                except: pass
            asyncio.run_coroutine_threadsafe(stop_ub(), loop)
        
        userbot = None
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            if DATABASE_URL:
                c.execute("DELETE FROM settings WHERE key IN ('session_string', 'api_id', 'api_hash')")
            else:
                c.execute("DELETE FROM settings WHERE key IN ('session_string', 'api_id', 'api_hash')")
        
        bot.answer_callback_query(call.id, "Session Cleared")
        bot.edit_message_text("✅ **Userbot Logged Out Successfully**\nSession deleted.", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
        bot.send_message(call.message.chat.id, get_dashboard_text(), reply_markup=get_dashboard_markup(), parse_mode="Markdown")

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
        bot.send_message(message.chat.id, "⏳ Verifying OTP...")
        async def verify_otp_task():
            client = login_data[uid].get("client")
            try:
                await client.sign_in(phone_number=login_data[uid]["phone"], phone_code_hash=login_data[uid]["phone_code_hash"], phone_code=otp)
                bot.send_message(message.chat.id, "✅ OTP Verified! Finalizing setup...")
                await complete_login(uid, client, message.chat.id)
            except SessionPasswordNeeded:
                admin_states[uid] = "awaiting_password"
                bot.send_message(message.chat.id, "🔐 Step 5: Please send your **Cloud Password**.", parse_mode="Markdown")
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ OTP Error: {e}")
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

    # --- History Scraper Flow ---
    elif state.startswith("hist_setup_count_"):
        pid = int(state.split("_")[-1])
        if not text.isdigit():
            bot.reply_to(message, "Please send a number.")
            return
        login_data[uid] = {"hist_count": int(text)}
        admin_states[uid] = f"hist_setup_start_date_{pid}"
        bot.send_message(message.chat.id, "📍 Enter **Start Date** (DD/MM/YYYY):\n(Example: `01/01/2024`)")

    elif state.startswith("hist_setup_start_date_"):
        pid = int(state.split("_")[-1])
        try:
            dt = datetime.strptime(text, "%d/%m/%Y").replace(tzinfo=timezone.utc)
            login_data[uid]["start_date"] = dt
            admin_states[uid] = f"hist_setup_end_date_{pid}"
            bot.send_message(message.chat.id, "📍 Enter **End Date** (DD/MM/YYYY):\n(Example: `31/12/2024`)")
        except ValueError:
            bot.reply_to(message, "Invalid format. Use DD/MM/YYYY.")

    elif state.startswith("hist_setup_end_date_"):
        pid = int(state.split("_")[-1])
        try:
            dt = datetime.strptime(text, "%d/%m/%Y").replace(tzinfo=timezone.utc)
            count = login_data[uid]["hist_count"]
            start_date = login_data[uid]["start_date"]
            end_date = dt
            admin_states.pop(uid)
            login_data.pop(uid)
            bot.send_message(message.chat.id, f"🚀 **Scrape Started**\nPair: `{pid}` | Max: `{count}`\nRange: `{start_date.date()}` to `{end_date.date()}`")
            asyncio.run_coroutine_threadsafe(run_history_scrape(message.chat.id, pid, count, start_date, end_date), loop)
        except ValueError:
            bot.reply_to(message, "Invalid format. Use DD/MM/YYYY.")

async def run_history_scrape(admin_chat_id, pair_id, limit, start_date, end_date):
    if not userbot: return
    task_key = f"hist_{pair_id}"
    running_tasks[task_key] = True
    
    pair = get_target_pair(pair_id)
    if not pair: return
    pid, sid, tid, s_title, t_title, is_mon, is_live = pair
    
    collected = 0
    scanned = 0
    status_msg = bot.send_message(admin_chat_id, f"📜 Scraping `{s_title}` history...")
    
    try:
        async for m in userbot.get_chat_history(sid):
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 History scrape for `{s_title}` stopped by user.")
                break
                
            scanned += 1
            if m.date > end_date: continue
            if m.date < start_date: break # Assuming history is newest to oldest
            
            if m.media:
                media_type = m.media.value
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    if DATABASE_URL:
                        c.execute(
                            "INSERT INTO collected_media (pair_id, source_message_id, media_type, caption) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (pair_id, m.id, media_type, m.caption or "")
                        )
                    else:
                        c.execute(
                            "INSERT OR IGNORE INTO collected_media (pair_id, source_message_id, media_type, caption) VALUES (?, ?, ?, ?)",
                            (pair_id, m.id, media_type, m.caption or "")
                        )
                    if c.rowcount > 0: collected += 1
            
            if collected >= limit: break
            if scanned % 100 == 0:
                try: bot.edit_message_text(f"📜 Scraping `{s_title}`...\nScanned: `{scanned}`\nCollected: `{collected}/{limit}`", admin_chat_id, status_msg.message_id)
                except: pass
        
        bot.send_message(admin_chat_id, f"✅ History Scrape Done: `{s_title}`\nCollected: `{collected}`")
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Scrape Error: {e}")
    finally:
        running_tasks.pop(task_key, None)

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

async def run_collection(admin_chat_id, pair_id, limit=300):
    if not userbot: return
    task_key = f"coll_{pair_id}"
    running_tasks[task_key] = True
    
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT source_id, source_title FROM target_pairs WHERE id = {p}", (pair_id,))
        row = c.fetchone()
    
    if not row: return
    sid, title = row
    collected = 0
    scanned = 0
    limit_text = f"`{limit}`" if limit else "all"
    status_msg = bot.send_message(admin_chat_id, f"📥 Collecting {limit_text} from `{title}`...")
    
    try:
        async for m in userbot.get_chat_history(sid, limit=limit):
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 Collection for `{title}` stopped by user.")
                break
            scanned += 1
            if m.media:
                media_type = m.media.value
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    if DATABASE_URL:
                        c.execute(
                            "INSERT INTO collected_media (pair_id, source_message_id, media_type, caption) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (pair_id, m.id, media_type, m.caption or "")
                        )
                    else:
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
    finally:
        running_tasks.pop(task_key, None)

async def run_release(admin_chat_id, pair_id):
    if not userbot: return
    task_key = f"rel_{pair_id}"
    running_tasks[task_key] = True
    
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT source_id, target_id FROM target_pairs WHERE id = {p}", (pair_id,))
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
        p = get_placeholder()
        c.execute(f"SELECT id, source_message_id FROM collected_media WHERE pair_id = {p} AND released = 0", (pair_id,))
        items = c.fetchall()
    
    if not items:
        bot.send_message(admin_chat_id, "No pending items to release.")
        return

    sent = 0
    status_msg = bot.send_message(admin_chat_id, f"🚀 Releasing `{len(items)}` items...")
    
    for row_id, smid in items:
        if not running_tasks.get(task_key):
            bot.send_message(admin_chat_id, f"🛑 Release stopped by user.")
            break
        try:
            try: await userbot.copy_message(target_id, sid, smid)
            except: await userbot.forward_messages(target_id, sid, smid)
            
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                c.execute(f"UPDATE collected_media SET released = 1 WHERE id = {p}", (row_id,))
            sent += 1
            if sent % 5 == 0:
                try: bot.edit_message_text(f"🚀 Releasing...\nSent: `{sent}/{len(items)}`", admin_chat_id, status_msg.message_id)
                except: pass
            await asyncio.sleep(0.8)
        except Exception as e:
            logger.error(f"Release error: {e}")
            
    bot.send_message(admin_chat_id, f"✅ Release Complete: Sent `{sent}` items.")
    running_tasks.pop(task_key, None)

# -----------------------------
# Watchdog
# -----------------------------
async def userbot_watchdog():
    """
    Periodically check if the userbot session is still valid.
    If banned or deactivated, clear session and notify admin.
    """
    while True:
        global userbot
        if userbot and userbot.is_connected:
            try:
                await userbot.get_me()
            except Exception as e:
                err_msg = str(e).lower()
                # Common errors for banned/nuked accounts
                if "deactivated" in err_msg or "authorized" in err_msg or "auth_key" in err_msg:
                    logger.warning(f"WATCHDOG: Userbot session invalid: {e}")
                    try: await userbot.stop()
                    except: pass
                    userbot = None
                    
                    # Clear session from DB
                    with db_conn() as conn:
                        c = conn.cursor()
                        c.execute("DELETE FROM settings WHERE key IN ('session_string', 'api_id', 'api_hash')")
                    
                    bot.send_message(ADMIN_ID, f"⚠️ **USERBOT SESSION EXPIRED/BANNED**\n\nThe account has been deactivated or unauthorized. Session has been cleared.\nError: `{e}`", parse_mode="Markdown")
                else:
                    logger.error(f"WATCHDOG: Unexpected error: {e}")
        
        await asyncio.sleep(1800) # Check every 30 minutes

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
                # Use a random param to avoid cache
                requests.get(f"{url}?t={int(time.time())}", timeout=12)
            else:
                logger.info("KEEP_ALIVE: Public URL not detected yet; skipping ping cycle.")
        except Exception as e:
            logger.warning(f"KEEP_ALIVE: Ping failed: {e}")
        finally:
            time.sleep(300) # 5 minutes (Render sleep is 15 mins)

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
    # Start web server IMMEDIATELY for Render health checks
    logger.info(f"Starting web server on port {PORT}...")
    threading.Thread(target=run_web, daemon=True).start()
    
    try:
        init_db()
    except Exception as e:
        logger.error(f"DB Init Error: {e}")

    asyncio.create_task(userbot_watchdog())
    threading.Thread(target=keep_alive_worker, daemon=True).start()

    # Try to start existing session
    try:
        ok, msg = await start_userbot()
        if ok: logger.info("✅ Userbot started")
    except Exception as e: logger.error(f"Userbot error: {e}")

    # Start telebot polling with AUTO-RESTART
    def run_polling():
        while True:
            try:
                logger.info("🚀 Starting Admin Bot polling...")
                bot.remove_webhook()
                bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=60)
            except Exception as e:
                logger.error(f"❌ Polling crashed: {e}. Restarting in 10s...")
                time.sleep(10)
    
    threading.Thread(target=run_polling, daemon=True).start()
    logger.info("✨ Admin bot monitor started")
    
    await idle()

if __name__ == "__main__":
    loop.run_until_complete(main())
