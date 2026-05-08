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
                    filter_type TEXT DEFAULT 'all',
                    UNIQUE(source_id, target_id)
                )
            """)
            # Migration
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_monitoring INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_live INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN filter_type TEXT DEFAULT 'all'")
            except: pass
            
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    thread_id INTEGER,
                    media_type TEXT,
                    caption TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    released INTEGER DEFAULT 0,
                    pair_id INTEGER,
                    UNIQUE(source_chat_id, source_message_id)
                )
            """)
            # Migrations for existing tables
            try: c.execute("ALTER TABLE collected_media ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN pair_id INTEGER")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN thread_id INTEGER")
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
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN filter_type TEXT DEFAULT 'all'")
            except: pass
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    thread_id INTEGER,
                    media_type TEXT,
                    caption TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    released INTEGER DEFAULT 0,
                    pair_id INTEGER,
                    UNIQUE(source_chat_id, source_message_id)
                )
            """)
            # Migration check
            try: c.execute("ALTER TABLE collected_media ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN pair_id INTEGER")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN timestamp DATETIME DEFAULT CURRENT_TIMESTAMP")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN filter_type TEXT DEFAULT 'all'")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN thread_id INTEGER")
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
        c.execute("SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live, filter_type FROM target_pairs")
        return c.fetchall()

def get_target_pair(pid):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live, filter_type FROM target_pairs WHERE id = {p}", (pid,))
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
topic_cache = {}   # {(target_id, source_id, thread_id): target_thread_id}

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
    with db_conn() as conn:
        c = conn.cursor()
        # Single query to get pairs AND pending counts (Optimization)
        c.execute("""
            SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live, filter_type,
            (SELECT COUNT(*) FROM collected_media WHERE pair_id = target_pairs.id AND released = 0) as pending
            FROM target_pairs
        """)
        pairs = c.fetchall()
        
    for pid, sid, tid, s_title, t_title, is_mon, is_live, filter_type, pending in pairs:
        mon_status = "👁️" if is_mon else ""
        live_status = "⚡" if is_live else ""
        btn_text = f"📁 {s_title} ➔ {t_title} {mon_status}{live_status} ({pending})"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"pair_view_{pid}"))
    
    markup.add(InlineKeyboardButton("➕ Add New Pair", callback_data="pair_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

def show_pair_view(chat_id, message_id, pid):
    try:
        row = get_target_pair(pid)
        if not row:
            bot.send_message(chat_id, f"❌ Pair not found (ID: {pid}). It may have been deleted.")
            return
            
        pid, sid, tid, s_title, t_title, is_mon, is_live, filter_type = row
        stats = get_pair_stats(pid)
        
        mon_status = "🟢 Monitoring" if is_mon else "⚪️ Idle"
        live_status = "🟢 Live Forwarding" if is_live else "⚪️ Idle"
        filter_label = filter_type.upper()
        task_info = ""
        if is_task_running(f"hist_{pid}"): task_info += "\n🏃 **History Scrape:** `RUNNING`"
        if is_task_running(f"coll_{pid}"): task_info += "\n🏃 **Collection:** `RUNNING`"
        if is_task_running(f"rel_{pid}"): task_info += "\n🏃 **Release:** `RUNNING`"
        
        text = (
            f"📁 **Pair Management**\n\n"
            f"Source: `{s_title}` (`{sid}`)\n"
            f"Target: `{t_title}` (`{tid}`)\n\n"
            f"📊 Collected: `{stats['total']}`\n"
            f"📥 Pending: `{stats['pending']}`\n\n"
            f"🎯 **Filter:** `{filter_label}`\n"
            f"🤖 **Automation Status:**\n"
            f"Monitor: `{mon_status}`\n"
            f"Live: `{live_status}`"
            f"{task_info}"
        )
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=pair_view_markup(pid), parse_mode="Markdown")
        except Exception as e:
            if "message is not modified" in str(e):
                pass
            else:
                raise e
    except Exception as e:
        logger.error(f"Pair View Error: {e}")
        bot.send_message(chat_id, f"❌ Error opening pair management: {e}")

def pair_view_markup(pair_id):
    pair = get_target_pair(pair_id)
    if not pair: return InlineKeyboardMarkup()
    
    pid, sid, tid, s_title, t_title, is_mon, is_live, filter_type = pair
    markup = InlineKeyboardMarkup(row_width=2)
    
    mon_btn = "🛑 Stop Monitor" if is_mon else "👁️ Monitor"
    live_btn = "🛑 Stop Live" if is_live else "⚡ Live Forward"
    
    filter_icon = "📑"
    if filter_type == "media": filter_icon = "🖼️"
    elif filter_type == "text": filter_icon = "📝"
    
    markup.add(
        InlineKeyboardButton(mon_btn, callback_data=f"pair_toggle_mon_{pair_id}"),
        InlineKeyboardButton(live_btn, callback_data=f"pair_toggle_live_{pair_id}")
    )
    markup.add(InlineKeyboardButton(f"{filter_icon} Filter: {filter_type.upper()}", callback_data=f"pair_cycle_filter_{pair_id}"))
    
    # Check if a manual task is running
    is_hist = is_task_running(f"hist_{pair_id}")
    is_coll = is_task_running(f"coll_{pair_id}")
    is_rel = is_task_running(f"rel_{pair_id}")
    
    if is_hist: markup.add(InlineKeyboardButton("🛑 Stop History Scrape", callback_data=f"pair_stop_task_hist_{pair_id}"))
    else: markup.add(InlineKeyboardButton("📜 History Scraper", callback_data=f"pair_hist_menu_{pair_id}"))
    
    if is_coll: markup.add(InlineKeyboardButton("🛑 Stop Collection", callback_data=f"pair_stop_task_coll_{pair_id}"))
    else: markup.add(InlineKeyboardButton("📥 Collect Now", callback_data=f"pair_collect_{pair_id}"))

    markup.add(InlineKeyboardButton("🔄 Refresh Stats", callback_data=f"pair_view_{pair_id}"))
    
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

def get_type_selection_markup(prefix):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("👤 Personal", callback_data=f"{prefix}_type_private_0"),
        InlineKeyboardButton("👥 Groups", callback_data=f"{prefix}_type_group_0")
    )
    markup.add(
        InlineKeyboardButton("📢 Channels", callback_data=f"{prefix}_type_channel_0"),
        InlineKeyboardButton("🤖 Bots", callback_data=f"{prefix}_type_bot_0")
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

async def get_chat_selection_markup(prefix, category, page=0):
    markup = InlineKeyboardMarkup(row_width=1)
    if not userbot or not userbot.is_connected:
        return None
    
    chats = []
    async for dialog in userbot.get_dialogs():
        chat = dialog.chat
        if category == "private":
            if chat.type == enums.ChatType.PRIVATE and not (chat.username and chat.username.lower().endswith("bot")):
                chats.append(chat)
        elif category == "bot":
            if chat.type == enums.ChatType.PRIVATE and (chat.username and chat.username.lower().endswith("bot")):
                chats.append(chat)
        elif category == "group":
            if chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
                chats.append(chat)
        elif category == "channel":
            if chat.type == enums.ChatType.CHANNEL:
                chats.append(chat)
    
    # Pagination (10 per page)
    start = page * 10
    end = start + 10
    for chat in chats[start:end]:
        chat_type = chat.type
        if chat_type == enums.ChatType.PRIVATE:
            is_bot = chat.username and chat.username.lower().endswith("bot")
            icon = "🤖" if is_bot else "👤"
            title = f"{chat.first_name or ''} {chat.last_name or ''}".strip() or chat.username or "User"
        elif chat_type == enums.ChatType.CHANNEL:
            icon = "📢"
            title = chat.title or "Channel"
        else:
            icon = "👥"
            title = chat.title or "Group"
            
        markup.add(InlineKeyboardButton(f"{icon} {title}", callback_data=f"{prefix}_id_{chat.id}"))
    
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{prefix}_type_{category}_{page-1}"))
    if end < len(chats): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_type_{category}_{page+1}"))
    if nav: markup.add(*nav)
    
    markup.add(InlineKeyboardButton("🔙 Back to Types", callback_data=f"{prefix}_back"))
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
            in_memory=True,
            device_model="PC 64bit",
            system_version="Windows 11",
            app_version="4.11.2",
            sleep_threshold=60 # Handle long floodwaits gracefully
        )
        await userbot.start()
        # Register automation handlers
        await setup_automation_handlers(userbot)
        return True, "Userbot started"
    except Exception as e:
        return False, str(e)

async def ensure_userbot():
    """Ensures the userbot is connected and ready."""
    global userbot
    if not userbot:
        ok, msg = await start_userbot()
        if not ok: return False, msg
    
    if not userbot.is_connected:
        try: 
            await userbot.start()
            await setup_automation_handlers(userbot)
        except Exception as e: 
            return False, f"Connection failed: {e}"
    
    return True, "Connected"

async def setup_automation_handlers(client: Client):
    @client.on_message()
    async def auto_handler(c, m):
        # Fetch active pairs
        pairs = get_target_pairs()
        for pid, sid, tid, s_title, t_title, is_mon, is_live, filter_type in pairs:
            # We match numeric IDs
            if str(m.chat.id) == str(sid):
                # Apply filter
                if filter_type == "media" and not m.media: continue
                if filter_type == "text" and m.media: continue
                
                # 1) Monitor: Save to DB if monitoring is ON
                if is_mon:
                    if m.media:
                        m_thread = getattr(m, "message_thread_id", None)
                        with db_conn() as conn:
                            db_c = conn.cursor()
                            p = get_placeholder()
                            if DATABASE_URL:
                                db_c.execute("INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (pid, sid, m.id, m_thread, m_type, m.caption or ""))
                            else:
                                db_c.execute("INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?)", (pid, sid, m.id, m_thread, m_type, m.caption or ""))
                    elif m.text: # Collect text if allowed
                        m_thread = getattr(m, "message_thread_id", None)
                        with db_conn() as conn:
                            db_c = conn.cursor()
                            p = get_placeholder()
                            if DATABASE_URL:
                                db_c.execute("INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (pid, sid, m.id, m_thread, 'text', m.text))
                            else:
                                db_c.execute("INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?)", (pid, sid, m.id, m_thread, 'text', m.text))

                # 2) Live Forward: Copy message to target if live is ON
                if is_live:
                    try:
                        # Topic Handling
                        target_thread = None
                        m_thread = getattr(m, "message_thread_id", None)
                        if m_thread:
                            cache_key = (tid, sid, m_thread)
                            if cache_key in topic_cache:
                                target_thread = topic_cache[cache_key]
                            else:
                                try:
                                    t_chat = await resolve_target_id(userbot, str(tid))
                                    if getattr(t_chat, "is_forum", False):
                                        # Match source topic name to target
                                        src_topics = await userbot.get_forum_topics(sid)
                                        src_topic = next((t for t in src_topics if t.id == m_thread), None)
                                        if src_topic:
                                            t_topics = await userbot.get_forum_topics(t_chat.id)
                                            match = next((t for t in t_topics if t.title == src_topic.title), None)
                                            if match:
                                                target_thread = match.id
                                            else:
                                                # Create matching topic in target
                                                try:
                                                    new_t = await userbot.create_forum_topic(t_chat.id, src_topic.title)
                                                    target_thread = new_t.id
                                                    logger.info(f"✨ Created new topic '{src_topic.title}' in target {tid}")
                                                except Exception as te:
                                                    logger.error(f"❌ Failed to create topic in target: {te}")
                                            
                                            if target_thread:
                                                topic_cache[cache_key] = target_thread
                                except Exception as fe:
                                    logger.error(f"Topic Resolution Error for Pair {pid}: {fe}")
                        
                        await m.copy(tid, message_thread_id=target_thread)
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
    
@bot.message_handler(commands=['ping'])
def cmd_ping(message):
    if message.from_user.id != ADMIN_ID: return
    bot.reply_to(message, f"🏓 **Pong!**\n\nI am currently awake and running.\nTime: `{datetime.now().strftime('%H:%M:%S')}`", parse_mode="Markdown")

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
        bot.answer_callback_query(call.id)
        bot.edit_message_text(get_dashboard_text(), call.message.chat.id, call.message.message_id, reply_markup=get_dashboard_markup(), parse_mode="Markdown")

    elif data == "pairs_main":
        bot.answer_callback_query(call.id)
        try:
            markup = pairs_list_markup()
            bot.edit_message_text("🎯 **Target Pairs**\nSelect a pair to manage collection or release:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Pairs List Error: {e}")
            bot.send_message(call.message.chat.id, f"❌ Error loading pairs: {e}")

    elif data == "pair_add_start":
        bot.answer_callback_query(call.id)
        bot.edit_message_text("📂 **Step 1: Select Source Type**\nChoose the category of the source chat:", call.message.chat.id, call.message.message_id, reply_markup=get_type_selection_markup("sel_src"), parse_mode="Markdown")

    elif data.startswith("sel_src_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "back":
            bot.edit_message_text("📂 **Step 1: Select Source Type**\nChoose the category of the source chat:", call.message.chat.id, call.message.message_id, reply_markup=get_type_selection_markup("sel_src"), parse_mode="Markdown")
        elif parts[2] == "type":
            category = parts[3]
            page = int(parts[4])
            async def show_src_list():
                markup = await get_chat_selection_markup("sel_src", category, page)
                if markup:
                    bot.edit_message_text(f"🎯 **Select Source ({category.title()})**\nChoose the chat to collect from:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            asyncio.run_coroutine_threadsafe(show_src_list(), loop)
        elif parts[2] == "id":
            sid = int(parts[3])
            login_data[uid] = {"source_id": sid}
            bot.edit_message_text("📂 **Step 2: Select Target Type**\nChoose the category of the target chat:", call.message.chat.id, call.message.message_id, reply_markup=get_type_selection_markup("sel_tgt"), parse_mode="Markdown")

    elif data.startswith("sel_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "back":
            bot.edit_message_text("📂 **Step 2: Select Target Type**\nChoose the category of the target chat:", call.message.chat.id, call.message.message_id, reply_markup=get_type_selection_markup("sel_tgt"), parse_mode="Markdown")
        elif parts[2] == "type":
            category = parts[3]
            page = int(parts[4])
            async def show_tgt_list():
                markup = await get_chat_selection_markup("sel_tgt", category, page)
                if markup:
                    bot.edit_message_text(f"🎯 **Select Target ({category.title()})**\nChoose the chat to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            asyncio.run_coroutine_threadsafe(show_tgt_list(), loop)
        elif parts[2] == "id":
            tid = int(parts[3])
            sid = login_data[uid]["source_id"]
            bot.edit_message_text("⏳ Resolving pair...", call.message.chat.id, call.message.message_id)
            
            async def finalize_pair():
                try:
                    is_ok, msg = await ensure_userbot()
                    if not is_ok:
                        bot.send_message(call.message.chat.id, f"❌ Error: {msg}")
                        return
                    
                    # Try to get chat info as gracefully as possible
                    s_chat = None
                    t_chat = None
                    
                    try:
                        s_chat = await userbot.get_chat(sid)
                    except RPCError as e:
                        if "CHANNEL_PRIVATE" in str(e):
                            # Last ditch effort: search dialogs
                            async for dialog in userbot.get_dialogs(limit=100):
                                if dialog.chat.id == sid:
                                    s_chat = dialog.chat
                                    break
                        if not s_chat: raise e

                    try:
                        t_chat = await resolve_target_id(userbot, tid)
                    except RPCError as e:
                        if "CHANNEL_PRIVATE" in str(e):
                            async for dialog in userbot.get_dialogs(limit=100):
                                if dialog.chat.id == tid:
                                    t_chat = dialog.chat
                                    break
                        if not t_chat: raise e
                    
                    s_title = s_chat.title or f"{s_chat.first_name or ''} {s_chat.last_name or ''}".strip() or "Source"
                    t_title = t_chat.title or f"{t_chat.first_name or ''} {t_chat.last_name or ''}".strip() or "Target"
                    
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"INSERT INTO target_pairs (source_id, target_id, source_title, target_title) VALUES ({p}, {p}, {p}, {p}) ON CONFLICT DO NOTHING", (sid, tid, s_title, t_title))
                    
                    bot.edit_message_text(f"✅ **Pair Added Successfully!**\n\nSource: `{s_title}`\nTarget: `{t_title}`", call.message.chat.id, call.message.message_id, reply_markup=pairs_list_markup(), parse_mode="Markdown")
                except Exception as e:
                    logger.error(f"Pair Finalize Error: {e}")
                    err_msg = str(e)
                    if "CHANNEL_PRIVATE" in err_msg or "CHAT_WRITE_FORBIDDEN" in err_msg:
                        err_msg = "❌ The UserAccount is not a part of the Source or Target channel/group. Please join them first and retry. (Or the channel might have been deleted/removed)."
                    bot.send_message(call.message.chat.id, f"⚠️ **Error adding pair**\n\n{err_msg}", parse_mode="Markdown")
            
            asyncio.run_coroutine_threadsafe(finalize_pair(), loop)

    elif data.startswith("pair_view_"):
        bot.answer_callback_query(call.id)
        pid = int(data.split("_")[-1])
        show_pair_view(call.message.chat.id, call.message.message_id, pid)

    elif data.startswith("pair_toggle_mon_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        if not row:
            bot.answer_callback_query(call.id, "Pair not found")
            return
        new_val = 0 if row[5] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_monitoring = {p} WHERE id = {p}", (new_val, pid))
        
        if new_val == 1:
            bot.send_message(call.message.chat.id, "👁️ Monitoring Started! Initializing full history scan in background...")
            asyncio.run_coroutine_threadsafe(run_collection(call.message.chat.id, pid, limit=None), loop)
        
        bot.answer_callback_query(call.id, f"Monitor {'Started' if new_val else 'Stopped'}")
        show_pair_view(call.message.chat.id, call.message.message_id, pid)

    elif data.startswith("pair_toggle_live_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        if not row:
            bot.answer_callback_query(call.id, "Pair not found")
            return
        new_val = 0 if row[6] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_live = {p} WHERE id = {p}", (new_val, pid))
        bot.answer_callback_query(call.id, f"Live Forward {'Started' if new_val else 'Stopped'}")
        show_pair_view(call.message.chat.id, call.message.message_id, pid)

    elif data.startswith("pair_cycle_filter_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        if not row: return
        
        current = row[7] # filter_type
        next_filter = "all"
        if current == "all": next_filter = "media"
        elif current == "media": next_filter = "text"
        else: next_filter = "all"
        
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET filter_type = {p} WHERE id = {p}", (next_filter, pid))
            
        bot.answer_callback_query(call.id, f"Filter set to: {next_filter.upper()}")
        show_pair_view(call.message.chat.id, call.message.message_id, pid)

    elif data.startswith("pair_hist_menu_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🔢 Count Based", callback_data=f"pair_hist_type_count_{pid}"),
            InlineKeyboardButton("📅 Date Based", callback_data=f"pair_hist_type_date_{pid}")
        )
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"pair_view_{pid}"))
        bot.edit_message_text("📜 **History Scraper**\n\nChoose your scraping mode:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("pair_hist_type_count_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        admin_states[uid] = f"hist_setup_count_only_{pid}"
        bot.send_message(call.message.chat.id, "🔢 **Count Based Scrape**\n\nHow many messages would you like to scrape?")

    elif data.startswith("pair_hist_type_date_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        admin_states[uid] = f"hist_setup_date_start_{pid}"
        bot.send_message(call.message.chat.id, "📅 **Date Based Scrape**\n\nEnter **Start Date** (DD/MM/YYYY):")

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
        bot.answer_callback_query(call.id)
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("⚡ Instant Release", callback_data=f"pair_rel_now_{pid}"),
            InlineKeyboardButton("⏰ Scheduled (Slow)", callback_data=f"pair_rel_slow_{pid}")
        )
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"pair_view_{pid}"))
        bot.edit_message_text("🚀 **Release Engine**\n\nChoose release mode:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("pair_rel_now_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "🚀 Starting Instant Release...")
        asyncio.run_coroutine_threadsafe(run_release(call.message.chat.id, pid, interval=1.5), loop)
        show_pair_view(call.message.chat.id, call.message.message_id, pid)

    elif data.startswith("pair_rel_slow_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        admin_states[uid] = f"rel_setup_interval_{pid}"
        bot.send_message(call.message.chat.id, "⏰ **Slow Release Setup**\n\nEnter the **interval** between items in seconds:\n(Example: `60` for 1 minute, `300` for 5 minutes)")

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

    elif state.startswith("rel_setup_interval_"):
        pid = int(state.split("_")[-1])
        if not text.isdigit():
            bot.reply_to(message, "Please send a number.")
            return
        interval = int(text)
        admin_states.pop(uid)
        bot.send_message(message.chat.id, f"🚀 **Slow Release Started**\nPair: `{pid}` | Interval: `{interval}s`")
        asyncio.run_coroutine_threadsafe(run_release(message.chat.id, pid, interval=interval), loop)

    elif state.startswith("hist_setup_count_only_"):
        pid = int(state.split("_")[-1])
        if not text.isdigit():
            bot.reply_to(message, "⚠️ Please send a valid number.")
            return
        count = int(text)
        admin_states.pop(uid)
        bot.send_message(message.chat.id, f"🔢 **Count-Based Scrape**\n\n🎯 Pair ID: `{pid}`\n📥 Limit: `{count}` messages\n\n🚀 *Initializing engine...*", parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(run_history_scrape(message.chat.id, pid, limit=count), loop)

    elif state.startswith("hist_setup_date_start_"):
        pid = int(state.split("_")[-1])
        try:
            dt = datetime.strptime(text, "%d/%m/%Y").replace(tzinfo=timezone.utc)
            login_data[uid] = {"start_date": dt}
            admin_states[uid] = f"hist_setup_date_end_{pid}"
            bot.send_message(message.chat.id, "📅 **Start Date Set!**\n\nNow enter **End Date** (DD/MM/YYYY):\n(Example: `10/05/2026`)")
        except ValueError:
            bot.reply_to(message, "⚠️ Invalid format. Please use `DD/MM/YYYY`.")

    elif state.startswith("hist_setup_date_end_"):
        pid = int(state.split("_")[-1])
        try:
            end_dt = datetime.strptime(text, "%d/%m/%Y").replace(tzinfo=timezone.utc)
            start_dt = login_data[uid]["start_date"]
            admin_states.pop(uid)
            login_data.pop(uid, None)
            bot.send_message(message.chat.id, f"📅 **Date-Based Scrape**\n\n🎯 Pair ID: `{pid}`\n⏳ Range: `{start_dt.strftime('%d/%b/%Y')}` to `{end_dt.strftime('%d/%b/%Y')}`\n\n🚀 *Starting background collection...*", parse_mode="Markdown")
            asyncio.run_coroutine_threadsafe(run_history_scrape(message.chat.id, pid, start_date=start_dt, end_date=end_dt), loop)
        except ValueError:
            bot.reply_to(message, "⚠️ Invalid format. Please use `DD/MM/YYYY`.")

async def run_history_scrape(admin_chat_id, pair_id, limit=None, start_date=None, end_date=None):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return

    task_key = f"hist_{pair_id}"
    running_tasks[task_key] = True
    
    pair = get_target_pair(pair_id)
    if not pair: return
    pid, sid, tid, s_title, t_title, is_mon, is_live, filter_type = pair
    
    collected = 0
    scanned = 0
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛑 Stop Scrape", callback_data=f"pair_stop_task_hist_{pair_id}"))
    status_msg = bot.send_message(admin_chat_id, f"📜 **History Scrape: `{s_title}`**\n\n🔍 Scanned: `0`\n📥 Collected: `0`", reply_markup=markup, parse_mode="Markdown")
    
    try:
        # Force peer resolution (Anti PeerIdInvalid)
        target_chat = await resolve_target_id(userbot, sid)
        sid_resolved = target_chat.id
        
        async for m in userbot.get_chat_history(sid_resolved):
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 History scrape for `{s_title}` stopped by user.")
                break
            
            scanned += 1
            await asyncio.sleep(0.05) # Anti-ban: micro-delay to look less automated
            # Date filter
            if end_date and m.date > end_date: continue
            if start_date and m.date < start_date: break # History is newest to oldest
            
            # Apply filter
            if filter_type == "media" and not m.media: continue
            if filter_type == "text" and m.media: continue

            if m.media:
                media_type = m.media.value
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    if DATABASE_URL:
                        c.execute(
                            "INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), media_type, m.caption or "")
                        )
                    else:
                        c.execute(
                            "INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?)",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), media_type, m.caption or "")
                        )
                    if c.rowcount > 0: collected += 1
            elif m.text:
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    if DATABASE_URL:
                        c.execute(
                            "INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), 'text', m.text)
                        )
                    else:
                        c.execute(
                            "INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?)",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), 'text', m.text)
                        )
                    if c.rowcount > 0: collected += 1
            
            if limit and collected >= limit: break
            
            if scanned % 100 == 0:
                l_text = f" / {limit}" if limit else ""
                try: bot.edit_message_text(f"📜 **History Scrape: `{s_title}`**\n\n🔍 Scanned: `{scanned}`\n📥 Collected: `{collected}{l_text}`\n\n🚀 *Processing...*", admin_chat_id, status_msg.message_id, reply_markup=markup, parse_mode="Markdown")
                except: pass
        
        bot.send_message(admin_chat_id, f"✅ History Scrape Done: `{s_title}`\nCollected: `{collected}`")
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Scrape Error: {e}")
    finally:
        running_tasks.pop(task_key, None)

async def resolve_target_id(client: Client, target_ref: str):
    try:
        # 1. Try direct ID (int)
        if str(target_ref).lstrip("-").isdigit():
            return await client.get_chat(int(target_ref))
        # 2. Try username/ref
        return await client.get_chat(target_ref)
    except Exception as e:
        # 3. Aggressive Search: iterate through many dialogs to find the peer
        try:
            async for dialog in client.get_dialogs(limit=200):
                if str(dialog.chat.id) == str(target_ref) or (dialog.chat.username and dialog.chat.username.lower() == str(target_ref).replace("@", "").lower()):
                    return dialog.chat
        except: pass
        
        err_msg = str(e)
        if "CHANNEL_PRIVATE" in err_msg or "CHAT_WRITE_FORBIDDEN" in err_msg:
            raise ValueError("❌ The UserAccount is not a part of the Source or Target channel/group. Please join them first and retry.")
        raise e
    raise ValueError(f"Could not find or access chat: {target_ref}. Make sure the userbot is a member of this chat.")

async def run_collection(admin_chat_id, pair_id, limit=300):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return
        
    task_key = f"coll_{pair_id}"
    running_tasks[task_key] = True
    
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT source_id, source_title, filter_type FROM target_pairs WHERE id = {p}", (pair_id,))
        row = c.fetchone()
    
    if not row: return
    sid, title, filter_type = row
    collected = 0
    scanned = 0
    limit_text = f"`{limit}`" if limit else "all"
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛑 Stop Collection", callback_data=f"pair_stop_task_coll_{pair_id}"))
    status_msg = bot.send_message(admin_chat_id, f"📥 **Collection: `{title}`**\n\n🔍 Scanned: `0`\n📥 New items: `0`", reply_markup=markup, parse_mode="Markdown")
    
    try:
        # Force peer resolution (Anti PeerIdInvalid)
        target_chat = await resolve_target_id(userbot, sid)
        sid_resolved = target_chat.id
        
        async for m in userbot.get_chat_history(sid_resolved, limit=limit):
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 Collection for `{title}` stopped by user.")
                break
            scanned += 1
            await asyncio.sleep(0.05) # Anti-ban delay
            
            # Apply filter
            if filter_type == "media" and not m.media: continue
            if filter_type == "text" and m.media: continue

            if m.media:
                media_type = m.media.value
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    if DATABASE_URL:
                        c.execute(
                            "INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), media_type, m.caption or "")
                        )
                    else:
                        c.execute(
                            "INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?)",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), media_type, m.caption or "")
                        )
                    if c.rowcount > 0: collected += 1
            elif m.text:
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    if DATABASE_URL:
                        c.execute(
                            "INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), 'text', m.text)
                        )
                    else:
                        c.execute(
                            "INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, thread_id, media_type, caption) VALUES (?, ?, ?, ?, ?, ?)",
                            (pair_id, sid_resolved, m.id, getattr(m, "message_thread_id", None), 'text', m.text)
                        )
                    if c.rowcount > 0: collected += 1

            if scanned % 100 == 0:
                try: bot.edit_message_text(f"📥 **Collection: `{title}`**\n\n🔍 Scanned: `{scanned}`\n📥 New items: `{collected}`\n\n🚀 *Downloading metadata...*", admin_chat_id, status_msg.message_id, reply_markup=markup, parse_mode="Markdown")
                except: pass
        bot.send_message(admin_chat_id, f"✅ Collection Done: `{title}`\nNew items: `{collected}`")
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Collection Error: {e}")
    finally:
        running_tasks.pop(task_key, None)

async def run_release(admin_chat_id, pair_id, interval=1.2):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return

    task_key = f"rel_{pair_id}"
    running_tasks[task_key] = True
    
    try:
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"SELECT source_id, target_id, source_title FROM target_pairs WHERE id = {p}", (pair_id,))
            row = c.fetchone()
        
        if not row: return
        sid, tid_ref, s_title = row
    
        try:
            target_chat = await resolve_target_id(userbot, tid_ref)
            target_id = target_chat.id
        except Exception as e:
            bot.send_message(admin_chat_id, f"❌ Target Error: {e}")
            return

        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"SELECT id, source_message_id, thread_id FROM collected_media WHERE pair_id = {p} AND released = 0", (pair_id,))
            items = c.fetchall()
        
        if not items:
            bot.send_message(admin_chat_id, "No pending items to release.")
            return

        sent = 0
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🛑 Stop Release", callback_data=f"pair_stop_task_rel_{pair_id}"))
        status_msg = bot.send_message(admin_chat_id, f"🚀 Releasing `{len(items)}` items...", reply_markup=markup)
        
        for row_id, smid, sthread in items:
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 Release stopped by user.")
                break
                
            # Topic Handling Logic
            target_thread = None
            if sthread:
                cache_key = (target_id, sid, sthread)
                if cache_key in topic_cache:
                    target_thread = topic_cache[cache_key]
                else:
                    try:
                        # Only attempt if target is forum
                        if getattr(target_chat, "is_forum", False):
                            # Get source topic name
                            src_topics = await userbot.get_forum_topics(sid)
                            src_topic = next((t for t in src_topics if t.id == sthread), None)
                            if src_topic:
                                # Find or create in target
                                try:
                                    t_topics = await userbot.get_forum_topics(target_id)
                                    match = next((t for t in t_topics if t.title == src_topic.title), None)
                                    if match:
                                        target_thread = match.id
                                    else:
                                        # Create new topic
                                        new_t = await userbot.create_forum_topic(target_id, src_topic.title)
                                        target_thread = new_t.id
                                    topic_cache[cache_key] = target_thread
                                except Exception as e:
                                    logger.warning(f"Could not create/find topic: {e}")
                    except Exception as e:
                        logger.error(f"Topic error: {e}")

            try:
                success = False
                sent_msg = None
                try:
                    sent_msg = await userbot.copy_message(target_id, sid, smid, message_thread_id=target_thread)
                    success = True
                except Exception as e:
                    # If copy fails (e.g. restricted content), try download/upload
                    try:
                        msg = await userbot.get_messages(sid, smid)
                        if msg.empty:
                            logger.error(f"Message {smid} is empty or deleted.")
                        elif msg.media:
                            # Try to download
                            path = await msg.download()
                            if path:
                                try:
                                    if msg.photo:
                                        sent_msg = await userbot.send_photo(target_id, path, caption=msg.caption, message_thread_id=target_thread)
                                    elif msg.video:
                                        sent_msg = await userbot.send_video(target_id, path, caption=msg.caption, message_thread_id=target_thread)
                                    else:
                                        sent_msg = await userbot.send_document(target_id, path, caption=msg.caption, message_thread_id=target_thread)
                                    success = True
                                finally:
                                    if os.path.exists(path): os.remove(path)
                        elif msg.text:
                            sent_msg = await userbot.send_message(target_id, msg.text, message_thread_id=target_thread)
                            success = True
                    except Exception as e2:
                        logger.error(f"Deep copy failed for {smid}: {e2}")
                        err_msg = str(e2)
                        if "CHANNEL_PRIVATE" in err_msg or "CHAT_WRITE_FORBIDDEN" in err_msg:
                            err_msg = "❌ The UserAccount is not a member of the Source or Target chat. Please join them first and retry."
                        bot.send_message(admin_chat_id, f"⚠️ **Forward Failed**\nMessage ID: `{smid}`\nError: `{err_msg}`", parse_mode="Markdown")
                
                if success:
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"UPDATE collected_media SET released = 1 WHERE id = {p}", (row_id,))
                    sent += 1
                if sent % 5 == 0:
                    try: bot.edit_message_text(f"🚀 Releasing `{s_title}`...\nSent: `{sent}/{len(items)}`", admin_chat_id, status_msg.message_id, reply_markup=markup)
                    except: pass
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Release error: {e}")
                
        last_link = ""
        if 'sent_msg' in locals() and sent_msg and hasattr(sent_msg, "link") and sent_msg.link:
            last_link = f"\n\n[🔗 View Last Sent Message]({sent_msg.link})"
            
        bot.send_message(admin_chat_id, f"✅ **Release Complete**\nTarget: `{target_chat.title or target_id}`\nSent: `{sent}` items{last_link}", parse_mode="Markdown", disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Global Release Error: {e}")
        bot.send_message(admin_chat_id, f"❌ Release Crashed: {e}")
    finally:
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
        # 1) Render Env
        url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
        if url: return url.rstrip("/")
        
        # 2) DB Saved (Auto-detected from first visit)
        url = get_setting("detected_url")
        if url: return url.rstrip("/")
        
        # 3) Railway Env
        url = os.getenv("RAILWAY_STATIC_URL", "").strip()
        if url:
            if url.startswith("http"): return url.rstrip("/")
            return f"https://{url}".rstrip("/")
        
        # 4) Manual Generic
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
                    logger.info(f"KEEP_ALIVE: Monitoring URL: {detected_url}")
                
                resp = requests.get(f"{url}?t={int(time.time())}", timeout=15)
                logger.info(f"KEEP_ALIVE: Ping sent to {url}, Status: {resp.status_code}")
            else:
                # If no URL is set in ENV, we can't ping
                logger.warning("KEEP_ALIVE: No RENDER_EXTERNAL_URL or WEB_URL found. Bot will likely sleep on Render Free tier!")
        except Exception as e:
            logger.warning(f"KEEP_ALIVE: Ping failed: {e}")
        finally:
            time.sleep(240) # 4 minutes

from flask import Flask, request

app = Flask(__name__)
@app.route("/")
def health():
    # Auto-detect URL from the first request
    if not get_setting("detected_url"):
        # We assume https because Render/Railway use it
        protocol = "https" if request.is_secure or "https" in request.url_root else "http"
        detected = f"{protocol}://{request.host}"
        set_setting("detected_url", detected)
        logger.info(f"KEEP_ALIVE: Auto-detected and SAVED public URL: {detected}")
    
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
    if not DATABASE_URL:
        logger.warning("⚠️ WARNING: You are using SQLite. Your data (pairs, media) will be DELETED every time Render restarts!")
        logger.warning("Please set a DATABASE_URL (PostgreSQL) for permanent storage.")

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
                # Use delete_webhook instead of remove_webhook to avoid argument errors
                bot.delete_webhook(drop_pending_updates=True)
                bot.infinity_polling(skip_pending=True, timeout=20)
            except Exception as e:
                logger.error(f"❌ Polling crashed: {e}. Restarting in 10s...")
                time.sleep(10)
    
    threading.Thread(target=run_polling, daemon=True).start()
    logger.info("✨ Admin bot monitor started")
    
    await idle()

if __name__ == "__main__":
    loop.run_until_complete(main())
