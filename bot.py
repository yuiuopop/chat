import os
import asyncio
import threading
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone

# Pyrogram sync import needs a current event loop on Python 3.10+
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import requests
from flask import Flask
from dotenv import load_dotenv
from pyrogram import Client, filters, idle, enums
from pyrogram.types import Message
from pyrogram.errors import RPCError, SessionPasswordNeeded
from pyrogram.handlers import MessageHandler
import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton


load_dotenv()

# -----------------------------
# Config
# -----------------------------
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
USER_SESSION_STRING = os.getenv("USER_SESSION_STRING", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "8080"))
KEEP_ALIVE_URL = os.getenv("KEEP_ALIVE_URL", "")

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("saved_to_target_userbot")


# --- Bot Helpers ---
def safe_edit_message(text, chat_id, message_id, reply_markup=None, parse_mode="Markdown"):
    try:
        return bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup, parse_mode=parse_mode)
    except telebot.apihelper.ApiTelegramException as e:
        if "message is not modified" in e.description:
            return None
        raise e

# -----------------------------
# DB (single-file sqlite)

# -----------------------------
# Use persistent disk path on Render when provided.
# Set one of these env vars on Render:
#   DATA_DIR=/var/data
# or SQLITE_DB_PATH=/var/data/saved_to_target.db
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "").strip()
DATA_DIR = os.getenv("DATA_DIR", "").strip()

if SQLITE_DB_PATH:
    DB_PATH = SQLITE_DB_PATH
elif DATA_DIR:
    DB_PATH = os.path.join(DATA_DIR, "saved_to_target.db")
else:
    DB_PATH = "saved_to_target.db"

# Ensure parent dir exists when absolute/custom path is used
db_parent = os.path.dirname(DB_PATH)
if db_parent:
    os.makedirs(db_parent, exist_ok=True)


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

@contextmanager
def db_conn():
    if DATABASE_URL:
        # PostgreSQL
        import psycopg2
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = True
    else:
        # SQLite
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
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        
        if DATABASE_URL:
            # PostgreSQL
            c.execute("CREATE TABLE IF NOT EXISTS sent_messages (id SERIAL PRIMARY KEY, source_message_id BIGINT UNIQUE, target_chat_id BIGINT, sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            c.execute("CREATE TABLE IF NOT EXISTS source_chats (chat_id BIGINT PRIMARY KEY, title TEXT, monitor_enabled INTEGER DEFAULT 0, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            c.execute("CREATE TABLE IF NOT EXISTS collected_media (id SERIAL PRIMARY KEY, source_chat_id BIGINT NOT NULL, source_message_id BIGINT NOT NULL, media_type TEXT, caption TEXT, collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, released INTEGER DEFAULT 0, target_chat_id BIGINT, released_at TIMESTAMP, UNIQUE(source_chat_id, source_message_id))")
        else:
            # SQLite
            c.execute("CREATE TABLE IF NOT EXISTS sent_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, source_message_id INTEGER UNIQUE, target_chat_id INTEGER, sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            c.execute("CREATE TABLE IF NOT EXISTS source_chats (chat_id INTEGER PRIMARY KEY, title TEXT, monitor_enabled INTEGER DEFAULT 0, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            c.execute("CREATE TABLE IF NOT EXISTS collected_media (id INTEGER PRIMARY KEY AUTOINCREMENT, source_chat_id INTEGER NOT NULL, source_message_id INTEGER NOT NULL, media_type TEXT, caption TEXT, collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, released INTEGER DEFAULT 0, target_chat_id INTEGER, released_at TIMESTAMP, UNIQUE(source_chat_id, source_message_id))")

        # Migration for older source_chats table
        try:
            c.execute("ALTER TABLE source_chats ADD COLUMN monitor_enabled INTEGER DEFAULT 0")
        except Exception:
            pass
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
                """
                INSERT INTO settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, str(value))
            )



def get_target_ref():
    return (get_setting("target_chat_ref", "") or get_setting("target_chat_id", "") or "").strip()


def already_sent(source_message_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT 1 FROM sent_messages WHERE source_message_id = {p}", (source_message_id,))
        return c.fetchone() is not None



def mark_sent(source_message_id, target_chat_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"INSERT INTO sent_messages (source_message_id, target_chat_id) VALUES ({p}, {p}) ON CONFLICT DO NOTHING",
            (source_message_id, target_chat_id)
        )



def clear_sent_records():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM sent_messages")


def add_source_chat(chat_id: int, title: str):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                "INSERT INTO source_chats (chat_id, title, monitor_enabled) VALUES (%s, %s, 0) ON CONFLICT(chat_id) DO UPDATE SET title = EXCLUDED.title",
                (chat_id, title)
            )
        else:
            c.execute(
                "INSERT INTO source_chats (chat_id, title, monitor_enabled) VALUES (?, ?, 0) ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title",
                (chat_id, title)
            )



def remove_source_chat(chat_id: int):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM source_chats WHERE chat_id = {p}", (chat_id,))



def list_source_chats():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT chat_id, title, monitor_enabled FROM source_chats ORDER BY added_at DESC")
        return c.fetchall()


def is_source_chat(chat_id: int) -> bool:
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT 1 FROM source_chats WHERE chat_id = {p}", (chat_id,))
        return c.fetchone() is not None



def set_monitor_enabled(chat_id: int, enabled: bool):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"UPDATE source_chats SET monitor_enabled = {p} WHERE chat_id = {p}",
            (1 if enabled else 0, chat_id)
        )
        return c.rowcount > 0



def is_monitor_enabled(chat_id: int) -> bool:
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT monitor_enabled FROM source_chats WHERE chat_id = {p}", (chat_id,))
        row = c.fetchone()
        return bool(row and int(row[0]) == 1)



def get_source_title(chat_id: int):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT title FROM source_chats WHERE chat_id = {p}", (chat_id,))
        row = c.fetchone()
        return row[0] if row else None



def save_collected_media(source_chat_id: int, source_message_id: int, media_type: str, caption: str):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                "INSERT INTO collected_media (source_chat_id, source_message_id, media_type, caption) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (source_chat_id, source_message_id, media_type, caption or "")
            )
        else:
            c.execute(
                "INSERT OR IGNORE INTO collected_media (source_chat_id, source_message_id, media_type, caption) VALUES (?, ?, ?, ?)",
                (source_chat_id, source_message_id, media_type, caption or "")
            )
        return c.rowcount > 0



def get_monitor_stats(source_chat_id: int):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"SELECT COUNT(*), SUM(CASE WHEN released = 0 THEN 1 ELSE 0 END), SUM(CASE WHEN released = 1 THEN 1 ELSE 0 END) FROM collected_media WHERE source_chat_id = {p}",
            (source_chat_id,)
        )
        row = c.fetchone() or (0, 0, 0)
        return {
            "total": int(row[0] or 0),
            "unreleased": int(row[1] or 0),
            "released": int(row[2] or 0),
        }



def get_unreleased_collected(source_chat_id: int, limit: int):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"SELECT id, source_message_id, media_type FROM collected_media WHERE source_chat_id = {p} AND released = 0 ORDER BY id ASC LIMIT {p}",
            (source_chat_id, limit)
        )
        return c.fetchall()



def mark_collected_released(row_id: int, target_chat_id: int):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"UPDATE collected_media SET released = 1, target_chat_id = {p}, released_at = CURRENT_TIMESTAMP WHERE id = {p}",
            (target_chat_id, row_id)
        )



# -----------------------------
# Clients
# -----------------------------
userbot = None
bot = telebot.TeleBot(BOT_TOKEN)
admin_states = {}
login_data = {}
active_collect_full = {}

# Heartbeats for watchdog supervision
last_poll_heartbeat = time.time()
last_userbot_heartbeat = time.time()
hb_lock = threading.Lock()


def is_admin(user_id: int) -> bool:
    if ADMIN_ID == 0:
        return True
    return user_id == ADMIN_ID


def set_heartbeat(kind: str):
    global last_poll_heartbeat, last_userbot_heartbeat
    now = time.time()
    with hb_lock:
        if kind == "poll":
            last_poll_heartbeat = now
        elif kind == "userbot":
            last_userbot_heartbeat = now


def get_heartbeats():
    with hb_lock:
        return last_poll_heartbeat, last_userbot_heartbeat


def main_menu_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, row_width=3)
    kb.add(
        KeyboardButton("🏠 DASHBOARD"),
        KeyboardButton("⚙️ SETUP"),
        KeyboardButton("📡 USERBOT")
    )
    kb.add(
        KeyboardButton("📂 SOURCES"),
        KeyboardButton("🎯 TARGET"),
        KeyboardButton("🧲 MONITOR")
    )
    kb.add(
        KeyboardButton("🚀 TRANSFER"),
        KeyboardButton("📊 STATUS"),
        KeyboardButton("❓ HELP")
    )
    return kb



def dashboard_inline_keyboard():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🔄 REFRESH", callback_data="dash_refresh"),
        InlineKeyboardButton("📡 CONSOLE", callback_data="dash_bot_status")
    )
    markup.add(
        InlineKeyboardButton("📂 SOURCES", callback_data="sources_list_0"),
        InlineKeyboardButton("🎯 TARGET", callback_data="target_view")
    )
    markup.add(
        InlineKeyboardButton("🚀 QUICK RELEASE", callback_data="quick_release_all"),
        InlineKeyboardButton("👤 ACCOUNT", callback_data="user_acc_main")
    )
    markup.add(
        InlineKeyboardButton("⚙️ SETTINGS", callback_data="settings_main"),
        InlineKeyboardButton("🧹 CLEAR CACHE", callback_data="clear_sent_confirm")
    )
    return markup



def user_account_keyboard():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("👥 Groups", callback_data="user_acc_list_groups_0"),
        InlineKeyboardButton("📣 Channels", callback_data="user_acc_list_channels_0")
    )
    markup.add(
        InlineKeyboardButton("🤖 Bots", callback_data="user_acc_list_bots_0"),
        InlineKeyboardButton("👤 Private", callback_data="user_acc_list_private_0")
    )
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup



def settings_inline_keyboard():
    mode = get_setting("forward_mode", "media")
    auto = get_setting("auto_forward", "false")
    
    markup = InlineKeyboardMarkup(row_width=1)
    
    mode_text = "📑 Mode: Everything" if mode == "all" else "🖼 Mode: Media Only"
    auto_text = "⚡ Auto-Forward: ON" if auto == "true" else "💤 Auto-Forward: OFF"
    
    markup.add(InlineKeyboardButton(mode_text, callback_data="settings_toggle_mode"))
    markup.add(InlineKeyboardButton(auto_text, callback_data="settings_toggle_auto"))
    markup.add(InlineKeyboardButton("🧹 Clear Sent History", callback_data="clear_sent_confirm"))
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup



def setup_inline_keyboard():
    api_id = get_setting("api_id", "") or API_ID
    api_hash = get_setting("api_hash", "") or API_HASH
    session_val = get_setting("user_session_string", "") or USER_SESSION_STRING

    markup = InlineKeyboardMarkup(row_width=1)
    
    id_status = "✅" if api_id and str(api_id) != "0" else "❌"
    hash_status = "✅" if api_hash else "❌"
    sess_status = "✅" if session_val else "❌"

    markup.add(InlineKeyboardButton(f"{id_status} Set API ID", callback_data="setup_api_id"))
    markup.add(InlineKeyboardButton(f"{hash_status} Set API Hash", callback_data="setup_api_hash"))
    
    sess_row = [InlineKeyboardButton(f"{sess_status} Login", callback_data="setup_login")]
    if session_val:
        sess_row.append(InlineKeyboardButton("🔴 Remove Session", callback_data="setup_session_remove_confirm"))
    markup.row(*sess_row)

    markup.add(InlineKeyboardButton("🗑 Wipe All Credentials", callback_data="setup_clear_confirm"))
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))

    return markup




def dashboard_text():
    t = get_target_ref() or "NONE"
    a = get_setting("auto_forward", "false").upper()
    sources = list_source_chats()
    
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM sent_messages")
        sent_count = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM collected_media WHERE released = 0")
        queue_count = c.fetchone()[0]

    poll_hb, user_hb = get_heartbeats()
    now = time.time()
    poll_status = "🟢 ACTIVE" if now - poll_hb < 180 else "🔴 STALE"
    userbot_status = "🟢 ONLINE" if (userbot and userbot.is_connected) else "🔴 OFFLINE"

    m = get_setting("forward_mode", "media").upper()
    af_emoji = "🟢" if a == "TRUE" else "🔴"

    text = f"┏━━━━━━━ SYSTEM CONSOLE ━━━━━━━┓\n"
    text += f"┃ 🤖 BOT  : {poll_status:<16}┃\n"
    text += f"┃ 📡 USER : {userbot_status:<16}┃\n"
    text += f"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛\n\n"
    
    text += f"💬 *QUOTES*\n"
    text += f"\"The best way to predict the future is to create it.\"\n\n"
    
    text += f"📊 *QUEUE PERFORMANCE*\n"
    text += f"├─ 📂 Sources: `{len(sources)}` configured\n"
    text += f"├─ ⏳ Pending: `{queue_count}` items\n"
    text += f"├─ ✅ Released: `{sent_count}` total\n"
    text += f"└─ {af_emoji} Auto-Forward: `{a}`\n\n"
    
    text += f"⚙️ *CONFIGURATION*\n"
    text += f"├─ 🎯 Target: `{t}`\n"
    text += f"└─ 🛠 Mode: `{m}`\n\n"
    
    text += f"🕹 *QUICK ACTIONS*"
    return text






def sources_list_keyboard(page=0):
    rows = list_source_chats()
    per_page = 5
    total_pages = max(1, (len(rows) + per_page - 1) // per_page)
    p = min(page, total_pages - 1)
    chunk = rows[p * per_page:(p + 1) * per_page]
    
    markup = InlineKeyboardMarkup()
    for cid, title, mon in chunk:
        mon_icon = "🟢" if int(mon or 0) == 1 else "🔴"
        btn_text = f"{mon_icon} {title or cid}"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"src_manage_{cid}"))
    
    # Pagination
    nav_btns = []
    if p > 0:
        nav_btns.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"sources_list_{p-1}"))
    if p < total_pages - 1:
        nav_btns.append(InlineKeyboardButton("Next ➡️", callback_data=f"sources_list_{p+1}"))
    if nav_btns:
        markup.row(*nav_btns)
        
    markup.add(
        InlineKeyboardButton("➕ Add Source", callback_data="sources_add_start"),
        InlineKeyboardButton("🔖 Add Saved Messages", callback_data="sources_add_saved")
    )
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup



def source_manage_keyboard(chat_id):
    mon = is_monitor_enabled(chat_id)
    stats = get_monitor_stats(chat_id)
    
    markup = InlineKeyboardMarkup(row_width=2)
    mon_btn = InlineKeyboardButton("🔴 Disable Monitor" if mon else "🟢 Enable Monitor", callback_data=f"src_toggle_{chat_id}")
    markup.add(mon_btn)
    
    markup.add(
        InlineKeyboardButton("📥 Scrape History", callback_data=f"src_scrape_menu_{chat_id}"),
        InlineKeyboardButton("🚀 Release N", callback_data=f"src_release_n_{chat_id}")
    )

    markup.add(
        InlineKeyboardButton("🗑 Remove Source", callback_data=f"src_delete_confirm_{chat_id}"),
        InlineKeyboardButton("🔙 List", callback_data="sources_list_0")
    )
    return markup, stats


def scrape_options_keyboard(chat_id):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🔢 By Count", callback_data=f"scrape_type_count_{chat_id}"),
        InlineKeyboardButton("📅 By Date", callback_data=f"scrape_type_date_{chat_id}")
    )
    markup.add(
        InlineKeyboardButton("♾ Full History", callback_data=f"scrape_type_full_{chat_id}"),
        InlineKeyboardButton("⏪ Before Added Date", callback_data=f"scrape_type_pre_added_{chat_id}")
    )
    markup.add(InlineKeyboardButton("🔙 Back to Source", callback_data=f"src_manage_{chat_id}"))

    return markup



def build_temp_client(api_id: int, api_hash: str) -> Client:
    return Client(
        name=":memory:",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
        workers=2,
    )


@bot.callback_query_handler(func=lambda call: True)

def handle_callbacks(call):
    global userbot
    uid = call.from_user.id
    if not is_admin(uid):
        bot.answer_callback_query(call.id, "Unauthorized.")
        return


    data = call.data
    
    if data == "dash_main" or data == "dash_refresh":
        bot.edit_message_text(
            dashboard_text(),
            call.message.chat.id,
            call.message.message_id,
            reply_markup=dashboard_inline_keyboard(),
            parse_mode="Markdown"
        )
        bot.answer_callback_query(call.id, "Dashboard updated")

    elif data.startswith("sources_list_"):
        page = int(data.split("_")[-1])
        bot.edit_message_text(
            "📂 *Managed Sources*\n\n_Select a source to configure or monitor._",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=sources_list_keyboard(page),
            parse_mode="Markdown"
        )

    elif data.startswith("src_manage_"):
        cid = int(data.split("_")[-1])
        kb, stats = source_manage_keyboard(cid)
        title = get_source_title(cid) or str(cid)
        mon = "🟢 ON" if is_monitor_enabled(cid) else "🔴 OFF"
        text = (
            f"📂 *Source:* `{title}`\n"
            f"🆔 *ID:* `{cid}`\n\n"
            f"🛰 *Monitor Status:* {mon}\n"
            f"📊 *Collected:* `{stats['total']}`\n"
            f"📥 *Unreleased:* `{stats['unreleased']}`\n"
            f"📤 *Released:* `{stats['released']}`"
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    elif data.startswith("src_toggle_"):
        cid = int(data.split("_")[-1])
        new_state = not is_monitor_enabled(cid)
        set_monitor_enabled(cid, new_state)
        bot.answer_callback_query(call.id, f"Monitor {'Enabled' if new_state else 'Disabled'}")
        # Refresh management view
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': f"src_manage_{cid}", 'message': call.message, 'id': call.id}))

    elif data == "target_view":
        t = get_target_ref() or "Not set"
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔄 Change Target", callback_data="target_change_prompt"))
        kb.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
        bot.edit_message_text(f"🎯 *Target Configuration*\n\nCurrent Target: `{t}`\n\n_You can also use /settarget <id> directly._", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    elif data == "target_change_prompt":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "Please send the new target chat ID or @username.\n(Example: `-1001234567890`)")

    elif data == "settings_main":
        bot.edit_message_text("⚙️ *System Settings*", call.message.chat.id, call.message.message_id, reply_markup=settings_inline_keyboard(), parse_mode="Markdown")

    elif data == "settings_toggle_mode":
        current = get_setting("forward_mode", "media")
        new = "all" if current == "media" else "media"
        set_setting("forward_mode", new)
        bot.answer_callback_query(call.id, f"Mode set to {new.upper()}")
        bot.edit_message_text("⚙️ *System Settings*", call.message.chat.id, call.message.message_id, reply_markup=settings_inline_keyboard(), parse_mode="Markdown")

    elif data == "settings_toggle_auto":
        current = get_setting("auto_forward", "false")
        new = "true" if current == "false" else "false"
        set_setting("auto_forward", new)
        bot.answer_callback_query(call.id, f"Auto-Forward {'Enabled' if new == 'true' else 'Disabled'}")
        bot.edit_message_text("⚙️ *System Settings*", call.message.chat.id, call.message.message_id, reply_markup=settings_inline_keyboard(), parse_mode="Markdown")


    elif data == "sources_add_start":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "Please send the chat ID of the new source.\n(Use /listgroups to find IDs)")

    elif data == "sources_add_saved":
        if userbot is None or not userbot.is_connected:

            bot.answer_callback_query(call.id, "❌ Userbot not connected", show_alert=True)
            return
        
        async def do_add_saved():
            try:
                me = await userbot.get_me()
                add_source_chat(me.id, "Saved Messages")
                bot.answer_callback_query(call.id, "✅ Added Saved Messages as a source!")
                handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "sources_list_0", 'message': call.message, 'id': call.id}))
            except Exception as e:
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")
        
        asyncio.run_coroutine_threadsafe(do_add_saved(), loop)


    elif data.startswith("src_delete_confirm_"):
        cid = int(data.split("_")[-1])
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✅ Confirm Delete", callback_data=f"src_delete_do_{cid}"))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data=f"src_manage_{cid}"))
        bot.edit_message_text(f"⚠️ *Confirm Removal*\n\nAre you sure you want to remove this source?\n`{cid}`", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    elif data.startswith("src_delete_do_"):
        cid = int(data.split("_")[-1])
        remove_source_chat(cid)
        bot.answer_callback_query(call.id, "Source removed")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "sources_list_0", 'message': call.message, 'id': call.id}))

    elif data == "dash_bot_status":
        bot.answer_callback_query(call.id, "Refreshing connection info...")
        # (This could trigger a quick check or just refresh the dash)
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "dash_main", 'message': call.message, 'id': call.id}))

    elif data == "dash_main":
        safe_edit_message(dashboard_text(), call.message.chat.id, call.message.message_id, reply_markup=dashboard_inline_keyboard())

    elif data == "dash_refresh":
        bot.answer_callback_query(call.id, "Stats refreshed!")
        safe_edit_message(dashboard_text(), call.message.chat.id, call.message.message_id, reply_markup=dashboard_inline_keyboard())


    elif data == "clear_sent_confirm":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✅ Confirm Clear", callback_data="clear_sent_do"))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data="dash_main"))
        bot.edit_message_text("🧹 *Confirm Clear*\n\nThis will clear all sent-message history, allowing you to resend the same media.", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    elif data == "clear_sent_do":
        clear_sent_records()
        bot.answer_callback_query(call.id, "History cleared")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "dash_main", 'message': call.message, 'id': call.id}))
    
    elif data == "quick_release_all":
        bot.answer_callback_query(call.id, "🚀 Starting Global Release...")
        async def run_bulk():
            await release_engine(call.message.chat.id)
        asyncio.run_coroutine_threadsafe(run_bulk(), loop)


    elif data.startswith("src_scrape_menu_"):
        cid = int(data.split("_")[-1])
        title = get_source_title(cid) or str(cid)
        bot.edit_message_text(f"📥 *Scrape History:* `{title}`\n\nChoose how you want to collect old content:", call.message.chat.id, call.message.message_id, reply_markup=scrape_options_keyboard(cid), parse_mode="Markdown")

    elif data.startswith("scrape_type_count_"):
        cid = int(data.split("_")[-1])
        admin_states[uid] = f"awaiting_scrape_count_{cid}"
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "🔢 *How many messages* should I scan?\n(Enter a number, e.g., `200`)", parse_mode="Markdown")

    elif data.startswith("scrape_type_date_"):
        cid = int(data.split("_")[-1])
        admin_states[uid] = f"awaiting_scrape_date_start_{cid}"
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "📅 *Enter Start Date*\n(Format: `YYYY-MM-DD`, e.g., `2024-01-01`)", parse_mode="Markdown")

    elif data.startswith("scrape_type_full_"):
        cid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "🚀 Starting Full History Scrape...")
        # Trigger background task for full scrape
        async def run_full():
            await advanced_scrape_task(call.message.chat.id, cid, mode="full")
        asyncio.run_coroutine_threadsafe(run_full(), loop)

    elif data.startswith("scrape_type_pre_added_"):
        cid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "🚀 Scraping history BEFORE group was added...")
        
        async def run_pre():
            # Get added_at from DB
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                c.execute(f"SELECT added_at FROM source_chats WHERE chat_id = {p}", (cid,))
                row = c.fetchone()
                added_at_str = row[0] if row else None
            
            # If we have a date, use it as the end_date for our backwards scan
            # (In our task, end_date is where we start scanning backwards from)
            end_date = None
            if added_at_str:
                # added_at_str is usually YYYY-MM-DD HH:MM:SS
                end_date = added_at_str.split()[0] # Just the date part YYYY-MM-DD
            
            await advanced_scrape_task(call.message.chat.id, cid, end_date=end_date)
        
        asyncio.run_coroutine_threadsafe(run_pre(), loop)



    elif data.startswith("src_release_n_"):
        cid = int(data.split("_")[-1])
        admin_states[uid] = f"awaiting_release_count_{cid}"
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "🚀 *How many items* should I release from this source?\n(Enter a number, or type `all`)", parse_mode="Markdown")



    elif data == "user_acc_main":
        bot.edit_message_text("👤 *User Account Dashboard*\n\nBrowse and inspect the chats in your account:", call.message.chat.id, call.message.message_id, reply_markup=user_account_keyboard(), parse_mode="Markdown")

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
            
            kb = InlineKeyboardMarkup(row_width=1)
            for chat in page_items:
                title = chat.title or chat.first_name or str(chat.id)
                kb.add(InlineKeyboardButton(f"👁 {title}", callback_data=f"user_acc_view_{chat.id}"))
            
            # Nav buttons
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"user_acc_list_{category}_{page-1}"))
            if end < len(all_dialogs):
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"user_acc_list_{category}_{page+1}"))
            if nav:
                kb.add(*nav)
            
            kb.add(InlineKeyboardButton("🔙 Back to Categories", callback_data="user_acc_main"))
            
            msg = f"👤 *Account Browser:* {category.capitalize()}\nPage {page + 1} | Total: {len(all_dialogs)}"
            safe_edit_message(msg, call.message.chat.id, call.message.message_id, reply_markup=kb)

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
                
                info = f"📋 *Chat Details:*\n\n"
                info += f"🏷 *Title:* `{title}`\n"
                info += f"🆔 *ID:* `{chat.id}`\n"
                info += f"📂 *Type:* `{chat.type.value}`\n"
                info += f"💬 *Messages:* `{msg_count}`\n"
                if hasattr(chat, 'members_count') and chat.members_count:
                    info += f"👥 *Members:* `{chat.members_count}`\n"
                if chat.username:
                    info += f"🔗 *Username:* @{chat.username}\n"
                
                kb = InlineKeyboardMarkup(row_width=2)
                kb.add(
                    InlineKeyboardButton("➕ Add as Source", callback_data=f"quick_add_src_{chat.id}"),
                    InlineKeyboardButton("🎯 Set as Target", callback_data=f"quick_set_tgt_{chat.id}")
                )
                # Determine category for "Back" button
                cat = "groups"
                if chat.type == enums.ChatType.CHANNEL: cat = "channels"
                elif chat.type == enums.ChatType.BOT: cat = "bots"
                elif chat.type == enums.ChatType.PRIVATE: cat = "private"
                
                kb.add(InlineKeyboardButton("🔙 Back to List", callback_data=f"user_acc_list_{cat}_0"))
                
                safe_edit_message(info, call.message.chat.id, call.message.message_id, reply_markup=kb)

            except Exception as e:
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")

        asyncio.run_coroutine_threadsafe(run_view(), loop)

    elif data.startswith("quick_add_src_"):
        chat_id = int(data.split("_")[-1])
        async def do_add():
            chat = await userbot.get_chat(chat_id)
            add_source_chat(chat.id, chat.title or chat.first_name or str(chat.id))
            bot.answer_callback_query(call.id, "✅ Added as Source!")
            handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': f"user_acc_view_{chat_id}", 'message': call.message, 'id': call.id}))
        asyncio.run_coroutine_threadsafe(do_add(), loop)

    elif data.startswith("quick_set_tgt_"):
        chat_id = int(data.split("_")[-1])
        set_setting("target_chat_id", chat_id)
        set_setting("target_chat_ref", str(chat_id))
        bot.answer_callback_query(call.id, "🎯 Target Updated!")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': f"user_acc_view_{chat_id}", 'message': call.message, 'id': call.id}))

        bot.send_message(call.message.chat.id, "Please use /setapihash <your_api_hash> to update it.")
    
    elif data == "setup_login":
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "🚀 Starting login process...\nPlease use /login to begin.")

    elif data == "setup_session_remove_confirm":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✅ Confirm Logout", callback_data="setup_session_remove_do"))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data="setup_main"))
        bot.edit_message_text("🔴 *Confirm Logout*\n\nAre you sure you want to remove the current session string? This will stop the userbot.", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    elif data == "setup_session_remove_do":
        if userbot:

            try:
                asyncio.run_coroutine_threadsafe(userbot.stop(), loop)
            except: pass
            userbot = None
        
        set_setting("user_session_string", "")
        bot.answer_callback_query(call.id, "Session removed")
        # Return to setup menu
        bot.edit_message_text("⚙️ *System Configuration*", call.message.chat.id, call.message.message_id, reply_markup=setup_inline_keyboard(), parse_mode="Markdown")

    elif data == "setup_main":
        bot.edit_message_text("⚙️ *System Configuration*", call.message.chat.id, call.message.message_id, reply_markup=setup_inline_keyboard(), parse_mode="Markdown")


    elif data == "setup_clear_confirm":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✅ Confirm Clear", callback_data="setup_clear_do"))
        kb.add(InlineKeyboardButton("❌ Cancel", callback_data="dash_main"))
        bot.edit_message_text("⚠️ *Confirm Wipe*\n\nThis will remove all API credentials and session strings from the database.", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="Markdown")

    elif data == "setup_clear_do":
        set_setting("api_id", "")
        set_setting("api_hash", "")
        set_setting("user_session_string", "")
        bot.answer_callback_query(call.id, "Credentials wiped")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "dash_main", 'message': call.message, 'id': call.id}))





async def forward_from_saved_message(msg: Message) -> bool:
    global userbot
    if userbot is None:
        return False
    target_raw = get_target_ref()
    if not target_raw:
        return False
    if str(get_setting("auto_forward", "false")).lower() != "true":
        return False
    # If mode is media-only, skip text messages
    if get_setting("forward_mode", "media") == "media" and not msg.media:
        return False
    if already_sent(msg.id):
        return False


    target_id = target_raw
    source_chat_id = msg.chat.id if msg.chat else None
    if source_chat_id is None:
        return False

    try:
        await userbot.copy_message(
            chat_id=target_id,
            from_chat_id=source_chat_id,
            message_id=msg.id
        )
        try:
            mark_sent(msg.id, int(target_raw))
        except Exception:
            mark_sent(msg.id, 0)
        logger.info(f"Forwarded saved media {msg.id} -> {target_id}")
        return True
    except RPCError as e:
        logger.error(f"Failed forwarding {msg.id} to {target_id}: {e}")
        return False


async def saved_media_listener(client: Client, message: Message):
    try:
        set_heartbeat("userbot")
        # Process media from configured source chats
        if message.chat and is_source_chat(message.chat.id):
            mode = get_setting("forward_mode", "media")
            is_media = bool(message.media)
            
            if (mode == "all" or is_media) and is_monitor_enabled(message.chat.id):
                media_type = message.media.value if is_media else "text"
                collected = save_collected_media(
                    message.chat.id,
                    message.id,
                    media_type,
                    message.caption or message.text or ""
                )

                if collected:
                    logger.info(f"Collected media {message.id} ({media_type}) from source {message.chat.id}")
                else:
                    logger.info(f"Skipped collect for {message.chat.id}:{message.id} (already in queue)")
            elif message.media:
                logger.info(f"Media seen in source {message.chat.id} but monitor is OFF")
            # Optional live-forward mode
            await forward_from_saved_message(message)
    except Exception as e:
        logger.error(f"Listener error: {e}")


def get_runtime_credentials():
    db_api_id = get_setting("api_id", "")
    db_api_hash = get_setting("api_hash", "")
    db_session = get_setting("user_session_string", "")

    final_api_id = int(db_api_id) if str(db_api_id).strip() else API_ID
    final_api_hash = db_api_hash.strip() if str(db_api_hash).strip() else API_HASH
    final_session = db_session.strip() if str(db_session).strip() else USER_SESSION_STRING
    return final_api_id, final_api_hash, final_session


async def start_or_reload_userbot() -> tuple[bool, str]:
    """Start userbot from DB/env credentials, or reload if already running."""
    global userbot
    final_api_id, final_api_hash, final_session = get_runtime_credentials()
    if not all([final_api_id, final_api_hash, final_session]):
        return False, "Missing API ID / API Hash / Session."

    try:
        if userbot is not None:
            try:
                await userbot.stop()
            except Exception:
                pass
            userbot = None

        userbot = Client(
            "saved_forward_userbot",
            api_id=final_api_id,
            api_hash=final_api_hash,
            session_string=final_session,
            in_memory=True,
            workers=4,
        )
        # Important: do NOT use filters.me here, otherwise we only receive our own messages
        # and monitoring from source groups/channels won't work.
        # Register handler for all messages (not just media) to allow text forwarding
        userbot.add_handler(MessageHandler(saved_media_listener))
        await userbot.start()

        me = await userbot.get_me()
        return True, f"Userbot running as @{me.username or me.id}"
    except Exception as e:
        userbot = None
        return False, f"Failed to start userbot: {e}"


# -----------------------------
# Admin bot commands
# -----------------------------
@bot.message_handler(commands=["start", "help"])
def cmd_start(message):
    if not is_admin(message.from_user.id):
        bot.reply_to(message, "Unauthorized.")
        return
    bot.reply_to(
        message,
        dashboard_text(),
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )
    # Also send the inline dashboard for immediate interaction
    bot.send_message(
        message.chat.id,
        "🎮 *Interactive Control Panel*",
        reply_markup=dashboard_inline_keyboard(),
        parse_mode="Markdown"
    )


@bot.message_handler(commands=["menu"])
def cmd_menu(message):
    if not is_admin(message.from_user.id):
        return
    bot.reply_to(message, "Main menu opened.", reply_markup=main_menu_keyboard())


@bot.message_handler(func=lambda m: m.text in ["🏠 Dashboard", "❓ Help", "⚙️ Setup", "📡 Userbot", "📂 Sources", "🎯 Target", "🧲 Monitor", "🚀 Transfer", "📊 Status"])
def menu_router(message):
    if not is_admin(message.from_user.id):
        return
    text = message.text
    set_heartbeat("poll")
    if text == "🏠 Dashboard":

        bot.reply_to(message, dashboard_text(), parse_mode="Markdown", reply_markup=main_menu_keyboard())
        bot.send_message(message.chat.id, "🎮 *Interactive Control Panel*", reply_markup=dashboard_inline_keyboard(), parse_mode="Markdown")
    elif text == "❓ Help":
        bot.reply_to(
            message,
            (
                "Commands:\n"
                "/setapiid <id>\n/setapihash <hash>\n/setsession <session>\n/login\n"
                "/startuserbot\n/showapi\n/clearapi\n\n"
                "/settarget <chat_id>\n/showtarget\n"
                "/setsource <chat_id>\n/delsource <chat_id>\n/showsources\n/listgroups [page]\n\n"
            "/monitoron <chat_id>\n/monitoroff <chat_id>\n/monitorstatus <chat_id>\n/release <chat_id> <N>\n\n"
                "/collecthistory <chat_id> <N>\n/collectall <N_per_source>\n/collectfull <chat_id>\n/cancelcollect <chat_id>\n\n"
                "/autoon /autooff\n/sendsource <chat_id> <N>\n/sendlast <N>\n/resendlast <N>\n/showmedia <N>\n/clearsent"
            ),
            reply_markup=main_menu_keyboard()
        )
    elif text == "⚙️ Setup":
        bot.reply_to(
            message,
            "⚙️ *System Configuration*\n\nFollow the steps below to connect your Telegram account. You need an API ID and Hash from [my.telegram.org](https://my.telegram.org).",
            parse_mode="Markdown",
            reply_markup=setup_inline_keyboard()
        )
    elif text == "📡 Userbot":
        bot.reply_to(
            message,
            "Userbot controls:\n/startuserbot\n/status",
            reply_markup=main_menu_keyboard()
        )
    elif text == "📂 Sources":
        bot.reply_to(
            message,
            "📂 *Managed Sources*\n\n_Select a source to configure or monitor._",
            parse_mode="Markdown",
            reply_markup=sources_list_keyboard(0)
        )
    elif text == "🎯 Target":
        t = get_target_ref() or "Not set"
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🔄 Change Target", callback_data="target_change_prompt"))
        kb.add(InlineKeyboardButton("🔙 Dashboard", callback_data="dash_main"))
        bot.reply_to(
            message,
            f"🎯 *Target Configuration*\n\nCurrent Target: `{t}`",
            parse_mode="Markdown",
            reply_markup=kb
        )
    elif text == "🧲 Monitor":
        bot.reply_to(
            message,
            "🧲 *Monitoring Console*\n\n_Manage which sources are currently being watched for new media._",
            parse_mode="Markdown",
            reply_markup=sources_list_keyboard(0)
        )
    elif text == "🚀 Transfer":
        bot.reply_to(
            message,
            "Transfer controls:\n/autoon (live forward)\n/autooff\n/sendsource <chat_id> <N>\n/sendlast <N>\n/resendlast <N>",
            reply_markup=main_menu_keyboard()
        )
    elif text == "📊 Status":
        bot.reply_to(message, dashboard_text(), parse_mode="Markdown", reply_markup=dashboard_inline_keyboard())


@bot.message_handler(commands=["settarget"])
def cmd_settarget(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /settarget -1001234567890 OR /settarget @channelusername")
        return
    target_ref = parts[1].strip()
    if target_ref.startswith("https://t.me/"):
        target_ref = "@" + target_ref.split("/")[-1]
    if target_ref.startswith("t.me/"):
        target_ref = "@" + target_ref.split("/")[-1]
    set_setting("target_chat_ref", target_ref)
    try:
        set_setting("target_chat_id", int(target_ref))
    except Exception:
        pass
    bot.reply_to(message, f"✅ Target set to: `{target_ref}`", parse_mode="Markdown", reply_markup=main_menu_keyboard())

@bot.message_handler(commands=["showtarget"])
def cmd_showtarget(message):
    if not is_admin(message.from_user.id):
        return
    t = get_target_ref() or "Not set"
    bot.reply_to(message, f"🎯 Current target: `{t}`", parse_mode="Markdown", reply_markup=main_menu_keyboard())


@bot.message_handler(commands=["checktarget"])
def cmd_checktarget(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return
    target_ref = get_target_ref()
    if not target_ref:
        bot.reply_to(message, "Set target first with /settarget.")
        return

    async def run_check():
        try:
            me = await userbot.get_me()
            chat = await resolve_target_id(userbot, target_ref)
            title = chat.title or chat.first_name or str(chat.id)
            bot.reply_to(
                message,
                f"✅ Target reachable.\nUserbot ID: `{me.id}`\nTarget: `{title}`\nResolved ID: `{chat.id}`",
                parse_mode="Markdown"
            )
        except Exception as e:
            bot.reply_to(message, f"❌ Target access failed for current userbot session:\n`{e}`", parse_mode="Markdown")


    asyncio.run_coroutine_threadsafe(run_check(), loop)


@bot.message_handler(commands=["setapiid"])
def cmd_setapiid(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /setapiid 123456")
        return
    try:
        api_id = int(parts[1])
        if api_id <= 0:
            raise ValueError
        set_setting("api_id", api_id)
        bot.reply_to(message, "✅ API ID saved.", reply_markup=main_menu_keyboard())
    except ValueError:
        bot.reply_to(message, "Invalid API ID.")


@bot.message_handler(commands=["setapihash"])
def cmd_setapihash(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        bot.reply_to(message, "Usage: /setapihash <api_hash>")
        return
    set_setting("api_hash", parts[1].strip())
    bot.reply_to(message, "✅ API Hash saved.", reply_markup=main_menu_keyboard())


@bot.message_handler(commands=["setsession"])
def cmd_setsession(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        bot.reply_to(message, "Usage: /setsession <string_session>")
        return
    set_setting("user_session_string", parts[1].strip())
    bot.reply_to(message, "✅ User session saved.", reply_markup=main_menu_keyboard())


@bot.message_handler(commands=["showapi"])
def cmd_showapi(message):
    if not is_admin(message.from_user.id):
        return
    api_id = get_setting("api_id", "") or API_ID
    api_hash = get_setting("api_hash", "") or API_HASH
    session_val = get_setting("user_session_string", "") or USER_SESSION_STRING

    hash_mask = (api_hash[:4] + "..." + api_hash[-4:]) if api_hash and len(api_hash) > 8 else ("set" if api_hash else "not set")
    sess_mask = ("set" if session_val else "not set")
    bot.reply_to(
        message,
        f"API ID: `{api_id}`\nAPI Hash: `{hash_mask}`\nSession: `{sess_mask}`",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )


@bot.message_handler(commands=["clearapi"])
def cmd_clearapi(message):
    if not is_admin(message.from_user.id):
        return
    set_setting("api_id", "")
    set_setting("api_hash", "")
    set_setting("user_session_string", "")
    bot.reply_to(message, "🧹 Stored API ID/API Hash/Session removed.", reply_markup=main_menu_keyboard())


@bot.message_handler(commands=["startuserbot"])
def cmd_startuserbot(message):
    if not is_admin(message.from_user.id):
        return

    async def run():
        ok, msg = await start_or_reload_userbot()
        bot.reply_to(message, msg)

    asyncio.run_coroutine_threadsafe(run(), loop)
    bot.reply_to(message, "Starting/reloading userbot...")


@bot.message_handler(commands=["login"])
def cmd_login(message):
    if not is_admin(message.from_user.id):
        return
    uid = message.from_user.id
    admin_states[uid] = "awaiting_phone"
    login_data[uid] = {}
    bot.reply_to(message, "Send phone number with country code.\nExample: `+14155550123`", parse_mode="Markdown")


@bot.message_handler(func=lambda m: m.from_user and m.from_user.id in admin_states, content_types=["text"])
def admin_state_handler(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    if not state:
        return

    # --- Scraping States ---
    if state.startswith("awaiting_scrape_count_"):
        cid = int(state.split("_")[-1])
        try:
            count = int(message.text.strip())
            admin_states.pop(uid, None)
            bot.reply_to(message, f"🔢 Starting scrape of `{count}` messages for `{cid}`...", parse_mode="Markdown")
            async def run_c():
                await advanced_scrape_task(message.chat.id, cid, limit=count)
            asyncio.run_coroutine_threadsafe(run_c(), loop)
        except ValueError:
            bot.reply_to(message, "❌ Invalid number. Please enter digits only.")
        return

    if state.startswith("awaiting_scrape_date_start_"):
        cid = int(state.split("_")[-1])
        date_str = message.text.strip()
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            admin_states[uid] = f"awaiting_scrape_date_end_{cid}_{date_str}"
            bot.reply_to(message, f"📅 *Start Date Set:* `{date_str}`\n\nNow enter the *End Date* (Format: `YYYY-MM-DD`)\n(Or type `today`)", parse_mode="Markdown")
        except ValueError:
            bot.reply_to(message, "❌ Invalid date format. Use `YYYY-MM-DD`.")
        return

    if state.startswith("awaiting_scrape_date_end_"):
        parts = state.split("_")
        cid = int(parts[4])
        start_date_str = parts[5]
        end_date_str = message.text.strip().lower()
        if end_date_str == "today":
            end_date_str = datetime.now().strftime("%Y-%m-%d")
        
        try:
            datetime.strptime(end_date_str, "%Y-%m-%d")
            admin_states.pop(uid, None)
            bot.reply_to(message, f"📅 Starting date-range scrape for `{cid}`\nFrom `{start_date_str}` to `{end_date_str}`...")
            async def run_d():
                await advanced_scrape_task(message.chat.id, cid, start_date=start_date_str, end_date=end_date_str)
            asyncio.run_coroutine_threadsafe(run_d(), loop)
        except ValueError:
            bot.reply_to(message, "❌ Invalid date format. Use `YYYY-MM-DD`.")
        return

    # --- Release States ---
    if state.startswith("awaiting_release_count_"):
        cid = int(state.split("_")[-1])
        text = message.text.strip().lower()
        admin_states.pop(uid, None)
        
        limit = None
        if text != "all":
            try:
                limit = int(text)
            except ValueError:
                bot.reply_to(message, "❌ Invalid number. Release cancelled.")
                return
        
        bot.reply_to(message, f"🚀 Starting release for `{cid}`...")
        async def run_rel():
            await release_engine(message.chat.id, source_chat_id=cid, limit=limit)
        asyncio.run_coroutine_threadsafe(run_rel(), loop)
        return

    # --- Login States ---



    if state == "awaiting_phone":
        phone = message.text.strip()
        api_id, api_hash, _ = get_runtime_credentials()
        if not api_id or not api_hash:
            bot.reply_to(message, "Set API credentials first: /setapiid and /setapihash")
            admin_states.pop(uid, None)
            login_data.pop(uid, None)
            return

        async def send_code():
            try:
                tmp = build_temp_client(api_id, api_hash)
                await tmp.connect()
                sent = await tmp.send_code(phone)
                login_data[uid] = {
                    "phone": phone,
                    "api_id": api_id,
                    "api_hash": api_hash,
                    "tmp_client": tmp,
                    "phone_code_hash": sent.phone_code_hash
                }
                admin_states[uid] = "awaiting_otp"
                bot.reply_to(message, "OTP sent. Send the code (digits only).")
            except Exception as e:
                admin_states.pop(uid, None)
                login_data.pop(uid, None)
                bot.reply_to(message, f"Failed to send OTP: {e}")

        asyncio.run_coroutine_threadsafe(send_code(), loop)
        return

    if state == "awaiting_otp":
        otp = message.text.strip().replace(" ", "")
        data = login_data.get(uid, {})
        tmp: Client = data.get("tmp_client")
        if not tmp:
            bot.reply_to(message, "Login session expired. Run /login again.")
            admin_states.pop(uid, None)
            login_data.pop(uid, None)
            return

        async def sign_in():
            try:
                await tmp.sign_in(
                    phone_number=data["phone"],
                    phone_code_hash=data["phone_code_hash"],
                    phone_code=otp
                )
                s = await tmp.export_session_string()
                set_setting("user_session_string", s)
                await tmp.disconnect()
                admin_states.pop(uid, None)
                login_data.pop(uid, None)
                bot.reply_to(message, "Session generated and saved.\nRestart service to apply.")
            except SessionPasswordNeeded:
                admin_states[uid] = "awaiting_2fa"
                bot.reply_to(message, "2FA enabled. Send your Telegram 2FA password.")
            except Exception as e:
                try:
                    await tmp.disconnect()
                except Exception:
                    pass
                admin_states.pop(uid, None)
                login_data.pop(uid, None)
                bot.reply_to(message, f"Login failed: {e}")

        asyncio.run_coroutine_threadsafe(sign_in(), loop)
        return

    if state == "awaiting_2fa":
        password = message.text.strip()
        data = login_data.get(uid, {})
        tmp: Client = data.get("tmp_client")
        if not tmp:
            bot.reply_to(message, "Login session expired. Run /login again.")
            admin_states.pop(uid, None)
            login_data.pop(uid, None)
            return

        async def finish_2fa():
            try:
                await tmp.check_password(password)
                s = await tmp.export_session_string()
                set_setting("user_session_string", s)
                await tmp.disconnect()
                admin_states.pop(uid, None)
                login_data.pop(uid, None)
                bot.reply_to(message, "Session generated and saved.\nRestart service to apply.")
            except Exception as e:
                try:
                    await tmp.disconnect()
                except Exception:
                    pass
                admin_states.pop(uid, None)
                login_data.pop(uid, None)
                bot.reply_to(message, f"2FA failed: {e}")

        asyncio.run_coroutine_threadsafe(finish_2fa(), loop)
        return


@bot.message_handler(commands=["autoon"])
def cmd_autoon(message):
    if not is_admin(message.from_user.id):
        return
    set_setting("auto_forward", "true")
    bot.reply_to(message, "Auto-forward enabled.")


@bot.message_handler(commands=["autooff"])
def cmd_autooff(message):
    if not is_admin(message.from_user.id):
        return
    set_setting("auto_forward", "false")
    bot.reply_to(message, "Auto-forward disabled.")


@bot.message_handler(commands=["status"])
def cmd_status(message):
    if not is_admin(message.from_user.id):
        return
    t = get_target_ref() or "Not set"
    a = get_setting("auto_forward", "false")
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM sent_messages")
        sent_count = c.fetchone()[0]
    bot.reply_to(
        message,
        f"Target: `{t}`\nAuto: `{a}`\nSent records: `{sent_count}`\nSources: `{len(list_source_chats())}`",
        parse_mode="Markdown"
    )


@bot.message_handler(commands=["listgroups"])
def cmd_listgroups(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return

    page = 0
    parts = message.text.split()
    if len(parts) == 2:
        try:
            page = max(0, int(parts[1]) - 1)
        except ValueError:
            pass

    async def run_list():
        dialogs = []
        async for d in userbot.get_dialogs():
            ct = d.chat.type.name if d.chat and d.chat.type else ""
            if ct in ["GROUP", "SUPERGROUP", "CHANNEL"]:
                dialogs.append((d.chat.id, d.chat.title or str(d.chat.id), ct))
        total = len(dialogs)
        per_page = 20
        total_pages = max(1, (total + per_page - 1) // per_page)
        p = min(page, total_pages - 1)
        chunk = dialogs[p * per_page:(p + 1) * per_page]

        lines = [f"Total groups/channels: {total}", f"Page {p+1}/{total_pages}", ""]
        for i, (cid, title, ctype) in enumerate(chunk, start=1 + p * per_page):
            lines.append(f"{i}. {title} | id={cid} | {ctype}")

        text = "\n".join(lines)
        if len(text) > 3500:
            text = text[:3500] + "\n..."
        bot.reply_to(message, f"<pre>{text}</pre>", parse_mode="HTML")

    asyncio.run_coroutine_threadsafe(run_list(), loop)
    bot.reply_to(message, "Fetching groups/channels...")


@bot.message_handler(commands=["setsource"])
def cmd_setsource(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /setsource -1001234567890")
        return
    try:
        chat_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "Invalid chat id.")
        return

    async def run_set():
        try:
            chat = await userbot.get_chat(chat_id)
            title = chat.title or str(chat_id)
            add_source_chat(chat_id, title)
            bot.reply_to(message, f"Added source: `{title}` (`{chat_id}`)", parse_mode="Markdown")
        except Exception as e:
            bot.reply_to(message, f"Failed to add source: {e}")

    asyncio.run_coroutine_threadsafe(run_set(), loop)
    bot.reply_to(message, "Validating source chat...")


@bot.message_handler(commands=["delsource"])
def cmd_delsource(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /delsource -1001234567890")
        return
    try:
        chat_id = int(parts[1])
        remove_source_chat(chat_id)
        bot.reply_to(message, "Source removed.")
    except ValueError:
        bot.reply_to(message, "Invalid chat id.")


@bot.message_handler(commands=["showsources"])
def cmd_showsources(message):
    if not is_admin(message.from_user.id):
        return
    rows = list_source_chats()
    if not rows:
        bot.reply_to(message, "No source chats configured. Use /setsource <chat_id>.")
        return
    lines = [f"Configured sources: {len(rows)}", ""]
    for i, (cid, title, monitor_enabled) in enumerate(rows, start=1):
        mon = "ON" if int(monitor_enabled or 0) == 1 else "OFF"
        lines.append(f"{i}. {title or cid} | id={cid} | monitor={mon}")
    text = "\n".join(lines)
    if len(text) > 3500:
        text = text[:3500] + "\n..."
    bot.reply_to(message, f"<pre>{text}</pre>", parse_mode="HTML")


@bot.message_handler(commands=["monitoron"])
def cmd_monitoron(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /monitoron -1001234567890")
        return
    try:
        chat_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "Invalid chat id.")
        return
    if not is_source_chat(chat_id):
        bot.reply_to(message, "Add this as source first: /setsource <chat_id>")
        return
    set_monitor_enabled(chat_id, True)
    bot.reply_to(message, "✅ Monitoring enabled for this source.")


@bot.message_handler(commands=["monitoroff"])
def cmd_monitoroff(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /monitoroff -1001234567890")
        return
    try:
        chat_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "Invalid chat id.")
        return
    if not is_source_chat(chat_id):
        bot.reply_to(message, "This chat is not in sources.")
        return
    set_monitor_enabled(chat_id, False)
    bot.reply_to(message, "🛑 Monitoring disabled for this source.")


@bot.message_handler(commands=["monitorstatus"])
def cmd_monitorstatus(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /monitorstatus -1001234567890")
        return
    try:
        chat_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "Invalid chat id.")
        return
    if not is_source_chat(chat_id):
        bot.reply_to(message, "This chat is not in sources.")
        return
    stats = get_monitor_stats(chat_id)
    mon = "ON" if is_monitor_enabled(chat_id) else "OFF"
    bot.reply_to(
        message,
        f"Source: `{chat_id}`\nMonitor: `{mon}`\nCollected: `{stats['total']}`\nUnreleased: `{stats['unreleased']}`\nReleased: `{stats['released']}`",
        parse_mode="Markdown"
    )


@bot.message_handler(commands=["release"])
def cmd_release(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return
    parts = message.text.split()
    if len(parts) != 3:
        bot.reply_to(message, "Usage: /release -1001234567890 20")
        return
    try:
        source_chat_id = int(parts[1])
        n = int(parts[2])
        if n < 1:
            bot.reply_to(message, "N must be >= 1")
            return
    except ValueError:
        bot.reply_to(message, "Invalid source chat id or N.")
        return

    target_raw = get_target_ref()
    if not target_raw:
        bot.reply_to(message, "Set target first with /settarget")
        return
    target_id = target_raw

    async def run_release():
        # Validate target access early
        try:
            await userbot.get_chat(target_id)
        except Exception as e:
            bot.reply_to(message, f"❌ Cannot access target chat `{target_id}`: {e}", parse_mode="Markdown")
            return

        items = get_unreleased_collected(source_chat_id, n)
        if not items:
            bot.reply_to(message, "No unreleased collected media for this source.")
            return
        sent = 0
        failed = 0
        fail_reasons = []
        for row_id, source_message_id, media_type in items:
            try:
                delivered = False
                # 1) Try copy (keeps source clean, common success path)
                try:
                    await userbot.copy_message(target_id, source_chat_id, source_message_id)
                    delivered = True
                except Exception as e1:
                    # 2) Fallback: try forward
                    try:
                        await userbot.forward_messages(target_id, source_chat_id, source_message_id)
                        delivered = True
                    except Exception as e2:
                        failed += 1
                        reason = f"{source_message_id}: copy={e1} | forward={e2}"
                        logger.error(f"Release failed for {source_chat_id}:{reason}")
                        if len(fail_reasons) < 5:
                            fail_reasons.append(reason)

                if delivered:
                    (mark_collected_released(row_id, int(target_raw)) if str(target_raw).lstrip("-").isdigit() else mark_collected_released(row_id, 0))
                    sent += 1
                await asyncio.sleep(0.7)
            except Exception as e:
                failed += 1
                logger.error(f"Release failed for {source_chat_id}:{source_message_id}: {e}")
                if len(fail_reasons) < 5:
                    fail_reasons.append(f"{source_message_id}: {e}")

        summary = f"✅ Released {sent}/{len(items)} media to target."
        if failed > 0:
            summary += f"\n❌ Failed: {failed}"
            if fail_reasons:
                summary += "\n\nFirst errors:\n" + "\n".join(f"- {r}" for r in fail_reasons)
        bot.reply_to(message, summary)

    asyncio.run_coroutine_threadsafe(run_release(), loop)
    bot.reply_to(message, "Started release job...")


@bot.message_handler(commands=["collecthistory"])
def cmd_collecthistory(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return
    parts = message.text.split()
    if len(parts) != 3:
        bot.reply_to(message, "Usage: /collecthistory -1001234567890 200")
        return
    try:
        source_chat_id = int(parts[1])
        n = int(parts[2])
        if n < 1:
            bot.reply_to(message, "N must be >= 1")
            return
    except ValueError:
        bot.reply_to(message, "Invalid source chat id or N.")
        return
    if not is_source_chat(source_chat_id):
        bot.reply_to(message, "Add this source first with /setsource <chat_id>.")
        return

    async def run_collect():
        collected = 0
        scanned = 0
        mode = get_setting("forward_mode", "media")
        async for m in userbot.get_chat_history(source_chat_id, limit=n):
            scanned += 1
            is_media = bool(m.media)
            if mode == "media" and not is_media:
                continue
            
            media_type = m.media.value if is_media else "text"
            ok = save_collected_media(source_chat_id, m.id, media_type, m.caption or m.text or "")

            if ok:
                collected += 1
            await asyncio.sleep(0.05)
        title = get_source_title(source_chat_id) or str(source_chat_id)
        bot.reply_to(message, f"✅ Collected `{collected}` new media from `{title}` (scanned `{scanned}` messages).", parse_mode="Markdown")

    asyncio.run_coroutine_threadsafe(run_collect(), loop)
    bot.reply_to(message, "Started history collection...")


@bot.message_handler(commands=["collectall"])
def cmd_collectall(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /collectall 500")
        return
    try:
        per_source_limit = int(parts[1])
        if per_source_limit < 1:
            bot.reply_to(message, "N_per_source must be >= 1")
            return
    except ValueError:
        bot.reply_to(message, "Invalid N_per_source.")
        return

    sources = list_source_chats()
    if not sources:
        bot.reply_to(message, "No sources configured. Add with /setsource first.")
        return

    async def run_collectall():
        total_collected = 0
        total_scanned = 0
        per_source_results = []

        for source_row in sources:
            source_chat_id = int(source_row[0])
            source_title = source_row[1] or str(source_chat_id)
            source_collected = 0
            source_scanned = 0
            mode = get_setting("forward_mode", "media")
            try:
                async for m in userbot.get_chat_history(source_chat_id, limit=per_source_limit):
                    source_scanned += 1
                    is_media = bool(m.media)
                    if mode == "media" and not is_media:
                        continue

                    media_type = m.media.value if is_media else "text"
                    ok = save_collected_media(source_chat_id, m.id, media_type, m.caption or m.text or "")

                    if ok:
                        source_collected += 1
                    await asyncio.sleep(0.03)
            except Exception as e:
                logger.error(f"collectall failed for {source_chat_id}: {e}")

            total_collected += source_collected
            total_scanned += source_scanned
            per_source_results.append((source_title, source_chat_id, source_collected, source_scanned))

        lines = [
            f"✅ CollectAll done.",
            f"Sources: {len(per_source_results)}",
            f"Collected: {total_collected}",
            f"Scanned: {total_scanned}",
            ""
        ]
        for title, cid, ccount, scount in per_source_results:
            lines.append(f"- {title} ({cid}): +{ccount} collected, {scount} scanned")
        text = "\n".join(lines)
        if len(text) > 3500:
            text = text[:3500] + "\n..."
        bot.reply_to(message, f"<pre>{text}</pre>", parse_mode="HTML")

    asyncio.run_coroutine_threadsafe(run_collectall(), loop)
    bot.reply_to(message, f"Started collect-all for {len(sources)} source(s)...")


@bot.message_handler(commands=["collectfull"])
def cmd_collectfull(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /collectfull -1001234567890")
        return
    try:
        source_chat_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "Invalid source chat id.")
        return
    if not is_source_chat(source_chat_id):
        bot.reply_to(message, "Add this source first with /setsource <chat_id>.")
        return

    key = str(source_chat_id)
    active_collect_full[key] = True

    async def run_collect_full():
        collected = 0
        scanned = 0
        try:
            async for m in userbot.get_chat_history(source_chat_id):
                if not active_collect_full.get(key, False):
                    break
                scanned += 1
                if m.media:
                    media_type = m.media.value if m.media else "unknown"
                    ok = save_collected_media(source_chat_id, m.id, media_type, m.caption or "")
                    if ok:
                        collected += 1
                if scanned % 500 == 0:
                    bot.reply_to(message, f"CollectFull progress for `{source_chat_id}`: scanned `{scanned}`, collected `{collected}`", parse_mode="Markdown")
                await asyncio.sleep(0.01)
        except Exception as e:
            bot.reply_to(message, f"CollectFull error: {e}")
        finally:
            active_collect_full.pop(key, None)
            bot.reply_to(message, f"✅ CollectFull finished for `{source_chat_id}`. Scanned `{scanned}`, collected `{collected}`.", parse_mode="Markdown")

    asyncio.run_coroutine_threadsafe(run_collect_full(), loop)
    bot.reply_to(message, f"Started full history collection for `{source_chat_id}`. Use /cancelcollect {source_chat_id} to stop.", parse_mode="Markdown")


@bot.message_handler(commands=["cancelcollect"])
def cmd_cancelcollect(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /cancelcollect -1001234567890")
        return
    try:
        source_chat_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "Invalid source chat id.")
        return
    key = str(source_chat_id)
    if active_collect_full.get(key, False):
        active_collect_full[key] = False
        bot.reply_to(message, f"Stopping full collection for `{source_chat_id}`...", parse_mode="Markdown")
    else:
        bot.reply_to(message, "No active full-collection job for that source.")


@bot.message_handler(commands=["sendsource"])
def cmd_sendsource(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return

    parts = message.text.split()
    if len(parts) != 3:
        bot.reply_to(message, "Usage: /sendsource -1001234567890 20")
        return
    try:
        src_chat_id = int(parts[1])
        n = int(parts[2])
        if n < 1:
            bot.reply_to(message, "N must be >= 1")
            return
    except ValueError:
        bot.reply_to(message, "Invalid chat_id or N.")
        return

    if not is_source_chat(src_chat_id):
        bot.reply_to(message, "This chat is not configured as source. Add with /setsource <chat_id> first.")
        return

    async def run_send_source():
        target_raw = get_target_ref()
        if not target_raw:
            bot.reply_to(message, "Set target first with /settarget")
            return
        target_id = target_raw
        count = 0
        scan_limit = max(50, n * 15)
        async for m in userbot.get_chat_history(src_chat_id, limit=scan_limit):
            if count >= n:
                break
            if not m.media:
                continue
            if already_sent(m.id):
                continue
            try:
                await userbot.copy_message(target_id, src_chat_id, m.id)
                (mark_sent(m.id, int(target_raw)) if str(target_raw).lstrip("-").isdigit() else mark_sent(m.id, 0))
                count += 1
                await asyncio.sleep(0.7)
            except Exception as e:
                logger.error(f"sendsource failed for {src_chat_id}:{m.id}: {e}")
        bot.reply_to(message, f"Done. Sent {count} media from `{src_chat_id}` to target.", parse_mode="Markdown")

    asyncio.run_coroutine_threadsafe(run_send_source(), loop)
    bot.reply_to(message, "Started source transfer...")


@bot.message_handler(commands=["sendlast"])
def cmd_sendlast(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Configure API/session and restart.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /sendlast 20")
        return
    try:
        n = int(parts[1])
        if n < 1:
            bot.reply_to(message, "N must be >= 1")
            return
    except ValueError:
        bot.reply_to(message, "N must be a number")
        return

    async def run_sendlast(force=False):
        target_raw = get_target_ref()
        if not target_raw:
            bot.reply_to(message, "Set target first with /settarget")
            return
        target_id = target_raw
        me = await userbot.get_me()
        count = 0
        scan_limit = max(50, n * 15)
        async for m in userbot.get_chat_history(me.id, limit=scan_limit):
            if count >= n:
                break
            if not m.media:
                continue
            if (not force) and already_sent(m.id):
                continue
            try:
                await userbot.copy_message(target_id, me.id, m.id)
                (mark_sent(m.id, int(target_raw)) if str(target_raw).lstrip("-").isdigit() else mark_sent(m.id, 0))
                count += 1
                await asyncio.sleep(0.7)
            except Exception as e:
                logger.error(f"sendlast failed for {m.id}: {e}")
        mode = "force" if force else "normal"
        bot.reply_to(message, f"Done ({mode}). Sent {count} media to target.")

    asyncio.run_coroutine_threadsafe(run_sendlast(force=False), loop)
    bot.reply_to(message, "Started sending...")


@bot.message_handler(commands=["resendlast"])
def cmd_resendlast(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Configure API/session and restart or /startuserbot.")
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /resendlast 20")
        return
    try:
        n = int(parts[1])
        if n < 1:
            bot.reply_to(message, "N must be >= 1")
            return
    except ValueError:
        bot.reply_to(message, "N must be a number")
        return

    async def run_resend():
        target_raw = get_target_ref()
        if not target_raw:
            bot.reply_to(message, "Set target first with /settarget")
            return
        target_id = target_raw
        me = await userbot.get_me()
        count = 0
        scan_limit = max(50, n * 15)
        async for m in userbot.get_chat_history(me.id, limit=scan_limit):
            if count >= n:
                break
            if not m.media:
                continue
            try:
                await userbot.copy_message(target_id, me.id, m.id)
                # keep history updated even in force mode
                (mark_sent(m.id, int(target_raw)) if str(target_raw).lstrip("-").isdigit() else mark_sent(m.id, 0))
                count += 1
                await asyncio.sleep(0.7)
            except Exception as e:
                logger.error(f"resendlast failed for {m.id}: {e}")
        bot.reply_to(message, f"Done (force). Sent {count} media to target.")

    asyncio.run_coroutine_threadsafe(run_resend(), loop)
    bot.reply_to(message, "Started force resend...")


@bot.message_handler(commands=["clearsent"])
def cmd_clearsent(message):
    if not is_admin(message.from_user.id):
        return
    clear_sent_records()
    bot.reply_to(message, "Sent-history cleared. You can run /sendlast again.")


@bot.message_handler(commands=["showmedia"])
def cmd_showmedia(message):
    global userbot
    if not is_admin(message.from_user.id):
        return
    if userbot is None:
        bot.reply_to(message, "Userbot is not running. Use /startuserbot first.")
        return

    parts = message.text.split()
    n = 10
    if len(parts) == 2:
        try:
            n = int(parts[1])
            if n < 1:
                n = 10
        except ValueError:
            bot.reply_to(message, "Usage: /showmedia 10")
            return

    async def run_show():
        me = await userbot.get_me()
        rows = []
        scan_limit = max(50, n * 15)
        async for m in userbot.get_chat_history(me.id, limit=scan_limit):
            if len(rows) >= n:
                break
            if not m.media:
                continue
            media_type = m.media.value if m.media else "unknown"
            dt = m.date.strftime("%Y-%m-%d %H:%M")
            sent_flag = "sent" if already_sent(m.id) else "new"
            rows.append(f"{len(rows)+1}. id={m.id} | {media_type} | {dt} | {sent_flag}")

        if not rows:
            bot.reply_to(message, "No media found in Saved Messages.")
            return

        text = "Saved Messages media (recent):\n" + "\n".join(rows)
        # Telegram message size safety
        if len(text) > 3500:
            text = text[:3500] + "\n..."
        bot.reply_to(message, f"<pre>{text}</pre>", parse_mode="HTML")

    asyncio.run_coroutine_threadsafe(run_show(), loop)
    bot.reply_to(message, "Reading Saved Messages media...")


# -----------------------------
# Health + keepalive
# -----------------------------
async def advanced_scrape_task(admin_chat_id, source_chat_id, mode="count", limit=None, start_date=None, end_date=None):
    global userbot
    if not userbot:
        bot.send_message(admin_chat_id, "❌ Userbot not running.")
        return

    try:
        title = get_source_title(source_chat_id) or str(source_chat_id)
        collected = 0
        scanned = 0
        
        # Parse dates if provided
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if start_date else None
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if end_date else None
        
        # In Telegram, history goes backwards. So start_date (e.g. 2024) is OLDER than end_date (e.g. 2025).
        # We start from the end_date (newer) and go back to start_date (older).
        
        offset_date = end_dt if end_dt else None
        
        status_msg = bot.send_message(admin_chat_id, f"📥 Scraping `{title}`...")
        
        f_mode = get_setting("forward_mode", "media")

        async for m in userbot.get_chat_history(source_chat_id, limit=(limit or 1000000), offset_date=offset_date):
            scanned += 1
            
            # Check if we've passed the start date (gone too far back in time)
            if start_dt and m.date < start_dt:
                break
                
            is_media = bool(m.media)
            if f_mode == "media" and not is_media:
                continue

            media_type = m.media.value if is_media else "text"
            ok = save_collected_media(source_chat_id, m.id, media_type, m.caption or m.text or "")
            if ok:
                collected += 1
            
            if scanned % 100 == 0:
                try:
                    bot.edit_message_text(f"📥 Scraping `{title}`...\nScanned: `{scanned}`\nCollected: `{collected}`", admin_chat_id, status_msg.message_id)
                except: pass
            
            await asyncio.sleep(0.05)

        bot.send_message(admin_chat_id, f"✅ Done scraping `{title}`\n\nScanned: `{scanned}`\nCollected: `{collected}`")
    except Exception as e:
        logger.error(f"Scrape failed: {e}")
        bot.send_message(admin_chat_id, f"❌ Scrape failed for `{source_chat_id}`: {e}")


async def resolve_target_id(client: Client, target_ref: str):
    """Robustly resolve a chat ID or username into a Pyrogram chat object."""
    try:
        # 1. Try direct resolution (works if in cache)
        return await client.get_chat(target_ref)
    except Exception:
        # 2. Try parsing as integer and using get_chat
        try:
            if str(target_ref).lstrip("-").isdigit():
                return await client.get_chat(int(target_ref))
        except Exception: pass
        
        # 3. Last resort: Scan dialogs to 'meet' the peer
        async for dialog in client.get_dialogs(limit=50):
            if str(dialog.chat.id) == str(target_ref) or dialog.chat.username == str(target_ref).replace("@", ""):
                return dialog.chat
                
    raise ValueError(f"Could not find or 'meet' chat: {target_ref}. Try opening the group in your Telegram app first.")


async def release_engine(admin_chat_id, source_chat_id=None, limit=None):
    global userbot
    if not userbot:
        bot.send_message(admin_chat_id, "❌ Userbot not running.")
        return

    target_raw = get_target_ref()
    if not target_raw:
        bot.send_message(admin_chat_id, "❌ Set target first with /settarget")
        return
    
    # Resolve target ID robustly
    try:
        target_chat = await resolve_target_id(userbot, target_raw)
        target_id = target_chat.id
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Cannot access target `{target_raw}`: {e}")
        return


    try:
        # Get items to release
        # If source_chat_id is None, we need to get from all sources
        # For simplicity, we'll iterate through all sources if None
        sources = [source_chat_id] if source_chat_id else [row[0] for row in list_source_chats()]
        
        total_sent = 0
        total_failed = 0
        
        status_msg = bot.send_message(admin_chat_id, "🚀 *Release Engine Started*...", parse_mode="Markdown")

        for sid in sources:
            s_title = get_source_title(sid) or str(sid)
            # If we have a global limit and we've reached it, stop
            if limit and total_sent >= limit:
                break
                
            # Items for this specific source
            # If it's a global release, we don't have a per-source limit unless specified
            current_limit = (limit - total_sent) if limit else 1000000
            items = get_unreleased_collected(sid, current_limit)
            
            if not items:
                continue
                
            for row_id, source_message_id, media_type in items:
                try:
                    # Try copy, then forward
                    delivered = False
                    try:
                        await userbot.copy_message(target_id, sid, source_message_id)
                        delivered = True
                    except Exception:
                        try:
                            await userbot.forward_messages(target_id, sid, source_message_id)
                            delivered = True
                        except: pass

                    if delivered:
                        mark_collected_released(row_id, target_id)
                        total_sent += 1
                    else:
                        total_failed += 1
                        
                    if (total_sent + total_failed) % 10 == 0:
                        try:
                            bot.edit_message_text(f"🚀 *Release Engine Running*\n\nSent: `{total_sent}`\nFailed: `{total_failed}`\nCurrent Source: `{s_title}`", admin_chat_id, status_msg.message_id, parse_mode="Markdown")
                        except: pass
                        
                    await asyncio.sleep(0.7) # Flood protection
                    
                    if limit and total_sent >= limit:
                        break
                except Exception as e:
                    total_failed += 1
                    logger.error(f"Release engine error for {sid}:{source_message_id}: {e}")

        bot.send_message(admin_chat_id, f"✅ *Release Complete*\n\nTotal Sent: `{total_sent}`\nTotal Failed: `{total_failed}`", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Release engine crashed: {e}")
        bot.send_message(admin_chat_id, f"❌ Release engine crashed: {e}")


app = Flask(__name__)




@app.route("/")
def health():
    return "saved_to_target_userbot running", 200


def run_web():
    app.run(host="0.0.0.0", port=PORT)


def keep_alive_worker():
    """
    Periodically ping this service URL to reduce idle/sleep risk on some hosts.
    Auto-detects URL from env/providers; KEEP_ALIVE_URL still overrides.
    """
    detected_url = ""

    def detect_public_url() -> str:
        # 1) Explicit override
        if KEEP_ALIVE_URL.strip():
            return KEEP_ALIVE_URL.strip().rstrip("/")

        # 2) Common hosting provider envs
        render_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
        if render_url:
            return render_url.rstrip("/")

        railway_url = os.getenv("RAILWAY_STATIC_URL", "").strip()
        if railway_url:
            if railway_url.startswith("http://") or railway_url.startswith("https://"):
                return railway_url.rstrip("/")
            return f"https://{railway_url}".rstrip("/")

        fly_url = os.getenv("FLY_APP_NAME", "").strip()
        if fly_url:
            return f"https://{fly_url}.fly.dev".rstrip("/")

        # 3) Optional custom env
        web_url = os.getenv("WEB_URL", "").strip()
        if web_url:
            return web_url.rstrip("/")

        return ""

    while True:
        try:
            url = detect_public_url()
            if url and url != detected_url:
                detected_url = url
                logger.info(f"Keep-alive URL detected: {detected_url}")
            if url:
                requests.get(url, timeout=12)
            else:
                logger.info("Keep-alive URL not detected yet; skipping ping cycle.")
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")
        finally:
            # 10 min
            time.sleep(600)


def run_bot_polling_forever():
    """Run TeleBot polling forever with auto-restart on crashes/conflicts."""
    while True:
        try:
            logger.info("Starting bot polling loop...")
            bot.delete_webhook()
            bot.infinity_polling(
                skip_pending=True,
                timeout=30,
                long_polling_timeout=25
            )

        except Exception as e:
            logger.error(f"Bot polling crashed, retrying in 5s: {e}")
            time.sleep(5)


def userbot_watchdog():
    """Keep userbot alive by reloading credentials and restarting when disconnected."""
    while True:
        try:
            if userbot is None:
                time.sleep(8)
                continue
            if not userbot.is_connected:
                logger.warning("Userbot disconnected. Attempting auto-restart...")

                async def _restart():
                    ok, msg = await start_or_reload_userbot()
                    if ok:
                        logger.info(f"Watchdog restart success: {msg}")
                    else:
                        logger.error(f"Watchdog restart failed: {msg}")

                asyncio.run_coroutine_threadsafe(_restart(), loop)
        except Exception as e:
            logger.error(f"Userbot watchdog error: {e}")
        finally:
            time.sleep(12)


def supervisor_watchdog():
    """
    Strong anti-off supervision:
    - If poll heartbeat is stale, restart polling thread.
    - If userbot heartbeat is stale or disconnected, reload userbot.
    """
    poll_stale_seconds = 180
    userbot_stale_seconds = 300
    while True:
        try:
            poll_hb, user_hb = get_heartbeats()
            now = time.time()

            # Polling stale detector (logs only, infinity_polling handles itself)
            if now - poll_hb > poll_stale_seconds:
                logger.warning(f"Polling heartbeat stale ({int(now - poll_hb)}s). Bot might be idle.")
                # We don't spawn a new thread here anymore to avoid 409 conflicts


            # Userbot stale detector
            if now - user_hb > userbot_stale_seconds:
                logger.warning("Userbot heartbeat stale. Attempting userbot reload.")

                async def _reload():
                    ok, msg = await start_or_reload_userbot()
                    if ok:
                        logger.info(f"Supervisor reload success: {msg}")
                        set_heartbeat("userbot")
                    else:
                        logger.error(f"Supervisor reload failed: {msg}")

                asyncio.run_coroutine_threadsafe(_reload(), loop)
                set_heartbeat("userbot")
        except Exception as e:
            logger.error(f"Supervisor watchdog error: {e}")
        finally:
            time.sleep(30)


# -----------------------------
# Main
# -----------------------------
async def start_async():
    global userbot
    init_db()
    if get_setting("auto_forward") is None:
        set_setting("auto_forward", "false")

    ok, msg = await start_or_reload_userbot()
    if not ok:
        logger.warning("Userbot credentials incomplete. Admin bot will run; set creds using /setapiid /setapihash /setsession then restart.")
    else:
        logger.info(msg)

    # run resilient telebot polling in thread
    threading.Thread(target=run_bot_polling_forever, daemon=True).start()
    logger.info("Admin bot polling watchdog started")
    # run userbot watchdog
    threading.Thread(target=userbot_watchdog, daemon=True).start()
    logger.info("Userbot watchdog started")
    # run global supervisor
    threading.Thread(target=supervisor_watchdog, daemon=True).start()
    logger.info("Supervisor watchdog started")

    await idle()
    if userbot is not None:
        await userbot.stop()


if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    # Always start pinger; it auto-detects URL (or uses KEEP_ALIVE_URL override)
    threading.Thread(target=keep_alive_worker, daemon=True).start()
    loop.run_until_complete(start_async())



