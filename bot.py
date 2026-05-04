import os
import asyncio

# Fix for Pyrogram RuntimeError in Python 3.10+
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import threading
import logging
import requests
import psycopg2
from psycopg2 import pool as pg_pool
from contextlib import contextmanager
from dotenv import load_dotenv
from datetime import datetime

from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import SessionPasswordNeeded, Unauthorized
from pyrogram.storage import MemoryStorage

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from flask import Flask

# ==========================================
# ⚙️ CONFIGURATION
# ==========================================
load_dotenv()

API_ID = int(os.getenv("API_ID", "2040"))
API_HASH = os.getenv("API_HASH", "b18441a1ff607e10a989891a5462e627")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not all([BOT_TOKEN, DATABASE_URL]):
    print("WARNING: Missing essential environment variables (BOT_TOKEN, DATABASE_URL). Please check your .env file.")

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Ensure downloads directory exists
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ==========================================
# 🗄️ DATABASE OPERATIONS
# ==========================================
db_pool = None

def init_db_pool():
    global db_pool
    if db_pool is None and DATABASE_URL:
        try:
            db_pool = pg_pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
            logger.info("✅ Database connection pool initialized.")
        except Exception as e:
            logger.error(f"❌ Failed to initialize database pool: {e}")
            raise

@contextmanager
def get_connection():
    if db_pool is None:
        init_db_pool()
    conn = db_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        db_pool.putconn(conn)

def init_db():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                CREATE TABLE IF NOT EXISTS saved_media (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT,
                    source_chat_id BIGINT,
                    media_type TEXT,
                    file_path TEXT UNIQUE,
                    caption TEXT,
                    media_group_id TEXT,
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_released BOOLEAN DEFAULT FALSE
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS monitored_sources (
                    source_id BIGINT PRIMARY KEY,
                    title TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    allowed_media_types TEXT DEFAULT 'photo,video,document,audio,voice,animation',
                    min_file_size BIGINT DEFAULT 0,
                    caption_keywords TEXT DEFAULT '',
                    allowed_senders TEXT DEFAULT '',
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS target_groups (
                    target_id BIGINT PRIMARY KEY,
                    title TEXT
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS source_target_mapping (
                    id SERIAL PRIMARY KEY,
                    source_id BIGINT REFERENCES monitored_sources(source_id) ON DELETE CASCADE,
                    target_id BIGINT REFERENCES target_groups(target_id) ON DELETE CASCADE,
                    UNIQUE(source_id, target_id)
                )
            """)
            # Table to store the Pyrogram StringSession
            c.execute("""
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            logger.info("✅ Database schema initialized.")

# Media Queries
def save_media_record(message_id, source_chat_id, media_type, file_path, caption, media_group_id=None):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO saved_media (message_id, source_chat_id, media_type, file_path, caption, media_group_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (file_path) DO NOTHING
                """, (message_id, source_chat_id, media_type, file_path, caption, media_group_id))
                return c.rowcount > 0
    except Exception as e:
        logger.error(f"Error saving media record: {e}")
        return False

def get_unreleased_media(source_id=None):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                if source_id:
                    c.execute("""
                        SELECT id, file_path, media_type, caption 
                        FROM saved_media 
                        WHERE is_released = FALSE AND source_chat_id = %s
                        ORDER BY downloaded_at ASC
                    """, (source_id,))
                else:
                    c.execute("""
                        SELECT id, file_path, media_type, caption 
                        FROM saved_media 
                        WHERE is_released = FALSE
                        ORDER BY downloaded_at ASC
                    """)
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching unreleased media: {e}")
        return []

def mark_media_released(media_id):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE saved_media SET is_released = TRUE WHERE id = %s", (media_id,))
                return True
    except Exception as e:
        logger.error(f"Error marking media released: {e}")
        return False

# Group Management Queries
def add_monitored_source(source_id, title):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO monitored_sources (source_id, title, is_active)
                    VALUES (%s, %s, TRUE)
                    ON CONFLICT (source_id) DO UPDATE SET title = EXCLUDED.title, is_active = TRUE
                """, (source_id, title))
                return True
    except Exception as e:
        logger.error(f"Error adding monitored source: {e}")
        return False

def unregister_source(source_id):
    """Soft-delete a source when the bot is removed from it."""
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    "UPDATE monitored_sources SET is_active = FALSE WHERE source_id = %s",
                    (source_id,)
                )
                return True
    except Exception as e:
        logger.error(f"Error unregistering source: {e}")
        return False

def add_target_group(target_id, title):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO target_groups (target_id, title)
                    VALUES (%s, %s)
                    ON CONFLICT (target_id) DO UPDATE SET title = EXCLUDED.title
                """, (target_id, title))
                return True
    except Exception as e:
        logger.error(f"Error adding target group: {e}")
        return False

def add_mapping(source_id, target_id):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO source_target_mapping (source_id, target_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                """, (source_id, target_id))
                return True
    except Exception as e:
        logger.error(f"Error adding mapping: {e}")
        return False

def get_mappings():
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT m.source_id, s.title, m.target_id, t.title
                    FROM source_target_mapping m
                    JOIN monitored_sources s ON m.source_id = s.source_id
                    JOIN target_groups t ON m.target_id = t.target_id
                """)
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching mappings: {e}")
        return []

def get_targets_for_source(source_id):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT target_id FROM source_target_mapping WHERE source_id = %s", (source_id,))
                return [r[0] for r in c.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching targets for source: {e}")
        return []

def get_all_sources():
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT source_id, title FROM monitored_sources WHERE is_active = TRUE")
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching sources: {e}")
        return []

def get_all_targets():
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT target_id, title FROM target_groups")
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching targets: {e}")
        return []

def get_source_filters(source_id):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT allowed_media_types, min_file_size, caption_keywords, allowed_senders
                    FROM monitored_sources
                    WHERE source_id = %s
                """, (source_id,))
                row = c.fetchone()
                if row:
                    return {
                        'media_types': row[0],
                        'min_file_size': row[1],
                        'caption_keywords': row[2],
                        'allowed_senders': row[3]
                    }
                return None
    except Exception as e:
        logger.error(f"Error fetching source filters: {e}")
        return None

def update_source_filter(source_id, filter_key, value):
    valid_keys = ['allowed_media_types', 'min_file_size', 'caption_keywords', 'allowed_senders']
    if filter_key not in valid_keys:
        return False
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute(f"UPDATE monitored_sources SET {filter_key} = %s WHERE source_id = %s", (value, source_id))
                return True
    except Exception as e:
        logger.error(f"Error updating source filter: {e}")
        return False

def get_stats():
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT COUNT(*) FROM saved_media WHERE is_released = FALSE")
                unreleased = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM saved_media WHERE is_released = TRUE")
                released = c.fetchone()[0]
                return unreleased, released
    except:
        return 0, 0

# Session helpers
def get_session_string():
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT value FROM bot_settings WHERE key = 'session_string'")
                row = c.fetchone()
                return row[0] if row else None
    except:
        return None

def save_session_string(session_str):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO bot_settings (key, value) VALUES ('session_string', %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, (session_str,))
        return True
    except Exception as e:
        logger.error(f"Failed to save session: {e}")
        return False

# ==========================================
# 🤖 PYROGRAM USERBOT (LISTENER & DOWNLOADER)
# ==========================================

# Global user_client — created after login
user_client = None

def build_client(session_str):
    """Build a Pyrogram Client from a saved session string."""
    from pyrogram.storage import MemoryStorage
    return Client(
        name="userbot",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=session_str
    )

def register_userbot_handlers(client: Client):
    """Attach live listener to the client."""
    @client.on_message(filters.media & ~filters.me)
    async def live_listener(c: Client, message: Message):
        asyncio.create_task(download_media(c, message))

def passes_filters(message, filters_dict, is_pyrogram=True):
    """Check if a message passes the source filters. Works for Pyrogram and Telebot."""
    if not filters_dict:
        return True
        
    # 1. Media Type
    if is_pyrogram:
        media_type = message.media.value if message.media else None
    else:
        # For telebot, we calculate it differently
        if message.photo: media_type = "photo"
        elif message.video: media_type = "video"
        elif message.document: media_type = "document"
        elif message.audio: media_type = "audio"
        elif message.voice: media_type = "voice"
        elif message.animation: media_type = "animation"
        else: media_type = None

    if media_type and media_type not in filters_dict['media_types']:
        return False

    # 2. Minimum File Size
    min_size = filters_dict['min_file_size']
    if min_size > 0:
        file_size = 0
        if is_pyrogram:
            media_obj = getattr(message, media_type, None) if media_type else None
            file_size = getattr(media_obj, 'file_size', 0) if media_obj else 0
        else:
            # telebot
            if message.photo: file_size = message.photo[-1].file_size
            elif message.video: file_size = message.video.file_size
            elif message.document: file_size = message.document.file_size
            elif message.audio: file_size = message.audio.file_size
            elif message.voice: file_size = message.voice.file_size
            elif message.animation: file_size = message.animation.file_size
            
        if file_size is not None and file_size < min_size:
            return False

    # 3. Caption Keywords
    keywords = filters_dict['caption_keywords'].strip()
    if keywords:
        caption = message.caption or ""
        caption_lower = caption.lower()
        required_words = [w.strip().lower() for w in keywords.split(',') if w.strip()]
        if required_words and not any(w in caption_lower for w in required_words):
            return False

    # 4. Allowed Senders
    senders = filters_dict['allowed_senders'].strip()
    if senders:
        allowed_list = [s.strip().lower() for s in senders.split(',') if s.strip()]
        sender_id = str(message.from_user.id) if message.from_user else None
        
        # 'admin_only' check needs to be handled differently. For now we just check raw IDs
        if "admin_only" in allowed_list:
            # We skip admin check in raw filter for now, would require get_chat_member
            pass 
        elif sender_id and sender_id not in allowed_list:
            return False

    return True

async def download_media(client: Client, message: Message):
    if not message.media:
        return

    sources = get_all_sources()
    monitored_ids = [s[0] for s in sources]
    
    if message.chat.id not in monitored_ids:
        return

    # Check filters
    source_filters = get_source_filters(message.chat.id)
    if not passes_filters(message, source_filters, is_pyrogram=True):
        return

    try:
        media_type = message.media.value
        media_group_id = message.media_group_id
        logger.info(f"Downloading {media_type} from {message.chat.id}...")
        
        file_path = await client.download_media(
            message,
            file_name=f"{DOWNLOAD_DIR}/{message.chat.id}_{message.id}/"
        )
        
        if file_path:
            caption = message.caption or ""
            success = save_media_record(
                message.id,
                message.chat.id,
                media_type,
                file_path,
                caption,
                media_group_id
            )
            if success:
                logger.info(f"✅ Saved media record for {file_path}")
            else:
                logger.warning(f"⚠️ Media record already exists or failed for {file_path}")
    except Exception as e:
        logger.error(f"❌ Failed to download media: {e}")

# live_listener is registered dynamically via register_userbot_handlers() after login

active_scrapes = {}

async def scrape_history(chat_id: int, admin_chat_id: int, status_msg_id: int, limit: int = 0, start_date=None, end_date=None):
    """Scrape historical media from a specific chat with progress and cancellation."""
    scrape_id = f"{chat_id}_{status_msg_id}"
    active_scrapes[scrape_id] = False
    
    mode_text = f"dates: {start_date} to {end_date}" if start_date else f"limit: {limit}"
    logger.info(f"Starting historical scrape for {chat_id} ({mode_text})")
    
    count = 0
    scanned = 0
    try:
        if start_date and end_date:
            # Date mode
            async for message in user_client.get_chat_history(chat_id, offset_date=end_date):
                if active_scrapes.get(scrape_id, False):
                    break
                
                # Check if we passed the start date (Pyrogram iterates backward)
                if message.date < start_date:
                    break
                    
                scanned += 1
                if message.media:
                    await download_media(user_client, message)
                    count += 1
                    await asyncio.sleep(0.5)
                
                if scanned % 20 == 0:
                    markup = InlineKeyboardMarkup()
                    markup.row(InlineKeyboardButton("🛑 Cancel Scrape", callback_data=f"cancel_scrape_{scrape_id}"))
                    try:
                        bot.edit_message_text(f"⏳ *Scraping Progress...*\n\nScanned: `{scanned}` messages\nSaved: `{count}` media items\n\n_Scanning backwards by date..._", admin_chat_id, status_msg_id, reply_markup=markup, parse_mode="Markdown")
                    except: pass
        else:
            # Limit mode
            async for message in user_client.get_chat_history(chat_id, limit=limit):
                if active_scrapes.get(scrape_id, False):
                    break
                    
                scanned += 1
                if message.media:
                    await download_media(user_client, message)
                    count += 1
                    await asyncio.sleep(0.5)
                
                if scanned % 20 == 0:
                    markup = InlineKeyboardMarkup()
                    markup.row(InlineKeyboardButton("🛑 Cancel Scrape", callback_data=f"cancel_scrape_{scrape_id}"))
                    limit_text = f"`{limit}`" if limit > 0 else "All"
                    try:
                        bot.edit_message_text(f"⏳ *Scraping Progress...*\n\nScanned: `{scanned}` / {limit_text}\nSaved: `{count}` media items", admin_chat_id, status_msg_id, reply_markup=markup, parse_mode="Markdown")
                    except: pass
                    
    except Exception as e:
        logger.error(f"Error scraping history for {chat_id}: {e}")
    
    active_scrapes.pop(scrape_id, None)
    try:
        markup = InlineKeyboardMarkup()
        markup.row(InlineKeyboardButton("🔙 Back to Source Options", callback_data=f"src_options_{chat_id}"))
        bot.edit_message_text(f"✅ *Scrape Finished*\n\nScanned: `{scanned}`\nSaved: `{count}` media items", admin_chat_id, status_msg_id, reply_markup=markup, parse_mode="Markdown")
    except: pass
    logger.info(f"Finished scraping {chat_id}. Downloaded {count} media items.")

# ==========================================
# 🎛️ TELEBOT ADMIN DASHBOARD
# ==========================================
if BOT_TOKEN:
    bot = telebot.TeleBot(BOT_TOKEN)
else:
    bot = None

# admin_states stores per-user state
# Values: 'awaiting_phone', 'awaiting_otp', 'awaiting_2fa',
#         'awaiting_source', 'awaiting_target', 'awaiting_mapping'
admin_states = {}

# Temporary login data keyed by user_id
login_data = {}

def is_admin(user_id):
    if ADMIN_ID == 0:
        return True
    return user_id == ADMIN_ID

if bot:
    @bot.message_handler(commands=['start'])
    def start_cmd(message):
        if not is_admin(message.from_user.id):
            bot.reply_to(message, "❌ Unauthorized.")
            return
        show_main_menu(message.chat.id)

    def show_main_menu(chat_id, message_id=None):
        userbot_status = "✅ Connected" if user_client else "❌ Not Connected"
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("📊 Stats", callback_data="stats"),
            InlineKeyboardButton("📂 Sources", callback_data="sources")
        )
        markup.row(
            InlineKeyboardButton("🎯 Targets", callback_data="targets"),
            InlineKeyboardButton("🔗 Mappings", callback_data="mappings")
        )
        markup.row(InlineKeyboardButton("🚀 Release Media", callback_data="release"))
        markup.row(InlineKeyboardButton(f"🤖 Userbot: {userbot_status}", callback_data="userbot_menu"))

        text = (
            "💎 *Media Saver Pro — Dashboard*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "🟢 *Bot-as-Admin Mode*: Always active.\n"
            "  ↳ Add this bot as an admin to any group or channel to start saving media automatically.\n\n"
            f"🤖 *Userbot Mode*: {userbot_status}\n"
            "  ↳ Optional. Enables monitoring of restricted channels you cannot add the bot to."
        )
        try:
            if message_id:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="Markdown")
            else:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")
        except:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode="Markdown")

    @bot.callback_query_handler(func=lambda call: True)
    def callback_handler(call):
        global user_client
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Unauthorized", show_alert=True)
            return

        bot.answer_callback_query(call.id)

        if call.data == "userbot_menu":
            userbot_status = "✅ Connected" if user_client else "❌ Not Connected"
            markup = InlineKeyboardMarkup()
            if user_client:
                markup.row(InlineKeyboardButton("🔴 Disconnect Userbot", callback_data="disconnect_userbot"))
            else:
                markup.row(InlineKeyboardButton("🔗 Connect Userbot", callback_data="connect_userbot"))
            markup.row(InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
            text = (
                "🤖 *Userbot Mode*\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"Status: *{userbot_status}*\n\n"
                "The Userbot logs into your personal Telegram account to monitor *restricted channels* "
                "that don't allow bots.\n\n"
                "⚠️ *Bot-as-Admin mode works without this.* \n"
                "Only enable Userbot if you need to monitor channels where you cannot add the bot as admin."
            )
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data == "connect_userbot":
            admin_states[call.from_user.id] = "awaiting_phone"
            bot.send_message(
                call.message.chat.id,
                "📱 *Connect Userbot*\n\nSend your phone number in international format:\n`+1234567890`\n\n"
                "_Your number will only be used to generate a Telegram session._",
                parse_mode="Markdown"
            )
            return

        elif call.data == "disconnect_userbot":
            if user_client:
                asyncio.run_coroutine_threadsafe(user_client.stop(), loop)
                user_client = None
            save_session_string("")
            bot.edit_message_text(
                "✅ *Userbot Disconnected.*\n\nBot-as-Admin mode is still active.",
                call.message.chat.id, call.message.message_id, parse_mode="Markdown"
            )
            return
        
        if call.data == "stats":
            unreleased, released = get_stats()
            text = f"📊 *Statistics*\n\n📦 Unreleased: `{unreleased}`\n✅ Released: `{released}`"
            
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            
        elif call.data == "sources":
            sources = get_all_sources()
            text = "📂 *Monitored Sources*\n\n"
            markup = InlineKeyboardMarkup()
            
            if not sources:
                text += "No sources configured."
            else:
                for s_id, title in sources:
                    markup.row(InlineKeyboardButton(f"⚙️ {title}", callback_data=f"src_options_{s_id}"))
                    
            markup.row(InlineKeyboardButton("➕ Add Source", callback_data="add_source"))
            markup.row(InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
            
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data.startswith("src_options_"):
            # callback_data = "src_options_{s_id}" where s_id can be negative
            s_id = int(call.data[len("src_options_"):])
            markup = InlineKeyboardMarkup()
            if user_client:
                markup.row(InlineKeyboardButton("🕰 Scrape History (Userbot)", callback_data=f"scrape_{s_id}"))
            else:
                markup.row(InlineKeyboardButton("⚠️ Scrape needs Userbot", callback_data="userbot_menu"))
            markup.row(InlineKeyboardButton("🎛 Filter Settings", callback_data=f"filters_{s_id}"))
            markup.row(InlineKeyboardButton("🔙 Back", callback_data="sources"))
            bot.edit_message_text(
                f"⚙️ *Source Options*\n\nID: `{s_id}`",
                call.message.chat.id, call.message.message_id,
                reply_markup=markup, parse_mode="Markdown"
            )

        elif call.data.startswith("filters_"):
            s_id = int(call.data[len("filters_"):])
            filters_dict = get_source_filters(s_id)
            if not filters_dict:
                bot.answer_callback_query(call.id, "Filters not available for this source.", show_alert=True)
                return
            
            text = (
                f"🎛 *Filter Settings for Source `{s_id}`*\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"📸 *Media Types*: `{filters_dict['media_types']}`\n"
                f"⚖️ *Min File Size*: `{filters_dict['min_file_size']} bytes`\n"
                f"🔑 *Caption Keywords*: `{filters_dict['caption_keywords'] or 'None (Allow All)'}`\n"
                f"👤 *Allowed Senders*: `{filters_dict['allowed_senders'] or 'None (Allow All)'}`\n\n"
                "_Select a filter below to change it._"
            )
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("📸 Edit Types", callback_data=f"editf_types_{s_id}"))
            markup.row(InlineKeyboardButton("⚖️ Edit Size", callback_data=f"editf_size_{s_id}"))
            markup.row(InlineKeyboardButton("🔑 Edit Keywords", callback_data=f"editf_keys_{s_id}"))
            markup.row(InlineKeyboardButton("👤 Edit Senders", callback_data=f"editf_senders_{s_id}"))
            markup.row(InlineKeyboardButton("🔙 Back", callback_data=f"src_options_{s_id}"))
            
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data.startswith("editf_"):
            parts = call.data.split("_")
            f_type = parts[1]
            s_id = int(parts[2])
            admin_states[call.from_user.id] = f"editf_{f_type}_{s_id}"
            
            prompts = {
                "types": "Send allowed media types separated by comma.\nOptions: `photo,video,document,audio,voice,animation`\n\nExample: `photo,video`",
                "size": "Send minimum file size in **bytes**.\nExample: `1048576` (for 1MB) or `0` (for no limit)",
                "keys": "Send required caption keywords separated by comma.\nExample: `premium,vip`\nSend `clear` to remove filter.",
                "senders": "Send allowed user IDs separated by comma.\nExample: `12345678,87654321`\nSend `clear` to remove filter."
            }
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔙 Cancel", callback_data=f"filters_{s_id}"))
            bot.edit_message_text(prompts[f_type], call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data.startswith("scrape_"):
            s_id = int(call.data[len("scrape_"):])
            if not user_client:
                bot.answer_callback_query(call.id, "⚠️ Userbot not connected. Go to Userbot menu to connect.", show_alert=True)
                return
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔢 By Message Limit", callback_data=f"scrmode_limit_{s_id}"))
            markup.row(InlineKeyboardButton("📅 By Date Range", callback_data=f"scrmode_date_{s_id}"))
            markup.row(InlineKeyboardButton("🔙 Cancel", callback_data=f"src_options_{s_id}"))
            bot.edit_message_text(f"🕰 *Scrape History for `{s_id}`*\n\nChoose scraping mode:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data.startswith("scrmode_"):
            parts = call.data.split("_")
            mode = parts[1]
            s_id = int(parts[2])
            admin_states[call.from_user.id] = f"awaiting_scrape_{mode}_{s_id}"
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔙 Cancel", callback_data=f"scrape_{s_id}"))
            if mode == "limit":
                bot.edit_message_text("🔢 Send the number of messages to scan (e.g. `100` or `0` for all):", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            elif mode == "date":
                bot.edit_message_text("📅 Send Start Date and End Date in format `YYYY-MM-DD YYYY-MM-DD`.\n\nExample: `2024-01-01 2024-01-31`", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data.startswith("cancel_scrape_"):
            scrape_id = call.data[len("cancel_scrape_"):]
            active_scrapes[scrape_id] = True
            bot.answer_callback_query(call.id, "🛑 Cancelling scrape...", show_alert=False)

        elif call.data == "targets":
            targets = get_all_targets()
            text = "🎯 *Target Groups*\n\n"
            markup = InlineKeyboardMarkup()
            
            if not targets:
                text += "No targets configured."
            else:
                for t_id, title in targets:
                    text += f"- {title} (`{t_id}`)\n"
                    
            markup.row(InlineKeyboardButton("➕ Add Target", callback_data="add_target"))
            markup.row(InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
            
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data == "mappings":
            mappings = get_mappings()
            text = "🔗 *Source to Target Mappings*\n\n"
            markup = InlineKeyboardMarkup()
            
            if not mappings:
                text += "No mappings configured."
            else:
                for src_id, src_title, tgt_id, tgt_title in mappings:
                    text += f"• `{src_title}` ➡️ `{tgt_title}`\n"
                    
            markup.row(InlineKeyboardButton("➕ Add Mapping", callback_data="add_mapping"))
            markup.row(InlineKeyboardButton("🔙 Back", callback_data="main_menu"))
            
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data == "add_source":
            admin_states[call.from_user.id] = "awaiting_source"
            bot.send_message(
                call.message.chat.id,
                "**Add Monitored Source**\n\n"
                "**Option A**: Send Chat ID and Title (e.g., `-1001234567 My Channel`)\n"
                "**Option B**: Send a Telegram invite link or username (e.g., `t.me/example` or `@example`). "
                "_Note: Option B requires the Userbot to be connected._",
                parse_mode="Markdown"
            )
            
        elif call.data == "add_target":
            admin_states[call.from_user.id] = "awaiting_target"
            bot.send_message(call.message.chat.id, "Send the Target Chat ID and Title (e.g., `-1009876543 Dump Group`)")
            
        elif call.data == "add_mapping":
            admin_states[call.from_user.id] = "awaiting_mapping"
            bot.send_message(call.message.chat.id, "Send Source ID and Target ID separated by space.")

        elif call.data == "release":
            media = get_unreleased_media()
            if not media:
                bot.answer_callback_query(call.id, "No unreleased media found.", show_alert=True)
                return
                
            bot.send_message(call.message.chat.id, f"🚀 Found {len(media)} unreleased items. Starting release process...")
            threading.Thread(target=release_media_thread, args=(call.message.chat.id,)).start()

        elif call.data == "main_menu":
            show_main_menu(call.message.chat.id, call.message.message_id)

    # ── BOT-AS-ADMIN: Auto-register when bot is added to a group/channel ──
    @bot.my_chat_member_handler()
    def handle_bot_member_update(update):
        new_status = update.new_chat_member.status
        chat = update.chat
        if new_status in ['administrator', 'member']:
            add_monitored_source(chat.id, chat.title or f"Chat {chat.id}")
            logger.info(f"✅ Auto-registered source: {chat.title} ({chat.id})")
            if ADMIN_ID:
                try:
                    bot.send_message(
                        ADMIN_ID,
                        f"📢 *New Source Registered!*\n\n"
                        f"🏷 *Name*: {chat.title}\n"
                        f"🆔 *ID*: `{chat.id}`\n"
                        f"ℹ️ Bot was added as *{new_status}*. Media will now be saved automatically.",
                        parse_mode="Markdown"
                    )
                except:
                    pass
        elif new_status in ['kicked', 'left']:
            unregister_source(chat.id)
            logger.info(f"❌ Auto-unregistered source: {chat.title} ({chat.id})")

    # ── BOT-AS-ADMIN: Capture media from groups where bot is admin ──
    @bot.message_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'animation'],
                         func=lambda m: m.chat.type in ['group', 'supergroup'])
    def capture_group_media(message):
        sources = get_all_sources()
        monitored_ids = [s[0] for s in sources]
        if message.chat.id not in monitored_ids:
            return
        _save_bot_media(message)

    @bot.channel_post_handler(content_types=['photo', 'video', 'document', 'audio', 'voice', 'animation'])
    def capture_channel_media(message):
        sources = get_all_sources()
        monitored_ids = [s[0] for s in sources]
        if message.chat.id not in monitored_ids:
            return
        _save_bot_media(message)

    def _save_bot_media(message):
        """Extract file_id from a telebot message and download it via Bot API."""
        file_id, media_type = None, None
        if message.photo:
            file_id = message.photo[-1].file_id
            media_type = "photo"
        elif message.video:
            file_id = message.video.file_id
            media_type = "video"
        elif message.document:
            file_id = message.document.file_id
            media_type = "document"
        elif message.audio:
            file_id = message.audio.file_id
            media_type = "audio"
        elif message.voice:
            file_id = message.voice.file_id
            media_type = "voice"
        elif message.animation:
            file_id = message.animation.file_id
            media_type = "animation"

        if not file_id:
            return

        # Check filters
        source_filters = get_source_filters(message.chat.id)
        if not passes_filters(message, source_filters, is_pyrogram=False):
            return

        caption = message.caption or ""
        media_group_id = message.media_group_id
        
        # Download via Bot API to local disk
        try:
            file_info = bot.get_file(file_id)
            file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_info.file_path}"
            local_dir = os.path.join(DOWNLOAD_DIR, str(message.chat.id))
            os.makedirs(local_dir, exist_ok=True)
            local_path = os.path.join(local_dir, os.path.basename(file_info.file_path))
            if not os.path.exists(local_path):
                response = requests.get(file_url, stream=True)
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            save_media_record(message.message_id, message.chat.id, media_type, local_path, caption, media_group_id)
            logger.info(f"✅ [Bot-Admin] Saved {media_type} from {message.chat.id}")
        except Exception as e:
            logger.error(f"❌ [Bot-Admin] Failed to save media: {e}")

    @bot.message_handler(func=lambda m: m.from_user.id in admin_states)
    def handle_states(message):
        global user_client
        state = admin_states.get(message.from_user.id)

        # ── LOGIN FLOW ──
        if state == "awaiting_phone":
            phone = message.text.strip()
            login_data[message.from_user.id] = {"phone": phone, "client": None}
            bot.reply_to(message, "⏳ Sending OTP to your Telegram account...")

            async def send_code():
                tmp = Client(
                    name=":memory:",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    in_memory=True
                )
                await tmp.connect()
                sent = await tmp.send_code(phone)
                login_data[message.from_user.id]["client"] = tmp
                login_data[message.from_user.id]["phone_code_hash"] = sent.phone_code_hash
                admin_states[message.from_user.id] = "awaiting_otp"
                bot.send_message(
                    message.chat.id,
                    "✅ OTP sent! Please enter the code you received:\n"
                    "_(Send digits only, e.g. `12345`)_",
                    parse_mode="Markdown"
                )

            asyncio.run_coroutine_threadsafe(send_code(), loop)
            return

        if state == "awaiting_otp":
            otp = message.text.strip().replace(" ", "")
            data = login_data.get(message.from_user.id, {})
            tmp: Client = data.get("client")

            async def sign_in():
                global user_client
                try:
                    await tmp.sign_in(
                        phone_number=data["phone"],
                        phone_code_hash=data["phone_code_hash"],
                        phone_code=otp
                    )
                    session_str = await tmp.export_session_string()
                    save_session_string(session_str)
                    await tmp.disconnect()

                    # Build and start the real userbot
                    user_client = build_client(session_str)
                    register_userbot_handlers(user_client)
                    await user_client.start()

                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(
                        message.chat.id,
                        "🎉 *Login Successful!*\n\nUserbot is now active and listening for media.",
                        parse_mode="Markdown"
                    )
                except SessionPasswordNeeded:
                    admin_states[message.from_user.id] = "awaiting_2fa"
                    bot.send_message(
                        message.chat.id,
                        "🔐 Two-Factor Authentication is enabled.\nPlease send your *2FA password*:",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(message.chat.id, f"❌ Login failed: `{e}`", parse_mode="Markdown")

            asyncio.run_coroutine_threadsafe(sign_in(), loop)
            return

        if state == "awaiting_2fa":
            password = message.text.strip()
            data = login_data.get(message.from_user.id, {})
            tmp: Client = data.get("client")

            async def check_password():
                global user_client
                try:
                    await tmp.check_password(password)
                    session_str = await tmp.export_session_string()
                    save_session_string(session_str)
                    await tmp.disconnect()

                    user_client = build_client(session_str)
                    register_userbot_handlers(user_client)
                    await user_client.start()

                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(
                        message.chat.id,
                        "🎉 *Login Successful!*\n\nUserbot is now active and listening for media.",
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(message.chat.id, f"❌ 2FA failed: `{e}`", parse_mode="Markdown")

            asyncio.run_coroutine_threadsafe(check_password(), loop)
            return

        # ── NORMAL ADMIN STATES ──
        admin_states.pop(message.from_user.id, None)
        try:
            if state and state.startswith("awaiting_scrape_"):
                parts = state.split("_")
                mode = parts[2]
                s_id = int(parts[3])
                
                status_msg = bot.reply_to(message, "⏳ Initializing scraper...")
                
                if mode == "limit":
                    limit = int(message.text.strip())
                    asyncio.run_coroutine_threadsafe(scrape_history(s_id, message.chat.id, status_msg.message_id, limit=limit), loop)
                elif mode == "date":
                    dates = message.text.strip().split()
                    if len(dates) != 2:
                        bot.edit_message_text("❌ Invalid format. Please provide exactly two dates.", message.chat.id, status_msg.message_id)
                        return
                    start_date = datetime.strptime(dates[0], "%Y-%m-%d")
                    end_date = datetime.strptime(dates[1], "%Y-%m-%d")
                    # Ensure end_date includes the entire day
                    end_date = end_date.replace(hour=23, minute=59, second=59)
                    
                    if start_date > end_date:
                        bot.edit_message_text("❌ Start date cannot be after end date.", message.chat.id, status_msg.message_id)
                        return
                        
                    asyncio.run_coroutine_threadsafe(scrape_history(s_id, message.chat.id, status_msg.message_id, start_date=start_date, end_date=end_date), loop)
                return

            if state and state.startswith("editf_"):
                parts = state.split("_")
                f_type = parts[1]
                s_id = int(parts[2])
                
                val = message.text.strip()
                if val.lower() == 'clear': val = ''
                
                db_key = ""
                if f_type == "types": db_key = "allowed_media_types"
                elif f_type == "size": 
                    db_key = "min_file_size"
                    val = int(val)
                elif f_type == "keys": db_key = "caption_keywords"
                elif f_type == "senders": db_key = "allowed_senders"
                
                update_source_filter(s_id, db_key, val)
                
                markup = InlineKeyboardMarkup()
                markup.row(InlineKeyboardButton("🔙 Back to Filters", callback_data=f"filters_{s_id}"))
                bot.reply_to(message, "✅ Filter updated successfully.", reply_markup=markup)
                return

            if state == "awaiting_source":
                text = message.text.strip()
                if "t.me" in text or text.startswith("@"):
                    if not user_client:
                        bot.reply_to(message, "❌ Userbot is not connected. Cannot join via link. Please provide Chat ID instead.")
                        return
                    
                    bot.reply_to(message, "⏳ Attempting to join chat via Userbot...")
                    async def join_and_save():
                        try:
                            chat = await user_client.join_chat(text)
                            add_monitored_source(chat.id, chat.title)
                            bot.send_message(message.chat.id, f"✅ Userbot joined and added source: *{chat.title}* (`{chat.id}`)", parse_mode="Markdown")
                        except Exception as e:
                            try:
                                chat = await user_client.get_chat(text)
                                add_monitored_source(chat.id, chat.title)
                                bot.send_message(message.chat.id, f"✅ Source added (already joined): *{chat.title}* (`{chat.id}`)", parse_mode="Markdown")
                            except Exception as inner_e:
                                bot.send_message(message.chat.id, f"❌ Failed to join or fetch chat: `{e}` / `{inner_e}`", parse_mode="Markdown")
                    
                    asyncio.run_coroutine_threadsafe(join_and_save(), loop)
                else:
                    parts = text.split(" ", 1)
                    add_monitored_source(int(parts[0]), parts[1])
                    bot.reply_to(message, "✅ Source added.")
            elif state == "awaiting_target":
                parts = message.text.split(" ", 1)
                add_target_group(int(parts[0]), parts[1])
                bot.reply_to(message, "✅ Target added.")
            elif state == "awaiting_mapping":
                parts = message.text.split()
                add_mapping(int(parts[0]), int(parts[1]))
                bot.reply_to(message, "✅ Mapping added.")
        except Exception as e:
            bot.reply_to(message, f"❌ Error: {e}")

def release_media_thread(admin_chat_id):
    media_items = get_unreleased_media()
    success_count = 0
    
    loop = user_client.loop
    
    async def upload_job(items):
        nonlocal success_count
        for item in items:
            m_id, file_path, m_type, caption = item
            
            with get_connection() as conn:
                with conn.cursor() as c:
                    c.execute("SELECT source_chat_id FROM saved_media WHERE id = %s", (m_id,))
                    source_id = c.fetchone()[0]
                    
            targets = get_targets_for_source(source_id)
            if not targets:
                logger.warning(f"No targets mapped for source {source_id}. Skipping media {m_id}.")
                continue
                
            for target_id in targets:
                try:
                    if os.path.exists(file_path):
                        if m_type == "photo":
                            await user_client.send_photo(target_id, file_path, caption=caption)
                        elif m_type == "video":
                            await user_client.send_video(target_id, file_path, caption=caption)
                        elif m_type == "document":
                            await user_client.send_document(target_id, file_path, caption=caption)
                        else:
                            await user_client.send_document(target_id, file_path, caption=caption)
                        
                        logger.info(f"Released {file_path} to {target_id}")
                    else:
                        logger.warning(f"File not found: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to send to target: {e}")
                    
            mark_media_released(m_id)
            success_count += 1
            await asyncio.sleep(2) # rate limit

    if loop.is_running():
        asyncio.run_coroutine_threadsafe(upload_job(media_items), loop)
        bot.send_message(admin_chat_id, f"✅ Release job submitted to Userbot loop.")
    else:
        bot.send_message(admin_chat_id, f"❌ Userbot loop not running.")


# ==========================================
# 🌐 RENDER HEALTH CHECK SERVER
# ==========================================
app = Flask(__name__)

@app.route('/')
def health_check():
    return "Userbot Media Saver is running!", 200

def run_health_check_server():
    port = int(os.getenv("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

# ==========================================
# 🚀 MAIN RUNNER
# ==========================================
async def start_services():
    # Start Render Health Check Server IMMEDIATELY
    threading.Thread(target=run_health_check_server, daemon=True).start()
    logger.info("🌐 Health Check Server started.")

    logger.info("Initializing Database...")
    try:
        init_db()
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return

    # Try to restore a saved userbot session from DB
    session_str = get_session_string()
    if session_str:
        logger.info("🔑 Found saved session. Restoring Pyrogram Userbot...")
        try:
            user_client = build_client(session_str)
            register_userbot_handlers(user_client)
            await user_client.start()
            logger.info("✅ Userbot restored and started successfully!")
        except Unauthorized as e:
            logger.warning(f"⚠️ Saved session is invalid or expired (Unauthorized): {e}. Clearing session from database.")
            save_session_string("")
            user_client = None
        except Exception as e:
            logger.error(f"❌ Could not start Userbot due to a network or connection error: {e}")
            user_client = None
    else:
        logger.info("ℹ️ No saved session found. Admin must log in via the bot.")

    if bot:
        logger.info("Starting Telebot Admin UI...")
        current_loop = asyncio.get_running_loop()
        current_loop.run_in_executor(None, bot.infinity_polling)
        logger.info("✅ Admin Bot Started successfully!")
    else:
        logger.warning("BOT_TOKEN not found. Admin UI will not start.")

    logger.info("System is fully running. Press Ctrl+C to stop.")
    await idle()

    if user_client and user_client.is_connected:
        logger.info("Stopping Pyrogram Userbot...")
        await user_client.stop()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        logger.info("System stopped.")
