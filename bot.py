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
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    live_forward BOOLEAN DEFAULT FALSE
                )
            """)
            try:
                c.execute("ALTER TABLE monitored_sources ADD COLUMN live_forward BOOLEAN DEFAULT FALSE")
            except Exception:
                # Column likely already exists
                conn.rollback()
            else:
                conn.commit()

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

            # Multi-Session support: user_sessions table
            c.execute("""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    phone TEXT PRIMARY KEY,
                    session_string TEXT,
                    api_id INTEGER,
                    api_hash TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Migration: Add api_id and api_hash to existing sessions if missing
            for col, col_type in [("api_id", "INTEGER"), ("api_hash", "TEXT")]:
                try:
                    c.execute(f"ALTER TABLE user_sessions ADD COLUMN {col} {col_type}")
                except Exception:
                    conn.rollback()
                else:
                    conn.commit()
            
            # Migrate legacy session
            try:
                c.execute("SELECT value FROM bot_settings WHERE key = 'session_string'")
                legacy_session = c.fetchone()
                if legacy_session and legacy_session[0]:
                    c.execute("INSERT INTO user_sessions (phone, session_string) VALUES ('Legacy', %s) ON CONFLICT DO NOTHING", (legacy_session[0],))
                    c.execute("DELETE FROM bot_settings WHERE key = 'session_string'")
            except Exception as e:
                conn.rollback()
                logger.error(f"Error migrating legacy session: {e}")
            else:
                conn.commit()

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
def add_monitored_source(source_id, title, session_id='Legacy'):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO monitored_sources (source_id, title, is_active, session_id)
                    VALUES (%s, %s, TRUE, %s)
                    ON CONFLICT (source_id) DO UPDATE SET title = EXCLUDED.title, is_active = TRUE, session_id = EXCLUDED.session_id
                """, (source_id, title, session_id))
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

def add_target_group(target_id, title, session_id='Legacy'):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO target_groups (target_id, title, session_id)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (target_id) DO UPDATE SET title = EXCLUDED.title, session_id = EXCLUDED.session_id
                """, (target_id, title, session_id))
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

def get_all_sources(session_id=None):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                if session_id:
                    c.execute("SELECT source_id, title FROM monitored_sources WHERE is_active = TRUE AND session_id = %s", (session_id,))
                else:
                    c.execute("SELECT source_id, title FROM monitored_sources WHERE is_active = TRUE")
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching sources: {e}")
        return []

def get_all_targets(session_id=None):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                if session_id:
                    c.execute("SELECT target_id, title FROM target_groups WHERE session_id = %s", (session_id,))
                else:
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

def get_media_stats_by_source(session_id=None):
    """Return list of (source_id, title, unreleased_count, released_count) per source."""
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                if session_id:
                    c.execute("""
                        SELECT ms.source_id, ms.title,
                            COUNT(sm.id) FILTER (WHERE sm.is_released = FALSE) AS unreleased,
                            COUNT(sm.id) FILTER (WHERE sm.is_released = TRUE) AS released
                        FROM monitored_sources ms
                        LEFT JOIN saved_media sm ON ms.source_id = sm.source_chat_id
                        WHERE ms.is_active = TRUE AND ms.session_id = %s
                        GROUP BY ms.source_id, ms.title
                        ORDER BY unreleased DESC
                    """, (session_id,))
                else:
                    c.execute("""
                        SELECT ms.source_id, ms.title,
                            COUNT(sm.id) FILTER (WHERE sm.is_released = FALSE) AS unreleased,
                            COUNT(sm.id) FILTER (WHERE sm.is_released = TRUE) AS released
                        FROM monitored_sources ms
                        LEFT JOIN saved_media sm ON ms.source_id = sm.source_chat_id
                        WHERE ms.is_active = TRUE
                        GROUP BY ms.source_id, ms.title
                        ORDER BY unreleased DESC
                    """)
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching media stats by source: {e}")
        return []

def get_unreleased_media_for_source(source_id):
    """Return all unreleased media items for a specific source."""
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT id, file_path, media_type, caption
                    FROM saved_media
                    WHERE source_chat_id = %s AND is_released = FALSE
                    ORDER BY downloaded_at ASC
                """, (source_id,))
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching unreleased media for source {source_id}: {e}")
        return []

def get_all_media_for_source(source_id, limit=6, offset=0):
    """Return paginated media items for a source (all statuses)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT id, file_path, media_type, caption, is_released, downloaded_at
                    FROM saved_media
                    WHERE source_chat_id = %s
                    ORDER BY downloaded_at DESC
                    LIMIT %s OFFSET %s
                """, (source_id, limit, offset))
                rows = c.fetchall()
                c.execute("SELECT COUNT(*) FROM saved_media WHERE source_chat_id = %s", (source_id,))
                total = c.fetchone()[0]
                return rows, total
    except Exception as e:
        logger.error(f"Error fetching media for source {source_id}: {e}")
        return [], 0

def delete_media_record(media_id):
    """Delete a media record and its local file."""
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT file_path FROM saved_media WHERE id = %s", (media_id,))
                row = c.fetchone()
                if row and row[0] and os.path.exists(row[0]):
                    try:
                        os.remove(row[0])
                    except:
                        pass
                c.execute("DELETE FROM saved_media WHERE id = %s", (media_id,))
        return True
    except Exception as e:
        logger.error(f"Error deleting media {media_id}: {e}")
        return False

def get_all_sessions():
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT phone, session_string, api_id, api_hash FROM user_sessions WHERE is_active = TRUE")
                return c.fetchall()
    except Exception as e:
        logger.error(f"Error fetching sessions: {e}")
        return []

def save_session(phone, session_str, api_id=None, api_hash=None):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO user_sessions (phone, session_string, api_id, api_hash, is_active) 
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT (phone) DO UPDATE SET 
                        session_string = EXCLUDED.session_string,
                        api_id = EXCLUDED.api_id,
                        api_hash = EXCLUDED.api_hash,
                        is_active = TRUE
                """, (phone, session_str, api_id, api_hash))
        return True
    except Exception as e:
        logger.error(f"Failed to save session: {e}")
        return False

def delete_session(phone):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE user_sessions SET is_active = FALSE WHERE phone = %s", (phone,))
        return True
    except Exception as e:
        logger.error(f"Failed to delete session: {e}")
        return False

def get_setting(key, default=None):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT value FROM bot_settings WHERE key = %s", (key,))
                row = c.fetchone()
                return row[0] if row else default
    except:
        return default

def set_setting(key, value):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO bot_settings (key, value) VALUES (%s, %s)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """, (key, str(value)))
        return True
    except Exception as e:
        logger.error(f"Failed to set setting {key}: {e}")
        return False

def get_live_forward(source_id):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT live_forward FROM monitored_sources WHERE source_id = %s", (source_id,))
                row = c.fetchone()
                return row[0] if row else False
    except:
        return False

def toggle_live_forward(source_id):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE monitored_sources SET live_forward = NOT live_forward WHERE source_id = %s RETURNING live_forward", (source_id,))
                return c.fetchone()[0]
    except Exception as e:
        logger.error(f"Error toggling live forward: {e}")
        return False

# ==========================================
# 🤖 PYROGRAM USERBOT (LISTENER & DOWNLOADER)
# ==========================================

# Multi-session: dict of phone -> Pyrogram Client
active_clients = {}  # phone -> Client

def build_client(phone, session_str, api_id=None, api_hash=None):
    """Factory to create a Pyrogram client from a session string with custom API credentials."""
    # Fallback to global defaults if not provided per-session
    final_api_id = api_id if api_id else API_ID
    final_api_hash = api_hash if api_hash else API_HASH
    
    c = Client(
        name=f"sessions/{phone}",
        api_id=final_api_id,
        api_hash=final_api_hash,
        session_string=session_str,
        plugins=None,
        workers=4,
        storage=MemoryStorage()
    )
    c._phone = phone  # tag for identification
    return c

def register_userbot_handlers(client: Client):
    """Attach live listener to the client."""
    @client.on_message(filters.media)
    async def live_listener(c: Client, message: Message):
        # Allow self-sent media only if it's in a monitored source (like Saved Messages)
        if message.from_user and message.from_user.is_self:
            # We don't use ~filters.me anymore, but we must be careful not to loop
            # Check if this chat is specifically monitored
            pass 
        
        sources = get_all_sources(session_id=c._phone)
        monitored_ids = [s[0] for s in sources]
        if message.chat.id in monitored_ids:
            # Avoid processing messages sent by the bot to targets (if any)
            # but usually filters.media takes care of it.
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
                # Check for Live Forward
                if get_live_forward(message.chat.id):
                    # Fetch its ID for release
                    with get_connection() as conn:
                        with conn.cursor() as c:
                            c.execute("SELECT id FROM saved_media WHERE file_path = %s", (file_path,))
                            row = c.fetchone()
                            m_id = row[0] if row else None
                    if m_id:
                        await release_single_media(m_id, file_path, media_type, caption, message.chat.id)
            else:
                logger.warning(f"⚠️ Media record already exists or failed for {file_path}")
    except Exception as e:
        logger.error(f"❌ Failed to download media: {e}")

# live_listener is registered dynamically via register_userbot_handlers() after login

active_scrapes = {}

async def scrape_history(client: Client, chat_id: int, admin_chat_id: int, status_msg_id: int, limit: int = 0, start_date=None, end_date=None):
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
            async for message in client.get_chat_history(chat_id, offset_date=end_date):
                if active_scrapes.get(scrape_id, False):
                    break
                
                # Check if we passed the start date (Pyrogram iterates backward)
                if message.date < start_date:
                    break
                    
                scanned += 1
                if message.media:
                    await download_media(client, message)
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
            async for message in client.get_chat_history(chat_id, limit=limit if limit > 0 else None):
                if active_scrapes.get(scrape_id, False):
                    break
                    
                scanned += 1
                if message.media:
                    await download_media(client, message)
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
        active_count = len(active_clients)
        acct_label = f"{active_count} Account(s) Active" if active_count > 0 else "No Accounts"
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("📊 Analytics", callback_data="stats"),
            InlineKeyboardButton("📂 Sources", callback_data="sources")
        )
        markup.row(
            InlineKeyboardButton("🎯 Targets", callback_data="targets"),
            InlineKeyboardButton("🔗 Routing Map", callback_data="mappings")
        )
        markup.row(InlineKeyboardButton("🚀 Manual Release", callback_data="release"))
        markup.row(InlineKeyboardButton("⏱ Automation Config", callback_data="auto_release_menu"))
        markup.row(InlineKeyboardButton(f"🤖 Account Manager \u2022 {acct_label}", callback_data="account_manager"))

        text = (
            "<b>💎 SYSTEM DASHBOARD</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "<blockquote><b>🟢 Passive Mode (Bot)</b>\n"
            "<i>Always active. Add this bot as an admin to any group/channel to start capturing media.</i></blockquote>\n"
            f"<blockquote><b>🤖 Userbot Accounts</b>\n"
            f"<i>{active_count} session(s) are active and listening for media.</i></blockquote>"
        )
        try:
            if message_id:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode="HTML")
            else:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
        except:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda call: True)
    def callback_handler(call):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Unauthorized", show_alert=True)
            return

        bot.answer_callback_query(call.id)

        # ─── ACCOUNT MANAGER ───
        if call.data == "account_manager":
            sessions = get_all_sessions()
            markup = InlineKeyboardMarkup()
            text = (
                "<b>🤖 ACCOUNT MANAGER</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
            )
            if sessions:
                text += f"<i>{len(sessions)} Userbot account(s) registered.</i>"
                for phone, _ in sessions:
                    is_live = phone in active_clients
                    status_icon = "✅" if is_live else "🔴"
                    markup.row(InlineKeyboardButton(f"{status_icon} {phone}", callback_data=f"acct_dash_{phone}"))
            else:
                text += "<i>No accounts added yet. Add your first Telegram account below.</i>"
            markup.row(InlineKeyboardButton("➕ Add New Account", callback_data="connect_userbot"))
            markup.row(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="main_menu"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data.startswith("acct_dash_"):
            phone = call.data[len("acct_dash_"):]
            is_live = phone in active_clients
            status = "✅ Active" if is_live else "🔴 Offline"
            src_stats = get_media_stats_by_source(session_id=phone)
            src_count = len(src_stats)
            tgt_count = len(get_all_targets(session_id=phone))
            total_unreleased = sum(s[2] for s in src_stats)
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("📊 Media Statistics", callback_data=f"media_stats_{phone}"))
            markup.row(InlineKeyboardButton("📁 Browse Joined Chats", callback_data=f"browse_chats_{phone}_0"))
            markup.row(
                InlineKeyboardButton(f"📂 Sources ({src_count})", callback_data=f"acct_sources_{phone}"),
                InlineKeyboardButton(f"🎯 Targets ({tgt_count})", callback_data=f"acct_targets_{phone}")
            )
            if is_live:
                markup.row(InlineKeyboardButton("✨ Monitor Saved Messages", callback_data=f"quick_add_saved_{phone}"))
                markup.row(InlineKeyboardButton("🔴 Disconnect Account", callback_data=f"acct_disconnect_{phone}"))
            markup.row(InlineKeyboardButton("🔙 Back to Accounts", callback_data="account_manager"))
            text = (
                f"<b>🤖 ACCOUNT: <code>{phone}</code></b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<blockquote><b>Status:</b> {status}\n"
                f"<b>Sources Monitored:</b> {src_count}\n"
                f"<b>Target Channels:</b> {tgt_count}\n"
                f"<b>Pending Release:</b> {total_unreleased} items</blockquote>\n\n"
                f"<i>Tap 📊 Media Statistics to see a breakdown per source and release media.</i>"
            )
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data.startswith("acct_disconnect_"):
            phone = call.data[len("acct_disconnect_"):]
            client = active_clients.pop(phone, None)
            if client:
                asyncio.run_coroutine_threadsafe(client.stop(), loop)
            delete_session(phone)
            bot.answer_callback_query(call.id, f"Account {phone} disconnected.", show_alert=True)
            call.data = "account_manager"
            callback_handler(call)

        elif call.data.startswith("browse_chats_"):
            parts = call.data.split("_")
            # format: browse_chats_{phone}_{page}
            page = int(parts[-1])
            phone = "_".join(parts[2:-1])
            client = active_clients.get(phone)
            if not client or not client.is_connected:
                bot.answer_callback_query(call.id, "Account is offline.", show_alert=True)
                return
            per_page = 8

            async def fetch_chats():
                chats = []
                async for dialog in client.get_dialogs():
                    if dialog.chat.type.name in ["GROUP", "SUPERGROUP", "CHANNEL"]:
                        chats.append((dialog.chat.id, dialog.chat.title or str(dialog.chat.id)))
                    elif dialog.chat.type.name == "PRIVATE" and dialog.chat.id == (await client.get_me()).id:
                        chats.append((dialog.chat.id, "✨ Saved Messages"))
                return chats

            all_chats = asyncio.run_coroutine_threadsafe(fetch_chats(), loop).result(timeout=30)
            total = len(all_chats)
            chunk = all_chats[page * per_page:(page + 1) * per_page]

            markup = InlineKeyboardMarkup()
            for c_id, c_title in chunk:
                safe = c_title[:28]
                markup.row(
                    InlineKeyboardButton(f"📂 {safe}", callback_data=f"chat_info_{phone}_{c_id}"),
                )
            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("◀ Prev", callback_data=f"browse_chats_{phone}_{page-1}"))
            if (page + 1) * per_page < total:
                nav_row.append(InlineKeyboardButton("Next ▶", callback_data=f"browse_chats_{phone}_{page+1}"))
            if nav_row:
                markup.row(*nav_row)
            markup.row(InlineKeyboardButton("🔙 Back to Account", callback_data=f"acct_dash_{phone}"))
            text = (
                f"<b>📁 JOINED CHATS</b> \u2014 <code>{phone}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<i>Page {page+1} \u2022 {total} total chats. Tap a chat to add it as a Source or Target.</i>"
            )
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data.startswith("chat_info_"):
            parts = call.data.split("_")
            # format: chat_info_{phone}_{chat_id}
            c_id = int(parts[-1])
            phone = "_".join(parts[2:-1])
            markup = InlineKeyboardMarkup()
            markup.row(
                InlineKeyboardButton("➕ Add as Source", callback_data=f"quick_add_src_{phone}_{c_id}"),
                InlineKeyboardButton("➕ Add as Target", callback_data=f"quick_add_tgt_{phone}_{c_id}")
            )
            markup.row(InlineKeyboardButton("🔙 Back to Chats", callback_data=f"browse_chats_{phone}_0"))
            bot.edit_message_text(
                f"<b>Chat ID:</b> <code>{c_id}</code>\n\nWhat would you like to do with this chat?",
                call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML"
            )

        elif call.data.startswith("quick_add_src_"):
            parts = call.data.split("_")
            c_id = int(parts[-1])
            phone = "_".join(parts[3:-1])
            add_monitored_source(c_id, str(c_id), session_id=phone)
            bot.answer_callback_query(call.id, f"\u2705 Added as Source for {phone}!", show_alert=True)

        elif call.data.startswith("quick_add_tgt_"):
            parts = call.data.split("_")
            c_id = int(parts[-1])
            phone = "_".join(parts[3:-1])
            add_target_group(c_id, str(c_id), session_id=phone)
            bot.answer_callback_query(call.id, f"\u2705 Added as Target for {phone}!", show_alert=True)

        elif call.data.startswith("media_stats_"):
            phone = call.data[len("media_stats_"):]
            src_stats = get_media_stats_by_source(session_id=phone)
            markup = InlineKeyboardMarkup()
            text = (
                f"<b>📊 MEDIA STATISTICS</b> \u2014 <code>{phone}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
            )
            if not src_stats:
                text += "<i>No sources configured for this account.</i>"
            else:
                for src_id, title, unreleased, released in src_stats:
                    title_short = (title or str(src_id))[:30]
                    total_src = unreleased + released
                    text += f"<blockquote><b>{title_short}</b>\n📦 Unreleased: {unreleased} \u2022 ✅ Released: {released} \u2022 🗂 Total: {total_src}</blockquote>\n"
                    row = []
                    if total_src > 0:
                        row.append(InlineKeyboardButton(
                            f"📁 Browse ({total_src})",
                            callback_data=f"browse_media_{src_id}_0"
                        ))
                    if unreleased > 0:
                        row.append(InlineKeyboardButton(
                            f"🚀 Release ({unreleased})",
                            callback_data=f"release_src_{src_id}"
                        ))
                    if row:
                        markup.row(*row)
            markup.row(InlineKeyboardButton("🔙 Back to Account", callback_data=f"acct_dash_{phone}"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data.startswith("release_src_"):
            src_id = int(call.data[len("release_src_"):])
            media_items = get_unreleased_media_for_source(src_id)
            if not media_items:
                bot.answer_callback_query(call.id, "No unreleased media for this source.", show_alert=True)
                return
            targets = get_targets_for_source(src_id)
            if not targets:
                bot.answer_callback_query(call.id, "\u274c No targets mapped for this source!", show_alert=True)
                return

            status_msg = bot.send_message(
                call.message.chat.id,
                f"⏳ <b>Releasing {len(media_items)} items...</b>",
                parse_mode="HTML"
            )

            async def do_release(items, source_id, chat_id, msg_id):
                done = 0
                for m_id, file_path, m_type, caption in items:
                    result = await release_single_media(m_id, file_path, m_type, caption or "", source_id)
                    if result:
                        done += 1
                    await asyncio.sleep(1.5)
                try:
                    bot.edit_message_text(
                        f"✅ <b>Release Complete!</b>\n\n<b>{done}</b> / {len(items)} items sent to targets.",
                        chat_id, msg_id, parse_mode="HTML"
                    )
                except:
                    pass

            asyncio.run_coroutine_threadsafe(
                do_release(media_items, src_id, call.message.chat.id, status_msg.message_id), loop
            )

        elif call.data.startswith("browse_media_"):
            parts = call.data.split("_")
            page = int(parts[-1])
            src_id = int(parts[2])
            per_page = 6
            offset = page * per_page
            media_items, total = get_all_media_for_source(src_id, limit=per_page, offset=offset)
            total_pages = max(1, (total + per_page - 1) // per_page)

            markup = InlineKeyboardMarkup()
            type_icons = {"photo": "\ud83d\uddbc", "video": "\ud83c\udfa5", "document": "\ud83d\udcce", "audio": "\ud83c\udfa7", "voice": "\ud83c\udfa4", "animation": "\ud83c\udfac"}
            for m_id, file_path, m_type, caption, is_released, downloaded_at in media_items:
                icon = type_icons.get(m_type, "\ud83d\udcc4")
                status = "\u2705" if is_released else "\ud83d\udfe1"
                dt_str = downloaded_at.strftime("%d %b %H:%M") if downloaded_at else ""
                label = f"{icon} {status} {m_type.capitalize()} \u2022 {dt_str}"
                markup.row(InlineKeyboardButton(label, callback_data=f"view_media_{src_id}_{m_id}_{page}"))

            nav_row = []
            if page > 0:
                nav_row.append(InlineKeyboardButton("\u25c0 Prev", callback_data=f"browse_media_{src_id}_{page-1}"))
            if (page + 1) * per_page < total:
                nav_row.append(InlineKeyboardButton("Next \u25b6", callback_data=f"browse_media_{src_id}_{page+1}"))
            if nav_row:
                markup.row(*nav_row)
            markup.row(InlineKeyboardButton("\ud83d\udd19 Back to Stats", callback_data=f"back_to_stats_{src_id}"))

            bot.edit_message_text(
                f"<b>\ud83d\udcc1 MEDIA BROWSER</b>\n"
                f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"
                f"<blockquote><b>Source ID:</b> <code>{src_id}</code>\n"
                f"<b>Total Items:</b> {total} | <b>Page:</b> {page+1}/{total_pages}</blockquote>\n\n"
                f"<i>Tap any item to view and manage it.</i>",
                call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML"
            )

        elif call.data.startswith("view_media_"):
            parts = call.data.split("_")
            m_id = int(parts[3])
            src_id = int(parts[2])
            back_page = int(parts[4])
            try:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT id, file_path, media_type, caption, is_released, downloaded_at FROM saved_media WHERE id = %s", (m_id,))
                        row = c.fetchone()
            except:
                row = None

            if not row:
                bot.answer_callback_query(call.id, "Media not found.", show_alert=True)
                return

            _, file_path, m_type, caption, is_released, downloaded_at = row
            file_exists = os.path.exists(file_path) if file_path else False
            status = "\u2705 Released" if is_released else "\ud83d\udfe1 Unreleased"
            dt_str = downloaded_at.strftime("%Y-%m-%d %H:%M") if downloaded_at else "Unknown"

            markup = InlineKeyboardMarkup()
            if file_exists:
                markup.row(InlineKeyboardButton("\ud83d\udce4 Send to Me", callback_data=f"send_media_{m_id}"))
            if not is_released:
                markup.row(InlineKeyboardButton("\ud83d\ude80 Release to Targets", callback_data=f"release_one_{m_id}_{src_id}"))
            markup.row(InlineKeyboardButton("\ud83d\uddd1 Delete", callback_data=f"del_media_{m_id}_{src_id}_{back_page}"))
            markup.row(InlineKeyboardButton("\ud83d\udd19 Back to List", callback_data=f"browse_media_{src_id}_{back_page}"))

            file_status = 'Present ✅' if file_exists else 'Missing ❌'
            text = (
                f"<b>\ud83d\udcc4 MEDIA DETAILS</b>\n"
                f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n\n"
                f"<blockquote><b>ID:</b> <code>{m_id}</code>\n"
                f"<b>Type:</b> {m_type}\n"
                f"<b>Status:</b> {status}\n"
                f"<b>Saved:</b> {dt_str}\n"
                f'<b>File:</b> {file_status}</blockquote>\n'
            )
            if caption:
                text += f"\n<blockquote><b>Caption:</b>\n<i>{caption[:200]}</i></blockquote>"
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data.startswith("send_media_"):
            m_id = int(call.data[len("send_media_"):])
            try:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT file_path, media_type, caption FROM saved_media WHERE id = %s", (m_id,))
                        row = c.fetchone()
            except:
                row = None

            if not row or not os.path.exists(row[0]):
                bot.answer_callback_query(call.id, "\u274c File not found on server.", show_alert=True)
                return

            file_path, m_type, caption = row
            bot.answer_callback_query(call.id, "\ud83d\udce4 Sending...")
            try:
                if m_type == "photo":
                    bot.send_photo(call.message.chat.id, open(file_path, "rb"), caption=caption)
                elif m_type == "video":
                    bot.send_video(call.message.chat.id, open(file_path, "rb"), caption=caption)
                else:
                    bot.send_document(call.message.chat.id, open(file_path, "rb"), caption=caption)
            except Exception as e:
                bot.send_message(call.message.chat.id, f"\u274c Failed to send: <code>{e}</code>", parse_mode="HTML")

        elif call.data.startswith("release_one_"):
            parts = call.data.split("_")
            m_id = int(parts[2])
            src_id = int(parts[3])
            try:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT file_path, media_type, caption FROM saved_media WHERE id = %s", (m_id,))
                        row = c.fetchone()
            except:
                row = None
            if row:
                asyncio.run_coroutine_threadsafe(
                    release_single_media(m_id, row[0], row[1], row[2] or "", src_id), loop
                )
                bot.answer_callback_query(call.id, "\u2705 Queued for release!", show_alert=True)
            else:
                bot.answer_callback_query(call.id, "\u274c Media not found.", show_alert=True)

        elif call.data.startswith("del_media_"):
            parts = call.data.split("_")
            m_id = int(parts[2])
            src_id = int(parts[3])
            back_page = int(parts[4])
            delete_media_record(m_id)
            bot.answer_callback_query(call.id, "\ud83d\uddd1 Deleted.", show_alert=True)
            call.data = f"browse_media_{src_id}_{back_page}"
            callback_handler(call)

        elif call.data.startswith("back_to_stats_"):
            src_id = int(call.data[len("back_to_stats_"):])
            try:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT session_id FROM monitored_sources WHERE source_id = %s", (src_id,))
                        row = c.fetchone()
                phone = row[0] if row else "Legacy"
            except:
                phone = "Legacy"
            call.data = f"media_stats_{phone}"
            callback_handler(call)

        elif call.data.startswith("quick_add_saved_"):
            phone = call.data[len("quick_add_saved_"):]
            client = active_clients.get(phone)
            if not client: return
            async def add_saved():
                me = await client.get_me()
                add_monitored_source(me.id, "✨ Saved Messages", session_id=phone)
                bot.answer_callback_query(call.id, "✅ Saved Messages added as Source!", show_alert=True)
            asyncio.run_coroutine_threadsafe(add_saved(), loop)

        elif call.data == "default_api_id":
            uid = call.from_user.id
            if uid in login_data:
                login_data[uid]["api_id"] = None
                admin_states[uid] = "awaiting_api_hash"
                markup = InlineKeyboardMarkup()
                markup.row(InlineKeyboardButton("✨ Use Default API Hash", callback_data="default_api_hash"))
                bot.edit_message_text("🔑 Please enter your <b>API HASH</b>:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data == "default_api_hash":
            uid = call.from_user.id
            if uid in login_data:
                login_data[uid]["api_hash"] = None
                start_otp_flow(call.message)

        elif call.data == "connect_userbot":

            admin_states[call.from_user.id] = "awaiting_phone"
            bot.send_message(
                call.message.chat.id,
                "📱 <b>Add Userbot Account</b>\n\nSend your phone number in international format:\n<code>+1234567890</code>\n\n"
                "<i>Your number is only used to generate a session. You can add multiple accounts.</i>",
                parse_mode="HTML"
            )
            return


        if call.data == "stats":
            unreleased, released = get_stats()
            text = (
                "<b>📊 SYSTEM STATISTICS</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<blockquote><b>📦 Unreleased Media:</b> {unreleased} items</blockquote>\n"
                f"<blockquote><b>✅ Released Media:</b> {released} items</blockquote>"
            )
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="main_menu"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
            
        elif call.data == "sources":
            sources = get_all_sources()
            text = "<b>📂 MONITORED SOURCES</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"
            markup = InlineKeyboardMarkup()
            
            if not sources:
                text += "<i>No sources configured yet.</i>"
            else:
                text += "<i>Select a source below to configure its settings.</i>"
                for s_id, title in sources:
                    markup.row(InlineKeyboardButton(f"⚙️ {title}", callback_data=f"src_options_{s_id}"))
                    
            markup.row(InlineKeyboardButton("➕ Add New Source", callback_data="add_source"))
            markup.row(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="main_menu"))
            
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data.startswith("src_options_"):
            s_id = int(call.data[len("src_options_"):])
            
            # Check if we have an active session for this source
            try:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT session_id FROM monitored_sources WHERE source_id = %s", (s_id,))
                        row = c.fetchone()
                phone = row[0] if row else None
            except:
                phone = None

            markup = InlineKeyboardMarkup()
            if phone and phone in active_clients:
                markup.row(InlineKeyboardButton("🕰 Scrape History", callback_data=f"scrape_{s_id}"))
            else:
                markup.row(InlineKeyboardButton("⚠️ Scrape (Account Offline)", callback_data="account_manager"))
            
            live_fwd = get_live_forward(s_id)
            lf_status = "✅ ON" if live_fwd else "❌ OFF"
            markup.row(InlineKeyboardButton(f"⚡ Live Forward: {lf_status}", callback_data=f"toggle_lf_{s_id}"))
            
            markup.row(InlineKeyboardButton("🎛 Filter Settings", callback_data=f"filters_{s_id}"))
            markup.row(InlineKeyboardButton("🔙 Back to Sources", callback_data="sources"))
            bot.edit_message_text(
                f"<b>⚙️ SOURCE OPTIONS</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<blockquote><b>ID:</b> <code>{s_id}</code></blockquote>\n\n"
                f"<i>⚡ Live Forward instantly sends media to targets without waiting for release.</i>",
                call.message.chat.id, call.message.message_id,
                reply_markup=markup, parse_mode="HTML"
            )

        elif call.data.startswith("toggle_lf_"):
            s_id = int(call.data[len("toggle_lf_"):])
            toggle_live_forward(s_id)
            
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔙 Back to Source", callback_data=f"src_options_{s_id}"))
            bot.edit_message_text("✅ Live Forward mode toggled.", call.message.chat.id, call.message.message_id, reply_markup=markup)

        elif call.data == "auto_release_menu":
            enabled = str(get_setting("auto_release_enabled", "False")) == "True"
            interval = get_setting("auto_release_interval", 60)
            batch = get_setting("auto_release_batch_size", 0)
            batch_text = "All" if int(batch) == 0 else str(batch)
            status = "✅ ON" if enabled else "❌ OFF"
            
            text = (
                f"<b>⏱ AUTOMATION SETTINGS</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"<blockquote><b>Status:</b> {status}\n"
                f"<b>Interval:</b> {interval} minutes\n"
                f"<b>Batch Size:</b> {batch_text} items</blockquote>\n\n"
                f"<i>Auto-Release runs in the background and releases saved media to your target groups automatically based on these settings.</i>"
            )
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton(f"Toggle Status ({status})", callback_data="toggle_ar"))
            markup.row(InlineKeyboardButton("Edit Interval", callback_data="edit_ar_interval"))
            markup.row(InlineKeyboardButton("Edit Batch Size", callback_data="edit_ar_batch"))
            markup.row(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="main_menu"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif call.data == "toggle_ar":
            enabled = str(get_setting("auto_release_enabled", "False")) == "True"
            set_setting("auto_release_enabled", not enabled)
            bot.answer_callback_query(call.id, "Toggled Auto-Release.")
            # Trigger menu update
            call.data = "auto_release_menu"
            callback_handler(call)

        elif call.data == "edit_ar_interval":
            admin_states[call.from_user.id] = "awaiting_ar_interval"
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔙 Cancel", callback_data="auto_release_menu"))
            bot.edit_message_text("⏱ Send the interval in minutes (e.g. `60` for 1 hour):", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data == "edit_ar_batch":
            admin_states[call.from_user.id] = "awaiting_ar_batch"
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔙 Cancel", callback_data="auto_release_menu"))
            bot.edit_message_text("📦 Send the batch size (number of items to release at once, `0` means release all):", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

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
            # Find which session owns this source
            try:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT session_id FROM monitored_sources WHERE source_id = %s", (s_id,))
                        row = c.fetchone()
                phone = row[0] if row else None
            except:
                phone = None

            if not phone or phone not in active_clients:
                bot.answer_callback_query(call.id, "⚠️ Account for this source is offline.", show_alert=True)
                return
                
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🔢 By Message Limit", callback_data=f"scrmode_limit_{s_id}_{phone}"))
            markup.row(InlineKeyboardButton("📅 By Date Range", callback_data=f"scrmode_date_{s_id}_{phone}"))
            markup.row(InlineKeyboardButton("🔙 Cancel", callback_data=f"src_options_{s_id}"))
            bot.edit_message_text(f"🕰 *Scrape History for `{s_id}`*\n\nChoose scraping mode:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        elif call.data.startswith("scrmode_"):
            parts = call.data.split("_")
            mode = parts[1]
            s_id = int(parts[2])
            phone = parts[3]
            admin_states[call.from_user.id] = f"awaiting_scrape_{mode}_{s_id}_{phone}"
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
            success = save_media_record(message.message_id, message.chat.id, media_type, local_path, caption, media_group_id)
            if success:
                logger.info(f"✅ [Bot-Admin] Saved {media_type} from {message.chat.id}")
                if get_live_forward(message.chat.id):
                    with get_connection() as conn:
                        with conn.cursor() as c:
                            c.execute("SELECT id FROM saved_media WHERE file_path = %s", (local_path,))
                            row = c.fetchone()
                            m_id = row[0] if row else None
                    if m_id:
                        # Schedule it into the Pyrogram event loop
                        asyncio.run_coroutine_threadsafe(
                            release_single_media(m_id, local_path, media_type, caption, message.chat.id), 
                            loop
                        )
            else:
                logger.warning(f"⚠️ [Bot-Admin] Media record exists or failed for {local_path}")
        except Exception as e:
            logger.error(f"❌ [Bot-Admin] Failed to save media: {e}")

    def start_otp_flow(message):
        uid = message.from_user.id
        data = login_data.get(uid)
        if not data: return
        
        phone = data["phone"]
        aid = data.get("api_id") or API_ID
        ahash = data.get("api_hash") or API_HASH
        
        bot.send_message(message.chat.id, "⏳ Sending OTP to your Telegram account...")

        async def send_code():
            try:
                tmp = Client(
                    name=":memory:",
                    api_id=aid,
                    api_hash=ahash,
                    in_memory=True
                )
                await tmp.connect()
                sent = await tmp.send_code(phone)
                login_data[uid]["client"] = tmp
                login_data[uid]["phone_code_hash"] = sent.phone_code_hash
                admin_states[uid] = "awaiting_otp"
                bot.send_message(
                    message.chat.id,
                    "✅ <b>OTP sent!</b> Please enter the code you received:\n"
                    "<i>(Send digits only, e.g. 12345)</i>",
                    parse_mode="HTML"
                )
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ <b>Failed to send OTP:</b>\n<code>{e}</code>", parse_mode="HTML")
                admin_states.pop(uid, None)
                login_data.pop(uid, None)

        asyncio.run_coroutine_threadsafe(send_code(), loop)

    @bot.message_handler(func=lambda m: m.from_user.id in admin_states)
    def handle_states(message):

        state = admin_states.get(message.from_user.id)

        # ── LOGIN FLOW ──
        if state == "awaiting_phone":
            phone = message.text.strip()
            login_data[message.from_user.id] = {"phone": phone, "client": None}
            admin_states[message.from_user.id] = "awaiting_api_id"
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("✨ Use Default API ID", callback_data="default_api_id"))
            bot.reply_to(message, "🔢 Please enter your <b>API ID</b> from <a href='https://my.telegram.org'>my.telegram.org</a>:", reply_markup=markup, parse_mode="HTML", disable_web_page_preview=True)
            return

        if state == "awaiting_api_id":
            try:
                api_id = int(message.text.strip())
                login_data[message.from_user.id]["api_id"] = api_id
                admin_states[message.from_user.id] = "awaiting_api_hash"
                markup = InlineKeyboardMarkup()
                markup.row(InlineKeyboardButton("✨ Use Default API Hash", callback_data="default_api_hash"))
                bot.reply_to(message, "🔑 Please enter your <b>API HASH</b>:", reply_markup=markup, parse_mode="HTML")
            except:
                bot.reply_to(message, "❌ Invalid API ID. Please send numbers only.")
            return

        if state == "awaiting_api_hash":
            api_hash = message.text.strip()
            login_data[message.from_user.id]["api_hash"] = api_hash
            start_otp_flow(message)
            return

        if state == "awaiting_otp":
            otp = message.text.strip().replace(" ", "")
            data = login_data.get(message.from_user.id, {})
            tmp: Client = data.get("client")

            async def sign_in():
                try:
                    await tmp.sign_in(
                        phone_number=data["phone"],
                        phone_code_hash=data["phone_code_hash"],
                        phone_code=otp
                    )
                    session_str = await tmp.export_session_string()
                    phone = data["phone"]
                    aid = data.get("api_id")
                    ahash = data.get("api_hash")
                    
                    save_session(phone, session_str, aid, ahash)
                    await tmp.disconnect()

                    new_client = build_client(phone, session_str, aid, ahash)
                    register_userbot_handlers(new_client)
                    await new_client.start()
                    active_clients[phone] = new_client

                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(
                        message.chat.id,
                        f"🎉 <b>Login Successful!</b>\n\nAccount <code>{phone}</code> is now active.",
                        parse_mode="HTML"
                    )
                except SessionPasswordNeeded:
                    admin_states[message.from_user.id] = "awaiting_2fa"
                    bot.send_message(
                        message.chat.id,
                        "🔐 <b>2FA Required</b>\n\nPlease send your 2FA password:",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(message.chat.id, f"❌ Login failed: <code>{e}</code>", parse_mode="HTML")

            asyncio.run_coroutine_threadsafe(sign_in(), loop)
            return

        if state == "awaiting_2fa":
            password = message.text.strip()
            data = login_data.get(message.from_user.id, {})
            tmp: Client = data.get("client")

            async def check_password():
                try:
                    await tmp.check_password(password)
                    session_str = await tmp.export_session_string()
                    phone = data["phone"]
                    aid = data.get("api_id")
                    ahash = data.get("api_hash")

                    save_session(phone, session_str, aid, ahash)
                    await tmp.disconnect()

                    new_client = build_client(phone, session_str, aid, ahash)
                    register_userbot_handlers(new_client)
                    await new_client.start()
                    active_clients[phone] = new_client

                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(
                        message.chat.id,
                        f"🎉 <b>Login Successful!</b>\n\nAccount <code>{phone}</code> is now active.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    admin_states.pop(message.from_user.id, None)
                    login_data.pop(message.from_user.id, None)
                    bot.send_message(message.chat.id, f"❌ 2FA failed: <code>{e}</code>", parse_mode="HTML")

            asyncio.run_coroutine_threadsafe(check_password(), loop)
            return

        # ── NORMAL ADMIN STATES ──
        admin_states.pop(message.from_user.id, None)
        try:
            if state and state.startswith("awaiting_scrape_"):
                parts = state.split("_")
                mode = parts[2]
                s_id = int(parts[3])
                phone = parts[4]
                
                client = active_clients.get(phone)
                if not client:
                    bot.reply_to(message, "❌ Account is no longer active.")
                    return

                status_msg = bot.reply_to(message, "⏳ Initializing scraper...")
                
                if mode == "limit":
                    limit = int(message.text.strip())
                    asyncio.run_coroutine_threadsafe(scrape_history(client, s_id, message.chat.id, status_msg.message_id, limit=limit), loop)
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
                        
                    asyncio.run_coroutine_threadsafe(scrape_history(client, s_id, message.chat.id, status_msg.message_id, start_date=start_date, end_date=end_date), loop)
                return

            if state == "awaiting_ar_interval":
                try:
                    val = int(message.text.strip())
                    if val < 1:
                        bot.reply_to(message, "❌ Interval must be at least 1 minute.")
                        return
                    set_setting("auto_release_interval", val)
                    bot.reply_to(message, f"✅ Auto-Release interval set to {val} minutes.")
                except ValueError:
                    bot.reply_to(message, "❌ Invalid number.")
                return

            if state == "awaiting_ar_batch":
                try:
                    val = int(message.text.strip())
                    if val < 0:
                        bot.reply_to(message, "❌ Batch size cannot be negative.")
                        return
                    set_setting("auto_release_batch_size", val)
                    bot.reply_to(message, f"✅ Auto-Release batch size set to {val} items (0 means all).")
                except ValueError:
                    bot.reply_to(message, "❌ Invalid number.")
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
                    if not active_clients:
                        bot.reply_to(message, "❌ No active userbots connected. Please connect an account in the Account Manager first.")
                        return
                    
                    # Pick the first active client for manual joining
                    phone, client = next(iter(active_clients.items()))
                    
                    bot.reply_to(message, f"⏳ Resolving chat via <code>{phone}</code>...", parse_mode="HTML")
                    async def join_and_save():
                        frozen_hint = (
                            "\n\n⚠️ *Why did this happen?*\n"
                            "Telegram restricts joining via invite links for accounts that are new or flagged\\. "
                            "Your userbot account needs to be older/more active\\.\n\n"
                            "👉 *Workaround*: Get the Chat ID manually \\(e\\.g\\. via @username\\_to\\_id\\_bot\\) "
                            "and add the source as: `-1001234567 Channel Name`"
                        )
                        try:
                            # Detect if it's a private invite link (t.me/+xxx or joinchat)
                            is_private_link = "+joinchat" in text or "/+" in text
                            if not is_private_link:
                                # Public @username or t.me/username — use get_chat, no join needed
                                chat = await client.get_chat(text)
                                add_monitored_source(chat.id, chat.title, session_id=phone)
                                bot.send_message(message.chat.id, f"✅ *Source added to {phone}\\!*\n\n🏷 *{chat.title}*\n🆔 `{chat.id}`", parse_mode="MarkdownV2")
                            else:
                                # Private invite link — must join
                                chat = await client.join_chat(text)
                                add_monitored_source(chat.id, chat.title, session_id=phone)
                                bot.send_message(message.chat.id, f"✅ *Userbot {phone} joined source\\!*\n\n🏷 *{chat.title}*\n🆔 `{chat.id}`", parse_mode="MarkdownV2")
                        except Exception as e:
                            err_str = str(e)
                            if "FROZEN_METHOD_INVALID" in err_str:
                                bot.send_message(
                                    message.chat.id,
                                    "❌ *Telegram blocked the join request*\n\n"
                                    "`FROZEN_METHOD_INVALID` — Telegram has restricted your userbot from joining via invite links\\. "
                                    "This is a Telegram account trust restriction \\(not a bug\\)\\."
                                    + frozen_hint,
                                    parse_mode="MarkdownV2"
                                )
                            elif "FloodWait" in err_str:
                                bot.send_message(message.chat.id, "⏳ *Flood wait triggered\\.* Please wait a few minutes and try again\\.", parse_mode="MarkdownV2")
                            else:
                                bot.send_message(message.chat.id, f"❌ *Failed to add source\\.*\n\n`{err_str}`" + frozen_hint, parse_mode="MarkdownV2")
                    
                    asyncio.run_coroutine_threadsafe(join_and_save(), loop)
                else:
                    parts = text.split(" ", 1)
                    # If ID is provided manually, we don't strictly need a session but it's better to have one
                    phone = next(iter(active_clients.keys())) if active_clients else "Legacy"
                    add_monitored_source(int(parts[0]), parts[1], session_id=phone)
                    bot.reply_to(message, f"✅ Source added (assigned to {phone}).")
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

async def release_single_media(m_id, file_path, m_type, caption, source_id):
    """Release a single media item to all its target groups using the correct session client."""
    # Determine which session owns this source
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("SELECT session_id FROM monitored_sources WHERE source_id = %s", (source_id,))
                row = c.fetchone()
                phone = row[0] if row else None
    except:
        phone = None

    # Pick the right client
    client = active_clients.get(phone) if phone else (list(active_clients.values())[0] if active_clients else None)
    if not client or not client.is_connected:
        return False
        
    targets = get_targets_for_source(source_id)
    if not targets:
        return False
        
    success = False
    for target_id in targets:
        try:
            if os.path.exists(file_path):
                if m_type == "photo":
                    await client.send_photo(target_id, file_path, caption=caption)
                elif m_type == "video":
                    await client.send_video(target_id, file_path, caption=caption)
                else:
                    await client.send_document(target_id, file_path, caption=caption)
                success = True
                logger.info(f"Released {file_path} to {target_id}")
            else:
                logger.warning(f"File missing during release: {file_path}")
        except Exception as e:
            logger.error(f"Failed to send to {target_id}: {e}")
            
    if success:
        mark_media_released(m_id)
    return success

async def auto_release_loop():
    """Background loop to handle scheduled releases."""
    while True:
        await asyncio.sleep(60) # Check every minute
        enabled = str(get_setting("auto_release_enabled", "False")) == "True"
        if not enabled:
            continue
        if not active_clients:
            continue
            
        interval = int(get_setting("auto_release_interval", 60))
        last_run = get_setting("auto_release_last_run", None)
        now = datetime.now()
        
        if last_run:
            last_run_dt = datetime.fromisoformat(last_run)
            diff_mins = (now - last_run_dt).total_seconds() / 60
            if diff_mins < interval:
                continue
                
        # Time to run
        batch_size = int(get_setting("auto_release_batch_size", 0))
        media_items = get_unreleased_media()
        if not media_items:
            continue
            
        items_to_process = media_items[:batch_size] if batch_size > 0 else media_items
        logger.info(f"⏱ Auto-Release triggered. Releasing {len(items_to_process)} items.")
        
        for item in items_to_process:
            m_id, file_path, m_type, caption = item
            with get_connection() as conn:
                with conn.cursor() as c:
                    c.execute("SELECT source_chat_id FROM saved_media WHERE id = %s", (m_id,))
                    source_id = c.fetchone()[0]
                    
            await release_single_media(m_id, file_path, m_type, caption, source_id)
            await asyncio.sleep(2) # Prevent flooding
            
        set_setting("auto_release_last_run", now.isoformat())

def release_media_thread(admin_chat_id):
    media_items = get_unreleased_media()
    success_count = 0
    
    async def upload_job(items):
        nonlocal success_count
        for item in items:
            m_id, file_path, m_type, caption = item
            try:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT source_chat_id FROM saved_media WHERE id = %s", (m_id,))
                        row = c.fetchone()
                        source_id = row[0] if row else None
                
                if source_id:
                    await release_single_media(m_id, file_path, m_type, caption, source_id)
                    success_count += 1
            except Exception as e:
                logger.error(f"Error in mass release for item {m_id}: {e}")
            await asyncio.sleep(1.5)
        
        bot.send_message(admin_chat_id, f"✅ <b>Release Complete!</b> Released <code>{success_count}</code> items.", parse_mode="HTML")

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

    # Restore all saved sessions
    saved_sessions = get_all_sessions()
    if saved_sessions:
        logger.info(f"🔑 Found {len(saved_sessions)} saved session(s). Restoring...")
        for phone, session_str, api_id, api_hash in saved_sessions:
            try:
                client = build_client(phone, session_str, api_id, api_hash)
                register_userbot_handlers(client)
                await client.start()
                active_clients[phone] = client
                logger.info(f"✅ Userbot [{phone}] started successfully!")
            except Unauthorized as e:
                logger.warning(f"⚠️ Session [{phone}] is invalid (Unauthorized). Removing. {e}")
                delete_session(phone)
            except Exception as e:
                logger.error(f"❌ Could not start Userbot [{phone}]: {e}")
    else:
        logger.info("ℹ️ No saved sessions found. Admin must log in via the bot.")

    if bot:
        logger.info("Starting Telebot Admin UI...")
        current_loop = asyncio.get_running_loop()
        current_loop.run_in_executor(None, bot.infinity_polling)
        logger.info("✅ Admin Bot Started successfully!")
    else:
        logger.warning("BOT_TOKEN not found. Admin UI will not start.")

    # Start Auto-Release background loop
    asyncio.create_task(auto_release_loop())

    logger.info("System is fully running. Press Ctrl+C to stop.")
    await idle()

    # Shutdown all clients
    for phone, client in active_clients.items():
        if client.is_connected:
            logger.info(f"Stopping Userbot [{phone}]...")
            await client.stop()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        logger.info("System stopped.")

