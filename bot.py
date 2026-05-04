import os
import asyncio

# Fix for Pyrogram RuntimeError in Python 3.10+
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import threading
import logging
import psycopg2
from psycopg2 import pool as pg_pool
from contextlib import contextmanager
from dotenv import load_dotenv

from pyrogram import Client, filters, idle
from pyrogram.types import Message

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
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_released BOOLEAN DEFAULT FALSE
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS monitored_sources (
                    source_id BIGINT PRIMARY KEY,
                    title TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
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
            logger.info("✅ Database schema initialized.")

# Media Queries
def save_media_record(message_id, source_chat_id, media_type, file_path, caption):
    try:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO saved_media (message_id, source_chat_id, media_type, file_path, caption)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (file_path) DO NOTHING
                """, (message_id, source_chat_id, media_type, file_path, caption))
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

# ==========================================
# 🤖 PYROGRAM USERBOT (LISTENER & DOWNLOADER)
# ==========================================
if API_ID and API_HASH:
    user_client = Client("my_userbot", api_id=API_ID, api_hash=API_HASH)
else:
    user_client = None

async def download_media(client: Client, message: Message):
    if not message.media:
        return

    sources = get_all_sources()
    monitored_ids = [s[0] for s in sources]
    
    if message.chat.id not in monitored_ids:
        return

    try:
        media_type = message.media.value
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
                caption
            )
            if success:
                logger.info(f"✅ Saved media record for {file_path}")
            else:
                logger.warning(f"⚠️ Media record already exists or failed for {file_path}")
    except Exception as e:
        logger.error(f"❌ Failed to download media: {e}")

if user_client:
    @user_client.on_message(filters.media & ~filters.me)
    async def live_listener(client: Client, message: Message):
        """Listens for new incoming media in the background."""
        asyncio.create_task(download_media(client, message))

async def scrape_history(chat_id: int, limit: int = 100):
    """Scrape historical media from a specific chat."""
    logger.info(f"Starting historical scrape for {chat_id} (limit: {limit})")
    count = 0
    try:
        async for message in user_client.get_chat_history(chat_id, limit=limit):
            if message.media:
                await download_media(user_client, message)
                count += 1
                await asyncio.sleep(1) # Delay to avoid flooding
    except Exception as e:
        logger.error(f"Error scraping history for {chat_id}: {e}")
    
    logger.info(f"Finished scraping {chat_id}. Downloaded {count} media items.")
    return count

# ==========================================
# 🎛️ TELEBOT ADMIN DASHBOARD
# ==========================================
if BOT_TOKEN:
    bot = telebot.TeleBot(BOT_TOKEN)
else:
    bot = None

admin_states = {}

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
        
        text = (
            "💎 *Userbot Media Saver Dashboard*\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Welcome! Manage your Pyrogram userbot here."
        )
        bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode="Markdown")

    @bot.callback_query_handler(func=lambda call: True)
    def callback_handler(call):
        if not is_admin(call.from_user.id):
            bot.answer_callback_query(call.id, "Unauthorized", show_alert=True)
            return

        bot.answer_callback_query(call.id)
        
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
            s_id = int(call.data.split("_")[2])
            markup = InlineKeyboardMarkup()
            markup.row(InlineKeyboardButton("🕰 Scrape History", callback_data=f"scrape_{s_id}"))
            markup.row(InlineKeyboardButton("🔙 Back", callback_data="sources"))
            bot.edit_message_text(f"Options for source `{s_id}`:", call.message.chat.id, call.message.message_id, reply_markup=markup)

        elif call.data.startswith("scrape_"):
            s_id = int(call.data.split("_")[1])
            bot.send_message(call.message.chat.id, f"🕰 Started historical scrape for `{s_id}`. Check terminal for progress.")
            asyncio.run_coroutine_threadsafe(scrape_history(s_id, limit=50), user_client.loop)

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
            bot.send_message(call.message.chat.id, "Send the Source Chat ID and Title (e.g., `-1001234567 My Channel`)")
            
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
            start_cmd(call.message)

    @bot.message_handler(func=lambda m: m.from_user.id in admin_states)
    def handle_states(message):
        state = admin_states.pop(message.from_user.id)
        try:
            parts = message.text.split(" ", 1)
            if state == "awaiting_source":
                add_monitored_source(int(parts[0]), parts[1])
                bot.reply_to(message, "✅ Source added.")
            elif state == "awaiting_target":
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

    if not user_client:
        logger.error("API_ID and API_HASH not found. Cannot start Pyrogram Userbot.")
        return

    logger.info("Starting Pyrogram Userbot...")
    await user_client.start()
    logger.info("✅ Userbot Started successfully!")

    if bot:
        logger.info("Starting Telebot Admin UI...")
        # Run polling in an executor so it doesn't block the async loop
        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, bot.infinity_polling)
        logger.info("✅ Admin Bot Started successfully!")
    else:
        logger.warning("BOT_TOKEN not found. Admin UI will not start.")

    logger.info("System is fully running. Press Ctrl+C to stop.")
    await idle()
    
    logger.info("Stopping Pyrogram Userbot...")
    await user_client.stop()

if __name__ == "__main__":
    try:
        loop.run_until_complete(start_services())
    except KeyboardInterrupt:
        logger.info("System stopped.")
