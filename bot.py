#gimini
import os
import asyncio
import threading
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import requests
import signal
import sys
from flask import Flask
from dotenv import load_dotenv

from telethon import TelegramClient, events, functions, types, errors
from telethon.sessions import StringSession
from telethon.utils import pack_bot_file_id
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

# Create a global event loop for Telethon/Asyncio
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

load_dotenv()

# -----------------------------
# Config
# -----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
bot_fleet = {} # { bot_id: telebot_instance })
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN:
    print("WARNING: Missing BOT_TOKEN in .env. Admin features will be limited until set.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("userbot_v2")

# Topic Mirroring Cache
# {target_chat_id: {topic_title.lower(): top_message_id}}
topic_cache = {}

# Global State Dictionaries
login_data = {}    # { user_id: { state_data } }
admin_states = {}  # { user_id: "current_state" }
running_tasks = {} # { task_key: bool }

# -----------------------------
# DB (SQLite/PostgreSQL)
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = "userbot_v2.db"
USING_POSTGRES = False

@contextmanager
def db_conn():
    global USING_POSTGRES
    conn = None
    try:
        if DATABASE_URL:
            try:
                import psycopg2
                conn = psycopg2.connect(DATABASE_URL)
                conn.autocommit = True
                USING_POSTGRES = True
            except (ImportError, Exception) as e:
                logger.error(f"PostgreSQL connection failed: {e}. Falling back to SQLite.")
                conn = sqlite3.connect(DB_PATH)
                USING_POSTGRES = False
        else:
            conn = sqlite3.connect(DB_PATH)
            USING_POSTGRES = False
        
        yield conn
        # Commit for SQLite
        if not DATABASE_URL or isinstance(conn, sqlite3.Connection):
            conn.commit()
    finally:
        if conn:
            conn.close()

USING_POSTGRES = False

# Album Cache for grouping media
# {grouped_id: [message_objects]}
album_cache = {}

def get_placeholder(conn=None):
    # If we are explicitly told we are using Postgres or the connection is not a sqlite3 type
    if DATABASE_URL and (USING_POSTGRES or (conn and not isinstance(conn, sqlite3.Connection))):
        return "%s"
    return "?"

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
                    source_topic_id BIGINT DEFAULT NULL,
                    target_id BIGINT,
                    target_topic_id BIGINT DEFAULT NULL,
                    source_title TEXT,
                    target_title TEXT,
                    is_monitoring INTEGER DEFAULT 0,
                    is_live INTEGER DEFAULT 0,
                    is_mirror INTEGER DEFAULT 0,
                    UNIQUE(source_id, source_topic_id, target_id, target_topic_id)
                )
            """)
            # Migration
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN source_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN target_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_monitoring INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_live INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_mirror INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN content_filter TEXT DEFAULT 'everything'")
            except: pass
            # Update UNIQUE constraint for Postgres
            try:
                c.execute("ALTER TABLE target_pairs DROP CONSTRAINT IF EXISTS target_pairs_source_id_target_id_key")
                c.execute("ALTER TABLE target_pairs ADD CONSTRAINT unique_pair_topics UNIQUE (source_id, source_topic_id, target_id, target_topic_id)")
            except: pass

            c.execute("""
                CREATE TABLE IF NOT EXISTS topic_mappings (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_topic_id BIGINT,
                    target_chat_id BIGINT,
                    target_topic_id BIGINT,
                    UNIQUE(source_chat_id, source_topic_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS message_mappings (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    target_chat_id BIGINT,
                    target_msg_id BIGINT,
                    UNIQUE(source_chat_id, source_msg_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
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

            c.execute("""
                CREATE TABLE IF NOT EXISTS log_targets (
                    id SERIAL PRIMARY KEY,
                    target_id BIGINT UNIQUE,
                    target_type TEXT,
                    target_name TEXT,
                    bot_token TEXT
                )
            """)
            try: c.execute("ALTER TABLE log_targets ADD CONSTRAINT unique_log_target UNIQUE (target_id)")
            except: pass
            try: c.execute("ALTER TABLE log_targets ADD COLUMN bot_token TEXT")
            except: pass
            c.execute("""
                CREATE TABLE IF NOT EXISTS media_logs (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    log_target_id BIGINT,
                    file_id TEXT,
                    media_type TEXT
                )
            """)
            try: c.execute("ALTER TABLE media_logs ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_media_logs_unique ON media_logs(source_chat_id, source_message_id, log_target_id)")
            except: pass

            # Log Bot System Tables
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_bots (
                    id SERIAL PRIMARY KEY,
                    bot_token TEXT UNIQUE,
                    bot_username TEXT,
                    bot_id BIGINT UNIQUE
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_media (
                    id SERIAL PRIMARY KEY,
                    bot_id BIGINT,
                    log_msg_id BIGINT,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    file_id TEXT,
                    media_type TEXT,
                    caption TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bot_id, source_chat_id, source_msg_id)
                )
            """)
            try: c.execute("ALTER TABLE log_media ADD COLUMN log_msg_id BIGINT")
            except: pass

            # Banned Users Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS banned_users (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE,
                    username TEXT UNIQUE,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            # SQLite
            c.execute("""
                CREATE TABLE IF NOT EXISTS target_pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id BIGINT,
                    source_topic_id BIGINT DEFAULT NULL,
                    target_id BIGINT,
                    target_topic_id BIGINT DEFAULT NULL,
                    source_title TEXT,
                    target_title TEXT,
                    is_monitoring INTEGER DEFAULT 0,
                    is_live INTEGER DEFAULT 0,
                    is_mirror INTEGER DEFAULT 0,
                    UNIQUE(source_id, source_topic_id, target_id, target_topic_id)
                )
            """)
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN source_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN target_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_monitoring INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_live INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_mirror INTEGER DEFAULT 0")
            except: pass

            c.execute("""
                CREATE TABLE IF NOT EXISTS topic_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_topic_id BIGINT,
                    target_chat_id BIGINT,
                    target_topic_id BIGINT,
                    UNIQUE(source_chat_id, source_topic_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS message_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    target_chat_id BIGINT,
                    target_msg_id BIGINT,
                    UNIQUE(source_chat_id, source_msg_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
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

            c.execute("""
                CREATE TABLE IF NOT EXISTS log_targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_id BIGINT UNIQUE,
                    target_type TEXT,
                    target_name TEXT,
                    bot_token TEXT
                )
            """)
            try: c.execute("ALTER TABLE log_targets ADD COLUMN bot_token TEXT")
            except: pass
            try: c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_log_target_unique ON log_targets(target_id)")
            except: pass
            c.execute("""
                CREATE TABLE IF NOT EXISTS media_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    log_target_id BIGINT,
                    file_id TEXT,
                    media_type TEXT
                )
            """)
            try: c.execute("ALTER TABLE media_logs ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_media_logs_unique ON media_logs(source_chat_id, source_message_id, log_target_id)")
            except: pass

            # Log Bot System Tables
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_bots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_token TEXT UNIQUE,
                    bot_username TEXT,
                    bot_id BIGINT UNIQUE
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id BIGINT,
                    log_msg_id BIGINT,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    file_id TEXT,
                    media_type TEXT,
                    caption TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bot_id, source_chat_id, source_msg_id)
                )
            """)
            try: c.execute("ALTER TABLE log_media ADD COLUMN log_msg_id BIGINT")
            except: pass

            # Banned Users Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS banned_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id BIGINT UNIQUE,
                    username TEXT UNIQUE,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
    logger.info("DB initialized")

def is_user_banned(user_id, username=None):
    try:
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            if user_id:
                c.execute(f"SELECT 1 FROM banned_users WHERE user_id = {p}", (user_id,))
                if c.fetchone(): return True
            if username:
                clean_username = username.lower().replace("@", "")
                c.execute(f"SELECT 1 FROM banned_users WHERE username = {p}", (clean_username,))
                if c.fetchone(): return True
    except: pass
    return False

def ban_user(user_id=None, username=None):
    with db_conn() as conn:
        c = conn.cursor()
        uname = username.lower().replace("@", "") if username else None
        if DATABASE_URL:
            c.execute("INSERT INTO banned_users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username", (user_id, uname))
        else:
            c.execute("INSERT OR REPLACE INTO banned_users (user_id, username) VALUES (?, ?)", (user_id, uname))

def unban_user(user_id=None, username=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if user_id:
            c.execute(f"DELETE FROM banned_users WHERE user_id = {p}", (user_id,))
        elif username:
            clean_username = username.lower().replace("@", "")
            c.execute(f"DELETE FROM banned_users WHERE username = {p}", (clean_username,))

def get_banned_users():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, username FROM banned_users")
        return c.fetchall()

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

def add_target_pair(sid, source_topic_id, tid, target_topic_id, s_title, t_title):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                "INSERT INTO target_pairs (source_id, source_topic_id, target_id, target_topic_id, source_title, target_title) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (sid, source_topic_id, tid, target_topic_id, s_title, t_title)
            )
        else:
            c.execute(
                "INSERT INTO target_pairs (source_id, source_topic_id, target_id, target_topic_id, source_title, target_title) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (sid, source_topic_id, tid, target_topic_id, s_title, t_title)
            )

def save_topic_mapping(s_chat, s_topic, t_chat, t_topic):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                """
                INSERT INTO topic_mappings (source_chat_id, source_topic_id, target_chat_id, target_topic_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(source_chat_id, source_topic_id, target_chat_id) 
                DO UPDATE SET target_topic_id = EXCLUDED.target_topic_id
                """,
                (s_chat, s_topic, t_chat, t_topic)
            )
        else:
            c.execute(
                """
                INSERT INTO topic_mappings (source_chat_id, source_topic_id, target_chat_id, target_topic_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_chat_id, source_topic_id, target_chat_id) 
                DO UPDATE SET target_topic_id = excluded.target_topic_id
                """,
                (s_chat, s_topic, t_chat, t_topic)
            )

def get_topic_mapping(s_chat, s_topic, t_chat):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"SELECT target_topic_id FROM topic_mappings WHERE source_chat_id = {p} AND source_topic_id = {p} AND target_chat_id = {p}",
            (s_chat, s_topic, t_chat)
        )
        row = c.fetchone()
        return row[0] if row else None

def save_message_mapping(s_chat, s_msg, t_chat, t_msg):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                """
                INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(source_chat_id, source_msg_id, target_chat_id) 
                DO UPDATE SET target_msg_id = EXCLUDED.target_msg_id
                """,
                (s_chat, s_msg, t_chat, t_msg)
            )
        else:
            c.execute(
                """
                INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_chat_id, source_msg_id, target_chat_id) 
                DO UPDATE SET target_msg_id = excluded.target_msg_id
                """,
                (s_chat, s_msg, t_chat, t_msg)
            )

def get_message_mapping(s_chat, s_msg, t_chat):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"SELECT target_msg_id FROM message_mappings WHERE source_chat_id = {p} AND source_msg_id = {p} AND target_chat_id = {p}",
            (s_chat, s_msg, t_chat)
        )
        row = c.fetchone()
        return row[0] if row else None

def get_log_targets():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, target_id, target_type, target_name, bot_token FROM log_targets")
        return c.fetchall()

def add_log_target(target_id, target_type, target_name, bot_token=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                "INSERT INTO log_targets (target_id, target_type, target_name, bot_token) VALUES (%s, %s, %s, %s) ON CONFLICT(target_id) DO UPDATE SET bot_token = EXCLUDED.bot_token",
                (target_id, target_type, target_name, bot_token)
            )
        else:
            c.execute(
                "INSERT INTO log_targets (target_id, target_type, target_name, bot_token) VALUES (?, ?, ?, ?) ON CONFLICT(target_id) DO UPDATE SET bot_token = excluded.bot_token",
                (target_id, target_type, target_name, bot_token)
            )

def remove_log_target(row_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM log_targets WHERE id = {p}", (row_id,))

def save_media_log(source_chat_id, source_message_id, log_target_id, file_id, media_type):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                "INSERT INTO media_logs (source_chat_id, source_message_id, log_target_id, file_id, media_type) VALUES (%s, %s, %s, %s, %s) ON CONFLICT(source_chat_id, source_message_id, log_target_id) DO NOTHING",
                (source_chat_id, source_message_id, log_target_id, file_id, media_type)
            )
        else:
            c.execute(
                "INSERT OR IGNORE INTO media_logs (source_chat_id, source_message_id, log_target_id, file_id, media_type) VALUES (?, ?, ?, ?, ?)",
                (source_chat_id, source_message_id, log_target_id, file_id, media_type)
            )

def get_media_logs(limit=100, media_type=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        query = "SELECT source_message_id, log_target_id, file_id, media_type FROM media_logs"
        params = []
        if media_type:
            query += f" WHERE media_type = {p}"
            params.append(media_type)
        query += f" ORDER BY id DESC LIMIT {p}"
        params.append(limit)
        c.execute(query, tuple(params))
        return c.fetchall()

def get_vault_sources():
    with db_conn() as conn:
        c = conn.cursor()
        query = """
            SELECT DISTINCT p.source_id, p.source_title 
            FROM target_pairs p
            JOIN log_media m ON p.source_id = m.source_chat_id
        """
        c.execute(query)
        return c.fetchall()

def get_vaulted_media_for_source(source_id, bot_id=None, limit=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        
        query = f"""
            SELECT m.source_msg_id, m.file_id, m.media_type, m.caption, m.log_msg_id, m.bot_id
            FROM log_media m
            WHERE m.source_chat_id = {p}
        """
        params = [source_id]
        if bot_id:
            query += f" AND m.bot_id = {p}"
            params.append(bot_id)
            
        query += " ORDER BY m.source_msg_id ASC"
        
        if limit:
            query += f" LIMIT {p}"
            params.append(limit)
            
        c.execute(query, tuple(params))
        return c.fetchall()

def get_log_bot_stats(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        query = f"""
            SELECT p.source_id, p.source_title, COUNT(m.id) as item_count
            FROM log_media m
            JOIN target_pairs p ON m.source_chat_id = p.source_id
            WHERE m.bot_id = {p}
            GROUP BY p.source_id, p.source_title
            ORDER BY item_count DESC
        """
        c.execute(query, (bot_id,))
        return c.fetchall()

def get_target_pairs():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live, is_mirror, source_topic_id, target_topic_id, content_filter FROM target_pairs")
        return c.fetchall()

def get_target_pair(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live, is_mirror, source_topic_id, target_topic_id, content_filter FROM target_pairs WHERE id = {p}", (pair_id,))
        return c.fetchone()

def get_pair_stats(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT COUNT(*), SUM(CASE WHEN released = 0 THEN 1 ELSE 0 END) FROM collected_media WHERE pair_id = {p}", (pair_id,))
        row = c.fetchone()
        return {"total": row[0] or 0, "pending": row[1] or 0}

# -----------------------------
# Log Bot Helpers
# -----------------------------
def add_log_bot(token, username, bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                "INSERT INTO log_bots (bot_token, bot_username, bot_id) VALUES (%s, %s, %s) ON CONFLICT(bot_token) DO UPDATE SET bot_username = EXCLUDED.bot_username, bot_id = EXCLUDED.bot_id",
                (token, username, bot_id)
            )
        else:
            c.execute(
                "INSERT INTO log_bots (bot_token, bot_username, bot_id) VALUES (?, ?, ?) ON CONFLICT(bot_token) DO UPDATE SET bot_username = excluded.bot_username, bot_id = excluded.bot_id",
                (token, username, bot_id)
            )

def get_log_bots():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT bot_token, bot_username, bot_id FROM log_bots")
        return c.fetchall()

def delete_log_bot(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM log_bots WHERE bot_id = {p}", (bot_id,))
        c.execute(f"DELETE FROM log_media WHERE bot_id = {p}", (bot_id,))

def clear_bot_logs(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM log_media WHERE bot_id = {p}", (bot_id,))

def stop_task(task_key):
    if task_key in running_tasks:
        running_tasks.pop(task_key, None)
        return True
    return False

async def run_vault_release(sender_bot, admin_chat_id, source_id, target_id, interval=1.5, limit=None, log_target_id=None):
    """Releases vaulted media using the Userbot for forwarding."""
    task_key = f"vault_rel_{source_id}_{target_id}"
    if task_key in running_tasks:
        sender_bot.send_message(admin_chat_id, "⚠️ This release task is already running!")
        return

    running_tasks[task_key] = True
    
    try:
        # Get items to release
        items = get_vaulted_media_for_source(source_id, bot_id=log_target_id)
        if not items:
            sender_bot.send_message(admin_chat_id, "❌ No vaulted items found for this source.")
            return

        # Apply the limit if one was provided
        if limit and isinstance(limit, int):
            items = items[:limit]

        total_to_send = len(items)
        success = 0
        failed = 0
        
        # Stop button markup
        stop_markup = InlineKeyboardMarkup().add(InlineKeyboardButton("🛑 Stop Transfer", callback_data=f"lb_stop_rel_{task_key}"))
        status_msg = sender_bot.send_message(admin_chat_id, f"🚀 **Starting batch transfer...**\nProcessing `{total_to_send}` items via Log Bot engine.", reply_markup=stop_markup, parse_mode="Markdown")

        for i, item in enumerate(items):
            if task_key not in running_tasks:
                sender_bot.send_message(admin_chat_id, "🛑 **Release Stopped** by user.")
                break
                
            source_msg_id, file_id, m_type, caption, log_msg_id, bot_id = item
            
            try:
                # 1. Forward from Log Bot using Userbot
                if log_msg_id is None or bot_id is None:
                    raise ValueError("Missing logging IDs for Userbot forwarding")
                
                log_bot_peer = await userbot.get_input_entity(int(bot_id))
                await userbot.forward_messages(
                    entity=int(target_id),
                    messages=int(log_msg_id),
                    from_peer=log_bot_peer
                )
                success += 1
            except Exception as e:
                logger.error(f"Vault Release item error (Userbot): {e}")
                # Fallback: Send directly via Bot API if Userbot fails or data is missing
                try:
                    if not file_id:
                        logger.warning(f"Skipping item {source_msg_id}: No file_id found.")
                        failed += 1
                        continue
                        
                    if m_type == "photo": sender_bot.send_photo(target_id, file_id, caption=caption)
                    elif m_type == "video": sender_bot.send_video(target_id, file_id, caption=caption)
                    elif m_type == "document": sender_bot.send_document(target_id, file_id, caption=caption)
                    elif m_type == "audio": sender_bot.send_audio(target_id, file_id, caption=caption)
                    elif m_type == "animation": sender_bot.send_animation(target_id, file_id, caption=caption)
                    elif m_type == "sticker": sender_bot.send_sticker(target_id, file_id)
                    success += 1
                except Exception as e2:
                    logger.error(f"Vault Release fallback error (Bot API): {e2}")
                    failed += 1

            # Status Update every 5 items
            if (i + 1) % 5 == 0 or (i + 1) == total_to_send:
                try: sender_bot.edit_message_text(f"🚀 **Transferring...**\nProgress: `{i+1}/{total_to_send}`\nSuccess: `{success}`", admin_chat_id, status_msg.message_id, reply_markup=stop_markup, parse_mode="Markdown")
                except: pass

            await asyncio.sleep(interval)
            
        sender_bot.send_message(admin_chat_id, f"✅ **Batch Transfer Complete**\nSent: `{success}`\nFailed: `{failed}`", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Global Release Error: {e}")
        sender_bot.send_message(admin_chat_id, f"❌ Engine Error: {e}")
    finally:
        running_tasks.pop(task_key, None)

def save_logged_media(bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                """INSERT INTO log_media (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s) 
                   ON CONFLICT(bot_id, source_chat_id, source_msg_id) DO UPDATE SET 
                   log_msg_id = EXCLUDED.log_msg_id, file_id = EXCLUDED.file_id, 
                   media_type = EXCLUDED.media_type, caption = EXCLUDED.caption""",
                (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption)
            )
        else:
            c.execute(
                """INSERT INTO log_media (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(bot_id, source_chat_id, source_msg_id) DO UPDATE SET 
                   log_msg_id = excluded.log_msg_id, file_id = excluded.file_id, 
                   media_type = excluded.media_type, caption = excluded.caption""",
                (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption)
            )

def get_logged_media_stats(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT COUNT(*) FROM log_media WHERE bot_id = {p}", (bot_id,))
        return c.fetchone()[0] or 0

def fetch_logged_media(bot_id, limit=1000):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT source_chat_id, source_msg_id, file_id, media_type, caption FROM log_media WHERE bot_id = {p} ORDER BY timestamp DESC LIMIT {p}", (bot_id, limit))
        return c.fetchall()

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
    is_online = userbot and userbot.is_connected()
    status = "🟢 ACTIVE" if is_online else "🔴 OFFLINE"
    
    text = f"✨ **SYSTEM CONSOLE**\n"
    text += f"Status: `{status}`\n"
    if is_online and hasattr(userbot, '_me') and userbot._me:
        name = userbot._me.first_name or "User"
        text += f"Account: `{name}`\n"
    
    text += "\n_Manage your automation pairs below:_"
    return text

def get_dashboard_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    is_online = userbot and userbot.is_connected()
    
    if is_online:
        markup.add(InlineKeyboardButton("🎯 Target Pairs", callback_data="pairs_main"))
        markup.add(InlineKeyboardButton("🚀 Release from Vault", callback_data="vault_rel_main"))
        markup.add(InlineKeyboardButton("👤 User Account", callback_data="user_acc_main"))
        markup.add(InlineKeyboardButton("📜 Log Bots", callback_data="log_bot_main"))
        markup.add(InlineKeyboardButton("🚫 Ban List", callback_data="banlist_main"))
    else:
        markup.add(InlineKeyboardButton("🔌 Connect Userbot", callback_data="user_connect_start"))
    
    return markup

def log_bot_list_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    bots = get_log_bots()
    for token, username, bot_id in bots:
        stats = get_logged_media_stats(bot_id)
        markup.add(InlineKeyboardButton(f"🤖 @{username} ({stats})", callback_data=f"log_bot_view_{bot_id}"))
    
    markup.add(InlineKeyboardButton("➕ Add Log Bot", callback_data="log_bot_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

def log_bot_view_markup(bot_id):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("📥 Fetch Logs", callback_data=f"log_bot_fetch_{bot_id}"),
        InlineKeyboardButton("🗑 Remove", callback_data=f"log_bot_delete_confirm_{bot_id}")
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="log_bot_main"))
    return markup

def pairs_list_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    pairs = get_target_pairs()
    for pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, c_filter in pairs:
        stats = get_pair_stats(pid)
        mon_status = "👁️" if is_mon else ""
        live_status = "⚡" if is_live else ""
        topic_status = "🧵" if (s_topic or t_topic) else ""
        
        btn_text = f"📁 {topic_status}{s_title} ➔ {t_title} {mon_status}{live_status} ({stats['pending']})"
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
            
        pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, c_filter = row
        stats = get_pair_stats(pid)
        mon_status = "🟢 Running" if is_mon else "🔴 Stopped"
        live_status = "🟢 Running" if is_live else "🔴 Stopped"
        mir_status = "🟢 Enabled" if is_mir else "🔴 Disabled"
        
        src_text = f"`{s_title}`" + (f" • Topic: `{s_topic}`" if s_topic else "")
        tgt_text = f"`{t_title}`" + (f" • Topic: `{t_topic}`" if t_topic else "")

        text = (
            f"📁 **Pair Management**\n\n"
            f"Source: {src_text}\n"
            f"Target: {tgt_text}\n\n"
            f"📊 Collected: `{stats['total']}`\n"
            f"📥 Pending: `{stats['pending']}`\n\n"
            f"🤖 **Automation Status:**\n"
            f"Monitor: `{mon_status}`\n"
            f"Live: `{live_status}`\n"
            f"Mirror: `{mir_status}`"
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
    
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, c_filter = pair
    markup = InlineKeyboardMarkup(row_width=2)
    
    mon_btn = "🛑 Stop Monitor" if is_mon else "👁️ Monitor"
    live_btn = "🛑 Stop Live" if is_live else "⚡ Live Forward"
    mir_btn = "🛑 Stop Mirror" if is_mir else "🔀 Mirror Mode"
    
    markup.add(
        InlineKeyboardButton(mon_btn, callback_data=f"pair_toggle_mon_{pair_id}"),
        InlineKeyboardButton(live_btn, callback_data=f"pair_toggle_live_{pair_id}")
    )
    markup.add(InlineKeyboardButton(mir_btn, callback_data=f"pair_toggle_mir_{pair_id}"))
    
    # Content Filter Button
    cf = pair[10] or "everything"
    cf_map = {"everything": "🔄 All Content", "media": "🖼️ Media Only", "text": "📝 Text Only"}
    cf_text = cf_map.get(cf, "🔄 All Content")
    markup.add(InlineKeyboardButton(f"Filter: {cf_text}", callback_data=f"pair_toggle_filter_{pair_id}"))
    
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

def banlist_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    banned = get_banned_users()
    for uid, uname in banned:
        identifier = uid if uid else uname
        label = f"🚫 {uname if uname else uid}"
        markup.add(InlineKeyboardButton(label, callback_data=f"unban_confirm_{identifier}"))
    
    markup.add(InlineKeyboardButton("➕ Add to Ban List", callback_data="ban_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

async def get_chat_selection_markup(prefix, page=0):
    markup = InlineKeyboardMarkup(row_width=1)
    if not userbot or not userbot.is_connected():
        return None
    
    chats = []
    # Fetch enough dialogs to populate selection
    async for dialog in userbot.iter_dialogs(limit=100):
        entity = dialog.entity
        # Filter for relevant chat types
        if isinstance(entity, (types.Chat, types.Channel, types.User)):
            chats.append(dialog)
    
    # Pagination
    start = page * 10
    end = start + 10
    page_items = chats[start:end]
    
    if not page_items:
        markup.add(InlineKeyboardButton("❌ No Groups Found", callback_data="none"))
        return markup
        
    for dialog in page_items:
        chat = dialog.entity
        is_forum = getattr(chat, "forum", False)
        
        # Better visual distinction
        if isinstance(chat, types.Channel):
            if is_forum:
                icon = "🏛️"
                title = f"『 TOPIC 』 {chat.title}"
            elif chat.broadcast:
                icon = "📢"
                title = chat.title or "Channel"
            else:
                icon = "👥"
                title = chat.title or "Group"
        elif isinstance(chat, types.Chat):
            icon = "👥"
            title = chat.title or "Group"
        elif isinstance(chat, types.User):
            if chat.bot: icon = "🤖"
            else: icon = "👤"
            title = f"{chat.first_name or ''} {chat.last_name or ''}".strip() or "Private Chat"
        else:
            icon = "💬"
            title = "Unknown"

        markup.add(
            InlineKeyboardButton(
                f"{icon} {title}",
                callback_data=f"{prefix}_{chat.id}"
            )
        )
    
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{prefix}_page_{page-1}"))
    if end < len(chats): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_page_{page+1}"))
    if nav: markup.add(*nav)
    
    markup.add(InlineKeyboardButton("🔙 Cancel", callback_data="pairs_main"))
    return markup

async def get_topic_selection_markup(chat_id, prefix):
    markup = InlineKeyboardMarkup(row_width=1)
    if not userbot or not userbot.is_connected():
        return None
    
    try:
        result = await userbot(functions.channels.GetForumTopicsRequest(
            channel=chat_id,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        topics = getattr(result, "topics", [])
        
        if not topics:
            markup.add(InlineKeyboardButton("⚠️ No Topics Found", callback_data="noop"))
            # Even if no topics found, allow selecting the whole group
            markup.add(InlineKeyboardButton("🏢 Select Entire Group", callback_data=f"{prefix}_{chat_id}_0"))
        else:
            # Add option to select the entire group as a source
            markup.add(InlineKeyboardButton("🏢 Select Entire Group", callback_data=f"{prefix}_{chat_id}_0"))
            for topic in topics:
                # Telethon Forum topics: top_message is the anchor/starter message ID
                topic_anchor_id = getattr(topic, "top_message", None)
                topic_title = getattr(topic, "title", f"Topic {topic_anchor_id}")
                if topic_anchor_id:
                    markup.add(
                        InlineKeyboardButton(
                            f"🧵 {topic_title}",
                            callback_data=f"{prefix}_{chat_id}_{topic_anchor_id}"
                        )
                    )
                    
    except Exception as e:
        logger.error(f"Telethon Topic Fetch Error: {e}")
        markup.add(InlineKeyboardButton("❌ Failed To Load Topics", callback_data="noop"))
        
    markup.add(InlineKeyboardButton("🔙 Back to Chats", callback_data="pair_add_start"))
    return markup

# -----------------------------
# Userbot Logic
# -----------------------------
async def get_or_create_target_topic(client, target_chat_id, topic_title, source_chat_id=None, source_topic_id=None, icon_emoji_id=None):
    """
    Search for a topic by title in target chat. If not found, create it.
    Uses database mapping first, then topic_cache.
    """
    if not topic_title: return None
    
    t_chat_id = int(target_chat_id)
    title_key = topic_title.lower().strip()
    logger.info(f"TOPIC_SEARCH: Looking for '{topic_title}' (Key: '{title_key}') in {t_chat_id}")
    
    # 1) Check Database Mapping (Most Reliable)
    if source_chat_id and source_topic_id:
        existing_mapping = get_topic_mapping(source_chat_id, source_topic_id, t_chat_id)
        if existing_mapping:
            return existing_mapping
    
    # 2) Check Cache
    if t_chat_id in topic_cache and title_key in topic_cache[t_chat_id]:
        res = topic_cache[t_chat_id][title_key]
        if source_chat_id and source_topic_id:
            save_topic_mapping(source_chat_id, source_topic_id, t_chat_id, res)
        return res
    
    # 3) Fetch Topics from Telegram
    try:
        result = await client(functions.channels.GetForumTopicsRequest(
            channel=t_chat_id,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        if t_chat_id not in topic_cache:
            topic_cache[t_chat_id] = {}
            
        for topic in result.topics:
            topic_cache[t_chat_id][topic.title.lower().strip()] = topic.top_message
            
        if title_key in topic_cache[t_chat_id]:
            res = topic_cache[t_chat_id][title_key]
            if source_chat_id and source_topic_id:
                save_topic_mapping(source_chat_id, source_topic_id, t_chat_id, res)
            return res
            
        # 4) Create if not found
        logger.info(f"MIRROR: Creating new topic '{topic_title}' in {t_chat_id} (Icon: {icon_emoji_id})")
        created = await client(functions.channels.CreateForumTopicRequest(
            channel=t_chat_id,
            title=topic_title,
            icon_emoji_id=int(icon_emoji_id) if icon_emoji_id else None
        ))
        
        await asyncio.sleep(1)
        res_after = await client(functions.channels.GetForumTopicsRequest(
            channel=t_chat_id,
            offset_date=0, offset_id=0, offset_topic=0, limit=20
        ))
        for t in res_after.topics:
            topic_cache[t_chat_id][t.title.lower().strip()] = t.top_message
            
        final_id = topic_cache[t_chat_id].get(title_key)
        if final_id and source_chat_id and source_topic_id:
            save_topic_mapping(source_chat_id, source_topic_id, t_chat_id, final_id)
            
        return final_id
    except Exception as e:
        logger.error(f"Mirroring Error (get_or_create): {e}")
        return None

async def start_userbot():
    global userbot
    api_id = get_setting("api_id")
    api_hash = get_setting("api_hash")
    session_string = get_setting("session_string")
    
    if not (api_id and api_hash and session_string):
        return False, "Missing credentials"
    
    try:
        if userbot:
            try: await userbot.disconnect()
            except: pass
            
        userbot = TelegramClient(
            StringSession(session_string),
            int(api_id),
            api_hash,
            device_model="PC 64bit",
            system_version="Windows 11",
            app_version="4.11.2"
        )
        await userbot.connect()
        # Cache user identity for synchronous access in UI
        userbot._me = await userbot.get_me()
        # Register automation handlers
        setup_automation_handlers(userbot)
        return True, "Userbot started"
    except Exception as e:
        return False, str(e)

async def ensure_userbot():
    """Ensures the userbot is connected and ready."""
    global userbot
    if not userbot:
        ok, msg = await start_userbot()
        if not ok: return False, msg
    
    if not userbot.is_connected():
        try: 
            await userbot.connect()
            setup_automation_handlers(userbot)
        except Exception as e: 
            return False, f"Connection failed: {e}"
    
    return True, "Connected"

async def forward_to_log_bots(client, message, source_chat_id, source_msg_id):
    """Sends collected content to all registered log bots."""
    if not message: return
    bots = get_log_bots()
    if not bots: return
    
    for token, username, bot_id in bots:
        # Run vaulting in background tasks
        asyncio.create_task(vault_media(client, message, int(source_chat_id), int(bot_id), int(source_msg_id), username))

async def vault_media(client, message, source_chat_id, log_chat_id, source_msg_id, t_name):
    """Helper to forward to vault and save the permanent File ID"""
    try:
        # RESOLVE ENTITY: Fetch the access hash for the bot
        try:
            target_peer = await client.get_input_entity(int(log_chat_id))
        except:
            # If not found, try to 'find' the bot by ID directly (forces session lookup)
            target_peer = await client.get_entity(int(log_chat_id))

        # SEND CONTENT with metadata for Log Bot extraction
        metadata = f"SID: {source_chat_id} | MID: {source_msg_id}\n"
        caption_text = metadata + (message.message or "")
        
        try:
            vaulted = await client.send_message(
                entity=target_peer,
                file=message.media if message.media else None,
                message=caption_text
            )
            await asyncio.sleep(2)
        except errors.FloodWaitError as fwe:
            logger.warning(f"⏳ VAULT FLOOD: Waiting {fwe.seconds}s...")
            await asyncio.sleep(fwe.seconds)
        except Exception as e:
            if "protected" in str(e).lower() or "forward" in str(e).lower():
                logger.info(f"🛡️ VAULT: Protected chat detected. Attempting direct download/upload...")
                path = await client.download_media(message)
                vaulted = await client.send_message(
                    entity=target_peer,
                    file=path,
                    message=caption_text
                )
                if path and os.path.exists(path): os.remove(path)
            else:
                raise e
            
        if vaulted:
            logger.info(f"✅ VAULT: Message {source_msg_id} logged successfully to @{t_name}")
            # Immediately save log_msg_id to prevent NoneType errors in release
            save_logged_media(
                bot_id=int(log_chat_id),
                log_msg_id=int(vaulted.id),
                source_chat_id=int(source_chat_id),
                source_msg_id=int(source_msg_id),
                file_id=None, # Bot API file_id will be filled by the Log Bot's own listener
                media_type=type(message.media).__name__ if message.media else "text",
                caption=message.message or ""
            )
    except Exception as e:
        logger.error(f"VAULT ERROR for @{t_name}: {e}")

def setup_automation_handlers(client: TelegramClient):
    @client.on(events.NewMessage)
    async def auto_handler(event):
        m = event.message
        if not m: return

        # --- BAN LIST CHECK ---
        sender_id = m.sender_id
        sender_username = getattr(m.sender, 'username', None)
        if is_user_banned(sender_id, sender_username):
            logger.info(f"🚫 BLOCKED: Ignored message from banned user {sender_id} (@{sender_username})")
            return

        pairs = get_target_pairs()
        for pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf in pairs:
            
            # Normalize ID matching
            source_id_str = str(sid).replace("-100", "")
            msg_id_str = str(m.chat_id).replace("-100", "")

            if source_id_str == msg_id_str:
                # --- TOPIC DETECTION ---
                msg_topic_anchor = None
                if m.reply_to:
                    msg_topic_anchor = getattr(m.reply_to, 'reply_to_top_id', None) or m.reply_to.reply_to_msg_id
                
                if not msg_topic_anchor and getattr(m, 'forum_topic', False):
                    msg_topic_anchor = m.id
                
                if not msg_topic_anchor and m.reply_to_msg_id:
                    msg_topic_anchor = m.reply_to_msg_id

                # --- CONTENT FILTERING ---
                cf = cf or "everything"
                if cf == "media" and not m.media:
                    continue
                if cf == "text" and m.media:
                    continue

                # Specific topic filtering
                if s_topic and str(s_topic) not in [None, 0, "0", "None"]:
                    if str(msg_topic_anchor) != str(s_topic):
                        continue

                # --- LOGGING & COLLECTION LOGIC (is_mon) ---
                if is_mon and m.media:
                    m_type = type(m.media).__name__
                    with db_conn() as conn:
                        c = conn.cursor()
                        if DATABASE_URL:
                            c.execute(
                                "INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                (pid, sid, m.id, m_type, m.message or "")
                            )
                        else:
                            c.execute(
                                "INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES (?, ?, ?, ?, ?)",
                                (pid, sid, m.id, m_type, m.message or "")
                            )
                    
                    # Instantly send to log bots in background
                    asyncio.create_task(forward_to_log_bots(client, m, sid, m.id))

                if not is_live: 
                    # If we matched a pair but live is off, we still break to prevent 
                    # processing the same message for other generic pairs.
                    break

                # --- ALBUM / SINGLE MESSAGE LOGIC ---
                if m.grouped_id:
                    if m.grouped_id not in album_cache:
                        album_cache[m.grouped_id] = [m]
                        # Wait for all parts
                        async def delayed_send(gid, t_id, mir_toggle, s_id, def_topic):
                            await asyncio.sleep(5.0) 
                            messages = album_cache.pop(gid, [])
                            if messages:
                                await send_mirrored_content(client, t_id, messages, def_topic, mir_toggle, s_id)
                        asyncio.create_task(delayed_send(m.grouped_id, tid, is_mir, sid, t_topic))
                else:
                    await send_mirrored_content(client, tid, [m], t_topic, is_mir, sid)
                
                # CRITICAL: Break the pair loop once the message is handled to prevent duplication
                break

    async def send_mirrored_content(client, tid, messages, default_t_topic, is_mir, sid):
        """Unified Hub for mirrored sending with native Forum Topic support."""
        try:
            if not messages: return
            first_msg = messages[0]
            dest_topic_id = default_t_topic
            
            # 1. Resolve Topic Mapping
            if is_mir:
                source_top = getattr(first_msg.reply_to, 'reply_to_top_id', None) or first_msg.reply_to.reply_to_msg_id
                if source_top:
                    forum = getattr(first_msg.reply_to, "forum_topic", None)
                    src_title = getattr(forum, "title", None)
                    src_icon = None
                    if not src_title:
                        try:
                            res = await client(functions.channels.GetForumTopicsRequest(channel=int(sid), offset_date=0, offset_id=0, offset_topic=0, limit=100))
                            for t in res.topics:
                                if t.id == source_top:
                                    src_title = t.title
                                    src_icon = getattr(t, "icon_emoji_id", None)
                                    break
                        except: pass
                    
                    if src_title:
                        logger.info(f"MIRROR: Resolved source topic title: '{src_title}' (Icon: {src_icon})")
                        dest_topic_id = await get_or_create_target_topic(client, tid, src_title, sid, source_top, icon_emoji_id=src_icon)
                    else:
                        logger.warning(f"MIRROR: Could not resolve title for source topic {source_top}")

            # 2. Check if Target is a Forum
            is_forum = False
            try:
                # Ensure we have the -100 prefix for entity lookup
                real_tid = tid if str(tid).startswith("-100") else int(f"-100{str(tid).replace('-100', '')}")
                try:
                    tgt_ent = await client.get_entity(real_tid)
                    is_forum = getattr(tgt_ent, 'forum', False)
                except:
                    tgt_ent = await client.get_entity(tid)
                    is_forum = getattr(tgt_ent, 'forum', False)
            except: pass

            # 3. Resolve Reply Header
            reply_header = None
            if is_forum:
                reply_header = int(dest_topic_id) if dest_topic_id else None
                # If replying to a specific message inside the topic, use mapped ID
                if first_msg.reply_to_msg_id:
                    mapped = get_message_mapping(sid, first_msg.reply_to_msg_id, tid)
                    if mapped: reply_header = int(mapped)
            else:
                # Normal Group: Use Message Mapping for Replies
                if first_msg.reply_to_msg_id:
                    mapped = get_message_mapping(sid, first_msg.reply_to_msg_id, tid)
                    if mapped: reply_header = int(mapped)

            # 4. Send Content
            album_text = next((msg.message for msg in messages if msg.message), "")
            sent = None
            
            for attempt in range(3):
                try:
                    sent = await client.send_message(
                        entity=int(tid), 
                        message=album_text, 
                        file=[m.media for m in messages] if len(messages) > 1 else messages[0].media,
                        reply_to=reply_header
                    )
                    if sent:
                        first_id = sent[0].id if isinstance(sent, list) else sent.id
                        logger.info(f"✅ MIRROR: Sent to {tid} -> MSG ID: {first_id}")
                        save_message_mapping(sid, first_msg.id, tid, first_id)
                        break # Success!
                except errors.FloodWaitError as fwe:
                    logger.warning(f"⏳ MIRROR FLOOD: Waiting {fwe.seconds}s...")
                    await asyncio.sleep(fwe.seconds)
                except (errors.rpcerrorlist.WorkerBusyTooLongRetryError, errors.rpcerrorlist.TimedOutError):
                    await asyncio.sleep(2)
                except Exception as e:
                    err_msg = str(e).lower()
                    if "protected" in err_msg or "forward" in err_msg or "restricted" in err_msg:
                        logger.info("🛡️ MIRROR: Protected chat detected. Using fallback...")
                        sent = await execute_fallback_mirror(client, sid, tid, messages, first_msg, album_text, reply_header)
                        if sent: break # Success via fallback
                    else:
                        logger.error(f"MIRROR SEND ATTEMPT {attempt+1} FAILED: {e}")
                        if attempt == 2: # Last attempt
                            logger.error(f"❌ MIRROR: Final failure for message {first_msg.id}")
            
        except Exception as e:
            logger.error(f"Global Mirror Error: {e}")

    async def execute_fallback_mirror(client, sid, tid, messages, first_msg, album_text, reply_header):
        """Downloads and re-uploads protected content."""
        import os
        downloaded = []
        try:
            for m in messages:
                if m.media:
                    path = await client.download_media(m.media)
                    if path: downloaded.append(path)
            if downloaded:
                sent = await client.send_message(
                    entity=int(tid), message=album_text, 
                    file=downloaded if len(downloaded) > 1 else downloaded[0],
                    reply_to=reply_header
                )
                if sent:
                    first_id = sent[0].id if isinstance(sent, list) else sent.id
                    save_message_mapping(sid, first_msg.id, tid, first_id)
        finally:
            for p in downloaded:
                if os.path.exists(p): os.remove(p)

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
    
    if not userbot or not userbot.is_connected():
        bot.send_message(message.chat.id, "❌ Userbot is not connected. Use /start to connect first.")
        return

    status_msg = bot.send_message(message.chat.id, "🔍 Fetching your chats...")
    
    async def fetch_and_list():
        try:
            text = "📋 **Your Groups & Channels**\n\n"
            async for dialog in userbot.iter_dialogs(limit=50):
                entity = dialog.entity
                if isinstance(entity, (types.Chat, types.Channel)):
                    chat_type = "📢 Channel" if isinstance(entity, types.Channel) and entity.broadcast else "👥 Group"
                    title = entity.title or "Untitled"
                    text += f"{chat_type}: `{title}`\nID: `{entity.id}`\n\n"
            
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
    
@bot.message_handler(commands=['extract'])
def cmd_extract_media(message):
    """Retrieves media from your Vault using source message ID"""
    if message.from_user.id != ADMIN_ID: return
    try:
        args = message.text.split()
        if len(args) < 2: 
            return bot.reply_to(message, "💡 **Usage:** `/extract [message_id]`\n\nFind the ID in your collected logs.", parse_mode="Markdown")
        
        smid = args[1]
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT file_id, media_type FROM media_logs WHERE source_message_id = %s LIMIT 1", (smid,))
            res = c.fetchone()
        
        if res:
            file_id, m_type = res
            m_type = m_type.lower()
            
            bot.send_chat_action(message.chat.id, 'upload_document')
            caption = f"✅ **Extracted from Vault**\n\n🆔 Source ID: `{smid}`\n📂 Type: `{m_type}`"
            
            if "photo" in m_type:
                bot.send_photo(message.chat.id, file_id, caption=caption, parse_mode="Markdown")
            elif "video" in m_type:
                bot.send_video(message.chat.id, file_id, caption=caption, parse_mode="Markdown")
            else:
                bot.send_document(message.chat.id, file_id, caption=caption, parse_mode="Markdown")
        else:
            bot.reply_to(message, "❌ No record found in the vault for this ID.")
    except Exception as e:
        bot.reply_to(message, f"❌ Extraction Error: {e}")

@bot.message_handler(commands=['ping'])
def cmd_ping(message):
    if message.from_user.id != ADMIN_ID: return
    bot.reply_to(message, f"🏓 **Pong!**\n\nI am currently awake and running.\nTime: `{datetime.now().strftime('%H:%M:%S')}`", parse_mode="Markdown")

@bot.message_handler(commands=['ban'])
def cmd_ban_user(message):
    if message.from_user.id != ADMIN_ID: return
    try:
        args = message.text.split()
        if len(args) < 2:
            bot.reply_to(message, "💡 **Usage:** `/ban [username_or_id]`", parse_mode="Markdown")
            return
        
        target = args[1].replace("@", "")
        uid, uname = None, None
        if target.isdigit(): uid = int(target)
        else: uname = target
        
        ban_user(user_id=uid, username=uname)
        bot.reply_to(message, f"✅ **User Banned:** `{target}`\nTheir messages will no longer be processed.", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Ban Error: {e}")

@bot.message_handler(commands=['unban'])
def cmd_unban_user(message):
    if message.from_user.id != ADMIN_ID: return
    try:
        args = message.text.split()
        if len(args) < 2:
            bot.reply_to(message, "💡 **Usage:** `/unban [username_or_id]`", parse_mode="Markdown")
            return
        
        target = args[1].replace("@", "")
        uid, uname = None, None
        if target.isdigit(): uid = int(target)
        else: uname = target
        
        unban_user(user_id=uid, username=uname)
        bot.reply_to(message, f"✅ **User Unbanned:** `{target}`", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Unban Error: {e}")

@bot.message_handler(commands=['banlist'])
def cmd_ban_list(message):
    if message.from_user.id != ADMIN_ID: return
    bot.send_message(message.chat.id, "🚫 **Banned Users**\n\nMessages from these users are ignored by all automated tasks:", reply_markup=banlist_markup(), parse_mode="Markdown")

@bot.message_handler(commands=["logout"])
def cmd_logout(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Yes, Logout", callback_data="user_logout_do"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="dash_main"))
    bot.send_message(message.chat.id, "⚠️ **Logout Confirmation**\n\nThis will stop the userbot and delete the session from the database. Are you sure?", reply_markup=markup, parse_mode="Markdown")

async def finalize_pair_task(call, uid):
    try:
        data = login_data.get(uid)
        if not data:
            bot.send_message(call.message.chat.id, "❌ Session expired. Please try again.")
            return

        sid = data["source_id"]
        stid = data["source_topic_id"]
        tid = data["target_id"]
        ttid = data["target_topic_id"]

        bot.edit_message_text("⏳ Resolving pair details...", call.message.chat.id, call.message.message_id)
        
        s_chat = await userbot.get_entity(sid)
        t_chat = await userbot.get_entity(tid)
        
        s_title = getattr(s_chat, 'title', None) or getattr(s_chat, 'first_name', None) or str(sid)
        t_title = getattr(t_chat, 'title', None) or getattr(t_chat, 'first_name', None) or str(tid)
        
        add_target_pair(sid, stid, tid, ttid, s_title, t_title)
        
        success_text = f"✅ **Pair Added!**\n\n"
        success_text += f"Source: `{s_title}`" + (f" (Topic: `{stid}`)" if stid else "") + "\n"
        success_text += f"Target: `{t_title}`" + (f" (Topic: `{ttid}`)" if ttid else "")
        
        bot.send_message(call.message.chat.id, success_text, parse_mode="Markdown")
        bot.send_message(call.message.chat.id, "🎯 **Target Pairs**", reply_markup=pairs_list_markup())
        
        # Cleanup
        login_data.pop(uid, None)
    except Exception as e:
        logger.error(f"Finalize Pair Error: {e}")
        bot.send_message(call.message.chat.id, f"❌ Pair error: {e}")

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

    elif data == "vault_rel_main":
        bot.answer_callback_query(call.id)
        sources = get_vault_sources()
        if not sources:
            bot.edit_message_text("❌ No vaulted media found. Make sure you have collected media and set up a Log Target.", call.message.chat.id, call.message.message_id, reply_markup=get_dashboard_markup())
            return
            
        markup = InlineKeyboardMarkup(row_width=1)
        for sid, title in sources:
            markup.add(InlineKeyboardButton(f"📁 {title}", callback_data=f"vault_src_{sid}"))
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
        
        bot.edit_message_text("🚀 **Vault Release Engine**\n\nSelect the source group you want to release media for:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("vault_src_"):
        bot.answer_callback_query(call.id)
        sid = int(data.split("_")[-1])
        login_data[uid] = {"vault_source_id": sid}
        
        async def show_tgt():
            markup = await get_chat_selection_markup("vault_tgt", 0)
            bot.edit_message_text("🎯 **Select Target Chat**\n\nChoose the group/channel where you want to release this media.\n⚠️ **IMPORTANT**: The Main Bot must be an admin in the target chat!", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(show_tgt(), loop)
        
    elif data.startswith("vault_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_tgt_list():
                markup = await get_chat_selection_markup("vault_tgt", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_tgt_list(), loop)
        else:
            tid = int(parts[2])
            sid = login_data.get(uid, {}).get("vault_source_id")
            if not sid:
                bot.send_message(call.message.chat.id, "❌ Session expired. Please start over.")
                return
            
            # Start background task
            login_data.pop(uid, None)
            bot.edit_message_text(f"🚀 **Starting Vault Release**\n\nDistributing media to target: `{tid}`\nThis may take some time due to Telegram rate limits.", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
            asyncio.run_coroutine_threadsafe(run_vault_release(bot, call.message.chat.id, sid, tid), loop)

    elif data == "log_bot_main":
        bot.answer_callback_query(call.id)
        bot.edit_message_text("📜 **Log Bot System**\nManage your backup bots and storage:", call.message.chat.id, call.message.message_id, reply_markup=log_bot_list_markup(), parse_mode="Markdown")

    elif data == "banlist_main":
        bot.answer_callback_query(call.id)
        bot.edit_message_text("🚫 **Banned Users List**\n\nSelect a user to unban or add a new one:", call.message.chat.id, call.message.message_id, reply_markup=banlist_markup(), parse_mode="Markdown")

    elif data == "ban_add_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_ban_target"
        bot.send_message(call.message.chat.id, "🚫 **Add to Ban List**\n\nPlease send the **Username** or **User ID** you want to block.")

    elif data.startswith("unban_confirm_"):
        target = data.replace("unban_confirm_", "")
        bot.answer_callback_query(call.id)
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Confirm Unban", callback_data=f"unban_do_{target}"))
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data="banlist_main"))
        bot.edit_message_text(f"❓ **Unban User:** `{target}`?", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("unban_do_"):
        target = data.replace("unban_do_", "")
        bot.answer_callback_query(call.id, "User Unbanned")
        uid, uname = None, None
        if target.isdigit(): uid = int(target)
        else: uname = target
        unban_user(user_id=uid, username=uname)
        bot.edit_message_text("🚫 **Banned Users List**\n\nSelect a user to unban or add a new one:", call.message.chat.id, call.message.message_id, reply_markup=banlist_markup(), parse_mode="Markdown")

    elif data == "log_bot_add_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_log_bot_token"
        bot.send_message(call.message.chat.id, "📜 **Add Log Bot**\nPlease send the **Bot Token** of your backup bot.\n\n_Note: You must create this bot via @BotFather._")

    elif data.startswith("log_bot_view_"):
        bot_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        stats = get_logged_media_stats(bot_id)
        
        # Find username
        bots = get_log_bots()
        username = next((b[1] for b in bots if b[2] == bot_id), "Unknown")
        
        text = f"🤖 **Log Bot:** @{username}\n\n"
        text += f"📊 **Stats:**\n"
        text += f"📦 Total Items: `{stats}`\n\n"
        text += "_Click Fetch to download a file containing all logged media IDs._"
        
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=log_bot_view_markup(bot_id), parse_mode="Markdown")

    elif data.startswith("log_bot_fetch_"):
        bot_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "📂 Generating log file...")
        
        media = fetch_logged_media(bot_id)
        if not media:
            bot.send_message(call.message.chat.id, "❌ No logs found for this bot.")
            return
            
        file_content = f"LOG MEDIA REPORT - BOT ID: {bot_id}\n"
        file_content += "="*40 + "\n\n"
        for sid, smid, fid, mtype, cap in media:
            file_content += f"SOURCE: {sid} | MSG: {smid} | TYPE: {mtype}\n"
            file_content += f"FILE_ID: {fid}\n"
            if cap: file_content += f"CAPTION: {cap[:50]}...\n"
            file_content += "-"*20 + "\n"
            
        filename = f"log_media_{bot_id}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(file_content)
            
        # Find username
        bots = get_log_bots()
        username = next((b[1] for b in bots if b[2] == bot_id), str(bot_id))
        
        with open(filename, "rb") as f:
            bot.send_document(call.message.chat.id, f, caption=f"📂 Media Logs for @{username}")
        
        try: os.remove(filename)
        except: pass

    elif data.startswith("log_bot_delete_confirm_"):
        bot_id = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Confirm Delete", callback_data=f"log_bot_delete_do_{bot_id}"))
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"log_bot_view_{bot_id}"))
        bot.edit_message_text(f"⚠️ **Delete Log Bot?**\n\nThis will stop the bot and delete all `{get_logged_media_stats(bot_id)}` logged media records from the database. This cannot be undone!", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("log_bot_delete_do_"):
        bot_id = int(data.split("_")[-1])
        delete_log_bot(bot_id)
        # Remove from fleet
        if bot_id in log_bot_manager.bots:
            try:
                log_bot_manager.bots[bot_id].stop_polling()
                del log_bot_manager.bots[bot_id]
            except: pass
            
        bot.answer_callback_query(call.id, "Log Bot Removed")
        bot.edit_message_text("📜 **Log Bot System**\nManage your backup bots and storage:", call.message.chat.id, call.message.message_id, reply_markup=log_bot_list_markup(), parse_mode="Markdown")

    elif data == "pairs_main":
        bot.answer_callback_query(call.id)
        try:
            markup = pairs_list_markup()
            bot.edit_message_text("🎯 **Target Pairs**\nSelect a pair to manage collection or release:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Pairs List Error: {e}")
            bot.send_message(call.message.chat.id, f"❌ Error loading pairs: {e}")

    elif data == "pair_add_start":
        bot.answer_callback_query(call.id, "🔍 Loading your chats...")
        async def show_src_list():
            try:
                is_ok, msg = await ensure_userbot()
                if not is_ok:
                    bot.send_message(call.message.chat.id, f"❌ Userbot connection failed: {msg}\n\nPlease go to **👤 User Account** and ensure your session is active.")
                    return
                
                markup = await get_chat_selection_markup("sel_src", 0)
                if markup:
                    bot.edit_message_text("🎯 **Select Source Chat**\nChoose the group or channel to collect from:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                else:
                    bot.edit_message_text("❌ No chats found. Make sure your userbot is in at least one group or channel.", call.message.chat.id, call.message.message_id, reply_markup=get_dashboard_markup())
            except Exception as e:
                logger.error(f"Add Pair Start Error: {e}")
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")
        asyncio.run_coroutine_threadsafe(show_src_list(), loop)

    elif data.startswith("sel_src_topic_"):
        bot.answer_callback_query(call.id)
        # Safer parsing for negative IDs with underscores
        payload = data.replace("sel_src_topic_", "", 1)
        sid_str, stid_str = payload.rsplit("_", 1)
        sid = int(sid_str)
        stid = int(stid_str)
        
        # 0 = entire topic group/forum
        if stid == 0:
            stid = None
            
        login_data[uid] = {"source_id": sid, "source_topic_id": stid}
        async def show_tgt():
            markup = await get_chat_selection_markup("sel_tgt", 0)
            bot.edit_message_text("🎯 **Select Target Chat**\nChoose the group or channel to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(show_tgt(), loop)

    elif data.startswith("sel_src_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_src_list():
                markup = await get_chat_selection_markup("sel_src", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_src_list(), loop)
        else:
            sid = int(parts[2])
            async def handle_src():
                try:
                    full_chat = await userbot.get_entity(sid)
                    is_forum = getattr(full_chat, "forum", False)
                    
                    if is_forum:
                        markup = await get_topic_selection_markup(sid, "sel_src_topic")
                        bot.edit_message_text(f"🧵 **『 {getattr(full_chat, 'title', 'Forum')} 』**\nSelect a source topic:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                    else:
                        login_data[uid] = {"source_id": sid, "source_topic_id": None}
                        markup = await get_chat_selection_markup("sel_tgt", 0)
                        bot.edit_message_text("🎯 **Select Target Chat**\nChoose the group or channel to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                except Exception as e:
                    bot.send_message(call.message.chat.id, f"❌ Error: {e}")
            asyncio.run_coroutine_threadsafe(handle_src(), loop)

    elif data.startswith("sel_tgt_topic_"):
        bot.answer_callback_query(call.id)
        # Safer parsing for negative IDs with underscores
        payload = data.replace("sel_tgt_topic_", "", 1)
        tid_str, ttid_str = payload.rsplit("_", 1)
        tid = int(tid_str)
        ttid = int(ttid_str)
        
        # 0 = entire topic group/forum
        if ttid == 0:
            ttid = None
            
        login_data[uid]["target_id"] = tid
        login_data[uid]["target_topic_id"] = ttid
        asyncio.run_coroutine_threadsafe(finalize_pair_task(call, uid), loop)

    elif data.startswith("sel_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_tgt_list():
                markup = await get_chat_selection_markup("sel_tgt", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_tgt_list(), loop)
        else:
            tid = int(parts[2])
            async def handle_tgt():
                try:
                    full_chat = await userbot.get_entity(tid)
                    is_forum = getattr(full_chat, "forum", False)
                    
                    if is_forum:
                        markup = await get_topic_selection_markup(tid, "sel_tgt_topic")
                        bot.edit_message_text(f"🧵 **『 {getattr(full_chat, 'title', 'Forum')} 』**\nSelect a target topic:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                    else:
                        login_data[uid]["target_id"] = tid
                        login_data[uid]["target_topic_id"] = None
                        await finalize_pair_task(call, uid)
                except Exception as e:
                    bot.send_message(call.message.chat.id, f"❌ Error: {e}")
            asyncio.run_coroutine_threadsafe(handle_tgt(), loop)

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

    elif data.startswith("pair_toggle_mir_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        if not row:
            bot.answer_callback_query(call.id, "Pair not found")
            return
        # row[7] is is_mirror
        new_val = 0 if row[7] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_mirror = {p} WHERE id = {p}", (new_val, pid))
        bot.answer_callback_query(call.id, f"Mirror Mode {'Enabled' if new_val else 'Disabled'}")
        show_pair_view(call.message.chat.id, call.message.message_id, pid)

    elif data.startswith("pair_toggle_filter_"):
        pid = int(data.split("_")[-1])
        pair = get_target_pair(pid)
        if not pair: return
        current = pair[10] or "everything"
        next_filter = "media" if current == "everything" else "text" if current == "media" else "everything"
        
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET content_filter = {p} WHERE id = {p}", (next_filter, pid))
        
        bot.answer_callback_query(call.id, f"🎯 Filter: {next_filter.title()}")
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
                try: await userbot.disconnect()
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
            async for dialog in userbot.iter_dialogs():
                entity = dialog.entity
                if category == "groups" and isinstance(entity, (types.Chat, types.Channel)) and not getattr(entity, 'broadcast', False):
                    all_dialogs.append(entity)
                elif category == "channels" and isinstance(entity, types.Channel) and entity.broadcast:
                    all_dialogs.append(entity)
                elif category == "bots" and isinstance(entity, types.User) and entity.bot:
                    all_dialogs.append(entity)
                elif category == "private" and isinstance(entity, types.User) and not entity.bot:
                    all_dialogs.append(entity)
            
            # Pagination
            page_size = 8
            start = page * page_size
            end = start + page_size
            page_items = all_dialogs[start:end]
            
            markup = InlineKeyboardMarkup(row_width=1)
            for chat in page_items:
                title = getattr(chat, 'title', None) or getattr(chat, 'first_name', None) or str(chat.id)
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
                chat = await userbot.get_entity(chat_id)
                # For message count, we can use a trick with limit=0
                history = await userbot.get_messages(chat, limit=0)
                msg_count = history.total
                title = getattr(chat, 'title', None) or getattr(chat, 'first_name', 'Unknown')
                
                info = f"📋 **Chat Details:**\n\n"
                info += f"🏷 **Title:** `{title}`\n"
                info += f"🆔 **ID:** `{chat.id}`\n"
                info += f"📂 **Type:** `{type(chat).__name__}`\n"
                info += f"💬 **Messages:** `{msg_count}`\n"
                
                if hasattr(chat, 'username') and chat.username:
                    info += f"🔗 **Username:** @{chat.username}\n"
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🔙 Back to List", callback_data=f"user_acc_main"))
                
                bot.edit_message_text(info, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

            except Exception as e:
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")

        asyncio.run_coroutine_threadsafe(run_view(), loop)

async def complete_login(uid, client: TelegramClient, chat_id):
    session_string = client.session.save()
    set_setting("api_id", login_data[uid]["api_id"])
    set_setting("api_hash", login_data[uid]["api_hash"])
    set_setting("session_string", session_string)
    
    admin_states.pop(uid, None)
    login_data.pop(uid, None)
    
    bot.send_message(chat_id, "✅ **Userbot Connected (Telethon)!**", parse_mode="Markdown")
    
    # Restart the global userbot with new session
    ok, msg = await start_userbot()
    if ok:
        bot.send_message(chat_id, get_dashboard_text(), reply_markup=get_dashboard_markup(), parse_mode="Markdown")
    else:
        bot.send_message(chat_id, f"❌ Failed to start userbot: {msg}")

@bot.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and admin_states.get(m.from_user.id))
def handle_state_inputs(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    text = message.text.strip()
    
    # --- Ban List System ---
    if state == "awaiting_ban_target":
        target = text.replace("@", "")
        b_uid, b_uname = None, None
        if target.isdigit(): b_uid = int(target)
        else: b_uname = target
        
        ban_user(user_id=b_uid, username=b_uname)
        admin_states.pop(uid, None)
        bot.reply_to(message, f"✅ **User Banned:** `{target}`\nTheir messages will no longer be processed.", parse_mode="Markdown")
        bot.send_message(message.chat.id, "🚫 **Banned Users**", reply_markup=banlist_markup(), parse_mode="Markdown")

    # --- Logging System ---
    # --- Log Bot System ---
    if state == "awaiting_log_bot_token":
        token = text
        admin_states.pop(uid, None)
        bot.send_message(message.chat.id, "⏳ Verifying Log Bot Token...")
        
        try:
            temp_bot = telebot.TeleBot(token)
            bot_info = temp_bot.get_me()
            
            # Save to DB
            add_log_bot(token, bot_info.username, bot_info.id)
            
            # Start in fleet
            log_bot_manager.add_bot(token)
            
            bot.send_message(message.chat.id, f"✅ **Log Bot Added!**\nUsername: @{bot_info.username}\nID: `{bot_info.id}`", parse_mode="Markdown")
            bot.send_message(message.chat.id, "📜 **Log Bot System**", reply_markup=log_bot_list_markup(), parse_mode="Markdown")
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Failed to verify Bot Token: {e}")
            
    # --- Login Flow ---
    elif state == "awaiting_api_id":
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
        bot.send_message(message.chat.id, "⏳ Sending OTP (Telethon)...")
        async def send_otp_task():
            try:
                temp_client = TelegramClient(StringSession(), login_data[uid]["api_id"], login_data[uid]["api_hash"])
                await temp_client.connect()
                send_code = await temp_client.send_code_request(login_data[uid]["phone"])
                login_data[uid]["client"] = temp_client
                login_data[uid]["phone_code_hash"] = send_code.phone_code_hash
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
                await client.sign_in(phone=login_data[uid]["phone"], code=otp, phone_code_hash=login_data[uid]["phone_code_hash"])
                bot.send_message(message.chat.id, "✅ OTP Verified!")
                await complete_login(uid, client, message.chat.id)
            except errors.SessionPasswordNeededError:
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
                await client.sign_in(password=text)
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
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic = pair
    
    collected = 0
    scanned = 0
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛑 Stop Scrape", callback_data=f"pair_stop_task_hist_{pair_id}"))
    status_msg = bot.send_message(admin_chat_id, f"📜 **History Scrape: `{s_title}`**\n\n🔍 Scanned: `0`\n📥 Collected: `0`", reply_markup=markup, parse_mode="Markdown")
    
    try:
        # Force peer resolution (Anti PeerIdInvalid)
        target_chat = await resolve_target_id(userbot, sid)
        sid_resolved = target_chat.id
        
        # Telethon uses iter_messages for history
        target_topic = int(s_topic) if s_topic and str(s_topic) != "0" else None
        async for m in userbot.iter_messages(sid_resolved, reply_to=target_topic):
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 History scrape for `{s_title}` stopped by user.")
                break
            
            scanned += 1
            # Date filter
            if end_date and m.date > end_date: continue
            if start_date and m.date < start_date: break # History is newest to oldest

            # --- BAN LIST CHECK ---
            sender_id = m.sender_id
            sender_username = getattr(m.sender, 'username', None)
            if is_user_banned(sender_id, sender_username):
                continue

            if m.media:
                m_type = type(m.media).__name__
                with db_conn() as conn:
                    c = conn.cursor()
                    if DATABASE_URL:
                        c.execute(
                            "INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (pair_id, sid_resolved, m.id, m_type, m.message or "")
                        )
                    else:
                        c.execute(
                            "INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES (?, ?, ?, ?, ?)",
                            (pair_id, sid_resolved, m.id, m_type, m.message or "")
                        )
                    if c.rowcount > 0: 
                        collected += 1
                        # Instantly send to log targets
                        await forward_to_log_bots(userbot, m, sid_resolved, m.id)
            
            if limit and collected >= limit: break
            
            if scanned % 50 == 0:
                l_text = f" / {limit}" if limit else ""
                try: bot.edit_message_text(f"📜 **History Scrape: `{s_title}`**\n\n🔍 Scanned: `{scanned}`\n📥 Collected: `{collected}{l_text}`", admin_chat_id, status_msg.message_id, reply_markup=markup, parse_mode="Markdown")
                except: pass
            await asyncio.sleep(0.1)
        
        bot.send_message(admin_chat_id, f"✅ History Scrape Done: `{s_title}`\nCollected: `{collected}`")
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Scrape Error: {e}")
    finally:
        running_tasks.pop(task_key, None)

async def resolve_target_id(client: TelegramClient, target_ref):
    try:
        return await client.get_entity(target_ref)
    except Exception as e:
        logger.error(f"Entity Resolve Error: {e}")
        # Try finding via dialogs if ref is just an ID
        async for dialog in client.iter_dialogs(limit=200):
            if str(dialog.id) == str(target_ref):
                return dialog.entity
    raise ValueError(f"Could not find or access chat: {target_ref}")

async def run_collection(admin_chat_id, pair_id, limit=300):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return
        
    task_key = f"coll_{pair_id}"
    running_tasks[task_key] = True
    
    row = get_target_pair(pair_id)
    if not row: return
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic = row
    collected = 0
    scanned = 0
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛑 Stop Collection", callback_data=f"pair_stop_task_coll_{pair_id}"))
    status_msg = bot.send_message(admin_chat_id, f"📥 **Collection: `{s_title}`**\n\n🔍 Scanned: `0`\n📥 New items: `0`", reply_markup=markup, parse_mode="Markdown")
    
    try:
        # Telethon iter_messages is powerful and supports reply_to (topic) filtering natively
        # Use int(s_topic) if it's a valid thread ID, otherwise None for entire chat
        target_topic = int(s_topic) if s_topic and str(s_topic) != "0" else None
        async for m in userbot.iter_messages(sid, limit=limit, reply_to=target_topic):
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 Collection for `{s_title}` stopped by user.")
                break
            
            scanned += 1
            
            # --- BAN LIST CHECK ---
            sender_id = m.sender_id
            sender_username = getattr(m.sender, 'username', None)
            if is_user_banned(sender_id, sender_username):
                continue

            if m.media:
                m_type = type(m.media).__name__
                with db_conn() as conn:
                    c = conn.cursor()
                    if DATABASE_URL:
                        c.execute("INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", (pair_id, sid, m.id, m_type, m.message or ""))
                    else:
                        c.execute("INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES (?, ?, ?, ?, ?)", (pair_id, sid, m.id, m_type, m.message or ""))
                    if c.rowcount > 0: 
                        collected += 1
                        # Instantly send to log targets
                        await forward_to_log_bots(userbot, m, sid, m.id)
            
            if scanned % 20 == 0:
                try: bot.edit_message_text(f"📥 **Collection: `{s_title}`**\n\n🔍 Scanned: `{scanned}`\n📥 New items: `{collected}`", admin_chat_id, status_msg.message_id, reply_markup=markup, parse_mode="Markdown")
                except: pass
            await asyncio.sleep(0.5)

        bot.send_message(admin_chat_id, f"✅ Collection Done: `{s_title}`\nNew items: `{collected}`")
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
            c.execute(f"SELECT source_id, target_id, source_title, is_mirror, source_topic_id, target_topic_id, content_filter FROM target_pairs WHERE id = {p}", (pair_id,))
            row = c.fetchone()
        
        if not row: return
        sid_ref, tid_ref, s_title, is_mir, s_topic, t_topic, cf = row
    
        try:
            # Pre-resolve entities to warm up Telethon cache and ensure access
            source_chat = await resolve_target_id(userbot, sid_ref)
            target_chat = await resolve_target_id(userbot, tid_ref)
        except Exception as e:
            bot.send_message(admin_chat_id, f"❌ Connection Error: {e}\n\nMake sure the bot is a member of both chats.")
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
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🛑 Stop Release", callback_data=f"pair_stop_task_rel_{pair_id}"))
        status_msg = bot.send_message(admin_chat_id, f"🚀 Releasing `{len(items)}` items...", reply_markup=markup)
        
        for row_id, smid in items:
            if not running_tasks.get(task_key): break
            
            try:
                msg = await userbot.get_messages(sid_ref, ids=smid)
                if not msg: continue

                # --- CONTENT FILTERING ---
                cf = cf or "everything"
                if cf == "media" and not msg.media:
                    # Mark as released so we don't try again
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"UPDATE collected_media SET released = 1 WHERE id = {p}", (row_id,))
                    continue
                if cf == "text" and msg.media:
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"UPDATE collected_media SET released = 1 WHERE id = {p}", (row_id,))
                    continue

                target_topic_anchor = t_topic
                
                # Handle Mirroring ID detection for release
                if is_mir:
                    s_top = None
                    if msg.reply_to:
                        s_top = getattr(msg.reply_to, 'reply_to_top_id', None) or msg.reply_to.reply_to_msg_id
                    
                    if s_top:
                        # Priority check database mapping
                        mapped = get_topic_mapping(sid_ref, s_top, tid_ref)
                        if mapped:
                            target_topic_anchor = mapped

                # Resolve reply mapping
                reply_to_val = None
                if getattr(msg, "reply_to_msg_id", None):
                    reply_to_val = get_message_mapping(sid_ref, msg.reply_to_msg_id, tid_ref)

                # Construct Topic Header
                # If it's a specific reply, use it. Otherwise, use the Topic Header ID.
                final_reply_target = reply_to_val if reply_to_val else target_topic_anchor

                sent_msg = await userbot.send_message(
                    entity=target_chat,
                    message=msg.message or "",
                    file=msg.media,
                    reply_to=int(final_reply_target) if final_reply_target else None
                )
                
                if sent_msg:
                    save_message_mapping(sid_ref, msg.id, tid_ref, sent_msg.id)
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

        bot.send_message(admin_chat_id, f"✅ Release Complete: Sent `{sent}` items.")
    except Exception as e:
        logger.error(f"Global Release Error: {e}")
        bot.send_message(admin_chat_id, f"❌ Release Crashed: {e}")
    finally:
        running_tasks.pop(task_key, None)

async def run_vault_release(sender_bot, admin_chat_id, source_id, target_id, interval=1.2, log_target_id=None):
    task_key = f"vault_rel_{source_id}_{target_id}"
    running_tasks[task_key] = True
    
    try:
        items = get_vaulted_media_for_source(source_id, log_target_id)
        if not items:
            sender_bot.send_message(admin_chat_id, "❌ No vaulted media found for this source.")
            return
            
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🛑 Stop Release", callback_data=f"lb_stop_rel_{task_key}"))
        status_msg = sender_bot.send_message(admin_chat_id, f"🚀 **Initializing Userbot Release Engine...**\nItems: `{len(items)}`", reply_markup=markup, parse_mode="Markdown")
        
        sent = 0
        failed = 0
        
        # We use the userbot to forward messages from the Log Bot's chat
        # Since the Userbot is already authorized and in the target chats, this avoids "chat not found".
        for smid, file_id, m_type, caption, log_msg_id, bot_id in items:
            if not running_tasks.get(task_key):
                sender_bot.send_message(admin_chat_id, "🛑 **Release stopped by user.**", parse_mode="Markdown")
                break
                
            try:
                # 1. Resolve the Log Bot chat peer
                log_bot_peer = await userbot.get_input_entity(int(bot_id))
                
                # 2. Forward from Log Bot to Target
                await userbot.forward_messages(
                    entity=int(target_id),
                    messages=int(log_msg_id),
                    from_peer=log_bot_peer
                )
                sent += 1
                
                if sent % 5 == 0:
                    try: sender_bot.edit_message_text(f"🚀 **Userbot Releasing...**\nSent: `{sent}/{len(items)}`", admin_chat_id, status_msg.message_id, reply_markup=markup, parse_mode="Markdown")
                    except: pass
            except Exception as item_err:
                logger.error(f"Vault Release item error (Userbot): {item_err}")
                # Fallback to Log Bot direct send if userbot fails (unlikely)
                try:
                    m_type_lower = (m_type or "").lower()
                    cap = caption or ""
                    if m_type == "Text": sender_bot.send_message(target_id, cap or " ")
                    elif "photo" in m_type_lower: sender_bot.send_photo(target_id, file_id, caption=cap)
                    elif "video" in m_type_lower: sender_bot.send_video(target_id, file_id, caption=cap)
                    else: sender_bot.send_document(target_id, file_id, caption=cap)
                    sent += 1
                except:
                    failed += 1
                
            await asyncio.sleep(interval)
            
        sender_bot.send_message(admin_chat_id, f"✅ **Release Done**\nSent: `{sent}` items.\nFailed: `{failed}`", parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Global Vault Release Error: {e}")
        sender_bot.send_message(admin_chat_id, f"❌ Vault Release Crashed: {e}")
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
        if userbot and userbot.is_connected():
            try:
                await userbot.get_me()
            except Exception as e:
                err_msg = str(e).lower()
                if "deactivated" in err_msg or "authorized" in err_msg or "simultaneous" in err_msg or "ip address" in err_msg:
                    logger.warning(f"WATCHDOG: Userbot session invalid or conflict: {e}")
                    try: await userbot.disconnect()
                    except: pass
                    userbot = None
                    
                    # Clear session from DB to force re-login
                    with db_conn() as conn:
                        c = conn.cursor()
                        c.execute("DELETE FROM settings WHERE key IN ('session_string', 'api_id', 'api_hash')")
                    logger.info("WATCHDOG: Session cleared from DB due to conflict/invalidation.")
                    
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

def shutdown_handler(*args):
    logger.warning("🛑 Shutting down cleanly...")
    try:
        bot.stop_polling()
    except:
        pass
    try:
        if userbot and userbot.is_connected():
            loop.run_until_complete(userbot.disconnect())
    except:
        pass
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# -----------------------------
# Log Bot System (Fleet Manager)
# -----------------------------
class LogBotManager:
    def __init__(self):
        self.bots = {} # { bot_id: telebot_instance }
        self.states = {} # { bot_id: { user_id: { state_data } } }

    def start_all(self):
        logger.info("📡 Initializing Log Bot Fleet...")
        bots = get_log_bots()
        for token, username, bot_id in bots:
            try:
                self.add_bot(token)
            except Exception as e:
                logger.error(f"Failed to start Log Bot {username}: {e}")

    def add_bot(self, token):
        new_bot = telebot.TeleBot(token)
        bot_info = new_bot.get_me()
        bot_id = bot_info.id
        
        if bot_id in self.bots:
            return bot_id
            
        self.bots[bot_id] = new_bot
        self._setup_handlers(new_bot, bot_id)
        
        # Start polling in a separate thread
        def run_polling():
            while True:
                try:
                    logger.info(f"🚀 Log Bot @{bot_info.username} started polling.")
                    new_bot.delete_webhook(drop_pending_updates=True)
                    # Use a shorter timeout and skip pending to reduce conflict duration
                    new_bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
                except Exception as e:
                    if "Conflict" in str(e):
                        logger.warning(f"⚠️ Log Bot @{bot_info.username} conflict. Retrying in 15s...")
                        time.sleep(15)
                    else:
                        logger.error(f"Log Bot @{bot_info.username} crashed: {e}")
                        time.sleep(10)
        
        threading.Thread(target=run_polling, daemon=True).start()
        return bot_id

    def _setup_handlers(self, bot_instance, bot_id):
        @bot_instance.message_handler(commands=['start'])
        def cmd_start(message):
            if message.from_user.id != ADMIN_ID: return
            count = get_logged_media_stats(bot_id)
            text = (f"🤖 **Vault Manager Online**\n\n"
                    f"📊 **Storage Stats:**\n"
                    f"📦 Total Vaulted: `{count}` items\n\n"
                    f"Use /grouplist to see categorized media.")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("📤 Send Log", callback_data="lb_vault_main"))
            bot_instance.send_message(message.chat.id, text, reply_markup=markup, parse_mode="Markdown")

        @bot_instance.message_handler(commands=['get'])
        def fetch_from_vault(message):
            if message.from_user.id != ADMIN_ID: return
            try:
                args = message.text.split()
                if len(args) < 2:
                    bot_instance.reply_to(message, "❌ Usage: `/get [Fetch_ID]`")
                    return
                fetch_id = args[1]
                with db_conn() as conn:
                    c = conn.cursor()
                    # Use %s for PostgreSQL
                    c.execute("SELECT file_id, media_type, caption FROM log_media WHERE log_msg_id = %s AND bot_id = %s", (fetch_id, bot_id))
                    res = c.fetchone()
                if res:
                    file_id, m_type, caption = res
                    bot_instance.send_chat_action(message.chat.id, 'upload_document')
                    if m_type == "photo": bot_instance.send_photo(message.chat.id, file_id, caption=caption)
                    elif m_type == "video": bot_instance.send_video(message.chat.id, file_id, caption=caption)
                    else: bot_instance.send_document(message.chat.id, file_id, caption=caption)
                else:
                    bot_instance.reply_to(message, "🔍 ID not found in this bot's vault.")
            except Exception as e:
                bot_instance.reply_to(message, f"❌ Error: {e}")

        @bot_instance.message_handler(commands=['getcount'])
        def fetch_recent_batch(message):
            if message.from_user.id != ADMIN_ID: return
            try:
                args = message.text.split()
                count = int(args[1]) if len(args) > 1 and args[1].isdigit() else 5
                if count > 30: count = 30 
                with db_conn() as conn:
                    c = conn.cursor()
                    c.execute("SELECT file_id, media_type, caption, log_msg_id FROM log_media WHERE bot_id = %s ORDER BY timestamp DESC LIMIT %s", (bot_id, count))
                    results = c.fetchall()
                if not results:
                    bot_instance.reply_to(message, "🔍 Vault empty.")
                    return
                for f_id, m_t, cap, l_id in reversed(results):
                    full_cap = f"🆔 ID: `{l_id}`\n\n{cap or ''}"
                    if m_t == "photo": bot_instance.send_photo(message.chat.id, f_id, caption=full_cap, parse_mode="Markdown")
                    elif m_t == "video": bot_instance.send_video(message.chat.id, f_id, caption=full_cap, parse_mode="Markdown")
                    else: bot_instance.send_document(message.chat.id, f_id, caption=full_cap, parse_mode="Markdown")
                    time.sleep(0.5)
            except Exception as e:
                bot_instance.reply_to(message, f"❌ Error: {e}")

        @bot_instance.message_handler(commands=['grouplist'])
        def cmd_group_list(message):
            if message.from_user.id != ADMIN_ID: return
            with db_conn() as conn:
                c = conn.cursor()
                # Correct GROUP BY for PostgreSQL
                c.execute("""
                    SELECT m.source_chat_id, p.source_title, COUNT(m.id)
                    FROM log_media m
                    LEFT JOIN target_pairs p ON m.source_chat_id = p.source_id
                    WHERE m.bot_id = %s
                    GROUP BY m.source_chat_id, p.source_title
                """, (bot_id,))
                groups = c.fetchall()
            if not groups:
                bot_instance.send_message(message.chat.id, "📭 No media found.")
                return
            markup = InlineKeyboardMarkup(row_width=1)
            for sid, title, cnt in groups:
                if sid is None or sid == 0: continue
                markup.add(InlineKeyboardButton(f"📁 {title or 'Direct'} — {cnt}", callback_data=f"v_group_stats_{sid}"))
            bot_instance.send_message(message.chat.id, "📂 **Vault Groups**", reply_markup=markup, parse_mode="Markdown")

        @bot_instance.message_handler(commands=['getbyid'])
        def fetch_by_group_id(message):
            if message.from_user.id != ADMIN_ID: return
            try:
                args = message.text.split()
                if len(args) < 2: return bot_instance.reply_to(message, "❌ `/getbyid [ID] [Count]`")
                group_id, count = int(args[1]), (int(args[2]) if len(args) > 2 else 5)
                with db_conn() as conn:
                    c = conn.cursor()
                    c.execute("SELECT file_id, media_type, caption, log_msg_id FROM log_media WHERE source_chat_id = %s AND bot_id = %s ORDER BY timestamp DESC LIMIT %s", (group_id, bot_id, count))
                    results = c.fetchall()
                for f_id, m_t, cap, l_id in reversed(results):
                    bot_instance.send_photo(message.chat.id, f_id, caption=f"🆔 ID: `{l_id}`\n{cap or ''}") if m_t == "photo" else bot_instance.send_document(message.chat.id, f_id, caption=f"🆔 ID: `{l_id}`")
                    time.sleep(0.5)
            except Exception as e: bot_instance.reply_to(message, f"❌ Error: {e}")

        @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("v_group_stats_"))
        def handle_group_stats(call):
            sid = int(call.data.split("_")[-1])
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder(conn)
                # FIX: Use adaptive placeholder for the title lookup
                title_query = f"SELECT source_title FROM target_pairs WHERE source_id = {p} LIMIT 1"
                c.execute(title_query, (sid,))
                res = c.fetchone()
                
                # FIX: Use adaptive placeholder for count
                c.execute(f"SELECT COUNT(*) FROM log_media WHERE source_chat_id = {p} AND bot_id = {p}", (sid, bot_id))
                total = c.fetchone()[0]

            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🚀 Send batch to Group", callback_data=f"v_dump_start_{sid}"))
            markup.add(InlineKeyboardButton("🔙 Back to List", callback_data="lb_vault_main"))

            msg = (f"📊 **Group Statistics**\n\n"
                   f"🏷 **Title:** `{res[0] if res else 'Unknown'}`\n"
                   f"🆔 **ID:** `{sid}`\n"
                   f"📦 **Total Media:** `{total}`\n\n"
                   f"💡 Click the button below to send this media into a different group via the Log Bot.")
            bot_instance.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("v_dump_start_"))
        def start_dump_flow(call):
            sid = int(call.data.split("_")[-1])
            login_data[call.from_user.id] = {"dump_sid": sid}
            
            async def get_list():
                markup = await get_chat_selection_markup("lb_vault_tgt", 0) # Reuse the tgt selection markup
                if not markup:
                    bot_instance.answer_callback_query(call.id, "❌ Main Userbot Offline", show_alert=True)
                    return
                bot_instance.edit_message_text(
                    "🎯 **Select Destination**\nWhere should the Log Bot send this media?",
                    call.message.chat.id, call.message.message_id, reply_markup=markup
                )
            asyncio.run_coroutine_threadsafe(get_list(), loop)

        @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("lb_vault_tgt_") and login_data.get(call.from_user.id, {}).get("dump_sid"))
        def finish_dump_flow(call):
            # Format: lb_vault_tgt_{tid}
            parts = call.data.split("_")
            if parts[3] == "page": return # Handled by the generic tgt handler if needed
            
            target_chat_id = int(parts[3])
            source_chat_id = login_data.get(call.from_user.id, {}).get("dump_sid")
            
            if not source_chat_id:
                bot_instance.answer_callback_query(call.id, "❌ Session expired")
                return

            login_data[call.from_user.id]["dump_tid"] = target_chat_id
            admin_states[f"lb_{bot_id}_{call.from_user.id}"] = f"wait_dump_count_{target_chat_id}"
            bot_instance.edit_message_text(
                f"🔢 **How many items?**\nEnter the number of media to send to target `{target_chat_id}`:",
                call.message.chat.id, call.message.message_id
            )

        @bot_instance.message_handler(content_types=['photo', 'video', 'document', 'audio', 'animation', 'sticker'])
        def handle_logging(message):
            try:
                m_type = "document"
                file_id = None
                caption = message.caption or ""
                if message.photo: m_type, file_id = "photo", message.photo[-1].file_id
                elif message.video: m_type, file_id = "video", message.video.file_id
                elif message.document: m_type, file_id = "document", message.document.file_id
                elif message.audio: m_type, file_id = "audio", message.audio.file_id
                elif message.animation: m_type, file_id = "animation", message.animation.file_id
                elif message.sticker: m_type, file_id = "sticker", message.sticker.file_id
                
                if file_id:
                    sid, mid = 0, message.message_id
                    if caption and "SID:" in caption and "MID:" in caption:
                        try:
                            parts = caption.split("|")
                            sid = int(parts[0].replace("SID:", "").strip())
                            mid = int(parts[1].split("\n")[0].replace("MID:", "").strip())
                            caption = caption.split("\n", 1)[1] if "\n" in caption else ""
                        except: pass
                    
                    save_logged_media(bot_id, message.message_id, sid, mid, file_id, m_type, caption)
                    if sid == 0 and message.from_user.id == ADMIN_ID:
                        bot_instance.reply_to(message, f"✅ **Saved to Vault!**\n🆔 ID: `{message.message_id}`\nFetch: `/get {message.message_id}`")
            except Exception as e: logger.error(f"Logging Error: {e}")

        @bot_instance.callback_query_handler(func=lambda call: True)
        def handle_log_bot_callbacks(call):
            if call.from_user.id != ADMIN_ID: return
            data = call.data
            uid = call.from_user.id
            
            if data == "lb_vault_main":
                bot_instance.answer_callback_query(call.id)
                stats = get_log_bot_stats(bot_id)
                if not stats:
                    bot_instance.send_message(call.message.chat.id, "❌ No vaulted media found for this bot.")
                    return
                    
                markup = InlineKeyboardMarkup(row_width=1)
                for sid, title, count in stats:
                    markup.add(InlineKeyboardButton(f"📁 {title} ({count})", callback_data=f"lb_vault_src_{sid}"))
                markup.add(InlineKeyboardButton("🔙 Cancel", callback_data="lb_cancel"))
                
                bot_instance.edit_message_text("🚀 **Select Source Group**\n\nWhich group's vaulted content do you want to send?", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

            elif data.startswith("lb_vault_src_"):
                bot_instance.answer_callback_query(call.id)
                sid = int(data.split("_")[-1])
                login_data[uid] = {"vault_source_id": sid}
                
                async def show_tgt():
                    markup = await get_chat_selection_markup("lb_vault_tgt", 0)
                    if not markup:
                        main_bot_username = bot.get_me().username
                        msg = "⚠️ **Userbot Offline**\n\nI cannot fetch your group list because the main userbot is not connected.\n\nPlease go to your **Main Admin Bot** and use the **'Connect Userbot'** button."
                        btn = InlineKeyboardMarkup().add(InlineKeyboardButton("🔌 Connect at Main Bot", url=f"https://t.me/{main_bot_username}"))
                        btn.add(InlineKeyboardButton("🔙 Back", callback_data="lb_vault_main"))
                        bot_instance.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=btn, parse_mode="Markdown")
                        return
                        
                    bot_instance.edit_message_text("🎯 **Select Target Chat**\n\nChoose the group/channel where you want to release this media.\n⚠️ **IMPORTANT**: The Main Bot must be an admin in the target chat!", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                asyncio.run_coroutine_threadsafe(show_tgt(), loop)
                
            elif data.startswith("lb_vault_tgt_"):
                bot_instance.answer_callback_query(call.id)
                parts = data.split("_")
                if parts[3] == "page":
                    page = int(parts[4])
                    async def update_tgt_list():
                        markup = await get_chat_selection_markup("lb_vault_tgt", page)
                        if markup:
                            bot_instance.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
                    asyncio.run_coroutine_threadsafe(update_tgt_list(), loop)
                else:
                    tid = int(parts[3])
                    sid = login_data.get(uid, {}).get("vault_source_id")
                    if not sid:
                        bot_instance.send_message(call.message.chat.id, "❌ Session expired. Please start over.")
                        return
                    
                    # Instead of starting, ask for interval
                    login_data[uid]["vault_target_id"] = tid
                    admin_states[f"lb_{bot_id}_{uid}"] = "awaiting_rel_interval"
                    bot_instance.edit_message_text("⏳ **Release Interval**\n\nPlease send the time period (in seconds) between each message:\n(Example: `1.5` or `3`)", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
                    
            elif data == "lb_cancel":
                bot_instance.answer_callback_query(call.id)
                admin_states.pop(f"lb_{bot_id}_{uid}", None)
                cmd_start(call.message)

            elif data.startswith("lb_stop_rel_"):
                bot_instance.answer_callback_query(call.id, "🛑 Stopping...")
                task_key = data.replace("lb_stop_rel_", "")
                stop_task(task_key)

            elif data.startswith("lb_do_release_"):
                # lb_do_release_{sid}_{tid}_{interval}
                bot_instance.answer_callback_query(call.id)
                parts = data.split("_")
                sid, tid = int(parts[3]), int(parts[4])
                interval = float(parts[5])
                bot_instance.edit_message_text(f"🚀 **Initializing Engine...**\nInterval: `{interval}s`", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
                asyncio.run_coroutine_threadsafe(run_vault_release(bot_instance, call.message.chat.id, sid, tid, interval=interval), loop)

        @bot_instance.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and admin_states.get(f"lb_{bot_id}_{m.from_user.id}"))
        def handle_lb_messages(message):
            uid = message.from_user.id
            state = admin_states.get(f"lb_{bot_id}_{uid}")
            text = message.text.strip()
            
            if state == "awaiting_rel_interval":
                try:
                    interval = float(text)
                    if interval < 0.1: raise ValueError()
                    
                    admin_states.pop(f"lb_{bot_id}_{uid}", None)
                    sid = login_data.get(uid, {}).get("vault_source_id")
                    tid = login_data.get(uid, {}).get("vault_target_id")
                    
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("🚀 Start Release", callback_data=f"lb_do_release_{sid}_{tid}_{interval}"))
                    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="lb_cancel"))
                    
                    bot_instance.send_message(message.chat.id, f"✅ **Interval Set: `{interval}s`**\nReady to release from `{sid}` to `{tid}`.", reply_markup=markup, parse_mode="Markdown")
                except:
                    bot_instance.reply_to(message, "⚠️ Invalid interval. Please send a number (e.g. `2.0`).")

            elif state.startswith("wait_dump_count_"):
                try:
                    target_cid = int(state.split("_")[-1])
                    count = int(text)
                    source_cid = login_data[uid]["dump_sid"]
                    admin_states.pop(f"lb_{bot_id}_{uid}", None)
                    
                    # Start the background task
                    asyncio.run_coroutine_threadsafe(
                        run_vault_release(
                            sender_bot=bot_instance, 
                            admin_chat_id=message.chat.id, 
                            source_id=source_cid, 
                            target_id=target_cid, 
                            interval=2.0, 
                            log_target_id=bot_id,
                            limit=count
                        ), 
                        loop
                    )
                except Exception as e:
                    bot_instance.reply_to(message, f"❌ Error: {e}. Please send a number.")

log_bot_manager = LogBotManager()

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

    # Boot all saved Log Bots
    try:
        log_bot_manager.start_all()
    except Exception as e:
        logger.error(f"Error booting log bots: {e}")

    asyncio.create_task(userbot_watchdog())
    threading.Thread(target=keep_alive_worker, daemon=True).start()

    # Try to start existing session
    try:
        ok, msg = await start_userbot()
        if ok: 
            logger.info("✅ Userbot started")
            # Cache warmer: fetch dialogs to avoid PeerIdInvalid
            logger.info("📡 Warming up peer cache...")
            async for _ in userbot.iter_dialogs(limit=50): pass
            logger.info("✅ Peer cache warmed")
    except Exception as e: 
        logger.error(f"Userbot startup error: {e}")
        if "AuthKeyDuplicatedError" in str(e):
            logger.critical("🚨 CRITICAL: Duplicate session detected. Please log out from other devices or delete session from DB.")

    # Start telebot polling with AUTO-RESTART
    def run_polling():
        while True:
            try:
                logger.info("🚀 Starting Admin Bot polling...")
                bot.delete_webhook(drop_pending_updates=True)
                # Reduced timeout and conflict handling
                bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
            except Exception as e:
                if "Conflict" in str(e):
                    logger.warning("⚠️ Main Admin Bot conflict. Retrying in 20s...")
                    time.sleep(20)
                else:
                    logger.error(f"❌ Polling crashed: {e}. Restarting in 30s...")
                    time.sleep(30)
    
    polling_thread = threading.Thread(target=run_polling, daemon=True)
    polling_thread.start()
    logger.info("✨ Admin bot monitor started")
    
    if userbot:
        try:
            await userbot.run_until_disconnected()
        except Exception as e:
            logger.error(f"Userbot disconnected with error: {e}")
            if "AuthKeyDuplicatedError" in str(e):
                logger.critical("🚨 CRITICAL: Duplicate session detected. Stopping Userbot loop.")
            # Keep the main thread alive so background bots keep working
            while True:
                await asyncio.sleep(3600)
    else:
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    loop.run_until_complete(main())
