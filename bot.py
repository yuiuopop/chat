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

def send_monitor_log(text):
    """Sends background activity directly to the Admin."""
    try:
        ts = datetime.now().strftime('%H:%M:%S')
        bot.send_message(ADMIN_ID, f"🔩 **SYSTEM LOG** [{ts}]\n`{text}`", parse_mode="Markdown")
    except: pass

# Global State Dictionaries
login_data = {}    # { user_id: { state_data } }
admin_states = {}  # { user_id: "current_state" }
running_tasks = {} # { task_key: bool }
topic_creation_lock = asyncio.Lock()

# -----------------------------
# DB (SQLite/PostgreSQL)
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
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
    if DATABASE_URL and USING_POSTGRES:
        return "%s"
    return "?"


def init_db():
    try:
        with db_conn() as conn:
            c = conn.cursor()
            if DATABASE_URL:
                # Postgres
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
                        pair_id INTEGER,
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
                        UNIQUE(pair_id, source_chat_id, source_message_id)
                    )
                """)
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
                        source_topic_name TEXT DEFAULT 'General',
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(bot_id, source_chat_id, source_msg_id)
                    )
                """)
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
                        pair_id INTEGER,
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
                        UNIQUE(pair_id, source_chat_id, source_message_id)
                    )
                """)
                c.execute("""
                    CREATE TABLE IF NOT EXISTS log_targets (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        target_id BIGINT UNIQUE,
                        target_type TEXT,
                        target_name TEXT,
                        bot_token TEXT
                    )
                """)
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
                        source_topic_name TEXT DEFAULT 'General',
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(bot_id, source_chat_id, source_msg_id)
                    )
                """)
                c.execute("""
                    CREATE TABLE IF NOT EXISTS banned_users (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id BIGINT UNIQUE,
                        username TEXT UNIQUE,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)

        # --- UNIVERSAL ROBUST MIGRATIONS ---
        with db_conn() as conn:
            c = conn.cursor()
            migrations = [
                ("message_mappings", "pair_id", "BIGINT"),
                ("log_media", "source_reply_to", "BIGINT"),
                ("log_media", "source_topic_name", "TEXT DEFAULT 'General'"),
                ("log_media", "log_msg_id", "BIGINT"),
                ("collected_media", "pair_id", "INTEGER"),
                ("target_pairs", "cf", "TEXT DEFAULT 'everything'"),
                ("log_targets", "bot_token", "TEXT")
            ]
            for table, col, col_type in migrations:
                try:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                    logger.info(f"✅ Migration: Added {col} to {table}")
                except:
                    pass
        logger.info("DB initialized and migrated.")
    except Exception as e:
        logger.error(f"❌ DATABASE INIT ERROR: {e}")

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

def save_message_mapping(s_chat, s_msg, t_chat, t_msg, pair_id=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        try:
            if DATABASE_URL:
                c.execute(
                    """
                    INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id, pair_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(source_chat_id, source_msg_id, target_chat_id) 
                    DO UPDATE SET target_msg_id = EXCLUDED.target_msg_id, pair_id = EXCLUDED.pair_id
                    """,
                    (s_chat, s_msg, t_chat, t_msg, pair_id)
                )
            else:
                c.execute(
                    """
                    INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id, pair_id)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(source_chat_id, source_msg_id, target_chat_id) 
                    DO UPDATE SET target_msg_id = excluded.target_msg_id, pair_id = excluded.pair_id
                    """,
                    (s_chat, s_msg, t_chat, t_msg, pair_id)
                )
        except Exception as e:
            # If the column still isn't there, fall back to the old way so it doesn't crash
            if "pair_id" in str(e):
                logger.warning(f"⚠️ save_message_mapping: Falling back due to missing pair_id column: {e}")
                c.execute(f"INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id) VALUES ({p}, {p}, {p}, {p})", 
                          (s_chat, s_msg, t_chat, t_msg))
            else:
                raise e

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
            SELECT m.source_msg_id, m.file_id, m.media_type, m.caption, m.log_msg_id, m.bot_id, m.source_topic_name, m.source_reply_to
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
        running_tasks[task_key] = False
        return True
    return False

async def run_vault_release(sender_bot, admin_chat_id, source_id, target_id, interval=2.5, limit=None, log_target_id=None, target_topic_id=None):
    """Releases vaulted media using the Userbot for forwarding with robust rate limiting."""
    task_key = f"vault_rel_{source_id}_{target_id}"
    if task_key in running_tasks:
        sender_bot.send_message(admin_chat_id, "⚠️ Task already running!")
        return
    
    running_tasks[task_key] = True
    await ensure_userbot()

    try:
        items = get_vaulted_media_for_source(source_id, bot_id=log_target_id, limit=limit)
        if not items:
            sender_bot.send_message(admin_chat_id, "❌ No vaulted items found.")
            return

        total = len(items)
        success, failed = 0, 0
        status_msg = sender_bot.send_message(admin_chat_id, f"🚀 **Initializing Transfer...**\nItems: `{total}`")

        # Robust ID normalization
        def normalize_tid(x):
            raw = str(x)
            if not raw.startswith("-") and len(raw) > 8:
                return int(f"-100{raw}")
            return int(raw)

        final_tid = normalize_tid(target_id)
        resolved_topics_cache = {}

        for i, item in enumerate(items):
            if not running_tasks.get(task_key):
                sender_bot.send_message(admin_chat_id, "🛑 **Release Stopped** by user.")
                break
            
            smid, file_id, m_type, caption, l_mid, b_id, src_topic_name, s_rid = item
            
            # Resolve Topic
            thread_id = target_topic_id
            if not thread_id and src_topic_name:
                if src_topic_name in resolved_topics_cache:
                    thread_id = resolved_topics_cache[src_topic_name]
                else:
                    thread_id = await resolve_target_topic_id(userbot, target_id, source_id, src_topic_name)
                    resolved_topics_cache[src_topic_name] = thread_id

            # Reconstruct Reply
            final_reply_to = thread_id
            if s_rid and int(s_rid) > 0:
                mapped_target_id = get_message_mapping(source_id, s_rid, target_id)
                if mapped_target_id:
                    final_reply_to = int(mapped_target_id)

            try:
                m_type = m_type.lower()
                sent_item = None
                
                if "photo" in m_type: 
                    sent_item = sender_bot.send_photo(final_tid, file_id, caption=caption, message_thread_id=final_reply_to)
                elif "video" in m_type: 
                    sent_item = sender_bot.send_video(final_tid, file_id, caption=caption, message_thread_id=final_reply_to)
                elif "audio" in m_type: 
                    sent_item = sender_bot.send_audio(final_tid, file_id, caption=caption, message_thread_id=final_reply_to)
                elif "animation" in m_type: 
                    sent_item = sender_bot.send_animation(final_tid, file_id, caption=caption, message_thread_id=final_reply_to)
                elif "sticker" in m_type: 
                    sent_item = sender_bot.send_sticker(final_tid, file_id, message_thread_id=final_reply_to)
                else: 
                    sent_item = sender_bot.send_document(final_tid, file_id, caption=caption, message_thread_id=final_reply_to)
                
                if sent_item:
                    save_message_mapping(source_id, smid, target_id, sent_item.message_id, pair_id=None)
                    success += 1
                else:
                    failed += 1

            except errors.FloodWaitError as e:
                logger.warning(f"⚠️ Rate limited. Waiting {e.seconds} seconds.")
                await asyncio.sleep(e.seconds)
                # Note: We skip the item here to prevent endless loops, but could retry
            except Exception as e:
                if "message thread not found" in str(e).lower():
                    logger.error("❌ Topic ID invalid. Resetting cache.")
                    resolved_topics_cache.pop(src_topic_name, None)
                logger.error(f"❌ LOGBOT SEND ERROR: {e}")
                failed += 1

            if (i + 1) % 5 == 0 or (i + 1) == total:
                try: 
                    sender_bot.edit_message_text(
                        f"📊 **Progress:** `{i+1}/{total}`\n✅ Success: `{success}`\n❌ Failed: `{failed}`", 
                        admin_chat_id, status_msg.message_id
                    )
                except: pass
            
            # DYNAMIC INTERVAL: Slow down if success rate is high to prevent flood
            current_sleep = interval if success % 10 != 0 else interval * 1.5
            await asyncio.sleep(current_sleep)

        sender_bot.send_message(admin_chat_id, f"✅ **Vault Release Complete!**\nSuccess: `{success}`\nFailed: `{failed}`")
    except Exception as e:
        logger.error(f"Vault Engine Error: {e}")
        sender_bot.send_message(admin_chat_id, f"❌ Release Crashed: {e}")
    finally:
        running_tasks.pop(task_key, None)

def save_logged_media(bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, source_topic_name="General", source_reply_to=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if DATABASE_URL:
            c.execute(
                """INSERT INTO log_media (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, source_topic_name, source_reply_to) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
                   ON CONFLICT(bot_id, source_chat_id, source_msg_id) DO UPDATE SET 
                   log_msg_id = EXCLUDED.log_msg_id, file_id = EXCLUDED.file_id, 
                   media_type = EXCLUDED.media_type, caption = EXCLUDED.caption,
                   source_topic_name = EXCLUDED.source_topic_name,
                   source_reply_to = EXCLUDED.source_reply_to""",
                (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, source_topic_name, source_reply_to)
            )
        else:
            c.execute(
                """INSERT INTO log_media (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, source_topic_name, source_reply_to) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(bot_id, source_chat_id, source_msg_id) DO UPDATE SET 
                   log_msg_id = excluded.log_msg_id, file_id = excluded.file_id, 
                   media_type = excluded.media_type, caption = excluded.caption,
                   source_topic_name = excluded.source_topic_name,
                   source_reply_to = excluded.source_reply_to""",
                (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, source_topic_name, source_reply_to)
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
        c.execute(f"SELECT source_chat_id, source_msg_id, file_id, media_type, caption, source_reply_to FROM log_media WHERE bot_id = {p} ORDER BY timestamp DESC LIMIT {p}", (bot_id, limit))
        return c.fetchall()

# -----------------------------
# Global State
# -----------------------------
bot = telebot.TeleBot(BOT_TOKEN)
userbot = None

admin_states = {}
login_data = {} # Temporary storage for login steps
running_tasks = {} # Track long-running tasks for cancellation: { "hist_1": True, "coll_1": True }

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

    markup.add(InlineKeyboardButton("🧹 Clear Pair Map", callback_data=f"pair_clear_map_{pair_id}"))
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
        try:
            logger.info(f"MIRROR: Creating new topic '{topic_title}' in {t_chat_id} (Icon: {icon_emoji_id})")
            created = await client(functions.channels.CreateForumTopicRequest(
                channel=t_chat_id,
                title=topic_title,
                icon_emoji_id=int(icon_emoji_id) if icon_emoji_id else None
            ))
            
            # SAFE WAY to get the ID:
            final_id = None
            for update in created.updates:
                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                    final_id = update.message.id
                    break
            
            if not final_id and created.updates:
                final_id = created.updates[0].id

            if final_id:
                if t_chat_id not in topic_cache:
                    topic_cache[t_chat_id] = {}
                topic_cache[t_chat_id][title_key] = final_id
        except Exception as e:
            logger.error(f"Failed to create topic in get_or_create: {e}")
            final_id = None
        
        if not final_id:
            # Fallback: re-fetch
            await asyncio.sleep(1)
            res_after = await client(functions.channels.GetForumTopicsRequest(
                channel=t_chat_id,
                offset_date=0, offset_id=0, offset_topic=0, limit=50
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

async def resolve_source_topic_name(client, chat_id, message):
    """Resolves the title of the forum topic the message belongs to."""
    if not message.reply_to:
        logger.debug(f"MIRROR: Message has no reply_to header in {chat_id}")
        return "General"
    
    top_id = getattr(message.reply_to, 'reply_to_top_id', None) or message.reply_to.reply_to_msg_id
    logger.debug(f"MIRROR: Resolved top_id {top_id} for chat {chat_id}")
    
    if not top_id:
        return "General"
        
    try:
        # Check if the chat is a forum
        entity = await resolve_target_id(client, chat_id)
        is_forum = getattr(entity, 'forum', False)
        logger.debug(f"MIRROR: Chat {chat_id} is_forum: {is_forum}")
        
        if not is_forum:
            return "General"

        # Fetch topics and match ID
        logger.debug(f"MIRROR: Fetching forum topics for {chat_id}...")
        res = await client(functions.channels.GetForumTopicsRequest(
            channel=entity, offset_date=0, offset_id=0, offset_topic=0, limit=100
        ))
        for t in res.topics:
            if t.id == top_id:
                logger.info(f"MIRROR: Successfully resolved topic title: '{t.title}'")
                return t.title
        
        logger.warning(f"MIRROR: Topic ID {top_id} not found in topic list for {chat_id}")
    except Exception as e:
        logger.error(f"MIRROR: Source Topic Resolution Failed for chat {chat_id}: {e}")
    
    logger.debug(f"MIRROR: Falling back to 'General' for chat {chat_id}")
    return "General"

async def resolve_target_topic_id(client, target_chat_id, source_chat_id, source_msg_topic_name):
    try:
        # Normalize name
        t_name = (source_msg_topic_name or "General").lower().strip()
        
        # 1. Immediate Cache Check
        if target_chat_id in topic_cache and t_name in topic_cache[target_chat_id]:
            return topic_cache[target_chat_id][t_name]

        # 2. Use Lock to prevent duplicate creation attempts
        async with topic_creation_lock:
            # Check cache again inside lock (Double-Checked Locking)
            if target_chat_id in topic_cache and t_name in topic_cache[target_chat_id]:
                return topic_cache[target_chat_id][t_name]

            # Use our aggressive resolver instead of raw get_entity
            target_entity = await resolve_target_id(client, target_chat_id)
            if not getattr(target_entity, 'forum', False):
                return None
            
            # Fetch existing topics to see if another process just created it
            topics = await client(functions.channels.GetForumTopicsRequest(
                channel=target_entity, offset_date=0, offset_id=0, offset_topic=0, limit=100
            ))
            
            if target_chat_id not in topic_cache:
                topic_cache[target_chat_id] = {}

            for t in topics.topics:
                topic_cache[target_chat_id][t.title.lower().strip()] = t.id
                if t.title.lower().strip() == t_name:
                    return t.id

            # 3. Create if truly missing
            logger.info(f"✨ Creating new topic: {source_msg_topic_name}")
            created = await client(functions.channels.CreateForumTopicRequest(
                channel=target_entity,
                title=source_msg_topic_name
            ))
            
            new_id = None
            for update in created.updates:
                if hasattr(update, 'message') and hasattr(update.message, 'id'):
                    new_id = update.message.id
                    break
            
            if not new_id and created.updates:
                new_id = created.updates[0].id

            if new_id:
                topic_cache[target_chat_id][t_name] = new_id
                return new_id
            
            return None

    except Exception as e:
        logger.error(f"Topic Resolver Error: {e}")
        return None

async def vault_media(client, message, source_chat_id, log_chat_id, source_msg_id, t_name):
    """Helper to forward to vault and save the permanent File ID"""
    try:
        # Resolve the actual topic name from the source message
        src_topic_name = await resolve_source_topic_name(client, source_chat_id, message)
        
        # Resolve the reply-to ID (if any)
        src_reply_id = message.reply_to.reply_to_msg_id if message.reply_to else 0

        target_peer = await resolve_target_id(client, log_chat_id)
        
        # We find or create a topic IN THE LOG BOT'S VAULT matches the source topic
        vault_topic_id = await resolve_target_topic_id(client, log_chat_id, source_chat_id, src_topic_name)

        # We embed RID (Reply ID) and TOPIC into the metadata string for the Log Bot to parse
        metadata = f"SID: {source_chat_id} | MID: {source_msg_id} | RID: {src_reply_id} | TOPIC: {src_topic_name}\n"
        caption_text = metadata + (message.message or "")
        
        vaulted = None
        try:
            vaulted = await client.send_message(
                entity=target_peer,
                file=message.media if message.media else None,
                message=caption_text,
                reply_to=vault_topic_id # Forces the backup into the right topic!
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
                    message=caption_text,
                    reply_to=vault_topic_id
                )
                if path and os.path.exists(path): os.remove(path)
            else:
                raise e
            
        if vaulted:
            logger.info(f"✅ VAULT: Message {source_msg_id} logged successfully to @{t_name}")
            # Save mapping immediately for internal release engine tracking
            save_logged_media(
                bot_id=int(log_chat_id),
                log_msg_id=int(vaulted.id),
                source_chat_id=int(source_chat_id),
                source_msg_id=int(source_msg_id),
                file_id=None, # Will be updated by Log Bot's listener
                media_type=type(message.media).__name__ if message.media else "text",
                caption=message.message or "",
                source_topic_name=src_topic_name,
                source_reply_to=src_reply_id
            )
    except Exception as e:
        logger.error(f"VAULT ERROR for @{t_name}: {e}")

def setup_automation_handlers(client: TelegramClient):
    logger.info("⚙️  Setting up Automation Handlers...")
    @client.on(events.NewMessage)
    async def auto_handler(event):
        try:
            m = event.message
            if not m: return
            
            logger.debug(f"HEARTBEAT: New message in {m.chat_id}")

            # --- BAN LIST CHECK ---
            sender_id = m.sender_id
            sender_username = getattr(m.sender, 'username', None)
            if is_user_banned(sender_id, sender_username):
                logger.info(f"🚫 BLOCKED: Ignored message from banned user {sender_id} (@{sender_username})")
                return

            pairs = get_target_pairs()
            for pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf in pairs:
                
                # Robust ID normalization
                def normalize(x):
                    return int(str(x).replace("-100", ""))

                if normalize(sid) == normalize(m.chat_id):
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
                            p = get_placeholder()
                            if DATABASE_URL:
                                c.execute(
                                    f"INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES ({p}, {p}, {p}, {p}, {p}) ON CONFLICT DO NOTHING",
                                    (pid, sid, m.id, m_type, m.message or "")
                                )
                            else:
                                c.execute(
                                    f"INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES ({p}, {p}, {p}, {p}, {p})",
                                    (pid, sid, m.id, m_type, m.message or "")
                                )
                        
                        # Instantly send to log bots in background
                        asyncio.create_task(forward_to_log_bots(client, m, sid, m.id))

                    if not is_live: 
                        continue # Check next pair instead of breaking

                    # --- ALBUM / SINGLE MESSAGE LOGIC ---
                    if m.grouped_id:
                        album_key = f"{pid}_{m.grouped_id}"
                        if album_key not in album_cache:
                            album_cache[album_key] = [m]
                            # Wait for all parts
                            async def delayed_send(key, t_id, mir_toggle, s_id, def_topic, pair_idx):
                                await asyncio.sleep(5.0) 
                                messages = album_cache.pop(key, [])
                                if messages:
                                    # SAFETY: Ensure no duplicate message objects in the same album
                                    unique_msgs = {msg.id: msg for msg in messages}.values()
                                    sorted_msgs = sorted(unique_msgs, key=lambda x: x.id)
                                    
                                    logger.info(f"MIRROR: Sending unique album ({len(sorted_msgs)} parts) for Pair {pair_idx}")
                                    await execute_perform_mirror(client, t_id, sorted_msgs, def_topic, mir_toggle, s_id, pair_id=pair_idx)
                            asyncio.create_task(delayed_send(album_key, tid, is_mir, sid, t_topic, pid))
                        else:
                            # Only add if this specific message ID isn't already in the list for this pair
                            if m.id not in [msg.id for msg in album_cache[album_key]]:
                                album_cache[album_key].append(m)
                    else:
                        logger.info(f"MIRROR: Sending single message from {sid} to {tid}")
                        await execute_perform_mirror(client, tid, [m], t_topic, is_mir, sid, pair_id=pid)
                    
                    # Removed break to allow multiple pairs for the same source chat.
                else:
                    logger.debug(f"AUTO_HANDLER: Chat ID {m.chat_id} did not match pair source {sid}")
        except Exception as e:
            logger.error(f"AUTO_HANDLER ERROR: {e}")

    async def execute_perform_mirror(client, tid, messages, default_t_topic, is_mir, sid, pair_id=None):
        try:
            if not messages: return
            first_msg = messages[0]
            
            # 1. Resolve Topic ID (Thread)
            final_topic_id = default_t_topic 
            if is_mir:
                src_topic_name = await resolve_source_topic_name(client, sid, first_msg)
                send_monitor_log(f"Incoming msg from Topic: '{src_topic_name}'")
                
                resolved_id = await resolve_target_topic_id(client, tid, sid, src_topic_name)
                final_topic_id = resolved_id if resolved_id else default_t_topic
                send_monitor_log(f"Target Resolved to ID: {final_topic_id}")

            # 2. RESOLVE REPLY (Recursive Reply Mapping)
            reply_to_id = final_topic_id # Default to the topic anchor
            
            if first_msg.reply_to_msg_id:
                # Check our database: "What was the Target ID for Source Message X?"
                mapped_target_id = get_message_mapping(sid, first_msg.reply_to_msg_id, tid)
                if mapped_target_id:
                    reply_to_id = int(mapped_target_id)
                    logger.info(f"🔗 REPLY MATCH: Source {first_msg.reply_to_msg_id} -> Target {reply_to_id}")
                    send_monitor_log(f"🔗 Reply Linked to Target ID: {reply_to_id}")
                else:
                    logger.debug("🔗 REPLY: No mapping found, defaulting to topic root.")

            # 3. Construct Send Parameters
            album_text = next((msg.message for msg in messages if msg.message), "")
            
            # 4. SEND
            sent = await client.send_message(
                entity=int(tid),
                message=album_text,
                file=[m.media for m in messages] if len(messages) > 1 else first_msg.media,
                reply_to=reply_to_id # This now points to the specific message if found
            )

            if sent:
                # Save the mapping of the message we JUST sent
                sent_id = sent[0].id if isinstance(sent, list) else sent.id
                save_message_mapping(sid, first_msg.id, tid, sent_id, pair_id=pair_id)
                logger.info(f"✅ MIRROR: Sent successfully to {tid}")
                send_monitor_log(f"✅ Successfully Mirrored to {tid}")

        except errors.FloodWaitError as e:
            logger.warning(f"⚠️ Flood wait: {e.seconds}s. Waiting...")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            if "message thread not found" in str(e).lower():
                logger.error("❌ Target Topic not found. Clearing cache.")
                topic_cache.pop(int(tid), None)
            logger.error(f"❌ MIRROR FATAL ERROR: {e}")

    async def execute_fallback_mirror(client, sid, tid, messages, first_msg, album_text, reply_header, pair_id=None):
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
                    save_message_mapping(sid, first_msg.id, tid, first_id, pair_id=pair_id)
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
            p = get_placeholder()
            c.execute(f"SELECT file_id, media_type FROM media_logs WHERE source_message_id = {p} LIMIT 1", (smid,))
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

@bot.message_handler(commands=['clear_map'])
def cmd_clear_message_mappings(message):
    if message.from_user.id != ADMIN_ID: return
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔥 Confirm Wipe", callback_data="do_clear_mappings"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="dash_main"))
    
    bot.send_message(
        message.chat.id, 
        "⚠️ **Database Cleanup**\n\nThis will delete all stored message links. Reply bubbles will no longer work for old messages. Continue?", 
        reply_markup=markup, 
        parse_mode="Markdown"
    )

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
        
        s_chat = await resolve_target_id(userbot, sid)
        t_chat = await resolve_target_id(userbot, tid)
        
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

@bot.callback_query_handler(func=lambda call: call.data == "do_clear_mappings")
def handle_clear_mappings_callback(call):
    if call.from_user.id != ADMIN_ID: return
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM message_mappings")
            # Optional: c.execute("DELETE FROM topic_mappings")
            
        bot.answer_callback_query(call.id, "✅ Database Wiped")
        bot.edit_message_text("✅ **All message mappings have been cleared.**\nYour database is now clean.", call.message.chat.id, call.message.message_id)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"❌ Cleanup failed: {e}")

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
                    full_chat = await resolve_target_id(userbot, sid)
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
                    full_chat = await resolve_target_id(userbot, tid)
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

    elif data.startswith("pair_clear_map_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "Cleaning pair memory...")
        
        try:
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                # Only delete mappings associated with this specific pair
                c.execute(f"DELETE FROM message_mappings WHERE pair_id = {p}", (pid,))
                # Optionally clear topic mappings for this pair too
                c.execute(f"DELETE FROM topic_mappings WHERE target_chat_id = (SELECT target_id FROM target_pairs WHERE id = {p})", (pid,))
            
            bot.send_message(call.message.chat.id, f"✅ **Pair {pid} Memory Cleared!**\nReply bubbles for old messages in this pair will no longer be mapped.")
            show_pair_view(call.message.chat.id, call.message.message_id, pid)
        except Exception as e:
            bot.send_message(call.message.chat.id, f"❌ Cleanup failed: {e}")

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
                chat = await resolve_target_id(userbot, chat_id)
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
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf = pair
    
    collected = 0
    scanned = 0
    status_msg = bot.send_message(admin_chat_id, f"⏳ **History Scrape: `{s_title}`**\n\nPreserving reply bubbles...")

    try:
        # Resolve entities
        source_chat = await resolve_target_id(userbot, sid)
        target_chat = await resolve_target_id(userbot, tid)

        # Topic settings
        src_topic = int(s_topic) if s_topic and str(s_topic) != "0" else None
        tgt_topic_id = int(t_topic) if t_topic and str(t_topic) != "0" else None

        # CRITICAL: We use reverse=True to send oldest messages first.
        # This ensures the 'parent' of a reply is already in the target group.
        async for m in userbot.iter_messages(source_chat, limit=limit, offset_date=end_date, reverse=True, reply_to=src_topic):
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, "🛑 Scrape stopped by user.")
                break
            
            scanned += 1
            if start_date and m.date < start_date: continue

            # --- REPLY RESOLUTION ---
            # Default to the Topic Root
            final_reply_to = tgt_topic_id 

            if m.reply_to_msg_id:
                # Ask DB: "Did I send the message this guy is replying to?"
                mapped_parent_id = get_message_mapping(sid, m.reply_to_msg_id, tid)
                if mapped_parent_id:
                    final_reply_to = int(mapped_parent_id)

            # --- SEND CONTENT ---
            try:
                # Use send_message instead of forward to keep mapping control
                sent = await userbot.send_message(
                    entity=target_chat,
                    message=m.message or "",
                    file=m.media,
                    reply_to=final_reply_to
                )

                if sent:
                    # IMPORTANT: Save the map so future history messages can reply to this one
                    save_message_mapping(sid, m.id, tid, sent.id, pair_id=pid)
                    collected += 1
                    
                    # Log bot vaulting (optional)
                    if is_mon and m.media:
                        asyncio.create_task(forward_to_log_bots(userbot, m, sid, m.id))

            except errors.FloodWaitError as fwe:
                await asyncio.sleep(fwe.seconds)
            except Exception as e:
                logger.error(f"History Send Error: {e}")

            if scanned % 10 == 0:
                try: bot.edit_message_text(f"📜 **History Scrape: `{s_title}`**\n🔄 Scanned: `{scanned}`\n✅ Re-sent: `{collected}`", admin_chat_id, status_msg.message_id)
                except: pass

        bot.send_message(admin_chat_id, f"✅ **History Sync Complete!**\nGroup: `{s_title}`\nMessages: `{collected}`\nReply chains preserved.")

    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Scrape Error: {e}")
    finally:
        running_tasks.pop(task_key, None)

async def resolve_target_id(client: TelegramClient, target_ref):
    """Aggressively resolves chat entities to avoid PeerIdInvalid errors."""
    try:
        # 1. Try resolving as a direct integer or username (fastest if in cache)
        ref = int(target_ref) if str(target_ref).replace("-", "").isdigit() else target_ref
        return await client.get_entity(ref)
    except Exception:
        logger.info(f"🔍 Peer {target_ref} not in local cache. Refreshing dialogs...")
        try:
            # 2. Search through the last 200 dialogs to discover the entity and its access hash
            # This is the most reliable way to refresh the session's knowledge of a peer
            async for dialog in client.iter_dialogs(limit=200):
                if str(dialog.id) == str(target_ref) or str(getattr(dialog.entity, 'id', None)) == str(target_ref):
                    logger.info(f"✅ Found peer {target_ref} in recent dialogs.")
                    return dialog.entity
            
            # 3. Last ditch effort: refresh knowledge via username or string if it failed
            return await client.get_entity(target_ref)
        except Exception as e:
            logger.error(f"❌ Failed to resolve peer {target_ref}: {e}")
            raise e

async def run_collection(admin_chat_id, pair_id, limit=300):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return
        
    task_key = f"coll_{pair_id}"
    running_tasks[task_key] = True
    
    row = get_target_pair(pair_id)
    if not row: return
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf = row
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
                    p = get_placeholder()
                    if DATABASE_URL:
                        c.execute(f"INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES ({p}, {p}, {p}, {p}, {p}) ON CONFLICT DO NOTHING", (pair_id, sid, m.id, m_type, m.message or ""))
                    else:
                        c.execute(f"INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption) VALUES ({p}, {p}, {p}, {p}, {p})", (pair_id, sid, m.id, m_type, m.message or ""))
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
                    p = get_placeholder()
                    c.execute(f"SELECT file_id, media_type, caption FROM log_media WHERE log_msg_id = {p} AND bot_id = {p}", (fetch_id, bot_id))
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
                    p = get_placeholder()
                    c.execute(f"SELECT file_id, media_type, caption, log_msg_id FROM log_media WHERE bot_id = {p} ORDER BY timestamp DESC LIMIT {p}", (bot_id, count))
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
                p = get_placeholder()
                c.execute(f"""
                    SELECT m.source_chat_id, p.source_title, COUNT(m.id)
                    FROM log_media m
                    LEFT JOIN target_pairs p ON m.source_chat_id = p.source_id
                    WHERE m.bot_id = {p}
                    GROUP BY m.source_chat_id, p.source_title
                """, (bot_id,))
                groups = c.fetchall()
            if not groups:
                bot_instance.send_message(message.chat.id, "📭 No media found.")
                return
            markup = InlineKeyboardMarkup(row_width=1)
            for sid, title, cnt in groups:
                if sid is None or sid == 0: continue
                markup.add(InlineKeyboardButton(f"📁 {title or 'Direct'} - {cnt}", callback_data=f"v_group_stats_{sid}"))
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
                    p = get_placeholder()
                    c.execute(f"SELECT file_id, media_type, caption, log_msg_id FROM log_media WHERE source_chat_id = {p} AND bot_id = {p} ORDER BY timestamp DESC LIMIT {p}", (group_id, bot_id, count))
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
                
                # Use standard string formatting for the query to avoid "end of input" errors
                # Ensure sid is passed as a tuple (sid,)
                if USING_POSTGRES:
                    c.execute("SELECT source_title FROM target_pairs WHERE source_id = %s LIMIT 1", (sid,))
                else:
                    c.execute("SELECT source_title FROM target_pairs WHERE source_id = ? LIMIT 1", (sid,))
                
                res = c.fetchone()
                title = res[0] if res else "Unknown Group"
                
                # Fetch count for this specific bot
                c.execute(f"SELECT COUNT(*) FROM log_media WHERE source_chat_id = {p} AND bot_id = {p}", (sid, bot_id))
                total = c.fetchone()[0]

            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🚀 Send batch to Group", callback_data=f"v_dump_start_{sid}"))
            markup.add(InlineKeyboardButton("🔙 Back to List", callback_data="lb_vault_main"))

            msg = (f"📊 **Group Statistics**\n\n"
                   f"🏷 **Title:** `{title}`\n"
                   f"🆔 **ID:** `{sid}`\n"
                   f"📦 **Total Media:** `{total}`\n\n"
                   f"💡 Click the button below to send this media into a different group via this Log Bot.")
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

        @bot_instance.message_handler(content_types=['photo', 'video', 'document', 'audio', 'animation', 'sticker'])
        def handle_logging(message):
            try:
                m_type = "document"
                file_id = None
                original_caption = message.caption or ""
                
                if message.photo: m_type, file_id = "photo", message.photo[-1].file_id
                elif message.video: m_type, file_id = "video", message.video.file_id
                elif message.document: m_type, file_id = "document", message.document.file_id
                elif message.audio: m_type, file_id = "audio", message.audio.file_id
                elif message.animation: m_type, file_id = "animation", message.animation.file_id
                elif message.sticker: m_type, file_id = "sticker", message.sticker.file_id
                
                if not file_id: return

                if "SID:" in original_caption and "MID:" in original_caption:
                    try:
                        # Parsing: SID: ... | MID: ... | RID: ... | TOPIC: ...
                        parts = original_caption.split("|")
                        sid = int(parts[0].replace("SID:", "").strip())
                        mid = int(parts[1].split("\n")[0].replace("MID:", "").strip())
                        
                        rid = 0
                        t_name = "General"
                        if "RID:" in original_caption:
                            try: rid = int(original_caption.split("RID:")[1].split("|")[0].split("\n")[0].strip())
                            except: pass
                        if "TOPIC:" in original_caption:
                            t_name = original_caption.split("TOPIC:")[1].split("\n")[0].strip()

                        final_caption = original_caption.split("\n", 1)[1] if "\n" in original_caption else ""
                        
                        save_logged_media(
                            bot_id=bot_id, log_msg_id=message.message_id, 
                            source_chat_id=sid, source_msg_id=mid, 
                            file_id=file_id, media_type=m_type, caption=final_caption, 
                            source_topic_name=t_name, source_reply_to=rid
                        )
                        bot_instance.reply_to(message, f"✅ Vaulted: `{mid}` from `{sid}`\nTopic: `{t_name}` | Reply: `{rid}`")
                        return
                    except Exception as e:
                        logger.error(f"Metadata Parse Error: {e}")

                # Fallback for manual uploads
                save_logged_media(bot_id, message.message_id, 0, message.message_id, file_id, m_type, original_caption, "General")
                if message.from_user.id == ADMIN_ID:
                    bot_instance.reply_to(message, f"✅ **Saved to Vault (Manual)!**\n🆔 ID: `{message.message_id}`")
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
                    
                    async def handle_dest():
                        try:
                            entity = await resolve_target_id(userbot, tid)
                            if getattr(entity, 'forum', False):
                                markup = await get_topic_selection_markup(tid, "lb_vault_topic")
                                bot_instance.edit_message_text(f"🧵 **Forum Detected**\nSelect a topic in `{entity.title}`:", call.message.chat.id, call.message.message_id, reply_markup=markup)
                            else:
                                # Standard group
                                is_dump = "dump_sid" in login_data.get(uid, {})
                                if is_dump:
                                    login_data[uid]["dump_tid"] = tid
                                    login_data[uid]["dump_topic"] = None
                                    admin_states[f"lb_{bot_id}_{uid}"] = f"wait_dump_count_{tid}"
                                    bot_instance.edit_message_text(f"🔢 **How many items?**\nEnter count for group `{tid}`:", call.message.chat.id, call.message.message_id)
                                else:
                                    login_data[uid]["vault_target_id"] = tid
                                    login_data[uid]["vault_topic_id"] = None
                                    admin_states[f"lb_{bot_id}_{uid}"] = "awaiting_rel_interval"
                                    bot_instance.edit_message_text("⏳ **Release Interval**\nEnter time (seconds) between messages:", call.message.chat.id, call.message.message_id)
                        except Exception as e:
                            bot_instance.send_message(call.message.chat.id, f"❌ Error: {e}")
                    asyncio.run_coroutine_threadsafe(handle_dest(), loop)

            elif data.startswith("lb_vault_topic_"):
                bot_instance.answer_callback_query(call.id)
                payload = data.replace("lb_vault_topic_", "")
                tid_str, topic_id_str = payload.rsplit("_", 1)
                tid = int(tid_str)
                topic_id = int(topic_id_str)
                topic_val = topic_id if topic_id != 0 else None
                
                is_dump = "dump_sid" in login_data.get(uid, {})
                if is_dump:
                    login_data[uid]["dump_tid"] = tid
                    login_data[uid]["dump_topic"] = topic_val
                    admin_states[f"lb_{bot_id}_{uid}"] = f"wait_dump_count_{tid}"
                    bot_instance.edit_message_text(f"🔢 **Topic Set!**\nEnter count for topic `{topic_id}`:", call.message.chat.id, call.message.message_id)
                else:
                    login_data[uid]["vault_target_id"] = tid
                    login_data[uid]["vault_topic_id"] = topic_val
                    admin_states[f"lb_{bot_id}_{uid}"] = "awaiting_rel_interval"
                    bot_instance.edit_message_text(f"⏳ **Topic Set!**\nEnter release interval (seconds):", call.message.chat.id, call.message.message_id)
                    
            elif data == "lb_cancel":
                bot_instance.answer_callback_query(call.id)
                admin_states.pop(f"lb_{bot_id}_{uid}", None)
                cmd_start(call.message)

            elif data.startswith("lb_stop_rel_"):
                bot_instance.answer_callback_query(call.id, "🛑 Stopping...")
                task_key = data.replace("lb_stop_rel_", "")
                stop_task(task_key)

            elif data.startswith("lb_do_release_"):
                # lb_do_release_{sid}_{tid}_{interval}_{topic}
                bot_instance.answer_callback_query(call.id)
                parts = data.split("_")
                sid, tid = int(parts[3]), int(parts[4])
                interval = float(parts[5])
                topic_id = int(parts[6])
                
                # FIX: Convert 0 back to None for the engine
                topic_val = topic_id if topic_id != 0 else None
                
                bot_instance.edit_message_text(f"🚀 **Initializing Engine...**\nInterval: `{interval}s`", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
                asyncio.run_coroutine_threadsafe(run_vault_release(bot_instance, call.message.chat.id, sid, tid, interval=interval, target_topic_id=topic_val, log_target_id=bot_id), loop)

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
                    t_topic = login_data.get(uid, {}).get("vault_topic_id")
                    
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("🚀 Start Release", callback_data=f"lb_do_release_{sid}_{tid}_{interval}_{t_topic or 0}"))
                    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="lb_cancel"))
                    
                    bot_instance.send_message(message.chat.id, f"✅ **Interval Set: `{interval}s`**\nReady to release from `{sid}` to `{tid}`.", reply_markup=markup, parse_mode="Markdown")
                except:
                    bot_instance.reply_to(message, "⚠️ Invalid interval. Please send a number (e.g. `2.0`).")

            elif state.startswith("wait_dump_count_"):
                try:
                    target_cid = int(state.split("_")[-1])
                    count = int(text)
                    
                    user_session = login_data.get(uid)
                    if not user_session or "dump_sid" not in user_session:
                        bot_instance.send_message(message.chat.id, "❌ Session expired.")
                        return

                    source_cid = user_session["dump_sid"]
                    target_topic = user_session.get("dump_topic") 
                    
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
                            limit=count,
                            target_topic_id=target_topic
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
                # Ensure no old webhooks are active
                bot.remove_webhook()
                # Optimized polling parameters for stability on Render
                bot.infinity_polling(
                    skip_pending=True, 
                    timeout=60, 
                    long_polling_timeout=30,
                    logger_level=logging.ERROR 
                )
            except Exception as e:
                if "Conflict" in str(e):
                    logger.warning("⚠️ Conflict: Another instance is running. Waiting 30s...")
                    time.sleep(30)
                else:
                    logger.error(f"❌ Polling crashed: {e}. Restarting in 15s...")
                    time.sleep(15)
    
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
