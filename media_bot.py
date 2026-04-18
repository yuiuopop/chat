# =========================
# 📦 IMPORTS
# =========================

import os
import time
import threading
import queue
import json
import tempfile
import random
from contextlib import contextmanager
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
 # make sure to have this file for cross-instance forwarding
import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import execute_values
import telebot
from telebot.types import InputMediaPhoto, InputMediaVideo
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


# =========================
# ⚙ CONFIGURATION
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
FIRST_ADMIN_ID = os.getenv("ADMIN_ID") # replace with your Telegram ID for initial admin access


REQUIRED_MEDIA = 12
INACTIVITY_LIMIT = 6 * 60 * 60  # 6 hours
FORWARD_DELAY = float(os.getenv("FORWARD_DELAY", "0.01"))
SEND_MAX_WORKERS = int(os.getenv("SEND_MAX_WORKERS", "8"))
SEND_RETRIES = int(os.getenv("SEND_RETRIES", "2"))
RECEIVER_CACHE_TTL = int(os.getenv("RECEIVER_CACHE_TTL", "10"))
BROADCAST_QUEUE_SIZE = int(os.getenv("BROADCAST_QUEUE_SIZE", "2000"))
DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "1"))
DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "15"))
MESSAGE_MAP_MODE = os.getenv("MESSAGE_MAP_MODE", "full").strip().lower()
MAP_RETENTION_DAYS = int(os.getenv("MAP_RETENTION_DAYS", "2"))
MAP_CLEANUP_INTERVAL_SECONDS = int(os.getenv("MAP_CLEANUP_INTERVAL_SECONDS", "300"))
MAP_INSERT_BATCH_SIZE = int(os.getenv("MAP_INSERT_BATCH_SIZE", "1000"))
MAP_DELETE_BATCH_SIZE = int(os.getenv("MAP_DELETE_BATCH_SIZE", "1000"))
MAX_WARNINGS = int(os.getenv("MAX_WARNINGS", "3"))
WARNING_COOLDOWN = int(os.getenv("WARNING_COOLDOWN", "30"))
WARNING_EXPIRY = int(os.getenv("WARNING_EXPIRY", "86400"))
FORCE_JOIN_CACHE_TTL = int(os.getenv("FORCE_JOIN_CACHE_TTL", "120"))
JOINED_STATUSES = ("member", "administrator", "creator", "owner", "restricted")
FORCE_JOIN_REMINDER_COOLDOWN = int(os.getenv("FORCE_JOIN_REMINDER_COOLDOWN", "21600"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL")

bot = telebot.TeleBot(BOT_TOKEN)
broadcast_queue = queue.Queue(maxsize=BROADCAST_QUEUE_SIZE)
media_groups = defaultdict(list)
album_timers = {}
activation_buffer = defaultdict(int)
activation_timer = {}
activation_lock = threading.Lock()
db_pool = None
receiver_cache_lock = threading.Lock()
receiver_cache = []
receiver_cache_at = 0.0
pending_recovery_import = set()
pending_fw_add = set()
pending_fw_remove = set()
pending_vip_add = set()
pending_vip_remove = set()
pending_fw_msg = set()
pending_admin_broadcast = set()
pending_admin_setcaption = set()
pending_admin_setwelcome = set()
pending_admin_addforward = set()
force_join_cache_lock = threading.Lock()
force_join_cache = {}
force_join_reminder_lock = threading.Lock()
force_join_reminder_at = {}
last_fw_msg_lock = threading.Lock()
last_fw_msg = {}

# =========================
# 🗄 DATABASE CONNECTION
# =========================

def init_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = pg_pool.SimpleConnectionPool(
            DB_POOL_MIN_CONN,
            DB_POOL_MAX_CONN,
            dsn=DATABASE_URL,
        )


@contextmanager
def get_connection():
    if db_pool is not None:
        conn = db_pool.getconn()
        use_pool = True
    else:
        conn = psycopg2.connect(DATABASE_URL)
        use_pool = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        if use_pool:
            db_pool.putconn(conn)
        else:
            conn.close()
# =========================
# 🧱 DATABASE INITIALIZATION
# =========================

def init_db():

    with get_connection() as conn:
        with conn.cursor() as c:

            # =========================
            # USERS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT UNIQUE,
                    banned BOOLEAN DEFAULT FALSE,
                    auto_banned BOOLEAN DEFAULT FALSE,
                    whitelisted BOOLEAN DEFAULT FALSE,
                    activation_media_count INTEGER DEFAULT 0,
                    total_media_sent INTEGER DEFAULT 0,
                    last_activation_time BIGINT
                )
            """)
            c.execute("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS referred_by BIGINT
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS recovery_users (
                    username TEXT PRIMARY KEY,
                    banned BOOLEAN DEFAULT FALSE
                )
            """)
            c.execute("""
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS referred_by BIGINT
            """)

            # =========================
            # ADMINS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY
                )
            """)

            # =========================
            # MESSAGE MAP TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS message_map (
                    bot_message_id BIGINT,
                    original_user_id BIGINT,
                    original_message_id BIGINT,
                    receiver_id BIGINT,
                    created_at BIGINT
                )
            """)
            c.execute("""
                ALTER TABLE message_map
                ADD COLUMN IF NOT EXISTS original_message_id BIGINT
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_bot_msg ON message_map(bot_message_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_original_user ON message_map(original_user_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_original_msg ON message_map(original_message_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_original_pair ON message_map(original_user_id, original_message_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON message_map(created_at)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_receiver_bot_msg ON message_map(receiver_id, bot_message_id)")
            c.execute("CREATE INDEX IF NOT EXISTS idx_original_pair_receiver ON message_map(original_user_id, original_message_id, receiver_id)")

            # =========================
            # ⚠ USER WARNINGS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS user_warnings (
                    user_id BIGINT PRIMARY KEY,
                    warnings INTEGER DEFAULT 0,
                    last_warning_time BIGINT
                )
            """)
            c.execute("""
                ALTER TABLE user_warnings
                ADD COLUMN IF NOT EXISTS last_reason TEXT
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS force_join_channels (
                    chat_id TEXT PRIMARY KEY
                )
            """)
            c.execute("""
                ALTER TABLE force_join_channels
                ADD COLUMN IF NOT EXISTS button_name TEXT
            """)
            c.execute("""
                ALTER TABLE force_join_channels
                ADD COLUMN IF NOT EXISTS invite_link TEXT
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS vip_users (
                    user_id BIGINT PRIMARY KEY
                )
            """)

            # =========================
            # BANNED WORDS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS banned_words (
                    word TEXT PRIMARY KEY
                )
            """)

            # =========================
            # SETTINGS TABLE
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS forward_targets (
                    chat_id BIGINT PRIMARY KEY
                )
            """)

            # Default Join Setting
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('join_open', 'true')
                ON CONFLICT DO NOTHING
            """)
            # =========================
            # 📦 DUPLICATE TRACKING
            # =========================
            c.execute("""
                CREATE TABLE IF NOT EXISTS media_duplicates (
                    file_id TEXT PRIMARY KEY,
                    first_sender BIGINT,
                    duplicate_count INTEGER DEFAULT 0
                )
            """)
            # Default Welcome Message
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('welcome_message', '👋 Welcome!\n\nPlease drop your username:')
                ON CONFLICT DO NOTHING
            """)

            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('duplicate_filter', 'false')
                ON CONFLICT DO NOTHING
            """)
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('maintenance_mode', 'false')
                ON CONFLICT DO NOTHING
            """)
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('force_join_enabled', 'false')
                ON CONFLICT DO NOTHING
            """)
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('force_join_chat_id', '')
                ON CONFLICT DO NOTHING
            """)
            c.execute("""
                INSERT INTO settings(key, value)
                VALUES('force_join_message', '🚫 Please join our channel to use the bot.')
                ON CONFLICT DO NOTHING
            """)
            # =========================
            # FIRST ADMIN INIT
            # =========================

            first_admin = FIRST_ADMIN_ID

            if first_admin:
                try:
                    first_admin = int(first_admin)

                    c.execute("""
                        INSERT INTO admins(user_id)
                        VALUES(%s)
                        ON CONFLICT DO NOTHING
                    """, (first_admin,))

                    print("First admin ensured.")

                except Exception as e:
                    print("Admin init error:", e)

# =========================
# 👤 USER EXISTENCE
# =========================
def delete_message_globally(receiver_id, bot_message_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT original_user_id, original_message_id
                FROM message_map
                WHERE receiver_id=%s AND bot_message_id=%s
                LIMIT 1
                """,
                (receiver_id, bot_message_id),
            )
            key_row = c.fetchone()

            if key_row and key_row[1] is not None:
                original_user_id, original_message_id = key_row
                c.execute(
                    """
                    DELETE FROM message_map
                    WHERE original_user_id=%s AND original_message_id=%s
                    RETURNING receiver_id, bot_message_id
                    """,
                    (original_user_id, original_message_id),
                )
                rows = c.fetchall()
            else:
                # Fallback for very old rows saved before original_message_id existed.
                c.execute(
                    """
                    DELETE FROM message_map
                    WHERE bot_message_id=%s
                    RETURNING receiver_id, bot_message_id
                    """,
                    (bot_message_id,),
                )
                rows = c.fetchall()

    deleted_count = 0
    for target_receiver_id, target_bot_msg_id in rows:
        try:
            bot.delete_message(target_receiver_id, target_bot_msg_id)
            deleted_count += 1
        except:
            pass

    return deleted_count
def purge_user_messages(user_id):

    total_deleted = 0
    while True:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    """
                    WITH batch AS (
                        SELECT ctid, bot_message_id, receiver_id
                        FROM message_map
                        WHERE original_user_id=%s
                        LIMIT %s
                    )
                    DELETE FROM message_map
                    USING batch
                    WHERE message_map.ctid = batch.ctid
                    RETURNING batch.bot_message_id, batch.receiver_id
                    """,
                    (user_id, MAP_DELETE_BATCH_SIZE),
                )
                rows = c.fetchall()

        if not rows:
            break

        for bot_msg_id, target_receiver_id in rows:
            try:
                bot.delete_message(target_receiver_id, bot_msg_id)
                total_deleted += 1
            except:
                pass
    return total_deleted

def purge_all_user_messages():

    deleted_count = 0
    while True:
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    """
                    WITH batch AS (
                        SELECT ctid, bot_message_id, receiver_id
                        FROM message_map
                        LIMIT %s
                    )
                    DELETE FROM message_map
                    USING batch
                    WHERE message_map.ctid = batch.ctid
                    RETURNING batch.bot_message_id, batch.receiver_id
                    """,
                    (MAP_DELETE_BATCH_SIZE,),
                )
                rows = c.fetchall()

        if not rows:
            break

        for bot_msg_id, receiver_id in rows:
            try:
                bot.delete_message(receiver_id, bot_msg_id)
                deleted_count += 1
            except:
                pass

    return deleted_count

def get_original_sender(bot_message_id, receiver_id=None):

    with get_connection() as conn:
        with conn.cursor() as c:
            if receiver_id is not None:
                c.execute(
                    """
                    SELECT original_user_id
                    FROM message_map
                    WHERE bot_message_id=%s AND receiver_id=%s
                    LIMIT 1
                    """,
                    (bot_message_id, receiver_id),
                )
            else:
                c.execute(
                    """
                    SELECT original_user_id
                    FROM message_map
                    WHERE bot_message_id=%s
                    LIMIT 1
                    """,
                    (bot_message_id,),
                )
            row = c.fetchone()

    return row[0] if row else None

def user_exists(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT 1 FROM users WHERE user_id=%s",
                (user_id,)
            )
            return c.fetchone() is not None
def set_referred_by(user_id, referrer_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET referred_by = %s
                WHERE user_id=%s AND referred_by IS NULL
            """, (referrer_id, user_id))

def add_referral_bonus(user_id):
    now = int(time.time())
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT last_activation_time FROM users WHERE user_id=%s", (user_id,))
            row = c.fetchone()
            if not row:
                return
            last_time = row[0]
            
            if last_time is None or last_time < now - INACTIVITY_LIMIT:
                new_last_time = now - INACTIVITY_LIMIT + 3600
            else:
                new_last_time = last_time + 3600
                
            c.execute("""
                UPDATE users
                SET last_activation_time = %s,
                    auto_banned = FALSE
                WHERE user_id=%s
            """, (new_last_time, user_id))
def add_user(user_id):
    now = int(time.time())
    free_activation_time = now - INACTIVITY_LIMIT + 3600
    
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO users(user_id, last_activation_time)
                VALUES(%s, %s)
                ON CONFLICT DO NOTHING
            """, (user_id, free_activation_time))

# =========================
# 🏷 USERNAME HELPERS
# =========================

def get_username(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT username FROM users WHERE user_id=%s",
                (user_id,)
            )
            row = c.fetchone()
            return row[0] if row else None


def set_username(user_id, username):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET username=%s
                WHERE user_id=%s
            """, (username.lower(), user_id))


def username_taken(username):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT 1 FROM users WHERE username=%s",
                (username.lower(),)
            )
            return c.fetchone() is not None


def get_recovery_ban_for_username(username):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT banned FROM recovery_users WHERE username=%s",
                (username.lower(),),
            )
            row = c.fetchone()
            return bool(row and row[0])


def export_recovery_payload():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT username, banned
                FROM users
                WHERE username IS NOT NULL
                ORDER BY username
                """
            )
            users = [{"username": r[0], "banned": bool(r[1])} for r in c.fetchall()]
            c.execute("SELECT word FROM banned_words ORDER BY word")
            words = [r[0] for r in c.fetchall()]
    return {
        "version": 1,
        "exported_at": int(time.time()),
        "users": users,
        "banned_words": words,
    }


def import_recovery_payload(payload):
    users = payload.get("users", [])
    words = payload.get("banned_words", [])

    imported_users = 0
    imported_words = 0
    with get_connection() as conn:
        with conn.cursor() as c:
            for item in users:
                username = str(item.get("username", "")).strip().lower()
                if not username:
                    continue
                banned = bool(item.get("banned", False))
                c.execute(
                    """
                    INSERT INTO recovery_users(username, banned)
                    VALUES(%s, %s)
                    ON CONFLICT (username) DO UPDATE SET banned=EXCLUDED.banned
                    """,
                    (username, banned),
                )
                imported_users += 1

            for word in words:
                clean_word = str(word).strip().lower()
                if not clean_word:
                    continue
                c.execute(
                    "INSERT INTO banned_words(word) VALUES(%s) ON CONFLICT DO NOTHING",
                    (clean_word,),
                )
                imported_words += 1

            c.execute(
                """
                UPDATE users u
                SET banned = r.banned
                FROM recovery_users r
                WHERE u.username = r.username
                """
            )

    return imported_users, imported_words
# =========================
# 👑 ADMIN HELPERS
# =========================

def is_admin(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT 1 FROM admins WHERE user_id=%s",
                (user_id,)
            )
            return c.fetchone() is not None


def should_store_mapping(sender_id, receivers=None):
    if MESSAGE_MAP_MODE == "off":
        return False
    if MESSAGE_MAP_MODE == "admin_only":
        if is_admin(sender_id):
            return True
        if receivers:
            return any(is_admin(uid) for uid in receivers)
        return False
    return True


def add_admin(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO admins(user_id)
                VALUES(%s)
                ON CONFLICT DO NOTHING
            """, (user_id,))


def remove_admin(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "DELETE FROM admins WHERE user_id=%s",
                (user_id,)
            )

def add_forward_target(chat_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO forward_targets(chat_id)
                VALUES(%s)
                ON CONFLICT DO NOTHING
                """,
                (chat_id,),
            )


def get_forward_targets():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT chat_id FROM forward_targets")
            return [row[0] for row in c.fetchall()]

def build_prefix(user_id):

    username = get_username(user_id)

    if username:
        return f"{username}~\n"

    return "👤 Unknown\n"

# =========================
# 🚫 BAN HELPERS
# =========================

def is_banned(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT banned FROM users WHERE user_id=%s",
                (user_id,)
            )
            row = c.fetchone()
            return row and row[0]


def ban_user(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET banned=TRUE WHERE user_id=%s",
                (user_id,)
            )


def unban_user(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET banned=FALSE WHERE user_id=%s",
                (user_id,)
            )
# =========================
# ⚠ WARNING HELPERS
# =========================

def get_warning_details(user_id):
    now = int(time.time())
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT warnings, last_warning_time, last_reason FROM user_warnings WHERE user_id=%s",
                (user_id,),
            )
            row = c.fetchone()
            if not row:
                return 0, "No warning reason recorded."
            warnings, last_warning_time, last_reason = row
            if last_warning_time and (now - last_warning_time) > WARNING_EXPIRY:
                c.execute(
                    "DELETE FROM user_warnings WHERE user_id=%s",
                    (user_id,),
                )
                return 0, "No warning reason recorded."
            return warnings, (last_reason or "No warning reason recorded.")


def get_warnings(user_id):
    return get_warning_details(user_id)[0]


def add_warning(user_id, reason="No reason provided"):
    now = int(time.time())
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT warnings, last_warning_time FROM user_warnings WHERE user_id=%s",
                (user_id,),
            )
            row = c.fetchone()

            if row:
                current_warnings, last_warning_time = row
                if last_warning_time and (now - last_warning_time) > WARNING_EXPIRY:
                    c.execute(
                        """
                        INSERT INTO user_warnings(user_id, warnings, last_warning_time, last_reason)
                        VALUES(%s, 1, %s, %s)
                        ON CONFLICT (user_id)
                        DO UPDATE SET
                            warnings = 1,
                            last_warning_time = EXCLUDED.last_warning_time,
                            last_reason = EXCLUDED.last_reason
                        RETURNING warnings
                        """,
                        (user_id, now, reason),
                    )
                    return c.fetchone()[0]
                if last_warning_time and (now - last_warning_time) < WARNING_COOLDOWN:
                    return current_warnings

            c.execute(
                """
                INSERT INTO user_warnings(user_id, warnings, last_warning_time, last_reason)
                VALUES(%s, 1, %s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET
                    warnings = user_warnings.warnings + 1,
                    last_warning_time = EXCLUDED.last_warning_time,
                    last_reason = EXCLUDED.last_reason
                RETURNING warnings
                """,
                (user_id, now, reason),
            )
            return c.fetchone()[0]


def reset_warnings(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "DELETE FROM user_warnings WHERE user_id=%s",
                (user_id,),
            )


def warning_action_for_count(warnings):
    if warnings >= MAX_WARNINGS:
        return "ban"
    if warnings == max(1, MAX_WARNINGS - 1):
        return "restrict"
    return "warn"
# =========================
# ⭐ WHITELIST HELPERS
# =========================

def is_whitelisted(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT whitelisted FROM users WHERE user_id=%s",
                (user_id,)
            )
            row = c.fetchone()
            return row and row[0]


def whitelist_user(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET whitelisted=TRUE WHERE user_id=%s",
                (user_id,)
            )
    try:
        bot.send_message(
            user_id,
            "⭐ You have been whitelisted!\nYou now have full access."
        )
    except Exception:
        pass


def remove_whitelist(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET whitelisted=FALSE WHERE user_id=%s",
                (user_id,)
            )
# =========================
# 🚪 JOIN CONTROL
# =========================

def is_join_open():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT value FROM settings WHERE key='join_open'"
            )
            row = c.fetchone()
            return row and row[0] == "true"


def set_join_status(status: bool):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE settings
                SET value=%s
                WHERE key='join_open'
            """, ("true" if status else "false",))


def is_maintenance_mode():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT value FROM settings WHERE key='maintenance_mode'")
            row = c.fetchone()
            return row and row[0] == "true"


def set_maintenance_mode(status: bool):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO settings(key, value)
                VALUES('maintenance_mode', %s)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """,
                ("true" if status else "false",),
            )

def get_media_caption():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT value FROM settings WHERE key='media_caption'")
            row = c.fetchone()
            return row[0] if row else None

def set_media_caption(caption_text: str):
    with get_connection() as conn:
        with conn.cursor() as c:
            if not caption_text:
                c.execute("DELETE FROM settings WHERE key='media_caption'")
            else:
                c.execute(
                    """
                    INSERT INTO settings(key, value)
                    VALUES('media_caption', %s)
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                    """,
                    (caption_text,)
                )


def _normalize_force_join_chat(raw_value):
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    if not value:
        return None

    # Keep private invite links exactly as provided.
    if "t.me/+" in value:
        return value

    # Public link -> convert to @username.
    if "t.me/" in value:
        slug = value.split("t.me/", 1)[1].split("?", 1)[0].strip("/")
        if slug:
            return f"@{slug}"

    if value.startswith("@"):
        return value

    return value


def _clear_force_join_cache():
    with force_join_cache_lock:
        force_join_cache.clear()


def is_force_join_enabled():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT value FROM settings WHERE key='force_join_enabled'")
            row = c.fetchone()
            return bool(row and row[0] == "true")


def get_force_join_channels():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT chat_id, button_name, invite_link FROM force_join_channels ORDER BY chat_id")
            rows = c.fetchall()
            channels = []
            for raw_chat_id, raw_button_name, raw_invite_link in rows:
                normalized = _normalize_force_join_chat(raw_chat_id)
                if normalized:
                    channels.append({
                        "chat_id": str(normalized),
                        "name": (raw_button_name.strip() if raw_button_name else str(normalized)),
                        "invite_link": (raw_invite_link.strip() if raw_invite_link else None),
                    })

            # Backward compatibility with legacy single-channel setting.
            if not channels:
                c.execute("SELECT value FROM settings WHERE key='force_join_chat_id'")
                legacy = c.fetchone()
                legacy_chat = _normalize_force_join_chat(legacy[0] if legacy else None)
                if legacy_chat:
                    channels.append({
                        "chat_id": str(legacy_chat),
                        "name": str(legacy_chat),
                        "invite_link": None,
                    })
            return channels


def get_force_join_chat():
    channels = get_force_join_channels()
    return channels[0]["chat_id"] if channels else None


def add_force_channel(chat_id, button_name=None, invite_link=None):
    normalized = _normalize_force_join_chat(chat_id)
    if not normalized:
        return False
    if not button_name:
        button_name = str(chat_id)
    invite_link = invite_link.strip() if isinstance(invite_link, str) and invite_link.strip() else None
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO force_join_channels(chat_id, button_name, invite_link)
                VALUES(%s, %s, %s)
                ON CONFLICT (chat_id)
                DO UPDATE SET
                    button_name=EXCLUDED.button_name,
                    invite_link=COALESCE(EXCLUDED.invite_link, force_join_channels.invite_link)
                """,
                (str(normalized), button_name, invite_link),
            )
    _clear_force_join_cache()
    return True


def remove_force_channel(chat_id):
    normalized = _normalize_force_join_chat(chat_id)
    if not normalized:
        return
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "DELETE FROM force_join_channels WHERE chat_id=%s",
                (str(normalized),),
            )
    _clear_force_join_cache()


def get_force_join_message():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT value FROM settings WHERE key='force_join_message'")
            row = c.fetchone()
            return row[0] if row and row[0] else "🚫 Please join our channel to use the bot."


def set_force_join_message(message):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO settings(key, value)
                VALUES('force_join_message', %s)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """,
                (message,),
            )


def set_force_join(chat_id, message):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO settings(key, value)
                VALUES('force_join_chat_id', %s)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """,
                (str(chat_id).strip(),),
            )
            c.execute(
                """
                INSERT INTO settings(key, value)
                VALUES('force_join_message', %s)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """,
                (message,),
            )
            c.execute(
                """
                INSERT INTO settings(key, value)
                VALUES('force_join_enabled', 'true')
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """
            )
    _clear_force_join_cache()
    add_force_channel(chat_id)


def disable_force_join():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO settings(key, value)
                VALUES('force_join_enabled', 'false')
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """
            )
    _clear_force_join_cache()


def is_vip(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT 1 FROM vip_users WHERE user_id=%s",
                (user_id,),
            )
            return c.fetchone() is not None


def add_vip(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                INSERT INTO vip_users(user_id)
                VALUES(%s)
                ON CONFLICT DO NOTHING
                """,
                (user_id,),
            )
    _clear_force_join_cache()


def remove_vip(user_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "DELETE FROM vip_users WHERE user_id=%s",
                (user_id,),
            )
    _clear_force_join_cache()


def _force_join_link(chat_ref):
    if isinstance(chat_ref, str):
        clean = chat_ref.strip()
        if clean.startswith("@"):
            return f"https://t.me/{clean[1:]}"
        if clean.startswith("https://t.me/") or clean.startswith("http://t.me/"):
            return clean
    return None


def parse_force_channel_input(text):
    parts = [p.strip() for p in str(text).split(" - ")]
    parts = [p for p in parts if p]
    if len(parts) >= 3:
        name = parts[0]
        chat_id = parts[1]
        invite_link = parts[2]
        return name, chat_id, invite_link
    if len(parts) == 2:
        name = parts[0]
        chat_id = parts[1]
        return name, chat_id, None
    if len(parts) == 1:
        token = parts[0]
        return token, token, None
    return None, None, None


def is_user_joined(user_id, force_refresh=False):
    if is_vip(user_id):
        return True

    channels = get_force_join_channels()
    if not channels:
        return True

    now = time.time()
    channels_key = tuple(str(ch.get("chat_id")) for ch in channels)
    cache_key = (channels_key, int(user_id))

    if not force_refresh:
        with force_join_cache_lock:
            cached = force_join_cache.get(cache_key)
            if cached and (now - cached[1]) < FORCE_JOIN_CACHE_TTL:
                return cached[0]

    joined = True
    had_verifiable_channel = False
    for channel in channels:
        chat_id = str(channel.get("chat_id", "")).strip()
        if not chat_id:
            continue

        # Private invite links cannot be verified with get_chat_member.
        if chat_id.startswith("https://t.me/+") or chat_id.startswith("http://t.me/+"):
            continue

        chat_ref = int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id
        had_verifiable_channel = True
        try:
            member = bot.get_chat_member(chat_ref, user_id)
            status = getattr(member, "status", "")
            if status not in JOINED_STATUSES:
                joined = False
                break
        except Exception:
            # Fail-open on API/check errors to avoid locking legitimate users out.
            print(f"Force join check skipped for chat {chat_id}: unable to verify membership right now.")
            continue

    if not had_verifiable_channel:
        joined = True

    with force_join_cache_lock:
        force_join_cache[cache_key] = (joined, now)

    return joined


def can_send_force_join_reminder(user_id):
    now = time.time()
    with force_join_reminder_lock:
        last = force_join_reminder_at.get(int(user_id), 0.0)
        if (now - last) < FORCE_JOIN_REMINDER_COOLDOWN:
            return False
        force_join_reminder_at[int(user_id)] = now
        return True


def send_force_join_ui(user_id):
    message = get_force_join_message()
    channels = get_force_join_channels()

    markup = InlineKeyboardMarkup()
    for ch in channels:
        join_link = ch.get("invite_link") or _force_join_link(ch.get("chat_id"))
        if join_link:
            label = str(ch.get("name") or ch.get("chat_id"))
            markup.add(
                InlineKeyboardButton(f"📢 {label}", url=join_link)
            )

    markup.add(
        InlineKeyboardButton("✅ I Joined", callback_data="check_join")
    )

    with last_fw_msg_lock:
        previous_id = last_fw_msg.get(int(user_id))

    if previous_id:
        try:
            bot.edit_message_text(
                message,
                chat_id=user_id,
                message_id=previous_id,
                reply_markup=markup
            )
            return
        except Exception:
            with last_fw_msg_lock:
                last_fw_msg.pop(int(user_id), None)

    sent = bot.send_message(
        user_id,
        message,
        reply_markup=markup
    )
    with last_fw_msg_lock:
        last_fw_msg[int(user_id)] = sent.message_id


def clear_force_join_ui(user_id):
    with last_fw_msg_lock:
        msg_id = last_fw_msg.pop(int(user_id), None)
    if not msg_id:
        return
    try:
        bot.delete_message(user_id, msg_id)
    except Exception:
        pass
# =========================
# 🧠 USER STATE RESOLVER
# =========================

def get_user_state(user_id):

    if is_admin(user_id):
        return "ADMIN"

    if is_banned(user_id):
        return "BANNED"

    if is_whitelisted(user_id):
        return "ACTIVE"

    username = get_username(user_id)

    if username is None:
        return "NO_USERNAME"

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT last_activation_time
                FROM users
                WHERE user_id=%s
            """, (user_id,))
            row = c.fetchone()

    if not row:
        return "JOINING"

    last_activation_time = row[0]

    if last_activation_time is None:
        return "JOINING"

    if last_activation_time < int(time.time()) - INACTIVITY_LIMIT:
        return "INACTIVE"

    return "ACTIVE"
# =========================
# 📊 GET ACTIVATION DATA
# =========================

def get_activation_data(user_id):
    """
    Returns:
        activation_media_count,
        total_media_sent,
        auto_banned,
        last_activation_time
    """

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT activation_media_count,
                       total_media_sent,
                       auto_banned,
                       last_activation_time
                FROM users
                WHERE user_id=%s
            """, (user_id,))
            return c.fetchone()
# =========================
# 📈 INCREMENT MEDIA
# =========================

def increment_media(user_id, amount=1):

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET activation_media_count = activation_media_count + %s,
                    total_media_sent = total_media_sent + %s
                WHERE user_id=%s
            """, (amount, amount, user_id))
#========================
#wellcome msg by admin helper
#========================   
def get_welcome_message():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT value FROM settings WHERE key='welcome_message'"
            )
            row = c.fetchone()
            return row[0] if row else "👋 Welcome!"

def set_welcome_message(text):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE settings
                SET value=%s
                WHERE key='welcome_message'
            """, (text,))
# =========================
# 🔄 ACTIVATE USER
# =========================

def activate_user(user_id):

    now = int(time.time())

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET activation_media_count = 0,
                    auto_banned = FALSE,
                    last_activation_time = %s
                WHERE user_id=%s
            """, (now, user_id))
# =========================
# ✅ CHECK ACTIVATION
# =========================

def check_activation(user_id):

    data = get_activation_data(user_id)

    if not data:
        return False

    activation_count, _, _, _ = data

    if activation_count >= REQUIRED_MEDIA:
        activate_user(user_id)
        return True

    return False
# =========================
# ⏳ AUTO INACTIVITY CHECK
# =========================

def auto_ban_inactive_users():

    limit = int(time.time()) - INACTIVITY_LIMIT

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET auto_banned = TRUE,
                    activation_media_count = 0
                WHERE auto_banned = FALSE
                  AND last_activation_time IS NOT NULL
                  AND last_activation_time < %s
            """, (limit,))
            
def is_duplicate_filter_enabled():
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "SELECT value FROM settings WHERE key='duplicate_filter'"
            )
            row = c.fetchone()
            return row and row[0] == "true"


def set_duplicate_filter(status: bool):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE settings
                SET value=%s
                WHERE key='duplicate_filter'
            """, ("true" if status else "false",))


def check_and_register_duplicate(file_id, sender_id):
    """
    Returns True if duplicate
    Returns False if first time
    """

    with get_connection() as conn:
        with conn.cursor() as c:

            c.execute(
                "SELECT 1 FROM media_duplicates WHERE file_id=%s",
                (file_id,)
            )
            exists = c.fetchone()

            if exists:
                c.execute("""
                    UPDATE media_duplicates
                    SET duplicate_count = duplicate_count + 1
                    WHERE file_id=%s
                """, (file_id,))
                return True

            else:
                c.execute("""
                    INSERT INTO media_duplicates(file_id, first_sender)
                    VALUES(%s, %s)
                """, (file_id, sender_id))
                return False
# =========================
# 🚪 START COMMAND
# =========================

@bot.message_handler(commands=['start'])
def start_command(message):

    user_id = message.chat.id

    parts = message.text.split()
    referrer_id = None
    if len(parts) > 1:
        try:
            referrer_id = int(parts[1])
        except ValueError:
            pass

    if is_maintenance_mode() and not is_admin(user_id):
        bot.send_message(user_id, "Bot is under maintenance. Try again later.")
        return

    # 🚫 Manual Ban
    if is_banned(user_id):
        bot.send_message(user_id, "🚫 You are banned.")
        return

    # 👑 Admin Auto Registration
    if is_admin(user_id):
        if not user_exists(user_id):
            add_user(user_id)

        if get_username(user_id) is None:
            set_username(user_id, "admin")

        bot.send_message(user_id, "👑 Admin access granted.")
        return

    # 🆕 New User
    if not user_exists(user_id):

        if not is_join_open():
            bot.send_message(
                user_id,
                "🚪 Joining is currently closed."
            )
            return

        add_user(user_id)

        if referrer_id and referrer_id != user_id and user_exists(referrer_id):
            set_referred_by(user_id, referrer_id)
            add_referral_bonus(referrer_id)
            try:
                bot.send_message(
                    referrer_id,
                    "🎉 Someone joined using your referral link! You earned 1 hour of free activity time."
                )
            except:
                pass

    # 🏷 Ask Username If Not Set
    if get_username(user_id) is None:
        bot.send_message(
            user_id,
            get_welcome_message()
        )
        
        return

    # 🧠 Show Current State
    state = get_user_state(user_id)

    if state == "JOINING":
        bot.send_message(
            user_id,
            f"🔒 Send {REQUIRED_MEDIA} media to join."
        )

    elif state == "INACTIVE":
        bot.send_message(
            user_id,
            f"⏳ You are inactive.\nSend {REQUIRED_MEDIA} media to reactivate."
        )

    else:
        bot.send_message(user_id, "👋 Welcome back!")
@bot.message_handler(commands=['referral'])
def referral_command(message):
    user_id = message.chat.id

    # 🚫 Restriction checks (important)
    if is_banned(user_id):
        bot.send_message(user_id, "🚫 You are banned.")
        return

    if get_username(user_id) is None:
        bot.send_message(user_id, "⚠️ Set username first using /start.")
        return

    try:
        bot_info = bot.get_me()

        # 🔗 Generate referral link
        ref_link = f"https://t.me/{bot_info.username}?start={user_id}"

        # 📊 Optional: count referrals (bonus improvement)
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    "SELECT COUNT(*) FROM users WHERE referred_by=%s",
                    (user_id,)
                )
                count = c.fetchone()[0]

        bot.send_message(
            user_id,
            f"🔗 Your referral link:\n{ref_link}\n\n"
            f"👥 Total referrals: {count}\n\n"
            "🎁 Earn 1 hour of activity time for each user who joins using your link!"
        )

    except Exception as e:
        print("Referral error:", e)
        bot.send_message(user_id, "⚠️ Failed to generate referral link. Try again later.")

# =========================
# 🏷 USERNAME CAPTURE
# =========================

@bot.message_handler(
    func=lambda m: get_username(m.chat.id) is None,
    content_types=['text']
)
def capture_username(message):

    user_id = message.chat.id
    username = message.text.strip().lower()

    # Prevent commands being treated as username
    if username.startswith('/'):
        return

    if len(username) < 3:
        bot.send_message(user_id, "Username too short. Try again.")
        return

    if username_taken(username):
        bot.send_message(user_id, "Username already taken. Try another.")
        return

    set_username(user_id, username)
    if get_recovery_ban_for_username(username):
        ban_user(user_id)
        bot.send_message(user_id, "Username recovered from backup with banned status.")
        return

    bot.send_message(
        user_id,
        f"✅ {username} set.\n\nNow send {REQUIRED_MEDIA} media to join."
    )
# =========================
# 🚫 BANNED WORD CHECK
# =========================

def contains_banned_word(text):

    if not text:
        return False

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT word FROM banned_words")
            words = [row[0] for row in c.fetchall()]

    text = text.lower()

    for word in words:
        if word in text:
            return True

    return False
# =========================
# 🔒 HANDLE RESTRICTIONS
# =========================

def handle_restrictions(message):

    user_id = message.chat.id
    state = get_user_state(user_id)

    # 🚫 Manual Ban
    if state == "BANNED":
        bot.send_message(user_id, "🚫 You are banned.")
        return True

    # 👑 Admin Bypass
    if state == "ADMIN":
        return False

    # ⭐ Whitelisted = Always Active
    if is_whitelisted(user_id):
        return False

    # 🚫 Word Filter (text only)
    if message.content_type == "text":
        if contains_banned_word(message.text):
            warnings = add_warning(user_id, "Banned word detected")
            action = warning_action_for_count(warnings)
            if action == "ban" and not is_whitelisted(user_id):
                ban_user(user_id)
                bot.send_message(
                    user_id,
                    f"🚫 You are banned. Warning limit reached ({warnings}/{MAX_WARNINGS})."
                )
            elif action == "restrict":
                bot.send_message(
                    user_id,
                    f"⏳ Temporary restriction warning ({warnings}/{MAX_WARNINGS}). One more violation may ban you."
                )
            else:
                bot.send_message(
                    user_id,
                    f"⚠️ Warning {warnings}/{MAX_WARNINGS} - banned word detected."
                )
            return True

    # ❌ No Username Yet
    if state == "NO_USERNAME":
        bot.send_message(
            user_id,
            "⚠️ Please set username first using /start."
        )
        return True

    # =========================
    # 🟡 JOINING STATE
    # =========================
    if state == "JOINING":

        if message.content_type in ['photo', 'video']:

            with activation_lock:
                activation_buffer[user_id] += 1

                if user_id in activation_timer:
                    return False  # allow relay but don't respond yet

                activation_timer[user_id] = True

            def finalize_activation():
                time.sleep(1.0)

                with activation_lock:
                    amount = activation_buffer.pop(user_id, 0)
                    activation_timer.pop(user_id, None)

                if amount > 0:
                    increment_media(user_id, amount)

                    activated = check_activation(user_id)

                    if activated:
                        bot.send_message(
                            user_id,
                            "🎉 You are now active for 6 hours!"
                        )
                    else:
                        remaining = REQUIRED_MEDIA - get_activation_data(user_id)[0]
                        bot.send_message(
                            user_id,
                            f"📸 {remaining} media left to join."
                        )

            threading.Thread(target=finalize_activation).start()

            return False  # allow media relay

        bot.send_message(
            user_id,
            f"🔒 Send {REQUIRED_MEDIA} media to join."
        )
        return True


    # =========================
    # 🔴 INACTIVE STATE
    # =========================
    if state == "INACTIVE":

        if message.content_type in ['photo', 'video']:

            with activation_lock:
                activation_buffer[user_id] += 1

                if user_id in activation_timer:
                    return False

                activation_timer[user_id] = True

            def finalize_reactivation():
                time.sleep(1.0)

                with activation_lock:
                    amount = activation_buffer.pop(user_id, 0)
                    activation_timer.pop(user_id, None)

                if amount > 0:
                    increment_media(user_id, amount)

                    activated = check_activation(user_id)

                    if activated:
                        bot.send_message(
                            user_id,
                            "🎉 You are reactivated for 6 hours!"
                        )
                    else:
                        remaining = REQUIRED_MEDIA - get_activation_data(user_id)[0]
                        bot.send_message(
                            user_id,
                            f"📸 {remaining} media left to reactivate."
                        )

            threading.Thread(target=finalize_reactivation).start()

            return False

        bot.send_message(
            user_id,
            f"⏳ You are inactive.\nSend {REQUIRED_MEDIA} media to reactivate."
        )
        return True


    # =========================
    # 🟢 ACTIVE STATE
    # =========================
    if state == "ACTIVE":

        if message.content_type in ['photo', 'video']:

            increment_media(user_id)
            renewed = check_activation(user_id)

        return False
# =========================
# 📥 GET ACTIVE RECEIVERS
# =========================

def get_active_receivers():

    active_cutoff = int(time.time()) - INACTIVITY_LIMIT
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT u.user_id
                FROM users u
                LEFT JOIN admins a ON u.user_id = a.user_id
                WHERE u.banned = FALSE
                  AND u.username IS NOT NULL
                  AND (
                        a.user_id IS NOT NULL
                        OR u.whitelisted = TRUE
                        OR (
                            u.last_activation_time IS NOT NULL
                            AND u.last_activation_time >= %s
                        )
                      )
            """, (active_cutoff,))
            return [row[0] for row in c.fetchall()]


def get_receivers_cached(force=False):
    global receiver_cache, receiver_cache_at
    now = time.time()
    with receiver_cache_lock:
        if force or (now - receiver_cache_at) >= RECEIVER_CACHE_TTL:
            receiver_cache = get_active_receivers()
            receiver_cache_at = now
        return list(receiver_cache)

# =========================
# 📝 SAVE MESSAGE MAP
# =========================

def save_mapping(bot_msg_id, original_user_id, original_message_id, receiver_id):

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO message_map
                (bot_message_id, original_user_id, original_message_id, receiver_id, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                bot_msg_id,
                original_user_id,
                original_message_id,
                receiver_id,
                int(time.time())
            ))


def save_mappings(rows):
    if not rows:
        return
    with get_connection() as conn:
        with conn.cursor() as c:
            execute_values(
                c,
                """
                INSERT INTO message_map
                (bot_message_id, original_user_id, original_message_id, receiver_id, created_at)
                VALUES %s
                """,
                rows,
                page_size=max(1, MAP_INSERT_BATCH_SIZE),
            )


def _retry_after_seconds(error):
    retry_after = getattr(error, "retry_after", None)
    if retry_after is not None:
        try:
            return float(retry_after)
        except Exception:
            return None
    result_json = getattr(error, "result_json", None)
    if isinstance(result_json, dict):
        params = result_json.get("parameters", {})
        retry_after = params.get("retry_after")
        if retry_after is not None:
            try:
                return float(retry_after)
            except Exception:
                return None
    return None


def _copy_message_with_retry(user_id, sender_id, message_id, reply_to_message_id=None, **kwargs):
    attempts = max(0, SEND_RETRIES) + 1
    for i in range(attempts):
        try:
            sent = bot.copy_message(
                chat_id=user_id,
                from_chat_id=sender_id,
                message_id=message_id,
                reply_to_message_id=reply_to_message_id,
                **kwargs
            )
            if FORWARD_DELAY > 0:
                time.sleep(FORWARD_DELAY)
            return sent
        except Exception as e:
            wait = _retry_after_seconds(e)
            if i < attempts - 1:
                if wait is not None:
                    time.sleep(max(0.05, wait))
                else:
                    time.sleep(0.15 * (i + 1))
                continue
            return None
    return None


def _send_text_with_retry(user_id, text, reply_to_message_id=None):
    attempts = max(0, SEND_RETRIES) + 1
    for i in range(attempts):
        try:
            sent = bot.send_message(user_id, text, reply_to_message_id=reply_to_message_id)
            if FORWARD_DELAY > 0:
                time.sleep(FORWARD_DELAY)
            return sent
        except Exception as e:
            wait = _retry_after_seconds(e)
            if i < attempts - 1:
                if wait is not None:
                    time.sleep(max(0.05, wait))
                else:
                    time.sleep(0.15 * (i + 1))
                continue
            return None
    return None


def broadcast_warning_notice(sender_id, target_id, warnings, reason):
    username = get_username(target_id) or "Unknown"
    text = (
        "⚠️ USER WARNING\n\n"
        f"👤 {username}\n"
        f"🆔 {target_id}\n"
        f"📊 {warnings}/{MAX_WARNINGS}\n"
        f"📝 Reason: {reason}"
    )
    receivers = [uid for uid in get_receivers_cached() if uid != sender_id]
    extra_targets = [cid for cid in get_forward_targets() if cid != sender_id]
    targets = list(dict.fromkeys(receivers + extra_targets))
    if not targets:
        return 0

    workers = max(1, min(SEND_MAX_WORKERS, len(targets)))
    sent_count = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_uid = {
            executor.submit(_send_text_with_retry, uid, text): uid
            for uid in targets
        }
        for future in as_completed(future_to_uid):
            try:
                if future.result():
                    sent_count += 1
            except Exception:
                pass
    return sent_count


def admin_broadcast_text(sender_id, text):
    receivers = [uid for uid in get_receivers_cached() if uid != sender_id]
    if is_force_join_enabled():
        receivers = [
            uid for uid in receivers
            if is_admin(uid) or is_vip(uid) or is_user_joined(uid)
        ]
    extra_targets = [cid for cid in get_forward_targets() if cid != sender_id]
    targets = list(dict.fromkeys(receivers + extra_targets))
    if not targets:
        return 0

    payload = f"📣 \n\n{text}"
    workers = max(1, min(SEND_MAX_WORKERS, len(targets)))
    sent_count = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_uid = {
            executor.submit(_send_text_with_retry, uid, payload): uid
            for uid in targets
        }
        for future in as_completed(future_to_uid):
            try:
                if future.result():
                    sent_count += 1
            except Exception:
                pass
    return sent_count
# =========================
# 🚀 BROADCAST WORKER
# =========================

def broadcast_worker():

    while True:
        job = broadcast_queue.get()

        try:
            if job["type"] == "single":
                _process_single(job["message"])

            elif job["type"] == "album":
                _process_album(job["messages"])
                # external_forward.forward_single(bot, message)


        except Exception as e:
            print("Broadcast error:", e)

        broadcast_queue.task_done()
# =========================
# 📤 PROCESS SINGLE MESSAGE
# =========================

def _process_single(message):

    sender_id = message.chat.id
    receivers = [uid for uid in get_receivers_cached() if uid != sender_id]
    if is_force_join_enabled():
        receivers = [
            uid for uid in receivers
            if is_admin(uid) or is_vip(uid) or is_user_joined(uid)
        ]
    extra_targets = [cid for cid in get_forward_targets() if cid != sender_id]
    targets = list(dict.fromkeys(receivers + extra_targets))
    store_mapping = should_store_mapping(sender_id, targets)
    mappings = []
    now = int(time.time())
    if not targets:
        return
    reply_map = {}
    if getattr(message, "reply_to_message", None):
        bot_msg_id_sender = message.reply_to_message.message_id
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    "SELECT original_user_id, original_message_id FROM message_map WHERE bot_message_id=%s AND receiver_id=%s LIMIT 1",
                    (bot_msg_id_sender, sender_id)
                )
                row = c.fetchone()
                if row and row[0]:
                    orig_uid, orig_msg_id = row[0], row[1]
                    reply_map[orig_uid] = orig_msg_id
                    
                    c.execute(
                        "SELECT receiver_id, bot_message_id FROM message_map WHERE original_user_id=%s AND original_message_id=%s",
                        (orig_uid, orig_msg_id)
                    )
                    for r_id, b_id in c.fetchall():
                        reply_map[r_id] = b_id

    if message.content_type == "text":
        prefix = build_prefix(sender_id)
        text_to_send = prefix + (message.text or "")
        send_fn = lambda uid: _send_text_with_retry(
            uid, text_to_send, reply_to_message_id=reply_map.get(uid)
        )

    else:
        media_caption = get_media_caption()
        username = get_username(sender_id) or ''
        original_caption = message.caption or ""

        if is_admin(sender_id):
            if media_caption:
                new_caption = media_caption.replace("{username}", username).replace("{caption}", original_caption).strip()
            else:
                new_caption = original_caption
        else:
            if media_caption:
                new_caption = media_caption.replace("{username}", username).replace("{caption}", "").strip()
            else:
                new_caption = ""

        send_fn = lambda uid: _copy_message_with_retry(
            uid, sender_id, message.message_id,
            reply_to_message_id=reply_map.get(uid),
            caption=new_caption
        )
    workers = max(1, min(SEND_MAX_WORKERS, len(targets)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_uid = {
            executor.submit(send_fn, user_id): user_id
            for user_id in targets
        }
        for future in as_completed(future_to_uid):
            user_id = future_to_uid[future]
            try:
                sent = future.result()
                if sent and store_mapping:
                    mappings.append((sent.message_id, sender_id, message.message_id, user_id, now))
                    if len(mappings) >= MAP_INSERT_BATCH_SIZE:
                        save_mappings(mappings)
                        mappings.clear()
            except Exception as e:
                print("Single send error:", e)
    if store_mapping:
        save_mappings(mappings)

   
# =========================
# 📸 PROCESS ALBUM MESSAGE
# =========================

def _process_album(messages):

    sender_id = messages[0].chat.id
    receivers = [uid for uid in get_receivers_cached() if uid != sender_id]
    if is_force_join_enabled():
        receivers = [
            uid for uid in receivers
            if is_admin(uid) or is_vip(uid) or is_user_joined(uid)
        ]
    extra_targets = [cid for cid in get_forward_targets() if cid != sender_id]
    targets = list(dict.fromkeys(receivers + extra_targets))
    store_mapping = should_store_mapping(sender_id, targets)
    mappings = []
    now = int(time.time())
    if not targets:
        return
    media_caption = get_media_caption()
    media_items = []
    
    for index, msg in enumerate(messages):
        new_caption = None
        new_ents = None
        
        if index == 0:
            original_caption = msg.caption or ""
            username = get_username(sender_id) or ''
            
            if is_admin(sender_id):
                if media_caption:
                    new_caption = media_caption.replace("{username}", username).replace("{caption}", original_caption).strip()
                else:
                    new_caption = original_caption
            else:
                if media_caption:
                    new_caption = media_caption.replace("{username}", username).replace("{caption}", "").strip()
                else:
                    new_caption = None  # 🚀 REMOVE FOR USERS
                    
        if msg.content_type == "photo":
            media_items.append((
                InputMediaPhoto(
                    media=msg.photo[-1].file_id,
                    caption=new_caption,
                    caption_entities=new_ents
                ),
                msg.message_id,
            ))
        elif msg.content_type == "video":
            media_items.append((
                InputMediaVideo(
                    media=msg.video.file_id,
                    caption=new_caption,
                    caption_entities=new_ents
                ),
                msg.message_id,
            ))
    chunks = [media_items[i:i+10] for i in range(0, len(media_items), 10)]

    def send_album_to_user(user_id):
        rows = []
        for chunk in chunks:
            chunk_media = [item[0] for item in chunk]
            sent_msgs = bot.send_media_group(user_id, chunk_media)
            if store_mapping:
                for sent, original_message_id in zip(sent_msgs, [item[1] for item in chunk]):
                    rows.append((sent.message_id, sender_id, original_message_id, user_id, now))
            if FORWARD_DELAY > 0:
                time.sleep(FORWARD_DELAY)
        return rows

    workers = max(1, min(SEND_MAX_WORKERS, len(targets)))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(send_album_to_user, user_id) for user_id in targets]
        for future in as_completed(futures):
            try:
                mappings.extend(future.result())
                if len(mappings) >= MAP_INSERT_BATCH_SIZE:
                    save_mappings(mappings)
                    mappings.clear()
            except Exception as e:
                print("Album send error:", e)
    if store_mapping:
        save_mappings(mappings)
    # external_forward.forward_album(bot, messages)


@bot.message_handler(
    func=lambda m: m.content_type == "text" and m.chat.id in (
        pending_fw_add | pending_fw_remove | pending_vip_add | pending_vip_remove | pending_fw_msg | pending_admin_broadcast | pending_admin_setcaption | pending_admin_setwelcome | pending_admin_addforward
    ),
    content_types=['text']
)
def handle_admin_pending_inputs(message):
    if not is_admin(message.chat.id):
        pending_fw_add.discard(message.chat.id)
        pending_fw_remove.discard(message.chat.id)
        pending_vip_add.discard(message.chat.id)
        pending_vip_remove.discard(message.chat.id)
        pending_fw_msg.discard(message.chat.id)
        pending_admin_broadcast.discard(message.chat.id)
        pending_admin_setcaption.discard(message.chat.id)
        pending_admin_setwelcome.discard(message.chat.id)
        pending_admin_addforward.discard(message.chat.id)
        return

    text = (message.text or "").strip()

    if message.chat.id in pending_fw_add:
        name, chat_id, invite_link = parse_force_channel_input(text)
        if chat_id and add_force_channel(chat_id, name, invite_link):
            bot.send_message(message.chat.id, "✅ Channel added.")
        else:
            bot.send_message(message.chat.id, "Invalid format. Use: Name - CHAT_ID - InviteLink")
        pending_fw_add.discard(message.chat.id)
        return

    if message.chat.id in pending_fw_remove:
        target = text.split(" - ", 1)[1].strip() if " - " in text else text
        remove_force_channel(target)
        bot.send_message(message.chat.id, "❌ Channel removed.")
        pending_fw_remove.discard(message.chat.id)
        return

    if message.chat.id in pending_vip_add:
        try:
            uid = int(text)
            add_vip(uid)
            bot.send_message(message.chat.id, "💎 VIP added.")
        except Exception:
            bot.send_message(message.chat.id, "Invalid ID.")
        pending_vip_add.discard(message.chat.id)
        return

    if message.chat.id in pending_vip_remove:
        try:
            uid = int(text)
            remove_vip(uid)
            bot.send_message(message.chat.id, "❌ VIP removed.")
        except Exception:
            bot.send_message(message.chat.id, "Invalid ID.")
        pending_vip_remove.discard(message.chat.id)
        return

    if message.chat.id in pending_fw_msg:
        new_msg = text
        if not new_msg:
            bot.send_message(message.chat.id, "Message cannot be empty.")
        else:
            set_force_join_message(new_msg)
            bot.send_message(message.chat.id, "✅ Firewall message updated.")
        pending_fw_msg.discard(message.chat.id)
        return

    if message.chat.id in pending_admin_broadcast:
        if text.lower() == "/cancel":
            bot.send_message(message.chat.id, "Broadcast cancelled.")
            pending_admin_broadcast.discard(message.chat.id)
            return
        if not text:
            bot.send_message(message.chat.id, "Broadcast text cannot be empty.")
            return
        sent = admin_broadcast_text(message.chat.id, text)
        bot.send_message(message.chat.id, f"📣 Broadcast sent to {sent} targets.")
        pending_admin_broadcast.discard(message.chat.id)
        return

    if message.chat.id in pending_admin_setcaption:
        if text.lower() == "/cancel":
            bot.send_message(message.chat.id, "Caption update cancelled.")
            pending_admin_setcaption.discard(message.chat.id)
            return
        if not text:
            bot.send_message(message.chat.id, "Caption cannot be empty. Type /cancel to abort or 'none' to clear.")
            return
        val = text if text.lower() != 'none' else ""
        set_media_caption(val)
        bot.send_message(message.chat.id, f"✅ Media caption updated to:\n{val}")
        pending_admin_setcaption.discard(message.chat.id)
        return

    if message.chat.id in pending_admin_setwelcome:
        if text.lower() == "/cancel":
            bot.send_message(message.chat.id, "Set welcome cancelled.")
            pending_admin_setwelcome.discard(message.chat.id)
            return
        if not text:
            bot.send_message(message.chat.id, "Text cannot be empty.")
            return
        val = "" if text.lower() == 'none' else text
        from media_bot import set_welcome_message
        try:
            set_welcome_message(val)
        except NameError:
            pass # Failsafe if not defined in outer scope but it is
        bot.send_message(message.chat.id, "✅ Welcome message updated.")
        pending_admin_setwelcome.discard(message.chat.id)
        return

    if message.chat.id in pending_admin_addforward:
        if text.lower() == "/cancel":
            bot.send_message(message.chat.id, "Add forward cancelled.")
            pending_admin_addforward.discard(message.chat.id)
            return
        chat_id = None
        target_name = None
        try:
            chat_id = int(text)
        except:
            if getattr(message, "forward_from_chat", None):
                chat_id = message.forward_from_chat.id
                target_name = getattr(message.forward_from_chat, "title", None)
            else:
                bot.send_message(message.chat.id, "Invalid CHAT_ID. Send a numeric ID or forward a text from the target channel.")
                return
        if chat_id:
            from media_bot import add_forward_target
            try:
                add_forward_target(chat_id)
            except NameError:
                pass
            name_str = f" {target_name}" if target_name else ""
            bot.send_message(message.chat.id, f"✅ Forward target added:{name_str} ({chat_id})")
            pending_admin_addforward.discard(message.chat.id)
        return

# =========================
# 🔁 RELAY HANDLER
# =========================

@bot.message_handler(
    func=lambda m: not m.text or not m.text.startswith('/'),
    content_types=['text', 'photo', 'video']
)
def relay(message):

    if is_maintenance_mode() and not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "Bot is under maintenance. Try again later.")
        return

    if is_force_join_enabled() and not is_admin(message.chat.id):
        joined = is_user_joined(message.chat.id)
        # Re-check once with fresh API data to avoid stale negative-cache blocking.
        if not joined:
            joined = is_user_joined(message.chat.id, force_refresh=True)
        if not joined:
            send_force_join_ui(message.chat.id)
            try:
                bot.delete_message(message.chat.id, message.message_id)
            except Exception:
                pass
            return
        clear_force_join_ui(message.chat.id)

    # =========================
    # ♻ DUPLICATE FILTER (EARLY)
    # =========================
    if message.content_type in ['photo', 'video'] and is_duplicate_filter_enabled():

        file_id = (
            message.photo[-1].file_id
            if message.content_type == 'photo'
            else message.video.file_id
        )

        is_dup = check_and_register_duplicate(file_id, message.chat.id)

        if is_dup:
            return  # silently ignore and DO NOT count activation
    if handle_restrictions(message):
        return
    # =========================
    # 1️⃣ TELEGRAM ALBUM
    # =========================
    if message.media_group_id:

        group_id = message.media_group_id
        media_groups[group_id].append(message)

        if group_id in album_timers:
            return

        album_timers[group_id] = True

        def finalize():
            time.sleep(1.0)

            album = media_groups.pop(group_id, [])
            album_timers.pop(group_id, None)

            if album:
                broadcast_queue.put({
                    "type": "album",
                    "messages": album
                })

        threading.Thread(target=finalize).start()
        return

    # =========================
    # 2️⃣ SINGLE PHOTO/VIDEO
    # =========================
    if message.content_type in ['photo', 'video']:
        broadcast_queue.put({
            "type": "single",
            "message": message
        })
        return

    # =========================
    # 3️⃣ TEXT
    # =========================
    broadcast_queue.put({
        "type": "single",
        "message": message
    })
# =========================
# ⏳ INACTIVITY SCHEDULER
# =========================

def inactivity_scheduler():

    while True:
        try:
            auto_ban_inactive_users()
        except Exception as e:
            print("Inactivity scheduler error:", e)

        time.sleep(60)  # check every 60 seconds
# =========================
# 🧹 MESSAGE MAP CLEANUP
# =========================

def message_map_cleanup_scheduler():

    while True:
        try:
            cutoff = int(time.time()) - (MAP_RETENTION_DAYS * 86400)
            while True:
                with get_connection() as conn:
                    with conn.cursor() as c:
                        c.execute(
                            """
                            WITH batch AS (
                                SELECT ctid
                                FROM message_map
                                WHERE created_at < %s
                                LIMIT %s
                            )
                            DELETE FROM message_map
                            USING batch
                            WHERE message_map.ctid = batch.ctid
                            RETURNING 1
                            """,
                            (cutoff, MAP_DELETE_BATCH_SIZE),
                        )
                        removed = len(c.fetchall())
                if removed < MAP_DELETE_BATCH_SIZE:
                    break
        except Exception as e:
            print("Cleanup error:", e)

        time.sleep(MAP_CLEANUP_INTERVAL_SECONDS)


def force_join_enforcement_scheduler():
    while True:
        try:
            if is_force_join_enabled():
                users = get_active_receivers()
                users = random.sample(users, min(len(users), 300))
                for user_id in users:
                    if is_admin(user_id) or is_vip(user_id):
                        continue
                    if not is_user_joined(user_id):
                        if can_send_force_join_reminder(user_id):
                            try:
                                bot.send_message(
                                    user_id,
                                    "🚫 Please rejoin required channels to continue using the bot."
                                )
                            except Exception:
                                pass
        except Exception as e:
            print("Force join check error:", e)

        time.sleep(300)
# =========================
# 🚀 START BACKGROUND WORKERS
# =========================

def start_background_workers():

    # Broadcast Worker
    threading.Thread(
        target=broadcast_worker,
        daemon=True
    ).start()

    # Inactivity Scheduler
    threading.Thread(
        target=inactivity_scheduler,
        daemon=True
    ).start()

    # Cleanup Scheduler
    threading.Thread(
        target=message_map_cleanup_scheduler,
        daemon=True
    ).start()

    # Force Join Enforcement Scheduler
    threading.Thread(
        target=force_join_enforcement_scheduler,
        daemon=True
    ).start()
    
# =========================
# ADMIN COMMANDS
# ========================
@bot.message_handler(commands=['dupon'])
def enable_duplicate_filter(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "Not admin.")
        return

    set_duplicate_filter(True)
    bot.send_message(message.chat.id, "✅ Duplicate filter ENABLED.")


@bot.message_handler(commands=['dupoff'])
def disable_duplicate_filter(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "Not admin.")
        return

    set_duplicate_filter(False)
    bot.send_message(message.chat.id, "❌ Duplicate filter DISABLED.")


@bot.message_handler(commands=['dupstatus'])
def duplicate_status(message):
    if not is_admin(message.chat.id):
        return

    status = "ON" if is_duplicate_filter_enabled() else "OFF"
    bot.send_message(message.chat.id, f"♻ Duplicate filter is {status}")
    
@bot.message_handler(commands=['del'])
def delete_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    bot_msg_id = message.reply_to_message.message_id

    deleted_count = delete_message_globally(message.chat.id, bot_msg_id)
    if deleted_count == 0:
        bot.send_message(
            message.chat.id,
            "No mapping found for this message. It may be old/cleaned or mapping mode is too strict."
        )
        return

    bot.send_message(message.chat.id, f"🗑 Message deleted in {deleted_count} chats.")
@bot.message_handler(commands=['addforward'])
def add_forward_target_cmd(message):

    if not is_admin(message.chat.id):
        return

    chat_id = None
    target_name = None
    parts = message.text.split()

    if len(parts) >= 2:
        try:
            chat_id = int(parts[1])
        except:
            bot.send_message(message.chat.id, "Invalid CHAT_ID.")
            return
    elif message.reply_to_message:
        source_chat = getattr(message.reply_to_message, "forward_from_chat", None)
        if source_chat:
            chat_id = source_chat.id
            target_name = getattr(source_chat, "title", None)

    if chat_id is None:
        bot.send_message(
            message.chat.id,
            "Usage: /addforward CHAT_ID\nOr reply to a forwarded message from the target group/channel."
        )
        return

    add_forward_target(chat_id)

    if target_name:
        bot.send_message(message.chat.id, f"Forward target added: {target_name} ({chat_id})")
    else:
        bot.send_message(message.chat.id, f"Forward target added: {chat_id}")

@bot.message_handler(commands=['purge'])
def purge_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    bot_msg_id = message.reply_to_message.message_id
    user_id = get_original_sender(bot_msg_id, message.chat.id)

    if not user_id:
        bot.send_message(message.chat.id, "User not found.")
        return

    purge_user_messages(user_id)
    bot.send_message(message.chat.id, "🔥 User messages purged.")
@bot.message_handler(commands=['apurgeall'])
def apurgeall_command(message):

    if not is_admin(message.chat.id):
        return

    deleted_count = purge_all_user_messages()
    bot.send_message(
        message.chat.id,
        f"Purged all mapped user messages. Deleted: {deleted_count}"
    )

def _panel_send_or_edit(chat_id, text, markup, message_id=None):
    if message_id is not None:
        try:
            bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="Markdown",
                reply_markup=markup,
            )
            return
        except Exception:
            pass
    bot.send_message(chat_id, text, parse_mode="Markdown", reply_markup=markup)


def _panel_main_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("📊 Stats", callback_data="panel_stats"),
        InlineKeyboardButton("👥 Users", callback_data="panel_users"),
    )
    markup.add(
        InlineKeyboardButton("🧱 Firewall", callback_data="panel_firewall"),
        InlineKeyboardButton("⚙ System", callback_data="panel_system"),
    )
    markup.add(
        InlineKeyboardButton("🚫 Moderation", callback_data="panel_moderation"),
        InlineKeyboardButton("💎 VIP", callback_data="panel_vip"),
    )
    return markup


def _panel_firewall_markup():
    status = "🟢 ON" if is_force_join_enabled() else "🔴 OFF"
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton(f"Status: {status}", callback_data="noop"))
    markup.add(
        InlineKeyboardButton("➕ Add Channel", callback_data="fw_add"),
        InlineKeyboardButton("➖ Remove Channel", callback_data="fw_remove"),
    )
    markup.add(
        InlineKeyboardButton("✏️ Edit Message", callback_data="fw_edit_msg"),
        InlineKeyboardButton("📊 Channels", callback_data="fw_status"),
    )
    markup.add(
        InlineKeyboardButton("🟢 Enable", callback_data="fw_on"),
        InlineKeyboardButton("🔴 Disable", callback_data="fw_off"),
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="panel_back"))
    return markup


def _panel_system_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🚪 Open Join", callback_data="admin_open_join"),
        InlineKeyboardButton("🔒 Close Join", callback_data="admin_close_join"),
    )
    markup.add(
        InlineKeyboardButton("📤 Export", callback_data="admin_export_recovery"),
        InlineKeyboardButton("📥 Import", callback_data="admin_import_recovery"),
    )
    markup.add(
        InlineKeyboardButton("🧹 Clear Map", callback_data="admin_clearmap"),
        InlineKeyboardButton("📊 Bot Stats", callback_data="admin_stats"),
    )
    markup.add(
        InlineKeyboardButton("📣 Broadcast", callback_data="panel_broadcast"),
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="panel_back"))
    return markup


def _panel_vip_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("➕ Add VIP", callback_data="vip_add"),
        InlineKeyboardButton("➖ Remove VIP", callback_data="vip_remove"),
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="panel_back"))
    return markup


def _panel_users_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🚫 Banned List", callback_data="admin_banned"),
        InlineKeyboardButton("👥 User List", callback_data="admin_userlist"),
    )
    markup.add(
        InlineKeyboardButton("🏆 Leaderboard", callback_data="admin_leaderboard"),
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="panel_back"))
    return markup


def _panel_moderation_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🚫 Banned List", callback_data="admin_banned"),
        InlineKeyboardButton("🧹 Clear Map", callback_data="admin_clearmap"),
    )
    markup.add(
        InlineKeyboardButton("📝 Edit Caption", callback_data="admin_setcaption"),
        InlineKeyboardButton("👋 Set Welcome", callback_data="admin_setwelcome")
    )
    markup.add(
        InlineKeyboardButton("➕ Add Forward", callback_data="admin_addforward")
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="panel_back"))
    return markup


@bot.message_handler(commands=['panel'])
def admin_panel(message):
    if not is_admin(message.chat.id):
        return
    _panel_send_or_edit(
        message.chat.id,
        "🛠 *Admin Dashboard*",
        _panel_main_markup(),
    )
@bot.message_handler(commands=['setwelcome'])
def set_welcome_cmd(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(
            message.chat.id,
            "Reply to a message to set it as welcome message."
        )
        return

    new_text = message.reply_to_message.text

    if not new_text:
        bot.send_message(message.chat.id, "Text only.")
        return

    set_welcome_message(new_text)

    bot.send_message(
        message.chat.id,
        "✅ Welcome message updated."
    )

@bot.message_handler(commands=['stats'])
def stats_command(message):

    if not is_admin(message.chat.id):
        return

    active_cutoff = int(time.time()) - INACTIVITY_LIMIT
    with get_connection() as conn:
        with conn.cursor() as c:

            c.execute("SELECT COUNT(*) FROM users")
            total = c.fetchone()[0]

            c.execute("""
                SELECT COUNT(*) FROM users u
                LEFT JOIN admins a ON u.user_id = a.user_id
                WHERE u.banned=FALSE
                  AND u.username IS NOT NULL
                  AND (
                        a.user_id IS NOT NULL
                        OR u.whitelisted=TRUE
                        OR (
                            u.last_activation_time IS NOT NULL
                            AND u.last_activation_time >= %s
                        )
                      )
            """, (active_cutoff,))
            active = c.fetchone()[0]

            c.execute("""
                SELECT COUNT(*) FROM users u
                LEFT JOIN admins a ON u.user_id = a.user_id
                WHERE u.banned=FALSE
                  AND u.username IS NOT NULL
                  AND a.user_id IS NULL
                  AND u.whitelisted=FALSE
                  AND u.last_activation_time IS NOT NULL
                  AND u.last_activation_time < %s
            """, (active_cutoff,))
            inactive = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM users WHERE banned=TRUE")
            banned = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM users WHERE whitelisted=TRUE")
            whitelisted = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM message_map")
            map_count = c.fetchone()[0]
            c.execute("SELECT COALESCE(SUM(duplicate_count), 0) FROM media_duplicates")
            duplicate_total = c.fetchone()[0]
    join_status = "OPEN" if is_join_open() else "CLOSED"

    bot.send_message(
        message.chat.id,
        f"""
📊 BOT STATS

👥 Total: {total}
🟢 Active: {active}
🔴 Inactive: {inactive}
🚫 Banned: {banned}
⭐ Whitelisted: {whitelisted}
♻ Duplicate Media: {duplicate_total}
📦 Message Map Rows: {map_count}
🚪 Join: {join_status}
        """
    )
@bot.message_handler(commands=['info'])
def info_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    bot_msg_id = message.reply_to_message.message_id
    user_id = get_original_sender(bot_msg_id, message.chat.id)

    if not user_id:
        bot.send_message(message.chat.id, "User not found.")
        return

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT username,
                       banned,
                       auto_banned,
                       whitelisted,
                       activation_media_count,
                       total_media_sent,
                       last_activation_time
                FROM users
                WHERE user_id=%s
            """, (user_id,))
            row = c.fetchone()

    if not row:
        bot.send_message(message.chat.id, "User not found.")
        return

    username, banned, auto_banned, whitelisted, act_count, total_media, last_time = row
    warnings, last_reason = get_warning_details(user_id)

    bot.send_message(
        message.chat.id,
        f"""
👤 USER INFO

🆔 ID: {user_id}
🏷 Username: {username}
📸 Activation Media: {act_count}
📦 Total Media Sent: {total_media}
⚠️ Warnings: {warnings}/{MAX_WARNINGS}
📝 Last Reason: {last_reason}

🚫 Manual Ban: {banned}
⏳ Auto Ban: {auto_banned}
⭐ Whitelisted: {whitelisted}
        """
    )

@bot.message_handler(commands=['warn'])
def warn_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    parts = message.text.split(maxsplit=1)
    reason = parts[1].strip() if len(parts) > 1 and parts[1].strip() else "No reason provided"

    target_id = get_original_sender(
        message.reply_to_message.message_id,
        message.chat.id
    )

    if not target_id:
        bot.send_message(message.chat.id, "User not found.")
        return

    if is_admin(target_id):
        bot.send_message(message.chat.id, "You cannot warn another admin.")
        return

    warnings = add_warning(target_id, reason)
    action = warning_action_for_count(warnings)

    try:
        bot.send_message(
            target_id,
            f"⚠️ You received a warning ({warnings}/{MAX_WARNINGS}).\nReason: {reason}"
        )
    except Exception:
        pass

    broadcast_warning_notice(message.chat.id, target_id, warnings, reason)

    if action == "ban" and not is_whitelisted(target_id):
        ban_user(target_id)
        bot.send_message(
            message.chat.id,
            f"🚫 User {target_id} banned (warnings exceeded: {warnings}/{MAX_WARNINGS})."
        )
    elif action == "restrict":
        try:
            bot.send_message(
                target_id,
                "⏳ You are temporarily restricted. Next violation can lead to ban."
            )
        except Exception:
            pass
        bot.send_message(
            message.chat.id,
            f"⏳ User {target_id} reached soft-punishment level ({warnings}/{MAX_WARNINGS}).\nReason: {reason}"
        )
    else:
        bot.send_message(
            message.chat.id,
            f"⚠️ Warning added.\nUser now has {warnings}/{MAX_WARNINGS} warnings.\nReason: {reason}"
        )


@bot.message_handler(commands=['warnings'])
def warnings_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    target_id = get_original_sender(
        message.reply_to_message.message_id,
        message.chat.id
    )

    if not target_id:
        bot.send_message(message.chat.id, "User not found.")
        return

    warnings, last_reason = get_warning_details(target_id)
    bot.send_message(
        message.chat.id,
        f"⚠️ User has {warnings}/{MAX_WARNINGS} warnings.\n📝 Last Reason: {last_reason}"
    )


@bot.message_handler(commands=['resetwarn'])
def resetwarn_command(message):

    if not is_admin(message.chat.id):
        return

    if not message.reply_to_message:
        bot.send_message(message.chat.id, "Reply to a relayed message.")
        return

    target_id = get_original_sender(
        message.reply_to_message.message_id,
        message.chat.id
    )

    if not target_id:
        bot.send_message(message.chat.id, "User not found.")
        return

    reset_warnings(target_id)
    bot.send_message(
        message.chat.id,
        f"✅ Warnings reset for {target_id}."
    )

@bot.message_handler(commands=['ban'])
def ban_command(message):

    if not is_admin(message.chat.id):
        return

    target_id = None

    # 🔹 1️⃣ If used as reply
    if message.reply_to_message:
        bot_msg_id = message.reply_to_message.message_id
        target_id = get_original_sender(bot_msg_id, message.chat.id)

        if not target_id:
            bot.send_message(message.chat.id, "User not found.")
            return

    # 🔹 2️⃣ If used with ID
    else:
        parts = message.text.split()

        if len(parts) < 2:
            bot.send_message(message.chat.id, "Usage:\n/ban USER_ID\nor reply to a relayed message.")
            return

        try:
            target_id = int(parts[1])
        except:
            bot.send_message(message.chat.id, "Invalid USER_ID.")
            return

    # 🔒 Final validation
    if not user_exists(target_id):
        bot.send_message(message.chat.id, "User not found in database.")
        return

    if is_admin(target_id):
        bot.send_message(message.chat.id, "You cannot ban another admin.")
        return

    ban_user(target_id)

    bot.send_message(
        message.chat.id,
        f"🚫 User {target_id} banned."
    )
@bot.message_handler(commands=['unban'])
def unban_command(message):

    if not is_admin(message.chat.id):
        return

    target_id = None

    # 🔹 1️⃣ If used as reply
    if message.reply_to_message:
        bot_msg_id = message.reply_to_message.message_id
        target_id = get_original_sender(bot_msg_id, message.chat.id)

        if not target_id:
            bot.send_message(message.chat.id, "User not found.")
            return

    # 🔹 2️⃣ If used with ID
    else:
        parts = message.text.split()

        if len(parts) < 2:
            bot.send_message(
                message.chat.id,
                "Usage:\n/unban USER_ID\nor reply to a relayed message."
            )
            return

        try:
            target_id = int(parts[1])
        except:
            bot.send_message(message.chat.id, "Invalid USER_ID.")
            return

    # 🔍 Final validation
    if not user_exists(target_id):
        bot.send_message(message.chat.id, "User not found in database.")
        return

    unban_user(target_id)

    bot.send_message(
        message.chat.id,
        f"✅ User {target_id} unbanned."
    )
@bot.message_handler(commands=['addadmin'])
def addadmin_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        return

    try:
        target_id = int(parts[1])
    except Exception:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    add_admin(target_id)
    bot.send_message(message.chat.id, "Admin added.")
@bot.message_handler(commands=['removeadmin'])
def removeadmin_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        return

    try:
        target_id = int(parts[1])
    except Exception:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    remove_admin(target_id)
    bot.send_message(message.chat.id, "Admin removed.")
@bot.message_handler(commands=['openjoin'])
def openjoin_command(message):

    if not is_admin(message.chat.id):
        return

    set_join_status(True)
    bot.send_message(message.chat.id, "Join opened.")
@bot.message_handler(commands=['closejoin'])
def closejoin_command(message):

    if not is_admin(message.chat.id):
        return

    set_join_status(False)
    bot.send_message(message.chat.id, "Join closed.")
@bot.message_handler(commands=['clearmap'])
def clearmap_command(message):

    if not is_admin(message.chat.id):
        return

    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("DELETE FROM message_map")

    bot.send_message(message.chat.id, "Message map cleared.")
@bot.message_handler(commands=['whitelist'])
def whitelist_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /whitelist USER_ID")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    whitelist_user(target_id)

    bot.send_message(
        message.chat.id,
        f"⭐ User {target_id} added to whitelist."
    )
@bot.message_handler(commands=['unwhitelist'])
def unwhitelist_command(message):

    if not is_admin(message.chat.id):
        return

    parts = message.text.split()

    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /unwhitelist USER_ID")
        return

    try:
        target_id = int(parts[1])
    except:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    remove_whitelist(target_id)

    bot.send_message(
        message.chat.id,
        f"❌ User {target_id} removed from whitelist."
    )
@bot.message_handler(commands=['closebot'])
def closebot_command(message):
    if not is_admin(message.chat.id):
        return
    set_maintenance_mode(True)
    bot.send_message(message.chat.id, "Maintenance mode enabled. Bot is closed for users.")


@bot.message_handler(commands=['openbot'])
def openbot_command(message):
    if not is_admin(message.chat.id):
        return
    set_maintenance_mode(False)
    bot.send_message(message.chat.id, "Maintenance mode disabled. Bot is open now.")


@bot.message_handler(commands=['setforcejoin'])
def set_force_join_cmd(message):
    if not is_admin(message.chat.id):
        return

    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage:\n/setforcejoin CHAT_ID_OR_@USERNAME [message]")
        return

    chat_ref = parts[1].strip()
    custom_message = parts[2] if len(parts) > 2 else "🚫 Please join to continue."
    set_force_join(chat_ref, custom_message)
    if "t.me/+" in chat_ref:
        bot.send_message(
            message.chat.id,
            "✅ Force join enabled, but invite-link based channels may fail membership verification. Prefer @username or numeric chat ID."
        )
    else:
        bot.send_message(message.chat.id, f"✅ Force join enabled for: {chat_ref}")


@bot.message_handler(commands=['addfw'])
def add_fw_channel(message):
    if not is_admin(message.chat.id):
        return

    payload = message.text.split(maxsplit=1)
    if len(payload) < 2:
        bot.send_message(message.chat.id, "Usage: /addfw Name - CHAT_ID - InviteLink\nor /addfw Name - CHAT_ID")
        return

    name, chat_id, invite_link = parse_force_channel_input(payload[1].strip())
    if chat_id and add_force_channel(chat_id, name, invite_link):
        bot.send_message(message.chat.id, "✅ Force-join channel added.")
    else:
        bot.send_message(message.chat.id, "Invalid channel reference.")


@bot.message_handler(commands=['removefw'])
def remove_fw_channel(message):
    if not is_admin(message.chat.id):
        return

    payload = message.text.split(maxsplit=1)
    if len(payload) < 2:
        bot.send_message(message.chat.id, "Usage: /removefw CHAT_ID\nor /removefw Name - CHAT_ID")
        return

    _, chat_id, _ = parse_force_channel_input(payload[1].strip())
    if not chat_id:
        bot.send_message(message.chat.id, "Invalid format.")
        return
    remove_force_channel(chat_id)
    bot.send_message(message.chat.id, "❌ Force-join channel removed.")


@bot.message_handler(commands=['addvip'])
def add_vip_cmd(message):
    if not is_admin(message.chat.id):
        return

    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /addvip USER_ID")
        return

    try:
        target_id = int(parts[1])
    except Exception:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    add_vip(target_id)
    bot.send_message(message.chat.id, f"💎 VIP added: {target_id}")


@bot.message_handler(commands=['removevip'])
def remove_vip_cmd(message):
    if not is_admin(message.chat.id):
        return

    parts = message.text.split()
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /removevip USER_ID")
        return

    try:
        target_id = int(parts[1])
    except Exception:
        bot.send_message(message.chat.id, "Invalid USER_ID.")
        return

    remove_vip(target_id)
    bot.send_message(message.chat.id, f"❌ VIP removed: {target_id}")


@bot.message_handler(commands=['disableforcejoin'])
def disable_force_join_cmd(message):
    if not is_admin(message.chat.id):
        return

    disable_force_join()
    bot.send_message(message.chat.id, "❌ Force join disabled.")


@bot.message_handler(commands=['forcejoinstatus'])
def force_join_status(message):
    if not is_admin(message.chat.id):
        return

    enabled = is_force_join_enabled()
    channels = get_force_join_channels()
    custom_message = get_force_join_message()
    channels_text = (
        "\n".join(
            f"- {ch['name']} | chat_id: {ch['chat_id']} | link: {ch.get('invite_link') or 'auto'}"
            for ch in channels
        )
        if channels else "None"
    )

    bot.send_message(
        message.chat.id,
        f"Force Join: {'ON' if enabled else 'OFF'}\nChannels: {channels_text}\nMessage: {custom_message}"
    )


@bot.message_handler(commands=['setfwmsg'])
def set_firewall_message_cmd(message):
    if not is_admin(message.chat.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.send_message(message.chat.id, "Usage: /setfwmsg Your new firewall message")
        return

    set_force_join_message(parts[1].strip())
    bot.send_message(message.chat.id, "✅ Firewall message updated.")


@bot.message_handler(commands=['broadcast'])
def broadcast_cmd(message):
    if not is_admin(message.chat.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        pending_admin_broadcast.add(message.chat.id)
        bot.send_message(message.chat.id, "📣 Send broadcast text. Type /cancel to stop.")
        return

    sent = admin_broadcast_text(message.chat.id, parts[1].strip())
    bot.send_message(message.chat.id, f"📣 Broadcast sent to {sent} targets.")


@bot.message_handler(commands=['adminmenu'])
def admin_menu(message):

    if not is_admin(message.chat.id):
        return

    bot.send_message(
        message.chat.id,
        """
🛠 ADMIN COMMAND MENU

📊 /stats  
→ Show bot statistics

🔎 /info USER_ID  
→ View user details

⚠ /warn (reply)  
→ Add warning to replied user

📌 /warnings (reply)  
→ View warnings for replied user

♻ /resetwarn (reply)  
→ Reset warnings for replied user

🚫 /ban USER_ID  
→ Manually ban user

✅ /unban USER_ID  
→ Remove manual ban

⭐ /whitelist USER_ID  
→ Bypass activation/inactivity

❌ /unwhitelist USER_ID  
→ Remove whitelist access

📢 /setforcejoin CHAT [message]  
→ Enable force join and set channel

➕ /addfw CHAT  
→ Add extra force-join channel

➖ /removefw CHAT  
→ Remove force-join channel

🚫 /disableforcejoin  
→ Disable force join

📋 /forcejoinstatus  
→ Show force join configuration

✏️ /setfwmsg TEXT  
→ Update firewall message text

📣 /broadcast TEXT  
→ Send admin broadcast to active users

💎 /addvip USER_ID  
→ Add VIP bypass user

❌ /removevip USER_ID  
→ Remove VIP bypass user

👑 /addadmin USER_ID  
→ Add new admin

🗑 /removeadmin USER_ID  
→ Remove admin

🚪 /openjoin  
→ Allow new users to join

🔒 /closejoin  
→ Stop new users from joining

🧹 /clearmap  
→ Clear message mapping table

🔥 /apurgeall  
→ Delete all mapped user messages globally

📦 /addword WORD  
→ Add banned word

❌ /removeword WORD  
→ Remove banned word

📃 /words  
→ Show banned words list
        """
    )
@bot.callback_query_handler(func=lambda call: call.data == "check_join")
def check_join_callback(call):
    user_id = call.from_user.id
    msg = call.message
    if is_user_joined(user_id, force_refresh=True):
        bot.answer_callback_query(call.id, "✅ Verified!")
        with last_fw_msg_lock:
            last_fw_msg.pop(int(user_id), None)
        try:
            bot.edit_message_text(
                "✅ You have joined all required channels.\n\n🎉 Access granted!",
                chat_id=msg.chat.id,
                message_id=msg.message_id
            )
        except Exception:
            try:
                bot.delete_message(msg.chat.id, msg.message_id)
            except Exception:
                pass
            bot.send_message(user_id, "🎉 You can now use the bot.")
    else:
        bot.answer_callback_query(call.id, "❌ You still haven't joined all channels.", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data == "noop")
def noop_callback(call):
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data in {
    "panel_back", "panel_stats", "panel_users", "panel_firewall", "panel_system", "panel_moderation", "panel_vip", "panel_broadcast"
})
def panel_navigation_callbacks(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Not admin.")
        return

    if call.data == "panel_back":
        bot.answer_callback_query(call.id)
        _panel_send_or_edit(
            call.message.chat.id,
            "🛠 *Admin Dashboard*",
            _panel_main_markup(),
            message_id=call.message.message_id,
        )
        return

    if call.data == "panel_stats":
        bot.answer_callback_query(call.id)
        stats_command(call.message)
        return

    if call.data == "panel_users":
        bot.answer_callback_query(call.id)
        _panel_send_or_edit(
            call.message.chat.id,
            "👥 *Users Panel*",
            _panel_users_markup(),
            message_id=call.message.message_id,
        )
        return

    if call.data == "panel_firewall":
        bot.answer_callback_query(call.id)
        _panel_send_or_edit(
            call.message.chat.id,
            "🧱 *Firewall Control Panel*",
            _panel_firewall_markup(),
            message_id=call.message.message_id,
        )
        return

    if call.data == "panel_system":
        bot.answer_callback_query(call.id)
        _panel_send_or_edit(
            call.message.chat.id,
            "⚙ *System Settings*",
            _panel_system_markup(),
            message_id=call.message.message_id,
        )
        return

    if call.data == "panel_moderation":
        bot.answer_callback_query(call.id)
        _panel_send_or_edit(
            call.message.chat.id,
            "🚫 *Moderation Panel*",
            _panel_moderation_markup(),
            message_id=call.message.message_id,
        )
        return

    if call.data == "panel_vip":
        bot.answer_callback_query(call.id)
        _panel_send_or_edit(
            call.message.chat.id,
            "💎 *VIP Management*",
            _panel_vip_markup(),
            message_id=call.message.message_id,
        )
        return

    if call.data == "panel_broadcast":
        pending_admin_broadcast.add(call.from_user.id)
        bot.answer_callback_query(call.id, "Awaiting broadcast text")
        bot.send_message(call.from_user.id, "📣 Send broadcast text. Type /cancel to stop.")
        return


@bot.callback_query_handler(func=lambda call: call.data in {
    "fw_on", "fw_off", "fw_add", "fw_remove", "vip_add", "vip_remove", "fw_status", "fw_edit_msg"
})
def firewall_ui_callbacks(call):
    actor_id = call.from_user.id
    admin_chat_id = call.message.chat.id
    if not is_admin(actor_id):
        bot.answer_callback_query(call.id, "Not admin.")
        return

    if call.data == "fw_on":
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    """
                    INSERT INTO settings(key, value)
                    VALUES('force_join_enabled', 'true')
                    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                    """
                )
        _clear_force_join_cache()
        bot.answer_callback_query(call.id, "🧱 Firewall Enabled")
        return

    if call.data == "fw_off":
        disable_force_join()
        bot.answer_callback_query(call.id, "🧱 Firewall Disabled")
        return

    if call.data == "fw_add":
        pending_fw_add.add(actor_id)
        bot.answer_callback_query(call.id, "Awaiting channel")
        bot.send_message(actor_id, "📢 Send channel as:\nName - CHAT_ID - InviteLink")
        return

    if call.data == "fw_remove":
        pending_fw_remove.add(actor_id)
        bot.answer_callback_query(call.id, "Awaiting channel")
        bot.send_message(actor_id, "❌ Send channel to remove")
        return

    if call.data == "vip_add":
        pending_vip_add.add(actor_id)
        bot.answer_callback_query(call.id, "Awaiting USER_ID")
        bot.send_message(actor_id, "💎 Send USER_ID to add VIP")
        return

    if call.data == "vip_remove":
        pending_vip_remove.add(actor_id)
        bot.answer_callback_query(call.id, "Awaiting USER_ID")
        bot.send_message(actor_id, "❌ Send USER_ID to remove VIP")
        return

    if call.data == "fw_status":
        enabled = is_force_join_enabled()
        channels = get_force_join_channels()
        channels_text = "\n".join(f"• {ch['name']}" for ch in channels) if channels else "None"
        bot.answer_callback_query(call.id)
        bot.send_message(
            admin_chat_id,
            f"🧱 FIREWALL STATUS\n\nStatus: {'ON ✅' if enabled else 'OFF ❌'}\n\nChannels:\n{channels_text}",
        )
        return

    if call.data == "fw_edit_msg":
        pending_fw_msg.add(actor_id)
        bot.answer_callback_query(call.id, "Awaiting message")
        bot.send_message(actor_id, "✏️ Send new firewall message:")
        return


@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
def admin_callbacks(call):

    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Not admin.")
        return

    data = call.data

    if data == "admin_stats":
        stats_command(call.message)

    elif data == "admin_open_join":
        set_join_status(True)
        bot.answer_callback_query(call.id, "Join opened.")

    elif data == "admin_close_join":
        set_join_status(False)
        bot.answer_callback_query(call.id, "Join closed.")

    elif data == "admin_clearmap":
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM message_map")
        bot.answer_callback_query(call.id, "Message map cleared.")

    elif data == "admin_setcaption":
        pending_admin_setcaption.add(call.from_user.id)
        bot.answer_callback_query(call.id, "Awaiting new caption")
        bot.send_message(call.from_user.id, "📝 Send the new caption template. Type /cancel to stop.\nUse {username} to include sender name.\nUse {caption} to include original caption.\nType 'none' to clear it entirely.")

    elif data == "admin_setwelcome":
        pending_admin_setwelcome.add(call.from_user.id)
        bot.answer_callback_query(call.id, "Awaiting welcome message")
        bot.send_message(call.from_user.id, "👋 Send the new welcome message text. Type /cancel to abort.\nType 'none' to remove it.")

    elif data == "admin_addforward":
        pending_admin_addforward.add(call.from_user.id)
        bot.answer_callback_query(call.id, "Awaiting forward target")
        bot.send_message(call.from_user.id, "➕ Send the CHAT_ID for the forward target. You can also forward a text message from the target channel/group here. Type /cancel to abort.")

    elif data == "admin_banned":
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT user_id FROM users WHERE banned=TRUE
                """)
                rows = c.fetchall()

        if rows:
            text = "\n".join(str(r[0]) for r in rows)
        else:
            text = "No banned users."

        bot.send_message(call.message.chat.id, text)

    elif data == "admin_userlist":
        bot.send_message(call.message.chat.id, "🔄 Fetching user data...")
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT u.user_id, u.username, u.total_media_sent, COUNT(r.user_id)
                    FROM users u
                    LEFT JOIN users r ON r.referred_by = u.user_id
                    WHERE u.username IS NOT NULL
                    GROUP BY u.user_id, u.username, u.total_media_sent
                    ORDER BY u.total_media_sent DESC
                    LIMIT 30
                """)
                rows = c.fetchall()
        
        if rows:
            lines = ["👥 *User List (Top 30)*", ""]
            for row in rows:
                uid, fallback, media, refs = row[0], row[1], row[2], row[3]
                display_name = f"@{fallback}"
                try:
                    chat = bot.get_chat(uid)
                    if chat.username:
                        display_name = f"@{chat.username}"
                    else:
                        full = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
                        if full: display_name = full
                except Exception:
                    pass
                lines.append(f"• {display_name} | 📸 {media} | 🎁 {refs} referrals")
            text = "\n".join(lines)
        else:
            text = "No users found."
        
        bot.send_message(call.message.chat.id, text, parse_mode="Markdown")

    elif data == "admin_leaderboard":
        bot.send_message(call.message.chat.id, "🔄 Fetching leaderboard...")
        with get_connection() as conn:
            with conn.cursor() as c:
                c.execute("""
                    SELECT u.user_id, u.username, COUNT(r.user_id) as refs
                    FROM users u
                    LEFT JOIN users r ON r.referred_by = u.user_id
                    WHERE u.username IS NOT NULL
                    GROUP BY u.user_id, u.username
                    HAVING COUNT(r.user_id) > 0
                    ORDER BY refs DESC
                    LIMIT 20
                """)
                rows = c.fetchall()
        
        if rows:
            lines = ["🏆 *Top Referrers Leaderboard*", ""]
            for i, row in enumerate(rows, 1):
                uid, fallback, refs = row[0], row[1], row[2]
                display_name = f"@{fallback}"
                try:
                    chat = bot.get_chat(uid)
                    if chat.username:
                        display_name = f"@{chat.username}"
                    else:
                        full = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
                        if full: display_name = full
                except Exception:
                    pass
                lines.append(f"{i}. {display_name} - {refs} invites")
            text = "\n".join(lines)
        else:
            text = "No referrals yet."
            
        bot.send_message(call.message.chat.id, text, parse_mode="Markdown")

    elif data == "admin_export_recovery":
        payload = export_recovery_payload()
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile("w", delete=False, suffix=".json", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
                temp_path = f.name
            with open(temp_path, "rb") as doc:
                bot.send_document(call.message.chat.id, doc, caption="Recovery export (username+banned+banned_words)")
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

    elif data == "admin_import_recovery":
        pending_recovery_import.add(call.message.chat.id)
        bot.send_message(call.message.chat.id, "Send the recovery JSON file as a document to import.")

    bot.answer_callback_query(call.id)


@bot.message_handler(content_types=['document'])
def import_recovery_document(message):
    if not is_admin(message.chat.id):
        return
    if message.chat.id not in pending_recovery_import:
        return

    try:
        file_info = bot.get_file(message.document.file_id)
        raw = bot.download_file(file_info.file_path)
        payload = json.loads(raw.decode("utf-8"))
        imported_users, imported_words = import_recovery_payload(payload)
        pending_recovery_import.discard(message.chat.id)
        bot.send_message(
            message.chat.id,
            f"Recovery import complete.\nUsers imported: {imported_users}\nBanned words imported: {imported_words}",
        )
    except Exception as e:
        bot.send_message(message.chat.id, f"Import failed: {e}")


@bot.message_handler(commands=['chatid'], content_types=['text'])
def get_chat_id(message):
    bot.reply_to(message, f"Chat ID: {message.chat.id}")
@bot.channel_post_handler(commands=['cchatid'])
def get_channel_id(message):
    bot.send_message(message.chat.id, f"Channel ID: {message.chat.id}")

@bot.message_handler(commands=['setmediacaption'])
def set_media_caption_cmd(message):
    if not is_admin(message.chat.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(message.chat.id, "Usage: /setmediacaption [Your custom caption]\nProvide 'none' to remove.\nUse {username} to include sender name.\nUse {caption} to include original caption.")
        return
    val = parts[1].strip()
    if val.lower() == 'none':
        val = ""
    set_media_caption(val)
    bot.send_message(message.chat.id, f"✅ Media caption updated to: {val}")

# =========================
# 🎁 REFERRAL SYSTEM
# =========================

def set_referred_by(user_id, referrer_id):
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute(
                "UPDATE users SET referred_by=%s WHERE user_id=%s",
                (referrer_id, user_id)
            )

def add_referral_bonus(referrer_id):
    now = int(time.time())
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("""
                UPDATE users
                SET last_activation_time = GREATEST(%s - %s, COALESCE(last_activation_time, 0)) + 3600,
                    auto_banned = FALSE,
                    activation_media_count = 0
                WHERE user_id=%s
            """, (now, INACTIVITY_LIMIT, referrer_id))

@bot.message_handler(commands=['menu'])
def menu_command(message):
    user_id = message.chat.id
    if is_banned(user_id):
        bot.send_message(user_id, "🚫 You are banned.")
        return

    now = int(time.time())
    with get_connection() as conn:
        with conn.cursor() as c:
            c.execute("SELECT COUNT(*) FROM users WHERE referred_by=%s", (user_id,))
            invites = c.fetchone()[0]
            
            c.execute("SELECT last_activation_time FROM users WHERE user_id=%s", (user_id,))
            row = c.fetchone()
            
    if row and row[0]:
        last_act = row[0]
        time_left = max(0, (last_act + INACTIVITY_LIMIT) - now)
    else:
        time_left = 0

    hours = time_left // 3600
    minutes = (time_left % 3600) // 60
    
    bot.send_message(
        user_id,
        f"📊 *Your Dashboard*\n\n"
        f"👥 *Users Invited:* {invites}\n"
        f"⏳ *Activity Time Left:* {hours}h {minutes}m\n\n"
        f"Use /referral to get your invite link and earn more time (1 invite = +1 hour)!",
        parse_mode="Markdown"
    )

# =========================
# 🚀 MAIN BOOT
# =========================

if __name__ == "__main__":

    print("Starting bot...")

    init_db_pool()
    init_db()
    print("Database ready.")

    start_background_workers()
    print("Background workers running.")

    bot.infinity_polling(skip_pending=True)

