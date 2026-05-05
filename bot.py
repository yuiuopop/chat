import os
import asyncio
import threading
import logging
import sqlite3
from contextlib import contextmanager

# Pyrogram sync import needs a current event loop on Python 3.10+
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

import requests
from flask import Flask
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import RPCError, SessionPasswordNeeded
from pyrogram.handlers import MessageHandler
import telebot


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


# -----------------------------
# DB (single-file sqlite)
# -----------------------------
DB_PATH = "saved_to_target.db"


@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


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
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_message_id INTEGER UNIQUE,
                target_chat_id INTEGER,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS source_chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    logger.info("DB initialized")


def get_setting(key, default=None):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = c.fetchone()
        return row[0] if row else default


def set_setting(key, value):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, str(value))
        )


def already_sent(source_message_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM sent_messages WHERE source_message_id = ?", (source_message_id,))
        return c.fetchone() is not None


def mark_sent(source_message_id, target_chat_id):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT OR IGNORE INTO sent_messages (source_message_id, target_chat_id)
            VALUES (?, ?)
            """,
            (source_message_id, target_chat_id)
        )


def clear_sent_records():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM sent_messages")


def add_source_chat(chat_id: int, title: str):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO source_chats (chat_id, title) VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title
            """,
            (chat_id, title)
        )


def remove_source_chat(chat_id: int):
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM source_chats WHERE chat_id = ?", (chat_id,))


def list_source_chats():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT chat_id, title FROM source_chats ORDER BY added_at DESC")
        return c.fetchall()


def is_source_chat(chat_id: int) -> bool:
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM source_chats WHERE chat_id = ?", (chat_id,))
        return c.fetchone() is not None


# -----------------------------
# Clients
# -----------------------------
userbot = None
bot = telebot.TeleBot(BOT_TOKEN)
admin_states = {}
login_data = {}


def is_admin(user_id: int) -> bool:
    if ADMIN_ID == 0:
        return True
    return user_id == ADMIN_ID


def build_temp_client(api_id: int, api_hash: str) -> Client:
    return Client(
        name=":memory:",
        api_id=api_id,
        api_hash=api_hash,
        in_memory=True,
        workers=2,
    )


async def forward_from_saved_message(msg: Message) -> bool:
    global userbot
    if userbot is None:
        return False
    target_raw = get_setting("target_chat_id", "")
    if not target_raw:
        return False
    if str(get_setting("auto_forward", "false")).lower() != "true":
        return False
    if not msg.media:
        return False
    if already_sent(msg.id):
        return False

    target_id = int(target_raw)
    source_chat_id = msg.chat.id if msg.chat else None
    if source_chat_id is None:
        return False

    try:
        await userbot.copy_message(
            chat_id=target_id,
            from_chat_id=source_chat_id,
            message_id=msg.id
        )
        mark_sent(msg.id, target_id)
        logger.info(f"Forwarded saved media {msg.id} -> {target_id}")
        return True
    except RPCError as e:
        logger.error(f"Failed forwarding {msg.id} to {target_id}: {e}")
        return False


async def saved_media_listener(client: Client, message: Message):
    try:
        # Process media from configured source chats (including Saved Messages if added)
        if message.chat and is_source_chat(message.chat.id):
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
        userbot.add_handler(MessageHandler(saved_media_listener, filters.me & filters.media))
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
        (
            "Commands:\n"
            "/settarget <chat_id>\n"
            "/showtarget\n"
            "/setapiid <id>\n"
            "/setapihash <hash>\n"
            "/setsession <string_session>\n"
            "/login  (generate session string from phone/OTP)\n"
            "/startuserbot  (start/reload userbot now)\n"
            "/showapi\n"
            "/clearapi\n"
            "/autoon\n"
            "/autooff\n"
            "/status\n"
            "/listgroups [page]\n"
            "/setsource <chat_id>\n"
            "/delsource <chat_id>\n"
            "/showsources\n"
            "/sendlast <N>  (send last N media from Saved Messages)\n"
            "/resendlast <N> (force resend, ignore sent-history)\n"
            "/clearsent (clear sent-history)\n"
            "/showmedia <N> (show last N media in Saved Messages)\n"
        )
    )


@bot.message_handler(commands=["settarget"])
def cmd_settarget(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) != 2:
        bot.reply_to(message, "Usage: /settarget -1001234567890")
        return
    try:
        target_id = int(parts[1])
        set_setting("target_chat_id", target_id)
        bot.reply_to(message, f"Target set to: `{target_id}`", parse_mode="Markdown")
    except ValueError:
        bot.reply_to(message, "Invalid target id.")


@bot.message_handler(commands=["showtarget"])
def cmd_showtarget(message):
    if not is_admin(message.from_user.id):
        return
    t = get_setting("target_chat_id", "Not set")
    bot.reply_to(message, f"Current target: `{t}`", parse_mode="Markdown")


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
        bot.reply_to(message, "API ID saved. Restart service to apply.")
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
    bot.reply_to(message, "API Hash saved. Restart service to apply.")


@bot.message_handler(commands=["setsession"])
def cmd_setsession(message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) != 2 or not parts[1].strip():
        bot.reply_to(message, "Usage: /setsession <string_session>")
        return
    set_setting("user_session_string", parts[1].strip())
    bot.reply_to(message, "User session saved. Restart service to apply.")


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
        parse_mode="Markdown"
    )


@bot.message_handler(commands=["clearapi"])
def cmd_clearapi(message):
    if not is_admin(message.from_user.id):
        return
    set_setting("api_id", "")
    set_setting("api_hash", "")
    set_setting("user_session_string", "")
    bot.reply_to(message, "Stored API ID/API Hash/Session removed. Restart service to apply.")


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
def login_state_handler(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    if not state:
        return

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
    t = get_setting("target_chat_id", "Not set")
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
    for i, (cid, title) in enumerate(rows, start=1):
        lines.append(f"{i}. {title or cid} | id={cid}")
    text = "\n".join(lines)
    if len(text) > 3500:
        text = text[:3500] + "\n..."
    bot.reply_to(message, f"<pre>{text}</pre>", parse_mode="HTML")


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
        target_raw = get_setting("target_chat_id", "")
        if not target_raw:
            bot.reply_to(message, "Set target first with /settarget")
            return
        target_id = int(target_raw)
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
                mark_sent(m.id, target_id)
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
        target_raw = get_setting("target_chat_id", "")
        if not target_raw:
            bot.reply_to(message, "Set target first with /settarget")
            return
        target_id = int(target_raw)
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
                mark_sent(m.id, target_id)
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
app = Flask(__name__)


@app.route("/")
def health():
    return "saved_to_target_userbot running", 200


def run_web():
    app.run(host="0.0.0.0", port=PORT)


def keep_alive_worker():
    while True:
        try:
            url = KEEP_ALIVE_URL.strip()
            if url:
                requests.get(url, timeout=12)
        except Exception as e:
            logger.warning(f"Keep-alive ping failed: {e}")
        finally:
            # 10 min
            import time
            time.sleep(600)


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

    # run telebot in thread
    threading.Thread(target=bot.infinity_polling, daemon=True).start()
    logger.info("Admin bot polling started")

    await idle()
    if userbot is not None:
        await userbot.stop()


if __name__ == "__main__":
    threading.Thread(target=run_web, daemon=True).start()
    if KEEP_ALIVE_URL:
        threading.Thread(target=keep_alive_worker, daemon=True).start()
    loop.run_until_complete(start_async())
