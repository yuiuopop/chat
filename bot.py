import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
import psycopg2
from psycopg2 import pool
import threading
import math
import os
import os

# ================= Configuration =================
# Prioritizes system environment variables (hosting sites) over .env defaults
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [int(i.strip()) for i in ADMIN_IDS_RAW.split(",") if i.strip()]

# Basic validation
if not BOT_TOKEN or not DATABASE_URL:
    print("CRITICAL: BOT_TOKEN or DATABASE_URL not found in environment!")
    # In some hosting environments, we want to fail fast

# Economics
FREE_STARTING_POINTS = 10
MEDIA_COST = 1
REFERRAL_BONUS = 2

upload_batches = {}
upload_lock = threading.Lock()
admin_active_category = {}

# ================= Database =================
# Connection pool for thread-safe PostgreSQL access
db_pool = None

def get_db_pool():
    global db_pool
    if db_pool is None:
        db_pool = psycopg2.pool.SimpleConnectionPool(1, 20, dsn=DATABASE_URL)
    return db_pool

def get_db():
    return get_db_pool().getconn()

def release_db(conn):
    get_db_pool().putconn(conn)

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                points INTEGER DEFAULT 10,
                referred_by BIGINT,
                join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                media_received INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                req_referrals INTEGER DEFAULT 0,
                is_hidden INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS media (
                id SERIAL PRIMARY KEY,
                file_id TEXT,
                media_type TEXT,
                file_unique_id TEXT,
                category_id INTEGER REFERENCES categories(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_category_stats (
                user_id BIGINT,
                category_id INTEGER,
                count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, category_id)
            )
        ''')
        cursor.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')
        
        # Ensure default settings
        cursor.execute("INSERT INTO settings (key, value) VALUES ('start_message', %s) ON CONFLICT (key) DO NOTHING", ("Welcome to the Media Bot! 📺\nUse the menu below to navigate.",))
        cursor.execute("INSERT INTO settings (key, value) VALUES ('media_caption', %s) ON CONFLICT (key) DO NOTHING", ("Enjoy this from {cat_name}! 🍿\nRemaining points: {points}",))

        # Seed Default Category
        cursor.execute("SELECT id FROM categories LIMIT 1")
        if not cursor.fetchone():
            cursor.execute("INSERT INTO categories (name) VALUES (%s)", ('📺 Watch Media',))
            
        conn.commit()
    finally:
        release_db(conn)

def add_user(user_id, username, starting_points, referred_by=None):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users (user_id, username, points, referred_by) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING", (user_id, username, starting_points, referred_by))
        conn.commit()
        return cursor.rowcount > 0
    except: return False
    finally: release_db(conn)

def get_user(user_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, username, points, referred_by, join_date::DATE FROM users WHERE user_id = %s", (user_id,))
        return cursor.fetchone()
    finally: release_db(conn)

def update_points(user_id, delta):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET points = points + %s WHERE user_id = %s", (delta, user_id))
        conn.commit()
    finally: release_db(conn)

def get_points(user_id):
    user = get_user(user_id)
    return user[2] if user else 0
    
def update_media_received(user_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET media_received = media_received + 1 WHERE user_id = %s", (user_id,))
        conn.commit()
    finally: release_db(conn)

def get_setting(key, default=None):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = %s", (key,))
        res = cursor.fetchone()
        return res[0] if res else default
    finally: release_db(conn)

def set_setting(key, value):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))
        conn.commit()
    finally: release_db(conn)

def increment_user_category_stat(user_id, category_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO user_category_stats (user_id, category_id, count) 
            VALUES (%s, %s, 1)
            ON CONFLICT(user_id, category_id) DO UPDATE SET count = user_category_stats.count + 1
        ''', (user_id, category_id))
        conn.commit()
    finally: release_db(conn)

def get_user_list_page(limit, offset):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT u.user_id, u.username, (SELECT COUNT(*) FROM users WHERE referred_by = u.user_id) as ref_count 
            FROM users u 
            ORDER BY u.join_date DESC 
            LIMIT %s OFFSET %s
        ''', (limit, offset))
        return cursor.fetchall()
    finally: release_db(conn)

def get_user_detail(user_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, username, points, referred_by, join_date, media_received FROM users WHERE user_id = %s", (user_id,))
        return cursor.fetchone()
    finally: release_db(conn)

def get_user_cat_breakdown(user_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT c.name, s.count 
            FROM user_category_stats s
            JOIN categories c ON s.category_id = c.id
            WHERE s.user_id = %s
        ''', (user_id,))
        return cursor.fetchall()
    finally: release_db(conn)

def get_total_referrals(user_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users WHERE referred_by = %s", (user_id,))
        res = cursor.fetchone()
        return res[0] if res else 0
    finally: release_db(conn)

def get_categories():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, is_hidden FROM categories")
        return cursor.fetchall()
    finally: release_db(conn)

def get_visible_categories():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name FROM categories WHERE is_hidden = 0")
        return cursor.fetchall()
    finally: release_db(conn)

def toggle_category_visibility(cat_id, hide):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE categories SET is_hidden = %s WHERE id = %s", (1 if hide else 0, cat_id))
        conn.commit()
    finally: release_db(conn)

def delete_category_db(cat_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM media WHERE category_id = %s", (cat_id,))
        cursor.execute("DELETE FROM categories WHERE id = %s", (cat_id,))
        conn.commit()
    finally: release_db(conn)

def get_category_req(cat_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT req_referrals FROM categories WHERE id = %s", (cat_id,))
        res = cursor.fetchone()
        return res[0] if res and res[0] else 0
    except: return 0
    finally: release_db(conn)

def update_category_req(cat_id, limit):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE categories SET req_referrals = %s WHERE id = %s", (limit, cat_id))
        conn.commit()
    finally: release_db(conn)

def add_category(name):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO categories (name) VALUES (%s) RETURNING id", (name,))
        cat_id = cursor.fetchone()[0]
        conn.commit()
        return cat_id
    except: return None
    finally: release_db(conn)

def add_media(file_id, media_type, file_unique_id=None, category_id=1):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO media (file_id, media_type, file_unique_id, category_id) VALUES (%s, %s, %s, %s) RETURNING id", (file_id, media_type, file_unique_id, category_id))
        media_id = cursor.fetchone()[0]
        conn.commit()
        return media_id
    finally: release_db(conn)

def check_duplicate_media(file_unique_id):
    if not file_unique_id: return False
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM media WHERE file_unique_id = %s", (file_unique_id,))
        return cursor.fetchone() is not None
    finally: release_db(conn)

def get_random_media(category_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, file_id, media_type FROM media WHERE category_id = %s ORDER BY RANDOM() LIMIT 1", (category_id,))
        return cursor.fetchone()
    finally: release_db(conn)

def delete_media(media_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM media WHERE id = %s", (media_id,))
        success = cursor.rowcount > 0
        conn.commit()
        return success
    finally: release_db(conn)

def get_media_page(category_id, limit, offset):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, media_type FROM media WHERE category_id = %s ORDER BY id DESC LIMIT %s OFFSET %s", (category_id, limit, offset))
        return cursor.fetchall()
    finally: release_db(conn)

def get_media_by_id(media_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT file_id, media_type FROM media WHERE id = %s", (media_id,))
        return cursor.fetchone()
    finally: release_db(conn)

def wipe_category(category_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM media WHERE category_id = %s", (category_id,))
        conn.commit()
    finally: release_db(conn)

def get_cat_stats(category_id):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM media WHERE category_id = %s", (category_id,))
        res = cursor.fetchone()
        return res[0] if res else 0
    finally: release_db(conn)

def get_stats():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        users_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM media")
        media_count = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(media_received) FROM users")
        ts = cursor.fetchone()[0]
        return users_count, media_count, int(ts) if ts else 0
    finally: release_db(conn)


# ================= UI & Keyboards =================
def get_main_keyboard(admin=False):
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    categories = get_visible_categories()
    
    # Generate buttons for all categories dynamically
    cat_buttons = [KeyboardButton(c[1]) for c in categories]
    markup.add(*cat_buttons)
    
    markup.add(KeyboardButton("🔗 Referral"), KeyboardButton("💰 Balance"))
    if admin:
        markup.add(KeyboardButton("👑 Admin Panel"))
    return markup

def get_admin_panel_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("📊 User Stats", callback_data="admin_stats"))
    markup.row(InlineKeyboardButton("🛠️ Manage Categories", callback_data="admin_manage_categories"), 
               InlineKeyboardButton("🏷️ Set Upload Category", callback_data="admin_setcat"))
    markup.row(InlineKeyboardButton("📁 Manage Media", callback_data="manage_cats"),
               InlineKeyboardButton("⚙️ Category Limits", callback_data="admin_limits"))
    markup.add(InlineKeyboardButton("🛠️ Tools", callback_data="admin_tools"))
    return markup

def generate_divisions_markup(cat_id):
    markup = InlineKeyboardMarkup()
    total_media = get_cat_stats(cat_id)
    
    if total_media == 0:
        markup.add(InlineKeyboardButton("No media in this category.", callback_data="ignore"))
        markup.add(InlineKeyboardButton("🔙 Back to Categories", callback_data="manage_cats"))
        return markup
        
    chunk_size = 100
    total_chunks = math.ceil(total_media / chunk_size)
    
    for i in range(total_chunks):
        start_count = (i * chunk_size) + 1
        end_count = min((i + 1) * chunk_size, total_media)
        target_page = (i * chunk_size) // 5 
        btn_text = f"📂 Media {start_count} - {end_count}"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"manage_page_{cat_id}_{target_page}"))
        
    markup.add(InlineKeyboardButton("🚨 Wipe Category 🚨", callback_data=f"wipe_media_init_{cat_id}"))
    markup.add(InlineKeyboardButton("🔙 Back to Categories", callback_data="manage_cats"))
    return markup

def generate_manage_markup(cat_id, page):
    markup = InlineKeyboardMarkup()
    total_media = get_cat_stats(cat_id)
    
    limit = 5
    offset = page * limit
    total_pages = math.ceil(total_media / limit) if total_media > 0 else 1
    
    media_items = get_media_page(cat_id, limit, offset)
    
    if media_items:
        for m_id, m_type in media_items:
            preview_btn = InlineKeyboardButton(f"👀 Preview [{m_type.upper()} {m_id}]", callback_data=f"preview_{m_id}_{cat_id}_{page}")
            delete_btn = InlineKeyboardButton(f"❌ Delete", callback_data=f"delmedia_{m_id}_{cat_id}_{page}")
            markup.add(preview_btn, delete_btn)
            
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"manage_page_{cat_id}_{page-1}"))
        else:
            nav_buttons.append(InlineKeyboardButton(" ", callback_data="ignore"))
            
        nav_buttons.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data="ignore"))
        
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f"manage_page_{cat_id}_{page+1}"))
        else:
            nav_buttons.append(InlineKeyboardButton(" ", callback_data="ignore"))
            
        markup.row(*nav_buttons)
        markup.add(InlineKeyboardButton("🔙 Back to Folders", callback_data=f"manage_divs_{cat_id}"))
        markup.add(InlineKeyboardButton("🚨 Wipe Category 🚨", callback_data=f"wipe_media_init_{cat_id}"))
    else:
        markup.add(InlineKeyboardButton("No media found.", callback_data="ignore"))
        markup.add(InlineKeyboardButton("🔙 Back to Folders", callback_data=f"manage_divs_{cat_id}"))
        
    return markup

# ================= Bot Initialization =================
if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
    print("[ERROR] Please set your BOT_TOKEN in the configuration section at the top of the file.")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN)

def is_admin(user_id):
    return user_id in ADMIN_IDS

# ================= Primary Handlers =================
@bot.message_handler(commands=['start'])
def handle_start(message):
    user_id = message.from_user.id
    username = message.from_user.username
    admin_mode = is_admin(user_id)
    
    args = message.text.split()
    referred_by = None
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])
        if referrer_id != user_id: referred_by = referrer_id

    is_new = add_user(user_id, username, FREE_STARTING_POINTS, referred_by)
    if is_new and referred_by:
        update_points(referred_by, REFERRAL_BONUS)
        try: bot.send_message(referred_by, f"🎉 Someone joined using your referral link! You earned {REFERRAL_BONUS} points.")
        except: pass
            
    if admin_mode:
        bot.reply_to(message, "👑 **Admin Access Granted!** Welcome to your media bot.", reply_markup=get_main_keyboard(admin=True), parse_mode="Markdown")
    else:
        start_msg = get_setting('start_message', "Welcome to the Media Bot! 📺\nUse the menu below to navigate.")
        bot.reply_to(message, start_msg, reply_markup=get_main_keyboard())

@bot.message_handler(commands=['newcategory'])
def handle_newcategory(message):
    if not is_admin(message.from_user.id): return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return bot.reply_to(message, "Usage: `/newcategory <Name>`\nEx: `/newcategory 🏎️ Cars`", parse_mode="Markdown")
        
    cat_name = args[1].strip()
    res = add_category(cat_name)
    if res:
        bot.reply_to(message, f"✅ Category `{cat_name}` created!\nYour keyboard has been updated.", reply_markup=get_main_keyboard(admin=True), parse_mode="Markdown")
    else:
        bot.reply_to(message, "❌ A category with that name already exists.")

def flush_upload_batch(chat_id):
    with upload_lock:
        if chat_id in upload_batches:
            saved = upload_batches[chat_id]['saved']
            dupes = upload_batches[chat_id]['dupes']
            cat_name = upload_batches[chat_id]['cat_name']
            del upload_batches[chat_id]
            try:
                if dupes > 0: bot.send_message(chat_id, f"✅ Added {saved} media item(s) to *{cat_name}*.\n⚠️ Ignored {dupes} duplicate(s)!", parse_mode="Markdown")
                else: bot.send_message(chat_id, f"✅ Added {saved} media item(s) to *{cat_name}*!", parse_mode="Markdown")
            except: pass

@bot.message_handler(content_types=['photo', 'video', 'document'])
def handle_media_upload(message):
    if not is_admin(message.from_user.id): return
        
    active_cat_id = admin_active_category.get(message.from_user.id)
    if not active_cat_id:
        bot.reply_to(message, "❌ **Upload Rejected** \nYou must set an Active Upload Category first via the Admin Panel.", parse_mode="Markdown")
        return
        
    if message.photo:
        file_id = message.photo[-1].file_id
        file_unique_id = message.photo[-1].file_unique_id
        media_type = 'photo'
    elif message.video:
        file_id = message.video.file_id
        file_unique_id = message.video.file_unique_id
        media_type = 'video'
    elif message.document:
        file_id = message.document.file_id
        file_unique_id = message.document.file_unique_id
        media_type = 'document'
    else: return
        
    with upload_lock:
        if message.chat.id not in upload_batches:
            cats = get_categories()
            cat_name = next((c[1] for c in cats if c[0] == active_cat_id), "Unknown")
            upload_batches[message.chat.id] = {'saved': 0, 'dupes': 0, 'timer': None, 'cat_name': cat_name}
        
        batch = upload_batches[message.chat.id]
        if check_duplicate_media(file_unique_id): batch['dupes'] += 1
        else:
            add_media(file_id, media_type, file_unique_id, category_id=active_cat_id)
            batch['saved'] += 1
            
        if batch['timer']: batch['timer'].cancel()
        batch['timer'] = threading.Timer(1.5, flush_upload_batch, args=(message.chat.id,))
        batch['timer'].start()

@bot.message_handler(func=lambda message: True)
def handle_text(message):
    user_id = message.from_user.id
    admin_mode = is_admin(user_id)
    text = message.text
    
    if text == "💰 Balance":
        user = get_user(user_id)
        if not user: return bot.reply_to(message, "Please type /start first.")
        bot.reply_to(message, f"👤 **Account Information**\n\n💰 **Balance:** {user[2]} points\n📅 **Date of Joining:** {user[4]}", parse_mode="Markdown")
        return
        
    if text == "🔗 Referral":
        points = get_points(user_id)
        referral_link = f"https://t.me/{bot.get_me().username}?start={user_id}"
        bot.reply_to(message, f"⭐️ **Your Stats**\nCurrent Points: {points}\n\n🔗 **Your Referral Link**\n`{referral_link}`\n\nFor every friend who joins, you get {REFERRAL_BONUS} extra points!", parse_mode="Markdown")
        return
        
    if text == "👑 Admin Panel" and admin_mode:
        bot.reply_to(message, "👑 **Admin Control Panel**\nSelect an operation below:", reply_markup=get_admin_panel_markup(), parse_mode="Markdown")
        return
        
    # Check if text targets a Media Category dynamically
    categories = get_categories()
    for cat_id, cat_name in categories:
        if text == cat_name:
            process_media_request(message, cat_id, cat_name, admin_mode)
            return

def process_media_request(message, cat_id, cat_name, admin_mode):
    user_id = message.from_user.id
    points = get_points(user_id)
    
    if not admin_mode:
        req_refs = get_category_req(cat_id)
        if req_refs > 0:
            actual_refs = get_total_referrals(user_id)
            if actual_refs < req_refs:
                return bot.reply_to(message, f"🔒 **Access Denied!**\n\nThe `{cat_name}` category requires at least **{req_refs} successful referrals** to unlock.\nYou currently have {actual_refs} referrals.\n\nUse your `🔗 Referral` link to invite more friends!", parse_mode="Markdown")
                
        if points < MEDIA_COST:
            return bot.reply_to(message, f"❌ You don't have enough points left!\nClick '🔗 Referral' to get your invite link.")
        
    media = get_random_media(cat_id)
    if not media:
        return bot.reply_to(message, f"Currently there is no media available in {cat_name}. Check back later!")
        
    _id, file_id, media_type = media
    
    if not admin_mode:
        update_points(user_id, -MEDIA_COST)
        update_media_received(user_id)
        increment_user_category_stat(user_id, cat_id)
        new_points = points - MEDIA_COST
        
        caption_tmpl = get_setting('media_caption', "Enjoy this from {cat_name}! 🍿\nRemaining points: {points}")
        caption_text = caption_tmpl.replace("{cat_name}", cat_name).replace("{points}", str(new_points))
    else:
        caption_text = f"🍿 {cat_name}\n[👑 Admin View: Unlimited]\n[ID: {_id}]"
    
    try:
        if media_type == 'photo': bot.send_photo(user_id, file_id, caption=caption_text)
        elif media_type == 'video': bot.send_video(user_id, file_id, caption=caption_text)
        else: bot.send_document(user_id, file_id, caption=caption_text)
    except:
        if not admin_mode: update_points(user_id, MEDIA_COST)

# ================= Admin Callbacks =================

@bot.callback_query_handler(func=lambda call: call.data == "admin_tools")
def cb_admin_tools(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("✍️ Edit Start Message", callback_data="tool_edit_start"))
    markup.add(InlineKeyboardButton("🎞️ Edit Media Caption", callback_data="tool_edit_caption"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_back"))
    bot.edit_message_text("🛠️ **Admin Tools**\nCustomize your bot's automated messages:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "tool_edit_start")
def cb_edit_start_init(call):
    if not is_admin(call.from_user.id): return
    msg = bot.send_message(call.message.chat.id, "✍️ Please send the new **Start Message** text.\n\n_Tip: Use Markdown for bold/italics!_")
    bot.register_next_step_handler(msg, process_start_msg_edit)
    bot.answer_callback_query(call.id)

def process_start_msg_edit(message):
    if not is_admin(message.from_user.id): return
    new_text = message.text
    set_setting('start_message', new_text)
    bot.reply_to(message, "✅ **Start Message Updated!**", parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data == "tool_edit_caption")
def cb_edit_caption_init(call):
    if not is_admin(call.from_user.id): return
    msg = bot.send_message(call.message.chat.id, "🎞️ Please send the new **Media Caption Template**.\n\n"
                                                  "Available Placeholders:\n"
                                                  "`{cat_name}` - Category Name\n"
                                                  "`{points}` - User Points Remaining\n\n"
                                                  "Example: _Here is your {cat_name}! You have {points} left._")
    bot.register_next_step_handler(msg, process_caption_edit)
    bot.answer_callback_query(call.id)

def process_caption_edit(message):
    if not is_admin(message.from_user.id): return
    new_text = message.text
    set_setting('media_caption', new_text)
    bot.reply_to(message, "✅ **Media Caption Updated!**", parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data == "admin_stats")
def cb_admin_stats(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    users_count, media_count, total_received = get_stats()
    text = (f"📊 **Bot Stats Dashboard**\n\n👥 **Total Registered Users:** {users_count}\n📦 **Total Media Uploaded:** {media_count}\n📤 **Total Media Distributed:** {total_received}")
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("👥 User List", callback_data="admin_user_list_0"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_back"))
    
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    except: pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_user_list_"))
def cb_admin_user_list(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    page = int(call.data.split('_')[3])
    limit = 10
    offset = page * limit
    
    users = get_user_list_page(limit, offset)
    users_count, _, _ = get_stats()
    total_pages = math.ceil(users_count / limit) if users_count > 0 else 1
    
    markup = InlineKeyboardMarkup()
    if users:
        for u_id, u_name, ref_count in users:
            display_name = u_name if u_name else f"ID:{u_id}"
            markup.add(InlineKeyboardButton(f"{display_name} ({ref_count} refs)", callback_data=f"user_detail_{u_id}_{page}"))
            
        nav_btns = []
        if page > 0: nav_btns.append(InlineKeyboardButton("⬅️", callback_data=f"admin_user_list_{page-1}"))
        nav_btns.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="ignore"))
        if page < total_pages - 1: nav_btns.append(InlineKeyboardButton("➡️", callback_data=f"admin_user_list_{page+1}"))
        markup.row(*nav_btns)
        
    markup.add(InlineKeyboardButton("🔙 Back to Stats", callback_data="admin_stats"))
    
    bot.edit_message_text("👥 **User Directory**\nSelect a user to view their full profile:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("user_detail_"))
def cb_admin_user_detail(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    pts = call.data.split('_')
    user_id = int(pts[2])
    back_page = int(pts[3])
    
    user = get_user_detail(user_id)
    if not user: return bot.answer_callback_query(call.id, "User not found")
    
    u_id, u_name, points, ref_by, join_date, total_media = user
    ref_count = get_total_referrals(user_id)
    breakdown = get_user_cat_breakdown(user_id)
    
    text = (f"👤 **User Profile: {u_name if u_name else 'N/A'}**\n\n"
            f"🆔 **ID:** `{u_id}`\n"
            f"📅 **Joined:** {join_date}\n"
            f"💰 **Points Balance:** {points}\n"
            f"👥 **Total Referrals:** {ref_count}\n"
            f"📦 **Total Media Extracted:** {total_media}\n\n"
            f"📑 **Category Breakdown:**\n")
    
    if breakdown:
        for cat_name, count in breakdown:
            text += f"- {cat_name}: {count}\n"
    else:
        text += "_No specific category data yet._"
        
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back to List", callback_data=f"admin_user_list_{back_page}"))
    
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_manage_categories")
def cb_manage_cats_main(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("➕ Create New Category", callback_data="admin_newcat"))
    markup.add(InlineKeyboardButton("✏️ Edit/Hide/Delete Categories", callback_data="admin_edit_cats_list"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_back"))
    bot.edit_message_text("🛠️ **Category Management**\nWhat would you like to do?", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_edit_cats_list")
def cb_edit_cats_list(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cats = get_categories()
    markup = InlineKeyboardMarkup()
    for c_id, c_name, c_hidden in cats:
        status = "👻 " if c_hidden else ""
        markup.add(InlineKeyboardButton(f"{status}{c_name}", callback_data=f"edit_cat_opts_{c_id}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_manage_categories"))
    bot.edit_message_text("✏️ **Select Category to Edit**\nGhost icon (👻) means category is hidden from users.", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_cat_opts_"))
def cb_edit_cat_opts(call):
    cat_id = int(call.data.split('_')[3])
    cats = get_categories()
    cat = next((c for c in cats if c[0] == cat_id), None)
    if not cat: return bot.answer_callback_query(call.id, "Category not found.")
    
    c_id, c_name, c_hidden = cat
    markup = InlineKeyboardMarkup(row_width=2)
    if c_hidden:
        markup.add(InlineKeyboardButton("👁️ Unhide Category", callback_data=f"toggle_hide_{c_id}_0"))
    else:
        markup.add(InlineKeyboardButton("👻 Hide Category", callback_data=f"toggle_hide_{c_id}_1"))
    
    markup.add(InlineKeyboardButton("🗑️ Delete Category", callback_data=f"del_cat_init_{c_id}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_edit_cats_list"))
    
    status_text = "HIDDEN" if c_hidden else "VISIBLE"
    bot.edit_message_text(f"✏️ **Editing: {c_name}**\nCurrent Status: `{status_text}`", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("toggle_hide_"))
def cb_toggle_hide(call):
    pts = call.data.split('_')
    cat_id = int(pts[2])
    hide = int(pts[3]) == 1
    toggle_category_visibility(cat_id, hide)
    bot.answer_callback_query(call.id, "Visibility updated!")
    cb_edit_cats_list(call)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_cat_init_"))
def cb_del_cat_init(call):
    cat_id = int(call.data.split('_')[3])
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("⚠️ YES, DELETE AND WIPE MEDIA ⚠️", callback_data=f"del_cat_confirm_{cat_id}"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"edit_cat_opts_{cat_id}"))
    bot.edit_message_text("🚨 **FINAL WARNING!** 🚨\nDeleting this category will permanently erase it AND ALL MEDIA INSIDE IT. This cannot be undone.", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("del_cat_confirm_"))
def cb_del_cat_confirm(call):
    cat_id = int(call.data.split('_')[3])
    delete_category_db(cat_id)
    bot.answer_callback_query(call.id, "Category and media deleted!")
    cb_edit_cats_list(call)

@bot.callback_query_handler(func=lambda call: call.data == "admin_newcat")
def cb_admin_newcat(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "✏️ **To create a new category, type:**\n`/newcategory <Name>`\n_Example: /newcategory 🏎️ Cars_", parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data == "admin_setcat")
def cb_admin_setcat(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cats = get_categories()
    if not cats: return bot.answer_callback_query(call.id, "No categories exist!")
    
    markup = InlineKeyboardMarkup()
    for c_id, c_name in cats: markup.add(InlineKeyboardButton(c_name, callback_data=f"setactive_{c_id}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_back"))
    
    bot.edit_message_text("🏷️ **Select Upload Category**\n\nAll media you send to the bot will be automatically stored there:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("setactive_"))
def cb_setactive(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cat_id = int(call.data.split('_')[1])
    cats = get_categories()
    cat_name = next((c[1] for c in cats if c[0] == cat_id), "Unknown")
    
    admin_active_category[call.from_user.id] = cat_id
    bot.edit_message_text(f"✅ **Active Category Set: {cat_name}**\n\nFeel free to forward bulk media to the bot now. It will instantly be cataloged under {cat_name}.", call.message.chat.id, call.message.message_id, reply_markup=get_admin_panel_markup(), parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_limits")
def cb_admin_limits(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cats = get_categories()
    markup = InlineKeyboardMarkup()
    for c_id, c_name in cats: 
        reqs = get_category_req(c_id)
        markup.add(InlineKeyboardButton(f"{c_name} (Req: {reqs} refs)", callback_data=f"manage_req_{c_id}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_back"))
    bot.edit_message_text("⚙️ **Category Limits**\nSelect a category to change its referral requirements:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("manage_req_"))
def cb_manage_req(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cat_id = int(call.data.split('_')[2])
    cats = get_categories()
    cat_name = next((c[1] for c in cats if c[0] == cat_id), "Unknown")
    
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, f"⚙️ To change the referral requirement for **{cat_name}**, type:\n`/setreq {cat_id} <limit>`\n_Example: /setreq {cat_id} 5_", parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data == "admin_panel_back")
def cb_panel_back(call):
    if not is_admin(call.from_user.id): return
    bot.edit_message_text("👑 **Admin Control Panel**\nSelect an operation below:", call.message.chat.id, call.message.message_id, reply_markup=get_admin_panel_markup(), parse_mode="Markdown")
    bot.answer_callback_query(call.id)

# --- Media Management via Categories ---
@bot.callback_query_handler(func=lambda call: call.data == "manage_cats")
def cb_manage_cats(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cats = get_categories()
    markup = InlineKeyboardMarkup()
    for c_id, c_name in cats: 
        m_count = get_cat_stats(c_id)
        markup.add(InlineKeyboardButton(f"{c_name} ({m_count} items)", callback_data=f"manage_divs_{c_id}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="admin_panel_back"))
    
    bot.edit_message_text("📁 **Manage Categories**\nSelect a category to explore its media:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("manage_divs_"))
def cb_manage_divs(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cat_id = int(call.data.split('_')[2])
    
    markup = generate_divisions_markup(cat_id)
    bot.edit_message_text("📁 **Category Folders**\nSelect a chunk to manage:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("manage_page_"))
def cb_manage_page(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    pts = call.data.split('_')
    cat_id = int(pts[2])
    page = int(pts[3])
    
    markup = generate_manage_markup(cat_id, page)
    try: bot.edit_message_text("📁 **Media Explorer**", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    except: pass
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("preview_"))
def cb_preview(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    media_id = call.data.split('_')[1]
    media = get_media_by_id(media_id)
    if not media: return bot.answer_callback_query(call.id, "Media no longer exists.")
    file_id, media_type = media
    bot.answer_callback_query(call.id, "Sending preview...")
    
    try:
        msg = f"👀 **Preview ID: {media_id}**"
        if media_type == 'photo': bot.send_photo(call.message.chat.id, file_id, caption=msg, parse_mode="Markdown")
        elif media_type == 'video': bot.send_video(call.message.chat.id, file_id, caption=msg, parse_mode="Markdown")
        else: bot.send_document(call.message.chat.id, file_id, caption=msg, parse_mode="Markdown")
    except: bot.send_message(call.message.chat.id, f"❌ Failed to preview Media ID {media_id}.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("delmedia_"))
def cb_delmedia(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    pts = call.data.split('_')
    media_id = pts[1]
    cat_id = int(pts[2])
    page = int(pts[3])
    
    if delete_media(media_id): bot.answer_callback_query(call.id, f"✅ Deleted!")
    else: bot.answer_callback_query(call.id, "Not found.")
    
    markup = generate_manage_markup(cat_id, page)
    try: bot.edit_message_text("📁 **Media Explorer**", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    except: pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("wipe_media_init_"))
def cb_wipe_init(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cat_id = int(call.data.split('_')[3])
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("⚠️ YES, DELETE EVERYTHING ⚠️", callback_data=f"wipe_media_confirm_{cat_id}"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"manage_divs_{cat_id}"))
    bot.edit_message_text("🚨 **WARNING!** 🚨\nAre you sure you want to permanently empty this entire category?", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("wipe_media_confirm_"))
def cb_wipe_confirm(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    cat_id = int(call.data.split('_')[3])
    
    wipe_category(cat_id)
    bot.answer_callback_query(call.id, "Wiped!")
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back to Categories", callback_data="manage_cats"))
    bot.edit_message_text("✅ Category emptied successfully.", call.message.chat.id, call.message.message_id, reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "ignore")
def cb_ignore(call): bot.answer_callback_query(call.id)

@bot.message_handler(commands=['setreq'])
def handle_setreq(message):
    if not is_admin(message.from_user.id): return
    args = message.text.split()
    if len(args) != 3:
        return bot.reply_to(message, "Usage: `/setreq <CategoryID> <Limit>`\nYou can get the Category ID from the Category Limits menu.", parse_mode="Markdown")
    try:
        cat_id = int(args[1])
        limit = int(args[2])
        update_category_req(cat_id, limit)
        bot.reply_to(message, f"✅ Done! The category now requires **{limit}** referrals to access.", parse_mode="Markdown")
    except:
        bot.reply_to(message, "Invalid number.")

# ================= Execution =================
if __name__ == "__main__":
    init_db()
    print("Database initialized.")
    print("Bot is polling...")
    try: bot.infinity_polling()
    except Exception as e: print(f"Error while polling: {e}")
