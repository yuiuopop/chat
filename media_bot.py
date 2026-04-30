import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
import sqlite3
import threading
import math

# ================= Configuration =================
# Replace with your actual Bot Token from BotFather
BOT_TOKEN = "8756272091:AAGEvJTyq0jPh1aFzDeYhvZ39c1D-TGCEok"

# List of admin Telegram User IDs
ADMIN_IDS = [8305774350] 

# Economics
FREE_STARTING_POINTS = 10
MEDIA_COST = 1
REFERRAL_BONUS = 2

upload_batches = {}
upload_lock = threading.Lock()
admin_active_category = {}

# ================= Database =================
local = threading.local()

def get_db():
    if not hasattr(local, 'db'):
        local.db = sqlite3.connect('media_bot.sqlite', check_same_thread=False)
    return local.db

def init_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            points INTEGER DEFAULT 10,
            referred_by INTEGER,
            join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            media_received INTEGER DEFAULT 0
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT,
            media_type TEXT,
            file_unique_id TEXT,
            category_id INTEGER,
            FOREIGN KEY(category_id) REFERENCES categories(id)
        )
    ''')
    try: cursor.execute("ALTER TABLE users ADD COLUMN join_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    except: pass
    try: cursor.execute("ALTER TABLE users ADD COLUMN media_received INTEGER DEFAULT 0")
    except: pass
    try: cursor.execute("ALTER TABLE media ADD COLUMN file_unique_id TEXT")
    except: pass
    try: cursor.execute("ALTER TABLE media ADD COLUMN category_id INTEGER DEFAULT 1")
    except: pass
    try: cursor.execute("ALTER TABLE categories ADD COLUMN req_referrals INTEGER DEFAULT 0")
    except: pass

    # Seed Default Category to prevent breakage
    cursor.execute("SELECT id FROM categories LIMIT 1")
    if not cursor.fetchone():
        cursor.execute("INSERT INTO categories (id, name) VALUES (1, '📺 Watch Media')")
        
    conn.commit()

def add_user(user_id, username, starting_points, referred_by=None):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users (user_id, username, points, referred_by) VALUES (?, ?, ?, ?)", (user_id, username, starting_points, referred_by))
        conn.commit()
        return True
    except: return False

def get_user(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, username, points, referred_by, DATE(join_date) FROM users WHERE user_id = ?", (user_id,))
    return cursor.fetchone()

def update_points(user_id, delta):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET points = points + ? WHERE user_id = ?", (delta, user_id))
    conn.commit()
    
def get_points(user_id):
    user = get_user(user_id)
    return user[2] if user else 0
    
def update_media_received(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET media_received = media_received + 1 WHERE user_id = ?", (user_id,))
    conn.commit()

def get_total_referrals(user_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,))
    res = cursor.fetchone()
    return res[0] if res else 0

def get_categories():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM categories")
    return cursor.fetchall()
    
def get_category_req(cat_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT req_referrals FROM categories WHERE id = ?", (cat_id,))
        res = cursor.fetchone()
        return res[0] if res and res[0] else 0
    except: return 0

def update_category_req(cat_id, limit):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("UPDATE categories SET req_referrals = ? WHERE id = ?", (limit, cat_id))
    conn.commit()

def add_category(name):
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        conn.commit()
        return cursor.lastrowid
    except: return None

def add_media(file_id, media_type, file_unique_id=None, category_id=1):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO media (file_id, media_type, file_unique_id, category_id) VALUES (?, ?, ?, ?)", (file_id, media_type, file_unique_id, category_id))
    conn.commit()
    return cursor.lastrowid

def check_duplicate_media(file_unique_id):
    if not file_unique_id: return False
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM media WHERE file_unique_id = ?", (file_unique_id,))
    return cursor.fetchone() is not None

def get_random_media(category_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, file_id, media_type FROM media WHERE category_id = ? ORDER BY RANDOM() LIMIT 1", (category_id,))
    return cursor.fetchone()

def delete_media(media_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM media WHERE id = ?", (media_id,))
    success = cursor.rowcount > 0
    conn.commit()
    return success

def get_media_page(category_id, limit, offset):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT id, media_type FROM media WHERE category_id = ? ORDER BY id DESC LIMIT ? OFFSET ?", (category_id, limit, offset))
    return cursor.fetchall()

def get_media_by_id(media_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT file_id, media_type FROM media WHERE id = ?", (media_id,))
    return cursor.fetchone()

def wipe_category(category_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM media WHERE category_id = ?", (category_id,))
    conn.commit()

def get_cat_stats(category_id):
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM media WHERE category_id = ?", (category_id,))
    res = cursor.fetchone()
    return res[0] if res else 0

def get_stats():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    users_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM media")
    media_count = cursor.fetchone()[0]
    cursor.execute("SELECT SUM(media_received) FROM users")
    ts = cursor.fetchone()[0]
    return users_count, media_count, ts if ts else 0


# ================= UI & Keyboards =================
def get_main_keyboard(admin=False):
    markup = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    categories = get_categories()
    
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
    markup.row(InlineKeyboardButton("➕ New Category", callback_data="admin_newcat"), 
               InlineKeyboardButton("🏷️ Set Upload Category", callback_data="admin_setcat"))
    markup.row(InlineKeyboardButton("📁 Manage Media", callback_data="manage_cats"),
               InlineKeyboardButton("⚙️ Category Limits", callback_data="admin_limits"))
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
        bot.reply_to(message, "Welcome to the Media Bot! 📺\nUse the menu below to navigate.", reply_markup=get_main_keyboard())

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
        new_points = points - MEDIA_COST
        caption_text = f"Enjoy this from {cat_name}! 🍿\nRemaining points: {new_points}"
    else:
        caption_text = f"🍿 {cat_name}\n[👑 Admin View: Unlimited]\n[ID: {_id}]"
    
    try:
        if media_type == 'photo': bot.send_photo(user_id, file_id, caption=caption_text)
        elif media_type == 'video': bot.send_video(user_id, file_id, caption=caption_text)
        else: bot.send_document(user_id, file_id, caption=caption_text)
    except:
        if not admin_mode: update_points(user_id, MEDIA_COST)

# ================= Admin Callbacks =================
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

# ================= Admin Callbacks =================
@bot.callback_query_handler(func=lambda call: call.data == "admin_stats")
def cb_admin_stats(call):
    if not is_admin(call.from_user.id): return bot.answer_callback_query(call.id, "Unauthorized")
    users_count, media_count, total_received = get_stats()
    text = (f"📊 **Bot Stats Dashboard**\n\n👥 **Total Registered Users:** {users_count}\n📦 **Total Media Uploaded:** {media_count}\n📤 **Total Media Distributed:** {total_received}")
    try: bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=get_admin_panel_markup(), parse_mode="Markdown")
    except: pass
    bot.answer_callback_query(call.id)

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

# ================= Execution =================
if __name__ == "__main__":
    init_db()
    print("Database initialized.")
    print("Bot is polling...")
    try: bot.infinity_polling()
    except Exception as e: print(f"Error while polling: {e}")
