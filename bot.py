import os
import re
import yt_dlp
import asyncio
from functools import partial
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message

# --- Configuration ---
API_ID = 2040  # default test API ID, user should probably use their own
API_HASH = "b18441a1ff607e10a989891a5462e627" # default test API Hash
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE" # Replace with your bot token

# State storage for callbacks
user_requests = {}

app = Client(
    "youtube_dl_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)

# --- Downloader Logic ---

async def run_in_executor(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(func, *args, **kwargs))

def _get_video_info(url: str):
    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': False
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        return ydl.extract_info(url, download=False)

async def fetch_available_formats(url: str):
    try:
        info = await run_in_executor(_get_video_info, url)
        
        resolutions = set()
        for f in info.get('formats', []):
            if f.get('vcodec') != 'none' and f.get('height'):
                resolutions.add(f['height'])
        
        resolutions = sorted(list(resolutions), reverse=True)
        
        options = []
        # Standard resolutions we want to offer
        standard_res = [1080, 720, 480, 360, 240, 144]
        for res in resolutions:
            if res in standard_res:
                options.append({
                    'id': f'res_{res}',
                    'label': f'{res}p',
                    'format': f'bestvideo[height<={res}]+bestaudio/best[height<={res}]'
                })
                # Remove from standard res so we don't duplicate
                standard_res.remove(res)
                
        # Also add audio option
        options.append({
            'id': 'audio_only',
            'label': 'Audio Only',
            'format': 'bestaudio/best'
        })
        
        return {
            'title': info.get('title', 'Unknown Video'),
            'options': options,
            'duration': info.get('duration', 0)
        }
    except Exception as e:
        print(f"Error fetching formats: {e}")
        return None

def _download_format(url: str, format_str: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    out_tmpl = os.path.join(output_dir, '%(title)s_%(id)s.%(ext)s')
    
    ydl_opts = {
        'format': format_str,
        'outtmpl': out_tmpl,
        'quiet': True,
        'merge_output_format': 'mp4' if 'video' in format_str or 'height' in format_str else 'mp3',
    }
    
    if format_str == 'bestaudio/best':
        ydl_opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        expected_filename = ydl.prepare_filename(info)
        
        if format_str == 'bestaudio/best':
            base, _ = os.path.splitext(expected_filename)
            expected_filename = base + '.mp3'
            
        if not os.path.exists(expected_filename):
            if 'requested_downloads' in info:
                expected_filename = info['requested_downloads'][0]['filepath']
            
        return expected_filename

async def download_video(url: str, format_str: str):
    output_dir = 'downloads'
    try:
        file_path = await run_in_executor(_download_format, url, format_str, output_dir)
        return file_path
    except Exception as e:
        print(f"Error downloading video: {e}")
        return None

# --- Telegram Bot Logic ---

def extract_urls(text: str):
    url_pattern = re.compile(
        r'(https?://(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?://(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})'
    )
    return url_pattern.findall(text)

@app.on_message(filters.command("start") & filters.private)
async def start_cmd(client: Client, message: Message):
    await message.reply_text(
        "👋 **Welcome to the YouTube Downloader Bot!**\n\n"
        "Send me a YouTube link, and I will let you choose the quality to download.\n"
        "Since I am powered by Pyrogram, I can upload files up to **2GB**!"
    )

@app.on_message(filters.text & filters.private)
async def handle_text(client: Client, message: Message):
    urls = extract_urls(message.text)
    if not urls:
        return
    
    url = urls[0]
    
    if "youtube.com" not in url and "youtu.be" not in url:
        await message.reply_text("Please send a valid YouTube link.")
        return
        
    status_msg = await message.reply_text("🔍 Fetching available formats...")
    
    info = await fetch_available_formats(url)
    if not info or not info['options']:
        await status_msg.edit_text("❌ Could not fetch formats for this video.")
        return
        
    buttons = []
    # Create keyboard layout (2 buttons per row)
    row = []
    for opt in info['options']:
        row.append(InlineKeyboardButton(opt['label'], callback_data=f"dl_{opt['id']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
        
    reply_markup = InlineKeyboardMarkup(buttons)
    
    # Store request data so we can access the URL and format str in the callback
    user_requests[status_msg.id] = {
        'url': url,
        'title': info['title'],
        'options': {opt['id']: opt['format'] for opt in info['options']}
    }
    
    await status_msg.edit_text(
        f"🎬 **{info['title']}**\n\nSelect download format:",
        reply_markup=reply_markup
    )

@app.on_callback_query(filters.regex(r"^dl_"))
async def handle_download_callback(client: Client, callback: CallbackQuery):
    opt_id = callback.data.replace("dl_", "")
    msg_id = callback.message.id
    
    request_data = user_requests.get(msg_id)
    if not request_data:
        await callback.answer("This request has expired. Please send the link again.", show_alert=True)
        return
        
    url = request_data['url']
    title = request_data['title']
    format_str = request_data['options'].get(opt_id)
    
    if not format_str:
        await callback.answer("Invalid format.", show_alert=True)
        return
        
    await callback.message.edit_text(f"⏳ Downloading **{title}**...\n\nThis may take a while depending on the file size and your selected quality.")
    
    file_path = await download_video(url, format_str)
    
    if not file_path or not os.path.exists(file_path):
        await callback.message.edit_text("❌ Download failed. The video might be restricted or unavailable in this quality.")
        return
        
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    await callback.message.edit_text(f"⬆️ Uploading **{title}** ({file_size_mb:.1f} MB) to Telegram...\n\nPyrogram allows uploads up to 2GB, so please be patient!")
    
    try:
        if opt_id == "audio_only":
            await client.send_audio(
                chat_id=callback.message.chat.id,
                audio=file_path,
                title=title
            )
        else:
            await client.send_video(
                chat_id=callback.message.chat.id,
                video=file_path,
                caption=title,
                supports_streaming=True
            )
        
        await callback.message.delete()
    except Exception as e:
        await callback.message.edit_text(f"❌ Upload failed: {e}")
    finally:
        # Cleanup
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        
        if msg_id in user_requests:
            del user_requests[msg_id]

if __name__ == "__main__":
    print("Starting bot...")
    app.run()
