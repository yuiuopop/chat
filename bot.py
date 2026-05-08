import os
import re
import asyncio
import shutil
import subprocess
from functools import partial
from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from pytubefix import YouTube

def _get_ffmpeg_path():
    if shutil.which("ffmpeg"):
        return shutil.which("ffmpeg")
    try:
        import imageio_ffmpeg
        return imageio_ffmpeg.get_ffmpeg_exe()
    except ImportError:
        print("FFmpeg not found! Installing locally via imageio-ffmpeg...")
        try:
            subprocess.run(["python", "-m", "pip", "install", "imageio-ffmpeg"], check=True)
            import imageio_ffmpeg
            return imageio_ffmpeg.get_ffmpeg_exe()
        except Exception as e:
            print(f"Failed to install FFmpeg locally: {e}")
            return "ffmpeg" # fallback

FFMPEG_PATH = _get_ffmpeg_path()

# --- Configuration ---
API_ID = 32840332  # default test API ID, user should probably use their own
API_HASH = "e59f2b027c453c4372e80fe28d70cb4a" # default test API Hash
BOT_TOKEN = "8756272091:AAGEvJTyq0jPh1aFzDeYhvZ39c1D-TGCEok" # Replace with your bot token

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

def _fetch_formats_sync(url: str):
    yt = YouTube(url)
    
    # Get audio
    audio_streams = yt.streams.filter(adaptive=True, type='audio')
    best_audio = max(audio_streams, key=lambda s: int(s.abr.replace('kbps', '')) if s.abr else 0, default=None)
    audio_size_mb = best_audio.filesize_mb if best_audio else 0
    
    # Get video
    video_streams = yt.streams.filter(adaptive=True, type='video', file_extension='mp4')
    res_map = {}
    for s in video_streams:
        if not s.resolution: continue
        res = int(s.resolution.replace('p', ''))
        if res not in res_map or s.filesize_mb > res_map[res].filesize_mb:
            res_map[res] = s
            
    options = []
    standard_res = [1080, 720, 480, 360, 240, 144]
    
    for res in sorted(list(res_map.keys()), reverse=True):
        if res in standard_res:
            total_size_mb = res_map[res].filesize_mb + audio_size_mb
            
            label = f'{res}p'
            if total_size_mb > 0:
                if total_size_mb >= 1024:
                    label += f' ({total_size_mb/1024:.1f}GB)'
                else:
                    label += f' ({total_size_mb:.0f}MB)'
            
            # Mark options that exceed Telegram's 2GB limit
            if total_size_mb > 1990:
                label += ' ⚠️'
            
            options.append({
                'id': f'res_{res}',
                'label': label,
                'format': str(res),
                'size_mb': total_size_mb
            })
            standard_res.remove(res)
            
    audio_label = 'Audio Only'
    if audio_size_mb > 0:
        audio_label += f' ({audio_size_mb:.0f}MB)'
        
    options.append({
        'id': 'audio_only',
        'label': audio_label,
        'format': 'audio_only',
        'size_mb': audio_size_mb
    })
    
    return {
        'title': yt.title,
        'options': options,
        'duration': yt.length
    }

async def fetch_available_formats(url: str):
    try:
        return await run_in_executor(_fetch_formats_sync, url)
    except Exception as e:
        print(f"Error fetching formats: {e}")
        return None

def _download_format(url: str, format_str: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    yt = YouTube(url)
    safe_title = re.sub(r'[\\/*?:"<>|]', "", yt.title)
    # Ensure title isn't too long or empty
    if not safe_title: safe_title = "video"
    safe_title = safe_title[:50]
    
    # Get best audio
    audio_streams = yt.streams.filter(adaptive=True, type='audio')
    best_audio = max(audio_streams, key=lambda s: int(s.abr.replace('kbps', '')) if s.abr else 0, default=None)
    
    if format_str == 'audio_only':
        filepath = os.path.join(output_dir, f"{safe_title}_audio.mp3")
        if os.path.exists(filepath): return filepath
        
        temp_audio = best_audio.download(output_path=output_dir, filename=f"temp_audio_{yt.video_id}.mp4")
        
        cmd = [
            FFMPEG_PATH, '-y', '-i', temp_audio,
            '-acodec', 'libmp3lame', '-q:a', '2', filepath
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.remove(temp_audio)
        return filepath
    else:
        res_str = f"{format_str}p"
        video_streams = yt.streams.filter(adaptive=True, type='video', resolution=res_str, file_extension='mp4')
        if not video_streams:
            return None
            
        best_video = max(video_streams, key=lambda s: s.filesize_mb)
        filepath = os.path.join(output_dir, f"{safe_title}_{res_str}.mp4")
        
        if os.path.exists(filepath): return filepath
        
        temp_video = best_video.download(output_path=output_dir, filename=f"temp_video_{yt.video_id}.mp4")
        
        if best_audio:
            temp_audio = best_audio.download(output_path=output_dir, filename=f"temp_audio_{yt.video_id}.mp4")
            cmd = [
                FFMPEG_PATH, '-y',
                '-i', temp_video, '-i', temp_audio,
                '-c:v', 'copy', '-c:a', 'aac', filepath
            ]
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            os.remove(temp_video)
            os.remove(temp_audio)
        else:
            # If no audio, just rename video
            os.rename(temp_video, filepath)
            
        return filepath

async def download_video(url: str, format_str: str):
    output_dir = 'downloads'
    try:
        return await run_in_executor(_download_format, url, format_str, output_dir)
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
        "👋 **Welcome to the ROCKS YouTube Downloader Bot!**\n\n"
        "Send me a YouTube link, and I will upload it to your chat.\n"
        "I can upload files up to **2GB**!"
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
        'options': {opt['id']: opt['format'] for opt in info['options']},
        'sizes': {opt['id']: opt.get('size_mb', 0) for opt in info['options']},
        'buttons': buttons
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
    
    # Pre-check: warn user BEFORE downloading if estimated size exceeds 2GB
    estimated_size = request_data.get('sizes', {}).get(opt_id, 0)
    if estimated_size > 1990:
        await callback.answer(
            f"⚠️ This quality is ~{estimated_size/1024:.1f}GB which exceeds Telegram's 2GB limit!\n"
            f"Please select a lower quality.",
            show_alert=True
        )
        return
        
    await callback.message.edit_text(f"⏳ Downloading **{title}**...\n\nThis may take a while depending on the file size and your selected quality.")
    
    file_path = await download_video(url, format_str)
    
    if not file_path or not os.path.exists(file_path):
        await callback.message.edit_text("❌ Download failed. The video might be restricted or unavailable in this quality.")
        return
        
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    
    if file_size_mb > 1990:
        await callback.message.edit_text(f"❌ The downloaded file is too large ({file_size_mb:.1f} MB).\n\nTelegram bots have a hard limit and cannot upload files larger than 2000 MiB (2 GB). Please select a lower quality video.")
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        return

    await callback.message.edit_text(f"⬆️ Uploading **{title}** ({file_size_mb:.1f} MB) to Telegram...\n\nBot allows uploads up to 2GB, so please be patient!")
    
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
