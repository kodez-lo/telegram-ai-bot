import os
import re
import time
import json
import math
import shutil
import sqlite3
import hashlib
import asyncio
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse, quote

import yt_dlp

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TimedOut, NetworkError, BadRequest, Forbidden
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ============================================================
# KODEZ VIDEOS DOWNLOADER V4
# Termux-friendly | Built by Krishna Kodez
# ============================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_NAME = "Kodez Videos Downloader"
DEVELOPER_NAME = "Krishna Kodez"
CHANNEL_USERNAME = "@kodez0"
CHANNEL_URL = "https://t.me/kodez0"

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(SCRIPT_DIR))).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
DOWNLOAD_ROOT = DATA_DIR / "downloads"
WELCOME_IMAGE = SCRIPT_DIR / "kodez_bot_logo.png"
WELCOME_ANIMATION = SCRIPT_DIR / "kodez_welcome.gif"
DB_PATH = DATA_DIR / "kodez_v4.db"
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

# Performance / safety defaults for a phone running Termux.
MAX_CONCURRENT_DOWNLOADS = 2
MAX_ACTIVE_JOB_PER_USER = 1
FREE_DAILY_LIMIT = 20
PREMIUM_DAILY_LIMIT = 100
MAX_UPLOAD_MB = 49
MAX_DURATION_SECONDS = 2 * 60 * 60
STALE_TEMP_HOURS = 8
CACHE_MAX_AGE_DAYS = 90
PROGRESS_EDIT_INTERVAL = 2.0

DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
QUEUE_LOCK = asyncio.Lock()
WAITING_COUNT = 0
ACTIVE_COUNT = 0
START_TIME = time.time()

ACTIVE_JOBS = {}
ACTIVE_JOBS_BY_ID = {}
ERROR_LOG = []

# One-time owner claim. Keep this private. Once claimed, owner ID is stored in SQLite.
OWNER_CLAIM_CODE = os.getenv("OWNER_CLAIM_CODE", "").strip()

LANG = {
    "en": {
        "invalid_url": "❌ Please send a valid public video link starting with http:// or https://",
        "busy": "⚠️ You already have an active job. Use /cancel first or wait for it to finish.",
        "limit": "🚦 Daily download limit reached. Try again tomorrow.",
        "maintenance": "🛠 The bot is currently under maintenance. Please try again shortly.",
        "joined": "✅ Channel membership verified. You can send your link now.",
        "join_needed": "🔒 Please join our official channel before using the downloader.",
        "cancelled": "🛑 Download cancelled.",
        "nothing_cancel": "ℹ️ You don't have an active download right now.",
    },
    "hi": {
        "invalid_url": "❌ Valid public video link bhejo jo http:// ya https:// se start ho.",
        "busy": "⚠️ Aapka ek download already chal raha hai. /cancel use karo ya complete hone do.",
        "limit": "🚦 Aaj ka download limit complete ho gaya. Kal phir try karo.",
        "maintenance": "🛠 Bot abhi maintenance par hai. Thodi der baad try karo.",
        "joined": "✅ Channel membership verify ho gayi. Ab link bhej sakte ho.",
        "join_needed": "🔒 Downloader use karne se pehle official channel join karo.",
        "cancelled": "🛑 Download cancel kar diya gaya.",
        "nothing_cancel": "ℹ️ Abhi koi active download nahi hai.",
    },
}

QUALITY_FORMATS = {
    "360p": "bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360][ext=mp4]/best[height<=360]",
    "480p": "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480][ext=mp4]/best[height<=480]",
    "720p": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720][ext=mp4]/best[height<=720]",
    "1080p": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[height<=1080]",
    "best": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
}

PLATFORM_EMOJI = {
    "instagram": "📸",
    "youtube": "▶️",
    "facebook": "📘",
    "twitter": "𝕏",
    "x": "𝕏",
    "tiktok": "🎵",
    "reddit": "👽",
    "vimeo": "🎞️",
}


@dataclass
class JobState:
    job_id: str
    user_id: int
    url: str
    mode: str = "video"
    quality: str = "best"
    cancel_event: threading.Event = field(default_factory=threading.Event)
    thread_state: dict = field(default_factory=dict)
    status_message: object = None
    started_at: float = field(default_factory=time.time)
    request_dir: str = ""


# ---------------------------- DATABASE ----------------------------

def db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_conn() as conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                language TEXT DEFAULT 'en',
                quality TEXT DEFAULT 'best',
                created_at TEXT NOT NULL,
                last_seen TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                url TEXT,
                title TEXT,
                mode TEXT,
                quality TEXT,
                platform TEXT,
                success INTEGER DEFAULT 1,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cache (
                cache_key TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                media_type TEXT NOT NULL,
                title TEXT,
                platform TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS premium (
                user_id INTEGER PRIMARY KEY,
                expires_at TEXT
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def register_user(user):
    if user is None:
        return
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, username, first_name, created_at, last_seen)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_seen=excluded.last_seen
            """,
            (user.id, user.username or "", user.first_name or "", now_iso(), now_iso()),
        )


def get_user_prefs(user_id):
    with db_conn() as conn:
        row = conn.execute(
            "SELECT language, quality FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
    if not row:
        return "en", "best"
    language = row["language"] if row["language"] in LANG else "en"
    quality = row["quality"] if row["quality"] in QUALITY_FORMATS else "best"
    return language, quality


def set_user_pref(user_id, key, value):
    if key not in {"language", "quality"}:
        return
    with db_conn() as conn:
        conn.execute(f"UPDATE users SET {key}=? WHERE user_id=?", (value, user_id))


def record_download(user_id, url, title, mode, quality, platform, success=1):
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO downloads(user_id,url,title,mode,quality,platform,success,created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (user_id, url, title, mode, quality, platform, success, now_iso()),
        )


def record_error(message):
    message = str(message)[:1000]
    ERROR_LOG.append((now_iso(), message))
    if len(ERROR_LOG) > 50:
        del ERROR_LOG[:-50]
    try:
        with db_conn() as conn:
            conn.execute("INSERT INTO errors(message,created_at) VALUES(?,?)", (message, now_iso()))
            conn.execute(
                "DELETE FROM errors WHERE id NOT IN (SELECT id FROM errors ORDER BY id DESC LIMIT 200)"
            )
    except Exception:
        pass


def daily_count(user_id):
    since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    with db_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) c FROM downloads WHERE user_id=? AND success=1 AND created_at>=?",
            (user_id, since),
        ).fetchone()
    return int(row["c"] if row else 0)


def is_premium(user_id):
    with db_conn() as conn:
        row = conn.execute("SELECT expires_at FROM premium WHERE user_id=?", (user_id,)).fetchone()
    if not row:
        return False
    if not row["expires_at"]:
        return True
    try:
        return datetime.fromisoformat(row["expires_at"]) > datetime.now(timezone.utc)
    except Exception:
        return False


def set_premium(user_id, days=None):
    expires = None
    if days:
        expires = (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO premium(user_id,expires_at) VALUES(?,?) ON CONFLICT(user_id) DO UPDATE SET expires_at=excluded.expires_at",
            (user_id, expires),
        )


def remove_premium(user_id):
    with db_conn() as conn:
        conn.execute("DELETE FROM premium WHERE user_id=?", (user_id,))


def get_setting(key, default=None):
    with db_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key, value):
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


def get_owner_id():
    value = get_setting("owner_id")
    try:
        return int(value) if value else None
    except Exception:
        return None


def is_admin(user_id):
    owner = get_owner_id()
    env_ids = {
        int(x.strip()) for x in os.getenv("KODEZ_ADMIN_IDS", "").split(",")
        if x.strip().isdigit()
    }
    return (owner is not None and user_id == owner) or user_id in env_ids


def cache_key(url, mode, quality):
    raw = f"{url}|{mode}|{quality}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()


def cache_get(key):
    cutoff = (datetime.now(timezone.utc) - timedelta(days=CACHE_MAX_AGE_DAYS)).isoformat()
    with db_conn() as conn:
        row = conn.execute(
            "SELECT * FROM cache WHERE cache_key=? AND created_at>=?", (key, cutoff)
        ).fetchone()
    return dict(row) if row else None


def cache_put(key, file_id, media_type, title, platform):
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO cache(cache_key,file_id,media_type,title,platform,created_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(cache_key) DO UPDATE SET
                file_id=excluded.file_id,
                media_type=excluded.media_type,
                title=excluded.title,
                platform=excluded.platform,
                created_at=excluded.created_at
            """,
            (key, file_id, media_type, title, platform, now_iso()),
        )


def cache_delete(key):
    with db_conn() as conn:
        conn.execute("DELETE FROM cache WHERE cache_key=?", (key,))


# ---------------------------- HELPERS ----------------------------

def tr(user_id, key):
    lang, _ = get_user_prefs(user_id)
    return LANG.get(lang, LANG["en"]).get(key, LANG["en"].get(key, key))


def is_safe_public_url(url):
    try:
        p = urlparse(url)
        if p.scheme not in {"http", "https"} or not p.hostname:
            return False
        host = p.hostname.lower().strip(".")
        if host in {"localhost", "0.0.0.0", "127.0.0.1", "::1"}:
            return False
        if host.endswith(".local"):
            return False
        # Block obvious literal private IPs.
        import ipaddress
        try:
            ip = ipaddress.ip_address(host)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False
        except ValueError:
            pass
        return True
    except Exception:
        return False


def clean_title(text):
    text = re.sub(r"[\r\n\t]+", " ", str(text or "Video"))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:120] if text else "Video"


def platform_from_info(info):
    value = str((info or {}).get("extractor_key") or (info or {}).get("extractor") or "Website")
    low = value.lower()
    for key in PLATFORM_EMOJI:
        if key in low:
            return key.title()
    return clean_title(value)[:30]


def platform_emoji(platform):
    low = str(platform).lower()
    for key, emoji in PLATFORM_EMOJI.items():
        if key in low:
            return emoji
    return "🌐"


def human_bytes(num):
    if not num:
        return "?"
    n = float(num)
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GB"


def human_duration(seconds):
    try:
        s = int(seconds or 0)
    except Exception:
        return "?"
    if s <= 0:
        return "?"
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def progress_bar(percent, blocks=10):
    try:
        p = max(0.0, min(100.0, float(percent)))
    except Exception:
        p = 0.0
    filled = int(round((p / 100) * blocks))
    return "█" * filled + "░" * (blocks - filled)


def parse_percent(value):
    if value is None:
        return None
    try:
        return float(str(value).replace("%", "").strip())
    except Exception:
        return None


def branded_caption(title, platform=None, mode="video"):
    icon = platform_emoji(platform)
    kind = "Audio" if mode == "audio" else "Video"
    return (
        f"🎬 {clean_title(title)}\n\n"
        f"{icon} {kind} processed with {BOT_NAME}\n"
        f"👨‍💻 Bot made by {DEVELOPER_NAME}\n"
        f"📢 Join our channel: {CHANNEL_USERNAME}"
    )


def channel_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📢 Join {CHANNEL_USERNAME}", url=CHANNEL_URL)]
    ])


def result_keyboard(bot_username=None):
    rows = [[InlineKeyboardButton(f"📢 Join {CHANNEL_USERNAME}", url=CHANNEL_URL)]]
    if bot_username:
        share_text = quote(f"Try {BOT_NAME} ⚡")
        share_url = f"https://t.me/share/url?url=https://t.me/{bot_username}&text={share_text}"
        rows.append([InlineKeyboardButton("🚀 Share Bot", url=share_url)])
    return InlineKeyboardMarkup(rows)


def cancel_keyboard(job_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛑 Cancel Download", callback_data=f"cancel:{job_id}")]
    ])


def quality_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("360p", callback_data="quality:360p"),
            InlineKeyboardButton("480p", callback_data="quality:480p"),
            InlineKeyboardButton("720p", callback_data="quality:720p"),
        ],
        [
            InlineKeyboardButton("1080p", callback_data="quality:1080p"),
            InlineKeyboardButton("✨ Best", callback_data="quality:best"),
        ],
    ])


def language_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🇬🇧 English", callback_data="lang:en"),
            InlineKeyboardButton("🇮🇳 Hinglish", callback_data="lang:hi"),
        ]
    ])


async def safe_reply(message, text, reply_markup=None):
    for attempt in range(3):
        try:
            return await message.reply_text(
                text,
                reply_markup=reply_markup,
                connect_timeout=20,
                read_timeout=60,
                write_timeout=60,
                pool_timeout=20,
            )
        except (TimedOut, NetworkError) as exc:
            record_error(f"safe_reply: {exc}")
            if attempt == 2:
                return None
            await asyncio.sleep(1.2 * (attempt + 1))


async def safe_edit(message, text, reply_markup=None):
    if message is None:
        return
    try:
        await message.edit_text(
            text,
            reply_markup=reply_markup,
            connect_timeout=20,
            read_timeout=60,
            write_timeout=60,
            pool_timeout=20,
        )
    except BadRequest:
        pass
    except (TimedOut, NetworkError) as exc:
        record_error(f"safe_edit: {exc}")


async def ensure_registered(update):
    user = update.effective_user
    if user:
        await asyncio.to_thread(register_user, user)


async def current_bot_username(context):
    try:
        return context.bot.username
    except Exception:
        try:
            me = await context.bot.get_me()
            return me.username
        except Exception:
            return None


async def user_can_download(user_id):
    limit = PREMIUM_DAILY_LIMIT if await asyncio.to_thread(is_premium, user_id) else FREE_DAILY_LIMIT
    count = await asyncio.to_thread(daily_count, user_id)
    return count < limit, count, limit


async def check_channel_membership(context, user_id):
    if get_setting("force_join", "off") != "on":
        return True
    try:
        member = await context.bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return str(member.status) not in {"left", "kicked"}
    except Exception as exc:
        # Fail open so the bot does not lock everyone out if Telegram membership lookup is unavailable.
        record_error(f"force_join_check: {exc}")
        return True


async def gate_download(update, context):
    user = update.effective_user
    if not user:
        return False
    if get_setting("maintenance", "off") == "on" and not is_admin(user.id):
        await safe_reply(update.effective_message, tr(user.id, "maintenance"))
        return False
    if not await check_channel_membership(context, user.id):
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📢 Join {CHANNEL_USERNAME}", url=CHANNEL_URL)],
            [InlineKeyboardButton("✅ I've Joined", callback_data="check_join")],
        ])
        await safe_reply(update.effective_message, tr(user.id, "join_needed"), kb)
        return False
    allowed, count, limit = await user_can_download(user.id)
    if not allowed:
        await safe_reply(update.effective_message, f"{tr(user.id, 'limit')}\n\n📊 Today: {count}/{limit}")
        return False
    if user.id in ACTIVE_JOBS:
        await safe_reply(update.effective_message, tr(user.id, "busy"))
        return False
    return True


# ---------------------------- yt-dlp WORKERS ----------------------------

def ytdlp_error_to_user(exc):
    text = str(exc).lower()
    if "private" in text or "login" in text or "cookies" in text:
        return "🔒 This video appears to be private/restricted or requires login."
    if "unsupported url" in text:
        return "🌐 This link is not supported by the downloader."
    if "not available" in text or "unavailable" in text or "removed" in text:
        return "🚫 This video is unavailable, removed, or region-restricted."
    if "403" in text or "forbidden" in text:
        return "🚫 The website blocked this request. Please try again later."
    if "timeout" in text or "timed out" in text:
        return "🌐 The source website timed out. Please try again."
    if "cancel" in text:
        return "🛑 Download cancelled."
    return "❌ I couldn't process this link. Please check the URL and try again."


def download_worker(job: JobState):
    request_dir = job.request_dir
    state = job.thread_state

    def hook(d):
        if job.cancel_event.is_set():
            raise yt_dlp.utils.DownloadError("Cancelled by user")
        info = d.get("info_dict") or {}
        if info and not state.get("info"):
            state["info"] = {
                "title": info.get("title"),
                "duration": info.get("duration"),
                "thumbnail": info.get("thumbnail"),
                "extractor": info.get("extractor_key") or info.get("extractor"),
                "id": info.get("id"),
            }
        state["status"] = d.get("status")
        state["downloaded_bytes"] = d.get("downloaded_bytes")
        state["total_bytes"] = d.get("total_bytes") or d.get("total_bytes_estimate")
        state["speed"] = d.get("speed")
        state["eta"] = d.get("eta")
        pct = parse_percent(d.get("_percent_str"))
        if pct is not None:
            state["percent"] = pct

    outtmpl = os.path.join(request_dir, "%(title).80B [%(id)s].%(ext)s")
    options = {
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "restrictfilenames": False,
        "windowsfilenames": True,
        "concurrent_fragment_downloads": 6,
        "retries": 4,
        "fragment_retries": 4,
        "extractor_retries": 3,
        "socket_timeout": 25,
        "progress_hooks": [hook],
        "overwrites": True,
        "continuedl": True,
    }

    if job.mode == "audio":
        options.update({
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
        })
    else:
        options.update({
            "format": QUALITY_FORMATS.get(job.quality, QUALITY_FORMATS["best"]),
            "merge_output_format": "mp4",
        })

    with yt_dlp.YoutubeDL(options) as ydl:
        # Extract metadata first inside the same YoutubeDL instance, then process it.
        # This gives us a quick preview without creating a second downloader process.
        info = ydl.extract_info(job.url, download=False)
        state["info"] = {
            "title": info.get("title"),
            "duration": info.get("duration"),
            "thumbnail": info.get("thumbnail"),
            "extractor": info.get("extractor_key") or info.get("extractor"),
            "id": info.get("id"),
        }
        if info.get("duration") and int(info["duration"]) > MAX_DURATION_SECONDS:
            raise yt_dlp.utils.DownloadError("Video duration exceeds bot limit")
        if job.cancel_event.is_set():
            raise yt_dlp.utils.DownloadError("Cancelled by user")
        try:
            final_info = ydl.process_ie_result(info, download=True)
        except Exception:
            if job.cancel_event.is_set():
                raise yt_dlp.utils.DownloadError("Cancelled by user")
            # Compatibility fallback for extractor results that cannot be replayed.
            final_info = ydl.extract_info(job.url, download=True)

    files = [p for p in Path(request_dir).iterdir() if p.is_file() and not p.name.endswith((".part", ".ytdl"))]
    if not files:
        raise FileNotFoundError("No downloaded output file was found")

    if job.mode == "audio":
        mp3s = [p for p in files if p.suffix.lower() == ".mp3"]
        file_path = max(mp3s or files, key=lambda p: p.stat().st_mtime)
    else:
        videos = [p for p in files if p.suffix.lower() in {".mp4", ".mkv", ".webm", ".mov"}]
        file_path = max(videos or files, key=lambda p: p.stat().st_mtime)

    state["status"] = "finished"
    return str(file_path), final_info or info


def metadata_worker(url):
    opts = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "socket_timeout": 20,
        "retries": 2,
        "extractor_retries": 2,
    }
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


async def progress_reporter(job: JobState):
    last_text = None
    preview_shown = False
    while not job.cancel_event.is_set():
        state = job.thread_state
        info = state.get("info") or {}
        status = state.get("status")
        if status == "finished":
            return

        if info:
            title = clean_title(info.get("title"))
            platform = clean_title(info.get("extractor"))
            duration = human_duration(info.get("duration"))
            pct = state.get("percent")
            if pct is not None:
                text = (
                    f"⚡ {platform_emoji(platform)} {platform}\n"
                    f"🎬 {title}\n"
                    f"⏱ {duration} • 🎯 {job.quality if job.mode == 'video' else 'MP3'}\n\n"
                    f"{progress_bar(pct)}  {pct:.1f}%\n"
                    f"📥 {human_bytes(state.get('downloaded_bytes'))} / {human_bytes(state.get('total_bytes'))}\n"
                    f"🚀 Speed: {human_bytes(state.get('speed'))}/s • ETA: {state.get('eta') if state.get('eta') is not None else '?'}s"
                )
            else:
                text = (
                    f"🔎 Video detected\n"
                    f"{platform_emoji(platform)} Platform: {platform}\n"
                    f"🎬 {title}\n"
                    f"⏱ Duration: {duration}\n"
                    f"🎯 Mode: {'MP3 Audio' if job.mode == 'audio' else job.quality}\n\n"
                    "⚡ Starting download..."
                )
            preview_shown = True
        else:
            text = "🔎 Reading link information...\n⚡ Preparing your download..."

        if text != last_text:
            await safe_edit(job.status_message, text, cancel_keyboard(job.job_id))
            last_text = text
        await asyncio.sleep(PROGRESS_EDIT_INTERVAL)


async def send_cached(message, cached, caption, bot_username):
    try:
        if cached["media_type"] == "audio":
            return await message.reply_audio(
                audio=cached["file_id"],
                caption=caption,
                reply_markup=result_keyboard(bot_username),
                connect_timeout=30,
                read_timeout=120,
                write_timeout=120,
                pool_timeout=30,
            )
        return await message.reply_video(
            video=cached["file_id"],
            caption=caption,
            supports_streaming=True,
            reply_markup=result_keyboard(bot_username),
            connect_timeout=30,
            read_timeout=120,
            write_timeout=120,
            pool_timeout=30,
        )
    except (BadRequest, Forbidden):
        return None


async def send_local_media(message, file_path, caption, mode, bot_username):
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if size_mb > MAX_UPLOAD_MB:
        raise ValueError(f"File too large for this bot upload ({size_mb:.1f} MB)")

    for attempt in range(3):
        try:
            with open(file_path, "rb") as media:
                if mode == "audio":
                    sent = await message.reply_audio(
                        audio=media,
                        caption=caption,
                        reply_markup=result_keyboard(bot_username),
                        connect_timeout=30,
                        read_timeout=300,
                        write_timeout=300,
                        pool_timeout=30,
                    )
                else:
                    sent = await message.reply_video(
                        video=media,
                        caption=caption,
                        supports_streaming=True,
                        reply_markup=result_keyboard(bot_username),
                        connect_timeout=30,
                        read_timeout=300,
                        write_timeout=300,
                        pool_timeout=30,
                    )
                return sent
        except (TimedOut, NetworkError) as exc:
            record_error(f"upload_retry: {exc}")
            if attempt == 2:
                raise
            await asyncio.sleep(2 * (attempt + 1))


async def process_download(message, context, url, mode="video", quality=None):
    global WAITING_COUNT, ACTIVE_COUNT
    user = message.from_user
    user_id = user.id
    _, saved_quality = await asyncio.to_thread(get_user_prefs, user_id)
    quality = quality or saved_quality

    key = cache_key(url, mode, quality)
    cached = await asyncio.to_thread(cache_get, key)
    bot_username = await current_bot_username(context)
    if cached:
        title = cached.get("title") or "Video"
        caption = branded_caption(title, cached.get("platform"), mode)
        sent = await send_cached(message, cached, caption, bot_username)
        if sent:
            await asyncio.to_thread(record_download, user_id, url, title, mode, quality, cached.get("platform") or "Cached", 1)
            await safe_reply(message, "⚡ Served instantly from Kodez Smart Cache.")
            return
        await asyncio.to_thread(cache_delete, key)

    job_id = hashlib.md5(f"{user_id}-{time.time()}".encode()).hexdigest()[:10]
    request_dir = tempfile.mkdtemp(prefix=f"job_{job_id}_", dir=DOWNLOAD_ROOT)
    job = JobState(job_id=job_id, user_id=user_id, url=url, mode=mode, quality=quality, request_dir=request_dir)
    ACTIVE_JOBS[user_id] = job
    ACTIVE_JOBS_BY_ID[job_id] = job

    job.status_message = await safe_reply(
        message,
        "🔎 Reading link information...\n⚡ Preparing your download...",
        cancel_keyboard(job_id),
    )

    acquired = False
    reporter = None
    try:
        async with QUEUE_LOCK:
            WAITING_COUNT += 1
            queue_pos = WAITING_COUNT
        if DOWNLOAD_SEMAPHORE.locked():
            await safe_edit(
                job.status_message,
                f"⏳ Server is busy. Your download is queued.\n📌 Queue position: ~{queue_pos}",
                cancel_keyboard(job_id),
            )

        await DOWNLOAD_SEMAPHORE.acquire()
        acquired = True
        async with QUEUE_LOCK:
            WAITING_COUNT = max(0, WAITING_COUNT - 1)
            ACTIVE_COUNT += 1

        if job.cancel_event.is_set():
            raise yt_dlp.utils.DownloadError("Cancelled by user")

        reporter = asyncio.create_task(progress_reporter(job))
        file_path, info = await asyncio.to_thread(download_worker, job)
        job.thread_state["status"] = "finished"
        if reporter:
            reporter.cancel()

        if job.cancel_event.is_set():
            raise yt_dlp.utils.DownloadError("Cancelled by user")

        title = clean_title((info or {}).get("title") or (job.thread_state.get("info") or {}).get("title"))
        platform = platform_from_info(info or job.thread_state.get("info") or {})

        await safe_edit(
            job.status_message,
            "✅ Download complete!\n📤 Uploading to Telegram...",
            cancel_keyboard(job_id),
        )

        caption = branded_caption(title, platform, mode)
        sent = await send_local_media(message, file_path, caption, mode, bot_username)

        file_id = None
        if mode == "audio" and getattr(sent, "audio", None):
            file_id = sent.audio.file_id
        elif mode == "video" and getattr(sent, "video", None):
            file_id = sent.video.file_id
        if file_id:
            await asyncio.to_thread(cache_put, key, file_id, mode, title, platform)

        await asyncio.to_thread(record_download, user_id, url, title, mode, quality, platform, 1)

        if job.status_message:
            try:
                await job.status_message.delete(connect_timeout=20, read_timeout=45, write_timeout=45)
            except Exception:
                pass

    except yt_dlp.utils.DownloadError as exc:
        record_error(f"yt-dlp: {exc}")
        msg = ytdlp_error_to_user(exc)
        await safe_edit(job.status_message, msg)
        await asyncio.to_thread(record_download, user_id, url, "Failed", mode, quality, "Unknown", 0)
    except ValueError as exc:
        record_error(str(exc))
        await safe_edit(
            job.status_message,
            f"📦 {exc}\n\nTry /quality and select a lower resolution, then send the link again."
        )
        await asyncio.to_thread(record_download, user_id, url, "Too large", mode, quality, "Unknown", 0)
    except (TimedOut, NetworkError) as exc:
        record_error(f"Telegram network: {exc}")
        await safe_edit(job.status_message, "🌐 Telegram connection timed out. Please try again in a moment.")
    except asyncio.CancelledError:
        job.cancel_event.set()
        raise
    except Exception as exc:
        record_error(f"process_download {type(exc).__name__}: {exc}")
        await safe_edit(job.status_message, "❌ Something went wrong while processing this link. Please try again.")
        await asyncio.to_thread(record_download, user_id, url, "Failed", mode, quality, "Unknown", 0)
    finally:
        if reporter and not reporter.done():
            reporter.cancel()
        if acquired:
            DOWNLOAD_SEMAPHORE.release()
            async with QUEUE_LOCK:
                ACTIVE_COUNT = max(0, ACTIVE_COUNT - 1)
        ACTIVE_JOBS.pop(user_id, None)
        ACTIVE_JOBS_BY_ID.pop(job_id, None)
        shutil.rmtree(request_dir, ignore_errors=True)


# ---------------------------- USER COMMANDS ----------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    user = update.effective_user
    premium = await asyncio.to_thread(is_premium, user.id)
    _, quality = await asyncio.to_thread(get_user_prefs, user.id)
    bot_username = await current_bot_username(context)

    text = (
        "⚡ KODEZ VIDEOS DOWNLOADER\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "🎬 Fast • Smart • Automatic\n\n"
        "Send a supported public video link and the bot will automatically process it using your saved quality.\n\n"
        "✨ V4 FEATURES\n"
        "• Live download progress\n"
        "• 360p → 1080p / Best quality\n"
        "• MP3 audio extraction\n"
        "• Thumbnail grabber\n"
        "• Smart duplicate cache\n"
        "• Download history & cancel support\n\n"
        f"🎯 Your quality: {quality}\n"
        f"{'👑 Premium account' if premium else '🆓 Free account'}\n\n"
        "🚀 QUICK START\n"
        "Paste your video link below 👇\n\n"
        "🔒 Only download content you own or have permission to use.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👨‍💻 Bot made by {DEVELOPER_NAME}\n"
        f"📢 Official Channel: {CHANNEL_USERNAME}"
    )

    share_rows = [
        [
            InlineKeyboardButton("🎯 Quality", callback_data="open_quality"),
            InlineKeyboardButton("🌐 Language", callback_data="open_language"),
        ],
        [
            InlineKeyboardButton("🎵 Audio", callback_data="audio_help"),
            InlineKeyboardButton("🖼 Thumbnail", callback_data="thumb_help"),
        ],
        [InlineKeyboardButton(f"📢 Join {CHANNEL_USERNAME}", url=CHANNEL_URL)],
    ]
    if bot_username:
        share_url = f"https://t.me/share/url?url=https://t.me/{bot_username}&text={quote('Fast video downloader by Krishna Kodez ⚡')}"
        share_rows.append([InlineKeyboardButton("🚀 Share Bot", url=share_url)])
    kb = InlineKeyboardMarkup(share_rows)

    if WELCOME_ANIMATION.exists():
        try:
            with open(WELCOME_ANIMATION, "rb") as media:
                await update.effective_message.reply_animation(
                    animation=media,
                    caption=text,
                    reply_markup=kb,
                    connect_timeout=20,
                    read_timeout=90,
                    write_timeout=90,
                )
            return
        except Exception as exc:
            record_error(f"welcome_animation: {exc}")

    if WELCOME_IMAGE.exists():
        try:
            with open(WELCOME_IMAGE, "rb") as img:
                await update.effective_message.reply_photo(
                    photo=img,
                    caption=text,
                    reply_markup=kb,
                    connect_timeout=20,
                    read_timeout=90,
                    write_timeout=90,
                )
            return
        except Exception as exc:
            record_error(f"welcome_image: {exc}")
    await safe_reply(update.effective_message, text, kb)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    text = (
        "🧭 Kodez V4 Help\n\n"
        "📥 Send a link → video downloads automatically\n"
        "/quality — set default video quality\n"
        "/audio <link> — download MP3 audio\n"
        "/thumbnail <link> — get video thumbnail\n"
        "/history — last downloads\n"
        "/clearhistory — clear your history\n"
        "/cancel — cancel current download\n"
        "/language — English / Hinglish\n"
        "/settings — your preferences & limits\n"
        "/premium — premium status\n"
        "/ping — connection check\n"
        "/status — bot health\n"
        "/myid — show your Telegram numeric ID\n"
        "/feedback <message> — send feedback\n\n"
        "🔒 Use only for content you own or have permission to download."
    )
    await safe_reply(update.effective_message, text, channel_keyboard())


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    uid = update.effective_user.id
    lang, quality = await asyncio.to_thread(get_user_prefs, uid)
    prem = await asyncio.to_thread(is_premium, uid)
    count = await asyncio.to_thread(daily_count, uid)
    limit = PREMIUM_DAILY_LIMIT if prem else FREE_DAILY_LIMIT
    text = (
        "⚙️ YOUR SETTINGS\n"
        "━━━━━━━━━━━━━━━━\n"
        f"🎯 Video quality: {quality}\n"
        f"🌐 Language: {'English' if lang == 'en' else 'Hinglish'}\n"
        f"{'👑 Premium' if prem else '🆓 Free'}\n"
        f"📊 Downloads today: {count}/{limit}\n\n"
        "Use /quality or /language to change preferences."
    )
    await safe_reply(update.effective_message, text)


async def quality_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    _, quality = await asyncio.to_thread(get_user_prefs, update.effective_user.id)
    await safe_reply(update.effective_message, f"🎯 Current quality: {quality}\n\nChoose your default video quality:", quality_keyboard())


async def language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    await safe_reply(update.effective_message, "🌐 Choose bot language:", language_keyboard())


async def audio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    if not await gate_download(update, context):
        return
    url = " ".join(context.args).strip()
    if not url:
        await safe_reply(update.effective_message, "🎵 Usage: /audio <video link>")
        return
    if not is_safe_public_url(url):
        await safe_reply(update.effective_message, tr(update.effective_user.id, "invalid_url"))
        return
    context.application.create_task(process_download(update.effective_message, context, url, mode="audio", quality="audio"), update=update)


async def thumbnail_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    url = " ".join(context.args).strip()
    if not url:
        await safe_reply(update.effective_message, "🖼 Usage: /thumbnail <video link>")
        return
    if not is_safe_public_url(url):
        await safe_reply(update.effective_message, tr(update.effective_user.id, "invalid_url"))
        return
    status = await safe_reply(update.effective_message, "🔎 Fetching thumbnail...")
    try:
        info = await asyncio.to_thread(metadata_worker, url)
        thumb = info.get("thumbnail")
        title = clean_title(info.get("title"))
        if not thumb:
            await safe_edit(status, "❌ No thumbnail was available for this link.")
            return
        await update.effective_message.reply_photo(
            photo=thumb,
            caption=f"🖼 {title}\n\n👨‍💻 {DEVELOPER_NAME} • {CHANNEL_USERNAME}",
            reply_markup=channel_keyboard(),
            connect_timeout=20,
            read_timeout=120,
            write_timeout=120,
        )
        try:
            await status.delete()
        except Exception:
            pass
    except Exception as exc:
        record_error(f"thumbnail: {exc}")
        await safe_edit(status, ytdlp_error_to_user(exc))


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    uid = update.effective_user.id
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT title,mode,quality,platform,created_at FROM downloads WHERE user_id=? AND success=1 ORDER BY id DESC LIMIT 8",
            (uid,),
        ).fetchall()
    if not rows:
        await safe_reply(update.effective_message, "📭 No download history yet.")
        return
    lines = ["🕘 YOUR RECENT DOWNLOADS", "━━━━━━━━━━━━━━━━"]
    for i, r in enumerate(rows, 1):
        lines.append(f"{i}. {clean_title(r['title'])}\n   {platform_emoji(r['platform'])} {r['platform']} • {r['mode']} • {r['quality']}")
    await safe_reply(update.effective_message, "\n\n".join(lines))


async def clear_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    uid = update.effective_user.id
    with db_conn() as conn:
        conn.execute("DELETE FROM downloads WHERE user_id=?", (uid,))
    await safe_reply(update.effective_message, "🧹 Your local bot history has been cleared.")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    job = ACTIVE_JOBS.get(uid)
    if not job:
        await safe_reply(update.effective_message, tr(uid, "nothing_cancel"))
        return
    job.cancel_event.set()
    await safe_reply(update.effective_message, tr(uid, "cancelled"))


async def premium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    uid = update.effective_user.id
    prem = await asyncio.to_thread(is_premium, uid)
    text = (
        "👑 PREMIUM STATUS\n"
        "━━━━━━━━━━━━━━━━\n"
        f"Status: {'ACTIVE ✅' if prem else 'FREE 🆓'}\n"
        f"Daily limit: {PREMIUM_DAILY_LIMIT if prem else FREE_DAILY_LIMIT}\n\n"
        "Premium mode is designed for higher daily usage and future creator features."
    )
    await safe_reply(update.effective_message, text)


async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    started = time.perf_counter()
    msg = await safe_reply(update.effective_message, "🏓 Pinging...")
    ms = int((time.perf_counter() - started) * 1000)
    await safe_edit(msg, f"🏓 Pong!\n⚡ Bot response: ~{ms} ms")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - START_TIME)
    h, rem = divmod(uptime, 3600)
    m, s = divmod(rem, 60)
    with db_conn() as conn:
        users = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        downloads = conn.execute("SELECT COUNT(*) c FROM downloads WHERE success=1").fetchone()["c"]
    text = (
        "🟢 KODEZ BOT STATUS\n"
        "━━━━━━━━━━━━━━━━\n"
        f"⚡ Status: Online\n"
        f"⏱ Uptime: {h}h {m}m {s}s\n"
        f"📥 Active jobs: {ACTIVE_COUNT}\n"
        f"⏳ Waiting: {WAITING_COUNT}\n"
        f"👥 Users: {users}\n"
        f"🎬 Successful downloads: {downloads}\n"
        f"🛠 Maintenance: {get_setting('maintenance','off').upper()}"
    )
    await safe_reply(update.effective_message, text)


async def myid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await safe_reply(update.effective_message, f"🆔 Your Telegram user ID: `{update.effective_user.id}`")


async def feedback_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    text = " ".join(context.args).strip()
    if not text:
        await safe_reply(update.effective_message, "💬 Usage: /feedback <your message>\nExample: /feedback Please add another quality option")
        return
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO feedback(user_id,message,created_at) VALUES(?,?,?)",
            (update.effective_user.id, text[:1500], now_iso()),
        )
    await safe_reply(update.effective_message, "✅ Feedback received. Thank you for helping improve Kodez Videos Downloader.")


async def receive_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ensure_registered(update)
    url = (update.effective_message.text or "").strip()
    uid = update.effective_user.id
    if not is_safe_public_url(url):
        await safe_reply(update.effective_message, tr(uid, "invalid_url"))
        return
    if not await gate_download(update, context):
        return
    _, quality = await asyncio.to_thread(get_user_prefs, uid)
    context.application.create_task(process_download(update.effective_message, context, url, mode="video", quality=quality), update=update)


# ---------------------------- CALLBACKS ----------------------------

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user
    await asyncio.to_thread(register_user, user)
    data = query.data or ""

    if data.startswith("quality:"):
        quality = data.split(":", 1)[1]
        if quality in QUALITY_FORMATS:
            premium_hd = get_setting("premium_hd", "off") == "on"
            if premium_hd and quality in {"1080p", "best"} and not await asyncio.to_thread(is_premium, user.id):
                await query.answer("👑 1080p/Best is currently a Premium feature.", show_alert=True)
                return
            await asyncio.to_thread(set_user_pref, user.id, "quality", quality)
            await query.edit_message_text(f"✅ Default video quality set to {quality}.\n\nSend a link and it will download automatically.")
        return

    if data == "open_quality":
        await query.message.reply_text("🎯 Choose your default video quality:", reply_markup=quality_keyboard())
        return

    if data == "open_language":
        await query.message.reply_text("🌐 Choose bot language:", reply_markup=language_keyboard())
        return

    if data == "audio_help":
        await query.message.reply_text("🎵 Audio mode\nUse: /audio <video link>\nThe bot will extract and send MP3 audio.")
        return

    if data == "thumb_help":
        await query.message.reply_text("🖼 Thumbnail mode\nUse: /thumbnail <video link>\nThe bot will fetch the best available thumbnail.")
        return

    if data.startswith("lang:"):
        lang = data.split(":", 1)[1]
        if lang in LANG:
            await asyncio.to_thread(set_user_pref, user.id, "language", lang)
            msg = "✅ Language set to English." if lang == "en" else "✅ Language Hinglish set ho gayi."
            await query.edit_message_text(msg)
        return

    if data.startswith("cancel:"):
        job_id = data.split(":", 1)[1]
        job = ACTIVE_JOBS_BY_ID.get(job_id)
        if job and job.user_id == user.id:
            job.cancel_event.set()
            await query.edit_message_text(tr(user.id, "cancelled"))
        else:
            await query.edit_message_text("ℹ️ This download is no longer active.")
        return

    if data == "check_join":
        if await check_channel_membership(context, user.id):
            await query.edit_message_text(tr(user.id, "joined"))
        else:
            await query.answer("Join the channel first.", show_alert=True)
        return


# ---------------------------- ADMIN COMMANDS ----------------------------

async def claim_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    current = get_owner_id()
    if current:
        await safe_reply(update.effective_message, "🔐 Owner has already been claimed.")
        return
    code = context.args[0].strip() if context.args else ""
    if code != OWNER_CLAIM_CODE:
        await safe_reply(update.effective_message, "❌ Invalid owner claim code.")
        return
    set_setting("owner_id", update.effective_user.id)
    await safe_reply(update.effective_message, "👑 Owner access activated for this Telegram account.")


async def admin_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_admin(uid):
        return
    with db_conn() as conn:
        users = conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
        downloads = conn.execute("SELECT COUNT(*) c FROM downloads WHERE success=1").fetchone()["c"]
        failed = conn.execute("SELECT COUNT(*) c FROM downloads WHERE success=0").fetchone()["c"]
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        today_d = conn.execute("SELECT COUNT(*) c FROM downloads WHERE success=1 AND created_at>=?", (today,)).fetchone()["c"]
        premium_n = conn.execute("SELECT COUNT(*) c FROM premium").fetchone()["c"]
    text = (
        "📊 ADMIN STATS\n"
        "━━━━━━━━━━━━━━━━\n"
        f"👥 Total users: {users}\n"
        f"🎬 Successful: {downloads}\n"
        f"❌ Failed: {failed}\n"
        f"📅 Today: {today_d}\n"
        f"👑 Premium rows: {premium_n}\n"
        f"⚡ Active: {ACTIVE_COUNT}\n"
        f"⏳ Waiting: {WAITING_COUNT}"
    )
    await safe_reply(update.effective_message, text)


async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    with db_conn() as conn:
        rows = conn.execute("SELECT user_id,username,first_name,last_seen FROM users ORDER BY last_seen DESC LIMIT 20").fetchall()
    lines = ["👥 RECENT USERS", "━━━━━━━━━━━━━━━━"]
    for r in rows:
        name = r["username"] and f"@{r['username']}" or r["first_name"] or "User"
        lines.append(f"• {name} — `{r['user_id']}`")
    await safe_reply(update.effective_message, "\n".join(lines))


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    text = " ".join(context.args).strip()
    if not text:
        await safe_reply(update.effective_message, "Usage: /broadcast Your message")
        return
    with db_conn() as conn:
        ids = [r[0] for r in conn.execute("SELECT user_id FROM users").fetchall()]
    sent = 0
    failed = 0
    status = await safe_reply(update.effective_message, f"📣 Broadcasting to {len(ids)} users...")
    for user_id in ids:
        try:
            await context.bot.send_message(user_id, text, connect_timeout=15, read_timeout=30, write_timeout=30)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await safe_edit(status, f"✅ Broadcast complete.\nSent: {sent}\nFailed: {failed}")


async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    with db_conn() as conn:
        rows = conn.execute("SELECT message,created_at FROM errors ORDER BY id DESC LIMIT 10").fetchall()
    if not rows:
        await safe_reply(update.effective_message, "✅ No recent errors logged.")
        return
    text = "🧾 RECENT ERRORS\n━━━━━━━━━━━━━━━━\n" + "\n\n".join(
        f"• {r['created_at'][:19]}\n{r['message'][:300]}" for r in rows
    )
    await safe_reply(update.effective_message, text[:4000])


async def maintenance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    value = (context.args[0].lower() if context.args else "")
    if value not in {"on", "off"}:
        await safe_reply(update.effective_message, f"🛠 Maintenance is {get_setting('maintenance','off').upper()}\nUsage: /maintenance on|off")
        return
    set_setting("maintenance", value)
    await safe_reply(update.effective_message, f"🛠 Maintenance mode: {value.upper()}")


async def forcejoin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    value = (context.args[0].lower() if context.args else "")
    if value not in {"on", "off"}:
        await safe_reply(update.effective_message, f"🔒 Force join is {get_setting('force_join','off').upper()}\nUsage: /forcejoin on|off\n\nNote: membership checks work best when the bot is an admin in the channel.")
        return
    set_setting("force_join", value)
    await safe_reply(update.effective_message, f"🔒 Force join: {value.upper()}")


async def premium_add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args or not context.args[0].isdigit():
        await safe_reply(update.effective_message, "Usage: /premium_add <user_id> [days]\nExample: /premium_add 123456789 30")
        return
    user_id = int(context.args[0])
    days = None
    if len(context.args) > 1 and context.args[1].isdigit():
        days = int(context.args[1])
    set_premium(user_id, days)
    await safe_reply(update.effective_message, f"👑 Premium enabled for {user_id}" + (f" for {days} days." if days else " permanently."))


async def premium_remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    if not context.args or not context.args[0].isdigit():
        await safe_reply(update.effective_message, "Usage: /premium_remove <user_id>")
        return
    user_id = int(context.args[0])
    remove_premium(user_id)
    await safe_reply(update.effective_message, f"✅ Premium removed for {user_id}.")


async def premiumhd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    value = (context.args[0].lower() if context.args else "")
    if value not in {"on", "off"}:
        await safe_reply(update.effective_message, f"👑 Premium HD lock is {get_setting('premium_hd','off').upper()}\nUsage: /premiumhd on|off")
        return
    set_setting("premium_hd", value)
    await safe_reply(update.effective_message, f"👑 Premium-only 1080p/Best: {value.upper()}")


async def feedbacks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    with db_conn() as conn:
        rows = conn.execute("SELECT user_id,message,created_at FROM feedback ORDER BY id DESC LIMIT 10").fetchall()
    if not rows:
        await safe_reply(update.effective_message, "💬 No feedback yet.")
        return
    text = "💬 RECENT FEEDBACK\n━━━━━━━━━━━━━━━━\n" + "\n\n".join(
        f"• `{r['user_id']}` — {r['created_at'][:19]}\n{r['message'][:500]}" for r in rows
    )
    await safe_reply(update.effective_message, text[:4000])


async def admin_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    text = (
        "👑 ADMIN PANEL\n"
        "━━━━━━━━━━━━━━━━\n"
        "/adminstats — global statistics\n"
        "/users — latest users\n"
        "/broadcast <text> — broadcast\n"
        "/logs — recent errors\n"
        "/maintenance on|off\n"
        "/forcejoin on|off\n"
        "/premium_add <id> [days]\n"
        "/premium_remove <id>\n"
        "/premiumhd on|off — lock 1080p/Best\n"
        "/feedbacks — recent user feedback"
    )
    await safe_reply(update.effective_message, text)


# ---------------------------- CLEANUP / ERROR HANDLER ----------------------------

def cleanup_temp_dirs():
    cutoff = time.time() - (STALE_TEMP_HOURS * 3600)
    if DOWNLOAD_ROOT.exists():
        for p in DOWNLOAD_ROOT.iterdir():
            try:
                if p.is_dir() and p.stat().st_mtime < cutoff:
                    shutil.rmtree(p, ignore_errors=True)
            except Exception:
                pass
    cutoff_cache = (datetime.now(timezone.utc) - timedelta(days=CACHE_MAX_AGE_DAYS)).isoformat()
    try:
        with db_conn() as conn:
            conn.execute("DELETE FROM cache WHERE created_at<?", (cutoff_cache,))
    except Exception:
        pass


async def cleanup_loop():
    while True:
        await asyncio.to_thread(cleanup_temp_dirs)
        await asyncio.sleep(1800)


async def post_init(application: Application):
    init_db()
    await asyncio.to_thread(cleanup_temp_dirs)
    application.create_task(cleanup_loop())


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    record_error(f"Unhandled {type(error).__name__}: {error}")
    print(f"Unhandled error: {type(error).__name__}: {error}")


# ---------------------------- MAIN ----------------------------

def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is required")
    init_db()

    request = HTTPXRequest(
        connection_pool_size=30,
        connect_timeout=20.0,
        read_timeout=60.0,
        write_timeout=60.0,
        pool_timeout=20.0,
        media_write_timeout=300.0,
    )
    get_updates_request = HTTPXRequest(
        connection_pool_size=10,
        connect_timeout=20.0,
        read_timeout=45.0,
        write_timeout=45.0,
        pool_timeout=20.0,
    )

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(request)
        .get_updates_request(get_updates_request)
        .post_init(post_init)
        .build()
    )

    # User commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CommandHandler("quality", quality_command))
    app.add_handler(CommandHandler("language", language_command))
    app.add_handler(CommandHandler("audio", audio_command))
    app.add_handler(CommandHandler("thumbnail", thumbnail_command))
    app.add_handler(CommandHandler("history", history_command))
    app.add_handler(CommandHandler("clearhistory", clear_history_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("premium", premium_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("myid", myid_command))
    app.add_handler(CommandHandler("feedback", feedback_command))

    # Admin commands
    app.add_handler(CommandHandler("claimadmin", claim_admin_command))
    app.add_handler(CommandHandler("admin", admin_help_command))
    app.add_handler(CommandHandler("adminstats", admin_stats_command))
    app.add_handler(CommandHandler("users", users_command))
    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("logs", logs_command))
    app.add_handler(CommandHandler("maintenance", maintenance_command))
    app.add_handler(CommandHandler("forcejoin", forcejoin_command))
    app.add_handler(CommandHandler("premium_add", premium_add_command))
    app.add_handler(CommandHandler("premium_remove", premium_remove_command))
    app.add_handler(CommandHandler("premiumhd", premiumhd_command))
    app.add_handler(CommandHandler("feedbacks", feedbacks_command))

    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, receive_link))
    app.add_error_handler(error_handler)

    print("=" * 56)
    print("⚡ Kodez Videos Downloader V4 is ONLINE")
    print("🎬 Direct Auto Download + Live Progress + Smart Cache")
    print(f"👨‍💻 Made by {DEVELOPER_NAME}")
    print(f"📢 Channel: {CHANNEL_USERNAME}")
    print("=" * 56)

    app.run_polling(
        drop_pending_updates=True,
        poll_interval=0.0,
        timeout=30,
        allowed_updates=Update.ALL_TYPES,
    )


if __name__ == "__main__":
    main()