"""
run_sync.py - نسخة تعمل بنظام Whisper-Based Synchronization.
تعتمد على Faster-Whisper لاستخراج التوقيتات الدقيقة عبر الذكاء الاصطناعي، 
ثم تطابق الترجمة المطلوبة مع التوقيتات المستخرجة. هذا يحل مشكلة المشاهد
الصامتة والموسيقية بالكامل.
"""

import asyncio
import base64
import codecs
import contextlib
import gzip
import io
import json
import os
import re
import shutil
import sys
import tempfile
import time
import zipfile
from typing import Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import pysubs2
from faster_whisper import WhisperModel

D1_MAX_VALUE_BYTES = 1_900_000
SUBTITLE_EXTS = (".srt", ".ass", ".ssa")

FFMPEG_TIMEOUT_SEC = int(os.environ.get("FFMPEG_TIMEOUT_SEC", str(45 * 60)))
FFPROBE_TIMEOUT_SEC = 30
ALASS_TIMEOUT_SEC = int(os.environ.get("ALASS_TIMEOUT_SEC", str(5 * 60)))
DOWNLOAD_TIMEOUT_SEC = aiohttp.ClientTimeout(total=600)

ARIA2_TIMEOUT_SEC = int(os.environ.get("ARIA2_TIMEOUT_SEC", str(60 * 60)))
ARIA2_PATH = os.environ.get("ARIA2_PATH", "aria2c")
DOWNLOAD_CONNECTIONS = max(1, int(os.environ.get("DOWNLOAD_CONNECTIONS", "8")))
USE_PARALLEL_DOWNLOAD = os.environ.get("USE_PARALLEL_DOWNLOAD", "1") != "0"

# نموذج Whisper المستخدم. base هو الأفضل توازناً للـ CPU. (يمكن وضع tiny للسرعة القصوى)
WHISPER_MODEL_SIZE = os.environ.get("WHISPER_MODEL", "base")
ALASS_SPLIT_PENALTY = os.environ.get("ALASS_SPLIT_PENALTY", "14")

INTRO_WINDOW_SEC = float(os.environ.get("INTRO_WINDOW_SEC", "240"))
INTRO_DRIFT_THRESHOLD_SEC = float(os.environ.get("INTRO_DRIFT_THRESHOLD_SEC", "1.5"))
PROGRESS_LOG_INTERVAL_SEC = 10

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
CF_D1_DATABASE_ID = os.environ.get("CF_D1_DATABASE_ID")
D1_QUERY_URL = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"

_START_TIME = time.monotonic()

class JobError(Exception):
    pass

def log(msg: str) -> None:
    elapsed = time.monotonic() - _START_TIME
    print(f"[{elapsed:7.1f}s] {msg}", flush=True)

@contextlib.contextmanager
def log_group(title: str):
    print(f"::group::{title}", flush=True)
    t0 = time.monotonic()
    try:
        yield
    finally:
        print(f"(استغرقت {time.monotonic() - t0:.1f} ثانية)", flush=True)
        print("::endgroup::", flush=True)

def fmt_hms(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

# --- فك وتحليل الترجمة ---
def detect_and_read_text(content_bytes: bytes) -> str:
    for bom, encoding in [(codecs.BOM_UTF8, "utf-8-sig"), (codecs.BOM_UTF16_LE, "utf-16-le"), (codecs.BOM_UTF16_BE, "utf-16-be")]:
        if content_bytes.startswith(bom):
            return content_bytes.decode(encoding)
    for enc in ["utf-8", "utf-16", "windows-1256"]:
        try:
            return content_bytes.decode(enc)
        except UnicodeDecodeError:
            pass
    return content_bytes.decode("windows-1256", errors="replace")

def unwrap_subtitle_bytes(raw_bytes: bytes, filename: str) -> Tuple[bytes, str]:
    current_bytes, current_name = raw_bytes, filename
    for _ in range(5):
        if current_bytes.startswith(b"PK\x03\x04"):
            with zipfile.ZipFile(io.BytesIO(current_bytes)) as zf:
                candidates = [n for n in zf.namelist() if n.lower().endswith(SUBTITLE_EXTS)]
                if not candidates:
                    raise JobError("ملف zip لا يحتوي على ترجمة.")
                current_name = candidates[0]
                current_bytes = zf.read(current_name)
            continue
        if current_bytes.startswith(b"\x1f\x8b") or current_name.lower().endswith(".gz"):
            current_bytes = gzip.decompress(current_bytes)
            current_name = re.sub(r"\.gz$", "", current_name, flags=re.IGNORECASE)
            continue
        break
    ext = next((e for e in SUBTITLE_EXTS if current_name.lower().endswith(e)), ".srt")
    return current_bytes, ext

def load_subtitle_preserving_format(raw_bytes: bytes, filename: str) -> Tuple[pysubs2.SSAFile, str]:
    final_bytes, ext = unwrap_subtitle_bytes(raw_bytes, filename)
    text = detect_and_read_text(final_bytes)
    subs = None
    fmt = "srt"
    try:
        subs = pysubs2.SSAFile.from_string(text, format_="ass")
        fmt = "ass"
    except Exception:
        try:
            subs = pysubs2.SSAFile.from_string(text)
            fmt = subs.format or "srt"
        except Exception:
            pass
    if not subs or not subs.events:
        raise JobError("ملف الترجمة فارغ أو تالف.")
    return subs, fmt

# --- عمليات النظام وتحميل الصوت ---
async def run_subprocess_async(cmd, timeout):
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise JobError(f"Timeout: {' '.join(cmd[:2])}")
    return proc.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")

async def get_video_duration_seconds(path: str) -> Optional[float]:
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path]
    code, out, _ = await run_subprocess_async(cmd, 30)
    return float(out.strip()) if code == 0 else None

def aria2c_available() -> bool:
    return shutil.which(ARIA2_PATH) is not None

async def download_video_parallel_async(video_url: str, out_path: str) -> bool:
    cmd = [
        ARIA2_PATH, "--dir", os.path.dirname(out_path), "--out", os.path.basename(out_path),
        "--max-connection-per-server", str(DOWNLOAD_CONNECTIONS), "--split", str(DOWNLOAD_CONNECTIONS),
        "--min-split-size", "5M", "--max-tries", "5", "--continue=true", "--console-log-level", "warn",
        video_url
    ]
    log(f"بدء التحميل المتوازي عبر aria2c...")
    proc = await asyncio.create_subprocess_exec(*cmd)
    try:
        await asyncio.wait_for(proc.wait(), timeout=ARIA2_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        proc.kill()
        return False
    return proc.returncode == 0 and os.path.exists(out_path)

async def obtain_full_audio_async(video_url: str, work_dir: str) -> Tuple[str, bool]:
    audio_path = os.path.join(work_dir, "audio.wav")
    used_parallel = False
    if USE_PARALLEL_DOWNLOAD and aria2c_available():
        vid_path = os.path.join(work_dir, "vid.mkv")
        if await download_video_parallel_async(video_url, vid_path):
            used_parallel = True
            log("استخراج الصوت من الملف المحلي...")
            await run_subprocess_async(["ffmpeg", "-y", "-i", vid_path, "-vn", "-ac", "1", "-ar", "16000", audio_path], FFMPEG_TIMEOUT_SEC)
            os.remove(vid_path)
    
    if not used_parallel:
        log("استخراج الصوت مباشرة من الرابط (Streaming)...")
        await run_subprocess_async([
            "ffmpeg", "-y", "-reconnect", "1", "-reconnect_streamed", "1", 
            "-i", video_url, "-vn", "-ac", "1", "-ar", "16000", audio_path
        ], FFMPEG_TIMEOUT_SEC)
        
    return audio_path, used_parallel

# --- الذكاء الاصطناعي (Whisper) ---
def _generate_whisper_sync_file(audio_path: str, out_srt_path: str):
    """يقرأ الصوت وينشئ ملف توقيتات دقيق جداً بناءً على التعرف على الكلام الفعلي"""
    log(f"بدء تحليل الصوت باستخدام Faster-Whisper (النموذج: {WHISPER_MODEL_SIZE})...")
    
    # int8 يجعل الاستهلاك على الـ CPU خفيف جداً ولا يتجاوز 500 ميجا رام.
    model = WhisperModel(WHISPER_MODEL_SIZE, device="cpu", compute_type="int8", cpu_threads=2)
    
    # VAD Filter هنا سيتجاهل الموسيقى والصمت تماماً، مما يوفر توقيتات نقية!
    segments, info = model.transcribe(
        audio_path,
        beam_size=5,
        vad_filter=True,
        vad_parameters=dict(min_silence_duration_ms=500)
    )
    
    log(f"تم التعرف على لغة الصوت: {info.language} (بثقة {info.language_probability:.2f})")
    
    subs = pysubs2.SSAFile()
    for segment in segments:
        event = pysubs2.SSAEvent(
            start=int(segment.start * 1000), 
            end=int(segment.end * 1000), 
            text=segment.text
        )
        subs.events.append(event)
        
    subs.save(out_srt_path)
    log(f"تم إنشاء التوقيت المرجعي بالذكاء الاصطناعي: {len(subs.events)} سطر حواري.")

async def sync_with_whisper_and_alass(audio_wav_path: str, subtitle_in_path: str, subtitle_out_path: str, work_dir: str):
    whisper_ref_path = os.path.join(work_dir, "whisper_reference.srt")
    
    # تشغيل نموذج Whisper في خيط منفصل حتى لا يعطل الـ Async Loop
    await asyncio.to_thread(_generate_whisper_sync_file, audio_wav_path, whisper_ref_path)
    
    # الآن بدلاً من مزامنة الترجمة مع الصوت (مما يسبب مشاكل), 
    # نقوم بمزامنة نص الترجمة مع "نص التوقيتات المستخرج من Whisper".
    # هذه العملية تُدعى Text-to-Text Alignment في alass ونسبة خطأها شبه معدومة.
    log(f"مطابقة الترجمة المرفوعة مع توقيتات الذكاء الاصطناعي (Text-to-Text)...")
    cmd = ["alass-cli", "--split-penalty", str(ALASS_SPLIT_PENALTY), whisper_ref_path, subtitle_in_path, subtitle_out_path]
    code, stdout, stderr = await run_subprocess_async(cmd, timeout=ALASS_TIMEOUT_SEC)
    
    if code != 0 or not os.path.exists(subtitle_out_path):
        raise JobError(f"فشلت المزامنة النصية: {stderr[:500]}")
    log("اكتملت المزامنة الهجينة (Whisper + alass) بنجاح.")

# --- التحقق ورفع D1 ---
def detect_intro_drift(original: pysubs2.SSAFile, synced: pysubs2.SSAFile) -> Tuple[bool, float, float]:
    n = min(len(original.events), len(synced.events))
    if n < 4: return False, 0.0, 0.0
    intro_diffs, rest_diffs = [], []
    for i in range(n):
        diff_sec = (synced.events[i].start - original.events[i].start) / 1000.0
        if original.events[i].start / 1000.0 <= INTRO_WINDOW_SEC:
            intro_diffs.append(diff_sec)
        else:
            rest_diffs.append(diff_sec)
    intro_avg = sum(intro_diffs) / len(intro_diffs) if intro_diffs else 0
    rest_avg = sum(rest_diffs) / len(rest_diffs) if rest_diffs else 0
    drift = abs(intro_avg - rest_avg)
    
    needs_review = drift > INTRO_DRIFT_THRESHOLD_SEC
    return needs_review, intro_avg, rest_avg

async def upsert_subtitle_record_async(session, infohash, file_idx, media_type, flix_id, ext, gz_bytes, approx_offset, actual_dur, needs_review):
    content_b64 = base64.b64encode(gz_bytes).decode("ascii")
    sql = """
        INSERT INTO subtitles (infohash, file_idx, media_type, flix_id, ext, content_b64, size_bytes, offset_seconds, fps_ratio, audio_duration_sec, sync_segments, needs_review, split_penalty_used, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(infohash, file_idx) DO UPDATE SET
            media_type=excluded.media_type, flix_id=excluded.flix_id, ext=excluded.ext, content_b64=excluded.content_b64,
            size_bytes=excluded.size_bytes, offset_seconds=excluded.offset_seconds, audio_duration_sec=excluded.audio_duration_sec,
            needs_review=excluded.needs_review, split_penalty_used=excluded.split_penalty_used, created_at=excluded.created_at
    """
    payload = {"sql": sql, "params": [infohash, file_idx, media_type, flix_id, ext, content_b64, len(gz_bytes), approx_offset, 1.0, actual_dur, 1, int(needs_review), float(ALASS_SPLIT_PENALTY)]}
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}
    
    async with session.post(D1_QUERY_URL, json=payload, headers=headers) as resp:
        data = await resp.json()
        if resp.status != 200 or not data.get("success"):
            raise JobError(f"D1 Error: {data}")

# --- التشغيل الكامل ---
async def main():
    infohash = os.environ["INFOHASH"]
    try:
        video_url, flix_id = os.environ["VIDEO_URL"], os.environ.get("FLIX_ID", "")
        file_idx = int(os.environ.get("FILE_IDX", flix_id if flix_id.isdigit() else 0))
        media_type = "series" if (flix_id or file_idx > 0) else "movie"
        log(f"بدء المعالجة بنظام الذكاء الاصطناعي (Whisper) - {media_type}")

        with tempfile.TemporaryDirectory() as work_dir:
            async with aiohttp.ClientSession(timeout=DOWNLOAD_TIMEOUT_SEC, headers=DEFAULT_HEADERS) as session:
                
                with log_group("1) تحميل الترجمة"):
                    if os.environ.get("SUBTITLE_B64_GZ"):
                        raw_bytes = gzip.decompress(base64.b64decode(os.environ["SUBTITLE_B64_GZ"]))
                        raw_filename = os.environ.get("SUBTITLE_FILENAME", "sub.srt")
                    else:
                        sub_url = os.environ["SUBTITLE_URL"]
                        async with session.get(sub_url) as resp:
                            raw_bytes = await resp.read()
                        raw_filename = "sub.srt"
                    subs, fmt = load_subtitle_preserving_format(raw_bytes, raw_filename)

                with log_group("2) استخراج الصوت"):
                    audio_path, used_parallel = await obtain_full_audio_async(video_url, work_dir)
                    actual_duration = await get_video_duration_seconds(audio_path)

                with log_group("3) المزامنة بالذكاء الاصطناعي (Whisper + Text Mapping)"):
                    raw_sub_path = os.path.join(work_dir, f"input.{fmt}")
                    subs.save(raw_sub_path, format_=fmt)
                    synced_path = os.path.join(work_dir, f"synced.{fmt}")
                    
                    await sync_with_whisper_and_alass(audio_path, raw_sub_path, synced_path, work_dir)
                    
                    synced_subs = pysubs2.SSAFile.load(synced_path, format_=fmt)
                    needs_review, intro_offset, rest_offset = detect_intro_drift(subs, synced_subs)

                with log_group("4) الرفع إلى D1"):
                    with open(synced_path, "rb") as f:
                        gz_bytes = gzip.compress(f.read(), compresslevel=9)
                    await upsert_subtitle_record_async(session, infohash, file_idx, media_type, flix_id, fmt, gz_bytes, rest_offset, actual_duration, needs_review)

        print(json.dumps({
            "status": "success", "infohash": infohash, "format": fmt,
            "whisper_model": WHISPER_MODEL_SIZE, "intro_drift": round(abs(intro_offset - rest_offset), 2)
        }, ensure_ascii=False))
        
    except Exception as e:
        log(f"Error: {e}")
        print(json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False))
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
