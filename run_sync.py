"""
run_sync.py - نسخة "تشغيلة واحدة" مخصّصة للعمل داخل GitHub Actions.
تستقبل مدخلاتها من متغيرات البيئة (من client_payload)، تعالج فيلم/حلقة واحدة،
وتخزن النتيجة في Cloudflare D1.

=== التعديلات الجوهرية في هذه النسخة ===

1) مشكلة "التحميل البطيء":
   قديمًا كان ffmpeg يقرأ الفيديو مباشرة من الرابط (streaming) عبر اتصال HTTP
   واحد فقط. كثير من بوابات التورنت/الديبريد تُحدّد سرعة كل اتصال منفرد
   (per-connection throttling) بغضّ النظر عن السرعة الكلية المتاحة.
   الحل: تحميل الفيديو محليًا أولاً باستخدام aria2c بعدد اتصالات متوازية
   (multi-connection download)، ثم تشغيل ffmpeg على الملف المحلي (سريع جدًا
   لأنه بدون شبكة). لو aria2c غير متاح أو فشل، نتراجع تلقائيًا لطريقة
   الـ streaming القديمة كـ fallback.

2) مشكلة "تضحية مزامنة البداية عند مشاهد محذوفة":
   (تم تحديث هذا الجزء ليستخدم Whisper AI للتعرف على الأصوات البشرية وتجاهل الموسيقى)
   نستخدم Faster-Whisper لاستخراج التوقيتات النقية من الصوت، ثم نستخدم alass
   لمطابقة الترجمة المرفوعة مع هذه التوقيتات لضمان دقة 100% وحل مشاكل المشاهد المحذوفة.
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

# --- تمت إضافة مكتبة الذكاء الاصطناعي هنا ---
from faster_whisper import WhisperModel

D1_MAX_VALUE_BYTES = 1_900_000
SUBTITLE_EXTS = (".srt", ".ass", ".ssa")

# --- تم إصلاح خطأ `invalid literal for int()` بإضافة .strip() or default ---
# مهلة استخراج الصوت الكامل عبر ffmpeg (على الملف المحلي أو streaming كـ fallback)
FFMPEG_TIMEOUT_SEC = int(os.environ.get("FFMPEG_TIMEOUT_SEC", "").strip() or str(45 * 60))
FFPROBE_TIMEOUT_SEC = 30
ALASS_TIMEOUT_SEC = int(os.environ.get("ALASS_TIMEOUT_SEC", "").strip() or str(20 * 60))
DOWNLOAD_TIMEOUT_SEC = aiohttp.ClientTimeout(total=600)

# مهلة تحميل الفيديو الكامل عبر aria2c (أكبر من مهلة ffmpeg لأن الأفلام الكبيرة
# ممكن تاخد وقت أطول في التحميل من وقت المعالجة)
ARIA2_TIMEOUT_SEC = int(os.environ.get("ARIA2_TIMEOUT_SEC", "").strip() or str(60 * 60))
ARIA2_PATH = os.environ.get("ARIA2_PATH", "aria2c")
DOWNLOAD_CONNECTIONS = max(1, int(os.environ.get("DOWNLOAD_CONNECTIONS", "").strip() or "8"))
USE_PARALLEL_DOWNLOAD = os.environ.get("USE_PARALLEL_DOWNLOAD", "1") != "0"

# نموذج Whisper المستخدم. base هو الأفضل توازناً للـ CPU.
WHISPER_MODEL_SIZE = os.environ.get("WHISPER_MODEL", "").strip() or "base"

# قيمة split-penalty لـ alass-cli. الافتراضي عند alass نفسه هو 7، لكن رفعناها
# هنا لتقليل الـ splits الخاطئة في مناطق ضعيفة الإشارة (سرد/موسيقى بلا حوار).
# جرّبوا قيم بين 10 و20 حسب نوعية المحتوى عندكم.
ALASS_SPLIT_PENALTY = os.environ.get("ALASS_SPLIT_PENALTY", "").strip() or "14"

# نافذة "بداية الحلقة" (بالثواني) التي نفحص فيها انجراف المزامنة، والعتبة
# التي لو تجاوزها الفرق بين إزاحة البداية وإزاحة باقي الحلقة، نعلّم السجل
# needs_review=1 بدل ما نثق فيه تلقائيًا.
INTRO_WINDOW_SEC = float(os.environ.get("INTRO_WINDOW_SEC", "").strip() or "240")
INTRO_DRIFT_THRESHOLD_SEC = float(os.environ.get("INTRO_DRIFT_THRESHOLD_SEC", "").strip() or "1.5")

# كل كام ثانية نطبع سطر تقدّم جديد للتحميل (عشان منغرقش اللوج بآلاف الأسطر)
PROGRESS_LOG_INTERVAL_SEC = 5

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
CF_D1_DATABASE_ID = os.environ.get("CF_D1_DATABASE_ID")
D1_QUERY_URL = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"

_START_TIME = time.monotonic()


class JobError(Exception):
    pass


# ============================================================
# أدوات اللوج - متوافقة مع GitHub Actions
# ============================================================
def log(msg: str) -> None:
    elapsed = time.monotonic() - _START_TIME
    print(f"[{elapsed:7.1f}s] {msg}", flush=True)


@contextlib.contextmanager
def log_group(title: str):
    """يفتح/يقفل مجموعة قابلة للطي في GitHub Actions log."""
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


# ============================================================
# فك التغليف والتحليل والتحديد التلقائي لصيغة الترجمة
# ============================================================
def detect_and_read_text(content_bytes: bytes) -> str:
    if content_bytes.startswith(codecs.BOM_UTF8):
        return content_bytes.decode("utf-8-sig")
    if content_bytes.startswith(codecs.BOM_UTF16_LE):
        return content_bytes.decode("utf-16-le")
    if content_bytes.startswith(codecs.BOM_UTF16_BE):
        return content_bytes.decode("utf-16-be")
    try:
        return content_bytes.decode("utf-8")
    except UnicodeDecodeError:
        pass
    try:
        return content_bytes.decode("utf-16")
    except UnicodeDecodeError:
        pass
    return content_bytes.decode("windows-1256", errors="replace")


def _pick_best_zip_member(zf: zipfile.ZipFile) -> Optional[str]:
    candidates = [n for n in zf.namelist() if not n.endswith("/") and n.lower().endswith(SUBTITLE_EXTS)]
    if not candidates:
        return None
    for ext in SUBTITLE_EXTS:
        for name in candidates:
            if name.lower().endswith(ext):
                return name
    return candidates[0]


def unwrap_subtitle_bytes(raw_bytes: bytes, filename: str) -> Tuple[bytes, str]:
    current_bytes, current_name = raw_bytes, filename
    for _ in range(5):
        if current_bytes.startswith(b"PK\x03\x04"):
            log(f"الترجمة داخل ملف zip، جاري فك الضغط... ({current_name})")
            with zipfile.ZipFile(io.BytesIO(current_bytes)) as zf:
                member = _pick_best_zip_member(zf)
                if not member:
                    raise JobError("ملف الـ zip لا يحتوي على ملف ترجمة مدعوم (.srt/.ass/.ssa)")
                current_bytes = zf.read(member)
                current_name = member
            continue
        if current_bytes.startswith(b"\x1f\x8b") or current_name.lower().endswith(".gz"):
            log(f"الترجمة مضغوطة gzip، جاري فك الضغط... ({current_name})")
            try:
                current_bytes = gzip.decompress(current_bytes)
            except Exception as e:
                raise JobError(f"فشل فك ضغط .gz: {e}")
            current_name = re.sub(r"\.gz$", "", current_name, flags=re.IGNORECASE)
            continue
        break

    lower_name = current_name.lower()
    ext = next((e for e in SUBTITLE_EXTS if lower_name.endswith(e)), None) or ".srt"
    return current_bytes, ext


def load_subtitle_preserving_format(raw_bytes: bytes, filename: str) -> Tuple[pysubs2.SSAFile, str]:
    final_bytes, ext = unwrap_subtitle_bytes(raw_bytes, filename)
    text_content = detect_and_read_text(final_bytes)

    subs = None
    detected_fmt = "srt"

    try:
        candidate = pysubs2.SSAFile.from_string(text_content, format_="ass")
        if candidate.events:
            subs = candidate
            detected_fmt = "ass"
    except Exception:
        pass

    if subs is None:
        try:
            candidate = pysubs2.SSAFile.from_string(text_content)
            if candidate.events:
                subs = candidate
                detected_fmt = candidate.format or "srt"
        except Exception:
            pass

    if subs is None or not subs.events:
        preview = text_content[:300].replace("\n", " \u23ce ")
        raise JobError(
            f"ملف الترجمة اتحمّل لكن مفيهوش أي أسطر مقروءة. "
            f"معاينة أول 300 حرف من الملف: {preview}"
        )

    log(f"تم تحليل الترجمة: {len(subs.events)} سطر، الصيغة: {detected_fmt}")
    return subs, detected_fmt


# ============================================================
# subprocess عام مع دعم قراءة الإخراج لحظيًا
# ============================================================
async def run_subprocess_async(cmd, timeout):
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise JobError(f"انتهت مهلة تنفيذ العملية (timeout بعد {timeout}s): {' '.join(cmd[:2])}...")
    return proc.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")


# ============================================================
# جلب مدة الفيديو (من رابط أو ملف محلي) - لعرض نسبة التقدّم فقط
# ============================================================
async def get_video_duration_seconds(video_path_or_url: str) -> Optional[float]:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        video_path_or_url,
    ]
    try:
        code, out, err = await run_subprocess_async(cmd, timeout=FFPROBE_TIMEOUT_SEC)
        if code != 0:
            log(f"تحذير: ffprobe فشل في معرفة مدة الفيديو (هنكمل من غير نسبة تقدّم): {err[:200]}")
            return None
        return float(out.strip())
    except Exception as e:
        log(f"تحذير: تعذّر جلب مدة الفيديو: {e}")
        return None


# ============================================================
# التحقق من توفر aria2c في البيئة
# ============================================================
def aria2c_available() -> bool:
    return shutil.which(ARIA2_PATH) is not None


# ============================================================
# تحميل الفيديو محليًا باتصالات متوازية عبر aria2c
# ============================================================
async def download_video_parallel_async(video_url: str, out_path: str) -> bool:
    out_dir = os.path.dirname(out_path)
    out_name = os.path.basename(out_path)

    cmd = [
        ARIA2_PATH,
        "--dir", out_dir,
        "--out", out_name,
        "--max-connection-per-server", str(DOWNLOAD_CONNECTIONS),
        "--split", str(DOWNLOAD_CONNECTIONS),
        "--min-split-size", "5M",
        "--max-tries", "5",
        "--retry-wait", "3",
        "--continue=true",
        "--allow-overwrite=true",
        "--summary-interval", str(PROGRESS_LOG_INTERVAL_SEC),
        "--console-log-level", "warn",
        "--user-agent", DEFAULT_HEADERS["User-Agent"],
        video_url,
    ]

    log(f"بدء التحميل المتوازي عبر aria2c ({DOWNLOAD_CONNECTIONS} اتصالات متزامنة)...")
    start = time.monotonic()

    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )

    progress_re = re.compile(r"\((\d{1,3})%\).*?DL:\s*([\d.]+\w?i?B)")
    last_log_time = 0.0

    async def read_output():
        nonlocal last_log_time
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode(errors="replace").strip()
            if not text:
                continue
            m = progress_re.search(text)
            now = time.monotonic()
            if m and now - last_log_time >= PROGRESS_LOG_INTERVAL_SEC:
                last_log_time = now
                pct, speed = m.group(1), m.group(2)
                log(f"  تحميل الفيديو (aria2c): {pct}% - السرعة: {speed}/s")
            elif not m:
                log(f"  aria2c: {text[:300]}")

    try:
        await asyncio.wait_for(read_output(), timeout=ARIA2_TIMEOUT_SEC)
        returncode = await asyncio.wait_for(proc.wait(), timeout=30)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        log(f"تحذير: انتهت مهلة aria2c (timeout بعد {ARIA2_TIMEOUT_SEC}s)، هنرجع لطريقة الـ streaming")
        return False

    if returncode != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) < 1000:
        log(f"تحذير: فشل aria2c (returncode={returncode})، هنرجع لطريقة الـ streaming")
        return False

    size_mb = os.path.getsize(out_path) / (1024 * 1024)
    elapsed = time.monotonic() - start
    speed_mb_s = size_mb / elapsed if elapsed > 0 else 0
    log(f"اكتمل تحميل الفيديو محليًا بنجاح. الحجم: {size_mb:.1f} MB "
        f"خلال {fmt_hms(elapsed)} (~{speed_mb_s:.1f} MB/s)")
    return True


# ============================================================
# استخراج الصوت الكامل - من رابط مباشر (streaming) - الطريقة القديمة كـ fallback
# ============================================================
async def extract_full_audio_from_url_async(video_url: str, out_wav_path: str, total_duration: Optional[float]) -> None:
    cmd = [
        "ffmpeg", "-y", "-v", "error",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", video_url,
        "-vn", "-ac", "1", "-ar", "16000",
        "-progress", "pipe:1", "-nostats",
        out_wav_path,
    ]
    log(f"بدء استخراج الصوت من الرابط مباشرة (streaming، بدون تحميل محلي)... المدة الكلية المتوقعة: "
        f"{fmt_hms(total_duration) if total_duration else 'غير معروفة'}")
    await _run_ffmpeg_extract(cmd, out_wav_path, total_duration)


# ============================================================
# استخراج الصوت من ملف محلي (سريع جدًا، بدون شبكة)
# ============================================================
async def extract_full_audio_from_local_file_async(video_path: str, out_wav_path: str, total_duration: Optional[float]) -> None:
    cmd = [
        "ffmpeg", "-y", "-v", "error",
        "-i", video_path,
        "-vn", "-ac", "1", "-ar", "16000",
        "-progress", "pipe:1", "-nostats",
        out_wav_path,
    ]
    log("بدء استخراج الصوت من الملف المحلي (سريع، بدون شبكة)...")
    await _run_ffmpeg_extract(cmd, out_wav_path, total_duration)


async def _run_ffmpeg_extract(cmd, out_wav_path: str, total_duration: Optional[float]) -> None:
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    last_log_time = 0.0
    start = time.monotonic()

    async def read_progress():
        nonlocal last_log_time
        assert proc.stdout is not None
        out_time_sec = 0.0
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            text = line.decode(errors="replace").strip()
            if text.startswith("out_time_ms="):
                try:
                    out_time_sec = int(text.split("=", 1)[1]) / 1_000_000
                except ValueError:
                    continue
                now = time.monotonic()
                if now - last_log_time >= PROGRESS_LOG_INTERVAL_SEC:
                    last_log_time = now
                    elapsed_real = now - start
                    if total_duration:
                        pct = min(100.0, out_time_sec / total_duration * 100)
                        speed = (out_time_sec / elapsed_real) if elapsed_real > 0 else 0
                        eta = (total_duration - out_time_sec) / speed if speed > 0 else None
                        eta_str = fmt_hms(eta) if eta is not None else "غير معروف"
                        log(f"  استخراج الصوت: {pct:5.1f}% "
                            f"({fmt_hms(out_time_sec)} / {fmt_hms(total_duration)}) "
                            f"- سرعة تقريبية: {speed:.2f}x - الوقت المتبقي التقريبي: {eta_str}")
                    else:
                        log(f"  استخراج الصوت: وصلنا لـ {fmt_hms(out_time_sec)} من الفيديو حتى الآن")

    try:
        await asyncio.wait_for(read_progress(), timeout=FFMPEG_TIMEOUT_SEC)
        returncode = await asyncio.wait_for(proc.wait(), timeout=30)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise JobError(f"انتهت مهلة استخراج الصوت (timeout بعد {FFMPEG_TIMEOUT_SEC}s)")

    stderr_bytes = await proc.stderr.read() if proc.stderr else b""
    stderr_text = stderr_bytes.decode(errors="replace")

    if returncode != 0 or not os.path.exists(out_wav_path) or os.path.getsize(out_wav_path) < 1000:
        raise JobError(f"فشل استخراج الصوت الكامل: {stderr_text[:500]}")

    size_mb = os.path.getsize(out_wav_path) / (1024 * 1024)
    log(f"اكتمل استخراج الصوت بنجاح. حجم ملف الصوت: {size_mb:.1f} MB "
        f"({fmt_hms(time.monotonic() - start)})")


# ============================================================
# تنسيق موحّد يجرّب التحميل المتوازي أولاً ثم يتراجع للـ streaming
# ============================================================
async def obtain_full_audio_async(video_url: str, work_dir: str, total_duration: Optional[float]) -> Tuple[str, bool]:
    audio_path = os.path.join(work_dir, "full_audio.wav")

    if USE_PARALLEL_DOWNLOAD and aria2c_available():
        video_local_path = os.path.join(work_dir, "source_video.mkv")
        downloaded = await download_video_parallel_async(video_url, video_local_path)
        if downloaded:
            try:
                await extract_full_audio_from_local_file_async(video_local_path, audio_path, total_duration)
                return audio_path, True
            finally:
                try:
                    if os.path.exists(video_local_path):
                        os.remove(video_local_path)
                except OSError:
                    pass
    elif USE_PARALLEL_DOWNLOAD and not aria2c_available():
        log("تحذير: aria2c غير مثبّت في البيئة، هنستخدم طريقة الـ streaming القديمة. ")

    await extract_full_audio_from_url_async(video_url, audio_path, total_duration)
    return audio_path, False


# ============================================================
# دالة الذكاء الاصطناعي (Whisper) الجديدة المضافة
# ============================================================
def _generate_whisper_sync_file(audio_path: str, out_srt_path: str):
    """يقرأ الصوت وينشئ ملف توقيتات دقيق جداً بناءً على التعرف الفعلي على الكلام (يتجاهل الموسيقى)"""
    log(f"بدء تحليل الصوت باستخدام Faster-Whisper (النموذج: {WHISPER_MODEL_SIZE})...")
    
    model = WhisperModel(WHISPER_MODEL_SIZE, device="cpu", compute_type="int8", cpu_threads=2)
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
    log(f"تم إنشاء التوقيت المرجعي بالذكاء الاصطناعي (خالٍ من الموسيقى): {len(subs.events)} سطر حواري.")


# ============================================================
# مزامنة الترجمة الكاملة (تم التعديل لدمج الذكاء الاصطناعي هنا)
# ============================================================
async def sync_subtitle_full_async(audio_wav_path: str, subtitle_in_path: str, subtitle_out_path: str, split_penalty: str, work_dir: str) -> None:
    # الخطوة 1: استخراج التوقيتات النقية بالذكاء الاصطناعي
    whisper_ref_path = os.path.join(work_dir, "whisper_reference.srt")
    await asyncio.to_thread(_generate_whisper_sync_file, audio_wav_path, whisper_ref_path)

    # الخطوة 2: استخدام alass لمطابقة الترجمة المرفوعة مع توقيتات الذكاء الاصطناعي بدقة عالية
    log(f"بدء مزامنة الترجمة عبر alass-cli (Text-to-Text alignment، split-penalty={split_penalty})...")
    
    # نستخدم ملف الـ srt الناتج من whisper كمرجع بدلاً من الصوت المباشر!
    cmd = ["alass-cli", "--split-penalty", str(split_penalty), whisper_ref_path, subtitle_in_path, subtitle_out_path]
    code, stdout, stderr = await run_subprocess_async(cmd, timeout=ALASS_TIMEOUT_SEC)
    
    if stdout.strip():
        log(f"مخرجات alass-cli: {stdout.strip()[:1500]}")
    if code != 0:
        raise JobError(f"خطأ في alass-cli: {stderr[:1000]}")
    if not os.path.exists(subtitle_out_path):
        raise JobError("alass-cli لم يُنتج ملف مخرجات رغم انتهائه بنجاح")
    
    log("اكتملت المزامنة الهجينة (Whisper + alass) بنجاح.")


def estimate_average_offset_seconds(original: pysubs2.SSAFile, synced: pysubs2.SSAFile) -> float:
    n = min(len(original.events), len(synced.events), 50)
    if n < 1:
        return 0.0
    diffs = [(synced.events[i].start - original.events[i].start) / 1000.0 for i in range(n)]
    return sum(diffs) / len(diffs)


# ============================================================
# كشف انجراف المزامنة في بداية الحلقة (intro drift detection)
# ============================================================
def detect_intro_drift(
    original: pysubs2.SSAFile,
    synced: pysubs2.SSAFile,
    intro_window_sec: float = INTRO_WINDOW_SEC,
    drift_threshold_sec: float = INTRO_DRIFT_THRESHOLD_SEC,
) -> Tuple[bool, float, float]:
    n = min(len(original.events), len(synced.events))
    if n < 4:
        return False, 0.0, 0.0

    intro_diffs, rest_diffs = [], []
    for i in range(n):
        diff_sec = (synced.events[i].start - original.events[i].start) / 1000.0
        if original.events[i].start / 1000.0 <= intro_window_sec:
            intro_diffs.append(diff_sec)
        else:
            rest_diffs.append(diff_sec)

    if not intro_diffs or not rest_diffs:
        return False, 0.0, 0.0

    intro_avg = sum(intro_diffs) / len(intro_diffs)
    rest_avg = sum(rest_diffs) / len(rest_diffs)
    drift = abs(intro_avg - rest_avg)

    needs_review = drift > drift_threshold_sec
    if needs_review:
        log(f"تحذير QA: انجراف مشبوه بين مزامنة البداية ({intro_avg:.2f}s) "
            f"وباقي الحلقة ({rest_avg:.2f}s) - الفرق {drift:.2f}s > العتبة "
            f"{drift_threshold_sec}s. تم تعليم السجل needs_review=1.")
    else:
        log(f"فحص QA: إزاحة البداية {intro_avg:.2f}s مقابل باقي الحلقة {rest_avg:.2f}s "
            f"(فرق {drift:.2f}s) - ضمن الحدود المقبولة.")

    return needs_review, intro_avg, rest_avg


# ============================================================
# Cloudflare D1
# ============================================================
async def upsert_subtitle_record_async(
    session, infohash, file_idx, media_type, flix_id, ext, gz_bytes,
    approx_offset_seconds, audio_duration_sec, needs_review: bool, split_penalty: str
):
    content_b64 = base64.b64encode(gz_bytes).decode("ascii")
    if len(content_b64) > D1_MAX_VALUE_BYTES:
        raise JobError(f"ملف الترجمة المضغوط أكبر من الحد المسموح في D1 ({len(content_b64)} بايت بعد base64)")

    fps_ratio = 1.0
    sync_segments = 1

    sql = """
        INSERT INTO subtitles (infohash, file_idx, media_type, flix_id, ext, content_b64, size_bytes, offset_seconds, fps_ratio, audio_duration_sec, sync_segments, needs_review, split_penalty_used, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(infohash, file_idx) DO UPDATE SET
            media_type = excluded.media_type,
            flix_id = excluded.flix_id,
            ext = excluded.ext,
            content_b64 = excluded.content_b64,
            size_bytes = excluded.size_bytes,
            offset_seconds = excluded.offset_seconds,
            fps_ratio = excluded.fps_ratio,
            audio_duration_sec = excluded.audio_duration_sec,
            sync_segments = excluded.sync_segments,
            needs_review = excluded.needs_review,
            split_penalty_used = excluded.split_penalty_used,
            created_at = excluded.created_at
    """
    payload = {
        "sql": sql,
        "params": [infohash, file_idx, media_type, flix_id, ext, content_b64, len(gz_bytes),
                   approx_offset_seconds, fps_ratio, audio_duration_sec, sync_segments,
                   1 if needs_review else 0, float(split_penalty)],
    }
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}
    log("جاري رفع الترجمة المزامنة إلى Cloudflare D1...")
    async with session.post(D1_QUERY_URL, json=payload, headers=headers) as resp:
        data = await resp.json()
        if resp.status != 200 or not data.get("success"):
            raise JobError(f"فشل تسجيل البيانات في D1: {data.get('errors', data)}")
    log("تم الحفظ في D1 بنجاح.")


# ============================================================
# التشغيلة الكاملة
# ============================================================
async def main():
    with log_group("1) قراءة الإعدادات والمدخلات"):
        video_url = os.environ.get("VIDEO_URL", "")
        infohash = os.environ.get("INFOHASH", "")

        flix_id = os.environ.get("FLIX_ID", "").strip()
        file_idx_raw = os.environ.get("FILE_IDX", "").strip()
        subtitle_filename = os.environ.get("SUBTITLE_FILENAME", "sub.srt").strip() or "sub.srt"

        file_idx = 0
        if file_idx_raw.isdigit():
            file_idx = int(file_idx_raw)
        elif flix_id.isdigit():
            file_idx = int(flix_id)

        media_type = "series" if (flix_id or file_idx > 0) else "movie"

        subtitle_b64_gz = os.environ.get("SUBTITLE_B64_GZ")
        subtitle_url = os.environ.get("SUBTITLE_URL")

        log(f"infohash={infohash} file_idx={file_idx} media_type={media_type} flix_id={flix_id or '-'}")
        log(f"video_url={video_url}")
        log(f"split_penalty={ALASS_SPLIT_PENALTY} parallel_download={USE_PARALLEL_DOWNLOAD} "
            f"connections={DOWNLOAD_CONNECTIONS} aria2c_available={aria2c_available()}")

        if not (CF_ACCOUNT_ID and CF_API_TOKEN and CF_D1_DATABASE_ID):
            print(json.dumps({"status": "error", "error": "إعدادات Cloudflare D1 غير مكتملة"}, ensure_ascii=False))
            sys.exit(1)

    try:
        with tempfile.TemporaryDirectory() as work_dir:
            async with aiohttp.ClientSession(timeout=DOWNLOAD_TIMEOUT_SEC, headers=DEFAULT_HEADERS) as session:

                with log_group("2) تحميل/تحليل ملف الترجمة الأصلي"):
                    if subtitle_b64_gz:
                        log("مصدر الترجمة: SUBTITLE_B64_GZ (مضمّنة في المدخلات)")
                        try:
                            raw_bytes = gzip.decompress(base64.b64decode(subtitle_b64_gz))
                            raw_filename = subtitle_filename
                        except Exception as e:
                            raise JobError(f"فشل فك ضغط بيانات الترجمة الممررة بـ Base64/Gzip: {e}")
                    elif subtitle_url:
                        log(f"مصدر الترجمة: رابط خارجي - {subtitle_url}")
                        async with session.get(subtitle_url) as resp:
                            if resp.status != 200:
                                body_preview = (await resp.text(errors="replace"))[:300]
                                raise JobError(
                                    f"فشل تحميل ملف الترجمة من الرابط (HTTP {resp.status}). "
                                    f"معاينة الرد: {body_preview}"
                                )
                            raw_bytes = await resp.read()
                        raw_filename = os.path.basename(urlparse(subtitle_url).path) or "sub.srt"
                    else:
                        raise JobError("يجب توفير إما SUBTITLE_B64_GZ أو SUBTITLE_URL")

                    subs, fmt = load_subtitle_preserving_format(raw_bytes, raw_filename)

                with log_group("3) معرفة مدة الفيديو (لعرض نسبة التقدّم فقط)"):
                    total_duration = await get_video_duration_seconds(video_url)
                    if total_duration:
                        log(f"مدة الفيديو: {fmt_hms(total_duration)}")

                with log_group("4) الحصول على الصوت الكامل (تحميل متوازي محلي، أو streaming كـ fallback)"):
                    audio_path, used_parallel = await obtain_full_audio_async(video_url, work_dir, total_duration)
                    actual_duration = (await get_video_duration_seconds(audio_path)) or total_duration or 0.0
                    log(f"طريقة الحصول على الصوت المستخدمة: "
                        f"{'تحميل متوازي محلي (aria2c)' if used_parallel else 'streaming مباشر (fallback)'}")

                with log_group("5) مزامنة الترجمة (الذكاء الاصطناعي Whisper + alass)"):
                    raw_subtitle_path = os.path.join(work_dir, f"input.{fmt}")
                    subs.save(raw_subtitle_path, format_=fmt)

                    synced_path = os.path.join(work_dir, f"synced.{fmt}")
                    # تمرير work_dir للدالة المحدثة
                    await sync_subtitle_full_async(audio_path, raw_subtitle_path, synced_path, ALASS_SPLIT_PENALTY, work_dir)

                    synced_subs = pysubs2.SSAFile.load(synced_path, format_=fmt)
                    approx_offset = estimate_average_offset_seconds(subs, synced_subs)
                    log(f"متوسط تقريبي للانزياح المُصحَّح (وصفي فقط): {approx_offset:.3f}s")

                    needs_review, intro_offset, rest_offset = detect_intro_drift(subs, synced_subs)

                with log_group("6) ضغط ورفع النتيجة إلى D1"):
                    with open(synced_path, "rb") as f:
                        final_bytes = f.read()
                    gz_bytes = gzip.compress(final_bytes, compresslevel=9)
                    log(f"حجم الترجمة بعد الضغط: {len(gz_bytes) / 1024:.1f} KB")

                    await upsert_subtitle_record_async(
                        session, infohash, file_idx, media_type, flix_id, fmt, gz_bytes,
                        approx_offset, actual_duration, needs_review, ALASS_SPLIT_PENALTY
                    )

        result = {
            "status": "success",
            "infohash": infohash,
            "file_idx": file_idx,
            "media_type": media_type,
            "flix_id": flix_id,
            "format": fmt,
            "video_duration_sec": round(actual_duration, 1) if actual_duration else None,
            "approx_offset_seconds": round(approx_offset, 3),
            "intro_offset_seconds": round(intro_offset, 3),
            "rest_offset_seconds": round(rest_offset, 3),
            "needs_review": needs_review,
            "split_penalty_used": ALASS_SPLIT_PENALTY,
            "used_parallel_download": used_parallel,
            "gzip_size_bytes": len(gz_bytes),
        }
        log(f"تمت المهمة بنجاح خلال {fmt_hms(time.monotonic() - _START_TIME)}")
        print(json.dumps(result, ensure_ascii=False))

    except JobError as e:
        log(f"فشل: {e}")
        print(json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False))
        sys.exit(1)
    except Exception as e:
        log(f"خطأ غير متوقع: {e}")
        print(json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
