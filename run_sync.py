"""
run_sync.py - نسخة "تشغيلة واحدة" (بدون FastAPI/job-queue) مخصّصة للعمل
داخل GitHub Actions. بتاخد مدخلاتها من متغيرات البيئة (اللي الـ workflow
بيمررها من client_payload)، تعالج فيلم واحد، وتخزن النتيجة في Cloudflare D1.

المدخلات المطلوبة (env vars):
  VIDEO_URL        - رابط مباشر لملف mkv
  INFOHASH         - الـ infohash الخاص بالتورنت (مفتاح D1)
  SUBTITLE_URL     - رابط الترجمة (srt/ass/ssa/gz/zip) - أو استخدم SUBTITLE_FILE_PATH بدلها
  CF_ACCOUNT_ID, CF_API_TOKEN, CF_D1_DATABASE_ID - بيانات الاتصال بـ D1
"""

import asyncio
import base64
import codecs
import gzip
import io
import json
import os
import re
import sys
import tempfile
import zipfile
from typing import Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import pysubs2

CHUNK_EXTENSION = "mkv"
TARGET_AUDIO_SEC = 20 * 60
PROBE_MB = 15
SAFETY_MARGIN = 1.20
MAX_CHUNK_MB = 1500
DOWNLOAD_TIMEOUT_SEC = aiohttp.ClientTimeout(total=600)
D1_MAX_VALUE_BYTES = 1_900_000
SUBTITLE_EXTS = (".srt", ".ass", ".ssa")

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
CF_D1_DATABASE_ID = os.environ.get("CF_D1_DATABASE_ID")
D1_QUERY_URL = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"


class JobError(Exception):
    pass


# ============================================================
# فك التغليف والتحليل (نفس منطق النسخة الرابعة)
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
            with zipfile.ZipFile(io.BytesIO(current_bytes)) as zf:
                member = _pick_best_zip_member(zf)
                if not member:
                    raise JobError("ملف الـ zip لا يحتوي على ملف ترجمة مدعوم (.srt/.ass/.ssa)")
                current_bytes = zf.read(member)
                current_name = member
            continue
        if current_bytes.startswith(b"\x1f\x8b") or current_name.lower().endswith(".gz"):
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
    fmt = ext.lstrip(".")
    try:
        subs = pysubs2.SSAFile.from_string(text_content, format_=fmt)
    except Exception as e:
        raise JobError(f"فشل تحليل ملف الترجمة ({fmt}): {e}")
    return subs, fmt


# ============================================================
# الشبكة و subprocess
# ============================================================
async def check_range_support_async(session, url):
    headers = {"Range": "bytes=0-1023"}
    async with session.get(url, headers=headers) as r:
        await r.read()
        return r.status == 206


async def download_range_async(session, url, start, end, output_path):
    headers = {"Range": f"bytes={start}-{end}"}
    async with session.get(url, headers=headers) as resp:
        with open(output_path, "wb") as f:
            async for chunk in resp.content.iter_chunked(1024 * 1024):
                f.write(chunk)
    return output_path


async def run_subprocess_async(cmd, timeout=180):
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise JobError("انتهت مهلة تنفيذ العملية (timeout)")
    return proc.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")


async def get_media_duration_seconds_async(path):
    cmd = ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", path]
    try:
        code, out, _ = await run_subprocess_async(cmd, timeout=30)
        return float(out.strip())
    except Exception:
        return None


async def extract_audio_async(video_chunk_path, audio_out_path, duration_sec):
    cmd = ["ffmpeg", "-y", "-v", "error", "-fflags", "+genpts+igndts", "-i", video_chunk_path,
           "-t", str(duration_sec), "-vn", "-ac", "1", "-ar", "16000", audio_out_path]
    code, _, _ = await run_subprocess_async(cmd, timeout=180)
    success = code == 0 and os.path.exists(audio_out_path) and os.path.getsize(audio_out_path) > 1000
    if not success:
        return False, 0
    return True, (await get_media_duration_seconds_async(audio_out_path) or 0)


async def download_and_extract_target_duration_async(session, url, output_dir, chunk_ext, probe_mb, target_sec, safety_margin, max_mb):
    probe_path = os.path.join(output_dir, f"probe_chunk.{chunk_ext}")
    await download_range_async(session, url, 0, int(probe_mb * 1024 * 1024), probe_path)
    probe_audio_path = os.path.join(output_dir, "probe_audio.wav")
    ok, probe_duration = await extract_audio_async(probe_path, probe_audio_path, duration_sec=999)

    if not ok or probe_duration <= 0:
        needed_mb = min(target_sec / 30 * probe_mb * 4, max_mb)
    else:
        mb_per_sec = probe_mb / probe_duration
        needed_mb = min(mb_per_sec * target_sec * safety_margin, max_mb)

    head_path = os.path.join(output_dir, f"head_chunk.{chunk_ext}")
    await download_range_async(session, url, 0, int(needed_mb * 1024 * 1024), head_path)
    audio_path = os.path.join(output_dir, "audio_head.wav")
    ok, actual_duration = await extract_audio_async(head_path, audio_path, target_sec)

    if ok and actual_duration >= target_sec * 0.9:
        return audio_path, actual_duration

    if needed_mb < max_mb:
        bigger_mb = min(needed_mb * 1.5, max_mb)
        await download_range_async(session, url, 0, int(bigger_mb * 1024 * 1024), head_path)
        ok, actual_duration = await extract_audio_async(head_path, audio_path, target_sec)

    return (audio_path if ok else None), actual_duration


# ============================================================
# معالجة الترجمة على مستوى الـ events
# ============================================================
def crop_subtitle(subs: pysubs2.SSAFile, max_seconds: float) -> pysubs2.SSAFile:
    max_ms = max_seconds * 1000
    cropped = pysubs2.SSAFile()
    cropped.info = dict(subs.info)
    cropped.styles = dict(subs.styles)
    cropped.events = [e.copy() for e in subs.events if e.start <= max_ms]
    return cropped


def apply_offset_and_fps(subs: pysubs2.SSAFile, offset_seconds: float, fps_ratio: float = 1.0) -> pysubs2.SSAFile:
    offset_ms = offset_seconds * 1000
    out = pysubs2.SSAFile()
    out.info = dict(subs.info)
    out.styles = dict(subs.styles)
    new_events = []
    for e in subs.events:
        new_e = e.copy()
        new_start = e.start * fps_ratio + offset_ms
        new_end = e.end * fps_ratio + offset_ms
        if new_end <= 0:
            continue
        new_e.start = max(0, int(round(new_start)))
        new_e.end = max(0, int(round(new_end)))
        new_events.append(new_e)
    out.events = new_events
    return out


def parse_alass_offset(alass_output: str):
    matches = re.findall(r"shifted block of (\d+) subtitles with length ([\d:.]+) by (-?[\d:.]+)", alass_output)
    if not matches:
        return None
    matches.sort(key=lambda m: int(m[0]), reverse=True)
    offset_str = matches[0][2]
    negative = offset_str.startswith("-")
    offset_str = offset_str.lstrip("-")
    parts = offset_str.split(":")
    if len(parts) == 3:
        h, m, s = parts
        total = int(h) * 3600 + int(m) * 60 + float(s)
    elif len(parts) == 2:
        m, s = parts
        total = int(m) * 60 + float(s)
    else:
        total = float(parts[0])
    return -total if negative else total


def parse_alass_fps_ratio(alass_output: str):
    m = re.search(r"ratio is ([\d.]+)\s*/\s*([\d.]+)", alass_output)
    if not m:
        return 1.0
    num, den = float(m.group(1)), float(m.group(2))
    return num / den if den != 0 else 1.0


# ============================================================
# Cloudflare D1
# ============================================================
async def upsert_subtitle_record_async(session, infohash, ext, gz_bytes, offset_seconds, fps_ratio, audio_duration_sec):
    content_b64 = base64.b64encode(gz_bytes).decode("ascii")
    if len(content_b64) > D1_MAX_VALUE_BYTES:
        raise JobError(f"ملف الترجمة المضغوط أكبر من الحد المسموح في D1 ({len(content_b64)} بايت بعد base64)")

    sql = """
        INSERT INTO subtitles (infohash, ext, content_b64, size_bytes, offset_seconds, fps_ratio, audio_duration_sec, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(infohash) DO UPDATE SET
            ext = excluded.ext, content_b64 = excluded.content_b64, size_bytes = excluded.size_bytes,
            offset_seconds = excluded.offset_seconds, fps_ratio = excluded.fps_ratio,
            audio_duration_sec = excluded.audio_duration_sec, created_at = excluded.created_at
    """
    payload = {"sql": sql, "params": [infohash, ext, content_b64, len(gz_bytes), offset_seconds, fps_ratio, audio_duration_sec]}
    headers = {"Authorization": f"Bearer {CF_API_TOKEN}", "Content-Type": "application/json"}
    async with session.post(D1_QUERY_URL, json=payload, headers=headers) as resp:
        data = await resp.json()
        if resp.status != 200 or not data.get("success"):
            raise JobError(f"فشل تسجيل البيانات في D1: {data.get('errors', data)}")


# ============================================================
# التشغيلة الكاملة
# ============================================================
async def main():
    video_url = os.environ["VIDEO_URL"]
    infohash = os.environ["INFOHASH"]
    subtitle_url = os.environ.get("SUBTITLE_URL")

    if not (CF_ACCOUNT_ID and CF_API_TOKEN and CF_D1_DATABASE_ID):
        print(json.dumps({"status": "error", "error": "إعدادات Cloudflare D1 غير مكتملة"}, ensure_ascii=False))
        sys.exit(1)

    try:
        with tempfile.TemporaryDirectory() as work_dir:
            async with aiohttp.ClientSession(timeout=DOWNLOAD_TIMEOUT_SEC) as session:
                if not subtitle_url:
                    raise JobError("SUBTITLE_URL مطلوب في هذه النسخة")
                async with session.get(subtitle_url) as resp:
                    if resp.status != 200:
                        raise JobError("فشل تحميل ملف الترجمة من الرابط")
                    raw_bytes = await resp.read()
                raw_filename = os.path.basename(urlparse(subtitle_url).path) or "sub.srt"

                subs, fmt = load_subtitle_preserving_format(raw_bytes, raw_filename)

                await check_range_support_async(session, video_url)
                audio_source, actual_duration = await download_and_extract_target_duration_async(
                    session, video_url, work_dir, CHUNK_EXTENSION, PROBE_MB, TARGET_AUDIO_SEC, SAFETY_MARGIN, MAX_CHUNK_MB
                )
                if not audio_source or actual_duration <= 5:
                    raise JobError("فشل استخراج صوت كافٍ من رابط الفيديو")

                cropped = crop_subtitle(subs, actual_duration + 5)
                cropped_path = os.path.join(work_dir, f"cropped.{fmt}")
                cropped.save(cropped_path, format_=fmt)

                cropped_synced_path = os.path.join(work_dir, f"cropped_synced.{fmt}")
                returncode, stdout, stderr = await run_subprocess_async(
                    ["alass-cli", audio_source, cropped_path, cropped_synced_path], timeout=120
                )
                if returncode != 0:
                    raise JobError(f"خطأ في alass-cli: {stderr}")

                alass_out = stderr + stdout
                offset = parse_alass_offset(alass_out)
                if offset is None:
                    raise JobError("لم يتم التعرف على قيمة الإزاحة من alass")
                fps_ratio = parse_alass_fps_ratio(alass_out)

                synced_full = apply_offset_and_fps(subs, offset, fps_ratio)
                final_path = os.path.join(work_dir, f"final_synced.{fmt}")
                synced_full.save(final_path, format_=fmt)

                with open(final_path, "rb") as f:
                    final_bytes = f.read()
                gz_bytes = gzip.compress(final_bytes, compresslevel=9)

                await upsert_subtitle_record_async(session, infohash, fmt, gz_bytes, offset, fps_ratio, actual_duration)

        result = {
            "status": "success",
            "infohash": infohash,
            "format": fmt,
            "actual_audio_duration_sec": round(actual_duration, 1),
            "offset_seconds": round(offset, 3),
            "fps_ratio": round(fps_ratio, 6),
            "gzip_size_bytes": len(gz_bytes),
        }
        print(json.dumps(result, ensure_ascii=False))

    except JobError as e:
        print(json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False))
        sys.exit(1)
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
