"""
run_sync.py - نسخة "تشغيلة واحدة" مخصّصة للعمل داخل GitHub Actions.
تستقبل مدخلاتها من متغيرات البيئة (من client_payload)، تعالج فيلم/حلقة واحدة،
وتخزن النتيجة في Cloudflare D1.

المدخلات:
  VIDEO_URL        - رابط مباشر لملف mkv
  INFOHASH         - الـ infohash الخاص بالتورنت
  FLIX_ID          - (اختياري) إذا وُجد رقم (مثلاً "27") يعتبرها حلقة مسلسل، وإذا خلا يعتبرها فيلم
  FILE_IDX         - (اختياري) رقم الملف داخل التورنت (إذا لم يرسل يؤخذ من FLIX_ID أو يوضع 0)
  SUBTITLE_B64_GZ  - نص ملف الترجمة مضغوط بـ Gzip ومحكّم بـ Base64
  SUBTITLE_URL     - (بديل) رابط الترجمة الاحتياطي
  ALASS_SPLIT_PENALTY - (اختياري) قيمة split-penalty الممررة لـ alass-cli (افتراضي: 7)

  CF_ACCOUNT_ID, CF_API_TOKEN, CF_D1_DATABASE_ID - بيانات الاتصال بـ D1

--------------------------------------------------------------------------
تعديل مهم عن النسخة السابقة:
--------------------------------------------------------------------------
النسخة القديمة كانت تستخدم `alass-cli --no-split` ثم تحسب خط انحدار خطي
واحد (offset + fps_ratio) من الأسطر المطابقة وتمدّه على الحلقة/الفيلم كله.
هذا يفشل تحديدًا عند وجود مشهد محذوف/مقطوع (شائع في الأنمي) لأن الجزء اللي
بعد نقطة القطع يحتاج إزاحة مختلفة عن الجزء اللي قبلها.

التعديل هنا:
  1) حذف --no-split وتفعيل split-penalty بحيث alass نفسه يكتشف نقاط
     القطع داخل نافذة الصوت (20 دقيقة تبقى كافية لمعظم حلقات الأنمي).
  2) بدل ما نعمل "خط واحد" من نتائج alass، بنستخدم التوقيت اللي رجّعه
     alass لكل سطر **مباشرة** (مطابقة سطر بسطر بالاندكس الأصلي)، وده
     بيحافظ تلقائيًا على أي تعدد في القطع اللي اكتشفه.
  3) الأسطر النادرة اللي بعد نهاية نافذة الصوت (غالبًا بس في نهايات
     الأفلام الطويلة) بتتمدد بخط محلي محسوب من آخر جزء متزامن فعليًا،
     مش من الحلقة كلها.
--------------------------------------------------------------------------
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
from typing import Optional, Tuple, List
from urllib.parse import urlparse

import aiohttp
import pysubs2

CHUNK_EXTENSION = "mkv"

TARGET_AUDIO_SEC = int(os.environ.get("TARGET_AUDIO_MINUTES", "20")) * 60

PROBE_MB = 15
SAFETY_MARGIN = 1.20
MAX_CHUNK_MB = 1500
DOWNLOAD_TIMEOUT_SEC = aiohttp.ClientTimeout(total=600)
D1_MAX_VALUE_BYTES = 1_900_000
SUBTITLE_EXTS = (".srt", ".ass", ".ssa")

# قيمة split-penalty لـ alass-cli. القيم الأصغر تخلي alass يكتشف نقاط قطع
# أكتر (حساس أكتر لمشاهد محذوفة قصيرة)، والقيم الأكبر تخليه يتجاهل فروق
# بسيطة ويعتبرها ضجيج. القيم المفيدة عادة بين 5 و20 (حسب نسخة alass).
ALASS_SPLIT_PENALTY = os.environ.get("ALASS_SPLIT_PENALTY", "7")

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
}

CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID")
CF_API_TOKEN = os.environ.get("CF_API_TOKEN")
CF_D1_DATABASE_ID = os.environ.get("CF_D1_DATABASE_ID")
D1_QUERY_URL = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/d1/database/{CF_D1_DATABASE_ID}/query"


class JobError(Exception):
    pass


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

    return subs, detected_fmt


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
    code, _, _ = await run_subprocess_async(cmd, timeout=240)
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
def crop_subtitle_with_indices(subs: pysubs2.SSAFile, max_seconds: float):
    """
    زي crop_subtitle القديمة، لكن بترجع كمان قائمة بالاندكسات الأصلية
    (المواقع في subs.events) لكل سطر تم تضمينه، عشان نقدر نربط ناتج
    alass بالسطر الأصلي المطابق بالظبط لاحقًا.
    """
    max_ms = max_seconds * 1000
    cropped = pysubs2.SSAFile()
    cropped.info = dict(subs.info)
    cropped.styles = dict(subs.styles)

    indices: List[int] = []
    events = []
    for idx, e in enumerate(subs.events):
        if e.start <= max_ms:
            indices.append(idx)
            events.append(e.copy())
    cropped.events = events
    return cropped, indices


def compute_linear_transform(orig_events, synced_events):
    n = min(len(orig_events), len(synced_events))
    if n < 2:
        return None

    xs = [orig_events[i].start for i in range(n)]
    ys = [synced_events[i].start for i in range(n)]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n

    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        ratio = 1.0
    else:
        numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        ratio = numerator / denominator

    offset_ms = mean_y - ratio * mean_x
    return ratio, offset_ms / 1000.0


def count_sync_segments(orig_events, synced_events, jump_threshold_ms=1000) -> int:
    """
    تقدير تقريبي لعدد "القطع" اللي اكتشفها alass، عن طريق حساب عدد
    القفزات الكبيرة (> jump_threshold_ms) في فرق التوقيت بين أسطر متتالية.
    مفيد فقط للتسجيل/المراقبة، مش مستخدم في بناء الملف النهائي.
    """
    n = min(len(orig_events), len(synced_events))
    if n < 2:
        return 1
    diffs = [synced_events[i].start - orig_events[i].start for i in range(n)]
    segments = 1
    for i in range(1, n):
        if abs(diffs[i] - diffs[i - 1]) > jump_threshold_ms:
            segments += 1
    return segments


def build_full_sync_from_alass(
    subs: pysubs2.SSAFile,
    original_indices: List[int],
    cropped_events,
    synced_cropped_events,
) -> pysubs2.SSAFile:
    """
    يبني الترجمة الكاملة المزامَنة اعتمادًا على توقيت alass لكل سطر
    مباشرة (split-aware) بدل خط انحدار واحد يُمدّ على الملف كله.

    - أي سطر أصلي داخل نافذة الصوت (اللي اتقارن فعليًا في alass) بياخد
      توقيته الجديد كما رجّعه alass حرفيًا -> بيحافظ على أي قطع/فواصل
      متعددة اكتشفها alass تلقائيًا.
    - أي سطر بعد نهاية نافذة الصوت (نادر، غالبًا بس في نهايات أفلام
      طويلة) بيتمدد بخط محلي (ratio, offset) محسوب من آخر جزء متزامن
      فعليًا، مش من الملف كله.
    """
    n = min(len(original_indices), len(synced_cropped_events))
    if n == 0:
        raise JobError("لا توجد أسطر مشتركة للمقارنة بعد المزامنة")

    direct_map = {}
    for i in range(n):
        orig_idx = original_indices[i]
        synced_e = synced_cropped_events[i]
        direct_map[orig_idx] = (synced_e.start, synced_e.end)

    # خط الامتداد للأسطر النادرة اللي بعد نهاية نافذة الصوت: بناخد آخر
    # جزء فعليًا متزامن (آخر ثلث الأسطر المقارنة) بدل الملف كله.
    tail_count = max(2, n // 3)
    tail_orig = cropped_events[n - tail_count:n]
    tail_synced = synced_cropped_events[n - tail_count:n]
    tail_transform = compute_linear_transform(tail_orig, tail_synced)
    tail_ratio, tail_offset_sec = tail_transform if tail_transform else (1.0, 0.0)
    tail_offset_ms = tail_offset_sec * 1000

    out = pysubs2.SSAFile()
    out.info = dict(subs.info)
    out.styles = dict(subs.styles)

    new_events = []
    for idx, e in enumerate(subs.events):
        if idx in direct_map:
            new_start, new_end = direct_map[idx]
        else:
            new_start = e.start * tail_ratio + tail_offset_ms
            new_end = e.end * tail_ratio + tail_offset_ms

        if new_end <= 0:
            continue

        new_e = e.copy()
        new_e.start = max(0, int(round(new_start)))
        new_e.end = max(0, int(round(new_end)))
        new_events.append(new_e)

    out.events = new_events
    return out


# ============================================================
# Cloudflare D1
# ============================================================
async def upsert_subtitle_record_async(
    session, infohash, file_idx, media_type, flix_id, ext, gz_bytes,
    offset_seconds, fps_ratio, audio_duration_sec, sync_segments
):
    content_b64 = base64.b64encode(gz_bytes).decode("ascii")
    if len(content_b64) > D1_MAX_VALUE_BYTES:
        raise JobError(f"ملف الترجمة المضغوط أكبر من الحد المسموح في D1 ({len(content_b64)} بايت بعد base64)")

    sql = """
        INSERT INTO subtitles (infohash, file_idx, media_type, flix_id, ext, content_b64, size_bytes, offset_seconds, fps_ratio, audio_duration_sec, sync_segments, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
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
            created_at = excluded.created_at
    """
    payload = {
        "sql": sql,
        "params": [infohash, file_idx, media_type, flix_id, ext, content_b64, len(gz_bytes), offset_seconds, fps_ratio, audio_duration_sec, sync_segments],
    }
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

    flix_id = os.environ.get("FLIX_ID", "").strip()
    file_idx_raw = os.environ.get("FILE_IDX", "").strip()
    subtitle_filename = os.environ.get("SUBTITLE_FILENAME", "sub.srt").strip() or "sub.srt"

    file_idx = 0
    if file_idx_raw.isdigit():
        file_idx = int(file_idx_raw)
    elif flix_id.isdigit():
        file_idx = int(flix_id)

    if flix_id or file_idx > 0:
        media_type = "series"
    else:
        media_type = "movie"

    subtitle_b64_gz = os.environ.get("SUBTITLE_B64_GZ")
    subtitle_url = os.environ.get("SUBTITLE_URL")

    if not (CF_ACCOUNT_ID and CF_API_TOKEN and CF_D1_DATABASE_ID):
        print(json.dumps({"status": "error", "error": "إعدادات Cloudflare D1 غير مكتملة"}, ensure_ascii=False))
        sys.exit(1)

    try:
        with tempfile.TemporaryDirectory() as work_dir:
            async with aiohttp.ClientSession(timeout=DOWNLOAD_TIMEOUT_SEC, headers=DEFAULT_HEADERS) as session:

                if subtitle_b64_gz:
                    try:
                        raw_bytes = gzip.decompress(base64.b64decode(subtitle_b64_gz))
                        raw_filename = subtitle_filename
                    except Exception as e:
                        raise JobError(f"فشل فك ضغط بيانات الترجمة الممررة بـ Base64/Gzip: {e}")
                elif subtitle_url:
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

                await check_range_support_async(session, video_url)
                audio_source, actual_duration = await download_and_extract_target_duration_async(
                    session, video_url, work_dir, CHUNK_EXTENSION, PROBE_MB, TARGET_AUDIO_SEC, SAFETY_MARGIN, MAX_CHUNK_MB
                )
                if not audio_source or actual_duration <= 5:
                    raise JobError("فشل استخراج صوت كافٍ من رابط الفيديو")

                # -------- التعديل الأساسي هنا --------
                cropped, original_indices = crop_subtitle_with_indices(subs, actual_duration + 5)
                cropped_path = os.path.join(work_dir, f"cropped.{fmt}")
                cropped.save(cropped_path, format_=fmt)

                cropped_synced_path = os.path.join(work_dir, f"cropped_synced.{fmt}")
                # حذفنا --no-split وفعّلنا split-penalty عشان alass يقدر
                # يكتشف مشاهد محذوفة/مقطوعة جوه نافذة الـ 20 دقيقة نفسها.
                returncode, stdout, stderr = await run_subprocess_async(
                    [
                        "alass-cli", audio_source, cropped_path, cropped_synced_path,
                        "--split-penalty", str(ALASS_SPLIT_PENALTY),
                    ],
                    timeout=180,
                )
                if returncode != 0:
                    raise JobError(f"خطأ في alass-cli: {stderr}")

                try:
                    synced_cropped_subs = pysubs2.SSAFile.load(cropped_synced_path, format_=fmt)
                except Exception as e:
                    raise JobError(f"فشل قراءة ملف الإخراج من alass-cli: {e}")

                if len(synced_cropped_subs.events) < 2 or len(cropped.events) < 2:
                    raise JobError(
                        "تعذّر حساب المزامنة - عدد أسطر الترجمة المتاحة للمقارنة غير كافٍ "
                        f"(الأصلي: {len(cropped.events)}, بعد المزامنة: {len(synced_cropped_subs.events)})"
                    )

                # بناء الترجمة الكاملة مباشرة من ناتج alass (split-aware)
                # بدل خط انحدار واحد يُمدّ على الملف كله.
                synced_full = build_full_sync_from_alass(
                    subs, original_indices, cropped.events, synced_cropped_subs.events
                )

                # قيم تقريبية للتسجيل/المراقبة فقط في D1 (مش مستخدمة في
                # بناء الملف النهائي، لأن كل سطر أخد توقيته الفعلي من alass).
                overall_transform = compute_linear_transform(cropped.events, synced_cropped_subs.events)
                overall_ratio, overall_offset = overall_transform if overall_transform else (1.0, 0.0)
                sync_segments = count_sync_segments(cropped.events, synced_cropped_subs.events)

                final_path = os.path.join(work_dir, f"final_synced.{fmt}")
                synced_full.save(final_path, format_=fmt)

                with open(final_path, "rb") as f:
                    final_bytes = f.read()
                gz_bytes = gzip.compress(final_bytes, compresslevel=9)

                await upsert_subtitle_record_async(
                    session, infohash, file_idx, media_type, flix_id, fmt, gz_bytes,
                    overall_offset, overall_ratio, actual_duration, sync_segments
                )

        result = {
            "status": "success",
            "infohash": infohash,
            "file_idx": file_idx,
            "media_type": media_type,
            "flix_id": flix_id,
            "format": fmt,
            "actual_audio_duration_sec": round(actual_duration, 1),
            "overall_offset_seconds": round(overall_offset, 3),
            "overall_fps_ratio": round(overall_ratio, 6),
            "sync_segments_detected": sync_segments,
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
