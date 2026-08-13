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

  CF_ACCOUNT_ID, CF_API_TOKEN, CF_D1_DATABASE_ID - بيانات الاتصال بـ D1

=== ملاحظة مهمة (إصلاح) ===
النسخة دي بتحل مشكلة جوهرية كانت موجودة: كان الكود بيقارن سطور الترجمة الأصلية
مع سطور مخرجات alass بالاعتماد على *ترتيبها في الليستة (index)* فقط. لو alass
أسقط سطر واحد (وده بيحصل كتير مع أسطر السرد فوق موسيقى/مؤثرات بدون كلام واضح)،
كل الفهرسة اللي بعده بتنزاح، فيبقى بيقارن سطر غلط بسطر غلط تمامًا، وده اللي كان
يسبب "تدمير" المزامنة بالكامل (أسطر بتتحذف فعليًا، وأسطر بتظهر في توقيت غلط
تمامًا) بدل مجرد خطأ بسيط في الإزاحة.

الحل: كل سطر بنبعته لـ alass بنحطله معرّف فريد (tag) في حقل Name (حقل مش
بيتلمس أو يتلمس بصريًا في العرض)، وبعد ما alass يرجّع النتيجة، بنطابق كل سطر
برجوع لمعرّفه هو مش لمكانه في الليستة. أي سطر alass أسقطه، بنكتشفه ونستبعده
من حساب الـ transform بدل ما يلخبط كل حاجة بعده.
"""

import asyncio
import base64
import codecs
import gzip
import io
import json
import os
import re
import statistics
import sys
import tempfile
import zipfile
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp
import pysubs2

CHUNK_EXTENSION = "mkv"

TARGET_AUDIO_SEC = int(os.environ.get("TARGET_AUDIO_MINUTES", "30")) * 60

PROBE_MB = 15
SAFETY_MARGIN = 1.20
MAX_CHUNK_MB = 2200
DOWNLOAD_TIMEOUT_SEC = aiohttp.ClientTimeout(total=900)
D1_MAX_VALUE_BYTES = 1_900_000
SUBTITLE_EXTS = (".srt", ".ass", ".ssa")

ALASS_SPLIT_PENALTY = os.environ.get("ALASS_SPLIT_PENALTY")
# رفعنا العتبة بشكل كبير: فرق أقل من كده بين سطرين متجاورين طبيعي جدًا
# (ضوضاء عادية في محاذاة alass) ومش دليل على قصّة حقيقية. القصّات
# الحقيقية بتعمل فرق كبير (ثواني) بيستمر عبر كذا سطر، مش نص ثانية.
SYNC_JUMP_THRESHOLD_MS = float(os.environ.get("SYNC_JUMP_THRESHOLD_MS", "2500"))
SYNC_MIN_SEGMENT_EVENTS = int(os.environ.get("SYNC_MIN_SEGMENT_EVENTS", "8"))
# أي قطعة زمنية أقصر من كده (بالثواني) شبه مؤكد إنها ضوضاء مش قصّة
# حقيقية - مشهد كامل اتقصّ عادةً بياخد وقت أطول من كده بكتير.
SYNC_MIN_SEGMENT_DURATION_SEC = float(os.environ.get("SYNC_MIN_SEGMENT_DURATION_SEC", "20"))

# لو alass (في وضع split) أسقط أكتر من النسبة دي من الأسطر أثناء القياس،
# بنعتبر إن نتيجة split مش موثوقة بما يكفي، وبنعمل تشغيلة احتياطية بوضع
# --no-split اللي بيضمن مطابقة كل الأسطر (زيرو إسقاط) بإزاحة عامة واحدة.
MAX_ALASS_DROP_RATIO = float(os.environ.get("MAX_ALASS_DROP_RATIO", "0.15"))

# لو حتى بعد الاحتياطي (no-split) نسبة الإسقاط لسه عالية جدًا (نادر لأن
# no-split بيفترض مايسقطش حاجة أصلًا)، هنا فعلًا نوقف لأن فيه مشكلة تانية
# (زي إن الترجمة مش بتاعة نفس النسخة من الفيديو أصلًا).
MAX_ALASS_DROP_RATIO_FALLBACK = float(os.environ.get("MAX_ALASS_DROP_RATIO_FALLBACK", "0.5"))

SYNC_ID_PREFIX = "SYNCID"

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


async def run_alass_pass(audio_source, cropped_path, out_path, fmt, work_dir, use_split: bool):
    """
    بتشغّل alass-cli مرة واحدة. use_split=True بيفعّل اكتشاف القصّ/المشاهد
    المحذوفة (وممكن يسقط أسطر من عملية القياس الداخلية). use_split=False
    بيشغّل alass بوضع --no-split، اللي بيحسب إزاحة/سرعة عامة واحدة بس،
    وده بيضمن إن كل الأسطر هتترجع متطابقة (مفيش إسقاط) - أضعف في التعامل
    مع القصّ، لكن مضمون التغطية 100%.
    """
    cmd = ["alass-cli", audio_source, cropped_path, out_path]
    if use_split:
        if ALASS_SPLIT_PENALTY:
            cmd += ["--split-penalty", str(ALASS_SPLIT_PENALTY)]
    else:
        cmd += ["--no-split"]

    returncode, stdout, stderr = await run_subprocess_async(cmd, timeout=180)
    if returncode != 0:
        raise JobError(f"خطأ في alass-cli ({'split' if use_split else 'no-split'}): {stderr}")

    try:
        return pysubs2.SSAFile.load(out_path, format_=fmt)
    except Exception as e:
        raise JobError(f"فشل قراءة ملف الإخراج من alass-cli: {e}")


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
# معالجة الترجمة
# ============================================================
def crop_subtitle(subs: pysubs2.SSAFile, max_seconds: float) -> pysubs2.SSAFile:
    max_ms = max_seconds * 1000
    cropped = pysubs2.SSAFile()
    cropped.info = dict(subs.info)
    cropped.styles = dict(subs.styles)
    cropped.events = [e.copy() for e in subs.events if e.start <= max_ms]
    # لازم تكون الأسطر مرتّبة زمنيًا قبل ما نوسمها، عشان ترتيب المعرّفات
    # (وبالتالي ترتيب الأزواج اللي هندخلها لحساب الـ transform) يبقى صح.
    cropped.events.sort(key=lambda e: e.start)
    return cropped


def tag_events_for_matching(cropped_subs: pysubs2.SSAFile) -> Dict[str, float]:
    """
    بتحط معرّف فريد لكل سطر (في حقل Name، مش بيتلمس بصريًا ومش بيأثر على
    التوقيت) قبل ما نبعت الملف لـ alass. ده بيسمحلنا بعدين إننا نلاقي كل
    سطر برجوع لهويته الحقيقية بدل ما نعتمد على مكانه في الليستة.
    """
    id_to_orig_start: Dict[str, float] = {}
    for idx, e in enumerate(cropped_subs.events):
        tag = f"{SYNC_ID_PREFIX}{idx:06d}"
        e.name = tag
        id_to_orig_start[tag] = e.start
    return id_to_orig_start


def extract_matched_pairs(
    id_to_orig_start: Dict[str, float], synced_events
) -> Tuple[List[Tuple[float, float]], int]:
    """
    بتدوّر في مخرجات alass عن الأسطر اللي لسه شايلة نفس معرّف الوسم، وبتبني
    أزواج (التوقيت الأصلي، التوقيت بعد المزامنة) بالاعتماد على الهوية مش
    على الترتيب. أي سطر alass أسقطه (معرّفه مش موجود في المخرجات) بيتسجّل
    كـ"محذوف" وما بيدخلش في حساب الـ transform.
    """
    seen = set()
    pairs: List[Tuple[float, float]] = []
    for e in synced_events:
        tag = e.name
        if tag in id_to_orig_start and tag not in seen:
            pairs.append((id_to_orig_start[tag], e.start))
            seen.add(tag)

    # نرتّب الأزواج حسب التوقيت *الأصلي* (يعني ترتيب أحداث الفيلم الحقيقي)
    # مش حسب ترتيب ظهورها في ملف alass، عشان لو alass غيّر ترتيب الكتابة
    # الداخلي ما يأثرش على حساب القفزات (jumps) بتاعتنا.
    pairs.sort(key=lambda p: p[0])
    dropped_count = len(id_to_orig_start) - len(seen)
    return pairs, dropped_count


class SyncSegment:
    """
    كل قطعة بقى ليها معادلتها الخاصة بالكامل: سرعة (fps_ratio) وإزاحة
    (offset_ms/intercept) مستقلين عن باقي القطع. ده بيمتص أي انجراف
    (drift) تدريجي في السرعة داخل القطعة نفسها، بدل ما يسرّب كقفزات
    وهمية صغيرة بتتفسّر غلط كـ"قصّات".
    """
    __slots__ = ("orig_start_ms", "orig_end_ms", "fps_ratio", "offset_ms")

    def __init__(self, orig_start_ms: float, orig_end_ms: float, fps_ratio: float, offset_ms: float):
        self.orig_start_ms = orig_start_ms
        self.orig_end_ms = orig_end_ms
        self.fps_ratio = fps_ratio
        self.offset_ms = offset_ms


def _linear_fit(xs: List[float], ys: List[float]) -> Tuple[float, float]:
    """أبسط انحدار خطي (slope, intercept) بطريقة المربعات الصغرى."""
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 1.0, mean_y - mean_x
    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denominator
    intercept = mean_y - slope * mean_x
    return slope, intercept


def _rolling_median(values: List[float], window: int) -> List[float]:
    """
    بتنعّم سلسلة القيم بمتوسط نافذة متحركة (median)، عشان نقلل تأثير أي
    سطر واحد شاذ (noise) قبل ما نحاول نكتشف قفزات حقيقية.
    """
    n = len(values)
    if n == 0:
        return []
    half = max(1, window) // 2
    out = []
    for i in range(n):
        lo = max(0, i - half)
        hi = min(n, i + half + 1)
        out.append(statistics.median(values[lo:hi]))
    return out


def _detect_breakpoints(
    xs: List[float],
    residuals: List[float],
    jump_threshold_ms: float,
    min_segment_events: int,
    min_segment_duration_ms: float,
    compare_window: int = 15,
) -> List[int]:
    """
    بتكتشف نقاط انقطاع حقيقية بس - مش أي تذبذب طبيعي بين سطرين متجاورين.
    بدل ما نقارن سطر بالسطر اللي جنبه (حساس جدًا للضوضاء)، بنقارن *متوسط*
    نافذة من الأسطر قبل النقطة المرشّحة بمتوسط نافذة من الأسطر بعدها. لو
    الفرق بين المتوسطين كبير، وكل قطعة ناتجة طولها كافٍ (عدد أسطر ومدة
    زمنية)، هنا بس نعتبرها قصّة حقيقية.
    """
    n = len(residuals)
    smoothed = _rolling_median(residuals, window=11)
    breakpoints = [0]
    seg_start = 0
    i = 1
    while i < n:
        before = smoothed[max(seg_start, i - compare_window):i]
        after = smoothed[i:i + compare_window]
        if before and after:
            diff = abs(statistics.median(after) - statistics.median(before))
        else:
            diff = abs(smoothed[i] - smoothed[i - 1])

        if diff > jump_threshold_ms:
            duration_ms = xs[i - 1] - xs[seg_start]
            if (i - seg_start) >= min_segment_events and duration_ms >= min_segment_duration_ms:
                breakpoints.append(i)
                seg_start = i
        i += 1
    breakpoints.append(n)
    return breakpoints


def compute_piecewise_transform(
    xs: List[float],
    ys: List[float],
    jump_threshold_ms: float = SYNC_JUMP_THRESHOLD_MS,
    min_segment_events: int = SYNC_MIN_SEGMENT_EVENTS,
    min_segment_duration_sec: float = SYNC_MIN_SEGMENT_DURATION_SEC,
) -> Optional[Tuple[float, List[SyncSegment]]]:
    """
    xs: توقيتات البداية الأصلية (قبل المزامنة)
    ys: توقيتات البداية المقابلة بعد المزامنة (نفس السطر، متطابق بالهوية
        مش بالفهرس - شوف extract_matched_pairs)

    الخوارزمية:
    1. نعمل انحدار خطي عام (global) على كل النقاط، بس عشان نستخدمه كمرجع
       لاكتشاف نقاط الانقطاع (breakpoints) وكـ fallback للقطع القصيرة.
    2. نكتشف نقاط الانقطاع الحقيقية بس (شوف _detect_breakpoints).
    3. كل قطعة ناتجة بتاخد انحدارها الخطي الخاص بيها (سرعة+إزاحة مستقلين)،
       لو عندها نقط كفاية لحساب موثوق - وإلا بترجع لقيمة الـ fallback العامة.
    """
    n = len(xs)
    if n < 2:
        return None

    global_fps_ratio, _ = _linear_fit(xs, ys)
    residuals = [ys[i] - global_fps_ratio * xs[i] for i in range(n)]

    breakpoints = _detect_breakpoints(
        xs, residuals, jump_threshold_ms, min_segment_events, min_segment_duration_sec * 1000
    )

    segments: List[SyncSegment] = []
    for bi in range(len(breakpoints) - 1):
        s, e = breakpoints[bi], breakpoints[bi + 1]
        seg_xs, seg_ys = xs[s:e], ys[s:e]

        if len(seg_xs) >= 5:
            local_slope, local_intercept = _linear_fit(seg_xs, seg_ys)
            # حماية: لو الانحدار المحلي طلع رقم غير منطقي (بسبب عدد نقط
            # قليل نسبيًا أو توزيع ملتوي)، منرجعش عليه - بنستخدم السرعة
            # العامة ونحسب بس الإزاحة (median) للقطعة دي.
            if not (0.8 <= local_slope <= 1.25):
                local_slope = global_fps_ratio
                local_intercept = statistics.median(
                    [seg_ys[i] - local_slope * seg_xs[i] for i in range(len(seg_xs))]
                )
        else:
            local_slope = global_fps_ratio
            local_intercept = statistics.median(
                [seg_ys[i] - local_slope * seg_xs[i] for i in range(len(seg_xs))]
            )

        segments.append(SyncSegment(seg_xs[0], seg_xs[-1], local_slope, local_intercept))

    return global_fps_ratio, segments


def get_segment_for_time(segments: List[SyncSegment], t_ms: float) -> SyncSegment:
    for seg in segments:
        if t_ms <= seg.orig_end_ms:
            return seg
    return segments[-1]


def apply_piecewise_transform(subs: pysubs2.SSAFile, segments: List[SyncSegment]) -> pysubs2.SSAFile:
    out = pysubs2.SSAFile()
    out.info = dict(subs.info)
    out.styles = dict(subs.styles)
    new_events = []
    for e in subs.events:
        seg = get_segment_for_time(segments, e.start)
        new_start = e.start * seg.fps_ratio + seg.offset_ms
        new_end = e.end * seg.fps_ratio + seg.offset_ms
        if new_end <= 0:
            # السطر ده بيقع بالكامل قبل بداية الفيديو الفعلية بعد التصحيح
            # (طبيعي لو أول offset سالب كبير)، فمفيش معنى نعرضه.
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
    session, infohash, file_idx, media_type, flix_id, ext, gz_bytes, offset_seconds, fps_ratio, audio_duration_sec, segments_count
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
        "params": [infohash, file_idx, media_type, flix_id, ext, content_b64, len(gz_bytes), offset_seconds, fps_ratio, audio_duration_sec, segments_count],
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

                cropped = crop_subtitle(subs, actual_duration + 5)
                if len(cropped.events) < 2:
                    raise JobError("عدد أسطر الترجمة داخل النطاق الزمني المتاح غير كافٍ للمزامنة")

                # نوسم كل سطر بمعرّف فريد قبل ما نبعته لـ alass - ده أساس الإصلاح.
                id_to_orig_start = tag_events_for_matching(cropped)
                total_tagged = len(id_to_orig_start)

                cropped_path = os.path.join(work_dir, f"cropped.{fmt}")
                cropped.save(cropped_path, format_=fmt)

                # --- التشغيلة الأولى: split mode (بيتعامل مع القصّ، وممكن يسقط أسطر قليلة أثناء القياس) ---
                cropped_synced_path = os.path.join(work_dir, f"cropped_synced.{fmt}")
                synced_cropped_subs = await run_alass_pass(
                    audio_source, cropped_path, cropped_synced_path, fmt, work_dir, use_split=True
                )
                matched_pairs, dropped_count = extract_matched_pairs(
                    id_to_orig_start, synced_cropped_subs.events
                )
                alass_mode_used = "split"

                # --- لو نسبة الإسقاط عالية، نجرب تشغيلة احتياطية بدون split (تغطية كاملة مضمونة) ---
                if total_tagged and (dropped_count / total_tagged) > MAX_ALASS_DROP_RATIO:
                    cropped_synced_nosplit_path = os.path.join(work_dir, f"cropped_synced_nosplit.{fmt}")
                    synced_cropped_nosplit_subs = await run_alass_pass(
                        audio_source, cropped_path, cropped_synced_nosplit_path, fmt, work_dir, use_split=False
                    )
                    fallback_pairs, fallback_dropped = extract_matched_pairs(
                        id_to_orig_start, synced_cropped_nosplit_subs.events
                    )
                    # نستخدم نتيجة no-split لو فعلًا أحسن (إسقاط أقل) من نتيجة split
                    if fallback_dropped < dropped_count:
                        matched_pairs, dropped_count = fallback_pairs, fallback_dropped
                        alass_mode_used = "no_split_fallback"

                if total_tagged and (dropped_count / total_tagged) > MAX_ALASS_DROP_RATIO_FALLBACK:
                    raise JobError(
                        f"alass أسقط {dropped_count} من أصل {total_tagged} سطر "
                        f"({dropped_count / total_tagged:.0%}) حتى بعد المحاولة الاحتياطية - "
                        "نسبة عالية جدًا بحيث إن نتيجة المزامنة مش موثوقة. الأرجح إن الترجمة "
                        "مش بتاعة نفس نسخة الفيديو (ترتيب مشاهد مختلف، إصدار مُعاد توزيعه، إلخ)."
                    )

                if len(matched_pairs) < 2:
                    raise JobError(
                        "بعد استبعاد الأسطر اللي أسقطها alass، الأسطر المتبقية "
                        "للمقارنة مش كافية لحساب المزامنة"
                    )

                xs = [p[0] for p in matched_pairs]
                ys = [p[1] for p in matched_pairs]

                transform = compute_piecewise_transform(xs, ys)
                if transform is None:
                    raise JobError("تعذّر حساب الإزاحة من الأسطر المتطابقة")
                global_fps_ratio, segments = transform

                synced_full = apply_piecewise_transform(subs, segments)
                final_path = os.path.join(work_dir, f"final_synced.{fmt}")
                synced_full.save(final_path, format_=fmt)

                with open(final_path, "rb") as f:
                    final_bytes = f.read()
                gz_bytes = gzip.compress(final_bytes, compresslevel=9)

                representative_offset_sec = segments[-1].offset_ms / 1000.0
                await upsert_subtitle_record_async(
                    session, infohash, file_idx, media_type, flix_id, fmt, gz_bytes,
                    representative_offset_sec, global_fps_ratio, actual_duration, len(segments),
                )

        result = {
            "status": "success",
            "infohash": infohash,
            "file_idx": file_idx,
            "media_type": media_type,
            "flix_id": flix_id,
            "format": fmt,
            "actual_audio_duration_sec": round(actual_duration, 1),
            "global_fps_ratio": round(global_fps_ratio, 6),
            "sync_segments_detected": len(segments),
            "alass_mode_used": alass_mode_used,
            "lines_compared_total": total_tagged,
            "lines_dropped_by_alass": dropped_count,
            "segments_detail": [
                {
                    "orig_start_sec": round(s.orig_start_ms / 1000, 1),
                    "orig_end_sec": round(s.orig_end_ms / 1000, 1),
                    "fps_ratio": round(s.fps_ratio, 6),
                    "offset_sec": round(s.offset_ms / 1000, 3),
                }
                for s in segments
            ],
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
