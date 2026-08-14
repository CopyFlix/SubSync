"""
التعديلات الجوهرية على run_sync.py - يستبدل منطق التحميل الجزئي/التخمين
وإعادة بناء التحويل الخطي بالكامل.

الفكرة: بدل ما نحمّل جزء من الفيديو ونخمّن حجمه ونحلل أول 20 دقيقة بس،
خلي ffmpeg يسحب الصوت من الرابط مباشرة (streaming) للحلقة/الفيلم كامل،
وخلي alass يشتغل على ملف الترجمة كامل بدون --no-splits عشان يقدر
يكتشف أي قطع/حذف مشاهد بنفسه (split-based alignment)، بدل ما نفرض
عليه تحويل خطي واحد يفشل في أي حالة غير "انزياح ثابت بسيط".
"""

import asyncio
import os

# ============================================================
# 1) استخراج الصوت الكامل مباشرة من الرابط (بدون تحميل يدوي/تخمين حجم)
# ============================================================
ALASS_TIMEOUT_SEC = 1800  # 30 دقيقة - عدّلها حسب مدة الحلقة/الفيلم المتوقعة
FFMPEG_TIMEOUT_SEC = 1800


async def run_subprocess_async(cmd, timeout):
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise JobError("انتهت مهلة تنفيذ العملية (timeout)")
    return proc.returncode, stdout.decode(errors="replace"), stderr.decode(errors="replace")


class JobError(Exception):
    pass


async def extract_full_audio_from_url_async(video_url: str, out_wav_path: str) -> None:
    """
    يخلي ffmpeg يقرأ الفيديو من الرابط مباشرة (streaming) ويطلّع الصوت
    الكامل بدون ما نحمّل الفيديو محليًا أو نخمّن حجمه. ده أبسط وأدق
    وأقل عرضة للأخطاء من منطق probe/estimate القديم، ولازم نستخرج
    الحلقة/الفيلم *كامل* عشان alass يقدر يشوف أي قطع مشهد ممكن يحصل
    في أي نقطة، مش بس أول 20 دقيقة.
    """
    cmd = [
        "ffmpeg", "-y", "-v", "error",
        # فلاجات مقاومة انقطاع الشبكة - مهمة لما بنقرأ من رابط مباشر
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-i", video_url,
        "-vn", "-ac", "1", "-ar", "16000",
        out_wav_path,
    ]
    code, _, stderr = await run_subprocess_async(cmd, timeout=FFMPEG_TIMEOUT_SEC)
    if code != 0 or not os.path.exists(out_wav_path) or os.path.getsize(out_wav_path) < 1000:
        raise JobError(f"فشل استخراج الصوت الكامل من الرابط: {stderr[:500]}")


# ============================================================
# 2) مزامنة الترجمة الكاملة مباشرة (بدون كروب / بدون إعادة بناء يدوي)
# ============================================================
async def sync_subtitle_full_async(audio_wav_path: str, subtitle_in_path: str, subtitle_out_path: str) -> None:
    """
    نشغّل alass على الملف الكامل *بدون* --no-splits عشان يقدر يعمل
    split عند أي قطع مشهد يكتشفه (بالظبط الحالة اللي وصفتها: مشهد
    محذوف/مقصوص يخلي الترجمة اللي بعده مش متزامنة). الافتراضي
    (split-penalty الافتراضي) بيوازن بين تصحيح القطع وعدم إدخال
    splits غير ضرورية.

    ملحوظة: من غير --no-splits العملية أبطأ شوية من قبل، لكنها هي
    الطريقة الصحيحة الوحيدة لحل المشكلة اللي بتوصفها. لو حابب توازن
    سرعة/دقة تقدر تضيف --split-penalty برقم (5-20 مقترح في توثيق
    alass) بدل ما تمنع الـ splits خالص.
    """
    cmd = ["alass-cli", audio_wav_path, subtitle_in_path, subtitle_out_path]
    code, _, stderr = await run_subprocess_async(cmd, timeout=ALASS_TIMEOUT_SEC)
    if code != 0:
        raise JobError(f"خطأ في alass-cli: {stderr}")
    if not os.path.exists(subtitle_out_path):
        raise JobError("alass-cli لم يُنتج ملف مخرجات")


# ============================================================
# استخدامها بدل الكتلة القديمة في main():
#
#   audio_path = os.path.join(work_dir, "full_audio.wav")
#   await extract_full_audio_from_url_async(video_url, audio_path)
#
#   raw_subtitle_path = os.path.join(work_dir, f"input.{fmt}")
#   subs.save(raw_subtitle_path, format_=fmt)
#
#   synced_path = os.path.join(work_dir, f"synced.{fmt}")
#   await sync_subtitle_full_async(audio_path, raw_subtitle_path, synced_path)
#
#   with open(synced_path, "rb") as f:
#       final_bytes = f.read()
#   gz_bytes = gzip.compress(final_bytes, compresslevel=9)
#
# احذف تمامًا: crop_subtitle, apply_offset_and_fps, compute_linear_transform,
# download_and_extract_target_duration_async, check_range_support_async,
# download_range_async, get_media_duration_seconds_async, extract_audio_async,
# وكل قيم PROBE_MB / SAFETY_MARGIN / MAX_CHUNK_MB / TARGET_AUDIO_SEC.
# offset_seconds و fps_ratio في D1 بقيت غير قابلة للحساب المباشر (لأن
# التصحيح بقى متعدد النقاط مش قيمة واحدة) - خزّن NULL/0 لهم أو احسب
# متوسط تقريبي للعرض فقط، مش للتصحيح.
