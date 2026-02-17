# transform.py
# =====================================================
# Transform layer: pure in-memory business logic
# =====================================================

from utils import pick
import cache
from utils import (
    normalize_id,
)
from datetime import datetime, timedelta, time

def classify_taps(rows, batas_in, batas_out):
    """
    rows       : list row attendance
    batas_in   : time
    batas_out  : time

    return:
        raw_in, raw_out
    """

    if not rows:
        return None, None

    in_rows = []
    out_rows = []

    for r in rows:
        t = r["time"]

        if batas_in and t < batas_in:
            in_rows.append(r)

        elif batas_out and t > batas_out:
            out_rows.append(r)

    raw_in = min(in_rows, key=lambda x: x["time"]) if in_rows else None
    raw_out = max(out_rows, key=lambda x: x["time"]) if out_rows else None

    return raw_in, raw_out


def build_attributes(
    *,
    has_daily=False,
    is_admin=False,
    invalid_device=False,
    is_late=False,
    is_early=False,
    mode="in"
):
    # =================================================
    # LEVEL 1 — ADMINISTRATIVE AUTHORITY
    # =================================================
    if has_daily:
        return "/Adm"

    if is_admin:
        return "/Adm"

    # =================================================
    # LEVEL 2 — DEVICE VALIDATION
    # =================================================
    if invalid_device:
        return "/X"

    # =================================================
    # LEVEL 3 — TIME ANALYSIS
    # =================================================
    flags = []

    if mode == "in":
        if is_late:
            flags.append("T")
    else:
        if is_early:
            flags.append("PC")

    return "/" + "/".join(flags) if flags else None


# =====================================================
# TIME RESOLUTION
# =====================================================

def resolve_time(tap, raw):
    """
    Priority:
    1. Tapping (ADMIN)
    2. Mesin
    3. None
    """
    if tap:
        return tap.get("tm") or pick(raw, "time"), "ADMIN"
    if raw:
        return raw.get("time"), "MESIN"
    return None, "AUTO"

# =====================================================
# STATUS RESOLUTION
# =====================================================

def resolve_status(
    time_final,
    valid_device,
    pegawai_active,
    tap,
    daily
):
    """
    Final status resolver (single point of truth)
    """
    if daily:
        return daily.get("status")

    if tap:
        return tap.get("status", "HADIR")

    if not pegawai_active:
        return "ALPA"

    if not time_final:
        return "ALPA"

    if not valid_device:
        return "ALPA"

    return "HADIR"

def to_minutes(val):
    if val is None:
        return None

    # MySQL TIME → timedelta
    if isinstance(val, timedelta):
        return int(val.total_seconds() // 60)

    # datetime.time
    if isinstance(val, time):
        return val.hour * 60 + val.minute

    # datetime
    if isinstance(val, datetime):
        return val.hour * 60 + val.minute

    # string
    if isinstance(val, str):
        parts = val.split(":")
        return int(parts[0]) * 60 + int(parts[1])

    return None


def diff_minutes(t1, t2):
    m1 = to_minutes(t1)
    m2 = to_minutes(t2)

    if m1 is None or m2 is None:
        return 0

    return m1 - m2

# def diff_minutes(t1, t2):
#     fmt = "%H:%M:%S" if len(t1.split(":")) == 3 else "%H:%M"
#     d1 = datetime.strptime(t1, fmt)
#     d2 = datetime.strptime(t2, fmt)
#     return int((d1 - d2).total_seconds() / 60)

# =====================================================
# NOTES EXTRACTOR
# =====================================================
def extract_notes(daily, tap_in, tap_out):
    """
    Extract notes dari sumber override
    """

    notes_hari = daily.get("notes") if daily else None
    notes_in = tap_in.get("notes") if tap_in else None
    notes_out = tap_out.get("notes") if tap_out else None

    # normalize kosong → None
    if notes_hari == "":
        notes_hari = None
    if notes_in == "":
        notes_in = None
    if notes_out == "":
        notes_out = None

    return notes_hari, notes_in, notes_out

def build_anomaly_flags(
    att_rows,
    raw_in,
    raw_out,
    valid_device_in,
    valid_device_out,
    jadwal_masuk,
    jadwal_pulang,
    tap_in,
    tap_out
):
    flags = []

    if not att_rows:
        flags.append("NO_TAP")

    if att_rows and not raw_in and not raw_out:
        flags.append("ONLY_MIDDLE")

    if att_rows and not raw_in:
        flags.append("NO_IN")

    if att_rows and not raw_out:
        flags.append("NO_OUT")

    if raw_in and not valid_device_in:
        flags.append("INVALID_IN")

    if raw_out and not valid_device_out:
        flags.append("INVALID_OUT")

    if not jadwal_masuk and not jadwal_pulang:
        flags.append("NO_SCHEDULE")

    if tap_in or tap_out:
        flags.append("ADMIN_OVERRIDE")

    return "|".join(flags) if flags else None

# =====================================================
# MAIN TRANSFORM
# =====================================================

def process_pegawai_fast(nik, date):
    """
    Build one absensi_summaries row (NO DB ACCESS)
    Return dict ready for insert
    """

    # =================================================
    # CONTEXT
    # =================================================
    ctx = cache.get_pegawai_ctx(nik)
    pegawai_active = bool(ctx)

    unit_id = normalize_id(ctx.get("unit_id")) if ctx else None
    sub_unit_id = normalize_id(ctx.get("sub_unit_id")) if ctx else None
    hist_lokasi = ctx.get("lokasi_kerja") if ctx else None

    lokasi_kerja = cache.build_lokasi_kerja(unit_id, hist_lokasi)

    # =================================================
    # RAW ATTENDANCE (LIST SEMUA TAP)
    # =================================================
    att_rows = cache.get_attendance(nik, date)

    # =================================================
    # ABSENT & TAPPING OVERRIDE
    # =================================================
    daily = cache.get_absent(nik, date)
    tap_in = cache.get_tap(nik, date, "in")
    tap_out = cache.get_tap(nik, date, "out")

    # =================================================
    # NOTES
    # =================================================
    notes_hari, notes_in, notes_out = extract_notes(
        daily,
        tap_in,
        tap_out
    )

    # =================================================
    # JADWAL (HARUS SEBELUM CLASSIFY)
    # =================================================
    jadwal_masuk, jadwal_pulang, penalti_in, penalti_out, sumber_jadwal = cache.resolve_jadwal_from_cache(
        nik, date, unit_id, sub_unit_id
    )

    # =================================================
    # CLASSIFY TAP BERDASARKAN JADWAL
    # =================================================
    raw_in, raw_out = classify_taps(
        att_rows,
        jadwal_masuk,
        jadwal_pulang
    )

    # =================================================
    # TIME FINAL (SETELAH CLASSIFY)
    # =================================================
    time_in_final, time_in_source = resolve_time(tap_in, raw_in)
    time_out_final, time_out_source = resolve_time(tap_out, raw_out)

    # =================================================
    # DEVICE RESOLUTION
    # =================================================
    def resolve_device(raw, source):
        if source == "ADMIN":
            return "Administratif", True, None

        if not raw:
            return None, False, None

        device_id = str(raw.get("device_id")).strip() if raw.get("device_id") else None

        if device_id is not None and not isinstance(device_id, str):
            raise ValueError(
                f"Invalid device_id type: {type(device_id)} | value={device_id}"
            )

        desc = cache.get_device_desc(unit_id, device_id)
        valid = cache.is_device_valid(device_id, lokasi_kerja)
        return desc, valid, device_id

    device_desc_in, valid_device_in, device_id_in = resolve_device(raw_in, time_in_source)
    device_desc_out, valid_device_out, device_id_out = resolve_device(raw_out, time_out_source)

    # =================================================
    # STATUS
    # =================================================
    status_masuk = resolve_status(
        time_in_final, valid_device_in, pegawai_active, tap_in, daily
    )

    status_pulang = resolve_status(
        time_out_final, valid_device_out, pegawai_active, tap_out, daily
    )

    status_hari = (
        daily["status"] if daily else
        "ALPA" if not pegawai_active else
        "HADIR" if status_masuk != "ALPA" or status_pulang != "ALPA" else
        "ALPA"
    )

    # =================================================
    # PENALTI CALCULATION
    # =================================================
    late_minutes = 0
    early_minutes = 0

    try:
        penalti_in = int(penalti_in) if penalti_in else 0
        penalti_out = int(penalti_out) if penalti_out else 0
    except:
        penalti_in = 0
        penalti_out = 0

    # LATE
    if time_in_final and jadwal_masuk:
        diff = diff_minutes(time_in_final, jadwal_masuk)
        if diff > 0:
            late_minutes = diff
    elif penalti_in:
        late_minutes = penalti_in

    # EARLY
    if time_out_final and jadwal_pulang:
        diff = diff_minutes(jadwal_pulang, time_out_final)
        if diff > 0:
            early_minutes = diff
    elif penalti_out:
        early_minutes = penalti_out

    # =================================================
    # ATTRIBUTES
    # =================================================
    attribute_in = build_attributes(
        has_daily=bool(daily),
        is_admin=bool(tap_in),
        invalid_device=not valid_device_in,
        is_late=late_minutes > 0,
        mode="in"
    )

    attribute_out = build_attributes(
        has_daily=bool(daily),
        is_admin=bool(tap_out),
        invalid_device=not valid_device_out,
        is_early=early_minutes > 0,
        mode="out"
    )

    # =================================================
    # FINAL NOTE
    # =================================================
    final_note = (
        f"DAILY_NOTE:{daily['status']}" if daily else
        "ADMIN_OVERRIDE" if tap_in or tap_out else
        "NO_ACTIVE_HISTORY" if not pegawai_active else
        "INVALID_DEVICE" if not valid_device_in or not valid_device_out else
        "AUTO"
    )

    filename_in = raw_in.get("filename") if time_in_source == "MESIN" else None
    filename_out = raw_out.get("filename") if time_out_source == "MESIN" else None

    anomaly_flags = build_anomaly_flags(
        att_rows,
        raw_in,
        raw_out,
        valid_device_in,
        valid_device_out,
        jadwal_masuk,
        jadwal_pulang,
        tap_in,
        tap_out
    )

    # =================================================
    # BUILD FINAL ROW
    # =================================================
    return {
        "nik": nik,
        "date": date,

        "time_in": pick(raw_in, "time"),
        "time_out": pick(raw_out, "time"),

        "time_in_final": time_in_final,
        "time_out_final": time_out_final,

        "time_in_source": time_in_source,
        "time_out_source": time_out_source,

        "status_masuk_final": status_masuk,
        "status_pulang_final": status_pulang,
        "status_hari_final": status_hari,

        "jadwal_masuk": jadwal_masuk,
        "jadwal_pulang": jadwal_pulang,
        "sumber_jadwal": sumber_jadwal,

        "device_desc_in": device_desc_in,
        "device_id_in": device_id_in,

        "device_desc_out": device_desc_out,
        "device_id_out": device_id_out,

        "valid_device_in": valid_device_in,
        "valid_device_out": valid_device_out,

        "lokasi_kerja": lokasi_kerja,
        "valid_devices": lokasi_kerja,
        "final_note": final_note,
        "is_final": 1,

        "filename_in": filename_in,
        "filename_out": filename_out,

        "late_minutes": late_minutes,
        "early_minutes": early_minutes,

        "attribute_in": attribute_in,
        "attribute_out": attribute_out,

        "notes_hari": notes_hari,
        "notes_in": notes_in,
        "notes_out": notes_out,
        
        "anomaly_flags": anomaly_flags,

    }
