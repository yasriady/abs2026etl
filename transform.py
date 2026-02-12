# transform.py
# =====================================================
# Transform layer: pure in-memory business logic
# =====================================================

from utils import pick
import cache

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

    unit_id = ctx.get("unit_id") if ctx else None
    sub_unit_id = ctx.get("sub_unit_id") if ctx else None
    hist_lokasi = ctx.get("lokasi_kerja") if ctx else None

    lokasi_kerja = cache.build_lokasi_kerja(unit_id, hist_lokasi)

    # =================================================
    # RAW ATTENDANCE
    # =================================================
    att = cache.get_attendance(nik, date) or {}
    raw_in = att.get("in")
    raw_out = att.get("out")

    # =================================================
    # ABSENT & TAPPING
    # =================================================
    daily = cache.get_absent(nik, date)
    tap_in = cache.get_tap(nik, date, "in")
    tap_out = cache.get_tap(nik, date, "out")

    # =================================================
    # TIME FINAL
    # =================================================
    time_in_final, time_in_source = resolve_time(tap_in, raw_in)
    time_out_final, time_out_source = resolve_time(tap_out, raw_out)

    # =================================================
    # DEVICE
    # =================================================
    def resolve_device(raw, source):
        if source == "ADMIN":
            return "Administratif", True, None

        if not raw:
            return None, False, None

        device_id = raw.get("device_id")
        desc = cache.get_device_desc(unit_id, device_id)
        valid = cache.is_device_valid(device_id, lokasi_kerja)
        return desc, valid, device_id

    device_desc_in, valid_device_in, device_id_in = resolve_device(raw_in, time_in_source)
    device_desc_out, valid_device_out, device_id_out = resolve_device(raw_out, time_out_source)

    # =================================================
    # JADWAL
    # =================================================
    jadwal_masuk, jadwal_pulang, sumber_jadwal = cache.resolve_jadwal_from_cache(
        nik, date, unit_id, sub_unit_id
    )

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

    final_note = (
        f"DAILY_NOTE:{daily['status']}" if daily else
        "ADMIN_OVERRIDE" if tap_in or tap_out else
        "NO_ACTIVE_HISTORY" if not pegawai_active else
        "INVALID_DEVICE" if not valid_device_in or not valid_device_out else
        "AUTO"
    )
    
    filename_in = raw_in.get("filename") if time_in_source == "MESIN" else None
    filename_out = raw_out.get("filename") if time_out_source == "MESIN" else None


    # =================================================
    # BUILD ROW
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
        "final_note": final_note,
        "is_final": 1,
        
        "filename_in": filename_in,
        "filename_out": filename_out,

    }
