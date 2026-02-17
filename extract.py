# extract.py
# =====================================================
# Extract layer: load data from DB into cache (once)
# =====================================================

from utils import log, time_block, normalize_nik
import cache
from utils import (
    normalize_id,
)
# =====================================================
# ATTENDANCE (ATT_DB)
# =====================================================

def extract_attendance(att_db, date, nik=None, stats=None):

    with time_block("extract_attendance", stats):
        with att_db.cursor() as cur:

            sql = """
                SELECT
                    TRIM(nik) AS nik,
                    DATE(`date`) AS tanggal,
                    `time`,
                    device_id,
                    filename,
                    lat,
                    `long`
                FROM DB_ATT_tbl_attendance
                WHERE `date` >= %s
                AND `date` < %s
            """

            params = [
                f"{date} 00:00:00",
                f"{date} 23:59:59"
            ]

            if nik:
                sql += " AND TRIM(nik) = %s"
                params.append(nik)

            sql += " ORDER BY nik, `time`"

            cur.execute(sql, params)

            for row in cur.fetchall():
                row["device_id"] = str(row["device_id"]).strip() if row["device_id"] else None

                row_nik = normalize_nik(row["nik"])
                cache.add_attendance(row_nik, row["tanggal"], row)

        log(f"Attendance loaded: {len(cache.ATT_MAP)} keys")

# =====================================================
# PEGAWAI / HISTORY (MAIN_DB)
# =====================================================

def extract_pegawai_ctx(main_db, date, unit_id=None, sub_unit_id=None, nik=None, stats=None):
    """
    Load active pegawai histories for date
    """
    with time_block("extract_pegawai", stats):
        with main_db.cursor() as cur:
            sql = """
                SELECT
                    mp.nik,
                    ph.id_unit,
                    ph.id_sub_unit,
                    ph.lokasi_kerja
                FROM pegawai_histories ph
                JOIN master_pegawais mp ON mp.id = ph.master_pegawai_id
                WHERE ph.begin_date <= %s
                  AND (ph.end_date IS NULL OR ph.end_date >= %s)
            """
            params = [date, date]

            if unit_id is not None:
                sql += " AND ph.id_unit = %s"
                params.append(normalize_id(unit_id))
            
            if sub_unit_id is not None:
                sql += " AND ph.id_sub_unit = %s"
                params.append(normalize_id(sub_unit_id))

            if nik:
                sql += " AND mp.nik = %s"
                params.append(nik)

            cur.execute(sql, params)

            for row in cur.fetchall():
                cache.add_pegawai_ctx(row)

        log(f"Pegawai ctx loaded: {len(cache.PEGAWAI_CTX)}")

# =====================================================
# DEVICE (AUX_DB)
# =====================================================

def extract_devices(aux_db, stats=None):
    """
    Load all devices into DEVICE_BY_UNIT
    """
    with time_block("extract_devices", stats):
        with aux_db.cursor() as cur:
            cur.execute("""
                SELECT id, unit_id, device_id, `desc`
                FROM tbl_device
            """)
            for row in cur.fetchall():
                cache.add_device(row)

        log(f"Devices loaded: {sum(len(v) for v in cache.DEVICE_BY_UNIT.values())}")

# =====================================================
# ABSENT / DAILY NOTE (AUX_DB)
# =====================================================

def extract_absent(aux_db, date, stats=None):
    with time_block("extract_absent", stats):
        with aux_db.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM tbl_absent
                WHERE `date` = %s
            """, [date])

            for row in cur.fetchall():
                cache.add_absent(row)

        log(f"Absent loaded: {len(cache.ABSENT_MAP)}")

# =====================================================
# TAPPING NOTE (AUX_DB)
# =====================================================

def extract_tapping(aux_db, date, stats=None):
    with time_block("extract_tapping", stats):
        with aux_db.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM tbl_absent_hourly
                WHERE `date` = %s
            """, [date])

            for row in cur.fetchall():
                cache.add_tap(row)

        log(f"Tapping loaded: {len(cache.TAP_MAP)}")

# =====================================================
# JADWAL (MAIN_DB)
# =====================================================

def extract_jadwal(main_db, date, stats=None):
    """
    Load all relevant jadwal into cache (NO filtering per pegawai)
    """
    with time_block("extract_jadwal", stats):
        with main_db.cursor() as cur:

            # Jadwal Pegawai
            cur.execute("""
                SELECT nik, date, jam_masuk, jam_pulang,
                    penalti_tidak_tap_in,
                    penalti_tidak_tap_out
                FROM jadwal_pegawais
                WHERE date = %s
            """, [date])
            for row in cur.fetchall():
                cache.add_jadwal_pegawai(row)

            # Jadwal Sub Unit
            cur.execute("""
                SELECT sub_unit_id, hari, jam_masuk, jam_pulang,
                    penalti_tidak_tap_in,
                    penalti_tidak_tap_out
                FROM jadwal_sub_units
                WHERE (start_date IS NULL OR start_date <= %s)
                  AND (end_date IS NULL OR end_date >= %s)
            """, [date, date])
            for row in cur.fetchall():
                cache.add_jadwal_sub_unit(row)

            # Jadwal Unit
            cur.execute("""
                SELECT unit_id, hari, jam_masuk, jam_pulang,
                    penalti_tidak_tap_in,
                    penalti_tidak_tap_out                                
                FROM jadwal_units
                WHERE (start_date IS NULL OR start_date <= %s)
                  AND (end_date IS NULL OR end_date >= %s)
            """, [date, date])
            for row in cur.fetchall():
                cache.add_jadwal_unit(row)

            # Jadwal Dinas
            cur.execute("""
                SELECT hari, jam_masuk, jam_pulang,
                    penalti_tidak_tap_in,
                    penalti_tidak_tap_out
                FROM jadwal_dinas
                WHERE (start_date IS NULL OR start_date <= %s)
                  AND (end_date IS NULL OR end_date >= %s)
            """, [date, date])
            for row in cur.fetchall():
                cache.add_jadwal_dinas(row)

        log(
            f"Jadwal loaded: "
            f"pegawai={len(cache.JADWAL_PEGAWAI)} "
            f"sub_unit={len(cache.JADWAL_SUB_UNIT)} "
            f"unit={len(cache.JADWAL_UNIT)} "
            f"dinas={len(cache.JADWAL_DINAS)}"
        )

# =====================================================
# MASTER EXTRACTOR
# =====================================================

def extract_all(main_db, aux_db, att_db, date, unit_id=None, sub_unit_id=None, nik=None, stats=None):
    """
    Run all extract steps for a date
    """
    extract_attendance(att_db, date, stats)
    extract_pegawai_ctx(main_db, date, unit_id, sub_unit_id, nik, stats)
    extract_devices(aux_db, stats)
    extract_absent(aux_db, date, stats)
    extract_tapping(aux_db, date, stats)
    extract_jadwal(main_db, date, stats)
