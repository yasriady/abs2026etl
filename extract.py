# extract.py
# =====================================================
# Extract layer: load data from DB into cache (once)
# =====================================================

from utils import log, time_block, normalize_nik
import cache

# =====================================================
# ATTENDANCE (ATT_DB)
# =====================================================

def extract_attendance(att_db, date, stats=None):
    """
    Load raw attendance for a date into ATT_MAP
    """
    with time_block("extract_attendance", stats):
        with att_db.cursor() as cur:
            cur.execute("""
                SELECT
                    TRIM(nik) AS nik,
                    DATE(`date`) AS tanggal,
                    `time`,
                    device_id,
                    filename,
                    lat,
                    `long`
                FROM DB_ATT_tbl_attendance
                WHERE DATE(`date`) = %s
                ORDER BY nik, `time`
            """, [date])

            for row in cur.fetchall():
                nik = normalize_nik(row["nik"])
                cache.add_attendance(nik, row["tanggal"], row)

        log(f"Attendance loaded: {len(cache.ATT_MAP)} keys")

# =====================================================
# PEGAWAI / HISTORY (MAIN_DB)
# =====================================================

def extract_pegawai_ctx(main_db, date, unit_id=None, sub_unit_id=None, stats=None):
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

            if unit_id:
                sql += " AND ph.id_unit = %s"
                params.append(unit_id)

            if sub_unit_id:
                sql += " AND ph.id_sub_unit = %s"
                params.append(sub_unit_id)

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
                SELECT unit_id, device_id, `desc`
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
                SELECT nik, date, jam_masuk, jam_pulang
                FROM jadwal_pegawais
                WHERE date = %s
            """, [date])
            for row in cur.fetchall():
                cache.add_jadwal_pegawai(row)

            # Jadwal Sub Unit
            cur.execute("""
                SELECT sub_unit_id, hari, jam_masuk, jam_pulang
                FROM jadwal_sub_units
                WHERE (start_date IS NULL OR start_date <= %s)
                  AND (end_date IS NULL OR end_date >= %s)
            """, [date, date])
            for row in cur.fetchall():
                cache.add_jadwal_sub_unit(row)

            # Jadwal Unit
            cur.execute("""
                SELECT unit_id, hari, jam_masuk, jam_pulang
                FROM jadwal_units
                WHERE (start_date IS NULL OR start_date <= %s)
                  AND (end_date IS NULL OR end_date >= %s)
            """, [date, date])
            for row in cur.fetchall():
                cache.add_jadwal_unit(row)

            # Jadwal Dinas
            cur.execute("""
                SELECT hari, jam_masuk, jam_pulang
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

def extract_all(main_db, aux_db, att_db, date, unit_id=None, sub_unit_id=None, stats=None):
    """
    Run all extract steps for a date
    """
    extract_attendance(att_db, date, stats)
    extract_pegawai_ctx(main_db, date, unit_id, sub_unit_id, stats)
    extract_devices(aux_db, stats)
    extract_absent(aux_db, date, stats)
    extract_tapping(aux_db, date, stats)
    extract_jadwal(main_db, date, stats)
