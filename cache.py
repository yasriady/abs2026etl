# cache.py
# =====================================================
# In-memory cache for ETL absensi
# =====================================================

from collections import defaultdict
from utils import (
    normalize_nik,
    normalize_csv,
    hari_int,
    hari_str,
)

# =====================================================
# ATTENDANCE CACHE
# =====================================================
# Key: (nik, date)
# Value: {"in": row, "out": row}

ATT_MAP = {}

def add_attendance(nik, date, row):
    nik = normalize_nik(nik)
    key = (nik, date)

    if key not in ATT_MAP:
        ATT_MAP[key] = {"in": row, "out": row}
    else:
        # first = in, last = out (query already ORDER BY time)
        ATT_MAP[key]["out"] = row

def get_attendance(nik, date):
    return ATT_MAP.get((normalize_nik(nik), date))

# =====================================================
# PEGAWAI / HISTORY CACHE
# =====================================================
# Key: nik
# Value: dict history aktif

PEGAWAI_CTX = {}

def add_pegawai_ctx(row):
    nik = normalize_nik(row["nik"])
    PEGAWAI_CTX[nik] = {
        "unit_id": row.get("id_unit"),
        "sub_unit_id": row.get("id_sub_unit"),
        "lokasi_kerja": normalize_csv(row.get("lokasi_kerja")),
    }

def get_pegawai_ctx(nik):
    return PEGAWAI_CTX.get(normalize_nik(nik))

# =====================================================
# DEVICE CACHE
# =====================================================
# DEVICE_BY_UNIT[unit_id][device_id] = desc

DEVICE_BY_UNIT = defaultdict(dict)

def add_device(row):
    unit_id = row.get("unit_id")
    device_id = row.get("device_id")
    desc = row.get("desc")

    if unit_id and device_id:
        DEVICE_BY_UNIT[unit_id][str(device_id)] = desc

def get_device_desc(unit_id, device_id):
    if not unit_id or not device_id:
        return None
    return DEVICE_BY_UNIT.get(unit_id, {}).get(str(device_id))

def build_lokasi_kerja(unit_id, hist_lokasi):
    """
    Source of truth lokasi kerja:
    - semua device_id milik unit
    - + lokasi_kerja dari history (jika ada)
    """
    lokasi = set()

    if unit_id and unit_id in DEVICE_BY_UNIT:
        lokasi.update(DEVICE_BY_UNIT[unit_id].keys())

    if hist_lokasi:
        lokasi.update(x.strip() for x in hist_lokasi.split(",") if x.strip())

    return ",".join(sorted(lokasi)) if lokasi else None

def is_device_valid(device_id, lokasi_kerja):
    if not device_id or not lokasi_kerja:
        return False
    allowed = {x.strip() for x in lokasi_kerja.split(",") if x.strip()}
    return str(device_id) in allowed

# =====================================================
# ABSENT / DAILY NOTE CACHE
# =====================================================
# ABSENT_MAP[(nik, date)] = row

ABSENT_MAP = {}

def add_absent(row):
    key = (normalize_nik(row["nik"]), row["date"])
    ABSENT_MAP[key] = row

def get_absent(nik, date):
    return ABSENT_MAP.get((normalize_nik(nik), date))

# =====================================================
# TAPPING NOTE CACHE
# =====================================================
# TAP_MAP[(nik, date, "in"|"out")] = row

TAP_MAP = {}

def add_tap(row):
    key = (normalize_nik(row["nik"]), row["date"], row["hour"])
    TAP_MAP[key] = row

def get_tap(nik, date, hour):
    return TAP_MAP.get((normalize_nik(nik), date, hour))

# =====================================================
# JADWAL CACHE
# =====================================================

# Jadwal pegawai: (nik, date)
JADWAL_PEGAWAI = {}

# Jadwal sub unit: (sub_unit_id, hari_int|hari_str)
JADWAL_SUB_UNIT = {}

# Jadwal unit: (unit_id, hari_int|hari_str)
JADWAL_UNIT = {}

# Jadwal dinas: hari_int|hari_str
JADWAL_DINAS = {}

def add_jadwal_pegawai(row):
    JADWAL_PEGAWAI[(normalize_nik(row["nik"]), row["date"])] = row

def add_jadwal_sub_unit(row):
    key = (row["sub_unit_id"], row["hari"])
    JADWAL_SUB_UNIT[key] = row

def add_jadwal_unit(row):
    key = (row["unit_id"], row["hari"])
    JADWAL_UNIT[key] = row

def add_jadwal_dinas(row):
    JADWAL_DINAS[row["hari"]] = row

def resolve_jadwal_from_cache(nik, date, unit_id, sub_unit_id):
    """
    Final jadwal resolver (NO DB)
    Priority:
    1. Pegawai
    2. Sub Unit
    3. Unit
    4. Dinas
    """
    # 1️⃣ Pegawai
    row = JADWAL_PEGAWAI.get((normalize_nik(nik), date))
    if row:
        return row["jam_masuk"], row["jam_pulang"], "pegawai"

    hi = hari_int(date)
    hs = hari_str(date)

    # 2️⃣ Sub Unit
    if sub_unit_id:
        row = (
            JADWAL_SUB_UNIT.get((sub_unit_id, hi)) or
            JADWAL_SUB_UNIT.get((sub_unit_id, hs))
        )
        if row:
            return row["jam_masuk"], row["jam_pulang"], "sub_unit"

    # 3️⃣ Unit
    if unit_id:
        row = (
            JADWAL_UNIT.get((unit_id, hi)) or
            JADWAL_UNIT.get((unit_id, hs))
        )
        if row:
            return row["jam_masuk"], row["jam_pulang"], "unit"

    # 4️⃣ Dinas
    row = JADWAL_DINAS.get(hi) or JADWAL_DINAS.get(hs)
    if row:
        return row["jam_masuk"], row["jam_pulang"], "dinas"

    return None, None, None
