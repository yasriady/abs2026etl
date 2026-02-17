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
from utils import (
    normalize_id,
)

# =====================================================
# ATTENDANCE CACHE
# =====================================================
# Key: (nik, date)
# Value: [row,row,row]   # list semua tap

ATT_MAP = {}

def add_attendance(nik, date, row):
    nik = normalize_nik(nik)
    key = (nik, date)

    if key not in ATT_MAP:
        ATT_MAP[key] = [row]
    else:
        ATT_MAP[key].append(row)

def get_attendance(nik, date):
    return ATT_MAP.get((normalize_nik(nik), date), [])

# =====================================================
# PEGAWAI / HISTORY CACHE
# =====================================================
# Key: nik
# Value: dict history aktif

PEGAWAI_CTX = {}

def add_pegawai_ctx(row):
    nik = normalize_nik(row["nik"])

    raw_unit = row.get("id_unit")
    raw_sub = row.get("id_sub_unit")

    unit_id = str(raw_unit).strip() if raw_unit is not None else None
    sub_unit_id = str(raw_sub).strip() if raw_sub is not None else None

    if nik not in PEGAWAI_CTX:
        PEGAWAI_CTX[nik] = {
            "unit_id": unit_id,
            "sub_unit_id": sub_unit_id,
            "lokasi_kerja": normalize_csv(row.get("lokasi_kerja")),
    }

def get_pegawai_ctx(nik):
    return PEGAWAI_CTX.get(normalize_nik(nik))

# =====================================================
# DEVICE CACHE
# =====================================================
# DEVICE_BY_UNIT[unit_id][device_id] = desc

DEVICE_BY_UNIT = defaultdict(dict)

def _norm_device_id(val):
    """
    Normalize device_id as pure string.
    Never cast to int.
    """
    if val is None:
        return None
    return str(val).strip()

# Hapus satu fungsi add_device, perbaiki yang tersisa
def add_device(row):
    """Load device into DEVICE_BY_UNIT[unit_id][device_id]"""

    raw_unit = row.get("unit_id")
    if raw_unit is None:
        return

    device_id = _norm_device_id(row.get("device_id"))
    desc = row.get("desc")

    if not device_id:
        return

    # split multi-unit
    unit_ids = [
        u.strip()
        for u in str(raw_unit).split(",")
        if u.strip()
    ]

    for unit_id in unit_ids:
        DEVICE_BY_UNIT[unit_id][device_id] = {
            "id": row["id"],
            "desc": desc
        }


def get_device_desc(unit_id, device_id):
    device_id = _norm_device_id(device_id)
    if not unit_id or not device_id:
        return None
    d = DEVICE_BY_UNIT.get(unit_id, {}).get(device_id)
    return d["desc"] if d else None


def build_lokasi_kerja(unit_id, hist_lokasi):
    lokasi = set()

    if unit_id:
        for uid in str(unit_id).split(","):
            uid = uid.strip()
            if uid in DEVICE_BY_UNIT:
                lokasi.update(
                    DEVICE_BY_UNIT[uid].keys()
                )

    if hist_lokasi:
        lokasi.update(
            _norm_device_id(x)
            for x in hist_lokasi.split(",")
            if x.strip()
        )

    return ",".join(sorted(lokasi)) if lokasi else None

def is_device_valid(device_id, lokasi_kerja):
    device_id = _norm_device_id(device_id)

    if not device_id or not lokasi_kerja:
        return False

    allowed = {
        _norm_device_id(x)
        for x in lokasi_kerja.split(",")
        if x.strip()
    }

    return device_id in allowed

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

# Perbaiki add_jadwal_sub_unit dan add_jadwal_unit
def add_jadwal_sub_unit(row):
    sub_unit_id = normalize_id(row["sub_unit_id"])
    if sub_unit_id is None:
        return
    key = (sub_unit_id, row["hari"])
    JADWAL_SUB_UNIT[key] = row

def add_jadwal_unit(row):
    unit_id = normalize_id(row["unit_id"])
    if unit_id is None:
        return
    key = (unit_id, row["hari"])
    JADWAL_UNIT[key] = row

def add_jadwal_dinas(row):
    """Add dinas schedule to cache"""
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
        # return row["jam_masuk"], row["jam_pulang"], "pegawai"  
        return (
            row["jam_masuk"],
            row["jam_pulang"],
            row.get("penalti_tidak_tap_in"),
            row.get("penalti_tidak_tap_out"),
            "pegawai"
        )


    hi = hari_int(date)
    hs = hari_str(date)

    # 2️⃣ Sub Unit
    if sub_unit_id:
        # Normalize to string for lookup
        sub_unit_id_str = str(sub_unit_id)  # 👈 Pastikan string
        row = (
            JADWAL_SUB_UNIT.get((sub_unit_id_str, hi)) or
            JADWAL_SUB_UNIT.get((sub_unit_id_str, hs))
        )
        if row:
            # return row["jam_masuk"], row["jam_pulang"], "sub_unit"
            return (
                row["jam_masuk"],
                row["jam_pulang"],
                row.get("penalti_tidak_tap_in"),
                row.get("penalti_tidak_tap_out"),
                "sub_unit"
            )
            
    # 3️⃣ Unit
    if unit_id:
        # Normalize to string for lookup
        unit_id_str = str(unit_id)  # 👈 Pastikan string
        row = (
            JADWAL_UNIT.get((unit_id_str, hi)) or
            JADWAL_UNIT.get((unit_id_str, hs))
        )
        if row:
            # return row["jam_masuk"], row["jam_pulang"], "unit"
            return (
                row["jam_masuk"],
                row["jam_pulang"],
                row.get("penalti_tidak_tap_in"),
                row.get("penalti_tidak_tap_out"),
                "unit"
            )

    # 4️⃣ Dinas
    row = JADWAL_DINAS.get(hi) or JADWAL_DINAS.get(hs)
    if row:
        # return row["jam_masuk"], row["jam_pulang"], "dinas"
        return (
            row["jam_masuk"],
            row["jam_pulang"],
            row.get("penalti_tidak_tap_in"),
            row.get("penalti_tidak_tap_out"),
            "dinas"
        )
    return None, None, None, None, None
