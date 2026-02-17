# utils.py
# =====================================================
# Shared utilities for ETL absensi
# =====================================================

import time
from datetime import datetime, timedelta
from contextlib import contextmanager

# =====================================================
# LOGGING
# =====================================================

def log(msg: str):
    """Standard log with timestamp"""
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")

def log_warn(msg: str):
    print(f"[WARN] {msg}")

def log_error(msg: str):
    print(f"[ERROR] {msg}")

# =====================================================
# TIMING / PROFILING
# =====================================================

@contextmanager
def time_block(label: str, stats: dict | None = None):
    """
    Usage:
        with time_block("extract", stats):
            ...
    """
    start = time.perf_counter()
    yield
    elapsed = (time.perf_counter() - start) * 1000  # ms
    if stats is not None:
        stats[f"{label}_ms"] = round(elapsed, 2)
    log(f"[TIMER] {label} = {elapsed:.2f} ms")

# =====================================================
# DATE UTILITIES
# =====================================================

def parse_date(date_str: str):
    """Parse YYYY-MM-DD to date"""
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def date_range(start, end):
    """Yield date from start to end (inclusive)"""
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)

def hari_str(date):
    """Python weekday → nama hari (db tolerant)"""
    return [
        "senin", "selasa", "rabu",
        "kamis", "jumat", "sabtu", "minggu"
    ][date.weekday()]

def hari_int(date):
    """Python weekday → db int (1–7)"""
    return date.weekday() + 1

# =====================================================
# SAFE DICT HELPERS
# =====================================================

def pick(row: dict | None, key: str, default=None):
    """Safe dict getter"""
    if not row:
        return default
    return row.get(key, default)

def first_or_none(seq):
    return seq[0] if seq else None

def last_or_none(seq):
    return seq[-1] if seq else None

# =====================================================
# BATCHING
# =====================================================

def chunked(iterable, size: int):
    """
    Yield list chunks from iterable.
    """
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

# =====================================================
# NORMALIZATION
# =====================================================

def normalize_nik(nik):
    """Trim & normalize NIK"""
    return nik.strip() if isinstance(nik, str) else nik

def normalize_csv(value: str | None):
    """Normalize comma separated string to sorted csv"""
    if not value:
        return None
    parts = {x.strip() for x in value.split(",") if x.strip()}
    return ",".join(sorted(parts)) if parts else None

# utils.py
def normalize_id(id_value):
    """
    Normalize ID to string or None.
    Critical for consistent cache lookups.
    """
    if id_value is None:
        return None
    return str(id_value).strip()