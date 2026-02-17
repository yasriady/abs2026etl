# load.py
# =====================================================
# Load layer: bulk upsert into absensi_summaries
# =====================================================

from utils import log, time_block, chunked

# =====================================================
# SQL TEMPLATE
# =====================================================

UPSERT_SQL = """
INSERT INTO absensi_summaries (
    nik, date,

    time_in, time_out,
    time_in_final, time_out_final,
    time_in_source, time_out_source,

    status_masuk_final,
    status_pulang_final,
    status_hari_final,

    jadwal_masuk, jadwal_pulang, sumber_jadwal,

    device_desc_in, device_id_in,
    device_desc_out, device_id_out,

    filename_in,
    filename_out,

    valid_device_in, valid_device_out,
    lokasi_kerja,
    valid_devices,
    final_note,
    is_final,
    late_minutes,
    early_minutes,
    attribute_in,
    attribute_out,
    
    notes_hari,
    notes_in,
    notes_out,
    anomaly_flags
    
) VALUES (
    %(nik)s, %(date)s,

    %(time_in)s, %(time_out)s,
    %(time_in_final)s, %(time_out_final)s,
    %(time_in_source)s, %(time_out_source)s,

    %(status_masuk_final)s,
    %(status_pulang_final)s,
    %(status_hari_final)s,

    %(jadwal_masuk)s, %(jadwal_pulang)s, %(sumber_jadwal)s,

    %(device_desc_in)s, %(device_id_in)s,
    %(device_desc_out)s, %(device_id_out)s,

    %(filename_in)s,
    %(filename_out)s,

    %(valid_device_in)s, %(valid_device_out)s,
    %(lokasi_kerja)s,
    %(valid_devices)s,
    %(final_note)s,
    %(is_final)s,
    %(late_minutes)s,
    %(early_minutes)s,
    %(attribute_in)s,
    %(attribute_out)s,
    
    %(notes_hari)s,
    %(notes_in)s,
    %(notes_out)s,
    %(anomaly_flags)s

)
ON DUPLICATE KEY UPDATE
    time_in_final       = VALUES(time_in_final),
    time_out_final      = VALUES(time_out_final),

    time_in_source      = VALUES(time_in_source),
    time_out_source     = VALUES(time_out_source),

    status_masuk_final  = VALUES(status_masuk_final),
    status_pulang_final = VALUES(status_pulang_final),
    status_hari_final   = VALUES(status_hari_final),

    jadwal_masuk        = VALUES(jadwal_masuk),
    jadwal_pulang       = VALUES(jadwal_pulang),
    sumber_jadwal       = VALUES(sumber_jadwal),

    device_desc_in      = VALUES(device_desc_in),
    device_id_in        = VALUES(device_id_in),

    device_desc_out     = VALUES(device_desc_out),
    device_id_out       = VALUES(device_id_out),

    valid_device_in     = VALUES(valid_device_in),
    valid_device_out    = VALUES(valid_device_out),
    
    filename_in = VALUES(filename_in),
    filename_out = VALUES(filename_out),
    
    lokasi_kerja        = VALUES(lokasi_kerja),
    valid_devices       = VALUES(valid_devices),
    final_note          = VALUES(final_note),
    is_final            = 1,
    late_minutes        = VALUES(late_minutes),
    early_minutes       = VALUES(early_minutes),
    attribute_in        = VALUES(attribute_in),
    attribute_out       = VALUES(attribute_out),
    
    notes_hari = VALUES(notes_hari),
    notes_in = VALUES(notes_in),
    notes_out = VALUES(notes_out),
    anomaly_flags = VALUES(anomaly_flags)

"""

# =====================================================
# BULK UPSERT
# =====================================================

def bulk_upsert(main_db, rows, batch_size=500, stats=None):
    """
    rows: list[dict] from transform layer
    """
    if not rows:
        return

    with time_block("load_upsert", stats):
        with main_db.cursor() as cur:
            for batch in chunked(rows, batch_size):
                cur.executemany(UPSERT_SQL, batch)

        log(f"Upserted rows: {len(rows)}")

# =====================================================
# TRANSACTION WRAPPER
# =====================================================

def load_rows(main_db, rows, batch_size=500, stats=None):
    """
    Safe transactional loader
    """
    if not rows:
        log("No rows to load")
        return

    main_db.begin()
    try:
        bulk_upsert(main_db, rows, batch_size, stats)
        main_db.commit()
    except Exception:
        main_db.rollback()
        raise
