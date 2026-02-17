#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# main.py
# =====================================================
# Orchestrator ETL absensi (production-grade)
# =====================================================

import argparse
import sys
import os
from datetime import datetime
from dotenv import load_dotenv
import pymysql

from utils import (
    log,
    parse_date,
    date_range,
    time_block,
)
from extract import extract_all
from transform import process_pegawai_fast
from load import load_rows
import cache

# =====================================================
# ENV & DB CONFIG
# =====================================================

ENV_PATH = "/var/www/monit.pekanbaru.go.id/absensi/.env"
# ENV_PATH = os.getenv("ENV_PATH", ".env")
load_dotenv(ENV_PATH)

ATT_DB = {
    "host": os.getenv("DB_ATT_HOST"),
    "port": int(os.getenv("DB_ATT_PORT", 3306)),
    "user": os.getenv("DB_ATT_USERNAME"),
    "password": os.getenv("DB_ATT_PASSWORD"),
    "database": os.getenv("DB_ATT_DATABASE"),
}

MAIN_DB = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
}

AUX_DB = {
    "host": os.getenv("DB_TEMP_HOST"),
    "port": int(os.getenv("DB_TEMP_PORT", 3306)),
    "user": os.getenv("DB_TEMP_USERNAME"),
    "password": os.getenv("DB_TEMP_PASSWORD"),
    "database": os.getenv("DB_TEMP_DATABASE"),
}

def connect(cfg):
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

# =====================================================
# ARGUMENTS
# =====================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL absensi_summaries (production-grade)"
    )
    parser.add_argument("--from", dest="date_from", required=True)
    parser.add_argument("--to", dest="date_to")
    parser.add_argument("--unit-id", dest="unit_id", type=int)
    parser.add_argument("--sub-unit-id", dest="sub_unit_id", type=int)
    parser.add_argument("--nik", dest="nik")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    return parser.parse_args()

# =====================================================
# MAIN ETL
# =====================================================

def run_etl(main_db, aux_db, att_db, date, args):
    stats = {}

    log(f"ETL start for date {date}")

    # -------------------------------------------------
    # EXTRACT
    # -------------------------------------------------
    with time_block("extract_total", stats):
        extract_all(
            main_db,
            aux_db,
            att_db,
            date,
            unit_id=args.unit_id,
            sub_unit_id=args.sub_unit_id,
            nik=args.nik,
            stats=stats,
        )

    # -------------------------------------------------
    # TRANSFORM
    # -------------------------------------------------
    rows = []
    with time_block("transform_total", stats):
        for nik in cache.PEGAWAI_CTX.keys() | {
            k[0] for k in cache.ATT_MAP.keys()
        }:
            ctx = cache.get_pegawai_ctx(nik)
            if args.unit_id and (not ctx or str(ctx["unit_id"]) != str(args.unit_id)):
                continue

            # FILTER NIK ARGUMENT
            if args.nik and nik != args.nik:
                continue
    
            # if ctx:
            #     print("UNIT:", ctx["unit_id"], "NIK:", nik)
            # else:
            #     print("UNIT: NONE", "NIK:", nik)
            
            row = process_pegawai_fast(nik, date)
            rows.append(row)

    log(f"Rows transformed: {len(rows)}")

    # -------------------------------------------------
    # LOAD
    # -------------------------------------------------
    if args.dry_run:
        log("Dry-run enabled, skipping load")
    else:
        load_rows(
            main_db,
            rows,
            batch_size=args.batch_size,
            stats=stats,
        )

    log(
        f"[ETL DONE] {date} | "
        f"extract={stats.get('extract_total_ms', 0)}ms "
        f"transform={stats.get('transform_total_ms', 0)}ms "
        f"load={stats.get('load_upsert_ms', 0)}ms "
        f"rows={len(rows)}"
    )

# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    args = parse_args()

    date_from = parse_date(args.date_from)
    date_to = parse_date(args.date_to) if args.date_to else date_from

    main_db = connect(MAIN_DB)
    aux_db = connect(AUX_DB)
    att_db = connect(ATT_DB)

    try:
        for d in date_range(date_from, date_to):
            # reset cache per date
            cache.ATT_MAP.clear()
            cache.PEGAWAI_CTX.clear()
            cache.DEVICE_BY_UNIT.clear()
            cache.ABSENT_MAP.clear()
            cache.TAP_MAP.clear()
            cache.JADWAL_PEGAWAI.clear()
            cache.JADWAL_SUB_UNIT.clear()
            cache.JADWAL_UNIT.clear()
            cache.JADWAL_DINAS.clear()

            run_etl(main_db, aux_db, att_db, d, args)

        log("ETL completed successfully")
        sys.exit(0)

    except Exception as e:
        log(f"[FATAL] {e}")
        sys.exit(1)

    finally:
        main_db.close()
        aux_db.close()
        att_db.close()
