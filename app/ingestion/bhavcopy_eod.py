# app/ingestion/bhavcopy_eod.py

import hashlib
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
import sys
import time

import httpx
import polars as pl
import tomllib
import re

from app.logging_config import setup_logging
from nse_core.db.session import get_session
from nse_core.db.repositories import (
    upsert_bhav_eod_raw,
    create_load_recon,
    create_bhavcopy_reload_audit,
    get_max_bhav_date,
    get_prior_success_recons_for_date,
    get_prior_success_recons_all_dates,
    truncate_bhavcopy_tables,
)


logger = logging.getLogger(__name__)

SOURCE_NSE_EOD = "NSE_EOD"

RUN_MODE_FULL = "FULL"
RUN_MODE_INCREMENTAL = "INCREMENTAL"
RUN_MODE_ADHOC = "ADHOC"

STATUS_SUCCESS = "SUCCESS"
STATUS_MISSING = "MISSING"
STATUS_DOWNLOAD_FAILED = "DOWNLOAD_FAILED"
STATUS_PARSE_FAILED = "PARSE_FAILED"
STATUS_ALREADY_LOADED = "ALREADY_LOADED"
STATUS_RELOADED_SUCCESS = "RELOADED_SUCCESS"
STATUS_DATE_MISMATCH_LOAD_SKIPPED = "DATE_MISMATCH_LOAD_SKIPPED"
STATUS_DATE_MISMATCH_RELOADED_SUCCESS = "DATE_MISMATCH_RELOADED_SUCCESS"

DATE_MATCHED = "MATCHED_DATE"
DATE_MISMATCHED = "MISMATCHED_DATE"

HASH_MATCHED = "MATCHED_HASH"
HASH_NEW = "NEW_HASH"
HASH_NOT_CHECKED = "NOT_CHECKED"
HASH_DATE_MISMATCH_NEW = "DATE_MISMATCH_NEW_HASH"

@dataclass
class BhavcopyConfig:
    base_url: str
    file_pattern: str
    full_load_start: date
    full_load_end: date
    incremental_max_lag_days: int
    use_incremental_end_date: bool
    incremental_end_date: Optional[date]
    adhoc_excel_path: str
    adhoc_excel_sheet_name: str
    request_timeout_secs: int
    max_retries: int
    retry_backoff_secs: int


def load_bhavcopy_config(config_path: Path = Path("config.toml")) -> BhavcopyConfig:
    with config_path.open("rb") as f:
        data = tomllib.load(f)

    cfg = data["etl"]["bhavcopy"]

    return BhavcopyConfig(
        base_url=cfg["base_url"],
        file_pattern=cfg["file_pattern"],
        full_load_start=date.fromisoformat(cfg["full_load_start"]),
        full_load_end=date.fromisoformat(cfg["full_load_end"]),
        incremental_max_lag_days=int(cfg.get("incremental_max_lag_days", 5)),
        use_incremental_end_date=bool(cfg.get("use_incremental_end_date", False)),
        incremental_end_date=(
            date.fromisoformat(cfg["incremental_end_date"])
            if cfg.get("incremental_end_date")
            else None
        ),
        adhoc_excel_path=cfg["adhoc_excel_path"],
        adhoc_excel_sheet_name=cfg.get("adhoc_excel_sheet_name", "Sheet1"),
        request_timeout_secs=int(cfg.get("request_timeout_secs", 30)),
        max_retries=int(cfg.get("max_retries", 3)),
        retry_backoff_secs=int(cfg.get("retry_backoff_secs", 5)),
)


def bhav_url_for_date(d: date, cfg: BhavcopyConfig) -> str:
    dd = f"{d.day:02d}"
    mm = f"{d.month:02d}"
    yyyy = f"{d.year:04d}"
    file_name = cfg.file_pattern.format(dd=dd, mm=mm, yyyy=yyyy)
    return f"{cfg.base_url}/{file_name}"

def download_bhavcopy(d: date, cfg: BhavcopyConfig) -> str:
    url = bhav_url_for_date(d, cfg)
    logger.info("Downloading bhavcopy for %s from %s", d, url)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) "
            "Gecko/20100101 Firefox/98.0"
        )
    }

    for attempt in range(1, cfg.max_retries + 1):
        try:
            with httpx.Client(headers=headers, timeout=cfg.request_timeout_secs) as client:
                resp = client.get(url)
                if resp.status_code == 404:
                    logger.error("Bhavcopy not found (404) for %s", d)
                    raise FileNotFoundError(f"Bhavcopy 404 for {d}: {url}")
                resp.raise_for_status()
                logger.info("Downloaded bhavcopy for %s (size=%d bytes)", d, len(resp.content))
                return resp.text
        except Exception as exc:
            logger.warning(
                "Error downloading bhavcopy for %s (attempt %d/%d): %s",
                d,
                attempt,
                cfg.max_retries,
                exc,
            )
            if attempt == cfg.max_retries:
                raise

            time.sleep(cfg.retry_backoff_secs)

def compute_file_hash(csv_text: str) -> str:
    return hashlib.sha256(csv_text.encode("utf-8")).hexdigest()

def _normalize_bhavcopy_col(name: str) -> str:
    # 1) strip leading/trailing whitespace
    name = name.strip()
    # 2) remove punctuation/special chars
    #    keep only word chars and whitespace
    name = re.sub(r"[^\w\s]", "", name)  # e.g. " CLOSE*" -> " CLOSE"
    # 3) collapse internal whitespace to a single underscore
    name = re.sub(r"\s+", "_", name)
    # 4) normalize case
    return name.upper()

def parse_bhavcopy(csv_text: str, trade_date: date) -> pl.DataFrame:
    """
    Parse raw CSV text into a Polars DataFrame with cleaned and typed bhavcopy columns.

    All string cleanup, invalid-value handling, and type casting is centralized here.
    DATE1 is parsed to pl.Date and validated against the expected bhavcopy date format.
    """

    df = pl.read_csv(
        csv_text.encode("utf-8"),
        ignore_errors=False,
    )

    # Normalize headers first
    df = df.rename(_normalize_bhavcopy_col)

    # Ensure expected columns exist
    expected_cols = {
        "SYMBOL",
        "SERIES",
        "DATE1",
        "PREV_CLOSE",
        "OPEN_PRICE",
        "HIGH_PRICE",
        "LOW_PRICE",
        "LAST_PRICE",
        "CLOSE_PRICE",
        "AVG_PRICE",
        "TTL_TRD_QNTY",
        "TURNOVER_LACS",
        "NO_OF_TRADES",
        "DELIV_QTY",
        "DELIV_PER",
    }
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Bhavcopy missing columns: {missing}")

    # Centralized cleanup + casting
    df = df.with_columns(
        # Text columns
        pl.col("SYMBOL")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .alias("SYMBOL"),

        pl.col("SERIES")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .alias("SERIES"),

        # Date column
        pl.col("DATE1")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.to_date(format="%d-%b-%Y", strict=True, exact=True)
        .alias("DATE1"),

        # Price / float columns
        pl.col("PREV_CLOSE")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("PREV_CLOSE"),

        pl.col("OPEN_PRICE")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("OPEN_PRICE"),

        pl.col("HIGH_PRICE")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("HIGH_PRICE"),

        pl.col("LOW_PRICE")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("LOW_PRICE"),

        pl.col("LAST_PRICE")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("LAST_PRICE"),

        pl.col("CLOSE_PRICE")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("CLOSE_PRICE"),

        pl.col("AVG_PRICE")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("AVG_PRICE"),

        pl.col("TURNOVER_LACS")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("TURNOVER_LACS"),

        # Integer columns
        pl.col("TTL_TRD_QNTY")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(r"[^0-9]", "")
        .cast(pl.Int64, strict=False)
        .alias("TTL_TRD_QNTY"),

        pl.col("NO_OF_TRADES")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(r"[^0-9]", "")
        .cast(pl.Int64, strict=False)
        .alias("NO_OF_TRADES"),

        pl.col("DELIV_QTY")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(r"[^0-9]", "")
        .cast(pl.Int64, strict=False)
        .alias("DELIV_QTY"),
        
        pl.col("DELIV_PER")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", "")
        .cast(pl.Float64, strict=False)
        .alias("DELIV_PER")
    )

    return df

def load_bhav_for_date(
    d: date,
    cfg: BhavcopyConfig,
    run_mode: str,
    allow_date_mismatch_reload: bool = False,
) -> None:
    """
    Download, parse, and load bhavcopy for a single trade date d.

    Writes raw rows to bhav_eod_raw and a recon entry.
    """
    session = get_session()
    file_name = bhav_url_for_date(d, cfg).split("/")[-1]

    try:
        try:
            csv_text = download_bhavcopy(d, cfg)
            file_hash = compute_file_hash(csv_text)
        except FileNotFoundError as exc:
            logger.error("Bhavcopy missing for %s: %s", d, exc)
            create_load_recon(
                session,
                load_date=d,
                source=SOURCE_NSE_EOD,
                run_mode=run_mode,
                file_name=file_name,
                file_hash=None,
                date_inside_file=None,
                expected_rows=0,
                loaded_rows=0,
                status=STATUS_MISSING,
                date_match_status=None,
                hash_match_status=HASH_NOT_CHECKED,
                error_message=str(exc),
            )
            session.commit()
            return
        except Exception as exc:
            logger.exception("Error downloading bhavcopy for %s", d)
            create_load_recon(
                session,
                load_date=d,
                source=SOURCE_NSE_EOD,
                run_mode=run_mode,
                file_name=file_name,
                file_hash=None,
                date_inside_file=None,
                expected_rows=0,
                loaded_rows=0,
                status=STATUS_DOWNLOAD_FAILED,
                date_match_status=None,
                hash_match_status=HASH_NOT_CHECKED,
                error_message=str(exc),
            )
            session.commit()
            return

        try:
            df = parse_bhavcopy(csv_text, d)
        except Exception as exc:
            logger.exception("Error parsing bhavcopy for %s", d)
            create_load_recon(
                session,
                load_date=d,
                source=SOURCE_NSE_EOD,
                run_mode=run_mode,
                file_name=file_name,
                file_hash=file_hash,
                date_inside_file=None,
                expected_rows=0,
                loaded_rows=0,
                status=STATUS_PARSE_FAILED,
                date_match_status=None,
                hash_match_status=HASH_NOT_CHECKED,
                error_message=str(exc),
            )
            session.commit()
            return

        expected_rows = df.height
        date_inside_file = df.select(pl.col("DATE1").first()).item()

        same_date_recons = get_prior_success_recons_for_date(session, SOURCE_NSE_EOD, d)
        same_date_hash_match = next(
            (r for r in same_date_recons if r.file_hash and r.file_hash == file_hash),
            None,
        )

        if date_inside_file != d:
            if not allow_date_mismatch_reload:
                create_load_recon(
                    session,
                    load_date=d,
                    source=SOURCE_NSE_EOD,
                    run_mode=run_mode,
                    file_name=file_name,
                    file_hash=file_hash,
                    date_inside_file=date_inside_file,
                    expected_rows=expected_rows,
                    loaded_rows=0,
                    status=STATUS_DATE_MISMATCH_LOAD_SKIPPED,
                    date_match_status=DATE_MISMATCHED,
                    hash_match_status=HASH_NOT_CHECKED,
                    error_message=f"DATE1 inside file ({date_inside_file}) does not match requested load_date ({d})",
                )
                session.commit()
                return

            all_success_recons = get_prior_success_recons_all_dates(session, SOURCE_NSE_EOD)
            any_hash_match = next(
                (r for r in all_success_recons if r.file_hash and r.file_hash == file_hash),
                None,
            )

            if any_hash_match is not None:
                create_load_recon(
                    session,
                    load_date=d,
                    source=SOURCE_NSE_EOD,
                    run_mode=run_mode,
                    file_name=file_name,
                    file_hash=file_hash,
                    date_inside_file=date_inside_file,
                    expected_rows=expected_rows,
                    loaded_rows=0,
                    status=STATUS_ALREADY_LOADED,
                    date_match_status=DATE_MISMATCHED,
                    hash_match_status=HASH_MATCHED,
                    matched_load_date=any_hash_match.load_date,
                    matched_file_name=any_hash_match.file_name,
                    error_message=None,
                )
                session.commit()
                return

            upsert_bhav_eod_raw(session, df)
            create_load_recon(
                session,
                load_date=d,
                source=SOURCE_NSE_EOD,
                run_mode=run_mode,
                file_name=file_name,
                file_hash=file_hash,
                date_inside_file=date_inside_file,
                expected_rows=expected_rows,
                loaded_rows=expected_rows,
                status=STATUS_DATE_MISMATCH_RELOADED_SUCCESS,
                date_match_status=DATE_MISMATCHED,
                hash_match_status=HASH_DATE_MISMATCH_NEW,
                error_message=None,
            )
            create_bhavcopy_reload_audit(
                session,
                source=SOURCE_NSE_EOD,
                load_date=d,
                run_mode=run_mode,
                file_name=file_name,
                file_hash=file_hash,
                date_inside_file=date_inside_file,
                reload_status=STATUS_DATE_MISMATCH_RELOADED_SUCCESS,
            )
            session.commit()
            logger.info(
                "Loaded date-mismatched bhavcopy for requested date %s using DATE1=%s",
                d,
                date_inside_file,
            )
            return

        if same_date_hash_match is not None:
            create_load_recon(
                session,
                load_date=d,
                source=SOURCE_NSE_EOD,
                run_mode=run_mode,
                file_name=file_name,
                file_hash=file_hash,
                date_inside_file=date_inside_file,
                expected_rows=expected_rows,
                loaded_rows=0,
                status=STATUS_ALREADY_LOADED,
                date_match_status=DATE_MATCHED,
                hash_match_status=HASH_MATCHED,
                matched_load_date=same_date_hash_match.load_date,
                matched_file_name=same_date_hash_match.file_name,
                error_message=None,
            )
            session.commit()
            logger.info("Bhavcopy for %s already loaded with matching hash", d)
            return

        upsert_bhav_eod_raw(session, df)

        if same_date_recons:
            status = STATUS_RELOADED_SUCCESS
            hash_status = HASH_NEW
            create_bhavcopy_reload_audit(
                session,
                source=SOURCE_NSE_EOD,
                load_date=d,
                run_mode=run_mode,
                file_name=file_name,
                file_hash=file_hash,
                date_inside_file=date_inside_file,
                reload_status=STATUS_RELOADED_SUCCESS,
            )
        else:
            status = STATUS_SUCCESS
            hash_status = HASH_NEW

        create_load_recon(
            session,
            load_date=d,
            source=SOURCE_NSE_EOD,
            run_mode=run_mode,
            file_name=file_name,
            file_hash=file_hash,
            date_inside_file=date_inside_file,
            expected_rows=expected_rows,
            loaded_rows=expected_rows,
            status=status,
            date_match_status=DATE_MATCHED,
            hash_match_status=hash_status,
            error_message=None,
        )
        session.commit()
        logger.info(
            "Loaded bhavcopy for %s: expected_rows=%d, loaded_rows=%d, status=%s",
            d,
            expected_rows,
            expected_rows,
            status,
        )
    finally:
        session.close()

def run_full_load():
    setup_logging()
    cfg = load_bhavcopy_config()
    logger.info(
        "Starting full bhavcopy load from %s to %s",
        cfg.full_load_start,
        cfg.full_load_end,
    )

    session = get_session()
    try:
        truncate_bhavcopy_tables(session)
        session.commit()
    finally:
        session.close()

    d = cfg.full_load_start
    while d <= cfg.full_load_end:
        try:
            load_bhav_for_date(d, cfg, run_mode=RUN_MODE_FULL)
        except Exception as exc:
            logger.exception("Unhandled error while loading bhavcopy for %s: %s", d, exc)
        d += timedelta(days=1)

    logger.info("Full bhavcopy load completed.")

def run_incremental_load():
    """
    Incremental EOD load:

    - Find the max DATE1 in bhav_eod_raw.
    - Start from next calendar day.
    - Load up to today (inclusive), with an optional max lag from config.
    """
    setup_logging()
    cfg = load_bhavcopy_config()
    system_today = date.today()
    end_date = (
        cfg.incremental_end_date
        if cfg.use_incremental_end_date and cfg.incremental_end_date is not None
        else system_today
    )

    if end_date > system_today:
        logger.warning(
            "Configured incremental_end_date %s is in the future. Clamping to today %s.",
            end_date,
            system_today,
        )
        end_date = system_today

    # Find last loaded bhav date
    session = get_session()
    try:
        max_date = get_max_bhav_date(session)
    finally:
        session.close()

    if max_date is None:
        logger.info(
            "No bhav_eod_raw data found. Falling back to full load "
            "from %s to %s",
            cfg.full_load_start,
            cfg.full_load_end,
        )
        run_full_load()
        return

    start_date = max_date + timedelta(days=1)
    if start_date > end_date:
        logger.info(
            "Bhavcopy data is already up to date for incremental window "
            "(max_date=%s, end_date=%s). Nothing to do.",
            max_date,
            end_date,
        )
        return

    # Optional safety: don't try to fill arbitrarily huge gaps unless configured
    # max_allowed_start = end_date - timedelta(days=cfg.incremental_max_lag_days)
    # if start_date < max_allowed_start:
    #     logger.warning(
    #         "Incremental start_date %s is more than %d days behind today (%s). "
    #         "Clamping to %s. Adjust incremental_max_lag_days in config.toml "
    #         "if you want a longer catch-up window.",
    #         start_date,
    #         cfg.incremental_max_lag_days,
    #         end_date,
    #         max_allowed_start,
    #     )
    #     start_date = max_allowed_start

    logger.info(
        "Starting incremental bhavcopy load from %s (after max_date=%s) to %s",
        start_date,
        max_date,
        end_date,
    )

    d = start_date
    while d <= end_date:
        try:
            load_bhav_for_date(d, cfg, run_mode=RUN_MODE_INCREMENTAL)
        except Exception as exc:
            logger.exception("Unhandled error while loading bhavcopy for %s: %s", d, exc)
            # load_bhav_for_date already writes recon with error; we just continue
        d += timedelta(days=1)

    logger.info("Incremental bhavcopy load completed.")

def read_adhoc_dates_from_excel(excel_path: Path, sheet_name: str) -> list[date]:
    df = pl.read_excel(excel_path, sheet_name=sheet_name)

    normalized_cols = {str(c).strip().lower(): c for c in df.columns}
    if "load_date" not in normalized_cols:
        raise ValueError(
            f"Excel file '{excel_path}' must contain a 'load_date' column. "
            f"Found columns: {df.columns}"
        )

    col = normalized_cols["load_date"]

    values = (
        df.select(pl.col(col).alias("load_date"))
        .drop_nulls()
        .with_columns(
            pl.col("load_date")
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.strptime(pl.Date, strict=False)
        )
        .drop_nulls()
        .unique()
        .sort("load_date")
    )

    dates = values["load_date"].to_list()

    if not dates:
        raise ValueError(
            f"Excel file '{excel_path}' does not contain any valid load_date values."
        )

    return dates

def run_adhoc_load():
    setup_logging()
    cfg = load_bhavcopy_config()

    excel_path = Path(cfg.adhoc_excel_path)
    logger.info("Starting adhoc bhavcopy load using Excel file: %s", excel_path)

    dates = read_adhoc_dates_from_excel(excel_path, cfg.adhoc_excel_sheet_name)
    logger.info("Adhoc load dates resolved from Excel: %s", dates)

    for d in dates:
        try:
            load_bhav_for_date(
                d,
                cfg,
                run_mode=RUN_MODE_ADHOC
            )
        except Exception as exc:
            logger.exception("Unhandled error while running adhoc load for %s: %s", d, exc)

    logger.info("Adhoc bhavcopy load completed for Excel file: %s", excel_path)

if __name__ == "__main__":

    # Mode selection: default 'full', or 'incremental' if passed.
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"

    if mode == "full":
        run_full_load()
    elif mode == "incremental":
        run_incremental_load()
    elif mode == "adhoc":
        run_adhoc_load()
    else:
        print(f"Unknown mode '{mode}'. Use 'full' or 'incremental', .")