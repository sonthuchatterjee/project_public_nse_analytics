from datetime import date, datetime
from typing import List, Optional

import polars as pl
from sqlalchemy import and_, select, func
from sqlalchemy.orm import Session

from .models import Symbol, OhlcDaily, BhavEodRaw, LoadRecon
from sqlalchemy.dialects.postgresql import insert as pg_insert


def get_or_create_symbol(
    session: Session,
    symbol: str,
    series: str = "EQ",
    exchange: str = "NSE",
    isin: Optional[str] = None,
) -> Symbol:
    stmt = (
        select(Symbol)
        .where(Symbol.symbol == symbol)
        .where(Symbol.series == series)
        .where(Symbol.exchange == exchange)
    )
    result = session.execute(stmt).scalar_one_or_none()

    if result is not None:
        return result

    obj = Symbol(
        symbol=symbol,
        series=series,
        exchange=exchange,
        isin=isin,
    )
    session.add(obj)
    session.flush()
    return obj


def list_symbols(
    session: Session,
    active_only: bool = True,
    exchange: str = "NSE",
) -> List[Symbol]:
    stmt = select(Symbol).where(Symbol.exchange == exchange)
    if active_only:
        stmt = stmt.where(Symbol.active.is_(True))

    result = session.execute(stmt).scalars().all()
    return list(result)


def get_ohlc_daily(
    session: Session,
    symbol: str,
    series: str,
    start: date,
    end: date,
    exchange: str = "NSE",
) -> pl.DataFrame:
    """
    Return daily OHLCV for a symbol between [start, end], as a Polars DataFrame
    with columns:
    ['ts', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'trades'].
    """
    # Find symbol_id via subquery
    subq = (
        select(Symbol.id)
        .where(
            and_(
                Symbol.symbol == symbol,
                Symbol.series == series,
                Symbol.exchange == exchange,
            )
        )
        .subquery()
    )

    stmt = (
        select(
            OhlcDaily.ts,
            OhlcDaily.open,
            OhlcDaily.high,
            OhlcDaily.low,
            OhlcDaily.close,
            OhlcDaily.volume,
            OhlcDaily.turnover,
            OhlcDaily.trades,
        )
        .where(OhlcDaily.symbol_id == subq.c.id)
        .where(OhlcDaily.ts >= start)
        .where(OhlcDaily.ts <= end)
        .order_by(OhlcDaily.ts)
    )

    rows = session.execute(stmt).all()
    if not rows:
        return pl.DataFrame(
            schema={
                "ts": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "turnover": pl.Float64,
                "trades": pl.Int64,
            }
        )

    df = pl.DataFrame(
        rows,
        schema=[
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "trades",
        ],
    )
    # Already ordered by ts in SQL; no extra sort needed
    return df

def upsert_ohlc_daily(
    session: Session,
    df: pl.DataFrame,
    trade_date: date,
    symbol_col: str = "symbol",
    series_col: str = "series",
) -> None:
    """
    Bulk upsert daily OHLC data for a single trade_date.

    Expected columns in df:
      - symbol (by default)
      - series (by default)
      - open, high, low, close, volume, turnover, trades
    """
    required_cols = {
        symbol_col,
        series_col,
        "open",
        "high",
        "low",
        "close",
        "volume",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for upsert_ohlc_daily: {missing}")

    records = []

    # rows(named=True) → list[dict]; fine for ETL volumes.[web:194][web:190]
    for row in df.rows(named=True):
        sym_obj = get_or_create_symbol(
            session,
            symbol=row[symbol_col],
            series=row[series_col],
        )
        rec = {
            "symbol_id": sym_obj.id,
            "ts": trade_date,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": int(row["volume"]),
            "turnover": row.get("turnover"),
            "trades": row.get("trades"),
        }
        records.append(rec)

    if not records:
        return

    insert_stmt = pg_insert(OhlcDaily).values(records)
    update_cols = {
        "open": insert_stmt.excluded.open,
        "high": insert_stmt.excluded.high,
        "low": insert_stmt.excluded.low,
        "close": insert_stmt.excluded.close,
        "volume": insert_stmt.excluded.volume,
        "turnover": insert_stmt.excluded.turnover,
        "trades": insert_stmt.excluded.trades,
    }
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["symbol_id", "ts"],
        set_=update_cols,
    )
    session.execute(upsert_stmt)

def create_load_recon(
    session: Session,
    load_date: date,
    source: str,
    file_name: Optional[str],
    expected_rows: int,
    loaded_rows: int,
    status: str,
    error_message: Optional[str] = None,
    checksum: Optional[str] = None,
) -> LoadRecon:
    obj = LoadRecon(
        load_date=load_date,
        source=source,
        file_name=file_name,
        status=status,
        expected_rows=expected_rows,
        loaded_rows=loaded_rows,
        checksum=checksum,
        error_message=error_message,
        created_at=datetime.utcnow(),
    )
    session.add(obj)
    return obj

def get_max_bhav_date(session: Session) -> Optional[date]:
    stmt = select(func.max(BhavEodRaw.date1))
    result = session.execute(stmt).scalar_one_or_none()
    return result

def upsert_bhav_eod_raw(
    session: Session,
    df: pl.DataFrame,
) -> None:
    """
    Upsert raw bhavcopy rows into bhav_eod_raw.

    Expects columns:
      SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE,
      LOW_PRICE, LAST_PRICE, CLOSE_PRICE, AVG_PRICE,
      TTL_TRD_QNTY, TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER
    """

    required_cols = {
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
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required bhavcopy columns: {missing}")

    records = []

    for row in df.rows(named=True):
        sym_obj = get_or_create_symbol(
            session,
            symbol=row["SYMBOL"],
            series=row["SERIES"],
        )
        rec = {
            "symbol_id": sym_obj.id,
            "symbol": row["SYMBOL"],
            "series": row["SERIES"],
            "date1": row["DATE1"],
            "prev_close": row["PREV_CLOSE"],
            "open_price": row["OPEN_PRICE"],
            "high_price": row["HIGH_PRICE"],
            "low_price": row["LOW_PRICE"],
            "last_price": row["LAST_PRICE"],
            "close_price": row["CLOSE_PRICE"],
            "avg_price": row["AVG_PRICE"],
            "ttl_trd_qnty": row["TTL_TRD_QNTY"],
            "turnover_lacs": row["TURNOVER_LACS"],
            "no_of_trades": row["NO_OF_TRADES"],
            "deliv_qty": row["DELIV_QTY"],
            "deliv_per": row["DELIV_PER"],
        }
        records.append(rec)

    if not records:
        return

    insert_stmt = pg_insert(BhavEodRaw).values(records)
    update_cols = {
        "prev_close": insert_stmt.excluded.prev_close,
        "open_price": insert_stmt.excluded.open_price,
        "high_price": insert_stmt.excluded.high_price,
        "low_price": insert_stmt.excluded.low_price,
        "last_price": insert_stmt.excluded.last_price,
        "close_price": insert_stmt.excluded.close_price,
        "avg_price": insert_stmt.excluded.avg_price,
        "ttl_trd_qnty": insert_stmt.excluded.ttl_trd_qnty,
        "turnover_lacs": insert_stmt.excluded.turnover_lacs,
        "no_of_trades": insert_stmt.excluded.no_of_trades,
        "deliv_qty": insert_stmt.excluded.deliv_qty,
        "deliv_per": insert_stmt.excluded.deliv_per,
    }
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["symbol_id", "date1"],
        set_=update_cols,
    )
    session.execute(upsert_stmt)