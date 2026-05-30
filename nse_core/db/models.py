from datetime import date, datetime
from sqlalchemy.orm import declarative_base, relationship

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint
)

Base = declarative_base()

class Symbol(Base):
    __tablename__ = "symbols"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(32), nullable=False)
    series = Column(String(8), nullable=False, default="EQ")
    isin = Column(String(32))
    exchange = Column(String(8), nullable=False, default="NSE")
    active = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        # Unique constraint on (symbol, series, exchange)
        # SQLAlchemy will create it from the combination below
        # if we add an explicit UniqueConstraint later if needed.
    )

    def __repr__(self) -> str:
        return f"<Symbol {self.exchange}:{self.symbol}-{self.series}>"


class OhlcDaily(Base):
    __tablename__ = "ohlc_daily"

    symbol_id = Column(Integer, ForeignKey("symbols.id"), primary_key=True)
    ts = Column(Date, primary_key=True)

    open = Column(Numeric(12, 4), nullable=False)
    high = Column(Numeric(12, 4), nullable=False)
    low = Column(Numeric(12, 4), nullable=False)
    close = Column(Numeric(12, 4), nullable=False)

    volume = Column(BigInteger, nullable=False)
    turnover = Column(Numeric(18, 2))
    trades = Column(BigInteger)

    symbol = relationship("Symbol", backref="ohlc_daily")


class Ohlc1m(Base):
    __tablename__ = "ohlc_1m"

    symbol_id = Column(Integer, ForeignKey("symbols.id"), primary_key=True)
    ts = Column(DateTime, primary_key=True)  # TIMESTAMPTZ in Postgres

    open = Column(Numeric(12, 4), nullable=False)
    high = Column(Numeric(12, 4), nullable=False)
    low = Column(Numeric(12, 4), nullable=False)
    close = Column(Numeric(12, 4), nullable=False)

    volume = Column(BigInteger, nullable=False)

    symbol = relationship("Symbol", backref="ohlc_1m")


class LoadRecon(Base):
    __tablename__ = "load_recon"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    load_date = Column(Date, nullable=False)
    source = Column(String(32), nullable=False)  # 'NSE_EOD', 'INTRADAY', etc.
    run_mode = Column(String(16), nullable=False)  # 'FULL', 'INCREMENTAL', 'ADHOC'

    file_name = Column(String(256))
    file_hash = Column(String(128))
    date_inside_file = Column(Date)

    expected_rows = Column(BigInteger)
    loaded_rows = Column(BigInteger)

    status = Column(String(48), nullable=False)
    date_match_status = Column(String(24))
    hash_match_status = Column(String(32))

    matched_load_date = Column(Date)
    matched_file_name = Column(String(256))

    error_message = Column(String(512))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

class BhavcopyReloadAudit(Base):
    __tablename__ = "bhavcopy_reload_audit"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source = Column(String(32), nullable=False)
    load_date = Column(Date, nullable=False)
    run_mode = Column(String(16), nullable=False)

    file_name = Column(String(256))
    file_hash = Column(String(128), nullable=False)
    date_inside_file = Column(Date)

    reload_status = Column(String(48), nullable=False)
    entered_at = Column(DateTime, nullable=False, default=datetime.utcnow)

class BhavEodRaw(Base):
    """
    Raw bhavcopy data, 1:1 with NSE sec_bhavdata_full CSV columns.
    """

    __tablename__ = "bhav_eod_raw"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    symbol_id = Column(Integer, ForeignKey("symbols.id"), nullable=False)
    # We also store the original symbol/series text for easy inspection
    symbol = Column(String(32), nullable=False)
    series = Column(String(8), nullable=False)

    date1 = Column(Date, nullable=False)  # from DATE1

    prev_close = Column(Numeric(12, 4), nullable=False)
    open_price = Column(Numeric(12, 4), nullable=False)
    high_price = Column(Numeric(12, 4), nullable=False)
    low_price = Column(Numeric(12, 4), nullable=False)
    last_price = Column(Numeric(12, 4), nullable=False)
    close_price = Column(Numeric(12, 4), nullable=False)
    avg_price = Column(Numeric(12, 4))

    ttl_trd_qnty = Column(BigInteger, nullable=False)
    turnover_lacs = Column(Numeric(18, 2))
    no_of_trades = Column(BigInteger)
    deliv_qty = Column(BigInteger)
    deliv_per = Column(Numeric(6, 2))

    symbol_ref = relationship("Symbol")

    __table_args__ = (
        UniqueConstraint("symbol_id", "date1", name="uq_bhav_symbol_date1"),)