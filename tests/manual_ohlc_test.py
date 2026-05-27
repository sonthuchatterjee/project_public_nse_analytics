from datetime import date

import polars as pl

from nse_core.db.session import get_session
from nse_core.db.repositories import upsert_ohlc_daily, get_ohlc_daily


def main():
    session = get_session()
    try:
        trade_date = date(2024, 5, 10)
        df = pl.DataFrame(
            [
                {
                    "symbol": "INFY",
                    "series": "EQ",
                    "open": 1400.0,
                    "high": 1420.0,
                    "low": 1390.0,
                    "close": 1410.0,
                    "volume": 1_000_000,
                    "turnover": 1_410_000_000.0,
                    "trades": 50_000,
                },
                {
                    "symbol": "TCS",
                    "series": "EQ",
                    "open": 3600.0,
                    "high": 3650.0,
                    "low": 3590.0,
                    "close": 3625.0,
                    "volume": 800_000,
                    "turnover": 2_900_000_000.0,
                    "trades": 40_000,
                },
            ]
        )

        upsert_ohlc_daily(session, df, trade_date)
        session.commit()

        df_infy = get_ohlc_daily(
            session,
            symbol="INFY",
            series="EQ",
            start=date(2024, 5, 1),
            end=date(2024, 5, 31),
        )
        print("INFY daily OHLCV (Polars):")
        print(df_infy)
    finally:
        session.close()


if __name__ == "__main__":
    main()