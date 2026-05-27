from datetime import date

import streamlit as st
import polars as pl
import sys
from pathlib import Path

# Add project root to sys.path so "import nse_core" works when run via Streamlit
ROOT = Path(__file__).resolve().parents[2]  # goes up from app/ui_streamlit/ to project root
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nse_core.db.session import get_session
from nse_core.db.repositories import get_ohlc_daily


def load_ohlc(symbol: str, start: date, end: date) -> pl.DataFrame:
    session = get_session()
    try:
        df = get_ohlc_daily(
            session,
            symbol=symbol,
            series="EQ",
            start=start,
            end=end,
        )
        return df
    finally:
        session.close()


def main():
    st.set_page_config(page_title="NSE Analytics - Prototype", layout="wide")

    st.title("NSE Analytics – Daily OHLC Prototype")

    col1, col2, col3 = st.columns(3)
    with col1:
        symbol = st.text_input("Symbol", value="INFY")
    with col2:
        start = st.date_input("Start date", value=date(2024, 5, 1))
    with col3:
        end = st.date_input("End date", value=date(2024, 5, 31))

    if st.button("Load OHLC"):
        if start > end:
            st.error("Start date must be before end date")
            return

        df_pl = load_ohlc(symbol.strip().upper(), start, end)

        if df_pl.height == 0:
            st.warning("No OHLC data found for this selection.")
            return

        st.subheader(f"{symbol.upper()} daily OHLCV")

        df_pd = df_pl.to_pandas()
        st.dataframe(df_pd)

        chart_df = df_pd[["ts", "close"]].set_index("ts")
        st.line_chart(chart_df, height=400)
        

if __name__ == "__main__":
    main()