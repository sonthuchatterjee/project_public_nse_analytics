from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.deps import get_db
from nse_core.db.repositories import get_ohlc_daily

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/ohlc")
def ohlc_daily(
    symbol: str,
    series: str = "EQ",
    start: date = date(2024, 5, 1),
    end: date = date(2024, 5, 31),
    db: Session = Depends(get_db),
):
    """
    Return daily OHLCV for a symbol between start and end (inclusive).
    """
    df = get_ohlc_daily(
        db,
        symbol=symbol,
        series=series,
        start=start,
        end=end,
    )

    if df.height == 0:
        # No data found
        raise HTTPException(status_code=404, detail="No OHLC data found")

    # Polars → list[dict] for JSON.[web:190]
    return df.to_dicts()