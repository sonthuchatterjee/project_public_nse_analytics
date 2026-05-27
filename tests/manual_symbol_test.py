# tests/manual_symbol_test.py

from nse_core.db.session import get_session
from nse_core.db.repositories import get_or_create_symbol, list_symbols


def main():
    session = get_session()
    try:
        s1 = get_or_create_symbol(session, "INFY", "EQ", "NSE")
        s2 = get_or_create_symbol(session, "TCS", "EQ", "NSE")
        session.commit()

        symbols = list_symbols(session)
        print("Symbols:", [f"{s.exchange}:{s.symbol}-{s.series}" for s in symbols])
    finally:
        session.close()


if __name__ == "__main__":
    main()