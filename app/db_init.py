import sys
from nse_core.db.session import engine
from nse_core.db.models import Base


def init_db(drop_existing: bool = False):
    if drop_existing:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    drop_existing = "--reset" in sys.argv
    init_db(drop_existing=drop_existing)