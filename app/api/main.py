from fastapi import FastAPI
from app.api.routers import health, market_data

def create_app() -> FastAPI:
    app = FastAPI(title="NSE Analytics API")

    app.include_router(health.router)
    app.include_router(market_data.router)

    return app

app = create_app()