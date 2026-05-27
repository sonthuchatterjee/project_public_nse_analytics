from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Environment variable name: DATABASE_URL
    database_url: str = (
        "postgresql+psycopg2://user:pass@localhost:5432/nse_analytics"
    )

    model_config = SettingsConfigDict(
        env_file=".env",           # load from .env in project root
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()