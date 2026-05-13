from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DATABASE_URL: str
    REDIS_URL: str = "redis://localhost:6379"

    class Config:
        env_file = ".env"
        extra = "ignore"   # don't error on extra keys in .env (e.g. WEBHOOK_SECRET)


settings = Settings()

# Backward-compat alias: existing code that imports `Constants` keeps working.
# New code should import `settings`.
Constants = settings
