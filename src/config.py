from pydantic_settings import BaseSettings  # pip install pydantic-settings


class Constants(BaseSettings):
    DATABASE_URL: str

    class Config:
        env_file = ".env"


Constants = Constants()
