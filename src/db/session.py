from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import sessionmaker
from src.config import Constants

engine = create_engine(Constants.DATABASE_URL, pool_pre_ping=True, pool_size=10)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Async — used by the worker (httpx is async, so we go async end-to-end)
# DATABASE_URL must look like: postgresql+asyncpg://user:pass@host/db
async_engine = create_async_engine(
    Constants.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://"),
    pool_pre_ping=True,
    pool_size=10,
)
AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,   # keep ORM attrs accessible after commit (worker pattern needs this)
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
