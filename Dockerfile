FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    sqlalchemy \
    asyncpg \
    psycopg2-binary \
    httpx \
    pydantic-settings \
    redis \
    prometheus-client

COPY . .

