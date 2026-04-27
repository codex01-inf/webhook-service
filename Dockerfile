FROM python:3.11-slim

WORKDIR .

RUN pip install fastapi uvicorn redis psycopg2-binary

COPY . .

