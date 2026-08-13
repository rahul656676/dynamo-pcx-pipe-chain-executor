FROM python:3.12-slim

WORKDIR /app

# Copy spec and data
COPY environment/spec /app/spec
COPY environment/data /app/data

# Copy solution
COPY solution/solve.py /app/solve.py
COPY solution/solve.sh /app/solve.sh

RUN chmod +x /app/solve.sh && mkdir -p /app/out
