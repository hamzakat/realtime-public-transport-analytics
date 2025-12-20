FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY tests/ ./tests/
COPY wait-for-services.sh .
RUN chmod +x wait-for-services.sh

ENV PYTHONPATH=/app

CMD ["pytest", "tests/", "-v"]