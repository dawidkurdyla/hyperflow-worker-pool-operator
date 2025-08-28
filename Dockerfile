FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY handlers.py .
COPY rabbitmq.py .
COPY helpers ./helpers

RUN pip install --no-cache-dir \
    kopf \
    kubernetes \
    pyyaml \
    requests

CMD ["kopf", "run", "handlers.py", "--verbose"]
