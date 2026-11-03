FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PORT=8000

WORKDIR /app

COPY server.py refresh_stats.py index.html ./

RUN mkdir -p /app/data/snapshots \
    && useradd -m -u 1000 app \
    && chown -R 1000:1000 /app

USER 1000

EXPOSE 8000

# Builds missing data/snapshots/*.json on first start (~12s, once), then serves.
CMD ["python3", "server.py"]
