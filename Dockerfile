FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MALLOC_ARENA_MAX=2 \
    BRIDGE_HEALTH_FILE=/tmp/bridge-health
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 10001 bridge \
    && useradd --uid 10001 --gid bridge --no-create-home bridge \
    && mkdir -p /data \
    && chown bridge:bridge /data
COPY bridge.py feishu_lite.py ./
USER 10001:10001
ENTRYPOINT ["python", "/app/bridge.py", "--config", "/config/config.toml"]
CMD ["--all"]
