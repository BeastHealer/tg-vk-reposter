FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    SESSION_DIR=/data

WORKDIR /app

RUN mkdir -p /data

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py vk_poster.py tg_handler.py main.py ./

# Сюда docker-compose монтирует каталог с reposter.session.
# Без тома Telegram запрашивает код при каждом пересоздании контейнера.
VOLUME ["/data"]

CMD ["python", "main.py"]
