FROM python:3.11-alpine

WORKDIR /app

# Instalacja niezbędnych pakietów systemowych
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Tworzenie katalogów
RUN mkdir -p /data templates

# Kopiowanie i instalacja zależności najpierw (lepsze cache'owanie)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && rm -rf /root/.cache/pip/*

# Kopiowanie plików aplikacji (zmieniane najczęściej, na końcu)
COPY telegram_reader.py .
COPY templates/index.html templates/

# Ustawienie zmiennych środowiskowych
ENV PYTHONUNBUFFERED=1 \
    TELEGRAM_SESSION_DIR=/data \
    PYTHONDONTWRITEBYTECODE=1

# Expose port dla serwera web i WebSocket
EXPOSE 8080 8765

CMD ["python", "telegram_reader.py"] 