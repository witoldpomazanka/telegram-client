FROM python:3.11-slim

WORKDIR /app

# Tworzenie katalogu na dane
RUN mkdir -p /data

# Kopiowanie plików aplikacji
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tworzenie katalogu na szablony
RUN mkdir -p templates

# Kopiowanie plików aplikacji
COPY telegram_reader.py .
COPY templates/index.html templates/

# Ustawienie zmiennych środowiskowych
ENV PYTHONUNBUFFERED=1
ENV TELEGRAM_SESSION_DIR=/data

# Expose port dla serwera web i WebSocket
EXPOSE 8080
EXPOSE 8765

CMD ["python", "telegram_reader.py"] 