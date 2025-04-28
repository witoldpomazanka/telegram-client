# Telegram Listener

Aplikacja nasłuchująca wiadomości Telegram i przekazująca je do webhooka.

## Wymagania

- Docker
- Numer telefonu zarejestrowany w Telegram
- API ID i API Hash z [Telegram API](https://my.telegram.org/apps)

## Konfiguracja

1. Sklonuj repozytorium:
```bash
git clone [URL_REPOZYTORIUM]
cd telegram-client
```

2. Skopiuj plik `.env.example` do `.env` i wypełnij zmienne środowiskowe:
```bash
cp .env.example .env
```

3. Edytuj plik `.env` i uzupełnij następujące zmienne:
```
TELEGRAM_API_ID=twoje_api_id
TELEGRAM_API_HASH=twoje_api_hash
TELEGRAM_PHONE=twój_numer_telefonu
N8N_WEBHOOK_URL=http://twój_webhook_url
```

## Budowanie i uruchamianie obrazu Dockerowego

1. Zbuduj obraz:
```bash
cd telethon-listener
docker build -t telegram-listener .
```

2. Uruchom kontener:
```bash
docker run -d \
  --name telegram-listener \
  -p 8080:8080 \
  -p 8765:8765 \
  -v $(pwd)/data:/data \
  --env-file ../.env \
  telegram-listener
```

3. Otwórz w przeglądarce interfejs Telegram Readera: http://localhost:8080

4. Jeśli nie jesteś zalogowany, zostaniesz poproszony o podanie kodu weryfikacyjnego, który otrzymasz SMS-em na swój numer telefonu.

5. Po poprawnym zalogowaniu, aplikacja rozpocznie nasłuchiwanie wiadomości.

## Zarządzanie kontenerem

1. Zatrzymanie kontenera:
```bash
docker stop telegram-listener
```

2. Usunięcie kontenera:
```bash
docker rm telegram-listener
```

3. Usunięcie obrazu:
```bash
docker rmi telegram-listener
```

4. Sprawdzenie logów:
```bash
docker logs -f telegram-listener
```

## Struktura projektu

```
telethon-listener/
├── Dockerfile
├── requirements.txt
├── telegram_reader.py
└── templates/
    └── index.html
```

## Rozwiązywanie problemów

1. Jeśli kontener nie może się połączyć z Telegramem:
   - Sprawdź, czy API ID i API Hash są poprawne
   - Upewnij się, że numer telefonu jest poprawny
   - Sprawdź połączenie internetowe

2. Jeśli nie otrzymujesz kodu weryfikacyjnego:
   - Upewnij się, że numer telefonu jest poprawny
   - Sprawdź, czy numer jest zarejestrowany w Telegram
   - Spróbuj ponownie po kilku minutach

## Licencja

MIT 