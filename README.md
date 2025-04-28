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

## Budowanie i uruchamianie obrazu Dockerowego

1. Zbuduj obraz:
```bash
docker build -t telegram-listener ./telethon-listener
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

1. Usunięcie obrazu:
```bash
docker rmi telegram-listener
```

2. Sprawdzenie logów:
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