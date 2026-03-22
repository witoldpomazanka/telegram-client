print("=== STARTUJEMY APLIKACJĘ ===")
import asyncio
import base64
import json
import logging
import logging.handlers
import os
import pathlib
import sys
from collections import deque
from datetime import datetime
from queue import Queue

import aiohttp
import asyncpg
import dateutil.parser
from aiohttp import web
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import DocumentAttributeImageSize, DocumentAttributeFilename
from telethon.tl.types import User, Channel, Chat
from telethon.tl.types import MessageReplyHeader
from telethon.tl.functions.channels import GetForumTopicsRequest

# Konfiguracja logowania
logger = logging.getLogger('telegram_reader')
logger.setLevel(logging.INFO)

# Formatter dla logów
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Handler dla pliku (w katalogu /logs zamontowanym w Dockerze) z rotacją dzienną
LOGS_DIR = '/logs'
if os.path.exists(LOGS_DIR):
    try:
        # TimedRotatingFileHandler: rotacja co 1 dzień ('D'), zachowuje ostatnie 30 dni (backupCount=30)
        file_handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(LOGS_DIR, 'telegram_reader.log'),
            when='D',
            interval=1,
            backupCount=30,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Logowanie do pliku w {LOGS_DIR} zostało uruchomione (rotacja dzienna)")
    except Exception as e:
        print(f"Nie udało się skonfigurować logowania do pliku: {e}")

# Handler dla konsoli (stdout)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

logging.getLogger('aiohttp.access').setLevel(logging.WARNING)
logging.getLogger('telethon.network.mtprotosender').setLevel(logging.WARNING)

# Ładowanie zmiennych środowiskowych
load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')
USERNAME = os.getenv('USERNAME')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL')

IGNORED_SENDERS = os.getenv('IGNORED_SENDERS', '')
IGNORED_SENDERS_LIST = [s.strip() for s in IGNORED_SENDERS.split(',') if s.strip()]

def parse_allowed_senders_spec(spec: str):
    """
    Format:
      - "Prywatny,Kieobasa" -> allow proste nazwy czatów/kanałów
      - "[GRUPA]TOPIC"      -> allow tylko TOPIC (forum topic) w GRUPA
    """
    allowed_simple = set()
    allowed_group_topics = {}
    if not spec:
        return allowed_simple, allowed_group_topics

    for raw in spec.split(','):
        token = raw.strip()
        if not token:
            continue

        if token.startswith('[') and ']' in token:
            close_idx = token.find(']')
            group = token[1:close_idx].strip()
            topic = token[close_idx + 1:].strip()
            if group and topic:
                allowed_group_topics.setdefault(group, set()).add(topic)
            else:
                allowed_simple.add(token)
        else:
            allowed_simple.add(token)

    return allowed_simple, allowed_group_topics


def is_allowed_by_spec(*, allowed_simple, allowed_group_topics, main_chat_title, full_display_title, chat_username, topic_name, sender_name=None):
    # Zbieramy kandydatów do porównania - filtrujemy None żeby sorted() działał
    candidates = {c for c in {main_chat_title, full_display_title, chat_username, sender_name} if c}

    if allowed_simple and (candidates & allowed_simple):
        hit = next(iter(candidates & allowed_simple))
        return True, f"simple_match={hit!r}"

    if allowed_group_topics and main_chat_title in allowed_group_topics:
        if topic_name and topic_name in allowed_group_topics[main_chat_title]:
            return True, f"group_topic_match={main_chat_title!r}[{topic_name!r}]"
        return False, f"group_topic_miss group={main_chat_title!r} topic={topic_name!r} allowed_topics={sorted(allowed_group_topics[main_chat_title])!r}"

    if allowed_simple or allowed_group_topics:
        return False, f"no_match candidates={sorted(candidates)!r}"

    return True, "no_allowlist_configured"

# Dane do połączenia z bazą PostgreSQL
PG_USER = os.getenv('POSTGRES_USER')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')
PG_DB = os.getenv('POSTGRES_DB')
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_PORT = os.getenv('POSTGRES_PORT', '5432')

# Tworzymy URL połączenia do bazy danych
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Nazwa tabeli dla wiadomości Telegram (bez konfliktu z tabelami n8n)
TELEGRAM_MESSAGES_TABLE = 'telegram_messages_history'

# Globalne zmienne dla procesu autoryzacji
phone_code_hash = None
client = None
db_pool = None

# Bufor na ostatnie wiadomości (max 100)
message_history = deque(maxlen=100)

# Kolejka do komunikacji między wątkami
message_queue = Queue()

# Lista aktywnych połączeń WebSocket
# Teraz może zawierać różne typy WebSocketów (natywne i aiohttp)
websocket_clients = set()

TOPIC_CACHE = {} # Dodaj to na górze przy zmiennych globalnych

async def get_topic_name(chat_id, topic_id):
    """Pobiera nazwę tematu (Topic) dla grup typu Forum"""
    if not topic_id: return None
    cache_key = f"{chat_id}_{topic_id}"
    if cache_key in TOPIC_CACHE: return TOPIC_CACHE[cache_key]
    try:
        # GetForumTopicsRequest wyciąga listę nazw tematów z kanału
        from telethon.tl.functions.channels import GetForumTopicsRequest
        result = await client(GetForumTopicsRequest(channel=chat_id, offset_date=None, offset_id=0, offset_topic=0, limit=100))
        for topic in result.topics:
            TOPIC_CACHE[f"{chat_id}_{topic.id}"] = topic.title
        return TOPIC_CACHE.get(cache_key, f"Topic {topic_id}")
    except Exception as e:
        logger.error(f"Błąd pobierania nazwy tematu: {e}")
        return f"Topic {topic_id}"

# Funkcja pomocnicza do wysyłania JSON przez różne typy WebSocket
async def send_websocket_json(ws, data):
    """Wysyła dane JSON przez WebSocket niezależnie od jego typu"""
    try:
        # Sprawdzamy, czy to WebSocketResponse (aiohttp)
        if hasattr(ws, 'send_json'):
            await ws.send_json(data)
        # Standardowy WebSocket (websockets)
        else:
            await ws.send(json.dumps(data))
        return True
    except Exception as e:
        logger.error(f"Błąd podczas wysyłania przez WebSocket: {str(e)}")
        return False


logger.info("=== START APLIKACJI ===")

grouped_messages_buffer = {}
grouped_messages_timers = {}
GROUPED_MESSAGE_TIMEOUT = 1.0  # sekundy


async def init_database():
    """Inicjalizuje połączenie z bazą danych i tworzy wymagane tabele"""
    global db_pool
    try:
        logger.info("Inicjalizacja połączenia z bazą danych...")
        # Tworzymy pulę połączeń do bazy danych
        db_pool = await asyncpg.create_pool(dsn=DATABASE_URL)

        async with db_pool.acquire() as conn:
            # Tworzymy tabelę wiadomości jeśli nie istnieje
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {TELEGRAM_MESSAGES_TABLE} (
                    id SERIAL PRIMARY KEY,
                    message TEXT,
                    chat_id TEXT,
                    chat_title TEXT,
                    chat_type TEXT,
                    sender_id TEXT,
                    sender_name TEXT,
                    timestamp TIMESTAMP WITH TIME ZONE,
                    received_at TIMESTAMP WITH TIME ZONE,
                    is_new BOOLEAN DEFAULT TRUE,
                    media JSONB
                )
            ''')

            # Sprawdzenie czy kolumna 'media' istnieje, jeśli nie - dodaj ją
            try:
                columns = await conn.fetch(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TELEGRAM_MESSAGES_TABLE}'"
                )
                column_names = [col['column_name'] for col in columns]

                if 'media' not in column_names:
                    logger.info("Dodawanie kolumny 'media' do tabeli wiadomości...")
                    await conn.execute(
                        f"ALTER TABLE {TELEGRAM_MESSAGES_TABLE} ADD COLUMN media JSONB"
                    )
            except Exception as e:
                logger.error(f"Błąd podczas sprawdzania/dodawania kolumny 'media': {str(e)}")

        logger.info("✓ Inicjalizacja bazy danych zakończona pomyślnie")

        return True
    except Exception as e:
        logger.error(f"Błąd podczas inicjalizacji bazy danych: {str(e)}")
        return False


async def get_media_from_message(message):
    """Pobiera media z wiadomości Telegram i konwertuje je do formatu odpowiedniego do zapisu w DB"""
    if not message.media:
        return []

    media_list = []

    try:
        # Obsługa różnych typów mediów
        if hasattr(message.media, 'photo'):
            # Pobieramy największą wersję zdjęcia
            photo = message.media.photo
            data = await message.download_media(bytes)

            if data:
                media_list.append({
                    'type': 'photo',
                    'mime_type': 'image/jpeg',
                    'data': base64.b64encode(data).decode('utf-8'),
                    'size': len(data),
                    'filename': f"photo_{photo.id}.jpg"
                })
            else:
                logger.warning("Nie udało się pobrać zdjęcia")

        elif hasattr(message.media, 'document'):
            doc = message.media.document

            # Sprawdzamy czy dokument to zdjęcie
            is_image = False
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeImageSize):
                    is_image = True
                    break

            # Jeśli to zdjęcie lub ma rozszerzenie obrazu, pobieramy
            mime_type = doc.mime_type or 'application/octet-stream'
            filename = None

            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    filename = attr.file_name
                    break

            if is_image or (mime_type and mime_type.startswith('image/')):
                data = await message.download_media(bytes)

                if data:
                    media_list.append({
                        'type': 'document',
                        'mime_type': mime_type,
                        'data': base64.b64encode(data).decode('utf-8'),
                        'size': len(data),
                        'filename': filename or f"document_{doc.id}.{mime_type.split('/')[-1]}"
                    })
                else:
                    logger.warning("Nie udało się pobrać dokumentu")

    except Exception as e:
        logger.error(f"Błąd podczas pobierania mediów: {str(e)}")
        logger.exception(e)

    return media_list


async def save_message_to_db(message_data):
    """Zapisuje wiadomość do bazy danych"""
    try:
        # Konwersja dat z ISO string do obiektów datetime jeśli są w formie string
        timestamp = message_data['timestamp']
        received_at = message_data['received_at']

        if isinstance(timestamp, str):
            timestamp = dateutil.parser.parse(timestamp)

        if isinstance(received_at, str):
            received_at = dateutil.parser.parse(received_at)

        # Serializacja mediów do JSON jeśli istnieją
        media_json = None
        if 'media' in message_data and message_data['media']:
            try:
                # Sprawdzamy czy media to już string JSON
                if isinstance(message_data['media'], str):
                    # Już jest w formie JSON, używamy bezpośrednio
                    media_json = message_data['media']
                else:
                    # Konwertujemy obiekty do JSON
                    media_json = json.dumps(message_data['media'])
            except Exception as e:
                logger.error(f"Błąd podczas serializacji mediów do JSON: {str(e)}")
                logger.exception(e)

        async with db_pool.acquire() as conn:
            # Sprawdzamy czy kolumna 'media' istnieje
            try:
                columns = await conn.fetch(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TELEGRAM_MESSAGES_TABLE}'"
                )
                column_names = [col['column_name'] for col in columns]

                if 'media' not in column_names:
                    logger.info("Dodawanie kolumny 'media' do tabeli wiadomości...")
                    await conn.execute(
                        f"ALTER TABLE {TELEGRAM_MESSAGES_TABLE} ADD COLUMN media JSONB"
                    )
            except Exception as e:
                logger.error(f"Błąd podczas sprawdzania kolumny 'media': {str(e)}")

            if media_json:
                await conn.execute(
                    f'''
                    INSERT INTO {TELEGRAM_MESSAGES_TABLE} 
                    (message, chat_id, chat_title, chat_type, sender_id, sender_name, timestamp, received_at, is_new, media)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ''',
                    message_data['message'],
                    message_data['chat_id'],
                    message_data['chat_title'],
                    message_data.get('chat_type', 'unknown'),
                    message_data['sender_id'],
                    message_data['sender_name'],
                    timestamp,
                    received_at,
                    message_data.get('is_new', True),
                    media_json
                )
            else:
                await conn.execute(
                    f'''
                    INSERT INTO {TELEGRAM_MESSAGES_TABLE} 
                    (message, chat_id, chat_title, chat_type, sender_id, sender_name, timestamp, received_at, is_new)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ''',
                    message_data['message'],
                    message_data['chat_id'],
                    message_data['chat_title'],
                    message_data.get('chat_type', 'unknown'),
                    message_data['sender_id'],
                    message_data['sender_name'],
                    timestamp,
                    received_at,
                    message_data.get('is_new', True)
                )
    except Exception as e:
        logger.error(f"Błąd podczas zapisywania wiadomości do bazy danych: {str(e)}")
        logger.exception(e)


async def get_latest_message_timestamp():
    """Pobiera timestamp ostatniej wiadomości z bazy danych"""
    try:
        async with db_pool.acquire() as conn:
            latest_timestamp = await conn.fetchval(
                f"SELECT timestamp FROM {TELEGRAM_MESSAGES_TABLE} ORDER BY timestamp DESC LIMIT 1"
            )
            return latest_timestamp
    except Exception as e:
        logger.error(f"Błąd podczas pobierania ostatniego timestampa: {str(e)}")
        return None


async def load_messages_from_db():
    """Ładuje wiadomości z bazy danych do bufora"""
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {TELEGRAM_MESSAGES_TABLE} ORDER BY timestamp DESC LIMIT 100"
            )

            # Konwertujemy wiersze do słowników
            messages = []
            for row in rows:
                message = dict(row)
                # Formatujemy timestampy do ISO
                message['timestamp'] = message['timestamp'].isoformat()
                message['received_at'] = message['received_at'].isoformat()

                # Przetwarzamy media z JSONB do obiektu Python
                if 'media' in message and message['media']:
                    try:
                        # Jeśli media to już string JSON, konwertujemy go do obiektu Python
                        if isinstance(message['media'], str):
                            message['media'] = json.loads(message['media'])
                        # Typ JSONB z PostgreSQL zostanie już zdeserialiozwany przez asyncpg
                        # Upewniamy się tylko, że media to lista
                        if not isinstance(message['media'], list):
                            message['media'] = [message['media']]
                    except Exception as e:
                        logger.error(f"Błąd podczas przetwarzania mediów dla wiadomości {message.get('id')}: {str(e)}")
                        message['media'] = []
                else:
                    message['media'] = []

                messages.append(message)

            # Czyszczenie bufora i dodawanie wiadomości
            message_history.clear()
            for msg in messages:  # Już są posortowane od DESC w zapytaniu SQL
                message_history.append(msg)

            return messages
    except Exception as e:
        logger.error(f"Błąd podczas ładowania wiadomości z bazy danych: {str(e)}")
        logger.exception(e)
        return []


async def mark_all_messages_as_old():
    """Oznacza wszystkie wiadomości jako stare (nie nowe)"""
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE {TELEGRAM_MESSAGES_TABLE} SET is_new = FALSE"
            )
    except Exception as e:
        logger.error(f"Błąd podczas oznaczania wiadomości jako stare: {str(e)}")


async def load_historical_messages():
    """Pobiera historyczne wiadomości z Telegrama i zapisuje je do bazy danych"""
    logger.info("=== load_historical_messages (POMINIĘTO) ===")
    return True

async def send_to_webhook(data, webhook_url=None):
    """Wysyła dane do webhooka"""
    if not webhook_url:
        webhook_url = N8N_WEBHOOK_URL

    if not webhook_url:
        logger.warning("Brak skonfigurowanego URL dla webhooka")
        return

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(webhook_url, json=data) as response:
                if response.status != 200:
                    logger.error(f"Błąd podczas wysyłania do webhooka, status: {response.status}")
                    response_text = await response.text()
                    logger.error(f"Odpowiedź z webhooka: {response_text}")
        except aiohttp.ClientConnectorError as e:
            logger.error(f"Błąd połączenia z webhookiem: {str(e)}")
            logger.error(f"Nie można połączyć z {webhook_url}")
        except Exception as e:
            logger.error(f"Błąd podczas wysyłania do webhooka: {str(e)}")
            logger.exception(e)


async def broadcast_message(message):
    """Wysyła wiadomość do wszystkich podłączonych klientów WebSocket"""
    if websocket_clients:
        # Upewnijmy się, że media są w poprawnym formacie zanim wyślemy przez WebSocket
        if 'media' in message and message['media']:
            try:
                # Sprawdzamy czy media są już listą
                if not isinstance(message['media'], list):
                    if isinstance(message['media'], str):
                        message['media'] = json.loads(message['media'])
                    # Jeśli nadal nie jest listą, to konwertujemy na listę
                    if not isinstance(message['media'], list):
                        message['media'] = [message['media']]
                # Upewniamy się, że wszystkie elementy w liście są prawidłowymi obiektami mediów
                message['media'] = [m for m in message['media'] if isinstance(m, dict) and 'data' in m]
            except Exception as e:
                logger.error(f"Błąd podczas przygotowywania mediów dla WebSocket: {str(e)}")
                message['media'] = []
        else:
            message['media'] = []

        message_json = {
            'type': 'new_message',
            'message': message
        }

        clients_to_remove = set()

        for ws in list(websocket_clients):
            try:
                success = await send_websocket_json(ws, message_json)
                if not success:
                    clients_to_remove.add(ws)
            except Exception as e:
                logger.error(f"Błąd podczas wysyłania wiadomości do klienta WebSocket: {str(e)}")
                clients_to_remove.add(ws)

        # Usuwamy zepsute połączenia
        for ws in clients_to_remove:
            websocket_clients.discard(ws)


async def handle_new_message(event):
    logger.info("=== handle_new_message START ===")
    try:
        chat = await event.get_chat()
        sender = await event.get_sender()

        # 1. Identyfikacja czatu i TEMATU (Topic/Forum)
        # Dla prywatnych czatów (User) title i username mogą być None - fallback na imię sendera
        sender_name_str = ((getattr(sender, 'first_name', '') or '') + ' ' + (getattr(sender, 'last_name', '') or '')).strip() or None
        main_chat_title = (
            getattr(chat, 'title', None)
            or getattr(chat, 'username', None)
            or sender_name_str
            or 'Prywatny'
        )
        topic_id = None
        topic_suffix = ""

        # Sprawdzamy czy wiadomość należy do konkretnego wątku (Topic)
        if event.message.reply_to and hasattr(event.message.reply_to, 'reply_to_top_id'):
            topic_id = event.message.reply_to.reply_to_top_id
            if getattr(chat, 'forum', False) and topic_id:
                topic_name = await get_topic_name(event.chat_id, topic_id)
                topic_suffix = f" [{topic_name}]"
            else:
                topic_name = None
        else:
            topic_name = None

        full_display_title = f"{main_chat_title}{topic_suffix}"

        # Ekstrakcja danych do logu "wszystko wszystko wszystko"
        sender_id = str(getattr(sender, 'id', None))
        sender_username = getattr(sender, 'username', None)
        sender_display_name = (((getattr(sender, 'first_name', '') or '') + ' ' + (getattr(sender, 'last_name', '') or '')).strip() or "Brak")
        message_text_raw = (event.raw_text or '').replace('\n', '\\n')
        chat_id = str(getattr(chat, 'id', None))
        
        logger.info(f"NOWA WIADOMOŚĆ: Czat: {main_chat_title} ({chat_id}) | Temat: {topic_name or 'Brak'} | Nadawca: {sender_display_name} (@{sender_username or 'Brak'}, ID: {sender_id}) | Treść: {message_text_raw}")

        # --- FILTR: TYLKO GŁÓWNE WIADOMOŚCI (NIE ODPOWIEDZI) ---
        # Sprawdzamy czy wiadomość jest odpowiedzią (reply) do innej wiadomości
        reply_to_msg_id = event.message.reply_to.reply_to_msg_id if event.message.reply_to else None
        
        is_topic_main = False
        if reply_to_msg_id is None:
            # Brak jakiegokolwiek reply - to na pewno główna wiadomość
            is_topic_main = True
        elif topic_id is not None and reply_to_msg_id == topic_id:
            # W Telegram Forum, pierwsza wiadomość w wątku technicznym ma reply_to_msg_id wskazujący na ID tematu
            is_topic_main = True
        else:
            # Jeśli jest reply_to_msg_id i NIE pasuje do topic_id, to jest to odpowiedź (komentarz)
            is_topic_main = False

        if not is_topic_main:
            logger.info(f"ODRZUCONO (REPLY): {full_display_title} | Powód: Wiadomość jest odpowiedzią (reply_to_msg_id={reply_to_msg_id}, topic_id={topic_id})")
            return

        # 2. ZAMIANA IGNOROWANYCH NA ALLOWED (Biała lista)
        # Pobieramy ALLOWED_SENDERS z .env
        allowed_env = os.getenv('ALLOWED_SENDERS', '')
        allowed_simple, allowed_group_topics = parse_allowed_senders_spec(allowed_env)
        chat_username = getattr(chat, 'username', '') or ''

        msg_preview = (event.raw_text or '').replace('\n', '\\n')
        if len(msg_preview) > 300:
            msg_preview = msg_preview[:300] + "…"

        details = {
            "decision_stage": "pre_allowlist",
            "chat_id": str(getattr(chat, 'id', None)),
            "chat_title": getattr(chat, 'title', None),
            "chat_username": chat_username,
            "chat_forum": bool(getattr(chat, 'forum', False)),
            "main_chat_title": main_chat_title,
            "topic_id": topic_id,
            "topic_name": topic_name,
            "full_display_title": full_display_title,
            "event_chat_id": str(getattr(event, 'chat_id', None)),
            "message_id": str(getattr(event.message, 'id', None)),
            "sender_id": str(getattr(sender, 'id', None)),
            "sender_username": getattr(sender, 'username', None),
            "sender_name": (((getattr(sender, 'first_name', '') or '') + ' ' + (getattr(sender, 'last_name', '') or '')).strip() or None),
            "date": event.date.isoformat() if getattr(event, 'date', None) else None,
            "has_media": bool(getattr(event.message, 'media', None)),
            "text_len": len(event.raw_text or ''),
            "text_preview": msg_preview,
            "allowed_env": allowed_env,
            "allowed_simple": sorted(allowed_simple),
            "allowed_group_topics": {k: sorted(v) for k, v in allowed_group_topics.items()},
        }
        # logger.info("MSG_CHECK %s", json.dumps(details, ensure_ascii=False))

        allowed, reason = is_allowed_by_spec(
            allowed_simple=allowed_simple,
            allowed_group_topics=allowed_group_topics,
            main_chat_title=main_chat_title,
            full_display_title=full_display_title,
            chat_username=chat_username,
            topic_name=topic_name,
            sender_name=sender_name_str,
        )
        if not allowed:
            logger.info(f"ODRZUCONO (ALLOWLIST): {full_display_title} | Powód: {reason}")
            return
        logger.info("ZAAKCEPTOWANO [%s]: %s", full_display_title, (event.raw_text or 'MEDIA/BRAK TEKSTU').replace('\n', ' '))

        message_timezone = event.date.tzinfo
        media_list = await get_media_from_message(event)

        # Budujemy dane do n8n
        message_data = {
            'message': event.raw_text,
            'chat_id': str(chat.id),
            'chat_title': full_display_title, # Tu będzie np. "Trading PRO [Sygnały]"
            'chat_type': 'channel' if isinstance(chat, Channel) else 'group',
            'sender_id': str(getattr(sender, 'id', 0)),
            'sender_name': ((getattr(sender, 'first_name', '') or '') + ' ' + (getattr(sender, 'last_name', '') or '')).strip() or 'Nieznany',
            'timestamp': event.date.isoformat(),
            'received_at': datetime.now(message_timezone).isoformat(),
            'is_new': True,
            'media': media_list,
            'parent_message_id': str(reply_to_msg_id) if reply_to_msg_id else None, # ID głównej wiadomości
            'is_topic_main': True # Zawsze True tutaj, bo odfiltrowaliśmy resztę wyżej
        }

        # Zapis i wysyłka (standardowo jak w Twoim kodzie)
        await save_message_to_db(message_data)
        message_history.append(message_data)
        await broadcast_message(message_data)

        await send_to_webhook(message_data)

    except Exception as e:
        logger.error(f"Błąd w handle_new_message: {str(e)}")
        logger.exception(e)


# Funkcja do zapisu zbuforowanego albumu
async def save_grouped_message(key):
    grouped = grouped_messages_buffer.pop(key, None)
    grouped_messages_timers.pop(key, None)
    if grouped:
        await save_message_to_db(grouped)
        # Przygotuj dane do WebSocket i webhooka
        websocket_data = grouped.copy()
        if isinstance(websocket_data['timestamp'] , datetime):
            websocket_data['timestamp'] = websocket_data['timestamp'].isoformat()
        if isinstance(websocket_data['received_at'], datetime):
            websocket_data['received_at'] = websocket_data['received_at'].isoformat()
            
        message_history.append(websocket_data)
        await broadcast_message(websocket_data)

        # Wysyłamy na webhook tylko jeśli to główny wątek/nowa wiadomość
        if grouped.get('is_topic_main'):
            await send_to_webhook(websocket_data)


async def handle_messages(request):
    global client
    if not client or not client.is_connected():
        return web.Response(text="Klient nie jest połączony")
    return web.Response(text="OK")


async def get_messages(request):
    try:
        # Pobieramy wiadomości z bazy danych
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {TELEGRAM_MESSAGES_TABLE} ORDER BY timestamp DESC LIMIT 100"
            )

        # Konwertujemy wiersze do słowników
        messages = []
        for row in rows:
            message = dict(row)
            # Formatujemy timestampy do ISO
            message['timestamp'] = message['timestamp'].isoformat()
            message['received_at'] = message['received_at'].isoformat()

            # Przetwarzamy media z JSONB do obiektu Python
            if 'media' in message and message['media']:
                try:
                    # Jeśli media to już string JSON, konwertujemy go do obiektu Python
                    if isinstance(message['media'], str):
                        message['media'] = json.loads(message['media'])
                    # Upewniamy się, że media to lista
                    if not isinstance(message['media'], list):
                        message['media'] = [message['media']]
                except Exception as e:
                    logger.error(f"Błąd podczas przetwarzania mediów dla wiadomości {message.get('id')}: {str(e)}")
                    message['media'] = []
            else:
                message['media'] = []

            messages.append(message)

        return web.json_response({'messages': messages})
    except Exception as e:
        logger.error(f"Błąd podczas pobierania wiadomości: {str(e)}")
        logger.exception(e)
        return web.json_response({'messages': list(message_history)})


async def index(request):
    file_path = pathlib.Path(__file__).parent / 'templates' / 'index.html'
    return web.FileResponse(path=file_path)


async def check_session(request):
    global client
    if not client or not client.is_connected():
        await init_client()
    try:
        if not client.is_connected():
            await client.connect()

        is_authorized = await client.is_user_authorized()
        status = "autoryzowany" if is_authorized else "nieautoryzowany"
        logger.info(f"Status klienta: {status}")
        return web.json_response({'authorized': is_authorized})
    except Exception as e:
        logger.error(f"Błąd podczas sprawdzania sesji: {str(e)}")
        return web.json_response({'authorized': False, 'error': str(e)})


async def request_code(request):
    global phone_code_hash
    try:
        if not client.is_connected():
            await client.connect()
        send_code_result = await client.send_code_request(PHONE)
        phone_code_hash = send_code_result.phone_code_hash
        return web.json_response({'success': True})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def verify_code(request):
    global phone_code_hash
    try:
        data = await request.json()
        code = data.get('code')
        if not code:
            return web.json_response({'success': False, 'error': 'Nie podano kodu'})
        await client.sign_in(PHONE, code, phone_code_hash=phone_code_hash)
        return web.json_response({'success': True})
    except Exception as e:
        return web.json_response({'success': False, 'error': str(e)})


async def init_client():
    logger.info("=== init_client ===")
    global client
    session_dir = os.getenv('TELEGRAM_SESSION_DIR', '/data')
    session_file = os.path.join(session_dir, 'telegram_reader_session.session')

    if not os.path.exists(session_dir):
        logger.warning(f"Katalog sesji nie istnieje, tworzę: {session_dir}")
        os.makedirs(session_dir)

    # Jeśli plik sesji istnieje ale jest nieważny, usuwamy go
    if os.path.exists(session_file):
        try:
            client = TelegramClient(session_file, API_ID, API_HASH)
            await client.connect()
            if not await client.is_user_authorized():
                logger.warning("Sesja nieważna - usuwam plik sesji")
                await client.disconnect()
                os.remove(session_file)
                client = None
        except Exception as e:
            logger.error(f"Błąd podczas sprawdzania sesji: {str(e)}")
            logger.warning("Usuwam uszkodzony plik sesji")
            os.remove(session_file)
            client = None

    # Tworzymy nowego klienta jeśli nie istnieje
    if not client:
        client = TelegramClient(session_file, API_ID, API_HASH)

    try:
        if not client.is_connected():
            await client.connect()

        if await client.is_user_authorized():
            logger.info("✓ Sesja jest aktywna i zautoryzowana")
            me = await client.get_me()
            logger.info(f"Zalogowano jako: {me.first_name} (@{me.username})")
            # Dodajemy handler po pomyślnej inicjalizacji
            client.add_event_handler(handle_new_message, events.NewMessage)
        else:
            logger.warning("✗ Sesja wymaga autoryzacji")
    except Exception as e:
        logger.error(f"Błąd podczas sprawdzania stanu sesji: {str(e)}")
        logger.warning("✗ Sesja wymaga autoryzacji")

    return client


async def reload_messages(request):
    """Endpunkt do ręcznego ponownego załadowania wiadomości"""
    global client

    if not client or not client.is_connected():
        return web.json_response({'success': False, 'error': 'Klient nie jest połączony'})

    try:
        # Pobieranie i zapisywanie historycznych wiadomości
        success = await load_historical_messages()

        if success:
            return web.json_response({'success': True, 'message': 'Wiadomości zostały ponownie załadowane'})
        else:
            return web.json_response({'success': False, 'error': 'Błąd podczas ładowania wiadomości'})
    except Exception as e:
        logger.error(f"Błąd podczas ponownego ładowania wiadomości: {str(e)}")
        logger.exception(e)
        return web.json_response({'success': False, 'error': str(e)})


async def main():

    """Główna funkcja programu"""
    global client

    logger.info("=== Uruchamianie aplikacji ===")

    # Inicjalizacja klienta Telegram
    client = await init_client()

    # Inicjalizacja bazy danych
    success = await init_database()
    if not success:
        logger.error("Nie udało się połączyć z bazą danych - aplikacja używa tylko pamięci")

    # Konfiguracja serwera HTTP
    app = web.Application()
    app.router.add_get('/', index)
    app.router.add_get('/messages', get_messages)
    app.router.add_post('/request_code', request_code)
    app.router.add_post('/verify_code', verify_code)
    app.router.add_get('/check_session', check_session)
    app.router.add_get('/reload', reload_messages)

    # Obsługa WebSocketa
    async def websocket_route_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        websocket_clients.add(ws)

        try:
            # Pobierz wszystkie wiadomości z bazy danych
            messages = await load_messages_from_db()

            # Dodatkowe sprawdzenie formatu danych przed wysłaniem
            for msg in messages:
                # Sprawdzamy czy media jest tablicą
                if 'media' in msg and not isinstance(msg['media'], list):
                    logger.warning(f"Media nie jest tablicą w WebSocket: {type(msg['media'])}")
                    try:
                        if isinstance(msg['media'], str):
                            msg['media'] = json.loads(msg['media'])
                        # Jeśli nadal nie jest listą, to konwertujemy
                        if not isinstance(msg['media'], list):
                            msg['media'] = [msg['media']]
                    except Exception as e:
                        logger.error(f"Błąd podczas przetwarzania mediów dla WebSocket: {str(e)}")
                        msg['media'] = []
                elif 'media' not in msg:
                    msg['media'] = []

            # Wysyłamy historię wiadomości do klienta WebSocket
            history_data = {
                'type': 'history',
                'messages': messages
            }
            await send_websocket_json(ws, history_data)

            # Nasłuchujemy na wiadomości od klienta
            async for msg in ws:
                if msg.type == web.WSMsgType.ERROR:
                    logger.error(f"Błąd WebSocket: {ws.exception()}")
        except Exception as e:
            logger.error(f"Błąd w obsłudze WebSocket: {str(e)}")
            logger.exception(e)
        finally:
            websocket_clients.discard(ws)

        return ws

    # Dodanie handlera WebSocket - tylko raz
    app.router.add_get('/ws', websocket_route_handler)

    # Uruchomienie serwera HTTP
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("Serwer HTTP uruchomiony na http://0.0.0.0:8080")

    # Uruchomienie klienta
    try:
        if client and await client.is_user_authorized():
            # Uruchamiamy klienta tylko jeśli jest zautoryzowany
            await client.run_until_disconnected()
        else:
            # Jeśli nie jest zautoryzowany, czekamy na autoryzację
            logger.warning("Oczekiwanie na autoryzację...")
            while True:
                await asyncio.sleep(5)  # Sprawdzamy co 5 sekund
                if client and await client.is_user_authorized():
                    logger.info("Klient został zautoryzowany!")
                    await client.run_until_disconnected()
                    break
    except Exception as e:
        logger.error(f"Błąd w głównej funkcji: {str(e)}")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
