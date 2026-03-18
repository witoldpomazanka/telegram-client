print("=== STARTUJEMY APLIKACJĘ ===")
import asyncio
import base64
import json
import logging
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
from telethon.tl.functions.messages import GetBotCallbackAnswerRequest
from telethon.tl.types import MessageReplyHeader
from telethon.tl.functions.channels import GetForumTopicsRequest

# Konfiguracja logowania
logger = logging.getLogger('telegram_reader')
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Logger dla operacji degen move
degen_logger = logging.getLogger('degen_move')
degen_logger.setLevel(logging.INFO)
degen_handler = logging.StreamHandler(sys.stdout)
degen_handler.setLevel(logging.INFO)
degen_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
degen_handler.setFormatter(degen_formatter)
degen_logger.addHandler(degen_handler)

logging.getLogger('aiohttp.access').setLevel(logging.WARNING)
logging.getLogger('telethon.network.mtprotosender').setLevel(logging.WARNING)

# Ładowanie zmiennych środowiskowych
load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')
USERNAME = os.getenv('USERNAME')
N8N_WEBHOOK_URL = os.getenv('N8N_WEBHOOK_URL')
N8N_DEGEN_WEBHOOK_URL = os.getenv('N8N_DEGEN_WEBHOOK_URL')
DEGEN_GEMS_CHANNEL_NAMES = os.getenv('DEGEN_GEMS_CHANNEL_NAMES', '')
DEGEN_GEMS_CHANNEL_LIST = [s.strip() for s in DEGEN_GEMS_CHANNEL_NAMES.split(',') if s.strip()]
SUPPORTED_CHAINS = os.getenv('SUPPORTED_CHAINS', 'Base,ETH,SOL')
SUPPORTED_CHAINS_LIST = [chain.strip() for chain in SUPPORTED_CHAINS.split(',')]
CHAIN_TO_CHANNEL_MAPPING = os.getenv('CHAIN_TO_CHANNEL_MAPPING', 'Base,channel1,Buy 0.1 BASE,ETH,channel2,Buy 0.1 ETH,SOL,channel3,Buy 0.1 SOL')
CHAIN_CHANNEL_MAP = {}

# Przetwarzanie mapowania łańcuchów na kanały i przyciski
chain_channel_pairs = [pair.strip() for pair in CHAIN_TO_CHANNEL_MAPPING.split(',')]
for i in range(0, len(chain_channel_pairs), 3):
    if i + 2 < len(chain_channel_pairs):
        CHAIN_CHANNEL_MAP[chain_channel_pairs[i]] = {
            'chat_id': chain_channel_pairs[i + 1],
            'button_text': chain_channel_pairs[i + 2]
        }

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


def is_allowed_by_spec(*, allowed_simple, allowed_group_topics, main_chat_title, full_display_title, chat_username, topic_name):
    candidates = {main_chat_title, full_display_title}
    if chat_username:
        candidates.add(chat_username)

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
    logger.info("=== load_historical_messages ===")
    global client
    try:
        if not client.is_connected() or not await client.is_user_authorized():
            logger.warning("Klient nie jest połączony lub wymaga autoryzacji")
            return False

        latest_timestamp = await get_latest_message_timestamp()
        added_messages = 0
        async for dialog in client.iter_dialogs():
            messages_to_process = []
            async for message in client.iter_messages(dialog.id, limit=100):
                if latest_timestamp and message.date <= latest_timestamp:
                    continue
                messages_to_process.append(message)
            # --- BUFOROWANIE ALBUMÓW ---
            grouped_buffer = {}
            for message in messages_to_process:
                try:
                    chat = await message.get_chat()
                    # Określamy typ czatu dla każdej wiadomości
                    chat_type = 'unknown'
                    if isinstance(chat, User):
                        chat_type = 'private'
                    elif isinstance(chat, Chat):
                        chat_type = 'group'
                    elif isinstance(chat, Channel):
                        if getattr(chat, 'broadcast', False):
                            chat_type = 'channel'
                        else:
                            chat_type = 'supergroup'
                    sender = await message.get_sender()
                    sender_name = getattr(sender, 'first_name', '') if sender else ''
                    if sender and getattr(sender, 'last_name', None):
                        sender_name += ' ' + sender.last_name
                    message_timezone = message.date.tzinfo
                    media_list = await get_media_from_message(message)
                    grouped_id = getattr(message, 'grouped_id', None)
                    chat_id = str(chat.id)
                    if grouped_id:
                        key = (chat_id, str(grouped_id))
                        if key not in grouped_buffer:
                            grouped_buffer[key] = {
                                'message': message.text or '',
                                'chat_id': chat_id,
                                'chat_title': getattr(chat, 'title', None) or getattr(chat, 'username', None) or 'Prywatny',
                                'chat_type': chat_type,
                                'sender_id': str(sender.id if sender else 0),
                                'sender_name': sender_name or 'Nieznany',
                                'timestamp': message.date,
                                'received_at': datetime.now(message_timezone),
                                'is_new': False,
                                'media': []
                            }
                        grouped_buffer[key]['media'].extend(media_list)
                        # Aktualizuj timestamp na najnowszy
                        grouped_buffer[key]['timestamp'] = message.date
                        grouped_buffer[key]['received_at'] = datetime.now(message_timezone)
                    else:
                        message_data = {
                            'message': message.text or '',
                            'chat_id': chat_id,
                            'chat_title': getattr(chat, 'title', None) or getattr(chat, 'username', None) or 'Prywatny',
                            'chat_type': chat_type,
                            'sender_id': str(sender.id if sender else 0),
                            'sender_name': sender_name or 'Nieznany',
                            'timestamp': message.date,
                            'received_at': datetime.now(message_timezone),
                            'is_new': False,
                            'media': media_list
                        }
                        await save_message_to_db(message_data)
                        added_messages += 1
                except Exception as e:
                    logger.error(f"Błąd podczas przetwarzania wiadomości historycznej: {str(e)}")
                    logger.exception(e)
            # Po przetworzeniu wszystkich wiadomości z dialogu, zapisz albumy
            for grouped in grouped_buffer.values():
                await save_message_to_db(grouped)
                added_messages += 1
        logger.info(f"Dodano {added_messages} nowych wiadomości do bazy danych")
        await load_messages_from_db()
        await mark_all_messages_as_old()
        return True
    except Exception as e:
        logger.error(f"Błąd podczas ładowania historycznych wiadomości: {str(e)}")
        logger.exception(e)
        return False


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
        main_chat_title = getattr(chat, 'title', None) or getattr(chat, 'username', 'Prywatny')
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
            "sender_name": ((getattr(sender, 'first_name', '') + ' ' + getattr(sender, 'last_name', '')).strip() or None),
            "date": event.date.isoformat() if getattr(event, 'date', None) else None,
            "has_media": bool(getattr(event.message, 'media', None)),
            "text_len": len(event.raw_text or ''),
            "text_preview": msg_preview,
            "allowed_env": allowed_env,
            "allowed_simple": sorted(allowed_simple),
            "allowed_group_topics": {k: sorted(v) for k, v in allowed_group_topics.items()},
        }
        logger.info("MSG_CHECK %s", json.dumps(details, ensure_ascii=False))

        allowed, reason = is_allowed_by_spec(
            allowed_simple=allowed_simple,
            allowed_group_topics=allowed_group_topics,
            main_chat_title=main_chat_title,
            full_display_title=full_display_title,
            chat_username=chat_username,
            topic_name=topic_name,
        )
        if not allowed:
            logger.info("MSG_SKIP %s reason=%s", full_display_title, reason)
            return
        logger.info("MSG_ACCEPT %s reason=%s", full_display_title, reason)

        # 3. IDENTYFIKACJA WIADOMOŚCI NADRZĘDNEJ (Parent)
        # parent_id to ID wiadomości, na którą ktoś odpisał (jeśli dotyczy)
        parent_id = event.message.reply_to.reply_to_msg_id if event.message.reply_to else None

        message_timezone = event.date.tzinfo
        media_list = await get_media_from_message(event)

        # Budujemy dane do n8n
        message_data = {
            'message': event.raw_text,
            'chat_id': str(chat.id),
            'chat_title': full_display_title, # Tu będzie np. "Trading PRO [Sygnały]"
            'chat_type': 'channel' if isinstance(chat, Channel) else 'group',
            'sender_id': str(getattr(sender, 'id', 0)),
            'sender_name': (getattr(sender, 'first_name', '') + ' ' + getattr(sender, 'last_name', '')).strip() or 'Nieznany',
            'timestamp': event.date.isoformat(),
            'received_at': datetime.now(message_timezone).isoformat(),
            'is_new': True,
            'media': media_list,
            'parent_message_id': str(parent_id) if parent_id else None, # ID głównej wiadomości
            'is_topic_main': parent_id is None or parent_id == topic_id # Flaga czy to start wątku
        }

        # Zapis i wysyłka (standardowo jak w Twoim kodzie)
        await save_message_to_db(message_data)
        message_history.append(message_data)
        await broadcast_message(message_data)

        # Degen filter
        if any(channel_name.lower() in full_display_title.lower() for channel_name in DEGEN_GEMS_CHANNEL_LIST):
            await send_to_webhook(message_data, N8N_DEGEN_WEBHOOK_URL)
        else:
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
        websocket_data['timestamp'] = websocket_data['timestamp'].isoformat()
        websocket_data['received_at'] = websocket_data['received_at'].isoformat()
        message_history.append(websocket_data)
        await broadcast_message(websocket_data)

        # Wysyłamy na webhook bez dodatkowego zagnieżdżenia
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


async def send_message_to_base_channel(message):
    """Wysyła wiadomość do kanału Base"""
    logger.info("Implementacja wysyłki do kanału Base")
    # TODO: Implementacja

async def send_message_to_eth_channel(message):
    """Wysyła wiadomość do kanału ETH"""
    logger.info("Implementacja wysyłki do kanału ETH")
    # TODO: Implementacja

async def wait_for_bot_response(client, chat_id, message_to_send=None, timeout=10):
    """Czeka na odpowiedź od bota w określonym czacie"""
    try:
        async with client.conversation(chat_id, timeout=timeout) as conv:
            if message_to_send:
                # Wysyłamy wiadomość w kontekście konwersacji
                await conv.send_message(message_to_send)
            # Czekamy na odpowiedź
            response = await conv.get_response()
            degen_logger.info(f"Otrzymano odpowiedź od bota: {response.text}")

            # Sprawdzamy czy wiadomość ma przyciski
            if response.reply_markup and hasattr(response.reply_markup, 'rows'):
                degen_logger.info("Wykryto przyciski w odpowiedzi bota")
                return response
            return response
    except Exception as e:
        degen_logger.error(f"Błąd podczas oczekiwania na odpowiedź bota: {str(e)}")
        raise

async def click_button(client, message, button_text):
    """Klika przycisk o określonej treści w wiadomości"""
    try:
        if not message.reply_markup or not hasattr(message.reply_markup, 'rows'):
            degen_logger.error("Brak przycisków w wiadomości")
            return None

        # Logujemy wszystkie dostępne przyciski
        degen_logger.info("Dostępne przyciski:")
        for row_idx, row in enumerate(message.reply_markup.rows):
            for button_idx, button in enumerate(row.buttons):
                degen_logger.info(f"Rząd {row_idx + 1}, Przycisk {button_idx + 1}:")
                degen_logger.info(f"  - Tekst: {button.text}")
                degen_logger.info(f"  - Typ: {type(button).__name__}")
                if hasattr(button, 'data'):
                    degen_logger.info(f"  - Data: {button.data}")

        # Szukamy przycisku o określonej treści
        for row in message.reply_markup.rows:
            for button in row.buttons:
                if button.text == button_text:
                    degen_logger.info(f"Znaleziono przycisk: {button_text}")
                    # Klikanie przycisku poprzez callback_query
                    await client(GetBotCallbackAnswerRequest(
                        peer=message.peer_id,
                        msg_id=message.id,
                        data=button.data
                    ))
                    return True

        degen_logger.error(f"Nie znaleziono przycisku o treści: {button_text}")
        return None
    except Exception as e:
        degen_logger.error(f"Błąd podczas klikania przycisku: {str(e)}")
        raise

async def send_message_to_sol_channel(message):
    """Wysyła sekwencję wiadomości do kanału SOL"""
    try:
        global client
        if not client or not client.is_connected():
            degen_logger.error("Klient Telegram nie jest połączony")
            raise Exception("Klient Telegram nie jest połączony")

        # Pobieramy chat_id i tekst przycisku z mapowania
        chain_config = CHAIN_CHANNEL_MAP.get('SOL')
        if not chain_config:
            degen_logger.error("Brak skonfigurowanego chat_id dla łańcucha SOL")
            raise Exception("Brak skonfigurowanego chat_id dla łańcucha SOL")

        chat_name = chain_config['chat_id']
        button_text = chain_config['button_text']

        # Szukamy konwersacji po nazwie
        degen_logger.info(f"Szukam konwersacji o nazwie: {chat_name}")
        async for dialog in client.iter_dialogs():
            if dialog.name == chat_name:
                chat_id = dialog.id
                degen_logger.info(f"Znaleziono konwersację o ID: {chat_id}")
                break
        else:
            degen_logger.error(f"Nie znaleziono konwersacji o nazwie: {chat_name}")
            raise Exception(f"Nie znaleziono konwersacji o nazwie: {chat_name}")

        degen_logger.info(f"Wysyłam sekwencję wiadomości do kanału SOL (chat_id: {chat_id}, przycisk: {button_text})")

        # Pierwsza wiadomość - komenda /buy_sell
        degen_logger.info("Wysyłam komendę /buy_sell")
        bot_response = await wait_for_bot_response(client, chat_id, "/buy_sell")
        degen_logger.info(f"Otrzymano odpowiedź od bota: {bot_response.text}")

        # Druga wiadomość - adres
        address = message.split('\n')[1].split(': ')[1]  # Wyciągamy adres z wiadomości
        degen_logger.info(f"Wysyłam adres: {address}")
        bot_response = await wait_for_bot_response(client, chat_id, address)

        # Klikamy przycisk z konfiguracji
        degen_logger.info(f"Próbuję kliknąć przycisk: {button_text}")
        await click_button(client, bot_response, button_text)

        # Czekamy na potwierdzenie transakcji
        degen_logger.info("Oczekuję na potwierdzenie transakcji")
        transaction_confirmed = False
        try:
            # Sprawdzamy kilka kolejnych wiadomości
            async for event in client.iter_messages(chat_id, limit=20, wait_time=65):
                if event.sender_id == bot_response.sender_id:
                    degen_logger.info(f"Otrzymano wiadomość: {event.text}")
                    # Sprawdzamy czy to właściwe potwierdzenie transakcji
                    if "The transaction executed successfully!" in event.text:
                        degen_logger.info("Transakcja potwierdzona!")
                        transaction_confirmed = True
                        # Czekamy 10 sekund przed kliknięciem przycisku sprzedaży
                        degen_logger.info("Czekam 10 sekund przed ustawieniem automatycznej sprzedaży...")
                        await asyncio.sleep(10)
                        # Czekamy na przyciski sprzedaży
                        degen_logger.info("Oczekuję na przyciski sprzedaży")
                        async for next_event in client.iter_messages(chat_id, limit=5, wait_time=65):
                            if next_event.sender_id == bot_response.sender_id and next_event.reply_markup:
                                degen_logger.info("Znaleziono przyciski sprzedaży")
                                # Szukamy przycisku "At 100% Rise Sell 50%"
                                for row in next_event.reply_markup.rows:
                                    for button in row.buttons:
                                        if button.text == "At 100% Rise Sell 50%":
                                            degen_logger.info("Klikam przycisk sprzedaży")
                                            await click_button(client, next_event, "At 100% Rise Sell 50%")
                                            return True
                        break
        except Exception as e:
            degen_logger.error(f"Błąd podczas oczekiwania na potwierdzenie transakcji: {str(e)}")
            raise

        if not transaction_confirmed:
            degen_logger.error("Nie otrzymano potwierdzenia transakcji")
            raise Exception("Nie otrzymano potwierdzenia transakcji")

        degen_logger.info("Sekwencja wiadomości dla SOL zakończona pomyślnie")
        return True

    except Exception as e:
        degen_logger.error(f"Błąd podczas wysyłania wiadomości do kanału SOL: {str(e)}")
        logger.exception(e)
        raise

async def handle_degen_move(request):
    """Obsługa endpointu /degen-move"""
    try:
        data = await request.json()
        degen_logger.info(f"Otrzymano request /degen-move z danymi: {data}")

        # Sprawdzenie czy to invalid content
        if data.get('content') == 'invalid':
            degen_logger.info("Otrzymano invalid content - pomijam")
            return web.json_response({'status': 'ignored'})

        # Sprawdzenie czy mamy wymagane pola
        if 'address' not in data or 'chain' not in data:
            degen_logger.error("Brak wymaganych pól w payload")
            return web.json_response(
                {'error': 'Brak wymaganych pól: address i chain'},
                status=400
            )

        address = data['address']
        chain = data['chain']

        # Walidacja łańcucha
        if chain not in SUPPORTED_CHAINS_LIST:
            degen_logger.error(f"Nieobsługiwany łańcuch: {chain}")
            return web.json_response(
                {'error': f'Nieobsługiwany łańcuch. Obsługiwane: {", ".join(SUPPORTED_CHAINS_LIST)}'},
                status=400
            )

        degen_logger.info(f"Przetwarzanie degen move dla adresu {address} na łańcuchu {chain}")

        # Przygotowanie wiadomości
        message = f"Nowy degen move!\nAdres: {address}\nŁańcuch: {chain}"

        # Wysyłka wiadomości do odpowiedniego kanału
        try:
            if chain == 'Base':
                await send_message_to_base_channel(message)
            elif chain == 'ETH':
                await send_message_to_eth_channel(message)
            elif chain == 'SOL':
                await send_message_to_sol_channel(message)

            degen_logger.info(f"Wiadomość wysłana do kanału dla łańcucha {chain}")
        except Exception as e:
            degen_logger.error(f"Błąd podczas wysyłania wiadomości do kanału: {str(e)}")
            logger.exception(e)
            return web.json_response(
                {'error': 'Błąd podczas wysyłania wiadomości do kanału'},
                status=500
            )

        return web.json_response({
            'status': 'success',
            'message': f'Przetworzono degen move dla {address} na {chain}'
        })

    except json.JSONDecodeError:
        degen_logger.error("Nieprawidłowy format JSON")
        return web.json_response(
            {'error': 'Nieprawidłowy format JSON'},
            status=400
        )
    except Exception as e:
        degen_logger.error(f"Błąd podczas przetwarzania requestu: {str(e)}")
        logger.exception(e)
        return web.json_response(
            {'error': 'Wewnętrzny błąd serwera'},
            status=500
        )

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
    app.router.add_post('/degen-move', handle_degen_move)  # Dodajemy nowy endpoint

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
            # Pobieranie historycznych wiadomości tylko jeśli klient jest zautoryzowany
            await load_historical_messages()
            # Uruchamiamy klienta tylko jeśli jest zautoryzowany
            await client.run_until_disconnected()
        else:
            # Jeśli nie jest zautoryzowany, czekamy na autoryzację
            logger.warning("Oczekiwanie na autoryzację...")
            while True:
                await asyncio.sleep(5)  # Sprawdzamy co 5 sekund
                if client and await client.is_user_authorized():
                    logger.info("Klient został zautoryzowany!")
                    await load_historical_messages()
                    await client.run_until_disconnected()
                    break
    except Exception as e:
        logger.error(f"Błąd w głównej funkcji: {str(e)}")
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
