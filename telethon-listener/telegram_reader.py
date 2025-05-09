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

# Konfiguracja logowania
logger = logging.getLogger('telegram_reader')
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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


async def send_to_webhook(data):
    if not N8N_WEBHOOK_URL:
        logger.warning("Brak skonfigurowanego URL dla webhooka (N8N_WEBHOOK_URL)")
        return

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(N8N_WEBHOOK_URL, json=data) as response:
                if response.status != 200:
                    logger.error(f"Błąd podczas wysyłania do webhooka, status: {response.status}")
                    response_text = await response.text()
                    logger.error(f"Odpowiedź z webhooka: {response_text}")
        except aiohttp.ClientConnectorError as e:
            logger.error(f"Błąd połączenia z webhookiem: {str(e)}")
            logger.error(f"Nie można połączyć z {N8N_WEBHOOK_URL}")
        except Exception as e:
            logger.error(f"Błąd podczas wysyłania do webhooka: {str(e)}")
            logger.exception(e)  # Pełny stacktrace


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


# Handler dla nowych wiadomości
async def handle_new_message(event):
    logger.info("=== handle_new_message START ===")
    logger.info(f"Lista ignorowanych nadawców: {IGNORED_SENDERS_LIST}")
    try:
        chat = await event.get_chat()
        sender = await event.get_sender()
        
        # Szczegółowe logowanie informacji o nadawcy
        sender_id = getattr(sender, 'id', 'Unknown ID')
        username = getattr(sender, 'username', None)
        first_name = getattr(sender, 'first_name', '')
        last_name = getattr(sender, 'last_name', '')
        sender_name = first_name
        if last_name:
            sender_name += ' ' + last_name
            
        # Szczegółowe logowanie informacji o czacie
        chat_id = getattr(chat, 'id', 'Unknown ID')
        chat_username = getattr(chat, 'username', None)
        chat_title = getattr(chat, 'title', None) or chat_username or 'Prywatny'
        
        logger.info(f"Nadawca ID: {sender_id}, Username: {username}, Imię: {first_name}, Nazwisko: {last_name}")
        logger.info(f"Treść wiadomości: {event.raw_text[:100]}...")  # pierwsze 100 znaków
        logger.info(f"Chat ID: {chat_id}, Nazwa: {chat_title}, Username: {chat_username}")
        
        # Określamy typ czatu
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
        
        logger.info(f"Typ chatu: {chat_type}")
        
        # Pomijanie wiadomości od wybranych nadawców lub z określonych chatów
        if username and username in IGNORED_SENDERS_LIST:
            logger.info(f"Pomijam wiadomość - username '{username}' jest na liście ignorowanych")
            return
        elif sender_name and sender_name in IGNORED_SENDERS_LIST:
            logger.info(f"Pomijam wiadomość - nazwa nadawcy '{sender_name}' jest na liście ignorowanych")
            return
        elif chat_title in IGNORED_SENDERS_LIST:
            logger.info(f"Pomijam wiadomość - nazwa czatu '{chat_title}' jest na liście ignorowanych")
            return
        
        message_timezone = event.date.tzinfo
        
        # Pobieramy media z wiadomości
        logger.info("Rozpoczynam pobieranie mediów...")
        media_list = await get_media_from_message(event)
        logger.info(f"Pobrano {len(media_list)} elementów mediów")
        
        # Obsługa albumów (grouped_id)
        grouped_id = getattr(event, 'grouped_id', None)
        if grouped_id:
            logger.info(f"Wykryto album (grouped_id: {grouped_id})")
            key = (str(event.chat_id), str(grouped_id))
            if key not in grouped_messages_buffer:
                logger.info("Tworzę nowy bufor dla albumu")
                grouped_messages_buffer[key] = {
                    'message': event.raw_text or '',
                    'chat_id': str(chat.id),
                    'chat_title': getattr(chat, 'title', None) or getattr(chat, 'username', None) or 'Prywatny',
                    'chat_type': chat_type,
                    'sender_id': str(sender.id if sender else 0),
                    'sender_name': sender_name or 'Nieznany',
                    'timestamp': event.date,
                    'received_at': datetime.now(message_timezone),
                    'is_new': True,
                    'media': []
                }
            # Dodaj media do bufora
            logger.info(f"Dodaję {len(media_list)} mediów do bufora albumu")
            grouped_messages_buffer[key]['media'].extend(media_list)
            # Aktualizuj timestamp na najnowszy
            grouped_messages_buffer[key]['timestamp'] = event.date
            grouped_messages_buffer[key]['received_at'] = datetime.now(message_timezone)
            # Ustaw/odnów timer
            if key in grouped_messages_timers:
                grouped_messages_timers[key].cancel()
            loop = asyncio.get_event_loop()
            grouped_messages_timers[key] = loop.call_later(
                GROUPED_MESSAGE_TIMEOUT,
                lambda: asyncio.ensure_future(save_grouped_message(key))
            )
            logger.info("Ustawiono timer dla albumu")
            return  # nie zapisuj pojedynczej wiadomości od razu
        
        logger.info("Przygotowuję dane wiadomości do zapisu")
        message_data = {
            'message': event.raw_text,
            'chat_id': str(chat.id),
            'chat_title': getattr(chat, 'title', None) or getattr(chat, 'username', None) or 'Prywatny',
            'chat_type': chat_type,
            'sender_id': str(sender.id if sender else 0),
            'sender_name': sender_name or 'Nieznany',
            'timestamp': event.date,
            'received_at': datetime.now(message_timezone),
            'is_new': True,
            'media': media_list
        }
        
        # Zapisujemy wiadomość do bazy danych
        logger.info("Zapisuję wiadomość do bazy danych")
        await save_message_to_db(message_data)
        logger.info("Wiadomość zapisana w bazie danych")
        
        # Przygotowujemy dane do wysyłki przez WebSocket i do bufora
        websocket_data = message_data.copy()
        websocket_data['timestamp'] = websocket_data['timestamp'].isoformat()
        websocket_data['received_at'] = websocket_data['received_at'].isoformat()
        
        logger.info("Dodaję wiadomość do historii")
        message_history.append(websocket_data)
        
        logger.info("Wysyłam wiadomość przez WebSocket")
        await broadcast_message(websocket_data)
        logger.info("Wiadomość wysłana przez WebSocket")
        
        # Wysyłamy na webhook bez dodatkowego zagnieżdżenia
        logger.info("Wysyłam wiadomość na webhook")
        await send_to_webhook(websocket_data)
        logger.info("Wiadomość wysłana na webhook")
        
    except Exception as e:
        logger.error(f"Błąd podczas przetwarzania wiadomości: {str(e)}")
        logger.exception(e)  # Dodajemy pełny stacktrace błędu
    
    logger.info("=== handle_new_message END ===")


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
