import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

DB_PATH = 'chat.db'
HTML_FILE = 'done.html'

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        'http://localhost:8000', 'http://127.0.0.1:8000',
        'https://the-game-persronal-production.up.railway.app',
        'https://frontend-production.up.railway.app'
    ],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],

)

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row

clients: Dict[str, List[WebSocket]] = {'admin': [], 'user': []}

class LocationPayload(BaseModel):
    lat: float
    lng: float
    acc: int
    stage: int

class PinPayload(BaseModel):
    lat: float
    lng: float
    label: Optional[str] = ''

class MessagePayload(BaseModel):
    msg: Optional[str] = None
    voiceB64: Optional[str] = None
    voiceMime: Optional[str] = None
    reply: bool = False

class ReplyPayload(BaseModel):
    msg: str


def dict_from_row(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def init_db() -> None:
    cursor = conn.cursor()
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS pins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lat REAL NOT NULL,
            lng REAL NOT NULL,
            label TEXT,
            created_at TEXT NOT NULL
        )'''
    )
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS location (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            lat REAL,
            lng REAL,
            acc INTEGER,
            stage INTEGER,
            updated_at TEXT
        )'''
    )
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msg TEXT,
            voiceB64 TEXT,
            voiceMime TEXT,
            reply INTEGER,
            shown INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )'''
    )
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS replies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msg TEXT NOT NULL,
            created_at TEXT NOT NULL
        )'''
    )
    conn.commit()


def get_pins() -> List[dict]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM pins ORDER BY created_at ASC')
    return [dict_from_row(row) for row in cursor.fetchall()]


def get_latest_location() -> Optional[dict]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM location WHERE id = 1')
    row = cursor.fetchone()
    return dict_from_row(row) if row else None


def upsert_location(payload: LocationPayload) -> dict:
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    cursor.execute(
        '''INSERT INTO location (id, lat, lng, acc, stage, updated_at)
           VALUES (1, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             lat = excluded.lat,
             lng = excluded.lng,
             acc = excluded.acc,
             stage = excluded.stage,
             updated_at = excluded.updated_at''',
        (payload.lat, payload.lng, payload.acc, payload.stage, now)
    )
    conn.commit()
    return get_latest_location()


def create_pin(payload: PinPayload) -> dict:
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    cursor.execute('INSERT INTO pins (lat, lng, label, created_at) VALUES (?, ?, ?, ?)', (payload.lat, payload.lng, payload.label or '', now))
    conn.commit()
    pin_id = cursor.lastrowid
    cursor.execute('SELECT * FROM pins WHERE id = ?', (pin_id,))
    row = cursor.fetchone()
    return dict_from_row(row)


def delete_pin(pin_id: int) -> None:
    cursor = conn.cursor()
    cursor.execute('DELETE FROM pins WHERE id = ?', (pin_id,))
    conn.commit()


def clear_pins() -> None:
    cursor = conn.cursor()
    cursor.execute('DELETE FROM pins')
    conn.commit()


def create_message(payload: MessagePayload) -> dict:
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    cursor.execute(
        'INSERT INTO messages (msg, voiceB64, voiceMime, reply, shown, created_at) VALUES (?, ?, ?, ?, 0, ?)',
        (payload.msg or None, payload.voiceB64 or None, payload.voiceMime or None, int(payload.reply), now)
    )
    conn.commit()
    message_id = cursor.lastrowid
    cursor.execute('SELECT * FROM messages WHERE id = ?', (message_id,))
    row = cursor.fetchone()
    return dict_from_row(row)


def get_unread_message() -> Optional[dict]:
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM messages WHERE shown = 0 ORDER BY created_at ASC LIMIT 1')
    row = cursor.fetchone()
    if not row:
        return None
    message = dict_from_row(row)
    cursor.execute('UPDATE messages SET shown = 1 WHERE id = ?', (message['id'],))
    conn.commit()
    return message


def create_reply(payload: ReplyPayload) -> dict:
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    cursor.execute('INSERT INTO replies (msg, created_at) VALUES (?, ?)', (payload.msg, now))
    conn.commit()
    reply_id = cursor.lastrowid
    cursor.execute('SELECT * FROM replies WHERE id = ?', (reply_id,))
    row = cursor.fetchone()
    return dict_from_row(row)


from fastapi import BackgroundTasks

async def broadcast(event: dict, target: Optional[str] = None) -> None:
    targets = [clients[target]] if target else clients.values()
    for group in targets:
        for websocket in list(group):
            try:
                await websocket.send_json(event)
            except Exception:
                try:
                    group.remove(websocket)
                except ValueError:
                    pass


@app.on_event('startup')
def startup():
    init_db()


@app.get('/')
def index():
    try:
        with open(HTML_FILE, 'r', encoding='utf-8') as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail='Frontend not found')


@app.get('/api/pins')
def api_get_pins():
    return get_pins()


@app.post('/api/pins')
async def api_create_pin(payload: PinPayload, background_tasks: BackgroundTasks):
    pin = create_pin(payload)
    background_tasks.add_task(broadcast, {'type': 'pin', 'pin': pin}, target='user')
    return pin


@app.delete('/api/pins')
async def api_clear_pins(background_tasks: BackgroundTasks):
    clear_pins()
    background_tasks.add_task(broadcast, {'type': 'pin', 'pin': None}, target='user')
    return {'status': 'ok'}


@app.delete('/api/pins/{pin_id}')
async def api_delete_pin(pin_id: int, background_tasks: BackgroundTasks):
    delete_pin(pin_id)
    background_tasks.add_task(broadcast, {'type': 'pin', 'pin': {'id': pin_id, 'deleted': True}}, target='user')
    return {'status': 'ok'}


@app.get('/api/location')
def api_get_location():
    loc = get_latest_location()
    return loc or {}


@app.post('/api/location')
async def api_post_location(payload: LocationPayload, background_tasks: BackgroundTasks):
    loc = upsert_location(payload)
    background_tasks.add_task(broadcast, {'type': 'location', 'location': loc}, target='admin')
    return loc


@app.get('/api/messages/unread')
def api_get_unread_message():
    msg = get_unread_message()
    return msg or {}


@app.post('/api/messages')
async def api_post_message(payload: MessagePayload, background_tasks: BackgroundTasks):
    message = create_message(payload)
    background_tasks.add_task(broadcast, {'type': 'message', 'payload': message}, target='user')
    return message


@app.post('/api/reply')
async def api_post_reply(payload: ReplyPayload, background_tasks: BackgroundTasks):
    reply = create_reply(payload)
    background_tasks.add_task(broadcast, {'type': 'reply', 'reply': reply}, target='admin')
    return reply


@app.post('/api/reset')
def api_reset():
    clear_pins()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM messages')
    cursor.execute('DELETE FROM replies')
    cursor.execute('DELETE FROM location')
    conn.commit()
    broadcast({'type': 'reset'}, None)
    return {'status': 'reset'}


@app.websocket('/ws/{client_type}')
async def websocket_endpoint(websocket: WebSocket, client_type: str):
    if client_type not in clients:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    clients[client_type].append(websocket)
    try:
        await websocket.send_json({
            'type': 'state',
            'pins': get_pins(),
            'location': get_latest_location() or {},
            'unread_messages': [get_unread_message()] if client_type == 'user' and get_unread_message() else []
        })
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
            except json.JSONDecodeError:
                continue
            if message.get('type') == 'location' and 'payload' in message:
                payload = message['payload']
                try:
                    loc = LocationPayload(**payload)
                    loc_dict = upsert_location(loc)
                    broadcast({'type': 'location', 'location': loc_dict}, target='admin')
                except Exception:
                    pass
            if message.get('type') == 'reply' and 'payload' in message:
                try:
                    reply = ReplyPayload(**message['payload'])
                    reply_dict = create_reply(reply)
                    broadcast({'type': 'reply', 'reply': reply_dict}, target='admin')
                except Exception:
                    pass
    except WebSocketDisconnect:
        if websocket in clients[client_type]:
            clients[client_type].remove(websocket)
    except Exception:
        if websocket in clients[client_type]:
            clients[client_type].remove(websocket)
