# bot_db.py — работа с базой данных бота
import sqlite3
from datetime import datetime
import json
import time

DB_FILE = "/opt/pvz-bot/bot_database.db"


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        vk_id INTEGER PRIMARY KEY,
        first_seen TEXT,
        yandex_cookies TEXT,
        connected INTEGER DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS processed_events (
        event_id TEXT PRIMARY KEY,
        received_at TEXT
    )''')
    conn.commit()
    conn.close()


def _connect():
    return sqlite3.connect(DB_FILE)


def is_event_processed(event_id):
    if not event_id:
        return False
    with _connect() as conn:
        return conn.execute(
            "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone() is not None


def mark_event_processed(event_id):
    if not event_id:
        return
    with _connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_events (event_id, received_at) VALUES (?, ?)",
            (event_id, datetime.now().isoformat())
        )


def get_or_create_user(vk_id):
    with _connect() as conn:
        if not conn.execute("SELECT 1 FROM users WHERE vk_id = ?", (vk_id,)).fetchone():
            conn.execute(
                "INSERT INTO users (vk_id, first_seen) VALUES (?, ?)",
                (vk_id, datetime.now().isoformat())
            )


def save_cookies(vk_id, cookies):
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET yandex_cookies = ?, connected = 1 WHERE vk_id = ?",
            (json.dumps(cookies), vk_id)
        )
    for ck in cookies:
        if ck.get('name') == 'Session_id':
            exp = ck.get('expiry')
            print(f"[Cookies] Session_id истекает: {datetime.fromtimestamp(exp)}" if exp
                  else "[Cookies] Session_id: expiry не задан")


def get_cookies(vk_id):
    with _connect() as conn:
        row = conn.execute(
            "SELECT yandex_cookies FROM users WHERE vk_id = ?", (vk_id,)
        ).fetchone()
    if not row or not row[0]:
        return None
    cookies = json.loads(row[0])
    now_ts = time.time()
    for ck in cookies:
        if ck.get('name') == 'Session_id':
            exp = ck.get('expiry')
            if exp and exp < now_ts:
                print(f"[Cookies] ⚠️ Session_id истёк {datetime.fromtimestamp(exp)} — нужна переавторизация")
                return None
            print(f"[Cookies] ✅ Session_id действителен до {datetime.fromtimestamp(exp)}" if exp
                  else "[Cookies] ✅ Session_id найден (без expiry)")
    return cookies