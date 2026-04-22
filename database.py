# database.py — База данных пользователей и сессий

import sqlite3
import json
import time
from datetime import datetime


DB_FILE = "/opt/pvz-bot/bot_database.db"


def init_db():
    """Инициализация базы данных"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Таблица пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        vk_id INTEGER PRIMARY KEY,
        first_seen TEXT,
        yandex_cookies TEXT,
        ozon_cookies TEXT,
        avito_cookies TEXT,
        connected INTEGER DEFAULT 0
    )''')
    
    # Таблица обработанных событий (для идемпотентности)
    c.execute('''CREATE TABLE IF NOT EXISTS processed_events (
        event_id TEXT PRIMARY KEY,
        received_at TEXT
    )''')
    
    conn.commit()
    conn.close()


def _connect():
    """Подключение к БД"""
    return sqlite3.connect(DB_FILE)


def is_event_processed(event_id):
    """Проверка обработки события"""
    if not event_id:
        return False
    with _connect() as conn:
        return conn.execute(
            "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone() is not None


def mark_event_processed(event_id):
    """Отметка события как обработанного"""
    if not event_id:
        return
    with _connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_events (event_id, received_at) VALUES (?, ?)",
            (event_id, datetime.now().isoformat())
        )


def get_or_create_user(vk_id):
    """Получение или создание пользователя"""
    with _connect() as conn:
        if not conn.execute("SELECT 1 FROM users WHERE vk_id = ?", (vk_id,)).fetchone():
            conn.execute(
                "INSERT INTO users (vk_id, first_seen) VALUES (?, ?)",
                (vk_id, datetime.now().isoformat())
            )


def save_cookies(vk_id, cookies, marketplace='yandex'):
    """
    Сохранение cookies пользователя
    
    Args:
        vk_id: ID пользователя
        cookies: Список cookies
        marketplace: Название маркетплейса ('yandex', 'ozon', 'avito')
    """
    column_map = {
        'yandex': 'yandex_cookies',
        'ozon': 'ozon_cookies',
        'avito': 'avito_cookies'
    }
    
    column = column_map.get(marketplace, 'yandex_cookies')
    
    with _connect() as conn:
        conn.execute(
            f"UPDATE users SET {column} = ?, connected = 1 WHERE vk_id = ?",
            (json.dumps(cookies), vk_id)
        )
    
    # Логирование Session_id
    for ck in cookies:
        if ck.get('name') == 'Session_id':
            exp = ck.get('expiry')
            if exp:
                print(f"[Cookies] Session_id ({marketplace}) истекает: {datetime.fromtimestamp(exp)}")
            else:
                print(f"[Cookies] Session_id ({marketplace}): expiry не задан")


def get_cookies(vk_id, marketplace='yandex'):
    """
    Получение cookies пользователя с проверкой срока действия
    
    Args:
        vk_id: ID пользователя
        marketplace: Название маркетплейса ('yandex', 'ozon', 'avito')
    
    Returns:
        list: Список cookies или None если нет/истекли
    """
    column_map = {
        'yandex': 'yandex_cookies',
        'ozon': 'ozon_cookies',
        'avito': 'avito_cookies'
    }
    
    column = column_map.get(marketplace, 'yandex_cookies')
    
    with _connect() as conn:
        row = conn.execute(
            f"SELECT {column} FROM users WHERE vk_id = ?", (vk_id,)
        ).fetchone()
    
    if not row or not row[0]:
        return None
    
    cookies = json.loads(row[0])
    now_ts = time.time()
    
    # Проверка срока действия Session_id
    for ck in cookies:
        if ck.get('name') == 'Session_id':
            exp = ck.get('expiry')
            if exp and exp < now_ts:
                print(f"[Cookies] ⚠️ Session_id ({marketplace}) истёк {datetime.fromtimestamp(exp)} — нужна переавторизация")
                return None
            if exp:
                print(f"[Cookies] ✅ Session_id ({marketplace}) действителен до {datetime.fromtimestamp(exp)}")
            else:
                print(f"[Cookies] ✅ Session_id ({marketplace}) найден (без expiry)")
    
    return cookies


def clear_user_cookies(vk_id, marketplace=None):
    """
    Очистка cookies пользователя
    
    Args:
        vk_id: ID пользователя
        marketplace: Если указан, очищает только этот маркетплейс, иначе все
    """
    with _connect() as conn:
        if marketplace:
            column_map = {
                'yandex': 'yandex_cookies',
                'ozon': 'ozon_cookies',
                'avito': 'avito_cookies'
            }
            column = column_map.get(marketplace)
            if column:
                conn.execute(
                    f"UPDATE users SET {column} = NULL WHERE vk_id = ?",
                    (vk_id,)
                )
        else:
            # Очищаем все cookies
            conn.execute(
                "UPDATE users SET yandex_cookies = NULL, ozon_cookies = NULL, avito_cookies = NULL WHERE vk_id = ?",
                (vk_id,)
            )
    
    print(f"[Database] Очищены cookies пользователя {vk_id}" + (f" ({marketplace})" if marketplace else " (все)"))


def is_user_connected(vk_id):
    """Проверка подключения пользователя"""
    with _connect() as conn:
        row = conn.execute(
            "SELECT connected FROM users WHERE vk_id = ?", (vk_id,)
        ).fetchone()
    return row and row[0] == 1


def get_user_marketplaces(vk_id):
    """
    Получение списка подключенных маркетплейсов пользователя
    
    Returns:
        dict: {'yandex': bool, 'ozon': bool, 'avito': bool}
    """
    with _connect() as conn:
        row = conn.execute(
            "SELECT yandex_cookies, ozon_cookies, avito_cookies FROM users WHERE vk_id = ?",
            (vk_id,)
        ).fetchone()
    
    if not row:
        return {'yandex': False, 'ozon': False, 'avito': False}
    
    return {
        'yandex': row[0] is not None,
        'ozon': row[1] is not None,
        'avito': row[2] is not None
    }