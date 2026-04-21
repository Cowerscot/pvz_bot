#!/usr/bin/env python3
# main.py — Точка входа: запускает бота

import sys
import os

# Добавляем путь к модулям
sys.path.insert(0, '/opt/pvz-bot')

print("🚀 Запуск бота...")

# Инициализация БД
from database import init_db
init_db()
print("  ✅ База данных готова")

# Импорт Flask и запуск вебхука
from flask import Flask, request
import requests
import subprocess
import random
import json
import time
import logging
from datetime import datetime

from config import (
    VK_ACCESS_TOKEN, VK_CONFIRMATION_TOKEN, VK_GROUP_ID,
    SELENIUM_URL_EXT, BOT_HOST, NOVNC_URL, MARKETPLACES
)
from vk_bot import handle_message, kb_start, kb_marketplace
from browser_manager import active_sessions, active_sessions_lock


# =============================================================================
# FLASK ПРИЛОЖЕНИЕ
# =============================================================================

app = Flask(__name__)

GROUP_ID = VK_GROUP_ID
CONFIRMATION_TOKEN = VK_CONFIRMATION_TOKEN


def send_message(peer_id, text, keyboard=None):
    """Отправка сообщения через VK API"""
    print(f"📩 → {peer_id}: {text[:80]}")
    params = {
        'peer_id': peer_id,
        'message': text,
        'random_id': random.randint(0, 2**63),
        'access_token': VK_ACCESS_TOKEN,
        'v': '5.199',
    }
    if keyboard:
        params['keyboard'] = json.dumps(keyboard)
    
    result = requests.post("https://api.vk.com/method/messages.send", data=params).json()
    if 'error' in result:
        print(f"❌ VK: {result['error']}")
    return result


@app.route('/callback', methods=['POST'])
def callback():
    """Вебхук для VK Callback API"""
    data = request.json
    print(f"📥 {data.get('type')}")

    if data.get('type') == 'confirmation':
        return CONFIRMATION_TOKEN

    event_id = data.get('event_id')
    from database import is_event_processed, mark_event_processed
    
    if is_event_processed(event_id):
        return 'ok'
    mark_event_processed(event_id)

    if data.get('type') == 'message_new':
        msg = data['object'].get('message', {})
        user_id = msg.get('from_id')
        peer_id = msg.get('peer_id')
        text = msg.get('text', '').strip()
        print(f"💬 {user_id}: {text}")
        
        if user_id:
            from database import get_or_create_user
            get_or_create_user(user_id)
            handle_message(peer_id or user_id, text, user_id, send_message)

    return 'ok'


@app.route('/')
def index():
    """Диагностическая страница"""
    with active_sessions_lock:
        count = len(active_sessions)
    diag = [f"🤖 PVZ Bot | Сессий: {count}", ""]

    try:
        r = requests.get(f'{SELENIUM_URL_EXT}/status', timeout=3)
        diag.append(f"✅ Selenium: {r.json().get('value', {}).get('ready', '?')}")
    except Exception as e:
        diag.append(f"❌ Selenium: {e}")

    try:
        out = subprocess.check_output(['ps', 'aux'], text=True)
        for kw in ['chrome', 'Xvfb', 'x11vnc', 'websockify', 'selenium']:
            found = any(kw in ln for ln in out.splitlines() if 'grep' not in ln)
            diag.append(f"{'✅' if found else '❌'} {kw}")
    except Exception as e:
        diag.append(f"⚠️ ps: {e}")

    return "<pre>\n".join(diag) + "\n</pre>"


# =============================================================================
# ЗАПУСК
# =============================================================================

if __name__ == '__main__':
    # Проверка доступности Selenium
    print("\n🔧 Bootstrap...")
    try:
        r = requests.get(f'{SELENIUM_URL_EXT}/status', timeout=5)
        print("  ✅ Selenium готов")
    except Exception as e:
        print(f"  ❌ Selenium: {e}")
    
    # Проверка noVNC
    try:
        r = requests.get(f'{BOT_HOST}/novnc/', timeout=3)
        print("  ✅ noVNC доступен")
    except Exception:
        print("  ⚠️ noVNC: недоступен")
    
    print("✅ Bootstrap завершён\n")
    
    print(f"🔗 {BOT_HOST}")
    
    # Запускаем Flask
    app.run(host='127.0.0.1', port=5000, debug=False)
