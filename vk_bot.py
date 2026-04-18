# vk_bot.py — Flask webhook + инфраструктура бота
import random
import json
import time
import requests
import subprocess
from datetime import datetime
from flask import Flask, request, send_file

from bot_db import init_db, is_event_processed, mark_event_processed, get_or_create_user
from bot_handlers import handle_message
from bot_sessions import active_sessions, active_sessions_lock, CHROME_PROFILE_DIR

app = Flask(__name__)
init_db()

GROUP_ID          = 237702703
CONFIRMATION_TOKEN = "fee275eb"
VK_TOKEN          = "vk1.a.Xr1Qgj3L7lqF4uQaE0nN07783Nj-sQTbdB46NFtizeoQzc1OklbKCTPw-d1maH-7wYFSn4JeU1_QMJjR-UA9u8bz6qAlOSReJYtq-l0Ub_dT4bRYH7nDD6Re8ybDfCU1g5bwEUkJHYMNT3crgyiXjS2FQKkQX8TKJqycOVEPrOpGM2TQLo_IzSOKBtOoIeflCTdrWcWQLWQrGjqumKozJw"
SELENIUM_URL      = "http://127.0.0.1:4444"


# ── VK API ────────────────────────────────────────────────────────────────────

def send_message(peer_id, text, keyboard=None):
    print(f"📩 → {peer_id}: {text[:80]}")
    params = {
        'peer_id': peer_id,
        'message': text,
        'random_id': random.randint(0, 2**63),
        'access_token': VK_TOKEN,
        'v': '5.199',
    }
    if keyboard:
        params['keyboard'] = json.dumps(keyboard)
    result = requests.post("https://api.vk.com/method/messages.send", data=params).json()
    if 'error' in result:
        print(f"❌ VK: {result['error']}")
    return result


# ── Webhook ───────────────────────────────────────────────────────────────────

@app.route('/callback', methods=['POST'])
def callback():
    data = request.json
    print(f"📥 {data.get('type')}")

    if data.get('type') == 'confirmation':
        return CONFIRMATION_TOKEN

    event_id = data.get('event_id')
    if is_event_processed(event_id):
        return 'ok'
    mark_event_processed(event_id)

    if data.get('type') == 'message_new':
        msg = data['object'].get('message', {})
        user_id = msg.get('from_id')
        peer_id = msg.get('peer_id')
        text    = msg.get('text', '').strip()
        print(f"💬 {user_id}: {text}")
        if user_id:
            get_or_create_user(user_id)
            handle_message(peer_id or user_id, text, user_id, send_message)

    return 'ok'


# ── Служебные маршруты ────────────────────────────────────────────────────────

@app.route('/')
def index():
    with active_sessions_lock:
        count = len(active_sessions)
    diag = [f"🤖 PVZ Bot | Сессий: {count}", ""]

    try:
        r = requests.get(f'{SELENIUM_URL}/status', timeout=3)
        diag.append(f"✅ Selenium: {r.json().get('value', {}).get('ready', '?')}")
    except Exception as e:
        diag.append(f"❌ Selenium: {e}")

    try:
        out = subprocess.check_output(['ps', 'aux'], text=True)
        for kw in ['chrome', 'Xvfb', 'x11vnc', 'websockify', 'selenium']:
            found = any(kw in ln for ln in out.splitlines() if 'grep' not in ln)
            diag.append(f"{'✅' if found else '❌'} {kw}")
    except Exception as e:
        diag.append(f"ps error: {e}")

    return "<pre>" + "\n".join(diag) + "</pre>"


@app.route('/novnc/<user_id>')
def novnc_redirect(user_id):
    return ('<!DOCTYPE html><html><head>'
            '<meta http-equiv="refresh" content="0;url=/novnc/?autoconnect=1&resize=scale">'
            '</head><body><a href="/novnc/?autoconnect=1&resize=scale">Открыть</a></body></html>')


@app.route('/screenshot')
def screenshot():
    import os
    path = '/opt/pvz-bot/report_page.png'
    if not os.path.exists(path):
        return 'Скриншот не найден', 404
    mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')
    return send_file(path, mimetype='image/png',
                     headers={'X-Screenshot-Time': mtime, 'Cache-Control': 'no-cache'})


# ── Bootstrap ─────────────────────────────────────────────────────────────────

def bootstrap():
    import os
    print("\n🔧 Bootstrap...")

    # Освобождаем порт 5000
    try:
        out = subprocess.check_output(['fuser', '5000/tcp'], text=True, stderr=subprocess.DEVNULL).strip()
        if out:
            subprocess.run(['fuser', '-k', '5000/tcp'], stderr=subprocess.DEVNULL)
            time.sleep(1)
            print("  ✅ Порт 5000 освобождён")
    except Exception:
        pass

    # Проверяем Docker
    try:
        subprocess.check_output(['docker', 'info'], stderr=subprocess.DEVNULL)
    except Exception as e:
        print(f"  ❌ Docker недоступен: {e}")
        return

    # Проверяем/запускаем Selenium
    selenium_ok = False
    try:
        r = requests.get(f'{SELENIUM_URL}/status', timeout=3)
        if r.json().get('value', {}).get('ready'):
            selenium_ok = True
            print("  ✅ Selenium готов")
    except Exception:
        pass

    if not selenium_ok:
        print("  🐳 Запускаю Selenium...")
        try:
            subprocess.run(['docker', 'rm', '-f', 'selenium-chrome'],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            os.makedirs(CHROME_PROFILE_DIR, exist_ok=True)
            subprocess.run([
                'docker', 'run', '-d',
                '-p', '4444:4444', '-p', '7900:7900',
                '--shm-size=2g', '--name', 'selenium-chrome',
                '--restart', 'unless-stopped',
                '-e', 'SE_VNC_NO_PASSWORD=1',
                '-v', f'{CHROME_PROFILE_DIR}:/home/seluser/chrome_profile',
                'selenium/standalone-chrome:latest'
            ], check=True, stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            print(f"  ❌ Не удалось запустить контейнер: {e}")
            return

        for i in range(30):
            time.sleep(1)
            try:
                if requests.get(f'{SELENIUM_URL}/status', timeout=2).json().get('value', {}).get('ready'):
                    print(f"  ✅ Selenium готов ({i+1} сек)")
                    selenium_ok = True
                    break
            except Exception:
                pass
        if not selenium_ok:
            print("  ❌ Selenium не поднялся за 30 сек")

    try:
        requests.get('http://127.0.0.1:7900', timeout=2)
        print("  ✅ noVNC доступен")
    except Exception:
        print("  ⚠️  noVNC не отвечает")

    print("✅ Bootstrap завершён\n")


if __name__ == '__main__':
    print("🚀 Запуск бота...\n🔗 https://pvz-bot.sytes.net")
    bootstrap()
    app.run(host='127.0.0.1', port=5000, debug=False)