# bot_engine.py — Движок бота: сессии, обработчики, VK-бот

import random
import json
import time
import threading
import logging
from datetime import datetime
from flask import Flask, request, send_file
import requests
import subprocess
import os

from config import (
    YANDEX_AUTH_URL, CHROME_PROFILE_DIR,
    send_vk_message, get_vk_message_id, save_vk_message_id, VK_ACCESS_TOKEN, VK_PEER_ID
)


# =============================================================================
# БАЗА ДАННЫХ
# =============================================================================

DB_FILE = "/opt/pvz-bot/bot_database.db"


def init_db():
    """Инициализация БД"""
    import sqlite3
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
    import sqlite3
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


# =============================================================================
# СЕССИИ И БРАУЗЕР
# =============================================================================

SELENIUM_URL = "http://127.0.0.1:4444/wd/hub"
NOVNC_URL = "https://pvz-bot.sytes.net/novnc/?autoconnect=1&resize=scale"

active_sessions: dict = {}
active_sessions_lock = threading.Lock()

_shared_driver = None
_shared_driver_lock = threading.Lock()


def _make_options():
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_argument('--disable-gpu')
    opts.add_experimental_option('excludeSwitches', ['enable-automation'])
    opts.add_experimental_option('useAutomationExtension', False)
    return opts


def get_shared_driver():
    global _shared_driver
    with _shared_driver_lock:
        if _shared_driver:
            try:
                _ = _shared_driver.current_url
                return _shared_driver
            except Exception:
                print("[Driver] Старый браузер мёртв, создаю новый")
                try:
                    _shared_driver.quit()
                except Exception:
                    pass
                _shared_driver = None
        last_err = None
        for attempt in range(3):
            try:
                print(f"[Driver] Создаю браузер (попытка {attempt+1})...")
                _shared_driver = webdriver.Remote(
                    command_executor=SELENIUM_URL,
                    options=_make_options()
                )
                print(f"[Driver] Готов: {_shared_driver.session_id[:8]}")
                return _shared_driver
            except Exception as e:
                last_err = e
                print(f"[Driver] Ошибка попытки {attempt+1}: {e}")
                time.sleep(3)
        raise RuntimeError(f"Не удалось создать сессию браузера: {last_err}")


def release_shared_driver():
    global _shared_driver
    with _shared_driver_lock:
        if _shared_driver:
            try:
                _shared_driver.quit()
            except Exception:
                pass
            _shared_driver = None
            print("[Driver] Браузер закрыт")


def close_session(user_id):
    with active_sessions_lock:
        active_sessions.pop(user_id, None)


def start_auth_session(user_id, peer_id, send_fn, cancel_kb, marketplace_kb, stats_kb):
    """Запуск авторизации через браузер (в отдельном потоке)"""
    try:
        try:
            driver = get_shared_driver()
        except Exception as e:
            send_fn(peer_id, f"❌ Браузер недоступен: {str(e)[:200]}")
            return

        with active_sessions_lock:
            active_sessions[user_id] = driver

        driver.get(YANDEX_AUTH_URL)
        time.sleep(3)
        print(f"[Auth] Открыта страница: {driver.current_url}")

        send_fn(peer_id,
            f"🔐 Авторизация в Яндекс.ПВЗ:\n\n"
            f"1. Открой ссылку:\n{NOVNC_URL}\n\n"
            f"2. Войди через аккаунт / СМС\n"
            f"3. Бот определит вход автоматически ✅\n\n"
            f"⏱ Ссылка активна 5 минут.",
            cancel_kb()
        )

        elapsed = 0
        while elapsed < 300:
            time.sleep(5)
            elapsed += 5

            with active_sessions_lock:
                if user_id not in active_sessions:
                    print(f"[Auth] {user_id} — сессия отменена")
                    return

            try:
                cur = driver.current_url
            except Exception as e:
                print(f"[Auth] Ошибка current_url: {e}")
                release_shared_driver()
                break

            if 'passport.yandex.ru' not in cur and 'auth' not in cur.lower():
                real_cookies = driver.get_cookies()
                save_cookies(user_id, real_cookies if real_cookies else [{'name': 'authorized', 'value': '1', 'domain': 'yandex.ru'}])
                send_fn(peer_id,
                    "✅ Авторизация успешна! Профиль сохранён.\n"
                    "🚀 Можно запрашивать статистику.",
                    stats_kb()
                )
                close_session(user_id)
                return

        send_fn(peer_id,
            "⏰ Время авторизации истекло (5 мин).\n"
            "Нажми 🟡 Яндекс ПВЗ чтобы попробовать снова.",
            marketplace_kb()
        )

    except Exception as e:
        send_fn(peer_id, f"❌ Ошибка браузера: {str(e)[:300]}")
        print(f"[Auth] Exception {user_id}: {e}")
    finally:
        close_session(user_id)


# =============================================================================
# ОБРАБОТЧИКИ КОМАНД
# =============================================================================


def _kb(*rows):
    """Хелпер для создания клавиатуры"""
    return {"buttons": [[{"action": {"type": "text", "label": lbl}}] for lbl in rows],
            "one_time": False, "inline": False}


def kb_policy():      return _kb("✅ Принимаю политику")
def kb_marketplace(): return _kb("🟡 Яндекс ПВЗ")
def kb_cancel():      return _kb("❌ Отменить авторизацию")
def kb_stats():       return _kb("📊 Статистика Яндекс", "🔄 Переподключить Яндекс")


def run_yandex_stats(user_id, peer_id, send_fn):
    """Парсинг Яндекс.ПВЗ и отправка результата"""

    class VkHandler(logging.Handler):
        def __init__(self):
            super().__init__()
            self.buf = []
            self.last_send = time.time()

        def emit(self, record):
            self.buf.append(record.getMessage())
            if len(self.buf) >= 5 or time.time() - self.last_send > 10:
                self.flush_buf()

        def flush_buf(self):
            if self.buf:
                send_fn(peer_id, "📋 Лог:\n" + "\n".join(self.buf))
                self.buf.clear()
                self.last_send = time.time()

    vk_h = VkHandler()
    logger = logging.getLogger(f'yandex_{user_id}')
    logger.setLevel(logging.DEBUG)
    logger.handlers = [logging.StreamHandler(), vk_h]
    logger.propagate = False

    try:
        cookies = get_cookies(user_id)
        if not cookies:
            send_fn(peer_id, "❌ Сессия не найдена. Авторизуйся заново.", kb_marketplace())
            return

        driver = get_shared_driver()
        logger.info(f"✅ Браузер готов: {driver.session_id[:8]}...")

        # Загружаем куки в браузер
        try:
            driver.get("https://yandex.ru")
            time.sleep(1)
            driver.delete_all_cookies()
            for ck in cookies:
                try:
                    c = {'name': ck['name'], 'value': ck['value'], 'domain': ck.get('domain', '.yandex.ru')}
                    if ck.get('path'):
                        c['path'] = ck['path']
                    driver.add_cookie(c)
                except Exception:
                    pass
            logger.info("✅ Куки загружены в браузер")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка загрузки куки: {e}")

        from yandex_core import process_yandex_report
        report_data = process_yandex_report(driver, logger)

        if not report_data or not report_data.get('pvz_data'):
            vk_h.flush_buf()
            send_fn(peer_id, "⚠️ Нет данных в отчёте.", kb_stats())
            return

        now = datetime.now()
        months = ['янв','фев','мар','апр','мая','июн','июл','авг','сен','окт','ноя','дек']
        lines = [f"📊 Яндекс.ПВЗ | {now.day} {months[now.month-1]}", "─────────────"]
        total_day = total_avg = total_forecast = 0

        for pvz_id, pvz in sorted(report_data['pvz_data'].items()):
            pid = str(int(float(pvz_id)))
            lines += [
                f"ID_{pid}:",
                f"  Вчера: {pvz['last_amount']:,} ₽",
                f"  Среднее: {pvz['avg_daily']:,} ₽/день",
                f"  Прогноз: {pvz['forecast']:,} ₽",
            ]
            total_day += pvz['last_amount']
            total_avg += pvz['avg_daily']
            total_forecast += pvz['forecast']

        lines += [
            "─────────────",
            f"Итого вчера: {total_day:,} ₽",
            f"Итого среднее: {total_avg:,} ₽/день",
            f"Итого прогноз: {total_forecast:,} ₽",
        ]
        vk_h.flush_buf()
        send_fn(peer_id, "\n".join(lines), kb_stats())

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[Stats] ОШИБКА {user_id}: {e}\n{tb}")
        vk_h.flush_buf()
        if "устарели" in str(e) or "Нет cookies" in str(e):
            send_fn(peer_id, f"🔑 {e}\nНажми 🔄 Переподключить Яндекс.", kb_stats())
        else:
            send_fn(peer_id, f"❌ Ошибка: {str(e)[:150]}\n\n{tb[-350:]}", kb_stats())


def handle_message(peer_id, text, user_id, send_fn):
    t = text.lower().strip()

    if t in ('/start', 'начать'):
        send_fn(peer_id,
            "👋 Привет! Я бот для отслеживания статистики ПВЗ.\n\n"
            "Прими Политику конфиденциальности для начала работы.",
            kb_policy()
        )

    elif t == '✅ принимаю политику':
        send_fn(peer_id, "Выбери маркетплейс:", kb_marketplace())

    elif t in ('🟡 яндекс пвз', '🔄 переподключить яндекс'):
        with active_sessions_lock:
            already = user_id in active_sessions
        if already:
            send_fn(peer_id, f"⚠️ Сессия уже открыта.\nПерейди: {NOVNC_URL}", kb_cancel())
            return
        threading.Thread(
            target=start_auth_session,
            args=(user_id, peer_id, send_fn, kb_cancel, kb_marketplace, kb_stats),
            daemon=True
        ).start()

    elif t == '❌ отменить авторизацию':
        close_session(user_id)
        send_fn(peer_id, "❌ Авторизация отменена.", kb_marketplace())

    elif t == '📊 статистика яндекс':
        if not get_cookies(user_id):
            send_fn(peer_id, "❌ Нет сохранённой сессии. Сначала авторизуйся.", kb_marketplace())
            return
        send_fn(peer_id, "⏳ Запускаю парсинг Яндекс.ПВЗ...")
        threading.Thread(target=run_yandex_stats, args=(user_id, peer_id, send_fn), daemon=True).start()

    else:
        send_fn(peer_id, "Нажми /start для начала работы.")


# =============================================================================
# FLASK ВЕБХОК
# =============================================================================

app = Flask(__name__)
init_db()

GROUP_ID          = 237702703
CONFIRMATION_TOKEN = "fee275eb"
VK_TOKEN          = "vk1.a.Xr1Qgj3L7lqF4uQaE0nN07783Nj-sQTbdB46NFtizeoQzc1OklbKCTPw-d1maH-7wYFSn4JeU1_QMJjR-UA9u8bz6qAlOSREJYtq-l0Ub_dT4bRYH7nDD6Re8ybDfCU1g5bwEUkJHYMNT3crgyiXjS2FQKkQX8TKJqycOVEPrOpGM2TQLo_IzSOKBtOoIeflCTdrWcWQLWQrGjqumKozJw"
SELENIUM_URL_EXT  = "http://127.0.0.1:4444"


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


@app.route('/')
def index():
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
        diag.append(f"ps error: {e}")

    return "<pre>" + "\n".join(diag) + "</pre>"


@app.route('/novnc/<user_id>')
def novnc_redirect(user_id):
    return ('<!DOCTYPE html><html><head>'
            '<meta http-equiv="refresh" content="0;url=/novnc/?autoconnect=1&resize=scale">'
            '</head><body><a href="/novnc/?autoconnect=1&resize=scale">Открыть</a></body></html>')


@app.route('/screenshot')
def screenshot():
    path = '/opt/pvz-bot/report_page.png'
    if not os.path.exists(path):
        return 'Скриншот не найден', 404
    mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M:%S')
    return send_file(path, mimetype='image/png',
                     headers={'X-Screenshot-Time': mtime, 'Cache-Control': 'no-cache'})


def bootstrap():
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
        r = requests.get(f'{SELENIUM_URL_EXT}/status', timeout=3)
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
                if requests.get(f'{SELENIUM_URL_EXT}/status', timeout=2).json().get('value', {}).get('ready'):
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
