# config.py — Конфигурация, БД, утилиты, браузер

import os
import sys
import json
import time
import shutil
import logging
import sqlite3
import requests
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


# =============================================================================
# === ПУТИ И НАСТРОЙКИ =========================================================
# =============================================================================

BASE_DIR = Path("/opt/pvz-bot")
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "Отчеты"

# Все служебные файлы в одной папке
CHROME_PROFILE_DIR = DATA_DIR / "chrome_profile"
LOGS_DIR = DATA_DIR / "logs"
MESSAGE_IDS_FILE = DATA_DIR / "message_ids.json"
VK_MESSAGE_IDS_FILE = DATA_DIR / "vk_message_ids.json"
DB_FILE = DATA_DIR / "bot_database.db"

# Selenium
SELENIUM_URL = "http://127.0.0.1:4444/wd/hub"
NOVNC_URL = "https://pvz-bot.sytes.net/novnc/?autoconnect=1&resize=scale"


def load_settings():
    """Загрузка настроек из JSON файла"""
    settings_file = DATA_DIR / "settings.json"
    try:
        with open(settings_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        default = {
            "file_logging_enabled": True,
            "vk": {"access_token": "", "peer_id": 0},
            "expenses": {
                "аренда": 110000,
                "комуналка": 10000,
                "зарплата_день": 5000,
                "прочее": 5000,
                "налог_процент": 6
            },
            "telegram": {
                "bot_token": "",
                "chat_id": "",
                "topic_id": 0,
                "proxy": ""
            },
            "urls": {
                "yandex_auth": "https://passport.yandex.ru/auth/list?retpath=https%3A%2F%2Flogistics.market.yandex.ru%2F...",
                "yandex_report": "https://hubs.market.yandex.ru/tpl-partner/148761735/month-reports?month=11&year=2025",
                "ozon_base": "https://turbo-pvz.ozon.ru",
                "ozon_reports": "https://turbo-pvz.ozon.ru/reports/subagent",
                "avito": "https://pvz.avito.ru"
            }
        }
        DATA_DIR.mkdir(exist_ok=True)
        with open(settings_file, 'w', encoding='utf-8') as f:
            json.dump(default, f, ensure_ascii=False, indent=2)
        return default


SETTINGS = load_settings()
FILE_LOGGING_ENABLED = SETTINGS.get("file_logging_enabled", SETTINGS.get("logging_enabled", True))
EXPENSES = SETTINGS.get("expenses", {})

# Telegram
tg_config = SETTINGS.get("telegram", {})
TELEGRAM_BOT_TOKEN = tg_config.get("bot_token", "")
TELEGRAM_CHAT_ID = tg_config.get("chat_id", "")
TELEGRAM_TOPIC_ID = tg_config.get("topic_id", 0)
TELEGRAM_PROXY = tg_config.get("proxy", "")

# URLs
urls_config = SETTINGS.get("urls", {})
YANDEX_AUTH_URL = urls_config.get("yandex_auth", "")
YANDEX_REPORT_URL = urls_config.get("yandex_report", "")
OZON_BASE_URL = urls_config.get("ozon_base", "")
OZON_REPORTS_URL = urls_config.get("ozon_reports", "")
AVITO_URL = urls_config.get("avito", "")

# VK
vk_config = SETTINGS.get("vk", {})
VK_ACCESS_TOKEN = vk_config.get("access_token", "")
VK_PEER_ID = vk_config.get("peer_id", 0)


# =============================================================================
# === БАЗА ДАННЫХ ==============================================================
# =============================================================================

def init_db():
    """Инициализация базы данных"""
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        vk_id INTEGER PRIMARY KEY,
        first_seen TEXT,
        yandex_cookies TEXT,
        ozon_cookies TEXT,
        avito_cookies TEXT,
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


def save_cookies(vk_id, marketplace, cookies):
    """Сохранение cookies пользователя для конкретного маркетплейса
    
    Args:
        vk_id: ID пользователя ВКонтакте
        marketplace: название маркетплейса ('yandex', 'ozon', 'avito')
        cookies: список cookie в формате Selenium
    """
    column_map = {
        'yandex': 'yandex_cookies',
        'ozon': 'ozon_cookies',
        'avito': 'avito_cookies'
    }
    
    if marketplace not in column_map:
        raise ValueError(f"Неизвестный маркетплейс: {marketplace}")
    
    column = column_map[marketplace]
    
    with _connect() as conn:
        conn.execute(
            f"UPDATE users SET {column} = ?, connected = 1 WHERE vk_id = ?",
            (json.dumps(cookies), vk_id)
        )
    
    # Логирование для отладки
    for ck in cookies:
        if ck.get('name') == 'Session_id':
            exp = ck.get('expiry')
            print(f"[Cookies] {marketplace.upper()} Session_id истекает: {datetime.fromtimestamp(exp)}" if exp
                  else f"[Cookies] {marketplace.upper()} Session_id: expiry не задан")


def get_cookies(vk_id, marketplace):
    """Получение cookies пользователя с проверкой срока действия
    
    Args:
        vk_id: ID пользователя ВКонтакте
        marketplace: название маркетплейса ('yandex', 'ozon', 'avito')
    
    Returns:
        list: список cookie или None если не найдены/истекли
    """
    column_map = {
        'yandex': 'yandex_cookies',
        'ozon': 'ozon_cookies',
        'avito': 'avito_cookies'
    }
    
    if marketplace not in column_map:
        raise ValueError(f"Неизвестный маркетплейс: {marketplace}")
    
    column = column_map[marketplace]
    
    with _connect() as conn:
        row = conn.execute(
            f"SELECT {column} FROM users WHERE vk_id = ?", (vk_id,)
        ).fetchone()
    
    if not row or not row[0]:
        return None
    
    cookies = json.loads(row[0])
    now_ts = time.time()
    
    # Проверяем срок действия сессии
    for ck in cookies:
        if ck.get('name') == 'Session_id':
            exp = ck.get('expiry')
            if exp and exp < now_ts:
                print(f"[Cookies] ⚠️ {marketplace.upper()} Session_id истёк {datetime.fromtimestamp(exp)} — нужна переавторизация")
                return None
            print(f"[Cookies] ✅ {marketplace.upper()} Session_id действителен до {datetime.fromtimestamp(exp)}" if exp
                  else f"[Cookies] ✅ {marketplace.upper()} Session_id найден (без expiry)")
    
    return cookies


# =============================================================================
# === ЛОГИРОВАНИЕ ==============================================================
# =============================================================================

def setup_logger(script_name="combined"):
    """Настройка логирования"""
    handlers = [logging.StreamHandler()]
    
    if FILE_LOGGING_ENABLED:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = LOGS_DIR / f"{script_name}_{ts}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", "%H:%M:%S"))
        handlers.append(file_handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=handlers
    )
    return logging.getLogger(__name__)


def save_snapshot(driver, logger, name):
    """Сохранение скриншота"""
    if not FILE_LOGGING_ENABLED:
        return
    try:
        LOGS_DIR.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        img_path = LOGS_DIR / f"{name}_{ts}.png"
        driver.save_screenshot(str(img_path))
        if FILE_LOGGING_ENABLED:
            logger.info(f"📸 {img_path.name}")
    except Exception as e:
        logger.error(f"Ошибка скриншота: {e}")


# =============================================================================
# === БРАУЗЕР ==================================================================
# =============================================================================

def create_driver(download_dir=None):
    """Создание локального Chrome WebDriver (для full_main.py)"""
    from selenium.webdriver.chrome.options import Options
    
    DATA_DIR.mkdir(exist_ok=True)
    CHROME_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)
    (REPORTS_DIR / "Яндекс").mkdir(exist_ok=True)
    (REPORTS_DIR / "Озон").mkdir(exist_ok=True)
    
    options = Options()
    
    if download_dir is None:
        download_dir = REPORTS_DIR
    
    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values.notifications": 2,
        "profile.exit_type": "Normal",
        "profile.exited_cleanly": True
    }
    options.add_experimental_option("prefs", prefs)
    
    # Общий профиль Chrome
    options.add_argument(f"--user-data-dir={CHROME_PROFILE_DIR}")
    options.add_argument("--profile-directory=Default")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-session-crashed-bubble")
    options.add_argument("--disable-infobars")
    
    os.environ['WDM_LOG'] = '0'
    os.environ['WDM_LOG_LEVEL'] = '0'
    
    for attempt in range(3):
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            return driver
        except Exception as e:
            print(f"❌ Ошибка запуска браузера (попытка {attempt + 1}): {e}")
            time.sleep(3)
    
    raise RuntimeError("Не удалось запустить Chrome")


def cleanup_chrome_profile():
    """Очистка кеша Chrome с сохранением данных авторизации"""
    if not CHROME_PROFILE_DIR.exists():
        return
    
    to_delete = [
        'Cache', 'Code Cache', 'GPUCache', 'Service Worker',
        'Default/Cache', 'Default/Code Cache', 'Default/GPUCache',
        'Default/Service Worker', 'ShaderCache', 'GraphiteDawnCache',
        'GrShaderCache', 'Default/Storage/ext', 'Default/Sync Extension Settings',
        'BrowserMetrics', 'CrashpadMetrics-active.pma',
        'optimization_guide_model_store', 'Default/optimization_guide_hint_cache_store',
        'Default/optimization_guide_model_metadata_store'
    ]
    
    network_path = CHROME_PROFILE_DIR / 'Default' / 'Network'
    deleted_size = 0
    
    for item in to_delete:
        path = CHROME_PROFILE_DIR / item
        if path.exists():
            try:
                if path.is_file():
                    size = path.stat().st_size
                    path.unlink()
                    deleted_size += size
                else:
                    size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                    shutil.rmtree(path)
                    deleted_size += size
            except Exception:
                pass
    
    if network_path.exists():
        for item in network_path.iterdir():
            if item.name not in ['Cookies', 'Cookies-journal']:
                try:
                    if item.is_file():
                        deleted_size += item.stat().st_size
                        item.unlink()
                    else:
                        size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
                        shutil.rmtree(item)
                        deleted_size += size
                except Exception:
                    pass
    
    if deleted_size > 0:
        mb = deleted_size / (1024 * 1024)
        print(f"🧹 Очищено: {mb:.1f} МБ")
    
    return deleted_size


# =============================================================================
# === TELEGRAM =================================================================
# =============================================================================

def send_or_update_telegram_message(text: str, logger, message_id=None):
    """Отправка или обновление сообщения в Telegram через SOCKS5 прокси"""
    try:
        session = requests.Session()
        session.proxies = {
            'http': TELEGRAM_PROXY,
            'https': TELEGRAM_PROXY
        } if TELEGRAM_PROXY else {}
        
        if message_id:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML"
            }
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "message_thread_id": TELEGRAM_TOPIC_ID
            }
        
        response = session.post(url, data=payload, timeout=30)
        result = response.json()
        session.close()
        
        if response.status_code == 200:
            if message_id:
                return True
            else:
                return result.get("result", {}).get("message_id")
        else:
            desc = result.get("description", "")
            if message_id and "message to edit not found" in desc:
                logger.warning(f"TG сообщение {message_id} не найдено, будет создано новое")
                return None
            if message_id and "message is not modified" in desc:
                logger.info("  ✅ TG сообщение не изменилось (контент тот же)")
                return True
            logger.error(f"Ошибка Telegram API: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
        return False


def get_message_id(date_str):
    """Получить ID сообщения для даты"""
    if not MESSAGE_IDS_FILE.exists():
        return None
    try:
        with open(MESSAGE_IDS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get(date_str)
    except:
        return None


def save_message_id(date_str, message_id):
    """Сохранить ID сообщения"""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = {}
        if MESSAGE_IDS_FILE.exists():
            with open(MESSAGE_IDS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        data[date_str] = message_id
        with open(MESSAGE_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения message_id: {e}")


# =============================================================================
# === ВКОНТАКТЕ =================================================================
# =============================================================================

def _to_unicode_bold(text: str) -> str:
    """Конвертирует текст в Unicode Bold символы"""
    latin_normal = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    latin_bold   = '𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵'
    latin_bold_list = list(latin_bold)
    result = []
    for ch in text:
        if ch in latin_normal:
            idx = latin_normal.index(ch)
            result.append(latin_bold_list[idx])
        else:
            result.append(ch)
    return ''.join(result)


def send_vk_message(peer_id, text, keyboard=None, access_token=None):
    """Отправка сообщения ВКонтакте
    
    Args:
        peer_id: ID получателя
        text: текст сообщения
        keyboard: клавиатура (dict)
        access_token: токен VK (если None, используется из конфига)
    
    Returns:
        dict: результат от VK API
    """
    import random
    token = access_token or VK_ACCESS_TOKEN
    
    print(f"📩 → {peer_id}: {text[:80]}")
    
    params = {
        'peer_id': peer_id,
        'message': text,
        'random_id': random.randint(0, 2**63),
        'access_token': token,
        'v': '5.199',
    }
    
    if keyboard:
        params['keyboard'] = json.dumps(keyboard)
    
    result = requests.post("https://api.vk.com/method/messages.send", data=params).json()
    
    if 'error' in result:
        print(f"❌ VK: {result['error']}")
    
    return result


def get_vk_message_id(date_str):
    """Получить ID сообщения VK для даты"""
    if not VK_MESSAGE_IDS_FILE.exists():
        return None
    try:
        with open(VK_MESSAGE_IDS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get(date_str)
    except:
        return None


def save_vk_message_id(date_str, message_id):
    """Сохранить ID сообщения VK"""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = {}
        if VK_MESSAGE_IDS_FILE.exists():
            with open(VK_MESSAGE_IDS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
        data[date_str] = message_id
        with open(VK_MESSAGE_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения VK message_id: {e}")


# =============================================================================
# === ФОРМАТИРОВАНИЕ ОТЧЁТОВ ===================================================
# =============================================================================

def format_vk_report(yandex_data, ozon_data, avito_data, report_date, expenses):
    """Форматирование отчёта для ВКонтакте"""
    
    def fmt(amount):
        if amount is None:
            return "—"
        return f"{int(round(amount)):,}".replace(',', ' ')
    
    def date_suffix(data_date):
        if not data_date:
            return ""
        current = datetime.now().date()
        delta = (current - data_date).days
        if delta != 1:
            months = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
            return f" ({data_date.day} {months[data_date.month - 1]})"
        return ""
    
    from calendar import monthrange
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    days_in_month = monthrange(now.year, now.month)[1]
    
    # === СУММЫ ===
    ozon_sum = sum(ozon_data['daily'].values()) if ozon_data and 'daily' in ozon_data else 0
    yandex_sum = sum(p['last_amount'] for p in yandex_data['pvz_data'].values()) if yandex_data and 'pvz_data' in yandex_data else 0
    avito_sum = sum(avito_data['daily'].values()) if avito_data and 'daily' in avito_data else 0
    total_sum = yandex_sum + ozon_sum + avito_sum
    
    ozon_avg = sum(ozon_data['avg'].values()) if ozon_data and 'avg' in ozon_data else 0
    yandex_avg = sum(p['avg_daily'] for p in yandex_data['pvz_data'].values()) if yandex_data and 'pvz_data' in yandex_data else 0
    avito_avg = sum(avito_data['avg'].values()) if avito_data and 'avg' in avito_data else 0
    total_avg = yandex_avg + ozon_avg + avito_avg
    
    ozon_forecast = sum(ozon_data['forecast'].values()) if ozon_data and 'forecast' in ozon_data else 0
    yandex_forecast = sum(p['forecast'] for p in yandex_data['pvz_data'].values()) if yandex_data and 'pvz_data' in yandex_data else 0
    avito_forecast = sum(avito_data['forecast'].values()) if avito_data and 'forecast' in avito_data else 0
    total_forecast = yandex_forecast + ozon_forecast + avito_forecast
    
    # === РАСХОДЫ ===
    rent = expenses["аренда"]
    utilities = expenses["комуналка"]
    salary = expenses["зарплата_день"] * days_in_month
    other = expenses["прочее"]
    tax_percent = expenses.get("налог_процент", 6)
    tax = int(total_forecast * tax_percent / 100)
    total_expenses = rent + utilities + salary + tax + other
    net_profit = total_forecast - total_expenses
    
    lines = []
    lines.append(f"📅 Дата: {report_date} {current_time}")
    lines.append("─────────────")
    
    # СУММА
    lines.append(f"💰 СУММА: {fmt(total_sum)} ₽")
    if ozon_data and 'daily' in ozon_data:
        lines.append("  Озон:")
        for pvz, amount in sorted(ozon_data['daily'].items()):
            lines.append(f"    {pvz}: {fmt(amount)} ₽")
    if yandex_data and 'pvz_data' in yandex_data:
        ds = date_suffix(yandex_data.get('last_date'))
        lines.append(f"  Яндекс:{ds}")
        for pvz_id, pvz in sorted(yandex_data['pvz_data'].items()):
            pid = str(int(float(pvz_id)))
            lines.append(f"    ID_{pid}: {fmt(pvz['last_amount'])} ₽")
    if avito_data and 'daily' in avito_data:
        ds = date_suffix(avito_data.get('last_date'))
        lines.append(f"  Авито:{ds}")
        for pvz, amount in sorted(avito_data['daily'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            lines.append(f"    {pvz_short}: {fmt(amount)} ₽")
    lines.append("─────────────")
    
    # СРЕДНЕЕ
    lines.append(f"📈 СРЕДНЕЕ: {fmt(total_avg)} ₽")
    if ozon_data and 'avg' in ozon_data:
        lines.append("  Озон:")
        for pvz, amount in sorted(ozon_data['avg'].items()):
            lines.append(f"    {pvz}: {fmt(amount)} ₽")
    if yandex_data and 'pvz_data' in yandex_data:
        ds = date_suffix(yandex_data.get('last_date'))
        lines.append(f"  Яндекс:{ds}")
        for pvz_id, pvz in sorted(yandex_data['pvz_data'].items()):
            pid = str(int(float(pvz_id)))
            lines.append(f"    ID_{pid}: {fmt(pvz['avg_daily'])} ₽")
    if avito_data and 'avg' in avito_data:
        ds = date_suffix(avito_data.get('last_date'))
        lines.append(f"  Авито:{ds}")
        for pvz, amount in sorted(avito_data['avg'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            lines.append(f"    {pvz_short}: {fmt(amount)} ₽")
    lines.append("─────────────")
    
    # ПРОГНОЗ
    lines.append(f"🔮 ПРОГНОЗ: {fmt(total_forecast)} ₽")
    if ozon_data and 'forecast' in ozon_data:
        lines.append("  Озон:")
        for pvz, amount in sorted(ozon_data['forecast'].items()):
            lines.append(f"    {pvz}: {fmt(amount)} ₽")
    if yandex_data and 'pvz_data' in yandex_data:
        ds = date_suffix(yandex_data.get('last_date'))
        lines.append(f"  Яндекс:{ds}")
        for pvz_id, pvz in sorted(yandex_data['pvz_data'].items()):
            pid = str(int(float(pvz_id)))
            lines.append(f"    ID_{pid}: {fmt(pvz['forecast'])} ₽")
    if avito_data and 'forecast' in avito_data:
        ds = date_suffix(avito_data.get('last_date'))
        lines.append(f"  Авито:{ds}")
        for pvz, amount in sorted(avito_data['forecast'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            lines.append(f"    {pvz_short}: {fmt(amount)} ₽")
    lines.append("─────────────")
    
    # РАСХОДЫ
    lines.append(f"💸 РАСХОДЫ: {fmt(total_expenses)} ₽")
    lines.append(f"  Аренда: {fmt(rent)} ₽")
    lines.append(f"  Ком. услуги: {fmt(utilities)} ₽")
    lines.append(f"  ФОТ: {fmt(salary)} ₽ ({days_in_month} дн.)")
    lines.append(f"  Налоги: {fmt(tax)} ₽ ({tax_percent}%)")
    lines.append(f"  Прочее: {fmt(other)} ₽")
    lines.append("─────────────")
    
    # ПРИБЫЛЬ
    lines.append(f"💵 ПРОГНОЗ ПРИБЫЛИ: {fmt(net_profit)} ₽")
    
    return "\n".join(lines)
