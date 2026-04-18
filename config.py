# config.py — Конфигурация, БД, утилиты, браузер

import time
import logging
import requests
import json
import shutil
from pathlib import Path
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import sys


# === ПУТИ ===
BASE_DIR = Path("/opt/pvz-bot")
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "Отчеты"

# Все служебные файлы в одной папке
CHROME_PROFILE_DIR = DATA_DIR / "chrome_profile"
LOGS_DIR = DATA_DIR / "logs"
MESSAGE_IDS_FILE = DATA_DIR / "message_ids.json"
VK_MESSAGE_IDS_FILE = DATA_DIR / "vk_message_ids.json"

# === НАСТРОЙКИ ===
def load_settings():
    """Загрузка настроек из JSON файла"""
    settings_file = DATA_DIR / "settings.json"
    try:
        with open(settings_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        default = {
            "file_logging_enabled": True,
            "vk": {
                "access_token": "",
                "peer_id": 0
            },
            "expenses": {
                "аренда": 110000,
                "комуналка": 10000,
                "зарплата_день": 5000,
                "прочее": 5000,
                "налог_процент": 6
            },
            "telegram": {
                "bot_token": "7997782946:AAGhw1ELWNyNNLTdDMJXVZVY3ZHhbriLIUQ",
                "chat_id": "-1002544884792",
                "topic_id": 4222,
                "proxy": "socks5://botuser:botpass123@213.142.146.23:1080"
            },
            "urls": {
                "yandex_auth": "https://passport.yandex.ru/auth/list?retpath=https%3A%2F%2Flogistics.market.yandex.ru%2F...",
                "yandex_report": "https://hubs.market.yandex.ru/tpl-partner/148761735/month-reports?month=11&tabFilter=MONTH_CLOSING_BILLING&year=2025",
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
# Обратная совместимость: поддержка старого и нового ключа
FILE_LOGGING_ENABLED = SETTINGS.get("file_logging_enabled", SETTINGS.get("logging_enabled", True))
EXPENSES = SETTINGS.get("expenses", {})

# === TELEGRAM ===
tg_config = SETTINGS.get("telegram", {})
TELEGRAM_BOT_TOKEN = tg_config.get("bot_token", "")
TELEGRAM_CHAT_ID = tg_config.get("chat_id", "")
TELEGRAM_TOPIC_ID = tg_config.get("topic_id", 0)
TELEGRAM_PROXY = tg_config.get("proxy", "")

# === URLs ===
urls_config = SETTINGS.get("urls", {})
YANDEX_AUTH_URL = urls_config.get("yandex_auth", "")
YANDEX_REPORT_URL = urls_config.get("yandex_report", "")
OZON_BASE_URL = urls_config.get("ozon_base", "")
OZON_REPORTS_URL = urls_config.get("ozon_reports", "")
AVITO_URL = urls_config.get("avito", "")


# === VKONTAKTE ===
vk_config = SETTINGS.get("vk", {})
VK_ACCESS_TOKEN = vk_config.get("access_token", "")
VK_PEER_ID = vk_config.get("peer_id", 0)


def _to_unicode_bold(text: str) -> str:
    """Конвертирует текст в Unicode Bold символы (латиница, цифры, кириллица)"""
    # Латиница + цифры
    latin_normal = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    latin_bold   = '𝗔𝗕𝗖𝗗𝗘𝗙𝗚𝗛𝗜𝗝𝗞𝗟𝗠𝗡𝗢𝗣𝗤𝗥𝗦𝗧𝗨𝗩𝗪𝗫𝗬𝗭𝗮𝗯𝗰𝗱𝗲𝗳𝗴𝗵𝗶𝗷𝗸𝗹𝗺𝗻𝗼𝗽𝗾𝗿𝘀𝘁𝘂𝘃𝘄𝘅𝘆𝘇𝟬𝟭𝟮𝟯𝟰𝟱𝟲𝟳𝟴𝟵'
    # Кириллицу оставляем как есть
    latin_bold_list = list(latin_bold)
    result = []
    for ch in text:
        if ch in latin_normal:
            idx = latin_normal.index(ch)
            result.append(latin_bold_list[idx])
        else:
            result.append(ch)
    return ''.join(result)


def html_to_vk_text(html_text: str) -> str:
    """Конвертация HTML тегов в форматированный текст для ВКонтакте"""
    import re
    text = html_text
    # <b>...</b> -> Unicode Bold
    text = re.sub(r'<b>(.*?)</b>', lambda m: _to_unicode_bold(m.group(1)), text, flags=re.DOTALL)
    # <i>...</i> -> убираем теги
    text = re.sub(r'<i>(.*?)</i>', r'\1', text, flags=re.DOTALL)
    # <code>...</code> -> убираем теги
    text = re.sub(r'<code>(.*?)</code>', r'\1', text, flags=re.DOTALL)
    # spoiler-теги — раскрываем содержимое
    text = re.sub(r"<span class='tg-spoiler'>(.*?)</span>", r'\1', text, flags=re.DOTALL)
    # Убираем все оставшиеся HTML-теги
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


def send_vk_message(text: str, logger, peer_id=None, message_id=None):
    """Отправка или редактирование сообщения в ВКонтакте (без прокси)"""
    _peer_id = peer_id or VK_PEER_ID
    if not VK_ACCESS_TOKEN or not _peer_id:
        logger.warning("ВК: не настроен access_token или peer_id")
        return False
    try:
        import requests as _requests
        import time as _time
        vk_text = html_to_vk_text(text)[:4096]

        if message_id:
            # Редактирование существующего сообщения
            url = "https://api.vk.com/method/messages.edit"
            params = {
                'access_token': VK_ACCESS_TOKEN,
                'peer_id': _peer_id,
                'message_id': message_id,
                'message': vk_text,
                'v': '5.199'
            }
            response = _requests.post(url, data=params, timeout=30)
            result = response.json()
            if 'response' in result and result['response'] == 1:
                logger.info("✅ ВК: сообщение обновлено")
                return message_id
            else:
                err = result.get('error', {})
                logger.warning(f"⚠️ ВК не удалось обновить: {err.get('error_msg', '?')} — отправляю новое")

        # Отправка нового сообщения
        url = "https://api.vk.com/method/messages.send"
        params = {
            'access_token': VK_ACCESS_TOKEN,
            'peer_id': _peer_id,
            'message': vk_text,
            'random_id': int(_time.time() * 1000),
            'v': '5.199'
        }
        response = _requests.post(url, data=params, timeout=30)
        result = response.json()
        if 'response' in result:
            new_id = result['response']
            logger.info(f"✅ ВК: сообщение отправлено (id={new_id})")
            return new_id
        else:
            err = result.get('error', {})
            logger.error(f"❌ ВК ошибка: {err.get('error_msg', 'неизвестная')} (код {err.get('error_code', '?')})")
            return False
    except Exception as e:
        logger.error(f"Ошибка отправки в ВК: {e}")
        return False


def get_vk_message_id(date_str):
    """Получить VK message_id для даты"""
    if not VK_MESSAGE_IDS_FILE.exists():
        return None
    try:
        with open(VK_MESSAGE_IDS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get(date_str)
    except:
        return None


def save_vk_message_id(date_str, message_id):
    """Сохранить VK message_id"""
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
        print(f"Ошибка сохранения vk_message_id: {e}")


# === ЛОГИРОВАНИЕ ===
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


# === БРАУЗЕР ===
def create_driver(download_dir=None):
    """Создание Chrome WebDriver с общим профилем
    
    Args:
        download_dir: Путь к папке загрузок (по умолчанию REPORTS_DIR)
    """
    from selenium.webdriver.chrome.options import Options
    
    # Создаём папки
    DATA_DIR.mkdir(exist_ok=True)
    CHROME_PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)
    (REPORTS_DIR / "Яндекс").mkdir(exist_ok=True)
    (REPORTS_DIR / "Озон").mkdir(exist_ok=True)
    
    options = Options()
    
    # Настройки загрузок
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
    
    import os
    # Отключаем вывод WebDriver Manager
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
    """Очистка ненужного кеша Chrome с сохранением данных авторизации"""
    if not CHROME_PROFILE_DIR.exists():
        return
    
    # Папки для удаления (кеш, временные файлы)
    to_delete = [
        'Cache', 'Code Cache', 'GPUCache', 'Service Worker',
        'Default/Cache', 'Default/Code Cache', 'Default/GPUCache',
        'Default/Service Worker', 'ShaderCache', 'GraphiteDawnCache',
        'GrShaderCache', 'Default/Storage/ext', 'Default/Sync Extension Settings',
        'BrowserMetrics', 'CrashpadMetrics-active.pma',
        'optimization_guide_model_store', 'Default/optimization_guide_hint_cache_store',
        'Default/optimization_guide_model_metadata_store'
    ]
    
    # Папки для частичной очистки (оставляем только login-данные)
    network_path = CHROME_PROFILE_DIR / 'Default' / 'Network'
    
    deleted_size = 0
    
    # Удаляем кеш
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
    
    # Очищаем Network (сохраняя Cookies для автологина)
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


# === TELEGRAM ===
def send_or_update_telegram_message(text: str, logger, message_id=None):
    """Отправка или обновление сообщения в Telegram через SOCKS5 прокси"""
    try:
        # Создаём сессию с SOCKS5 прокси ТОЛЬКО для Telegram
        from requests import Session
        
        session = Session()
        session.proxies = {
            'http': 'socks5://botuser:botpass123@213.142.146.23:8080',
            'https': 'socks5://botuser:botpass123@213.142.146.23:8080'
        }
        
        if message_id:
            # Обновление существующего сообщения
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "message_id": message_id,
                "text": text,
                "parse_mode": "HTML"
            }
        else:
            # Отправка нового сообщения
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
