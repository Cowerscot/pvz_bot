# config.py — Конфигурация бота (публичные настройки)
# Импорт чувствительных данных из secrets.py

from secrets import (
    # VK API
    VK_ACCESS_TOKEN, VK_PEER_ID, VK_GROUP_ID, VK_CONFIRMATION_TOKEN,
    # URLs
    BOT_HOST, SELENIUM_URL, SELENIUM_URL_EXT, NOVNC_URL,
    # Яндекс
    YANDEX_AUTH_URL, YANDEX_REPORT_URL,
    # Ozon
    OZON_BASE_URL, OZON_REPORTS_URL,
    # Avito
    AVITO_URL,
    # Пути
    REPORTS_DIR, CHROME_PROFILE_DIR, DB_FILE,
    # Selenium
    CHROME_OPTIONS,
)

# === Настройки логирования ===
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# === Таймауты ===
AUTH_TIMEOUT = 300  # 5 минут на авторизацию
DOWNLOAD_TIMEOUT = 30  # секунд на скачивание файла
SELENIUM_RETRY_ATTEMPTS = 3
SELENIUM_RETRY_DELAY = 3  # секунды

# === Маркетплейсы ===
MARKETPLACES = {
    'yandex': {'name': '🟡 Яндекс ПВЗ', 'enabled': True},
    'ozon': {'name': '🔵 Ozon ПВЗ', 'enabled': True},
    'avito': {'name': '🟢 Avito ПВЗ', 'enabled': True},
}

# === Хелперы для VK ===
def send_vk_message(peer_id, text, keyboard=None):
    """Отправка сообщения через VK API"""
    import json
    import random
    import requests
    
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


def get_vk_message_id():
    """Получение ID последнего сообщения (заглушка)"""
    return None


def save_vk_message_id(message_id):
    """Сохранение ID сообщения (заглушка)"""
    pass
