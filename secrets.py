# secrets.py — Чувствительные данные (токены, пароли, ключи)
# НЕ КОММИТИТЬ В GIT! Добавить в .gitignore

# === VK API ===
VK_ACCESS_TOKEN = "vk1.a.Xr1Qgj3L7lqF4uQaE0nN07783Nj-sQTbdB46NFtizeoQzc1OklbKCTPw-d1maH-7wYFSn4JeU1_QMJjR-UA9u8bz6qAlOSREJYtq-l0Ub_dT4bRYH7nDD6Re8ybDfCU1g5bwEUkJHYMNT3crgyiXjS2FQKkQX8TKJqycOVEPrOpGM2TQLo_IzSOKBtOoIeflCTdrWcWQLWQrGjqumKozJw"
VK_PEER_ID = 237702703  # ID группы
VK_GROUP_ID = 237702703
VK_CONFIRMATION_TOKEN = "fee275eb"

# === URLs и домены ===
BOT_HOST = "https://pvz-bot.sytes.net"
SELENIUM_URL = "http://127.0.0.1:4444/wd/hub"
SELENIUM_URL_EXT = "http://127.0.0.1:4444"
NOVNC_URL = "https://pvz-bot.sytes.net/novnc/?autoconnect=1&resize=scale"

# === Яндекс ПВЗ ===
YANDEX_AUTH_URL = "https://passport.yandex.ru/pwl-yandex/auth/add?retpath=https%3A%2F%2Flogistics.market.yandex.ru%2F...&cause=auth&process_uuid=858f9996-fe09-4ca6-9757-95ba98a8256a"
YANDEX_REPORT_URL = "https://logistics.market.yandex.ru/reports"

# === Ozon ===
OZON_BASE_URL = "https://ozon.ru"
OZON_REPORTS_URL = "https://seller.ozon.ru/api/reports"

# === Avito ===
AVITO_URL = "https://avito.ru"

# === Пути ===
REPORTS_DIR = "/opt/pvz-bot/reports"
CHROME_PROFILE_DIR = "/opt/pvz-bot/chrome_profile"
DB_FILE = "/opt/pvz-bot/bot_database.db"

# === Настройки Selenium ===
CHROME_OPTIONS = {
    'no-sandbox': True,
    'disable-dev-shm-usage': True,
    'disable-gpu': True,
    'disable-blink-features': 'AutomationControlled',
}
