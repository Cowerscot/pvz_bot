# bot_sessions.py — управление Selenium-браузером и авторизацией
import threading
import time
from selenium import webdriver

from bot_db import save_cookies
from full_config import YANDEX_AUTH_URL

SELENIUM_URL = "http://127.0.0.1:4444/wd/hub"
NOVNC_URL = "https://pvz-bot.sytes.net/novnc/?autoconnect=1&resize=scale"
CHROME_PROFILE_DIR = "/opt/pvz-bot/chrome_profile"

active_sessions: dict = {}
active_sessions_lock = threading.Lock()

_shared_driver = None
_shared_driver_lock = threading.Lock()


def _make_options():
    opts = webdriver.ChromeOptions()
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_argument('--disable-dev-shm-usage')
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
                import time as _t; _t.sleep(3)
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