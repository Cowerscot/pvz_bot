# browser_manager.py — Управление браузером и сессиями

import time
import json
import threading
from datetime import datetime
from selenium import webdriver

from config import SELENIUM_URL, CHROME_OPTIONS


# === Глобальные переменные ===
_shared_driver = None
_shared_driver_lock = threading.Lock()
active_sessions = {}
active_sessions_lock = threading.Lock()


def _make_options():
    """Создание настроек Chrome"""
    from selenium.webdriver.chrome.options import Options
    
    opts = Options()
    opts.add_argument('--no-sandbox')
    opts.add_argument('--disable-dev-shm-usage')
    opts.add_argument('--disable-blink-features=AutomationControlled')
    opts.add_argument('--disable-gpu')
    opts.add_experimental_option('excludeSwitches', ['enable-automation'])
    opts.add_experimental_option('useAutomationExtension', False)
    
    # Дополнительные настройки из конфига
    for opt_name, opt_value in CHROME_OPTIONS.items():
        if isinstance(opt_value, bool):
            if opt_value:
                opts.add_argument(f'--{opt_name}')
        elif isinstance(opt_value, str):
            opts.add_argument(f'--{opt_name}={opt_value}')
    
    return opts


def get_shared_driver():
    """Получение общего экземпляра браузера"""
    global _shared_driver
    
    with _shared_driver_lock:
        # Проверяем существующий драйвер
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
        
        # Создаём новый драйвер с повторными попытками
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
    """Освобождение общего браузера"""
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
    """Закрытие сессии пользователя"""
    with active_sessions_lock:
        active_sessions.pop(user_id, None)


def start_auth_session(user_id, peer_id, send_fn, marketplace_url, cancel_kb, stats_kb, timeout=300):
    """
    Запуск сессии авторизации для пользователя
    
    Args:
        user_id: ID пользователя
        peer_id: ID чата для отправки сообщений
        send_fn: Функция отправки сообщений
        marketplace_url: URL авторизации маркетплейса
        cancel_kb: Клавиатура отмены
        stats_kb: Клавиатура статистики
        timeout: Таймаут авторизации в секундах
    """
    try:
        # Получаем браузер
        try:
            driver = get_shared_driver()
        except Exception as e:
            send_fn(peer_id, f"❌ Браузер недоступен: {str(e)[:200]}")
            return
        
        # Регистрируем сессию
        with active_sessions_lock:
            active_sessions[user_id] = driver
        
        # Открываем страницу авторизации
        driver.get(marketplace_url)
        time.sleep(3)
        print(f"[Auth] Открыта страница: {driver.current_url}")
        
        # Отправляем инструкцию пользователю
        from config import NOVNC_URL
        send_fn(peer_id,
            f"🔐 Авторизация:\n\n"
            f"1. Открой ссылку:\n{NOVNC_URL}\n\n"
            f"2. Войди через аккаунт / СМС\n"
            f"3. Бот определит вход автоматически ✅\n\n"
            f"⏱ Ссылка активна 5 минут.",
            cancel_kb()
        )
        
        # Ожидание авторизации
        elapsed = 0
        while elapsed < timeout:
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
            
            # Проверка успешной авторизации
            if 'passport.yandex.ru' not in cur and 'auth' not in cur.lower():
                real_cookies = driver.get_cookies()
                save_cookies_raw(user_id, real_cookies if real_cookies else [{'name': 'authorized', 'value': '1', 'domain': 'yandex.ru'}])
                send_fn(peer_id,
                    "✅ Авторизация успешна! Профиль сохранён.\n"
                    "🚀 Можно запрашивать статистику.",
                    stats_kb()
                )
                close_session(user_id)
                return
        
        # Таймаут
        send_fn(peer_id,
            "⏰ Время авторизации истекло (5 мин).\n"
            "Нажми кнопку маркетплейса чтобы попробовать снова.",
            cancel_kb()
        )
        
    except Exception as e:
        send_fn(peer_id, f"❌ Ошибка браузера: {str(e)[:300]}")
        print(f"[Auth] Exception {user_id}: {e}")
    finally:
        close_session(user_id)


def save_cookies_raw(vk_id, cookies):
    """Сохранение cookies в базу данных (низкоуровневая функция)"""
    from database import save_cookies
    save_cookies(vk_id, cookies)
    for ck in cookies:
        if ck.get('name') == 'Session_id':
            exp = ck.get('expiry')
            if exp:
                print(f"[Cookies] Session_id истекает: {datetime.fromtimestamp(exp)}")
            else:
                print("[Cookies] Session_id: expiry не задан")


def is_user_authorized(vk_id, marketplace='yandex'):
    """
    Проверка авторизации пользователя
    
    Returns:
        bool: True если авторизован, False иначе
    """
    from database import get_cookies
    cookies = get_cookies(vk_id, marketplace)
    return cookies is not None


def clear_user_data(vk_id, marketplace=None):
    """
    Очистка данных пользователя
    
    Args:
        vk_id: ID пользователя
        marketplace: Если указан, очищает только этот маркетплейс, иначе все
    """
    from database import clear_user_cookies
    clear_user_cookies(vk_id, marketplace)
    print(f"[Data] Очищены данные пользователя {vk_id}" + (f" ({marketplace})" if marketplace else ""))