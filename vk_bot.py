# vk_bot.py — VK бот: обработчики сообщений, команды

import random
import json
import time
import threading
import logging
from datetime import datetime

from config import (
    VK_ACCESS_TOKEN, VK_CONFIRMATION_TOKEN, MARKETPLACES,
    YANDEX_AUTH_URL, NOVNC_URL
)
from database import (
    init_db, get_or_create_user, get_cookies, is_event_processed,
    mark_event_processed, get_user_marketplaces, clear_user_cookies
)
from browser_manager import (
    start_auth_session, close_session, active_sessions, active_sessions_lock,
    get_shared_driver, release_shared_driver
)


# =============================================================================
# КЛАВИАТУРЫ
# =============================================================================

def _kb(*rows):
    """Хелпер для создания клавиатуры"""
    return {
        "buttons": [[{"action": {"type": "text", "label": lbl}}] for lbl in rows],
        "one_time": False,
        "inline": False
    }


def kb_start():
    """Клавиатура старта"""
    return _kb("✅ Принимаю политику")


def kb_marketplace():
    """Клавиатура выбора маркетплейса"""
    buttons = []
    if MARKETPLACES.get('yandex', {}).get('enabled'):
        buttons.append("🟡 Яндекс ПВЗ")
    if MARKETPLACES.get('ozon', {}).get('enabled'):
        buttons.append("🔵 Ozon ПВЗ")
    if MARKETPLACES.get('avito', {}).get('enabled'):
        buttons.append("🟢 Avito ПВЗ")
    
    if not buttons:
        buttons = ["⚠️ Нет доступных маркетплейсов"]
    
    return _kb(*buttons)


def kb_cancel():
    """Клавиатура отмены"""
    return _kb("❌ Отменить авторизацию")


def kb_stats(marketplace='yandex'):
    """Клавиатура статистики"""
    mp_name = {'yandex': 'Яндекс', 'ozon': 'Ozon', 'avito': 'Avito'}.get(marketplace, '')
    buttons = [f"📊 Статистика {mp_name}", f"🗑️ Очистить данные {mp_name}", f"🔄 Переподключить {mp_name}", "⬅️ Назад"]
    return _kb(*buttons)


def kb_main_menu():
    """Главное меню"""
    return _kb("⬅️ Назад")


# =============================================================================
# ОБРАБОТЧИКИ МАРКЕТПЛЕЙСОВ
# =============================================================================

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
        cookies = get_cookies(user_id, 'yandex')
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
                    c = {
                        'name': ck['name'],
                        'value': ck['value'],
                        'domain': ck.get('domain', '.yandex.ru')
                    }
                    if ck.get('path'):
                        c['path'] = ck['path']
                    if ck.get('expiry'):
                        c['expiry'] = ck['expiry']
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
            send_fn(peer_id, "⚠️ Нет данных в отчёте.", kb_stats('yandex'))
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
        send_fn(peer_id, "\n".join(lines), kb_stats('yandex'))
        
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[Stats] ОШИБКА {user_id}: {e}\n{tb}")
        vk_h.flush_buf()
        if "устарели" in str(e) or "Нет cookies" in str(e) or "Файл не появился" in str(e):
            send_fn(peer_id, f"🔑 {e}\nНажми 🔄 Переподключить Яндекс.", kb_stats('yandex'))
        else:
            send_fn(peer_id, f"❌ Ошибка: {str(e)[:150]}", kb_stats('yandex'))


def run_ozon_stats(user_id, peer_id, send_fn):
    """Парсинг Ozon и отправка результата"""
    
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
    logger = logging.getLogger(f'ozon_{user_id}')
    logger.setLevel(logging.DEBUG)
    logger.handlers = [logging.StreamHandler(), vk_h]
    logger.propagate = False
    
    try:
        cookies = get_cookies(user_id, 'ozon')
        if not cookies:
            send_fn(peer_id, "❌ Сессия не найдена. Авторизуйся заново.", kb_marketplace())
            return
        
        driver = get_shared_driver()
        logger.info(f"✅ Браузер готов: {driver.session_id[:8]}...")
        
        # Загружаем куки
        try:
            driver.get("https://ozon.ru")
            time.sleep(1)
            driver.delete_all_cookies()
            for ck in cookies:
                try:
                    c = {'name': ck['name'], 'value': ck['value'], 'domain': ck.get('domain', '.ozon.ru')}
                    if ck.get('path'): c['path'] = ck['path']
                    if ck.get('expiry'): c['expiry'] = ck['expiry']
                    driver.add_cookie(c)
                except: pass
            logger.info("✅ Куки загружены")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка куки: {e}")
        
        from ozon_core import process_ozon_report
        report_data = process_ozon_report(driver, logger)
        
        if not report_data:
            vk_h.flush_buf()
            send_fn(peer_id, "⚠️ Нет данных.", kb_stats('ozon'))
            return
        
        now = datetime.now()
        months = ['янв','фев','мар','апр','мая','июн','июл','авг','сен','окт','ноя','дек']
        lines = [f"📊 Ozon | {now.day} {months[now.month-1]}", "─────────────"]
        
        for pvz, data in report_data.items():
            lines += [f"{pvz}:", f"  Выручка: {data.get('revenue', 0):,} ₽"]
        
        vk_h.flush_buf()
        send_fn(peer_id, "\n".join(lines), kb_stats('ozon'))
        
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[Ozon] ОШИБКА {user_id}: {e}\n{tb}")
        vk_h.flush_buf()
        send_fn(peer_id, f"❌ Ошибка: {str(e)[:150]}", kb_stats('ozon'))


def run_avito_stats(user_id, peer_id, send_fn):
    """Парсинг Avito и отправка результата"""
    
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
    logger = logging.getLogger(f'avito_{user_id}')
    logger.setLevel(logging.DEBUG)
    logger.handlers = [logging.StreamHandler(), vk_h]
    logger.propagate = False
    
    try:
        cookies = get_cookies(user_id, 'avito')
        if not cookies:
            send_fn(peer_id, "❌ Сессия не найдена. Авторизуйся заново.", kb_marketplace())
            return
        
        driver = get_shared_driver()
        logger.info(f"✅ Браузер готов: {driver.session_id[:8]}...")
        
        try:
            driver.get("https://avito.ru")
            time.sleep(1)
            driver.delete_all_cookies()
            for ck in cookies:
                try:
                    c = {'name': ck['name'], 'value': ck['value'], 'domain': ck.get('domain', '.avito.ru')}
                    if ck.get('path'): c['path'] = ck['path']
                    if ck.get('expiry'): c['expiry'] = ck['expiry']
                    driver.add_cookie(c)
                except: pass
            logger.info("✅ Куки загружены")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка куки: {e}")
        
        from avito_core import process_avito_report
        report_data = process_avito_report(driver, logger)
        
        if not report_data:
            vk_h.flush_buf()
            send_fn(peer_id, "⚠️ Нет данных.", kb_stats('avito'))
            return
        
        now = datetime.now()
        months = ['янв','фев','мар','апр','мая','июн','июл','авг','сен','окт','ноя','дек']
        lines = [f"📊 Avito | {now.day} {months[now.month-1]}", "─────────────"]
        
        for pvz, data in report_data.items():
            lines += [f"{pvz}:", f"  Выручка: {data.get('revenue', 0):,} ₽"]
        
        vk_h.flush_buf()
        send_fn(peer_id, "\n".join(lines), kb_stats('avito'))
        
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        print(f"[Avito] ОШИБКА {user_id}: {e}\n{tb}")
        vk_h.flush_buf()
        send_fn(peer_id, f"❌ Ошибка: {str(e)[:150]}", kb_stats('avito'))


def handle_marketplace_choice(user_id, peer_id, text, send_fn):
    """Обработка выбора маркетплейса"""
    t = text.lower().strip()
    
    # Кнопка Назад - возвращаем к выбору маркетплейса
    if t == '⬅️ назад':
        send_fn(peer_id, "Выбери маркетплейс:", kb_marketplace())
        return
    
    # Яндекс
    if t == '🟡 яндекс пвз':
        with active_sessions_lock:
            already = user_id in active_sessions
        if already:
            send_fn(peer_id, f"⚠️ Сессия уже открыта.\nПерейди: {NOVNC_URL}", kb_cancel())
            return
        
        # Проверяем есть ли уже cookies
        existing_cookies = get_cookies(user_id, 'yandex')
        if existing_cookies:
            send_fn(peer_id, 
                "✅ Вы уже авторизованы в Яндекс.ПВЗ.\n\n"
                "Выберите действие:",
                kb_stats('yandex')
            )
            return
        
        threading.Thread(
            target=start_auth_session,
            args=(user_id, peer_id, send_fn, YANDEX_AUTH_URL, kb_cancel, lambda: kb_stats('yandex')),
            daemon=True
        ).start()
        return
    
    # Ozon
    elif t == '🔵 ozon пвз':
        with active_sessions_lock:
            already = user_id in active_sessions
        if already:
            send_fn(peer_id, f"⚠️ Сессия уже открыта.\nПерейди: {NOVNC_URL}", kb_cancel())
            return
        
        existing_cookies = get_cookies(user_id, 'ozon')
        if existing_cookies:
            send_fn(peer_id, 
                "✅ Вы уже авторизованы в Ozon.\n\n"
                "Выберите действие:",
                kb_stats('ozon')
            )
            return
        
        from config import OZON_BASE_URL
        threading.Thread(
            target=start_auth_session,
            args=(user_id, peer_id, send_fn, OZON_BASE_URL + "/seller", kb_cancel, lambda: kb_stats('ozon')),
            daemon=True
        ).start()
        return
    
    # Avito
    elif t == '🟢 авито пвз':
        with active_sessions_lock:
            already = user_id in active_sessions
        if already:
            send_fn(peer_id, f"⚠️ Сессия уже открыта.\nПерейди: {NOVNC_URL}", kb_cancel())
            return
        
        existing_cookies = get_cookies(user_id, 'avito')
        if existing_cookies:
            send_fn(peer_id, 
                "✅ Вы уже авторизованы в Avito.\n\n"
                "Выберите действие:",
                kb_stats('avito')
            )
            return
        
        from config import AVITO_URL
        threading.Thread(
            target=start_auth_session,
            args=(user_id, peer_id, send_fn, AVITO_URL + "/profile", kb_cancel, lambda: kb_stats('avito')),
            daemon=True
        ).start()
        return
    
    # Переподключение
    elif t == '🔄 переподключить яндекс':
        clear_user_cookies(user_id, 'yandex')
        send_fn(peer_id, "🔄 Сбрасываю сессию Яндекс...\nТеперь авторизуйся заново.", kb_marketplace())
        return
    
    elif t == '🔄 переподключить ozon':
        clear_user_cookies(user_id, 'ozon')
        send_fn(peer_id, "🔄 Сбрасываю сессию Ozon...", kb_marketplace())
        return
    
    elif t == '🔄 переподключить avito':
        clear_user_cookies(user_id, 'avito')
        send_fn(peer_id, "🔄 Сбрасываю сессию Avito...", kb_marketplace())
        return
    
    # Очистка данных
    elif t == '🗑️ очистить данные яндекс':
        clear_user_cookies(user_id, 'yandex')
        send_fn(peer_id, "🗑️ Данные Яндекс очищены.\nАвторизуйся заново при необходимости.", kb_marketplace())
        return
    
    elif t == '🗑️ очистить данные ozon':
        clear_user_cookies(user_id, 'ozon')
        send_fn(peer_id, "🗑️ Данные Ozon очищены.", kb_marketplace())
        return
    
    elif t == '🗑️ очистить данные avito':
        clear_user_cookies(user_id, 'avito')
        send_fn(peer_id, "🗑️ Данные Avito очищены.", kb_marketplace())
        return
    
    elif t == '🗑️ очистить все данные':
        clear_user_cookies(user_id)
        send_fn(peer_id, "🗑️ Все данные очищены.\nНачните сначала с /start", kb_start())
        return
    
    # Статистика
    elif t == '📊 статистика яндекс':
        cookies = get_cookies(user_id, 'yandex')
        if not cookies:
            send_fn(peer_id, "❌ Нет сохранённой сессии. Сначала авторизуйся.", kb_marketplace())
            return
        send_fn(peer_id, "⏳ Запускаю парсинг Яндекс.ПВЗ...")
        threading.Thread(target=run_yandex_stats, args=(user_id, peer_id, send_fn), daemon=True).start()
        return
    
    elif t == '📊 статистика ozon':
        cookies = get_cookies(user_id, 'ozon')
        if not cookies:
            send_fn(peer_id, "❌ Нет сохранённой сессии. Сначала авторизуйся.", kb_marketplace())
            return
        send_fn(peer_id, "⏳ Запускаю парсинг Ozon...")
        threading.Thread(target=run_ozon_stats, args=(user_id, peer_id, send_fn), daemon=True).start()
        return
    
    elif t == '📊 статистика avito':
        cookies = get_cookies(user_id, 'avito')
        if not cookies:
            send_fn(peer_id, "❌ Нет сохранённой сессии. Сначала авторизуйся.", kb_marketplace())
            return
        send_fn(peer_id, "⏳ Запускаю парсинг Avito...")
        threading.Thread(target=run_avito_stats, args=(user_id, peer_id, send_fn), daemon=True).start()
        return


# =============================================================================
# ОБРАБОТЧИК СООБЩЕНИЙ
# =============================================================================

def handle_message(peer_id, text, user_id, send_fn):
    """Основной обработчик сообщений"""
    t = text.lower().strip()
    
    if t in ('/start', 'начать'):
        get_or_create_user(user_id)
        send_fn(peer_id,
            "👋 Привет! Я бот для отслеживания статистики ПВЗ.\n\n"
            "Прими Политику конфиденциальности для начала работы.",
            kb_start()
        )
    
    elif t == '✅ принимаю политику':
        send_fn(peer_id, "Выбери маркетплейс:", kb_marketplace())
    
    elif t == '❌ отменить авторизацию':
        close_session(user_id)
        send_fn(peer_id, "❌ Авторизация отменена.", kb_marketplace())
    
    else:
        handle_marketplace_choice(user_id, peer_id, text, send_fn)
