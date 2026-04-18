# bot_handlers.py — обработчики команд VK-бота
import threading
import time
import logging
from datetime import datetime

from bot_db import get_cookies
from bot_sessions import (
    active_sessions, active_sessions_lock,
    start_auth_session, close_session, get_shared_driver,
    release_shared_driver, NOVNC_URL
)


# ── Клавиатуры ────────────────────────────────────────────────────────────────

def _kb(*rows):
    """Хелпер для создания клавиатуры"""
    return {"buttons": [[{"action": {"type": "text", "label": lbl}}] for lbl in rows],
            "one_time": False, "inline": False}


def kb_policy():      return _kb("✅ Принимаю политику")
def kb_marketplace(): return _kb("🟡 Яндекс ПВЗ")
def kb_cancel():      return _kb("❌ Отменить авторизацию")
def kb_stats():       return _kb("📊 Статистика Яндекс", "🔄 Переподключить Яндекс")


# ── Статистика ────────────────────────────────────────────────────────────────

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
                    # Remote драйвер принимает только базовые поля
                    c = {'name': ck['name'], 'value': ck['value'], 'domain': ck.get('domain', '.yandex.ru')}
                    if ck.get('path'):
                        c['path'] = ck['path']
                    driver.add_cookie(c)
                except Exception as ce:
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


# ── Роутер команд ─────────────────────────────────────────────────────────────

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