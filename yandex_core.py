# yandex_core.py
import time
from pathlib import Path
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
from openpyxl import load_workbook
from collections import defaultdict
from calendar import monthrange

from full_config import YANDEX_AUTH_URL, YANDEX_REPORT_URL, REPORTS_DIR


def ensure_authorized(driver, logger):
    """Проверка авторизации — открываем целевой URL и смотрим куда попали"""
    logger.info("  🔐 Проверка авторизации...")

    original_handles = set(driver.window_handles)
    driver.execute_script("window.open('');")
    new_handle = [h for h in driver.window_handles if h not in original_handles][0]
    driver.switch_to.window(new_handle)
    driver.get(YANDEX_REPORT_URL)
    time.sleep(3)

    current_url = driver.current_url
    print(f"[Core] URL после открытия: {current_url}")

    if "passport.yandex.ru" not in current_url and "auth" not in current_url.lower():
        logger.info("  ✅ Авторизован")
        driver.close()
        driver.switch_to.window(list(driver.window_handles)[0])
        return

    logger.info("  ⚠️ Не авторизован — сессия устарела")
    driver.close()
    driver.switch_to.window(list(driver.window_handles)[0])
    raise Exception("Сессия устарела. Нажми 🔄 Переподключить Яндекс.")


def open_report_page_and_download(driver, logger, download_dir):
    import subprocess
    logger.info("  📄 Открытие страницы отчётов...")

    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[-1])

    driver.get(YANDEX_REPORT_URL)
    time.sleep(3)

    if "passport" in driver.current_url.lower() or "auth" in driver.current_url.lower():
        time.sleep(15)
        driver.get(YANDEX_REPORT_URL)
        time.sleep(2)

    wait = WebDriverWait(driver, 30)

    download_btn = None
    strategies = [
        lambda: wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Скачать')]"))),
        lambda: driver.find_element(By.XPATH, "//button[contains(., 'Скачать')]"),
    ]

    for i, strategy in enumerate(strategies, 1):
        try:
            download_btn = strategy()
            logger.info(f"  ✅ Кнопка найдена (стратегия {i})")
            break
        except:
            continue

    if not download_btn:
        driver.save_screenshot("/opt/pvz-bot/debug_page.png")
        page_text = driver.execute_script("return document.body.innerText;")
        logger.info(f"  📄 Текст страницы (первые 500): {page_text[:500]}")
        raise Exception("Кнопка 'Скачать' не найдена. Проверь скриншот.")

    if not download_btn.is_displayed():
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_btn)
        time.sleep(1)

    try:
        download_btn.click()
    except:
        driver.execute_script("arguments[0].click();", download_btn)

    logger.info("  ✅ Скачивание запущено")


def _get_container_download_dir(driver):
    """Определяем папку загрузок внутри контейнера"""
    try:
        home = driver.execute_script("return navigator.userAgent")
    except:
        pass
    # Стандартные пути для selenium/standalone-chrome
    return "/home/seluser/Downloads"


def wait_for_xlsx_download(download_dir, logger, timeout=60, container_name="selenium-chrome"):
    import subprocess
    logger.info("  ⏳ Ожидание файла в контейнере...")

    container_dl = "/home/seluser/Downloads"
    waited = 0

    # Фиксируем начальный список файлов в контейнере
    try:
        out = subprocess.check_output(
            ['docker', 'exec', container_name, 'find', container_dl, '-name', '*.xlsx'],
            stderr=subprocess.DEVNULL, text=True
        ).strip()
        initial_files = set(out.splitlines()) if out else set()
    except Exception as e:
        logger.warning(f"  ⚠️ Не удалось получить список файлов контейнера: {e}")
        initial_files = set()

    while waited < timeout:
        time.sleep(2)
        waited += 2
        try:
            out = subprocess.check_output(
                ['docker', 'exec', container_name, 'find', container_dl, '-name', '*.xlsx'],
                stderr=subprocess.DEVNULL, text=True
            ).strip()
            current_files = set(out.splitlines()) if out else set()
        except Exception:
            continue

        new_files = current_files - initial_files
        # Исключаем .crdownload (незавершённые)
        new_files = {f for f in new_files if not f.endswith('.crdownload')}

        if new_files:
            container_path = list(new_files)[0]
            filename = container_path.split('/')[-1]
            dest = download_dir / filename
            logger.info(f"  📦 Копирую из контейнера: {filename}")
            try:
                subprocess.check_call(
                    ['docker', 'cp', f'{container_name}:{container_path}', str(dest)],
                    stderr=subprocess.DEVNULL
                )
                time.sleep(1)
                logger.info(f"  ✅ Получен: {dest.name}")
                return dest
            except Exception as e:
                raise Exception(f"Не удалось скопировать файл из контейнера: {e}")

    raise Exception(f"Файл не загружен за {timeout} сек")


def analyze_report(filepath, logger):
    logger.info(f"  📊 Анализ отчёта...")
    wb = load_workbook(filepath, read_only=True)
    if "Транзакции" not in wb.sheetnames:
        raise ValueError("Лист 'Транзакции' не найден")

    sheet = wb["Транзакции"]
    headers = [cell.value for cell in sheet[1]]

    try:
        pvz_id_col = headers.index("ID ПВЗ") if "ID ПВЗ" in headers else 0
        time_col = headers.index("Время (мск)")
        amount_col = headers.index("Стоимость услуги, руб")
    except ValueError as e:
        raise ValueError(f"Не найдены колонки: {e}")

    pvz_daily_totals = defaultdict(lambda: defaultdict(float))

    for row in sheet.iter_rows(min_row=2, values_only=True):
        pvz_id = row[pvz_id_col]
        time_val = row[time_col]
        amount_val = row[amount_col]

        if not pvz_id or not time_val or not amount_val:
            continue

        if isinstance(time_val, datetime):
            day = time_val.date()
        elif isinstance(time_val, str):
            try:
                dt = datetime.strptime(time_val, "%Y-%m-%d %H:%M:%S")
                day = dt.date()
            except:
                continue
        else:
            continue

        try:
            amount = float(amount_val) if isinstance(amount_val, (int, float)) else float(str(amount_val).replace(',', '.'))
        except:
            continue

        pvz_daily_totals[pvz_id][day] += amount

    if not pvz_daily_totals:
        raise ValueError("Нет данных")

    result = {'pvz_data': {}, 'last_date': None}
    overall_last_date = None

    for pvz_id, daily_totals in pvz_daily_totals.items():
        last_date = max(daily_totals.keys())
        last_amount = round(daily_totals[last_date])

        if overall_last_date is None or last_date > overall_last_date:
            overall_last_date = last_date

        month_days = {k: v for k, v in daily_totals.items()
                      if k.month == last_date.month and k.year == last_date.year}
        available_days = len(month_days)
        avg_daily = round(sum(month_days.values()) / available_days) if available_days > 0 else 0

        days_in_month = monthrange(last_date.year, last_date.month)[1]
        first_working_day = min(month_days.keys()).day if month_days else 1
        working_days = days_in_month - (first_working_day - 1)
        forecast = round(avg_daily * working_days)

        result['pvz_data'][pvz_id] = {
            'last_date': last_date,
            'last_amount': last_amount,
            'avg_daily': avg_daily,
            'forecast': forecast
        }

    result['last_date'] = overall_last_date
    return result


def process_yandex_report(driver, logger):
    print(f"[Core] process_yandex_report начало")
    yandex_dir = REPORTS_DIR / "Яндекс"
    yandex_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().date()
    final_path = yandex_dir / f"{today.day:02d}.{today.month:02d}.{today.year}.xlsx"

    print(f"[Core] Ищу файл: {final_path}")
    if final_path.exists():
        logger.info(f"  ✅ Файл существует: {final_path.name}")
        print(f"[Core] Файл найден, запускаю analyze_report")
        return analyze_report(final_path, logger)

    try:
        ensure_authorized(driver, logger)
        time.sleep(2)
        open_report_page_and_download(driver, logger, yandex_dir)
        new_file = wait_for_xlsx_download(yandex_dir, logger)
        
        if new_file.exists():
            new_file.rename(final_path)

        report_data = analyze_report(final_path, logger)

        if len(driver.window_handles) > 1:
            driver.close()
            driver.switch_to.window(driver.window_handles[0])

        return report_data

    except Exception as e:
        import traceback
        print(f"[Core] ИСКЛЮЧЕНИЕ: {e}")
        print(f"[Core] Traceback: {traceback.format_exc()}")
        logger.error(f"Ошибка: {e}")
        if len(driver.window_handles) > 1:
            try:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
            except:
                pass
        raise