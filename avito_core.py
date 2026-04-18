# avito_core.py - Ядро обработки отчётов Avito

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

from config import REPORTS_DIR, AVITO_URL


def wait_for_authorization(driver, logger, timeout=300):
    """Ожидание авторизации Авито с автонажатием кнопки входа"""
    # Сразу пробуем нажать кнопку «Войти»
    try:
        submit_btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "submit-button"))
        )
        url_before = driver.current_url
        submit_btn.click()
        time.sleep(3)

        # Если URL изменился — авторизация прошла
        if driver.current_url != url_before and "/gw/login" not in driver.current_url:
            return True
    except Exception:
        pass  # Кнопки нет — страница уже авторизована или форма другая

    # Проверяем — вдруг уже авторизованы
    if "/gw/login" not in driver.current_url and "pvz.avito.ru" in driver.current_url:
        return True

    # Ждём ручной авторизации
    start = time.time()
    while time.time() - start < timeout:
        if "/gw/login" not in driver.current_url and "pvz.avito.ru" in driver.current_url:
            return True
        time.sleep(2)
    
    return False


def download_avito_report(driver, logger, avito_dir):
    """Скачивание отчёта Авито"""
    result_file = None
    try:
        # Прямой переход в раздел аналитики
        analytics_url = "https://pvz.avito.ru/analytics"
        if driver.current_url.rstrip('/') != analytics_url.rstrip('/'):
            driver.get(analytics_url)
            time.sleep(3)
        
        # Кнопка скачивания
        download_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "[class*='DownloadTableButton']"
            ))
        )
        
        # Удаляем только analytics.xlsx (дефолтное имя от Авито) перед скачиванием
        for known_name in ["analytics.xlsx", "analytics (1).xlsx", "analytics (2).xlsx"]:
            stale = avito_dir / known_name
            if stale.exists():
                try:
                    stale.unlink()
                except Exception:
                    pass

        # Запоминаем файлы до скачивания
        initial_files = set(avito_dir.glob("*.xlsx"))
        
        download_btn.click()
        time.sleep(2)
        
        # Ожидание появления нового файла
        for i in range(30):
            time.sleep(1)

            # Проверка временных файлов xlsx
            temp_files = [f for f in avito_dir.glob("*.crdownload") if "htm" not in f.name.lower()]
            temp_files += [f for f in avito_dir.glob("*.tmp") if "htm" not in f.name.lower()]
            if temp_files:
                continue

            current_files = set(avito_dir.glob("*.xlsx"))
            new_files = current_files - initial_files
            
            if new_files:
                new_file = max(new_files, key=lambda f: f.stat().st_mtime)
                if new_file.stat().st_size > 0:
                    result_file = new_file
                    break
                continue

            # analytics.xlsx мог быть перезаписан — проверяем по времени
            analytics_file = avito_dir / "analytics.xlsx"
            if analytics_file.exists() and analytics_file.stat().st_size > 0:
                if analytics_file.stat().st_mtime >= (time.time() - i - 5):
                    result_file = analytics_file
                    break

        if result_file is None:
            # Последний шанс — берём analytics.xlsx если есть
            analytics_file = avito_dir / "analytics.xlsx"
            if analytics_file.exists() and analytics_file.stat().st_size > 0:
                result_file = analytics_file

        return result_file

    except Exception as e:
        return None

    finally:
        # Удаляем мусорные htm/crdownload файлы
        for pattern in ["*.htm", "*.html", "*.crdownload", "*.tmp"]:
            for junk in avito_dir.glob(pattern):
                try:
                    junk.unlink(missing_ok=True)
                except Exception:
                    pass


def get_avito_monthly_totals(filepath, logger):
    """Получение сумм и количества дней по ПВЗ"""
    try:
        wb = load_workbook(filepath, data_only=True)
        
        target_sheet = None
        for sheet in wb.worksheets:
            if sheet.title == "Расширенная":
                target_sheet = sheet
                break
        
        if not target_sheet:
            return {}, {}, {}
        
        yesterday = (datetime.now() - timedelta(days=1)).date()
        month_start = yesterday.replace(day=1)
        
        target_sheet.delete_rows(1, 3)
        
        pvz_totals = defaultdict(float)
        pvz_dates = defaultdict(set)
        
        for row in target_sheet.iter_rows(min_row=1, values_only=True):
            if not row or len(row) < 10:
                continue
            
            pvz_name = row[1]
            date_val = row[2]
            amount_val = row[9]
            
            if not pvz_name or not amount_val:
                continue
            
            try:
                if isinstance(date_val, datetime):
                    row_date = date_val.date()
                elif isinstance(date_val, str):
                    for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y']:
                        try:
                            row_date = datetime.strptime(date_val, fmt).date()
                            break
                        except ValueError:
                            continue
                    else:
                        continue
                else:
                    continue
                
                if month_start <= row_date <= yesterday:
                    amount = float(amount_val)
                    pvz_name_str = str(pvz_name)
                    pvz_totals[pvz_name_str] += amount
                    pvz_dates[pvz_name_str].add(row_date)
                
            except (ValueError, TypeError):
                continue
        
        totals = {pvz: round(amount) for pvz, amount in pvz_totals.items()}
        days_count = {pvz: len(dates) for pvz, dates in pvz_dates.items()}
        first_day = {pvz: min(dates).day for pvz, dates in pvz_dates.items()}

        return totals, days_count, first_day
        
    except Exception as e:
        return {}, {}, {}


def analyze_avito_report(filepath, logger):
    """Анализ Excel отчёта Авито за вчерашний день"""
    try:
        wb = load_workbook(filepath, data_only=True)
        
        # Ищем лист "Расширенная"
        target_sheet = None
        for sheet in wb.worksheets:
            if sheet.title == "Расширенная":
                target_sheet = sheet
                break
        
        if not target_sheet:
            return {}
        
        # Вчерашняя дата для фильтрации
        yesterday = (datetime.now() - timedelta(days=1)).date()
        
        # Удаляем первые 3 строки
        target_sheet.delete_rows(1, 3)
        
        # Колонки: B=ПВЗ (индекс 1), C=Дата (индекс 2), J=Сумма (индекс 9)
        pvz_totals = defaultdict(float)
        
        for row in target_sheet.iter_rows(min_row=1, values_only=True):
            if not row or len(row) < 10:
                continue
            
            pvz_name = row[1]     # Колонка B
            date_val = row[2]     # Колонка C
            amount_val = row[9]   # Колонка J
            
            if not pvz_name or not amount_val:
                continue
            
            # Фильтр по вчерашней дате
            try:
                if isinstance(date_val, datetime):
                    row_date = date_val.date()
                elif isinstance(date_val, str):
                    for fmt in ['%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y']:
                        try:
                            row_date = datetime.strptime(date_val, fmt).date()
                            break
                        except ValueError:
                            continue
                    else:
                        continue
                else:
                    continue
                
                if row_date != yesterday:
                    continue
                
                amount = float(amount_val)
                pvz_totals[str(pvz_name)] += amount
                
            except (ValueError, TypeError):
                continue
        
        if not pvz_totals:
            return {}
        
        # Округляем суммы
        result = {pvz: round(amount) for pvz, amount in pvz_totals.items()}
        
        return result
        
    except Exception as e:
        raise


def process_avito_report(driver, logger):
    """Основная функция обработки отчёта Авито"""
    avito_dir = REPORTS_DIR / "Авито"
    avito_dir.mkdir(parents=True, exist_ok=True)
    
    # Проверяем существующий файл ЗА СЕГОДНЯ
    today = datetime.now().date()
    final_path = avito_dir / f"{today.day:02d}.{today.month:02d}.{today.year}.xlsx"
    
    if final_path.exists():
        # Анализируем существующий файл
        yesterday = (datetime.now() - timedelta(days=1)).date()
        pvz_data = analyze_avito_report(final_path, logger)
        
        # Получаем суммы, количество дней и первый рабочий день за месяц
        monthly_totals, days_count, first_day = get_avito_monthly_totals(final_path, logger)

        # Среднее за день = сумма_за_месяц / количество_дней_ПВЗ
        avg_data = {pvz: round(total / days_count[pvz]) if days_count.get(pvz, 0) > 0 else 0
                    for pvz, total in monthly_totals.items()}

        # Прогноз = среднее × рабочие дни ПВЗ в месяце
        days_in_month = monthrange(today.year, today.month)[1]
        forecast = {pvz: round(avg * (days_in_month - (first_day.get(pvz, 1) - 1)))
                    for pvz, avg in avg_data.items()}
        
        return {
            'daily': pvz_data,
            'avg': avg_data,
            'forecast': forecast,
            'last_date': yesterday
        }
    
    # Файла нет - скачиваем
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": str(avito_dir)
    })
    
    try:
        driver.get(AVITO_URL)
        time.sleep(3)
        
        if "/gw/login" in driver.current_url:
            if not wait_for_authorization(driver, logger):
                raise Exception("Ошибка авторизации Авито")
        
        # Скачивание отчёта
        downloaded_file = download_avito_report(driver, logger, avito_dir)
        if not downloaded_file:
            raise Exception("Не удалось скачать отчёт")
        
        # Переименовываем в дату вчера
        if downloaded_file.exists():
            downloaded_file.rename(final_path)
        
        # Удаляем лишние файлы
        for pattern in ["downloads.*", "*.htm", "*.html"]:
            for junk in avito_dir.glob(pattern):
                try:
                    junk.unlink(missing_ok=True)
                except Exception:
                    pass
        
        # Анализ за вчерашний день
        yesterday = (datetime.now() - timedelta(days=1)).date()
        pvz_data = analyze_avito_report(final_path, logger)
        
        # Получаем суммы, количество дней и первый рабочий день за месяц
        monthly_totals, days_count, first_day = get_avito_monthly_totals(final_path, logger)

        # Среднее за день
        avg_data = {pvz: round(total / days_count[pvz]) if days_count.get(pvz, 0) > 0 else 0
                    for pvz, total in monthly_totals.items()}

        # Прогноз
        days_in_month = monthrange(today.year, today.month)[1]
        forecast = {pvz: round(avg * (days_in_month - (first_day.get(pvz, 1) - 1)))
                    for pvz, avg in avg_data.items()}
        
        return {
            'daily': pvz_data,
            'avg': avg_data,
            'forecast': forecast,
            'last_date': yesterday
        }
        
    except Exception as e:
        raise
