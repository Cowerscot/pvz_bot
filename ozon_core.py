# ozon_core.py - Ядро обработки отчётов Ozon

import time
import re
from datetime import datetime, timedelta
from pathlib import Path
from calendar import monthrange

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

from config import OZON_BASE_URL, OZON_REPORTS_URL, REPORTS_DIR


def get_date_with_offset(days_offset=0):
    """Получить дату со смещением"""
    target_date = datetime.now() - timedelta(days=abs(days_offset))
    return target_date.strftime("%d.%m.%Y")


def get_month_start_date():
    """Получить 1 число текущего месяца"""
    now = datetime.now()
    return now.replace(day=1).strftime("%d.%m.%Y")


def get_days_in_current_month():
    """Получить количество дней в текущем месяце"""
    now = datetime.now()
    return monthrange(now.year, now.month)[1]


def calculate_forecast(pvz_data, days_passed):
    """Рассчитать прогноз на месяц"""
    days_in_month = get_days_in_current_month()
    current_day = datetime.now().day
    days_remaining = days_in_month - current_day
    forecast = {}
    for pvz, amount in pvz_data.items():
        daily_avg = amount / days_passed if days_passed > 0 else 0
        forecast[pvz] = amount + (daily_avg * days_remaining)
    return forecast


def close_modals(driver, logger):
    """Закрытие модальных окон"""
    closed_something = False
    
    try:
        selectors = [
            "//button[contains(text(), 'Отложить')]",
            "//button[contains(., 'Отложить')]",
            "//button[.//span[contains(text(), 'Отложить')]]"
        ]
        for selector in selectors:
            buttons = driver.find_elements(By.XPATH, selector)
            if buttons:
                for btn in buttons:
                    try:
                        if btn.is_displayed():
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(1)
                            closed_something = True
                            break
                    except:
                        continue
            if closed_something:
                break
    except:
        pass
    
    if not closed_something:
        try:
            close_buttons = driver.find_elements(
                By.XPATH,
                "//button[@aria-label='Закрыть'] | //button[contains(@class, 'close')]"
            )
            if close_buttons:
                for btn in close_buttons:
                    try:
                        if btn.is_displayed():
                            driver.execute_script("arguments[0].click();", btn)
                            time.sleep(1)
                            closed_something = True
                            break
                    except:
                        continue
        except:
            pass
    
    return closed_something


def wait_for_authorization(driver, logger, timeout=300):
    """Ожидание авторизации Озон"""
    start = time.time()
    
    while time.time() - start < timeout:
        url = driver.current_url
        if "sso.ozon.ru" not in url and "/login" not in url and "turbo-pvz.ozon.ru" in url:
            return True
        time.sleep(2)
    
    return False


def select_pvz(driver, logger):
    """Выбор ПВЗ Озон - пропускаем если один ПВЗ"""
    if "turbo-pvz.ozon.ru" in driver.current_url and "/stores" not in driver.current_url:
        return
    
    if "/stores" in driver.current_url:
        try:
            wait = WebDriverWait(driver, 5)
            pvz_title = wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//*[starts-with(normalize-space(text()), 'МОСКВА_')]")
                )
            )
            pvz_card = pvz_title.find_element(By.XPATH, "./ancestor::div[3]")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", pvz_card)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", pvz_card)
            time.sleep(2)
            return
        except:
            return


def select_date_range(driver, logger, start_date, end_date):
    """Выбор диапазона дат"""
    xpaths = [
        "//input[@id='input___v-0-4']",
        "//input[starts-with(@id, 'input___v-0-')]",
        "//input[contains(@placeholder,'ДД.МM.ГГГГ') or contains(@placeholder,'дд.мм.гггг') or contains(@placeholder,'ДД') or contains(@placeholder,'дд')]"
    ]
    period_input = None
    for xp in xpaths:
        try:
            period_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, xp))
            )
            break
        except:
            continue
    if not period_input:
        return False
    
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", period_input)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", period_input)
    time.sleep(1)
    driver.execute_script("arguments[0].value = '';", period_input)
    time.sleep(0.3)
    date_string = f"{start_date} – {end_date}"
    period_input.send_keys(date_string)
    time.sleep(1)
    return True


def create_report(driver, logger):
    """Создание отчёта"""
    try:
        create_report_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "._dialogActionBtns_szei1_8 > .ozi__button__primary__xYxcE > .ozi__button__content__xYxcE")
            )
        )
    except:
        try:
            create_report_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "_dialogActionBtns_szei1_8 .ozi__button__primary__5UTJi .ozi__button__content__5UTJi")
                )
            )
        except:
            create_report_btn = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "(//button[contains(@class, 'button') and contains(@class, 'primary')])[last()]")
                )
            )
    
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", create_report_btn)
    time.sleep(0.5)
    driver.execute_script("arguments[0].click();", create_report_btn)
    time.sleep(3)
    
    try:
        error_notification = driver.find_elements(By.CSS_SELECTOR, ".ozi__notification-template__shown__L414N")
        if error_notification:
            for notif in error_notification:
                if notif.is_displayed():
                    try:
                        close_btn = notif.find_element(By.CSS_SELECTOR, ".ozi__icon__cursor__pvBjg")
                        driver.execute_script("arguments[0].click();", close_btn)
                    except:
                        pass
                    time.sleep(1)
                    return "error"
    except:
        pass
    
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "._warning_zf76z_333"))
        )
        return "created"
    except:
        pass

    return "error"


def wait_for_report_ready(driver, logger, start_date, end_date, timeout=600):
    """Ожидание готовности отчёта"""
    period_text = f"{start_date} - {end_date}"
    start_time = time.time()
    last_log = 0
    
    time.sleep(5)
    
    while time.time() - start_time < timeout:
        try:
            driver.refresh()
            time.sleep(3)
            
            rows = driver.find_elements(By.XPATH, "//tr | //*[contains(@class, 'row')]")
            target_row = None
            
            for row in rows:
                if period_text in row.text:
                    target_row = row
                    break
            
            if not target_row:
                elapsed = int(time.time() - start_time)
                if elapsed - last_log >= 30:
                    last_log = elapsed
                time.sleep(10)
                continue
            
            if "Формируется" in target_row.text:
                elapsed = int(time.time() - start_time)
                if elapsed - last_log >= 30:
                    last_log = elapsed
                time.sleep(10)
                continue
            
            if "В архиве" in target_row.text:
                buttons = target_row.find_elements(By.TAG_NAME, "button")
                if buttons:
                    return buttons[-1]
            
            buttons = target_row.find_elements(By.TAG_NAME, "button")
            if buttons:
                return buttons[-1]
            
            time.sleep(5)
            
        except:
            time.sleep(5)
    
    return None


def download_turnover_report(driver, logger, download_btn, start_date, end_date, reports_dir):
    """Скачивание PDF отчёта"""
    logger.info(f"  🖱️ Клик по кнопке скачивания")

    clicked = False
    try:
        download_btn.click()
        clicked = True
    except Exception as e1:
        pass
    
    if not clicked:
        try:
            ActionChains(driver).move_to_element(download_btn).click().perform()
            clicked = True
        except Exception as e2:
            pass
    
    if not clicked:
        try:
            driver.execute_script("arguments[0].click();", download_btn)
            clicked = True
        except Exception as e3:
            return None

    time.sleep(2)
    
    turnover_element = None
    wait = WebDriverWait(driver, 10)

    dropdown_css_variants = [
        ".ozi__dropdown-item__dataContent__mpRrQ",
        "[class*='dropdown-item__dataContent']",
        "[class*='dropdown-item'] [class*='dataContent']",
        "[class*='dropdown-item']",
    ]
    for css in dropdown_css_variants:
        try:
            turnover_element = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, css))
            )
            break
        except:
            continue

    if not turnover_element:
        xpath_variants = [
            "//*[normalize-space(text())='Оборот']",
            "//*[contains(text(), 'Оборот')]",
            "//li[contains(text(), 'Оборот')]",
            "//span[contains(text(), 'Оборот')]",
            "//div[contains(text(), 'Оборот')]",
        ]
        for xp in xpath_variants:
            try:
                turnover_element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xp))
                )
                break
            except:
                continue

    if not turnover_element:
        return None

    try:
        driver.execute_script("arguments[0].click();", turnover_element)
    except Exception:
        try:
            turnover_element.click()
        except Exception as e:
            return None
    
    initial_files = set(reports_dir.glob("*.pdf"))
    
    for attempt in range(30):
        time.sleep(1)
        current_files = set(reports_dir.glob("*.pdf"))
        new_files = current_files - initial_files
        
        temp_files = list(reports_dir.glob("*.crdownload"))
        if temp_files:
            if attempt % 5 == 0:
                pass
            continue
        
        if new_files:
            pdf_path = max(new_files, key=lambda f: f.stat().st_mtime)
            if pdf_path.stat().st_size > 0:
                final_name = f"{start_date}-{end_date}.pdf"
                new_path = reports_dir / final_name
                
                if new_path.exists():
                    new_path.unlink()
                
                pdf_path.rename(new_path)
                return str(new_path)
    
    return None


def extract_pvz_data_from_pdf(pdf_path, logger):
    """Извлечение данных из PDF"""
    try:
        import warnings
        warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
        import pdfplumber
        
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            return None
        
        with pdfplumber.open(pdf_file) as pdf:
            all_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        
        sections = re.split(r'(МОСКВА_\d+)', all_text)
        pvz_results = {}
        i = 1
        
        while i < len(sections) - 1:
            pvz_name = sections[i].strip()
            if not re.match(r'МОСКВА_\d+', pvz_name):
                i += 1
                continue
            
            block = sections[i + 1]
            total_match = re.search(r'Итого по СД:\s*([\d\s,]+\.?\d*)', block)
            if total_match:
                amount_str = total_match.group(1).replace(' ', '').replace(',', '.')
                try:
                    amount = float(amount_str)
                    pvz_results[pvz_name] = amount
                except ValueError:
                    pass
            i += 2
        
        return pvz_results
    
    except ImportError:
        return None
    except Exception as e:
        return None


def find_report_by_date(driver, logger, start_date, end_date):
    """Поиск отчёта по периоду"""
    period_text = f"{start_date} - {end_date}"
    
    try:
        time.sleep(2)
        rows = driver.find_elements(By.XPATH, "//tr | //*[contains(@class, 'row')]")
        
        for row in rows:
            try:
                if period_text in row.text:
                    return row
            except:
                continue
        
        return None
    except Exception as e:
        return None


def get_download_button(driver, row, logger):
    """Получение кнопки скачивания из строки"""
    try:
        btn = row.find_element(
            By.CSS_SELECTOR,
            ".ozi__popover__fixReferenceSize__xaASc button"
        )
        return btn
    except:
        pass
    try:
        inner = row.find_element(
            By.CSS_SELECTOR,
            "[class*='truncate']"
        )
        btn = inner.find_element(By.XPATH, "./ancestor::button[1]")
        return btn
    except:
        pass
    try:
        btn = row.find_element(
            By.XPATH,
            ".//button[contains(@aria-label, 'Скачать') or .//span[contains(text(), 'Скачать')]]"
        )
        return btn
    except:
        pass
    try:
        btn = driver.find_element(
            By.CSS_SELECTOR,
            ".ozi__table-row__row__ZiaPl:nth-child(2) .ozi__popover__fixReferenceSize__xaASc button"
        )
        return btn
    except:
        pass
    try:
        buttons = row.find_elements(By.TAG_NAME, "button")
        if buttons:
            return buttons[-1]
    except:
        pass
    return None


def load_or_create_report(driver, logger, start_date, end_date, reports_dir):
    """Загрузка существующего отчёта или создание нового"""
    existing_report = find_report_by_date(driver, logger, start_date, end_date)
    
    pdf_path = None
    
    if existing_report:
        download_btn = get_download_button(driver, existing_report, logger)
        if not download_btn:
            return None
        pdf_path = download_turnover_report(driver, logger, download_btn, start_date, end_date, reports_dir)
    else:
        close_modals(driver, logger)
        
        create_btn = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.XPATH, "//button[contains(., 'Создать')]")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", create_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", create_btn)
        time.sleep(2)
        
        if not select_date_range(driver, logger, start_date, end_date):
            return None
        
        report_status = create_report(driver, logger)
        if report_status in ("not_ready", "error"):
            return None
        
        driver.get(OZON_REPORTS_URL)
        time.sleep(2)
        
        download_btn = wait_for_report_ready(driver, logger, start_date, end_date, timeout=600)
        if not download_btn:
            return None
        
        pdf_path = download_turnover_report(driver, logger, download_btn, start_date, end_date, reports_dir)
    
    if not pdf_path:
        return None
    
    pvz_data = extract_pvz_data_from_pdf(pdf_path, logger)
    if pvz_data is None:
        return None
    
    driver.get(OZON_REPORTS_URL)
    time.sleep(2)
    
    return pvz_data


def process_ozon_report(driver, logger):
    """Основная функция обработки отчёта Озон"""
    ozon_dir = REPORTS_DIR / "Озон"
    ozon_dir.mkdir(parents=True, exist_ok=True)
    
    now = datetime.now()
    date_offset = 1
    month_start = get_month_start_date()
    date_prev = get_date_with_offset(date_offset + 1)
    date_last = get_date_with_offset(date_offset)
    
    pdf1_name = f"{month_start}-{date_prev}.pdf"
    pdf2_name = f"{month_start}-{date_last}.pdf"
    pdf1_path = ozon_dir / pdf1_name
    pdf2_path = ozon_dir / pdf2_name
    
    if pdf1_path.exists() and pdf2_path.exists():
        report1_data = extract_pvz_data_from_pdf(str(pdf1_path), logger)
        report2_data = extract_pvz_data_from_pdf(str(pdf2_path), logger)
        
        if report1_data is not None and report2_data is not None:
            daily_av = {}
            for pvz in report2_data:
                amount2 = report2_data.get(pvz, 0)
                amount1 = report1_data.get(pvz, 0)
                daily_av[pvz] = amount2 - amount1
            
            days_passed = now.day - date_offset
            avg_av = {pvz: report2_data[pvz] / days_passed if days_passed > 0 else 0 for pvz in report2_data}
            forecast = calculate_forecast(report2_data, days_passed)
            
            return {
                "daily": daily_av,
                "avg": avg_av,
                "forecast": forecast
            }
    
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": str(ozon_dir)
    })
    
    try:
        driver.get(OZON_BASE_URL)
        time.sleep(3)
        
        if "sso.ozon.ru" in driver.current_url or "/login" in driver.current_url:
            if not wait_for_authorization(driver, logger):
                raise Exception("Ошибка авторизации Озон")
        
        select_pvz(driver, logger)
        
        driver.get(OZON_REPORTS_URL)
        WebDriverWait(driver, 30).until(EC.url_contains("/reports/subagent"))
        time.sleep(2)
        
        close_modals(driver, logger)
        time.sleep(1)
        
        report2_data = load_or_create_report(driver, logger, month_start, date_last, ozon_dir)
        
        if report2_data is None:
            raise Exception("Не удалось загрузить отчёт 2")
        
        report1_data = load_or_create_report(driver, logger, month_start, date_prev, ozon_dir)
        
        if report1_data is None:
            raise Exception("Не удалось загрузить отчёт 1")
        
        daily_av = {}
        for pvz in report2_data:
            amount2 = report2_data.get(pvz, 0)
            amount1 = report1_data.get(pvz, 0)
            daily_av[pvz] = amount2 - amount1
        
        days_passed = now.day - date_offset
        avg_av = {pvz: report2_data[pvz] / days_passed if days_passed > 0 else 0 for pvz in report2_data}
        forecast = calculate_forecast(report2_data, days_passed)
        
        return {
            "daily": daily_av,
            "avg": avg_av,
            "forecast": forecast
        }
        
    except Exception as e:
        raise
