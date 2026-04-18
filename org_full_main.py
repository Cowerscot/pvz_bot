# full_main.py

import time
from datetime import datetime, timedelta

from full_config import (
    setup_logger, create_driver, save_snapshot,
    send_or_update_telegram_message, get_message_id, save_message_id,
    send_vk_message, VK_PEER_ID, get_vk_message_id, save_vk_message_id,
    REPORTS_DIR
)
from yandex_core import process_yandex_report
from ozon_core import process_ozon_report
from avito_core import process_avito_report


def format_combined_report(yandex_data, ozon_data, avito_data, report_date, expenses):
    """Форматирование объединённого отчёта"""

    def format_amount(amount):
        if amount is None:
            return "—"
        return f"{int(round(amount)):,}".replace(',', ' ')

    def get_date_suffix(data_date, current_date):
        """Добавляет дату в скобках если данные не за вчера"""
        if not data_date:
            return ""
        delta = (current_date - data_date).days
        if delta != 1:
            months = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
            return f" ({data_date.day} {months[data_date.month - 1]})"
        return ""

    current_date = datetime.now().date()
    current_time = datetime.now().strftime("%H:%M")

    lines = []
    lines.append(f"📅 Дата: {report_date} {current_time}")
    lines.append("─────────────")

    # === СУММА ===
    ozon_sum = sum(ozon_data['daily'].values()) if ozon_data and 'daily' in ozon_data else 0

    yandex_sum = 0
    if yandex_data and 'pvz_data' in yandex_data:
        yandex_sum = sum(pvz['last_amount'] for pvz in yandex_data['pvz_data'].values())

    avito_sum = sum(avito_data['daily'].values()) if avito_data and 'daily' in avito_data else 0

    total_sum = yandex_sum + ozon_sum + avito_sum

    lines.append(f"<b>💰 Сумма: {format_amount(total_sum)} ₽</b>")

    if ozon_data and 'daily' in ozon_data:
        ozon_lines = []
        for pvz, amount in sorted(ozon_data['daily'].items()):
            ozon_lines.append(f"{pvz}: {format_amount(amount)} ₽")
        lines.append(f"<span class='tg-spoiler'>Озон:\n{chr(10).join(ozon_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Озон: ⏳ ожидается</span>")

    if yandex_data and 'pvz_data' in yandex_data:
        yandex_lines = []
        yandex_date = yandex_data.get('last_date')
        date_suffix = get_date_suffix(yandex_date, current_date)
        for pvz_id, pvz_data in sorted(yandex_data['pvz_data'].items()):
            pvz_id_clean = str(int(float(pvz_id))) if isinstance(pvz_id, (int, float, str)) else pvz_id
            yandex_lines.append(f"ID_{pvz_id_clean}: {format_amount(pvz_data['last_amount'])} ₽")
        lines.append(f"<span class='tg-spoiler'>Яндекс:{date_suffix}\n{chr(10).join(yandex_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Яндекс: ⏳ ожидается</span>")

    if avito_data and 'daily' in avito_data:
        avito_lines = []
        avito_date = avito_data.get('last_date')
        date_suffix = get_date_suffix(avito_date, current_date)
        for pvz, amount in sorted(avito_data['daily'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            avito_lines.append(f"{pvz_short}: {format_amount(amount)} ₽")
        lines.append(f"<span class='tg-spoiler'>Авито:{date_suffix}\n{chr(10).join(avito_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Авито: ⏳ ожидается</span>")
    lines.append("─────────────")

    # === СРЕДНЕЕ ===
    ozon_avg = sum(ozon_data['avg'].values()) if ozon_data and 'avg' in ozon_data else 0

    yandex_avg = 0
    if yandex_data and 'pvz_data' in yandex_data:
        yandex_avg = sum(pvz['avg_daily'] for pvz in yandex_data['pvz_data'].values())

    avito_avg = sum(avito_data['avg'].values()) if avito_data and 'avg' in avito_data else 0

    total_avg = yandex_avg + ozon_avg + avito_avg

    lines.append(f"<b>📈 Среднее: {format_amount(total_avg)} ₽</b>")

    if ozon_data and 'avg' in ozon_data:
        ozon_lines = []
        for pvz, amount in sorted(ozon_data['avg'].items()):
            ozon_lines.append(f"{pvz}: {format_amount(amount)} ₽")
        lines.append(f"<span class='tg-spoiler'>Озон:\n{chr(10).join(ozon_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Озон: ⏳ ожидается</span>")

    if yandex_data and 'pvz_data' in yandex_data:
        yandex_lines = []
        yandex_date = yandex_data.get('last_date')
        date_suffix = get_date_suffix(yandex_date, current_date)
        for pvz_id, pvz_data in sorted(yandex_data['pvz_data'].items()):
            pvz_id_clean = str(int(float(pvz_id))) if isinstance(pvz_id, (int, float, str)) else pvz_id
            yandex_lines.append(f"ID_{pvz_id_clean}: {format_amount(pvz_data['avg_daily'])} ₽")
        lines.append(f"<span class='tg-spoiler'>Яндекс:{date_suffix}\n{chr(10).join(yandex_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Яндекс: ⏳ ожидается</span>")

    if avito_data and 'avg' in avito_data:
        avito_lines = []
        avito_date = avito_data.get('last_date')
        date_suffix = get_date_suffix(avito_date, current_date)
        for pvz, amount in sorted(avito_data['avg'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            avito_lines.append(f"{pvz_short}: {format_amount(amount)} ₽")
        lines.append(f"<span class='tg-spoiler'>Авито:{date_suffix}\n{chr(10).join(avito_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Авито: ⏳ ожидается</span>")
    lines.append("─────────────")

    # === ПРОГНОЗ ===
    ozon_forecast = sum(ozon_data['forecast'].values()) if ozon_data and 'forecast' in ozon_data else 0

    yandex_forecast = 0
    if yandex_data and 'pvz_data' in yandex_data:
        yandex_forecast = sum(pvz['forecast'] for pvz in yandex_data['pvz_data'].values())

    avito_forecast = sum(avito_data['forecast'].values()) if avito_data and 'forecast' in avito_data else 0

    total_forecast = yandex_forecast + ozon_forecast + avito_forecast

    lines.append(f"<b>🔮 Прогноз: {format_amount(total_forecast)} ₽</b>")

    if ozon_data and 'forecast' in ozon_data:
        ozon_lines = []
        for pvz, amount in sorted(ozon_data['forecast'].items()):
            ozon_lines.append(f"{pvz}: {format_amount(amount)} ₽")
        lines.append(f"<span class='tg-spoiler'>Озон:\n{chr(10).join(ozon_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Озон: ⏳ ожидается</span>")

    if yandex_data and 'pvz_data' in yandex_data:
        yandex_lines = []
        yandex_date = yandex_data.get('last_date')
        date_suffix = get_date_suffix(yandex_date, current_date)
        for pvz_id, pvz_data in sorted(yandex_data['pvz_data'].items()):
            pvz_id_clean = str(int(float(pvz_id))) if isinstance(pvz_id, (int, float, str)) else pvz_id
            yandex_lines.append(f"ID_{pvz_id_clean}: {format_amount(pvz_data['forecast'])} ₽")
        lines.append(f"<span class='tg-spoiler'>Яндекс:{date_suffix}\n{chr(10).join(yandex_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Яндекс: ⏳ ожидается</span>")

    if avito_data and 'forecast' in avito_data:
        avito_lines = []
        avito_date = avito_data.get('last_date')
        date_suffix = get_date_suffix(avito_date, current_date)
        for pvz, amount in sorted(avito_data['forecast'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            avito_lines.append(f"{pvz_short}: {format_amount(amount)} ₽")
        lines.append(f"<span class='tg-spoiler'>Авито:{date_suffix}\n{chr(10).join(avito_lines)}</span>")
    else:
        lines.append("<span class='tg-spoiler'>Авито: ⏳ ожидается</span>")
    lines.append("─────────────")

    # === РАСХОДЫ ===
    from calendar import monthrange

    now = datetime.now()
    days_in_month = monthrange(now.year, now.month)[1]

    rent = expenses["аренда"]
    utilities = expenses["комуналка"]
    salary = expenses["зарплата_день"] * days_in_month
    other = expenses["прочее"]
    tax_percent = expenses.get("налог_процент", 6)
    tax = int(total_forecast * tax_percent / 100)

    total_expenses = rent + utilities + salary + tax + other

    lines.append(f"<b>💸 Расходы: {format_amount(total_expenses)} ₽</b>")
    lines.append(f"<span class='tg-spoiler'>Аренда: {format_amount(rent)} ₽")
    lines.append(f"Ком. услуги: {format_amount(utilities)} ₽")
    lines.append(f"ФОТ: {format_amount(salary)} ₽ ({days_in_month} дн.)")
    lines.append(f"Налоги: {format_amount(tax)} ₽ ({tax_percent}%)")
    lines.append(f"Прочее: {format_amount(other)} ₽</span>")
    lines.append("─────────────")

    # === ПРИБЫЛЬ ===
    net_profit = total_forecast - total_expenses
    lines.append(f"<b>💵 Прогноз прибыли: {format_amount(net_profit)} ₽</b>")

    return "\n".join(lines)


def format_vk_report(yandex_data, ozon_data, avito_data, report_date, expenses):
    """Форматирование отчёта для ВКонтакте"""

    def fmt(amount):
        if amount is None:
            return "—"
        return f"{int(round(amount)):,}".replace(',', ' ')

    def date_suffix(data_date):
        if not data_date:
            return ""
        current = datetime.now().date()
        delta = (current - data_date).days
        if delta != 1:
            months = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
            return f" ({data_date.day} {months[data_date.month - 1]})"
        return ""

    from calendar import monthrange
    now = datetime.now()
    current_time = now.strftime("%H:%M")
    days_in_month = monthrange(now.year, now.month)[1]

    # === СУММЫ ===
    ozon_sum = sum(ozon_data['daily'].values()) if ozon_data and 'daily' in ozon_data else 0
    yandex_sum = sum(p['last_amount'] for p in yandex_data['pvz_data'].values()) if yandex_data and 'pvz_data' in yandex_data else 0
    avito_sum = sum(avito_data['daily'].values()) if avito_data and 'daily' in avito_data else 0
    total_sum = yandex_sum + ozon_sum + avito_sum

    ozon_avg = sum(ozon_data['avg'].values()) if ozon_data and 'avg' in ozon_data else 0
    yandex_avg = sum(p['avg_daily'] for p in yandex_data['pvz_data'].values()) if yandex_data and 'pvz_data' in yandex_data else 0
    avito_avg = sum(avito_data['avg'].values()) if avito_data and 'avg' in avito_data else 0
    total_avg = yandex_avg + ozon_avg + avito_avg

    ozon_forecast = sum(ozon_data['forecast'].values()) if ozon_data and 'forecast' in ozon_data else 0
    yandex_forecast = sum(p['forecast'] for p in yandex_data['pvz_data'].values()) if yandex_data and 'pvz_data' in yandex_data else 0
    avito_forecast = sum(avito_data['forecast'].values()) if avito_data and 'forecast' in avito_data else 0
    total_forecast = yandex_forecast + ozon_forecast + avito_forecast

    # === РАСХОДЫ ===
    rent = expenses["аренда"]
    utilities = expenses["комуналка"]
    salary = expenses["зарплата_день"] * days_in_month
    other = expenses["прочее"]
    tax_percent = expenses.get("налог_процент", 6)
    tax = int(total_forecast * tax_percent / 100)
    total_expenses = rent + utilities + salary + tax + other
    net_profit = total_forecast - total_expenses

    lines = []
    lines.append(f"📅 Дата: {report_date} {current_time}")
    lines.append("─────────────")

    # СУММА
    lines.append(f"💰 СУММА: {fmt(total_sum)} ₽")
    if ozon_data and 'daily' in ozon_data:
        lines.append("  Озон:")
        for pvz, amount in sorted(ozon_data['daily'].items()):
            lines.append(f"    {pvz}: {fmt(amount)} ₽")
    if yandex_data and 'pvz_data' in yandex_data:
        ds = date_suffix(yandex_data.get('last_date'))
        lines.append(f"  Яндекс:{ds}")
        for pvz_id, pvz in sorted(yandex_data['pvz_data'].items()):
            pid = str(int(float(pvz_id)))
            lines.append(f"    ID_{pid}: {fmt(pvz['last_amount'])} ₽")
    if avito_data and 'daily' in avito_data:
        ds = date_suffix(avito_data.get('last_date'))
        lines.append(f"  Авито:{ds}")
        for pvz, amount in sorted(avito_data['daily'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            lines.append(f"    {pvz_short}: {fmt(amount)} ₽")
    lines.append("─────────────")

    # СРЕДНЕЕ
    lines.append(f"📈 СРЕДНЕЕ: {fmt(total_avg)} ₽")
    if ozon_data and 'avg' in ozon_data:
        lines.append("  Озон:")
        for pvz, amount in sorted(ozon_data['avg'].items()):
            lines.append(f"    {pvz}: {fmt(amount)} ₽")
    if yandex_data and 'pvz_data' in yandex_data:
        ds = date_suffix(yandex_data.get('last_date'))
        lines.append(f"  Яндекс:{ds}")
        for pvz_id, pvz in sorted(yandex_data['pvz_data'].items()):
            pid = str(int(float(pvz_id)))
            lines.append(f"    ID_{pid}: {fmt(pvz['avg_daily'])} ₽")
    if avito_data and 'avg' in avito_data:
        ds = date_suffix(avito_data.get('last_date'))
        lines.append(f"  Авито:{ds}")
        for pvz, amount in sorted(avito_data['avg'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            lines.append(f"    {pvz_short}: {fmt(amount)} ₽")
    lines.append("─────────────")

    # ПРОГНОЗ
    lines.append(f"🔮 ПРОГНОЗ: {fmt(total_forecast)} ₽")
    if ozon_data and 'forecast' in ozon_data:
        lines.append("  Озон:")
        for pvz, amount in sorted(ozon_data['forecast'].items()):
            lines.append(f"    {pvz}: {fmt(amount)} ₽")
    if yandex_data and 'pvz_data' in yandex_data:
        ds = date_suffix(yandex_data.get('last_date'))
        lines.append(f"  Яндекс:{ds}")
        for pvz_id, pvz in sorted(yandex_data['pvz_data'].items()):
            pid = str(int(float(pvz_id)))
            lines.append(f"    ID_{pid}: {fmt(pvz['forecast'])} ₽")
    if avito_data and 'forecast' in avito_data:
        ds = date_suffix(avito_data.get('last_date'))
        lines.append(f"  Авито:{ds}")
        for pvz, amount in sorted(avito_data['forecast'].items()):
            pvz_short = pvz[:20] + '...' if len(pvz) > 20 else pvz
            lines.append(f"    {pvz_short}: {fmt(amount)} ₽")
    lines.append("─────────────")

    # РАСХОДЫ
    lines.append(f"💸 РАСХОДЫ: {fmt(total_expenses)} ₽")
    lines.append(f"  Аренда: {fmt(rent)} ₽")
    lines.append(f"  Ком. услуги: {fmt(utilities)} ₽")
    lines.append(f"  ФОТ: {fmt(salary)} ₽ ({days_in_month} дн.)")
    lines.append(f"  Налоги: {fmt(tax)} ₽ ({tax_percent}%)")
    lines.append(f"  Прочее: {fmt(other)} ₽")
    lines.append("─────────────")

    # ПРИБЫЛЬ
    lines.append(f"💵 ПРОГНОЗ ПРИБЫЛИ: {fmt(net_profit)} ₽")

    return "\n".join(lines)


def load_existing_reports(logger):
    """Проверяет наличие файлов отчетов

    Returns:
        tuple: (has_yandex, has_ozon, has_avito) - bool флаги наличия файлов
    """
    now = datetime.now()
    today = now.date()

    # Яндекс - XLSX за СЕГОДНЯ
    yandex_filename = f"{today.day:02d}.{today.month:02d}.{today.year}.xlsx"
    yandex_file = REPORTS_DIR / "Яндекс" / yandex_filename
    has_yandex = yandex_file.exists()

    if has_yandex:
        logger.info(f"  Яндекс: ✓ {yandex_filename}")
    else:
        logger.info(f"  Яндекс: ✗ {yandex_filename}")

    # Озон - нужны ОБА PDF: с 1 числа по позавчера И по вчера
    month_start = f"01.{now.month:02d}.{now.year}"
    date_prev = (now - timedelta(days=2)).strftime("%d.%m.%Y")  # позавчера
    date_last = (now - timedelta(days=1)).strftime("%d.%m.%Y")  # вчера

    pdf1_name = f"{month_start}-{date_prev}.pdf"
    pdf2_name = f"{month_start}-{date_last}.pdf"

    pdf1_exists = (REPORTS_DIR / "Озон" / pdf1_name).exists()
    pdf2_exists = (REPORTS_DIR / "Озон" / pdf2_name).exists()

    has_ozon = pdf1_exists and pdf2_exists

    logger.info(f"  Озон PDF1: {'✓' if pdf1_exists else '✗'} {pdf1_name}")
    logger.info(f"  Озон PDF2: {'✓' if pdf2_exists else '✗'} {pdf2_name}")

    # Авито - XLSX за сегодня
    avito_filename = f"{today.day:02d}.{today.month:02d}.{today.year}.xlsx"
    avito_file = REPORTS_DIR / "Авито" / avito_filename
    has_avito = avito_file.exists()

    if has_avito:
        logger.info(f"  Авито: ✓ {avito_filename}")
    else:
        logger.info(f"  Авито: ✗ {avito_filename}")

    return has_yandex, has_ozon, has_avito


def main():
    logger = setup_logger("main")
    driver = None

    yandex_data = None
    ozon_data = None
    avito_data = None

    # Форматируем дату как "13 янв"
    now = datetime.now()
    months = ['янв', 'фев', 'мар', 'апр', 'мая', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
    report_date = f"{now.day} {months[now.month - 1]}"

    try:
        logger.info(f"🚀 Запуск {datetime.now().strftime('%H:%M:%S')}")

        # Создаём директории для отчётов
        (REPORTS_DIR / "Яндекс").mkdir(parents=True, exist_ok=True)
        (REPORTS_DIR / "Озон").mkdir(parents=True, exist_ok=True)
        (REPORTS_DIR / "Авито").mkdir(parents=True, exist_ok=True)

        # ПЕРВЫМ ДЕЛОМ - проверяем файлы отчетов
        logger.info("📊 Проверка файлов отчетов...")
        has_yandex, has_ozon, has_avito = load_existing_reports(logger)

        # Определяем что нужно загрузить
        need_yandex = not has_yandex
        need_ozon = not has_ozon
        need_avito = not has_avito

        logger.info("\nСтатус:")
        logger.info(f"  Яндекс: {'✓ есть' if has_yandex else '❌ загрузка'}")
        logger.info(f"  Озон: {'✓ есть' if has_ozon else '❌ загрузка'}")
        logger.info(f"  Авито: {'✓ есть' if has_avito else '❌ загрузка'}")

        # Запускаем браузер только если нужна загрузка
        need_download = need_yandex or need_ozon or need_avito

        if need_download:
            logger.info("\n🌐 Запуск браузера...")
            driver = create_driver()
        else:
            logger.info("\n✅ Все файлы найдены, пропускаем загрузку")

        message_id = get_message_id(report_date)
        from full_config import EXPENSES

        # === ЯНДЕКС ===
        if need_download and need_yandex:
            logger.info("📦 Загрузка Яндекс...")
            try:
                yandex_data = process_yandex_report(driver, logger)
                logger.info("✅ Яндекс загружен")
            except Exception as e:
                logger.error(f"❌ Яндекс: {e}")
                save_snapshot(driver, logger, "yandex_error")

        # === ОЗОН ===
        if need_download and need_ozon:
            logger.info("📦 Загрузка Озон...")
            try:
                ozon_data = process_ozon_report(driver, logger)
                logger.info("✅ Озон загружен")
            except Exception as e:
                logger.error(f"❌ Озон: {e}")
                save_snapshot(driver, logger, "ozon_error")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_driver()

        # === АВИТО ===
        if need_download and need_avito:
            logger.info("📦 Загрузка Авито...")
            try:
                avito_data = process_avito_report(driver, logger)
                logger.info("✅ Авито загружен")
            except Exception as e:
                logger.error(f"❌ Авито: {e}")
                save_snapshot(driver, logger, "avito_error")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = create_driver()

        # === ЧТЕНИЕ ДАННЫХ ИЗ ФАЙЛОВ ===
        logger.info("\n📊 Анализ отчётов...")

        # Яндекс - ВСЕГДА читаем файл
        from yandex_core import analyze_report
        try:
            for days_back in range(3):
                check_date = now.date() - timedelta(days=days_back)
                filename = f"{check_date.day:02d}.{check_date.month:02d}.{check_date.year}.xlsx"
                filepath = REPORTS_DIR / "Яндекс" / filename
                if filepath.exists():
                    yandex_data = analyze_report(filepath, logger)
                    break
        except Exception as e:
            logger.error(f"Ошибка анализа Яндекс: {e}")

        # Озон - ВСЕГДА читаем файлы
        from ozon_core import extract_pvz_data_from_pdf, calculate_forecast
        try:
            month_start = f"01.{now.month:02d}.{now.year}"
            date_prev = (now - timedelta(days=2)).strftime("%d.%m.%Y")
            date_last = (now - timedelta(days=1)).strftime("%d.%m.%Y")

            pdf1_name = f"{month_start}-{date_prev}.pdf"
            pdf2_name = f"{month_start}-{date_last}.pdf"

            pdf1_path = REPORTS_DIR / "Озон" / pdf1_name
            pdf2_path = REPORTS_DIR / "Озон" / pdf2_name

            if pdf1_path.exists() and pdf2_path.exists():
                report1_data = extract_pvz_data_from_pdf(str(pdf1_path), logger)
                report2_data = extract_pvz_data_from_pdf(str(pdf2_path), logger)

                if report1_data is not None and report2_data is not None:
                    daily_av = {}
                    for pvz in report2_data:
                        amount2 = report2_data.get(pvz, 0)
                        amount1 = report1_data.get(pvz, 0)
                        daily_av[pvz] = amount2 - amount1

                    days_passed = now.day - 1
                    avg_av = {pvz: report2_data[pvz] / days_passed if days_passed > 0 else 0
                              for pvz in report2_data}

                    # Озон: отчёт всегда с 1 числа, first_day=1 для всех ПВЗ
                    forecast = calculate_forecast(report2_data, days_passed)

                    ozon_data = {
                        "daily": daily_av,
                        "avg": avg_av,
                        "forecast": forecast
                    }
        except Exception as e:
            logger.error(f"Ошибка анализа Озон: {e}")

        # Авито - ВСЕГДА читаем файл
        from avito_core import analyze_avito_report, get_avito_monthly_totals
        from calendar import monthrange
        try:
            today = now.date()
            filename = f"{today.day:02d}.{today.month:02d}.{today.year}.xlsx"
            filepath = REPORTS_DIR / "Авито" / filename
            if filepath.exists():
                yesterday = today - timedelta(days=1)
                pvz_data = analyze_avito_report(filepath, logger)
                monthly_totals, days_count, first_day = get_avito_monthly_totals(filepath, logger)
                days_in_month = monthrange(today.year, today.month)[1]

                # Среднее за день = сумма_за_месяц / количество_дней_ПВЗ
                avg_data = {pvz: round(total / days_count[pvz]) if days_count.get(pvz, 0) > 0 else 0
                            for pvz, total in monthly_totals.items()}

                # Прогноз = среднее × рабочие дни ПВЗ в месяце
                forecast = {pvz: round(avg * (days_in_month - (first_day.get(pvz, 1) - 1)))
                            for pvz, avg in avg_data.items()}

                avito_data = {
                    'daily': pvz_data,
                    'avg': avg_data,
                    'forecast': forecast,
                    'last_date': yesterday
                }
        except Exception as e:
            logger.error(f"Ошибка анализа Авито: {e}")

        # === ОТПРАВКА В TELEGRAM ===
        logger.info("\n📤 Формирование отчёта...")
        report_text = format_combined_report(yandex_data, ozon_data, avito_data, report_date, EXPENSES)

                # === ОТПРАВКА В TELEGRAM ===
        if message_id:
            logger.info(f"  Обновление TG сообщения {message_id}...")
            result = send_or_update_telegram_message(report_text, logger, message_id)
            if result is None:
                new_id = send_or_update_telegram_message(report_text, logger)
                if new_id:
                    save_message_id(report_date, new_id)
                    logger.info(f"  ✅ TG новое сообщение {new_id}")
            elif result:
                logger.info("  ✅ TG сообщение обновлено")
        else:
            logger.info("  Отправка нового TG сообщения...")
            new_id = send_or_update_telegram_message(report_text, logger)
            if new_id:
                save_message_id(report_date, new_id)
                logger.info(f"  ✅ TG сообщение {new_id} отправлено")

                # === ОТПРАВКА В ВК ===
        if VK_PEER_ID:
            vk_msg_id = get_vk_message_id(report_date)
            vk_text = format_vk_report(yandex_data, ozon_data, avito_data, report_date, EXPENSES)
            if vk_msg_id:
                logger.info(f"  📤 Обновление ВК сообщения {vk_msg_id}...")
            else:
                logger.info("  📤 Отправка нового ВК сообщения...")
            result_id = send_vk_message(vk_text, logger, peer_id=VK_PEER_ID, message_id=vk_msg_id)
            if result_id and result_id != vk_msg_id:
                save_vk_message_id(report_date, result_id)
        else:
            logger.info("  ℹ️ ВК не настроен, пропускаем")

        if driver:
            save_snapshot(driver, logger, "success")
        logger.info("\n✅ Завершено")

    except Exception as e:
        logger.critical(f"❌ Критическая ошибка: {e}")
        if driver:
            save_snapshot(driver, logger, "fatal_error")

    finally:
        if driver:
            time.sleep(3)
            driver.quit()
            try:
                from full_config import cleanup_chrome_profile
                cleanup_chrome_profile()
            except Exception as e:
                logger.error(f"Ошибка очистки профиля: {e}")


if __name__ == "__main__":
    main()
