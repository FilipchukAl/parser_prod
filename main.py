# Импорт необходимых модулей
import os  # Работа с файловой системой
import sqlite3  # Работа с базой данных SQLite
import math  # Математические операции
from datetime import datetime  # Работа с датами
from multiprocessing import Pool, cpu_count  # Многопроцессорная обработка

import pandas as pd  # Обработка Excel-файлов

# =========================================================
# ПУТИ
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Директория скрипта
DATA_DIR = os.path.join(BASE_DIR, "data")  # Папка с Excel-файлами
OKPD_FILE = os.path.join(BASE_DIR, "okpd.txt")  # Файл со справочником ОКПД
DB_FILE = os.path.join(BASE_DIR, "data.db")  # Файл базы данных SQLite

# =========================================================
# EXCEL НАСТРОЙКИ
# =========================================================

ROW_OFFSET = 2  # Смещение строки для значений относительно кода ОКПД
MAIN_COL_OFFSET = 1  # Смещение колонки для значения текущего месяца
PATCH_COL_OFFSET = 2  # Смещение колонки для значения предыдущего месяца

# =========================================================
# КОСТЫЛИ (добавление данных из отсутвующих файлов или редактирование неверных данных)
# =========================================================

PATCHES = {
    datetime(2020, 12, 1): {  # Для декабря 2020
        "source_file": "2021.01.01.xlsx",  # Берем данные из января 2021
        "column_offset": PATCH_COL_OFFSET,  # Используем смещение патча
    },
    datetime(2024, 7, 1): {  # Для июля 2024
        "source_file": "2024.08.01.xlsx",  # Берем данные из августа 2024
        "column_offset": PATCH_COL_OFFSET,
    },
}

# =========================================================
# OKPD
# =========================================================

def load_okpd_list():
    """
    Загружает справочник ОКПД из текстового файла
    Структура файла: название - ОКПД

    Электроэнергия - 35.11.10
    Уголь коксующийся - 05.10.10.120
    ...
    """
    result = []  # Список для хранения пар (название, код ОКПД)

    with open(OKPD_FILE, "r", encoding="utf-8-sig") as f:  # Открытие файла
        for line in f:  # Чтение файла построчно
            line = line.strip()  # Удаление пробелов в начале и конце
            if not line or "-" not in line:  # Пропуск пустых строк и строк без "-"
                continue

            name, okpd = map(str.strip, line.split("-", 1))  # Разделение по первому "-"
            result.append((name, okpd))  # Добавление пары в результат

    if not result:  # Проверка, что файл не пуст
        raise RuntimeError("Файл okpd.txt пуст или некорректен")

    return result  # Возврат списка пар

# =========================================================
# SQLITE
# =========================================================

def init_db(okpd_columns):
    """Инициализация базы данных SQLite"""
    conn = sqlite3.connect(DB_FILE)  # Подключение к БД
    cur = conn.cursor()  # Создание курсора

    # Создание строки с определениями колонок для SQL-запроса
    col_defs = ", ".join(f'"{name}" REAL' for name, _ in okpd_columns)

    # Создание таблицы, если она не существует
    # Колонка с датой (первичный ключ)
    # Динамически созданные колонки для каждого названия ОКПД
    cur.execute(f""" 
        CREATE TABLE IF NOT EXISTS DATA (
            DATE TEXT PRIMARY KEY,
            {col_defs} 
        )
    """)

    conn.commit()  # Применение изменений
    return conn  # Возврат соединения

def sync_table_schema(conn, okpd_columns):
    """Синхронизация структуры таблицы с текущим справочником ОКПД"""
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(DATA)")  # Получение информации о колонках таблицы
    existing = {row[1] for row in cur.fetchall()}  # Множество существующих колонок

    for name, _ in okpd_columns:  # Для каждого названия из справочника
        if name not in existing:  # Если колонки нет в таблице
            cur.execute(f'ALTER TABLE DATA ADD COLUMN "{name}" REAL')  # Добавляем колонку

    conn.commit()  # Применение изменений

def get_existing_row(conn, date_str):
    """Получение существующей строки по дате"""
    cur = conn.cursor()
    cur.execute("SELECT * FROM DATA WHERE DATE = ?", (date_str,))  # Поиск строки по дате
    row = cur.fetchone()  # Получение одной строки

    if row is None:  # Если строка не найдена
        return None

    columns = [d[0] for d in cur.description]  # Получение имен колонок
    return dict(zip(columns, row))  # Возврат словаря {колонка: значение}

# =========================================================
# EXCEL: ИЗВЛЕЧЕНИЕ ЗНАЧЕНИЙ ТЕКУЩЕГО И ПРЕДЫДУЩЕГО МЕСЯЦА
# =========================================================

def extract_all_okpd_from_excel(args):
    """
    Извлекает значения для всех кодов ОКПД из Excel-файла.
    
    Args:
        args: кортеж (путь_к_excel, список_окпд)
    
    Returns:
        Словарь {название: (текущий_месяц, предыдущий_месяц)}
    """
    excel_path, okpd_list = args  # Распаковка аргументов

    okpd_to_name = {okpd: name for name, okpd in okpd_list}  # Словарь код->название
    found = {}  # Словарь для найденных значений

    xls = pd.ExcelFile(excel_path)  # Загрузка Excel-файла

    for sheet_name in xls.sheet_names:  # Для каждого листа в файле
        df = xls.parse(sheet_name=sheet_name, header=None)  # Чтение листа без заголовков

        for r in range(df.shape[0]):  # Для каждой строки
            for c in range(df.shape[1]):  # Для каждой колонки
                cell = str(df.iat[r, c]).strip()  # Получение значения ячейки

                if cell in okpd_to_name and cell not in found:  # Если это код ОКПД и еще не найден
                    try:
                        rr = r + ROW_OFFSET  # Вычисление строки со значениями

                        # Получение основного значения и значения патча
                        raw_main = df.iat[rr, c + MAIN_COL_OFFSET]
                        raw_patch = df.iat[rr, c + PATCH_COL_OFFSET]

                        # Конвертация в float с заменой запятой на точку
                        value_main = float(str(raw_main).replace(",", "."))
                        value_patch = float(str(raw_patch).replace(",", "."))

                        # Замена NaN на 0
                        if math.isnan(value_main):
                            value_main = 0
                        if math.isnan(value_patch):
                            value_patch = 0

                    except Exception:  # В случае ошибки - значения = 0
                        print(f"[WARN] {okpd_to_name[cell]} ({cell}) → значения = 0")
                        value_main = 0
                        value_patch = 0

                    found[cell] = (value_main, value_patch)  # Сохранение найденных значений

    result = {}  # Итоговый словарь
    for name, okpd in okpd_list:  # Для каждой пары название-код
        result[name] = found.get(okpd, (0, 0))  # Получение значений или (0, 0) если не найдено

    return result  # Возврат результата

# =========================================================
# ПОЛУЧЕНИЕ ЗНАЧЕНИЯ ПРЕДЫДУЩЕГО МЕСЯЦА ИЗ ТЕКУЩЕГО
# =========================================================

def apply_patch_logic(months_data):
    """
    Получение зачения предыдущего месяца из файла текущего месяца:
    Если у предыдущего месяца value_main == 0, а у текущего файла есть значение предыдущего месяца value_patch > 0,
    то значение value_patch переносится в value_main предыдущего.
    """
    for i in range(1, len(months_data)):  # Для каждой пары месяцев
        prev_month = months_data[i - 1]  # Предыдущий месяц
        curr_month = months_data[i]  # Текущий месяц

        for key in prev_month:  # Для каждого показателя
            prev_main, _ = prev_month[key]  # Основное значение предыдущего месяца
            _, curr_patch = curr_month[key]  # Значение value_patch в файле текущего месяца

            if prev_main == 0 and curr_patch > 0:  # Условие для замены
                prev_month[key] = (curr_patch, 0)  # Замена значения

# =========================================================
# ВСПОМОГАТЕЛЬНОЕ
# =========================================================

def flatten_month(month_data):
    """Преобразует словарь с кортежами значений в словарь только с основными значениями"""
    return {name: value_main for name, (value_main, _) in month_data.items()}

# =========================================================
# СОРТИРОВКА И ПРОВЕРКА
# =========================================================

def sort_table_by_date(conn):
    """Сортирует таблицу DATA по дате"""
    cur = conn.cursor()
    cur.execute("BEGIN TRANSACTION;")  # Начало транзакции
    cur.execute("CREATE TABLE DATA_SORTED AS SELECT * FROM DATA ORDER BY DATE;")  # Создание отсортированной копии
    cur.execute("DROP TABLE DATA;")  # Удаление старой таблицы
    cur.execute("ALTER TABLE DATA_SORTED RENAME TO DATA;")  # Переименование новой таблицы
    conn.commit()  # Применение изменений

def check_monthly_growth(conn):
    """Проверяет аномальный рост значений между месяцами"""
    cur = conn.cursor()
    cur.execute("SELECT * FROM DATA ORDER BY DATE")  # Получение всех строк отсортированных по дате

    rows = cur.fetchall()  # Все строки
    columns = [d[0] for d in cur.description]  # Имена колонок

    if len(rows) < 2:  # Если меньше двух строк - проверка не нужна
        return

    date_idx = columns.index("DATE")  # Индекс колонки с датой
    value_cols = [c for c in columns if c != "DATE"]  # Колонки со значениями

    for i in range(1, len(rows)):  # Для каждой пары последовательных месяцев
        prev_row = rows[i - 1]  # Предыдущая строка
        curr_row = rows[i]  # Текущая строка

        for col in value_cols:  # Для каждой колонки со значениями
            idx = columns.index(col)  # Индекс текущей колонки

            prev_val = prev_row[idx]  # Значение в предыдущем месяце
            curr_val = curr_row[idx]  # Значение в текущем месяце

            if prev_val in (None, 0) or curr_val is None:  # Пропуск нулевых или None значений
                continue

            growth = (curr_val - prev_val) / prev_val  # Расчет роста

            if abs(growth) > 1:  # Если рост более 100% или падение более 100%
                print(
                    f"[WARN] Аномальный рост '{col}': "
                    f"{prev_row[date_idx]} → {curr_row[date_idx]} "
                    f"({growth * 100:.1f}%)"  # Вывод предупреждения
                )

# =========================================================
# EXPORT SQLITE → CSV (десятичная запятая)
# =========================================================

def export_sqlite_to_csv(conn, output_path):
    """
    Экспортирует таблицу DATA в CSV.

    Особенности:
    - разделитель ;
    - десятичный разделитель ,
    - кодировка utf-8-sig (Excel-friendly)
    """

    # Загружаем данные из SQLite
    df = pd.read_sql_query("SELECT * FROM DATA ORDER BY DATE", conn)

    # Форматируем числовые колонки
    for col in df.columns:
        if col == "DATE":  # Колонку с датой не форматируем
            continue

        df[col] = (
            df[col]
            .fillna(0)  # Замена NaN на 0
            .map(lambda x: f"{x:.6f}".rstrip("0").rstrip(".").replace(".", ","))  # Форматирование
        )

    # Сохраняем CSV
    df.to_csv(
        output_path,
        sep=";",  # Разделитель - точка с запятой
        index=False,  # Без индекса
        encoding="utf-8-sig"  # Кодировка с BOM для Excel
    )

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    OKPD_LIST = load_okpd_list()  # Загрузка справочника ОКПД

    conn = init_db(OKPD_LIST)  # Инициализация БД
    sync_table_schema(conn, OKPD_LIST)  # Синхронизация структуры таблицы

    tasks = []  # Список задач для обработки

    for filename in sorted(os.listdir(DATA_DIR)):  # Просмотр файлов в папке data
        if not filename.endswith(".xlsx"):  # Пропуск не Excel-файлов
            continue

        try:
            # Парсинг даты из имени файла
            file_date = datetime.strptime(filename.replace(".xlsx", ""), "%Y.%m.%d")
        except ValueError:  # Если не удалось распарсить дату
            continue

        tasks.append((file_date, os.path.join(DATA_DIR, filename)))  # Добавление задачи

    # --- EXTRACT ---
    with Pool(cpu_count()) as pool:  # Создание пула процессов
        extracted = pool.map(  # Многопроцессорная обработка
            extract_all_okpd_from_excel,
            [(path, OKPD_LIST) for _, path in tasks]  # Подготовка аргументов
        )

    # --- ПОЛУЧЕНИЕ ЗНАЧЕНИЯ ПРЕДЫДУЩЕГО МЕСЯЦА ИЗ ТЕКУЩЕГО ---
    apply_patch_logic(extracted)

    # --- ЗАПИСЬ ОСНОВНЫХ МЕСЯЦЕВ ---
    for (date_obj, _), month_data in zip(tasks, extracted):  # Для каждого месяца
        date_str = date_obj.strftime("%Y.%m.%d")  # Преобразование даты в строку
        values = flatten_month(month_data)  # Получение только основных значений

        if get_existing_row(conn, date_str) is None:  # Если запись для этой даты отсутствует
            row = {"DATE": date_str, **values}  # Создание строки для вставки
            cols = ", ".join(f'"{k}"' for k in row)  # Имена колонок
            ph = ", ".join("?" * len(row))  # Плейсхолдеры для значений

            conn.execute(  # Вставка строки в БД
                f"INSERT INTO DATA ({cols}) VALUES ({ph})",
                tuple(row.values())
            )

    # --- КОТСТЫЛИ ДЛЯ ОТСУТСВУЮЩИХ ИЛИ НЕВЕРНЫХ МЕСЯЦЕВ ---
    for target_date, patch in PATCHES.items():  # Для каждого "костыля"
        # Извлечение данных из указанного файла
        values = extract_all_okpd_from_excel((
            os.path.join(DATA_DIR, patch["source_file"]),
            OKPD_LIST
        ))

        date_str = target_date.strftime("%Y.%m.%d")  # Дата для вставки
        flat = flatten_month(values)  # Только основные значения

        if get_existing_row(conn, date_str) is None:  # Если записи еще нет
            row = {"DATE": date_str, **flat}  # Создание строки
            cols = ", ".join(f'"{k}"' for k in row)  # Имена колонок
            ph = ", ".join("?" * len(row))  # Плейсхолдеры

            conn.execute(  # Вставка строки
                f"INSERT INTO DATA ({cols}) VALUES ({ph})",
                tuple(row.values())
            )

    conn.commit()  # Применение всех изменений в БД

    sort_table_by_date(conn)  # Сортировка таблицы по дате
    check_monthly_growth(conn)  # Проверка на аномальный рост

    # --- EXPORT CSV ---
    CSV_FILE = os.path.join(BASE_DIR, "data.csv")  # Путь для CSV-файла
    export_sqlite_to_csv(conn, CSV_FILE)  # Экспорт в CSV

    conn.close()  # Закрытие соединения с БД