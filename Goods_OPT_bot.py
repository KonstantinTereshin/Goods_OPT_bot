#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import telebot
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# ─────────────────────────────────────────────────────────────────────────────
# 1. Настройка логирования
# ─────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("GoodsOPTBot")
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.DEBUG
)

# ─────────────────────────────────────────────────────────────────────────────
# 2. Загрузка конфигурации из .env
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
MANAGER_TELEGRAM_ID    = os.getenv("MANAGER_TELEGRAM_ID", "")
OPT_MANAGER_TELEGRAM_ID= os.getenv("OPT_MANAGER_TELEGRAM_ID", MANAGER_TELEGRAM_ID)

MSSQL_SERVER   = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")

# Разбираем список notify-only менеджеров
opt_manager_ids = []
for mid in OPT_MANAGER_TELEGRAM_ID.split(","):
    mid = mid.strip()
    if mid.isdigit():
        opt_manager_ids.append(int(mid))

# Основной менеджер(ы)
manager_ids = []
logger.debug(f"MANAGER_TELEGRAM_ID from env: '{MANAGER_TELEGRAM_ID}'")
for mid in MANAGER_TELEGRAM_ID.split(","):
    mid = mid.strip()
    if mid.isdigit():
        manager_ids.append(int(mid))

logger.debug(f"Confirmation managers: {manager_ids}")
logger.debug(f"Notify-only managers:  {opt_manager_ids}")

# Проверяем, что менеджеры настроены
if not manager_ids:
    logger.warning("WARNING: No confirmation managers configured! MANAGER_TELEGRAM_ID is empty or invalid.")
if not opt_manager_ids:
    logger.warning("WARNING: No opt managers configured! OPT_MANAGER_TELEGRAM_ID is empty or invalid.")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Подключение к MSSQL
# ─────────────────────────────────────────────────────────────────────────────
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD}"
)
logger.info(f"Connecting to MSSQL at {MSSQL_SERVER}/{MSSQL_DATABASE}")
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# ─────────────────────────────────────────────────────────────────────────────
# 4. Инициализация бота
# ─────────────────────────────────────────────────────────────────────────────
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# Глобальные переменные для хранения состояния пользователей
user_context = {}
user_last_product_code = {}
user_urgency_choice = {}
user_self_delivery_mode = {}
user_selected_shop = {}
user_self_delivery_pending = {}
user_receiver_name = {}  # Новое: ФИО получателя для самовывоза
user_waiting_for_receiver = {}  # Новое: ожидание ввода ФИО
manager_self_delivery_responses = {}  # Новое: ответы менеджеров по самовывозу
manager_shop_selection_responses = {}  # Новое: ответы менеджеров по выбору магазина для обычных заказов

def clear_user_cache(uid):
    """
    Очищает кэш ответов менеджеров для конкретного пользователя при новом заказе.
    """
    logger.debug(f"[clear_user_cache] clearing cache for user {uid}")
    
    # Очищаем выбор срочности
    if uid in user_urgency_choice:
        del user_urgency_choice[uid]
    
    # Очищаем ожидание ввода получателя
    if uid in user_waiting_for_receiver:
        del user_waiting_for_receiver[uid]
    
    # Очищаем кэш ответов менеджеров по самовывозу
    keys_to_remove = []
    for key in manager_self_delivery_responses.keys():
        if str(uid) in key:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del manager_self_delivery_responses[key]
        logger.debug(f"[clear_user_cache] removed self_delivery response key: {key}")
    
    # Очищаем кэш ответов менеджеров по выбору магазина
    keys_to_remove = []
    for key in manager_shop_selection_responses.keys():
        if str(uid) in key:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del manager_shop_selection_responses[key]
        logger.debug(f"[clear_user_cache] removed shop_selection response key: {key}")
    
    logger.debug(f"[clear_user_cache] cache cleared for user {uid}")

# ─────────────────────────────────────────────────────────────────────────────
# 5. Вспомогательные функции
# ─────────────────────────────────────────────────────────────────────────────
def is_allowed_user(telegram_id: int) -> bool:
    """
    Проверка доступа в tbl_Telegram_ID_Goods_OPT_bot.
    Если пользователь найден — сохраняем контекст в user_context и возвращаем True.
    """
    logger.debug(f"[is_allowed_user] checking {telegram_id}")
    try:
        cursor.execute("""
			SELECT
    t.ID,
    t.K_ID,
    w.K_Name,
    t.Telegram_ID,
    t.FIO           AS Telegram_FIO,
    t.Emp_ID,
    e.FIO           AS Employee_FIO,
    t.self_delivery
FROM dbo.tbl_Telegram_ID_Goods_OPT_bot AS t
LEFT JOIN dbo.List_Kontr_wholesale AS w
    ON t.K_ID = w.K_ID
LEFT JOIN dbo.List_Emploees AS e
    ON t.Emp_ID = e.Emp_ID
            WHERE t.Telegram_ID = ?
        """, telegram_id)
        row = cursor.fetchone()
    except Exception as e:
        logger.error(f"DB error in is_allowed_user: {e}")
        return False

    if row:
        user_context[telegram_id] = {
            "K_ID": row[1],
            "K_Name": row[2],
            "Telegram_ID": row[3],
            "FIO": row[4],
            "Emp_ID": row[5],
            "Employee_FIO": row[6] or "(невідомо)",
            "self_delivery": bool(row[7])
        }
        logger.info(f"Loaded context for {telegram_id}: {user_context[telegram_id]}")
        return True

    return False

def get_product_info(code: int):
    logger.debug(f"[get_product_info] code={code}")
    try:
        cursor.execute("EXEC qry_goods_opt_bot ?", code)
        row = cursor.fetchone()
        logger.debug(f"[get_product_info] row={row}")
        if row:
            return {
                "Код": row[0],
                "Название": row[1],
                "Цена": row[2],
                "Brand_ID": row[3],  # строго Brand_ID
                "Фото": row[4]
            }
    except Exception as e:
        logger.error(f"DB error in get_product_info: {e}")
    return None

def get_stock_info(code: int):
    """
    Получение информации о наличии товара в магазинах через OPENQUERY.
    """
    logger.debug(f"[get_stock_info] code={code}")
    try:
        query = """
            SELECT k.K_Name, o.g_id, o.ostatok 
            FROM OPENQUERY(mysql_sales,'
                SELECT ko_id, g_id, ostatok FROM ostatki
                UNION ALL
                SELECT stock_id, g_id, ostatok FROM ostatki_sklad
            ') AS o 
            INNER JOIN dbo.List_Kontr AS k ON o.ko_id = k.K_ID
            WHERE o.g_id = ?
        """
        cursor.execute(query, code)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error in get_stock_info: {e}")
        return []

def get_available_shops(code: int):
    """
    Получение списка всех доступных магазинов с наличием товара.
    """
    logger.debug(f"[get_available_shops] code={code}")
    try:
        query = """
            SELECT k.K_ID, k.K_Name, o.ostatok 
            FROM OPENQUERY(mysql_sales,'
                SELECT ko_id, g_id, ostatok FROM ostatki
                UNION ALL
                SELECT stock_id, g_id, ostatok FROM ostatki_sklad
            ') AS o 
            INNER JOIN dbo.List_Kontr AS k ON o.ko_id = k.K_ID
            WHERE o.g_id = ? AND o.ostatok > 0
            ORDER BY o.ostatok DESC
        """
        cursor.execute(query, code)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error in get_available_shops: {e}")
        return []

def get_self_delivery_shops(code: int):
    """
    Получение списка магазинов для самовывоза из Киева.
    """
    logger.debug(f"[get_self_delivery_shops] code={code}")
    try:
        query = """
            SELECT TOP 5 K_ID, k_name 
            FROM vw_goods_ost_bot 
            WHERE k_name LIKE '/Киев%' AND g_id = ?
        """
        cursor.execute(query, code)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error in get_self_delivery_shops: {e}")
        return []

def get_shops_for_sensitive_brand(code: int):
    """
    Получение списка магазинов где есть товар для бренд-чувствительных товаров.
    Использует запрос: select K_ID, k_name from vw_goods_ost_bot where g_id = ?
    """
    logger.debug(f"[get_shops_for_sensitive_brand] code={code}")
    try:
        query = """
            SELECT K_ID, k_name 
            FROM vw_goods_ost_bot 
            WHERE g_id = ?
            ORDER BY k_name
        """
        cursor.execute(query, code)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error in get_shops_for_sensitive_brand: {e}")
        return []

def get_shops_for_opt_managers(code: int):
    """
    Получение списка магазинов для выбора OPT_MANAGER_TELEGRAM_ID.
    Использует новый запрос с UNION ALL для получения магазинов из остатков и Киевских магазинов.
    """
    logger.debug(f"[get_shops_for_opt_managers] code={code}")
    try:
        query = """
            SELECT 
                k.K_ID,
                k.k_name
            FROM 
                OPENQUERY(mysql_sales, '
                    SELECT 
                        stock_id AS ko_id, 
                        g_id, 
                        ostatok  
                    FROM 
                        ostatki_sklad
                ') AS o
            INNER JOIN 
                list_kontr AS k ON o.ko_id = k.K_ID
            WHERE 
                o.g_id = ?

            UNION ALL

            SELECT TOP 10
                K_ID,
                k_name
            FROM 
                vw_goods_ost_bot
            WHERE g_id = ?
        """
        cursor.execute(query, code, code)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error in get_shops_for_opt_managers: {e}")
        return []



def is_sensitive_brand(brand_id: int) -> bool:
    """
    Проверка флага чувствительности бренда в tbl_Brand_Goods_OPT_bot.
    """
    logger.debug(f"[is_sensitive_brand] brand_id={brand_id}")
    try:
        cursor.execute(
            "SELECT Flag FROM tbl_Brand_Goods_OPT_bot WHERE Brand_ID = ?",
            brand_id
        )
        row = cursor.fetchone()
        return bool(row and row[0] == 1)
    except Exception as e:
        logger.error(f"DB error in is_sensitive_brand: {e}")
        return False

def get_interest_info(code: int):
    """
    Вызов qry_g_id_interesting_shops_bot — клиенты, интересовавшиеся товаром за 2 недели.
    """
    try:
        cursor.execute("EXEC qry_g_id_interesting_shops_bot ?", code)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error in get_interest_info: {e}")
        return []

def get_zalog_info(code: int):
    """
    Получение информации о залогах товара через OPENQUERY.
    """
    sql_inner = (
        "SELECT g.created_at, g.product_id, f.name as filial_name, "
        "g.amount, g.interest_rate, s.name as seller_name, s.phone "
        "FROM secunda.guarantees g "
        "LEFT JOIN secunda.guarantee_products gp ON g.id = gp.guarantee_id "
        "LEFT JOIN secunda.filial f ON g.filial_id = f.id "
        "LEFT JOIN secunda.sellers s ON g.seller_id = s.id "
        "WHERE DATE(g.created_at) >= DATE_SUB(CURDATE(), INTERVAL 1 year) "
        "AND g.is_issued = 0 "
    )
    query = f"SELECT * FROM OPENQUERY(mysql_sales, '{sql_inner}') WHERE product_id = ?"
    logger.debug(f"[get_zalog_info] code={code}")
    try:
        cursor.execute(query, code)
        return cursor.fetchall()
    except Exception as e:
        logger.error(f"DB error in get_zalog_info: {e}")
        return []

def make_manager_card(product, ctx, urgent, interest=None, zalog=None, stock=None, status_note=None):
    """
    Формирует полный текст карточки товара для MANAGER_TELEGRAM_ID с интересами, залогами и наличием.
    """
    lines = []
    if status_note:
        lines.append(status_note)
    lines.append(f"\U0001F4E6 Код: {product['Код']}")
    lines.append(f"\U0001F4DB Название: {product['Название']}")
    lines.append(f"\U0001F4B0 Ціна: {product['Цена']} грн")
    lines.append(f"Клієнт: [ID: {ctx['K_ID']}] {ctx['K_Name']} ({ctx['FIO']})")
    lines.append(f"Менеджер клієнта: {ctx['Employee_FIO']}")
    lines.append(f"Терміновість: {'Термінове' if urgent else 'Не термінове'}")
    
    if stock:
        lines.append("\n\U0001F4E5 Наявність у магазинах:")
        # Ограничиваем количество магазинов до 5
        for i, row in enumerate(stock[:5]):
            lines.append(f"- {row[0]} • {row[2]} шт.")
        if len(stock) > 5:
            lines.append(f"... та ще {len(stock) - 5} магазинів")
    
    if interest:
        lines.append("\n\U0001F575️ Цим товаром за останні два тижні цікавилися:")
        # Ограничиваем количество записей до 3
        for i, row in enumerate(interest[:3]):
            date = row[0].strftime("%d.%m.%Y")
            lines.append(f"- {date} • {row[1]} • {row[2]} • {row[3]}")
        if len(interest) > 3:
            lines.append(f"... та ще {len(interest) - 3} запитів")
    
    if zalog:
        lines.append("\n\U0001F4BC Товар зараз у заставі:")
        # Ограничиваем количество записей до 2
        for i, row in enumerate(zalog[:2]):
            ondate = row[0].strftime("%d.%m.%Y")
            lines.append(f"- {ondate} • {row[2]} • {row[5]} • {row[6]}")
        if len(zalog) > 2:
            lines.append(f"... та ще {len(zalog) - 2} застав")
    
    return "\n".join(lines)

def make_product_card_only(product, ctx, urgent, status_note=None):
    """
    Формирует только карточку товара без дополнительной информации.
    """
    lines = []
    if status_note:
        lines.append(status_note)
    lines.append(f"📦 Код: {product['Код']}")
    lines.append(f"📋 Название: {product['Название']}")
    lines.append(f"💰 Ціна: {product['Цена']} грн")
    lines.append(f"Клієнт: [ID: {ctx['K_ID']}] {ctx['K_Name']} ({ctx['FIO']})")
    lines.append(f"Менеджер клієнта: {ctx['Employee_FIO']}")
    lines.append(f"Терміновість: {'Термінове' if urgent else 'Не термінове'}")
    
    return "\n".join(lines)

def make_interest_info_message(interest):
    """
    Формирует сообщение с информацией о заинтересованных клиентах.
    """
    if not interest:
        return None
    
    lines = []
    lines.append("🔍 Цим товаром за останні два тижні цікавилися:")
    
    for i, row in enumerate(interest):
        date = row[0].strftime("%d.%m.%Y")
        lines.append(f"- {date} • {row[1]} • {row[2]} • {row[3]}")
    
    return "\n".join(lines)

def make_zalog_info_message(zalog):
    """
    Формирует сообщение с информацией о залогах товара.
    """
    if not zalog:
        return None
    
    lines = []
    lines.append("🏦 Товар зараз у заставі:")
    
    for i, row in enumerate(zalog):
        ondate = row[0].strftime("%d.%m.%Y")
        lines.append(f"- {ondate} • {row[2]} • {row[5]} • {row[6]}")
    
    return "\n".join(lines)

def make_stock_info_message(shops):
    """
    Формирует сообщение с информацией о магазинах где есть товар.
    """
    if not shops:
        return None
    
    lines = []
    lines.append("🏪 Наявність у магазинах:")
    
    for i, row in enumerate(shops):
        lines.append(f"- {row[1]} (ID: {row[0]})")
    
    return "\n".join(lines)



def make_opt_manager_card(product, ctx, urgent, stock=None, status_note=None):
    """
    Формирует текст карточки товара для OPT_MANAGER_TELEGRAM_ID БЕЗ интересов и залогов.
    """
    lines = []
    if status_note:
        lines.append(status_note)
    lines.append(f"\U0001F4E6 Код: {product['Код']}")
    lines.append(f"\U0001F4DB Название: {product['Название']}")
    lines.append(f"\U0001F4B0 Ціна: {product['Цена']} грн")
    lines.append(f"Клієнт: [ID: {ctx['K_ID']}] {ctx['K_Name']} ({ctx['FIO']})")
    lines.append(f"Менеджер клієнта: {ctx['Employee_FIO']}")
    lines.append(f"Терміновість: {'Термінове' if urgent else 'Не термінове'}")
    
    return "\n".join(lines)

def make_self_delivery_card(product, ctx, selected_shop, available_shops, receiver_name=None, status_note=None):
    """
    Формирует карточку для самовывоза с выбором магазина.
    """
    lines = []
    if status_note:
        lines.append(status_note)
    lines.append(f"\U0001F4E6 Код: {product['Код']}")
    lines.append(f"\U0001F4DB Название: {product['Название']}")
    lines.append(f"\U0001F4B0 Ціна: {product['Цена']} грн")
    lines.append(f"Клієнт: [ID: {ctx['K_ID']}] {ctx['K_Name']} ({ctx['FIO']})")
    lines.append(f"Менеджер клієнта: {ctx['Employee_FIO']}")
    lines.append(f"\n\U0001F4E5 Обраний магазин клієнтом: {selected_shop[1]}")
    
    if receiver_name:
        lines.append(f"\U0001F464 Отримувач: {receiver_name}")
    
    if available_shops:
        lines.append("\n\U0001F4E5 Доступні магазини для самовивозу:")
        for shop in available_shops:
            lines.append(f"- {shop[1]} (ID: {shop[0]})")
    
    return "\n".join(lines)

def send_opt_manager_notification(product, ctx, urgent, status_note, stock=None):
    full_text = make_opt_manager_card(product, ctx, urgent, stock, status_note)
    for m_id in opt_manager_ids:
        try:
            bot.send_photo(m_id, product['Фото'], caption=full_text)
        except Exception as e:
            logger.error(f"Failed to send to opt manager {m_id}: {e}")

def send_sensitive_brand_notification(product, ctx, urgent, uid, code):
    """
    Отправляет разбитые сообщения менеджеру для бренд-чувствительных товаров.
    """
    logger.debug(f"[send_sensitive_brand_notification] sending to manager for sensitive brand, code={code}")
    
    if not manager_ids:
        logger.error("ERROR: No confirmation managers configured! Cannot send notification.")
        return False
    
    # Получаем всю информацию
    interest = get_interest_info(code)
    zalog = get_zalog_info(code)
    shops = get_shops_for_sensitive_brand(code)
    
    # Формируем сообщения
    card_text = make_product_card_only(product, ctx, urgent, "🔔 Клієнт зацікавився товаром (чувствительный бренд)")
    interest_text = make_interest_info_message(interest)
    zalog_text = make_zalog_info_message(zalog)
    stock_text = make_stock_info_message(shops)
    
    # Создаем клавиатуру
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("✅ Підтвердити", callback_data=f"approve_{uid}_{code}"),
        InlineKeyboardButton("❌ Відхилити", callback_data=f"reject_{uid}_{code}")
    )
    
    success = True
    
    for manager_id in manager_ids:
        try:
            # 1. Отправляем карточку товара с фото
            logger.debug(f"[send_sensitive_brand_notification] sending card to manager {manager_id}")
            bot.send_photo(manager_id, product['Фото'], caption=card_text)
            
            # 2. Отправляем информацию о заинтересованных (если есть)
            if interest_text:
                logger.debug(f"[send_sensitive_brand_notification] sending interest info to manager {manager_id}")
                bot.send_message(manager_id, interest_text)
            
            # 3. Отправляем информацию о залогах (если есть)
            if zalog_text:
                logger.debug(f"[send_sensitive_brand_notification] sending zalog info to manager {manager_id}")
                bot.send_message(manager_id, zalog_text)
            
            # 4. Отправляем информацию о наличии (если есть)
            if stock_text:
                logger.debug(f"[send_sensitive_brand_notification] sending stock info to manager {manager_id}")
                bot.send_message(manager_id, stock_text)
            
            # 5. Отправляем кнопки выбора
            logger.debug(f"[send_sensitive_brand_notification] sending buttons to manager {manager_id}")
            bot.send_message(manager_id, "Оберіть дію:", reply_markup=keyboard)
            
            logger.debug(f"[send_sensitive_brand_notification] all messages sent successfully to manager {manager_id}")
            
        except Exception as e:
            logger.error(f"Error sending sensitive brand notification to manager {manager_id}: {e}")
            success = False
    
    return success

def send_self_delivery_notification(product, ctx, selected_shop, available_shops, receiver_name=None):
    """
    Отправляет уведомление о самовывозе менеджерам опта.
    """
    logger.debug(f"[send_self_delivery_notification] product={product['Код']}, client={ctx['K_ID']}")
    
    # Создаем уникальный ключ для этого запроса
    request_key = f"{ctx['K_ID']}_{product['Код']}_{selected_shop[0]}"
    
    # Кэш уже очищен при новом заказе, поэтому всегда отправляем новое уведомление
    
    # Создаем карточку товара
    card_text = make_self_delivery_card(product, ctx, selected_shop, available_shops, receiver_name, 
                                       "Самовивіз товару потребує підтвердження якості товару")
    
    # Создаем кнопки для менеджеров
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Кнопка подтверждения выбранного магазина
    keyboard.add(InlineKeyboardButton(
        f"✅ Підтвердити самовивіз з {selected_shop[1]}", 
        callback_data=f"self_delivery_confirm_shop_{request_key}"
    ))
    
    # Кнопки для изменения магазина
    for shop in available_shops:
        if shop[0] != selected_shop[0]:  # Не показываем уже выбранный магазин
            keyboard.add(InlineKeyboardButton(
                f"🔄 Змінити на {shop[1]}", 
                callback_data=f"self_delivery_change_shop_{request_key}_{shop[0]}"
            ))
    
    # Кнопка отказа
    keyboard.add(InlineKeyboardButton(
        "❌ Відхилити самовивіз", 
        callback_data=f"self_delivery_reject_{request_key}"
    ))
    
    # Отправляем всем менеджерам опта
    for manager_id in opt_manager_ids:
        try:
            bot.send_photo(manager_id, product['Фото'], caption=card_text, reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Error sending self-delivery notification to manager {manager_id}: {e}")

def handle_self_delivery_decision(action, request_key, shop_id=None, manager_id=None):
    """
    Обрабатывает решение менеджера по самовывозу.
    """
    logger.debug(f"[handle_self_delivery_decision] action={action}, request_key={request_key}, shop_id={shop_id}")
    
    # Кэш уже очищен при новом заказе, поэтому всегда обрабатываем новый запрос
    
    # Сохраняем ответ первого менеджера
    manager_self_delivery_responses[request_key] = {
        'action': action,
        'shop_id': shop_id,
        'manager_id': manager_id
    }
    
    # Парсим request_key для получения данных
    parts = request_key.split("_")
    client_id = int(parts[0])
    code = int(parts[1])
    original_shop_id = int(parts[2])
    
    logger.debug(f"[handle_self_delivery_decision] parsed client_id={client_id}, code={code}, original_shop_id={original_shop_id}")
    logger.debug(f"[handle_self_delivery_decision] user_context keys: {list(user_context.keys())}")
    
    # Ищем контекст по Telegram_ID, а не по client_id
    ctx = None
    for telegram_id, context in user_context.items():
        if context.get('K_ID') == client_id:
            ctx = context
            logger.debug(f"[handle_self_delivery_decision] found context by K_ID={client_id} for telegram_id={telegram_id}")
            break
    
    if not ctx:
        logger.error(f"[SELF_DELIVERY] Не найден контекст для клиента {client_id}")
        return
    
    product = get_product_info(code)
    if not product:
        logger.error(f"[SELF_DELIVERY] Не найден товар {code}")
        return
    
    # Получаем данные по правильному telegram_id
    telegram_id = ctx.get('Telegram_ID')
    selected_shop = user_selected_shop.get(telegram_id)
    available_shops = get_self_delivery_shops(code)  # Используем функцию для самовывоза
    receiver_name = user_receiver_name.get(telegram_id)
    
    logger.debug(f"[handle_self_delivery_decision] telegram_id={telegram_id}, selected_shop={selected_shop}, receiver_name={receiver_name}")
    
    # Убираем кнопки у всех менеджеров и показываем результат
    for manager_id in opt_manager_ids:
        try:
            if action == "confirm_shop":
                message_text = f"✅ Підтверджено самовивіз з {selected_shop[1]}"
                logger.debug(f"[handle_self_delivery_decision] sending confirmation to manager {manager_id}: {message_text}")
            elif action == "change_shop":
                new_shop = next((shop for shop in available_shops if shop[0] == shop_id), None)
                if new_shop:
                    message_text = f"🔄 Змінено магазин на {new_shop[1]}"
                    # Обновляем выбранный магазин для клиента
                    user_selected_shop[client_id] = new_shop
                    logger.debug(f"[handle_self_delivery_decision] updated shop for client {client_id}: {new_shop[1]}")
                else:
                    message_text = f"🔄 Змінено магазин"
                logger.debug(f"[handle_self_delivery_decision] sending shop change to manager {manager_id}: {message_text}")
            elif action == "reject":
                message_text = "❌ Самовивіз відхилено"
                logger.debug(f"[handle_self_delivery_decision] sending rejection to manager {manager_id}: {message_text}")
            
            # Отправляем новое сообщение с тем же текстом карточки, но без кнопок
            card_text = make_self_delivery_card(product, ctx, selected_shop, available_shops, receiver_name, status_note=message_text)
            bot.send_photo(manager_id, product['Фото'], caption=card_text, reply_markup=None)
            logger.debug(f"[handle_self_delivery_decision] updated card sent to manager {manager_id}")
        except Exception as e:
            logger.error(f"Error sending response to manager {manager_id}: {e}")
    
    # Отправляем ответ клиенту
    try:
        logger.debug(f"[handle_self_delivery_decision] sending response to client telegram_id={telegram_id}, action={action}")
        
        if action == "confirm_shop":
            logger.debug(f"[handle_self_delivery_decision] sending confirmation to client telegram_id={telegram_id}")
            bot.send_message(telegram_id, f"✅ Менеджер підтвердив самовивіз з {selected_shop[1]}")
            bot.send_message(telegram_id, "📝 Натисніть кнопку нижче для підтвердження замовлення:")
            
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(InlineKeyboardButton("✅ Підтвердити замовлення", callback_data="confirm_self_delivery_order"))
            keyboard.add(InlineKeyboardButton("🔄 Вибрати інший товар", callback_data="change_product"))
            bot.send_message(telegram_id, "Підтвердіть замовлення:", reply_markup=keyboard)
            
        elif action == "change_shop":
            new_shop = next((shop for shop in available_shops if shop[0] == shop_id), None)
            if new_shop:
                logger.debug(f"[handle_self_delivery_decision] sending shop change to client telegram_id={telegram_id}: {new_shop[1]}")
                bot.send_message(telegram_id, f"🔄 Менеджер змінив магазин на {new_shop[1]}")
                bot.send_message(telegram_id, "📝 Натисніть кнопку нижче для підтвердження замовлення:")
                
                keyboard = InlineKeyboardMarkup(row_width=1)
                keyboard.add(InlineKeyboardButton("✅ Підтвердити замовлення", callback_data="confirm_self_delivery_order"))
                keyboard.add(InlineKeyboardButton("🔄 Вибрати інший товар", callback_data="change_product"))
                bot.send_message(telegram_id, "Підтвердіть замовлення:", reply_markup=keyboard)
            
        elif action == "reject":
            logger.debug(f"[handle_self_delivery_decision] sending rejection to client telegram_id={telegram_id}")
            bot.send_message(telegram_id, "❌ Менеджер відхилив самовивіз. Обраний товар є заставним.")
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(InlineKeyboardButton("🔄 Вибрати інший товар", callback_data="change_product"))
            bot.send_message(telegram_id, "Оберіть інший товар:", reply_markup=keyboard)
            
            # Очищаем кэш после отмены самовывоза
            clear_user_cache(telegram_id)
            
    except Exception as e:
        logger.error(f"Error sending response to client telegram_id={telegram_id}: {e}")
    
    logger.debug(f"[handle_self_delivery_decision] END - function completed")

def send_shop_selection_notification(product, ctx, urgent, status_note="🔔 Потрібно вибрати магазин для відправки товару"):
    """
    Отправляет уведомление оптовым менеджерам с выбором магазина для обычных заказов.
    """
    logger.debug(f"[send_shop_selection_notification] product={product['Код']}, client={ctx['K_ID']}")
    
    # Создаем уникальный ключ для этого запроса
    request_key = f"shop_selection_{ctx['K_ID']}_{product['Код']}"
    
    # Кэш уже очищен при новом заказе, поэтому всегда отправляем новое уведомление
    
    # Получаем информацию о наличии
    stock = get_stock_info(product['Код'])
    
    # Создаем карточку товара
    card_text = make_opt_manager_card(product, ctx, urgent, stock, status_note)
    
    # Получаем список магазинов для выбора (TOP 5)
    available_shops = get_shops_for_opt_managers(product['Код'])
    
    if not available_shops:
        # Если нет доступных магазинов, отправляем уведомление об ошибке
        for manager_id in opt_manager_ids:
            try:
                bot.send_message(manager_id, f"❌ Товар {product['Код']} недоступний в жодному магазині")
            except Exception as e:
                logger.error(f"Error sending no shops notification to manager {manager_id}: {e}")
        return
    
    # Создаем кнопки для выбора магазина
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Кнопки для выбора магазина
    for shop in available_shops:
        shop_id, shop_name = shop
        keyboard.add(InlineKeyboardButton(
            f"🏪 {shop_name}", 
            callback_data=f"select_shop_{request_key}_{shop_id}"
        ))
    
    # Кнопка отмены
    keyboard.add(InlineKeyboardButton(
        "❌ Скасувати замовлення", 
        callback_data=f"cancel_order_{request_key}"
    ))
    
    # Отправляем всем оптовым менеджерам
    for manager_id in opt_manager_ids:
        try:
            bot.send_photo(manager_id, product['Фото'], caption=card_text, reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Error sending shop selection notification to manager {manager_id}: {e}")

def handle_shop_selection_decision(action, request_key, shop_id=None, shop_name=None, manager_id=None):
    """
    Обрабатывает решение менеджера по выбору магазина для обычных заказов.
    """
    logger.debug(f"[handle_shop_selection_decision] START - action={action}, request_key={request_key}, shop_id={shop_id}")
    
    # Кэш уже очищен при новом заказе, поэтому всегда обрабатываем новый запрос
    
    # Сохраняем ответ первого менеджера
    manager_shop_selection_responses[request_key] = {
        'action': action,
        'shop_id': shop_id,
        'shop_name': shop_name,
        'manager_id': manager_id
    }
    logger.debug(f"[handle_shop_selection_decision] saved response for request_key: {request_key}")
    
    # Парсим request_key для получения данных
    # Формат: shop_selection_{client_id}_{code}
    parts = request_key.split("_")
    client_id = int(parts[2])  # shop_selection_10444_363482 -> parts[2] = 10444
    code = int(parts[3])       # shop_selection_10444_363482 -> parts[3] = 363482
    
    logger.debug(f"[handle_shop_selection_decision] parsed client_id={client_id}, code={code}")
    logger.debug(f"[handle_shop_selection_decision] user_context keys: {list(user_context.keys())}")
    
    # Ищем контекст по Telegram_ID, а не по client_id
    ctx = None
    for telegram_id, context in user_context.items():
        if context.get('K_ID') == client_id:
            ctx = context
            logger.debug(f"[handle_shop_selection_decision] found context by K_ID={client_id} for telegram_id={telegram_id}")
            break
    
    if not ctx:
        logger.error(f"[SHOP_SELECTION] Не найден контекст для клиента {client_id}")
        return
    
    logger.debug(f"[handle_shop_selection_decision] found context: {ctx}")
    
    product = get_product_info(code)
    if not product:
        logger.error(f"[SHOP_SELECTION] Не найден товар {code}")
        return
    
    logger.debug(f"[handle_shop_selection_decision] found product: {product}")
    
    urgent = user_urgency_choice.get(telegram_id, 0)
    logger.debug(f"[handle_shop_selection_decision] urgent={urgent}")
    
    # Убираем кнопки у всех менеджеров и показываем результат
    for manager_id in opt_manager_ids:
        try:
            if action == "select_shop":
                message_text = f"✅ Менеджер вибрав магазин для відправки: {shop_name}"
                logger.debug(f"[handle_shop_selection_decision] sending confirmation to manager {manager_id}: {message_text}")
            elif action == "cancel":
                message_text = "❌ Замовлення скасовано"
                logger.debug(f"[handle_shop_selection_decision] sending cancellation to manager {manager_id}: {message_text}")
            
            # Отправляем новое сообщение с тем же текстом карточки, но без кнопок
            card_text = make_opt_manager_card(product, ctx, urgent, status_note=message_text)
            bot.send_photo(manager_id, product['Фото'], caption=card_text, reply_markup=None)
            logger.debug(f"[handle_shop_selection_decision] updated card sent to manager {manager_id}")
        except Exception as e:
            logger.error(f"Error sending shop selection response to manager {manager_id}: {e}")
    
    # Отправляем ответ клиенту
    try:
        # Получаем правильный telegram_id для отправки сообщений
        telegram_id = ctx.get('Telegram_ID')
        logger.debug(f"[handle_shop_selection_decision] sending response to client telegram_id={telegram_id}, action={action}")
        
        if action == "select_shop":
            logger.debug(f"[handle_shop_selection_decision] sending shop selection confirmation")
            bot.send_message(telegram_id, "📦 Ваше замовлення обробляється...")
            
            # Выполняем процедуру с выбранным магазином
            try:
                logger.info(f"[SHOP_SELECTION_CONFIRM] Вызов процедуры create_transfer_opt_bot: K_ID={ctx['K_ID']}, code={code}, Emp_ID={ctx['Emp_ID']}, urgent={urgent}, Receiver='', shop_id={shop_id}")
                
                # Вызываем процедуру с OUTPUT параметром @result
                cursor.execute("DECLARE @result nvarchar(200); EXEC create_transfer_opt_bot ?, ?, ?, ?, ?, ?, @result OUTPUT; SELECT @result as result", 
                              ctx['K_ID'], code, ctx['Emp_ID'], urgent, '', shop_id)
                
                # Получаем результат процедуры
                if cursor.nextset():
                    result_row = cursor.fetchone()
                    if result_row and result_row[0]:
                        result = result_row[0]
                        logger.info(f"[SHOP_SELECTION_CONFIRM] Получен результат процедуры: {result}")
                    else:
                        result = "✅ Замовлення обробляється"
                        logger.info(f"[SHOP_SELECTION_CONFIRM] Результат процедуры пустой, используем статическое сообщение")
                else:
                    result = "✅ Замовлення обробляється"
                    logger.info(f"[SHOP_SELECTION_CONFIRM] Не удалось получить результат процедуры, используем статическое сообщение")
                
            except Exception as e:
                logger.error(f"DB error in shop selection processing: {e}")
                result = f"Помилка обробки: {str(e)}"
                logger.debug(f"[handle_shop_selection_decision] procedure failed with error: {e}")
            
            # Отправляем результат процедуры клиенту
            bot.send_message(telegram_id, result)
            logger.debug(f"[handle_shop_selection_decision] sent result to client: {result}")
            
            # Предлагаем выбрать другой товар после выполнения процедуры
            bot.send_message(telegram_id, "🛍 Якщо бажаєте, введіть код іншого товару для перегляду.")
            
            # Очищаем кэш после завершения заказа
            clear_user_cache(telegram_id)
            
        elif action == "cancel":
            logger.debug(f"[handle_shop_selection_decision] sending cancel confirmation")
            bot.send_message(telegram_id, "❌ Ваше замовлення скасовано менеджером.")
            keyboard = InlineKeyboardMarkup(row_width=1)
            keyboard.add(InlineKeyboardButton("🔄 Вибрати інший товар", callback_data="change_product"))
            bot.send_message(telegram_id, "Оберіть інший товар:", reply_markup=keyboard)
            
            # Очищаем кэш после отмены заказа
            clear_user_cache(telegram_id)
            
    except Exception as e:
        logger.error(f"Error sending response to client telegram_id={telegram_id}: {e}")
    
    logger.debug(f"[handle_shop_selection_decision] END - function completed")

# ─────────────────────────────────────────────────────────────────────────────
# 6. Обработчики команд и сообщений
# ─────────────────────────────────────────────────────────────────────────────
@bot.message_handler(commands=['start'])
def welcome(message):
    uid = message.from_user.id
    logger.debug(f"[/start] from {uid}")
    if is_allowed_user(uid):
        bot.reply_to(message, "Вітаю! Введіть, будь ласка, код товару:")
    else:
        bot.reply_to(message, "У вас немає доступу до цього бота.")

@bot.message_handler(func=lambda m: m.text and m.text.isdigit())
def handle_product_request(message):
    uid  = message.from_user.id
    code = int(message.text.strip())
    logger.debug(f"[product_request] user={uid}, code={code}")

    if not is_allowed_user(uid):
        bot.reply_to(message, "У вас немає доступу до цього бота.")
        return

    # Очищаем кэш при новом запросе товара
    clear_user_cache(uid)

    product = get_product_info(code)
    if not product:
        bot.reply_to(message, "Товар не знайдено. Спробуйте ще раз.")
        return

    # Отправляем фото и данные
    caption = (
        f"\U0001F4E6 Код: {product['Код']}\n"
        f"\U0001F4DB Название: {product['Название']}\n"
        f"\U0001F4B0 Ціна: {product['Цена']} грн"
    )
    bot.send_photo(uid, product['Фото'], caption=caption)

    # Сохраняем код
    user_last_product_code[uid] = code

    # Кнопки: запросить / выбрать другой
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📩 Запросити цей товар", callback_data="request_product"),
        InlineKeyboardButton("🔄 Вибрати інший товар", callback_data="change_product")
    )
    bot.send_message(uid, "Що бажаєте зробити далі?", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda c: c.data == "change_product")
def handle_change_product(c):
    uid = c.from_user.id
    # c.answer()  # убираем «крутилку»
    
    # Очищаем кэш при выборе другого товара
    clear_user_cache(uid)
    
    bot.send_message(uid, "Введіть код іншого товару:")

@bot.callback_query_handler(func=lambda c: c.data == "request_product")
def handle_request_product(c):
    uid = c.from_user.id
    # c.answer()
    logger.debug(f"[request_product] from {uid}")
    
    # Очищаем кэш при запросе товара
    clear_user_cache(uid)
    
    # Проверяем, есть ли у клиента доступ к самовывозу
    ctx = user_context.get(uid)
    if ctx and ctx.get('self_delivery'):
        # Показываем кнопки с самовывозом
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🚀 Термінове замовлення", callback_data="urgent_1"),
            InlineKeyboardButton("⏳ Не термінове замовлення", callback_data="urgent_0"),
            InlineKeyboardButton("🚚 Самовивіз з магазину", callback_data="self_delivery")
        )
        bot.send_message(uid, "Оберіть тип замовлення:", reply_markup=keyboard)
    else:
        # Обычные кнопки без самовывоза
        keyboard = InlineKeyboardMarkup()
        keyboard.add(
            InlineKeyboardButton("🚀 Термінове замовлення", callback_data="urgent_1"),
            InlineKeyboardButton("⏳ Не термінове замовлення", callback_data="urgent_0")
        )
        bot.send_message(uid, "Оберіть тип замовлення:", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda c: c.data == "self_delivery")
def handle_self_delivery_request(c):
    uid = c.from_user.id
    # c.answer()
    logger.debug(f"[self_delivery_request] from {uid}")
    
    code = user_last_product_code.get(uid)
    if not code:
        bot.send_message(uid, "Внутрішня помилка. Спробуйте ще раз.")
        return

    # Получаем список доступных магазинов для самовывоза из Киева
    available_shops = get_self_delivery_shops(code)
    if not available_shops:
        bot.send_message(uid, "На жаль, товар недоступний для самовивозу з Києва.")
        return

    # Показываем список магазинов
    shop_text = "Оберіть магазин для самовивозу з Києва:\n\n"
    for i, shop in enumerate(available_shops, 1):
        shop_text += f"{i}. {shop[1]}\n"
    
    bot.send_message(uid, shop_text)
    
    # Создаем клавиатуру с магазинами
    keyboard = InlineKeyboardMarkup(row_width=1)
    for i, shop in enumerate(available_shops, 1):
        keyboard.add(InlineKeyboardButton(
            f"{i}. {shop[1]}", 
            callback_data=f"select_shop:{shop[0]}"
        ))
    
    keyboard.add(InlineKeyboardButton("🔄 Вибрати інший товар", callback_data="change_product"))
    bot.send_message(uid, "Оберіть магазин:", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda c: c.data.startswith("select_shop:"))
def handle_shop_selection(c):
    uid = c.from_user.id
    # c.answer()
    shop_id = int(c.data.split(":")[1])
    logger.debug(f"[shop_selection] user={uid}, shop_id={shop_id}")
    
    code = user_last_product_code.get(uid)
    if not code:
        bot.send_message(uid, "Внутрішня помилка. Спробуйте ще раз.")
        return

    # Получаем информацию о выбранном магазине
    available_shops = get_self_delivery_shops(code)
    selected_shop = None
    for shop in available_shops:
        if shop[0] == shop_id:
            selected_shop = shop
            break
    
    if not selected_shop:
        bot.send_message(uid, "Помилка вибору магазину. Спробуйте ще раз.")
        return

    # Сохраняем выбранный магазин
    user_selected_shop[uid] = selected_shop
    user_self_delivery_mode[uid] = True
    
    # Показываем подтверждение
    bot.send_message(uid, f"Ви обрали магазин: {selected_shop[1]}")
    
    # Кнопки для заказа
    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton("📦 Замовити товар з магазину", callback_data="order_from_shop"),
        InlineKeyboardButton("🔄 Вибрати інший товар", callback_data="change_product")
    )
    bot.send_message(uid, "Що бажаєте зробити далі?", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda c: c.data == "order_from_shop")
def handle_order_from_shop(c):
    uid = c.from_user.id
    # c.answer()
    logger.debug(f"[order_from_shop] from {uid}")
    
    ctx = user_context.get(uid)
    code = user_last_product_code.get(uid)
    selected_shop = user_selected_shop.get(uid)
    
    if not ctx or not code or not selected_shop:
        bot.send_message(uid, "Внутрішня помилка. Спробуйте ще раз.")
        return
    
    # Запрашиваем ФИО получателя
    user_waiting_for_receiver[uid] = True
    bot.send_message(uid, 
        "📝 Для самовивозу потрібно вказати ФИО людини, яка буде забирати товар.\n"
        "❗️ Ця людина повинна мати при собі документ, що підтверджує її особу.\n\n"
        "Введіть ФИО отримувача:")

@bot.message_handler(func=lambda message: user_waiting_for_receiver.get(message.from_user.id, False))
def handle_receiver_name_input(message):
    uid = message.from_user.id
    receiver_name = message.text.strip()
    
    if len(receiver_name) < 2:
        bot.send_message(uid, "❌ ФИО повинно містити мінімум 2 символи. Спробуйте ще раз:")
        return
    
    # Сохраняем ФИО получателя
    user_receiver_name[uid] = receiver_name
    user_waiting_for_receiver[uid] = False
    
    ctx = user_context.get(uid)
    code = user_last_product_code.get(uid)
    selected_shop = user_selected_shop.get(uid)
    
    if not ctx or not code or not selected_shop:
        bot.send_message(uid, "Внутрішня помилка. Спробуйте ще раз.")
        return
    
    # Отправляем уведомление менеджерам с ФИО получателя
    product = get_product_info(code)
    if product:
        available_shops = get_self_delivery_shops(code)
        send_self_delivery_notification(product, ctx, selected_shop, available_shops, receiver_name)
    
    bot.send_message(uid, f"✅ ФИО отримувача збережено: {receiver_name}")
    bot.send_message(uid, "⏳ Очікуйте підтвердження від менеджера...")

@bot.callback_query_handler(func=lambda c: c.data.startswith("urgent_0") or c.data == "urgent_1")
def handle_urgency_choice(c):
    uid = c.from_user.id
    # c.answer()
    urgent = 1 if c.data == "urgent_1" else 0
    user_urgency_choice[uid] = urgent
    
    logger.debug(f"[urgency_choice] from {uid}, urgent={urgent}")
    
    ctx = user_context.get(uid)
    code = user_last_product_code.get(uid)
    
    if not ctx or not code:
        bot.send_message(uid, "Внутрішня помилка. Спробуйте ще раз.")
        return
    
    product = get_product_info(code)
    if not product:
        bot.send_message(uid, "Товар не знайдено.")
        return
    
    # Проверяем чувствительность бренда
    brand_id = product.get('Brand_ID')
    is_sensitive = is_sensitive_brand(brand_id) if brand_id else False
    
    if is_sensitive:
        # Для чувствительных брендов отправляем разбитые сообщения менеджеру
        logger.debug(f"[urgency_choice] sending sensitive brand notification for code={code}")
        
        success = send_sensitive_brand_notification(product, ctx, urgent, uid, code)
        
        if success:
            bot.send_message(uid, "Ваш запит відправлено менеджеру для підтвердження.")
        else:
            bot.send_message(uid, "Помилка: менеджер не налаштований. Зверніться до адміністратора.")
        
    else:
        # Для обычных брендов отправляем выбор магазина оптовым менеджерам
        bot.send_message(uid, "Ваш запит відправлено менеджерам для опрацювання зачекайте.")
        
        # Отправляем уведомление менеджерам опта с выбором магазина
        send_shop_selection_notification(product, ctx, urgent, "🔔 Клієнт зацікавився товаром")

@bot.callback_query_handler(func=lambda c: c.data.startswith("approve_") or c.data.startswith("reject_"))
def handle_decision(c):
    action_data = c.data.split("_")
    action = action_data[0] # approve или reject
    uid_str = action_data[1]
    code = int(action_data[2])
    # c.answer()
    logger.debug(f"[decision] action={action}, user={uid_str}, code={code}")

    ctx  = user_context.get(int(uid_str))
    if ctx is None:
        bot.send_message(int(uid_str), "Внутрішня помилка (контекст користувача). Спробуйте ще раз.")
        return
    product = get_product_info(code)
    if product is None:
        bot.send_message(int(uid_str), "Сталася внутрішня помилка. Спробуйте ще раз.")
        return

    urgent = user_urgency_choice.get(int(uid_str), 0)
    stock = get_stock_info(code)

    if action == "approve":
        logger.debug(f"[CONFIRM] manager approved request for user {uid_str}, code {code}")
        
        # Убираем кнопки у менеджера и показываем результат
        for manager_id in manager_ids:
            try:
                message_text = "✅ Замовлення підтверджено менеджером"
                logger.debug(f"[CONFIRM] sending confirmation to manager {manager_id}: {message_text}")
                
                # Отправляем новое сообщение с тем же текстом карточки, но без кнопок
                card_text = make_manager_card(product, ctx, urgent, interest=None, zalog=None, stock=None, status_note=message_text)
                bot.send_photo(manager_id, product['Фото'], caption=card_text, reply_markup=None)
                logger.debug(f"[CONFIRM] updated card sent to manager {manager_id}")
            except Exception as e:
                logger.error(f"Error sending confirmation to manager {manager_id}: {e}")
        
        # Отправляем уведомление менеджерам опта с выбором магазина
        logger.debug(f"[CONFIRM] sending shop selection notification to opt managers")
        send_shop_selection_notification(product, ctx, urgent, "✅ Замовлення підтверджено менеджером.")
        
    else:  # reject
        bot.send_message(int(uid_str), "Ваш запит відхилено. Спробуйте інший товар або зверніться до менеджера.")
        # ОПТОВЫМ МЕНЕДЖЕРАМ: отклонено менеджером
        send_opt_manager_notification(product, ctx, urgent, "❌ Замовлення відхилено менеджером.", stock)

@bot.callback_query_handler(func=lambda c: c.data.startswith("self_delivery_"))
def handle_self_delivery_decision_callback(c):
    logger.debug(f"[handle_self_delivery_decision_callback] callback_data: {c.data}")
    action_data = c.data.split("_")
    logger.debug(f"[handle_self_delivery_decision_callback] action_data: {action_data}")
    
    # Формат: self_delivery_confirm_shop_10444_405450_14177
    # action_data: ['self', 'delivery', 'confirm', 'shop', '10444', '405450', '14177']
    # Формат: self_delivery_reject_10444_405450_13819
    # action_data: ['self', 'delivery', 'reject', '10444', '405450', '13819']
    if len(action_data) >= 4:
        action = action_data[2]  # confirm, change, reject
        
        if action == "confirm":
            # Для confirm_shop: self_delivery_confirm_shop_10444_405450_14177
            if len(action_data) >= 7:
                request_key = f"{action_data[4]}_{action_data[5]}_{action_data[6]}"  # client_id_code_shop_id
                logger.debug(f"[handle_self_delivery_decision_callback] parsed action: {action}, request_key: {request_key}")
                logger.debug(f"[handle_self_delivery_decision_callback] calling handle_self_delivery_decision with confirm_shop")
                handle_self_delivery_decision("confirm_shop", request_key, manager_id=c.from_user.id)
            else:
                logger.error(f"[handle_self_delivery_decision_callback] invalid confirm_shop format: {action_data}")
        elif action == "change":
            # Для change_shop: self_delivery_change_shop_10444_405450_13819_10309
            if len(action_data) >= 7:
                request_key = f"{action_data[4]}_{action_data[5]}_{action_data[6]}"  # client_id_code_shop_id
                shop_id = int(action_data[7])  # новый shop_id
                logger.debug(f"[handle_self_delivery_decision_callback] parsed action: {action}, request_key: {request_key}, shop_id: {shop_id}")
                logger.debug(f"[handle_self_delivery_decision_callback] calling handle_self_delivery_decision with change_shop, shop_id: {shop_id}")
                handle_self_delivery_decision("change_shop", request_key, shop_id=shop_id, manager_id=c.from_user.id)
            else:
                logger.error(f"[handle_self_delivery_decision_callback] invalid change_shop format: {action_data}")
        elif action == "reject":
            # Для reject: self_delivery_reject_10444_405450_13819
            if len(action_data) >= 6:
                request_key = f"{action_data[3]}_{action_data[4]}_{action_data[5]}"  # client_id_code_shop_id
                logger.debug(f"[handle_self_delivery_decision_callback] parsed action: {action}, request_key: {request_key}")
                logger.debug(f"[handle_self_delivery_decision_callback] calling handle_self_delivery_decision with reject")
                handle_self_delivery_decision("reject", request_key, manager_id=c.from_user.id)
            else:
                logger.error(f"[handle_self_delivery_decision_callback] invalid reject format: {action_data}")
        else:
            logger.error(f"[handle_self_delivery_decision_callback] unknown action: {action}")
    else:
        logger.error(f"[handle_self_delivery_decision_callback] invalid action_data format: {action_data}")

@bot.callback_query_handler(func=lambda c: c.data == "confirm_self_delivery_order")
def handle_confirm_self_delivery_order(c):
    uid = c.from_user.id
    # c.answer()
    logger.debug(f"[confirm_self_delivery_order] from {uid}")
    
    ctx = user_context.get(uid)
    code = user_last_product_code.get(uid)
    selected_shop = user_selected_shop.get(uid)
    receiver_name = user_receiver_name.get(uid)
    
    if not ctx or not code or not selected_shop:
        bot.send_message(uid, "Внутрішня помилка. Спробуйте ще раз.")
        return
    
    # Выполняем процедуру с новыми параметрами
    try:
        logger.info(f"[SELF_DELIVERY_CONFIRM] Вызов процедуры create_transfer_opt_bot: K_ID={ctx['K_ID']}, code={code}, Emp_ID={ctx['Emp_ID']}, urgent=1, Receiver='{receiver_name}', shop_id={selected_shop[0]}")
        
        # Вызываем процедуру с OUTPUT параметром @result
        cursor.execute("DECLARE @result nvarchar(200); EXEC create_transfer_opt_bot ?, ?, ?, ?, ?, ?, @result OUTPUT; SELECT @result as result", 
                      ctx['K_ID'], code, ctx['Emp_ID'], 1, receiver_name or '', selected_shop[0])
        
        # Получаем результат процедуры
        if cursor.nextset():
            result_row = cursor.fetchone()
            if result_row and result_row[0]:
                result = result_row[0]
                logger.info(f"[SELF_DELIVERY_CONFIRM] Получен результат процедуры: {result}")
            else:
                result = "✅ Замовлення обробляється"
                logger.info(f"[SELF_DELIVERY_CONFIRM] Результат процедуры пустой, используем статическое сообщение")
        else:
            result = "✅ Замовлення обробляється"
            logger.info(f"[SELF_DELIVERY_CONFIRM] Не удалось получить результат процедуры, используем статическое сообщение")
        
    except Exception as e:
        logger.error(f"DB error in self-delivery confirm processing: {e}")
        result = f"Помилка обробки: {str(e)}"
        logger.debug(f"[SELF_DELIVERY_CONFIRM] procedure failed with error: {e}")
    
    # Отправляем результат процедуры клиенту
    bot.send_message(uid, result)
    logger.debug(f"[handle_confirm_self_delivery_order] sent result to client: {result}")
    
    # Уведомляем менеджеров о подтверждении заказа
    product = get_product_info(code)
    if product:
        for manager_id in opt_manager_ids:
            try:
                notification_text = (
                    f"✅ Клієнт підтвердив замовлення\n"
                    f"Клієнт: {ctx['K_Name']} ({ctx['FIO']})\n"
                    f"Товар: {product['Название']} (код: {product['Код']})\n"
                    f"Магазин: {selected_shop[1]}\n"
                    f"Отримувач: {receiver_name}"
                )
                logger.debug(f"[handle_confirm_self_delivery_order] sending confirmation to manager {manager_id}: {notification_text}")
                bot.send_message(manager_id, notification_text)
                logger.debug(f"[handle_confirm_self_delivery_order] confirmation sent to manager {manager_id}")
            except Exception as e:
                logger.error(f"Error sending confirmation to manager {manager_id}: {e}")
    
    # Очищаем состояние самовывоза
    user_self_delivery_mode.pop(uid, None)
    user_selected_shop.pop(uid, None)
    user_self_delivery_pending.pop(uid, None)
    user_receiver_name.pop(uid, None)
    
    # Предлагаем выбрать другой товар после выполнения процедуры
    bot.send_message(uid, "🛍 Якщо бажаєте, введіть код іншого товару для перегляду.")
    
    # Очищаем кэш после завершения самовывоза
    clear_user_cache(uid)

@bot.callback_query_handler(func=lambda c: c.data.startswith("select_shop_") or c.data.startswith("cancel_order_"))
def handle_shop_selection_callback(c):
    logger.debug(f"[handle_shop_selection_callback] callback_data: {c.data}")
    action_data = c.data.split("_")
    logger.debug(f"[handle_shop_selection_callback] action_data: {action_data}")
    
    # Проверяем первые два элемента для определения типа действия
    if len(action_data) >= 2 and action_data[0] == "select" and action_data[1] == "shop":
        # Формат: select_shop_shop_selection_{client_id}_{code}_{shop_id}
        # action_data: ['select', 'shop', 'shop', 'selection', '10444', '363482', '13819']
        logger.debug(f"[handle_shop_selection_callback] processing select_shop action")
        request_key = f"{action_data[2]}_{action_data[3]}_{action_data[4]}_{action_data[5]}"  # shop_selection_{client_id}_{code}
        shop_id = int(action_data[6])
        logger.debug(f"[handle_shop_selection_callback] action: select_shop, request_key: {request_key}, shop_id: {shop_id}")
        
        # Получаем название магазина по ID
        parts = request_key.split("_")
        code = int(parts[3])  # получаем code из shop_selection_{client_id}_{code} -> parts[3] = code
        logger.debug(f"[handle_shop_selection_callback] parsed code: {code}")
        available_shops = get_available_shops(code)
        shop_name = "неизвестный магазин"
        for shop in available_shops:
            if shop[0] == shop_id:  # shop[0] - это shop_id
                shop_name = shop[1]  # shop[1] - это shop_name
                break
        logger.debug(f"[handle_shop_selection_callback] found shop_name: {shop_name}")
        logger.debug(f"[handle_shop_selection_callback] calling handle_shop_selection_decision with shop_name: {shop_name}")
        handle_shop_selection_decision("select_shop", request_key, shop_id=shop_id, shop_name=shop_name, manager_id=c.from_user.id)
        
    elif len(action_data) >= 2 and action_data[0] == "cancel" and action_data[1] == "order":
        # Формат: cancel_order_shop_selection_{client_id}_{code}
        # action_data: ['cancel', 'order', 'shop', 'selection', '10444', '363482']
        logger.debug(f"[handle_shop_selection_callback] processing cancel_order action")
        request_key = f"{action_data[2]}_{action_data[3]}_{action_data[4]}_{action_data[5]}"  # shop_selection_{client_id}_{code}
        logger.debug(f"[handle_shop_selection_callback] action: cancel_order, request_key: {request_key}")
        logger.debug(f"[handle_shop_selection_callback] calling handle_shop_selection_decision with cancel")
        handle_shop_selection_decision("cancel", request_key, manager_id=c.from_user.id)
    else:
        logger.error(f"[handle_shop_selection_callback] unknown action pattern: {action_data}")

# ─────────────────────────────────────────────────────────────────────────────
# 7. Запуск polling
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("Starting bot polling…")
    bot.polling(non_stop=True)
