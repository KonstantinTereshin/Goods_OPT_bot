import os
import telebot
import pyodbc
from datetime import datetime
from dotenv import load_dotenv
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MANAGER_TELEGRAM_ID = int(os.getenv("MANAGER_TELEGRAM_ID"))
MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD}"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

pending_requests = {}
user_last_product_code = {}

def is_allowed_user(telegram_id):
    cursor.execute("SELECT COUNT(*) FROM tbl_Telegram_ID_Goods_OPT_bot WHERE Telegram_ID = ?", telegram_id)
    return cursor.fetchone()[0] > 0

def get_product_info(code):
    cursor.execute("exec qry_goods_opt_bot ?", code)
    row = cursor.fetchone()
    if row:
        return {
            "Код": row[0],
            "Название": row[1],
            "Цена": row[2],
            "Brand_ID": row[3],
            "Фото": row[4]
        }
    return None

def is_sensitive_brand(brand_id):
    cursor.execute("SELECT Flag FROM tbl_Brand_Goods_OPT_bot WHERE Brand_ID = ?", brand_id)
    row = cursor.fetchone()
    return row and row[0] == 1

def get_interest_info(code):
    cursor.execute("exec qry_g_id_interesting_shops ?", code)
    return cursor.fetchall()

def get_zalog_info(code):
    cursor.execute("exec qry_goods_zalog_bot ?", code)
    return cursor.fetchall()

@bot.message_handler(commands=['start'])
def welcome(message):
    if is_allowed_user(message.from_user.id):
        bot.reply_to(message, "Введіть, будь ласка, код товару:")
    else:
        bot.reply_to(message, "У вас немає доступу до цього бота.")

@bot.message_handler(func=lambda message: message.text.isdigit())
def handle_product_request(message):
    telegram_id = message.from_user.id
    code = int(message.text)

    if not is_allowed_user(telegram_id):
        bot.reply_to(message, "У вас немає доступу до цього бота.")
        return

    product = get_product_info(code)
    if not product:
        bot.reply_to(message, "Товар не знайдено. Спробуйте ще раз.")
        return

    caption = (
        f"\U0001F4E6 Код: {product['Код']}\n"
        f"\U0001F4DB Назва: {product['Название']}\n"
        f"\U0001F4B0 Ціна: {product['Цена']} грн"
    )
    bot.send_photo(chat_id=telegram_id, photo=product['Фото'], caption=caption)

    user_last_product_code[telegram_id] = code

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(text="📩 Запросити цей товар", callback_data="request_product"),
        InlineKeyboardButton(text="🔄 Вибрати інший товар", callback_data="change_product")
    )
    bot.send_message(chat_id=telegram_id, text="Що бажаєте зробити далі?", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data == "request_product")
def handle_request_product(call):
    telegram_id = call.from_user.id
    code = user_last_product_code.get(telegram_id)
    if not code:
        bot.send_message(telegram_id, "Спочатку оберіть товар, будь ласка.")
        return

    product = get_product_info(code)
    if not product:
        bot.send_message(telegram_id, "Товар не знайдено.")
        return

    if not is_sensitive_brand(product["Brand_ID"]):
        bot.send_message(telegram_id, "Ваш запит обробляється, очікуйте відповідь.")
        bot.send_message(telegram_id, "🛍 Якщо бажаєте, введіть код іншого товару для перегляду.")
        return

    interest_rows = get_interest_info(code)
    zalog_rows = get_zalog_info(code)

    if not interest_rows and not zalog_rows:
        bot.send_message(telegram_id, "Ваш запит обробляється, очікуйте відповідь.")
        bot.send_message(telegram_id, "🛍 Якщо бажаєте, введіть код іншого товару для перегляду.")
        return

    full_text = (
        f"\U0001F514 Клієнт зацікавився товаром:\n"
        f"Код: {product['Код']}\n"
        f"Назва: {product['Название']}\n"
        f"Ціна: {product['Цена']} грн\n"
    )

    if interest_rows:
        full_text += "\n\U0001F575️ Цим товаром за останній тиждень цікавилися:\n"
        for row in interest_rows:
            date = row[0].strftime("%d.%m.%Y")
            full_text += f"- {date} • {row[1]} • {row[2]} • {row[3]}\n"

    if zalog_rows:
        full_text += "\n\U0001F4BC Товар зараз у заставі:\n"
        for row in zalog_rows:
            on_date = row[1].strftime("%d.%m.%Y %H:%M")
            full_text += f"- {row[0]} • {on_date} • Менеджер: {row[3]} • Тел: {row[4]}\n"

    keyboard = InlineKeyboardMarkup()
    keyboard.add(
        InlineKeyboardButton(text="✅ Підтвердити", callback_data=f"approve:{telegram_id}"),
        InlineKeyboardButton(text="❌ Відхилити", callback_data=f"reject:{telegram_id}")
    )
    bot.send_message(chat_id=MANAGER_TELEGRAM_ID, text=full_text, reply_markup=keyboard)
    bot.send_message(telegram_id, "Ваш запит відправлено на перевірку менеджеру.")
    pending_requests[telegram_id] = code

@bot.callback_query_handler(func=lambda call: call.data == "change_product")
def handle_change_product(call):
    bot.send_message(call.from_user.id, "Введіть код іншого товару:")

@bot.callback_query_handler(func=lambda call: call.data.startswith("approve") or call.data.startswith("reject"))
def handle_decision(call):
    action, user_id_str = call.data.split(":")
    user_id = int(user_id_str)

    if action == "approve":
        bot.send_message(user_id, "Ваш запит підтверджено. Очікуйте дзвінка менеджера.")
    elif action == "reject":
        bot.send_message(user_id, "Ваш запит відхилено. Спробуйте інший товар або зверніться до менеджера.")
    bot.send_message(user_id, "🛍 Якщо бажаєте, введіть код іншого товару для перегляду.")

bot.polling(none_stop=True)
