from os import getenv
import zeep
from aiogram import Bot, Dispatcher, executor, types
import sqlite3
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep.transports import Transport
from rapidfuzz import fuzz
import json

# База данных
db = sqlite3.connect('goods.db')
sql = db.cursor()

sql.execute("""CREATE TABLE IF NOT EXISTS goods (
    code TEXT,
    name TEXT,
    botname TEXT,
    suppliername TEXT
    )""")

db.commit()

# Бот
API_TOKEN = 'BOT TOKEN HERE'
bot_token = getenv("BOT_TOKEN")
if not bot_token:
    exit("Error: no token provided")
bot = Bot(token=bot_token)
dp = Dispatcher(bot)

# Получаем данные для подключения к 1С из переменной окружения
login_1c = getenv("LOGIN_1C")
password_1c = getenv("PASSWORD_1C")
url_1c = getenv("URL_1C")


def get_data_from_1c():
    session = Session()
    session.auth = HTTPBasicAuth(login_1c, password_1c)
    client = zeep.Client(wsdl=url_1c, transport=Transport(session=session))
    return client.service.GetSearchData()


def get_partial_ratio(original_string, search_string):
    return fuzz.partial_ratio(original_string.upper(), search_string.upper())


@dp.message_handler(commands=['update'])
async def send_welcome(message: types.Message):
    # Получим данные из 1С
    data_from_1c = json.loads(get_data_from_1c())
    # Удалим текущие записи в таблице
    sql.execute("DELETE FROM goods")
    # Запишем новые данные в ьаблицу
    for element_from_1c in data_from_1c:
        sql.execute(f"INSERT INTO goods VALUES (?, ?, ?, ?)",
                    (
                        element_from_1c['code'],
                        element_from_1c['name'],
                        element_from_1c['botname'],
                        element_from_1c['suppliername']
                    ))
    # Примменим изменения
    db.commit()
    # Выведем сообщение что все сделали
    await message.reply("Обновление каталога завершено")


@dp.message_handler()
async def echo(message: types.Message):

    search_string = message.text
    # Создать структуру для поиска
    search_list = []

    # Получить данные из БД
    goods_data = sql.execute("SELECT * FROM goods")

    # Пройтись по всем данным и заполнить структуру поиска
    for value in goods_data:
        code = value[0]
        max_search_value = 0
        # Наименование
        name = value[1]
        if name != "":
            search_value = get_partial_ratio(name, search_string)
            if search_value > max_search_value:
                max_search_value = search_value

        # botname
        botname = value[2]
        if botname != "":
            search_value = get_partial_ratio(botname, search_string)
            if search_value > max_search_value:
                max_search_value = search_value

        # suppliername
        suppliername = value[3]
        if suppliername != "":
            search_value = get_partial_ratio(suppliername, search_string)
            if search_value > max_search_value:
                max_search_value = search_value

        search_list.append({'code': code, 'name': name, 'search_value': max_search_value})

    # Отсортировать данные
    search_list = sorted(search_list, key=lambda search_value: search_value['search_value'], reverse=True)

    message_text = ""
    i = 0
    for search_item in search_list:
        message_text += search_item["name"] + "\n"
        i += 1
        if i >= 10:
            break


    # Вывести 10 самых подходящих

    await message.answer(message_text)


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
