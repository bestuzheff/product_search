from os import getenv
import zeep
from aiogram import Bot, Dispatcher, executor, types
import sqlite3
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep.transports import Transport
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


@dp.message_handler(commands=['update'])
async def send_welcome(message: types.Message):
    # Получим данные из 1С
    dataFrom1C = json.loads(get_data_from_1c())
    # Удалим текущие записи в таблице
    sql.execute("DELETE FROM goods")
    # Запишем новые данные в ьаблицу
    for elDataFrom1C in dataFrom1C:
        sql.execute(f"INSERT INTO goods VALUES (?, ?, ?, ?)",
                    (
                        elDataFrom1C['code'],
                        elDataFrom1C['name'],
                        elDataFrom1C['botname'],
                        elDataFrom1C['suppliername']
                    ))
    # Примменим изменения
    db.commit()
    # Выведем сообщение что все сделали
    await message.reply("Обновление каталога завершено")


@dp.message_handler()
async def echo(message: types.Message):
    # old style:
    # await bot.send_message(message.chat.id, message.text)

    await message.answer(message.text)


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
