from clickhouse_driver import Client

client = Client(
    host='localhost',  # Адрес сервера
    port=9000,         # Порт ClickHouse (по умолчанию 9000 для TCP)
    user='user',    # Имя пользователя
    password='user',       # Пароль (если есть)
    database='ai'  # База данных
)
