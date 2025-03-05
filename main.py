from time import sleep
from urllib.parse import quote
from configs.database import client  # Клиент ClickHouse
import asyncio
import aiohttp
import requests
import logging
from datetime import datetime
from configs.proxy import proxy_list  # Список проксизщ


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Ограничение на количество одновременных запросов
SEMAPHORE_LIMIT = 10
# Задержка между запросами (в секундах)
DELAY_BETWEEN_REQUESTS = 2
# Максимальное кол-во ошибок подряд перед тайм-аутом
MAX_CONSECUTIVE_ERRORS = 5
# Таймм-аут при превышении лимита ошибок
ERROR_TIMEOUT = 60


async def product_check(products, page):
    if not products:  # Если список товаров пуст
        print(f"На странице {page} нет товаров. Прекращаем парсинг категории.")
        return True  # Устанавливаем флаг для остановки парсинга
    return False


async def fetch(session, url, headers, proxy):
    async with session.get(url, headers=headers, proxy=proxy) as response:
        if response.status == 429:
            logger.warning(
                f"Ошибка 429 на странице. Попробую снова через 7 секунд.")
            await asyncio.sleep(7)
            return await fetch(session, url, headers, proxy)
        elif response.status == 200:
            return await response.json()
        else:
            logger.error(f"Ошибка при запросе на страницу: {response.status}")
            await asyncio.sleep(5)
            return await fetch(session, url, headers, proxy)


async def parse_page(session, url_page, page, headers, proxy):
    url = f"{url_page}&page={page}"
    try:
        data = await fetch(session, url, headers, proxy)
        products = data.get("data", {}).get("products", [])

        stop_parsing = await product_check(products, page)
        if stop_parsing:
            return None

        if not products:  # Если список товаров пуст
            logger.info(
                f"На странице {page} нет товаров. Прекращаем парсинг категории.")
            return None

        return [i.get("id") for i in products]

    except Exception as e:
        logger.error(f"Произошла ошибка на странице {page}: {e}")
        await asyncio.sleep(5)
        return await parse_page(session, url_page, page, headers, proxy)


async def parse_category(semaphore, url_page, headers, proxy):
    async with semaphore:
        unic = []
        consecutive_errors = 0

        for page in range(1, 51):
            try:
                async with aiohttp.ClientSession() as session:
                    products_ids = await parse_page(session, url_page, page, headers, proxy)
                    if products_ids is None:
                        break
                    unic.extend(products_ids)
                    logger.info(f"Страница {page} завершена")

                # Задержка между запросами
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
                consecutive_errors = 0  # Сбрасываем счетчик ошибок

            except Exception as e:
                print(f"Ошибка при парсинге страницы {page}: {e}")
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    logger.warning(
                        f"Превышено максимальное количество ошибок. Тайм-аут {ERROR_TIMEOUT} секунд.")
                    await asyncio.sleep(ERROR_TIMEOUT)
                    consecutive_errors = 0  # Сбрасываем счетчик ошибок после тайм-аута

        await database(unic)
        logger.info("Категория завершена")
        await asyncio.sleep(7)


async def get_products_id(url_list, proxy_list, headers):
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    tasks = []

    for url_page, proxy in zip(url_list, proxy_list):
        proxy_url = proxy
        task = asyncio.create_task(parse_category(
            semaphore, url_page, headers, proxy_url))
        tasks.append(task)

    await asyncio.gather(*tasks)
    print("Работа выполнена")


# -----------------------------------------------


def get_category():

    url = "https://static-basket-01.wbbasket.ru/vol0/data/main-menu-ru-ru-v3.json"

    headers = {
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://www.wildberries.ru/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
    }

    response = requests.get(url=url, headers=headers)
    print("Статус-код:", response.status_code)

    return response


def items_check(response):

    categorys = []

    for i in response.json():
        if "childs" in i:
            for z in i["childs"]:
                if "childs" in z:
                    for c in z["childs"]:
                        if "childs" in c:
                            for v in c["childs"]:
                                if "childs" in v:
                                    for b in v["childs"]:
                                        categorys.append(
                                            {b.get("shard"): b.get("query")})
                                else:
                                    categorys.append(
                                        {v.get("shard"): v.get("query")})
                        else:
                            categorys.append(
                                {c.get("shard"): c.get("query")})
                else:
                    categorys.append(
                        {z.get("shard"): z.get("query")})

    return categorys


def is_valid_string(value):
    """
    Проверяет, что строка:
    - Не состоит только из цифр.
    - Не состоит только из символов (не букв и не цифр).
    - Может состоять из букв, букв и цифр, или букв и символов.
    """
    # Проверка, что строка не состоит только из цифр
    if value.isdigit():
        return False

    # Проверка, что строка не состоит только из символов (не букв и не цифр)
    if all(not char.isalnum() for char in value):
        return False

    # Если строка прошла обе проверки, она валидна
    return True


def import_csv_to_db(csv_file_path, batch_size=10000):
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)

        data_to_insert = []
        invalid_rows = 0
        total_inserted = 0

        for row in reader:
            string_value = row[0].strip()
            int_value = int(row[1].strip())

            if not is_valid_string(string_value):
                print(
                    f"Пропущена строка: {string_value} - не соответствует требованиям.")
                invalid_rows += 1
                continue

            data_to_insert.append((string_value, int_value))

            # Вставляем данные пакетами
            if len(data_to_insert) >= batch_size:
                try:
                    client.execute(
                        'INSERT INTO ai.search (query, count) VALUES',
                        data_to_insert
                    )
                    total_inserted += len(data_to_insert)
                    print(
                        f"Вставлено {len(data_to_insert)} строк. Всего вставлено: {total_inserted}")
                except Exception as e:
                    print(f"Ошибка при вставке данных в БД: {e}")
                data_to_insert = []  # Очищаем список для следующего пакета

        # Вставляем оставшиеся данные
        if data_to_insert:
            try:
                client.execute(
                    'INSERT INTO ai.search (query, count) VALUES',
                    data_to_insert
                )
                total_inserted += len(data_to_insert)
                print(
                    f"Вставлено {len(data_to_insert)} строк. Всего вставлено: {total_inserted}")
            except Exception as e:
                print(f"Ошибка при вставке данных в БД: {e}")

        print(
            f"Импорт завершен. Всего вставлено: {total_inserted}, пропущено: {invalid_rows}")


def reset_activity():
    """
    Обнуляет все значения активности в таблице перед началом парсинга.
    Использует ручное обновление через INSERT и SELECT.
    """
    try:
        # Создаем новую таблицу с обновленными значениями
        client.execute("""
            CREATE TABLE ai.all_sku_new 
            ( 
                sku Int64, 
                date_add DateTime,  
                status Int8
            ) 
            ENGINE = ReplacingMergeTree(status) 
            ORDER BY sku 
            SETTINGS index_granularity = 8192
                """)

        # Вставляем данные с обнуленным статусом
        client.execute("""
            INSERT INTO all_sku_new
            SELECT sku, date_add, 0
            FROM all_sku
        """)

        # Удаляем старую таблицу
        client.execute("DROP TABLE all_sku")

        # Переименовываем новую таблицу
        client.execute("RENAME TABLE all_sku_new TO all_sku")

        print("Активность всех артикулов обнулена.")
    except Exception as e:
        print(f"Ошибка при обнулении активности: {e}")

    try:
        print("Выполняем принудительное слияние данных...")
        client.execute("OPTIMIZE TABLE all_sku FINAL")
        print("Принудительное слияние завершено.")
    except Exception as e:
        print(f"Ошибка при выполнении принудительного слияния: {e}")


async def database(unic):
    if not unic:
        logger.info("Список товаров пуст. Пропускаем вставку в БД.")
        return

    client = await get_clickhouse_client()

    try:
        existing_skus_query = f"""
            SELECT sku
            FROM all_sku
            WHERE sku IN ({', '.join(map(str, unic))}) AND status = 1
        """
        existing_skus = set([row[0] for row in await client.execute(existing_skus_query)])
    except Exception as e:
        logger.error(f"Ошибка при получении существующих артикулов: {e}")
        return

    new_skus = [sku for sku in unic if sku not in existing_skus]

    if not new_skus:
        logger.info("Нет новых артикулов для вставки.")
        return

    updated_list = [(sku, datetime.now(), 1) for sku in new_skus]

    try:
        await client.execute(
            'INSERT INTO all_sku (sku, date_add, status) VALUES', updated_list
        )
        logger.info(f"Успешно вставлено {len(updated_list)} артикулов.")
    except Exception as e:
        logger.error(f"Ошибка при вставке артикулов в БД: {e}")

    try:
        logger.info("Выполняем принудительное слияние данных...")
        await client.execute("OPTIMIZE TABLE all_sku FINAL")
        logger.info("Принудительное слияние завершено.")
    except Exception as e:
        logger.error(f"Ошибка при выполнении принудительного слияния: {e}")


# def get_search(proxy_list, headers):
#     pack_size = 5000
#     offset = 0
#     while True:
#         query = f"SELECT query FROM ai.search LIMIT {pack_size} OFFSET {offset}"
#         rows = client.execute(query)

#         if not rows:
#             break

#         get_search_urls(rows, proxy_list=proxy_list, headers=headers)


def get_category_urls(categorys):
    category_urls_list = []
    for i in categorys:
        for shard, query in i.items():
            category_urls_list.append(f"https://catalog.wb.ru/catalog/{shard}/v2/catalog?ab_testing=false&appType=1&{query}&curr=rub&dest=-365403&lang=ru&sort=popular&spp=30&uclusters=3&uiv=8&uv=QIPAIUWVuZM4JDP0OG63lUJ4P6i9eUN1N3PA-cUsPc237y-PvwlER0UAOeLDPMVPPMzBQCtAwKS9ZEEJNfw-KToivZO6HsSxwfO5Zjb8P-lAPT9YQFe8mcQxu7w-7LyVw3XETUPrw3hBtEJnsgdByT-xwRQ7e0QhuYfFazKcPL8_2L7fPXnCCj4rLjdC0MALtoQ_jbhpQhe4IUB1upg6mT9_wO26SL9hP7qtiLSJPiE8vEFFulFAF777wYy528R6PEPDZLTIuuQekTtiQHm0MTpQvIOwT0BUwLBDeMTfvzAwL0QLPNg_VZmqsU24N6_2wUi5v8CVPkU_2DKKQMZBmg")

    return category_urls_list


# def get_search_urls(rows, proxy_list, headers):
#     search_urls_list = []
#     for i in rows:
#         if isinstance(i, tuple) and len(i) > 0:
#             query_str = i[0]  # Берём первый элемент кортежа
#         else:
#             query_str = str(i)  # Если это не кортеж, преобразуем в строку
#         encoded_i = quote(query_str)
#         search_urls_list.append(
#             f"https://search.wb.ru/exactmatch/ru/common/v9/search?ab_testing=false&appType=1&curr=rub&dest=-365403&lang=ru&query={encoded_i}&resultset=catalog&sort=popular&spp=30&suppressSpellcheck=false&uclusters=3&uiv=8&uv=QIPAIUWVuZM4JDP0OG63lUJ4P6i9eUN1N3PA-cUsPc237y-PvwlER0UAOeLDPMVPPMzBQCtAwKS9ZEEJNfw-KToivZO6HsSxwfO5Zjb8P-lAPT9YQFe8mcQxu7w-7LyVw3XETUPrw3hBtEJnsgdByT-xwRQ7e0QhuYfFazKcPL8_2L7fPXnCCj4rLjdC0MALtoQ_jbhpQhe4IUB1upg6mT9_wO26SL9hP7qtiLSJPiE8vEFFulFAF777wYy528R6PEPDZLTIuuQekjtiQHm0MTpQvIOwT0BUwLBDeMTfvzAwL0QLPNg_VZmqsU24N6_2wUi5v8CVPkU_2DKKQMZBmg")
#     get_products_id(url_list=search_urls_list,
#                     proxy_list=proxy_list, headers=headers)


if __name__ == '__main__':
    headers = {
        'accept': '*/*',
        'accept-language': 'ru,en;q=0.9',
        'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDEwMTQ4MTQsInZlcnNpb24iOjIsInVzZXIiOiIyMzA2MDc0NyIsInNoYXJkX2tleSI6IjE0IiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiNGJmZjExODlhY2UyNDc2ZGIxMzNmMGY0YWE0YTE2ZjUiLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY3ODA2MDIxNCwidmFsaWRhdGlvbl9rZXkiOiI3ZjBiNjkwMzQ0OTkzZDdmNGVlNjc3YzFjYzQ0YWMxMDA4MmQ3MmVhZmM2N2UxOWM0ZGUzZDEyNTAzZmQ5NjdjIiwicGhvbmUiOiJWNE45VW9Pei9ZS3R4R3lwV2JuZWlnPT0ifQ.ZoeIAdxDQAzGXS4t4aGAmhwtFn2nQl0IxQz2JRGCgggQIoYfcdRgZr7KKutUBdkNcyLL9yskz5XeDAlvpjgz_IluthqjYKG-Dk5WDeM_okphAMQGVdMMPQWWQfYRe72b9rRgUhHGQwxivMOMgnRamWXJ0kKNdUP_j8ksn4WL0cFuOrsaMucV95AMW7AZhXEsYfqJNJu8svR30fuND7hP-9wf0ussyCdvL4lFq05UKxkUqzSeT6_xf8ZV9Y2jQT1RUO4gJzOoZEvzUVR9g3epSi4IrkpUXgzbaPIgliM_1JGW8i4-oCUjxacQL3DyCJxI6IcO8ILvFj4V4vQ5EXxILg',
        'origin': 'https://www.wildberries.ru',
        'priority': 'u=1, i',
        'referer': 'https://www.wildberries.ru',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36',
        'x-queryid': 'qid1053173201172465741120250305003145',
        'x-userid': '23060747',
    }

    asyncio.run(get_products_id(get_category_urls(items_check(
        get_category())), proxy_list=proxy_list, headers=headers))

    # asyncio.run(get_products_id(get_category_urls(items_check(
    #     get_category())), proxy_list=proxy_list, headers=headers))
    # get_search(proxy_list=proxy_list, headers=headers)
