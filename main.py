import asyncio
import aiohttp
import requests
import json
import gzip
import io
from datetime import datetime
from configs.proxy import proxy_list  # Список прокси
from configs.database import client  # Клиент ClickHouse
from urllib.parse import quote

# Ограничение на количество одновременных запросов
SEMAPHORE_LIMIT = 10
# Задержка между запросами (в секундах)
DELAY_BETWEEN_REQUESTS = 2
PAGE_LIMIT = 50
FETCH_TIMEOUT = 10

# -------------------------------------------------------------------------- Общая часть


async def fetch(session, url, headers, proxy):
    """
    Выполняет запрос с использованием одного прокси.
    Бесконечно пытается подключиться, пока запрос не будет успешным.
    Если 5 раз подряд возникает ошибка 404, пропускаем категорию.
    """
    error_count = 0  # Счетчик ошибок подряд
    not_found_count = 0  # Счетчик ошибок 404 подряд

    while True:
        try:
            async with session.get(url, headers=headers, proxy=proxy) as response:
                if response.status == 200:
                    error_count = 0  # Сбрасываем счетчик ошибок при успешном запросе
                    not_found_count = 0  # Сбрасываем счетчик 404
                    # Декодируем Gzip
                    if 'application/json' in response.headers.get('content-type'):
                        return await response.json()
                    elif response.headers.get('content-encoding') == 'gzip':
                        buf = await response.read()
                        with gzip.GzipFile(fileobj=io.BytesIO(buf)) as f:
                            data = f.read().decode('utf-8')
                    else:
                        data = await response.text()
                    try:
                        json_data = json.loads(data)
                        return json_data
                    except json.JSONDecodeError:
                        print("Полученные данные не являются корректным JSON.")
                        return None
                elif response.status == 404:
                    not_found_count += 1  # Увеличиваем счетчик 404
                    error_count += 1  # Увеличиваем общий счетчик ошибок
                    print(
                        f"Ошибка 404: Страница не найдена. Повторная попытка... Ошибок 404 подряд: {not_found_count}")

                    # Если 5 раз подряд 404, пропускаем категорию
                    if not_found_count >= 5:
                        print("Пропускаем категорию из-за 5 ошибок 404 подряд.")
                        return None  # Пропускаем категорию
                else:
                    error_count += 1  # Увеличиваем счетчик ошибок
                    print(
                        f"Ошибка при запросе: {response.status}. Повторная попытка... Ошибок подряд: {error_count}")

                # Увеличиваем тайм-аут до 60 секунд после 5 ошибок подряд
                if error_count > 5:
                    await asyncio.sleep(60)  # Тайм-аут 1 минута
                else:
                    await asyncio.sleep(7)  # Тайм-аут 7 секунд
        except Exception as e:
            error_count += 1  # Увеличиваем счетчик ошибок
            print(
                f"Произошла ошибка: {e}. Повторная попытка... Ошибок подряд: {error_count}")

            # Увеличиваем тайм-аут до 60 секунд после 5 ошибок подряд
            if error_count > 5:
                await asyncio.sleep(60)  # Тайм-аут 1 минута
            else:
                await asyncio.sleep(7)  # Тайм-аут 7 секунд


async def process_page(session, url, headers, proxy, semaphore):
    """
    Обрабатывает одну страницу категории.
    """
    async with semaphore:
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)  # Задержка между запросами
        data = await fetch(session, url, headers, proxy)
        if data:
            return data.get("data", {}).get("products", [])
        else:
            return []

# -------------------------------------------------------------------------- Часть для работы с категориями


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

    categorys = {}

    for i in response.json():
        if "childs" in i:
            for z in i["childs"]:
                if "childs" in z:
                    for c in z["childs"]:
                        if "childs" in c:
                            for v in c["childs"]:
                                if "childs" in v:
                                    for b in v["childs"]:
                                        categorys[b.get("id")] = {
                                            b.get("shard"): b.get("query")}
                                else:
                                    categorys[v.get("id")] = {
                                        v.get("shard"): v.get("query")}
                        else:
                            categorys[c.get("id")] = {
                                c.get("shard"): c.get("query")}
                else:
                    categorys[z.get("id")] = {
                        z.get("shard"): z.get("query")}

    return categorys


async def process_category(cat_shard, cat_query, headers, proxy):
    """
    Обрабатывает одну категорию (50 страниц) с использованием одного прокси.
    Если fetch возвращает None, пропускаем категорию.
    """
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    unic = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for page in range(1, 51):  # 50 страниц
            url = f"https://catalog.wb.ru/catalog/{cat_shard}/v2/catalog?ab_testing=false&appType=1&{cat_query}&curr=rub&dest=-365403&lang=ru&page={page}&sort=popular&spp=30&uclusters=3&uiv=8&uv=QIPAIUWVuZM4JDP0OG63lUJ4P6i9eUN1N3PA-cUsPc237y-PvwlER0UAOeLDPMVPPMzBQCtAwKS9ZEEJNfw-KToivZO6HsSxwfO5Zjb8P-lAPT9YQFe8mcQxu7w-7LyVw3XETUPrw3hBtEJnsgdByT-xwRQ7e0QhuYfFazKcPL8_2L7fPXnCCj4rLjdC0MALtoQ_jbhpQhe4IUB1upg6mT9_wO26SL9hP7qtiLSJPiE8vEFFulFAF777wYy528R6PEPDZLTIuuQekTtiQHm0MTpQvIOwT0BUwLBDeMTfvzAwL0QLPNg_VZmqsU24N6_2wUi5v8CVPkU_2DKKQMZBmg"
            tasks.append(process_page(session, url, headers, proxy, semaphore))

        results = await asyncio.gather(*tasks)
        for products in results:
            if products is None:  # Если fetch вернул None, пропускаем категорию
                print(f"Категория {cat_shard} пропущена из-за ошибок 404.")
                return None
            for product in products:
                unic.append(product.get("id"))
    return unic


async def database(unic):
    """
    Выгружает данные в БД.
    """
    # Проверяем, не пуст ли список unic
    if not unic:
        print("Список товаров пуст. Пропускаем вставку в БД.")
        return

    # Получаем список артикулов, которые уже есть в БД со статусом 1
    try:
        existing_skus_query = f"""
            SELECT sku
            FROM all_sku
            WHERE sku IN ({', '.join(map(str, unic))}) AND status = 1
        """
        existing_skus = set([row[0]
                            for row in client.execute(existing_skus_query)])
    except Exception as e:
        print(f"Ошибка при получении существующих артикулов: {e}")
        return

    # Фильтруем артикулы: оставляем только те, которых нет в existing_skus
    new_skus = [sku for sku in unic if sku not in existing_skus]

    # Если новых артикулов нет, завершаем выполнение
    if not new_skus:
        print("Нет новых артикулов для вставки.")
        return

    # Преобразуем список новых артикулов в список кортежей для вставки
    # Все артикулы добавляются со статусом 1
    updated_list = [(sku, datetime.now(), 1) for sku in new_skus]

    # Вставляем данные в БД
    try:
        client.execute(
            'INSERT INTO all_sku (sku, date_add, status) VALUES', updated_list)
        print(f"Успешно вставлено {len(updated_list)} артикулов.")
    except Exception as e:
        print(f"Ошибка при вставке артикулов в БД: {e}")

    try:
        print("Выполняем принудительное слияние данных...")
        client.execute("OPTIMIZE TABLE all_sku FINAL")
        print("Принудительное слияние завершено.")
    except Exception as e:
        print(f"Ошибка при выполнении принудительного слияния: {e}")


async def category_worker(queue, headers, proxy):
    while True:
        try:
            category_id, category_data = queue.get_nowait()
        except asyncio.QueueEmpty:
            break  # Очередь пуста, завершаем работу

        try:
            cat_shard = list(category_data.keys())[0]
            cat_query = list(category_data.values())[0]
            if cat_query != "null" and cat_shard != "null":
                print(
                    f"Начата обработка категории {category_id} с прокси {proxy}")
                unic = await process_category(cat_shard, cat_query, headers, proxy)
                await database(unic)
                print(
                    f"Категория {category_id} завершена (ID: {category_id} : {datetime.now()})")
                with open("log.txt", "a", encoding="utf-8") as f:
                    f.write(
                        f"Категория завершена (ID: {category_id} : {datetime.now()})\n")
        except Exception as e:
            print(f"Ошибка при обработке категории {category_id}: {e}")
        finally:
            queue.task_done()


async def category_parser(categorys):
    """
    Основная функция для парсинга категорий.
    """
    headers = {
        'accept': '*/*',
        'accept-language': 'ru,en;q=0.9',
        'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDA2NTc1ODksInZlcnNpb24iOjIsInVzZXIiOiIyMzA2MDc0NyIsInNoYXJkX2tleSI6IjE0IiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiNGJmZjExODlhY2UyNDc2ZGIxMzNmMGY0YWE0YTE2ZjUiLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY3ODA2MDIxNCwidmFsaWRhdGlvbl9rZXkiOiI3ZjBiNjkwMzQ0OTkzZDdmNGVlNjc3YzFjYzQ0YWMxMDA4MmQ3MmVhZmM2N2UxOWM0ZGUzZDEyNTAzZmQ5NjdjIiwicGhvbmUiOiJWNE45VW9Pei9ZS3R4R3lwV2JuZWlnPT0ifQ.B-QwJwhWKcz5UDtocUY1cWpfvylMUazSDPEotDufMsLWiw32zw2fQbvZXdGRXg8Bb9gFeeL6ox0wjojGSm3Lu1sOTurYOJ5Xwees83sAw5agjIeHcJyMZHTblVE4gMEv96kr64FVVQs72Spo8z4vCwbRJcRhulP20nNjzHKTmCK8ZfZqWG_3fynFYtHHTY2NmvQJ567IHhnmO7mXaKV2TpFWMXSF6eDaG6NPbOUid1o-gdgHiOWFt3MFGaQTYfwERDAgc_zy3rJcCZFHuoMK9hSVZeescaJ7rNW4R9eX8ajjnprmPF5HlXL0NaF7Z28HheP8Pe4ZaC0NJUiDdRdi7A',
        'origin': 'https://www.wildberries.ru',
        'priority': 'u=1, i',
        'referer': 'https://www.wildberries.ru/catalog/zhenshchinam/odezhda/bluzki-i-rubashki',
        'sec-ch-ua': '"Not A(Brand";v="8", "Chromium";v="132", "YaBrowser";v="25.2", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 YaBrowser/25.2.0.0 Safari/537.36',
    }

    with open("log.txt", "w", encoding="utf-8") as f:
        f.write("")

    if not proxy_list:
        raise ValueError(
            "Список прокси пуст. Проверьте файл configs/proxy.py.")

    # Создаем очередь категорий
    queue = asyncio.Queue()

    # Добавляем все категории в очередь
    for category_id, category_data in categorys.items():
        queue.put_nowait((category_id, category_data))

    # Создаем worker'ов для обработки категорий
    workers = []
    for proxy in proxy_list[:10]:  # Используем первые 10 прокси
        worker = asyncio.create_task(category_worker(queue, headers, proxy))
        workers.append(worker)

    # Ждем завершения всех задач в очереди
    await queue.join()

    # Отменяем worker'ов после завершения
    for w in workers:
        w.cancel()

    print("Работа выполнена")

# -------------------------------------------------------------------------- Обнуоение активности sku + перенос запросов в БД


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


def get_search(pack_size, offset):
    while True:
        query = f"SELECT query FROM ai.search LIMIT {pack_size} OFFSET {offset}"
        rows = client.execute(query)

        if not rows:
            break

        offset += pack_size

        return rows, offset


def get_search_urls(rows):
    search_urls_list = []
    for i in rows:
        if isinstance(i, tuple) and len(i) > 0:
            query_str = i[0]  # Берём первый элемент кортежа
        else:
            query_str = str(i)  # Если это не кортеж, преобразуем в строку
        encoded_i = quote(query_str)
        search_urls_list.append(
            f"https://search.wb.ru/exactmatch/ru/common/v9/search?ab_testing=false&appType=1&curr=rub&dest=-365403&lang=ru&query={encoded_i}&resultset=catalog&sort=popular&spp=30&suppressSpellcheck=false&uclusters=3&uiv=8&uv=QIPAIUWVuZM4JDP0OG63lUJ4P6i9eUN1N3PA-cUsPc237y-PvwlER0UAOeLDPMVPPMzBQCtAwKS9ZEEJNfw-KToivZO6HsSxwfO5Zjb8P-lAPT9YQFe8mcQxu7w-7LyVw3XETUPrw3hBtEJnsgdByT-xwRQ7e0QhuYfFazKcPL8_2L7fPXnCCj4rLjdC0MALtoQ_jbhpQhe4IUB1upg6mT9_wO26SL9hP7qtiLSJPiE8vEFFulFAF777wYy528R6PEPDZLTIuuQekTtiQHm0MTpQvIOwT0BUwLBDeMTfvzAwL0QLPNg_VZmqsU24N6_2wUi5v8CVPkU_2DKKQMZBmg")

    return search_urls_list
# -------------------------------------------------------------------------- Часть для работы с запросами


async def process_search(url, headers, proxy):
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    unic = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for page in range(1, 51):
            page_url = url + f"&page={page}"
            tasks.append(process_page(session, page_url,
                                      headers, proxy, semaphore))

        results = await asyncio.gather(*tasks)
        for products in results:
            if products is None:
                print(f"Запрос пропущем из за ошибки 404")
                return None
            for product in products:
                unic.append(product.get("id"))
    return unic


async def search_worker(queue, headers, proxy):
    while True:
        try:
            url_mask = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        try:
            print(f"Начата обработка нового поискового запроса")
            unic = await process_search(url_mask, headers, proxy)
            await database(unic)
            print("Запрос завершен")
        except Exception as e:
            print(f"Ошибка при обработке запроса{e}")
        finally:
            queue.task_done()


async def search_parser(url_list, headers):
    if not proxy_list:
        raise ValueError(
            "Список прокси пуст. Проверьте файл configs/proxy.py.")
    queue = asyncio.Queue()
    for url_mask in url_list:
        queue.put_nowait(url_mask)
    workers = []
    for proxy in proxy_list[:10]:
        worker = asyncio.create_task(search_worker(queue, headers, proxy))
        workers.append(worker)

    await queue.join()

    for w in workers:
        w.cancel()

    print("Работа выполнена")


if __name__ == '__main__':
    headers = {
        'accept': '*/*',
        'accept-language': 'ru,en;q=0.9',
        'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE3NDA2NTc1ODksInZlcnNpb24iOjIsInVzZXIiOiIyMzA2MDc0NyIsInNoYXJkX2tleSI6IjE0IiwiY2xpZW50X2lkIjoid2IiLCJzZXNzaW9uX2lkIjoiNGJmZjExODlhY2UyNDc2ZGIxMzNmMGY0YWE0YTE2ZjUiLCJ1c2VyX3JlZ2lzdHJhdGlvbl9kdCI6MTY3ODA2MDIxNCwidmFsaWRhdGlvbl9rZXkiOiI3ZjBiNjkwMzQ0OTkzZDdmNGVlNjc3YzFjYzQ0YWMxMDA4MmQ3MmVhZmM2N2UxOWM0ZGUzZDEyNTAzZmQ5NjdjIiwicGhvbmUiOiJWNE45VW9Pei9ZS3R4R3lwV2JuZWlnPT0ifQ.B-QwJwhWKcz5UDtocUY1cWpfvylMUazSDPEotDufMsLWiw32zw2fQbvZXdGRXg8Bb9gFeeL6ox0wjojGSm3Lu1sOTurYOJ5Xwees83sAw5agjIeHcJyMZHTblVE4gMEv96kr64FVVQs72Spo8z4vCwbRJcRhulP20nNjzHKTmCK8ZfZqWG_3fynFYtHHTY2NmvQJ567IHhnmO7mXaKV2TpFWMXSF6eDaG6NPbOUid1o-gdgHiOWFt3MFGaQTYfwERDAgc_zy3rJcCZFHuoMK9hSVZeescaJ7rNW4R9eX8ajjnprmPF5HlXL0NaF7Z28HheP8Pe4ZaC0NJUiDdRdi7A',
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
        "Accept-Encoding": "identity"
    }

    reset_activity()
    asyncio.run(category_parser(items_check(get_category())))

    offset = 0

    for pack_size in range(5000, 1000000, 5000):
        rows, offset = get_search(pack_size, offset)
        search_urls_list = get_search_urls(rows)
        asyncio.run(search_parser(search_urls_list, headers))
