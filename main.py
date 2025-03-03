import asyncio
import aiohttp
import requests
import random
from datetime import datetime
from configs.proxy import proxy_list  # Список прокси
from configs.database import client  # Клиент ClickHouse

# Ограничение на количество одновременных запросов
SEMAPHORE_LIMIT = 10
# Задержка между запросами (в секундах)
DELAY_BETWEEN_REQUESTS = 2


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
                    return await response.json()
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
    double_check = f"""
        SELECT sku FROM all_sku_1 WHERE sku IN({', '.join(map(str, unic))})
    """
    result = client.execute(double_check)
    existing_skus = set([sku[0] for sku in result])

    double_check = list(set(unic) - existing_skus)
    updated_list = [(item, datetime.now(), 1) for item in double_check]

    client.execute(
        'INSERT INTO all_sku_1 (sku, date_add, status) VALUES', updated_list)


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


if __name__ == '__main__':
    asyncio.run(category_parser(items_check(get_category())))
