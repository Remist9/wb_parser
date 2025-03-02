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
DELAY_BETWEEN_REQUESTS = 1


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


async def fetch(session, url, headers, proxy, proxy_list, retries=3):
    try:
        async with session.get(url, headers=headers, proxy=proxy) as response:
            if response.status == 426:
                print(f"Ошибка 426. Попробую снова через 5 секунд.")
                await asyncio.sleep(5)
                return await fetch(session, url, headers, proxy, proxy_list, retries)
            elif response.status == 200:
                return await response.json()
            else:
                print(f"Ошибка при запросе: {response.status}")
                await asyncio.sleep(5)
                return await fetch(session, url, headers, proxy, proxy_list, retries)
    except Exception as e:
        if retries > 0:
            print(
                f"Произошла ошибка: {e}. Осталось попыток: {retries}. Меняем прокси и повторяем запрос.")
            new_proxy = random.choice(proxy_list)  # Выбираем новый прокси
            return await fetch(session, url, headers, new_proxy, proxy_list, retries - 1)
        else:
            print(f"Превышено количество попыток для URL: {url}")
            return None


async def process_page(session, url, headers, proxy, semaphore, proxy_list):
    async with semaphore:
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)  # Задержка между запросами
        try:
            data = await fetch(session, url, headers, proxy, proxy_list)
            if data:
                return data.get("data", {}).get("products", [])
            else:
                return []
        except Exception as e:
            print(f"Произошла ошибка: {e}. Меняем прокси и повторяем запрос.")
            new_proxy = random.choice(proxy_list)  # Выбираем новый прокси
            return await process_page(session, url, headers, new_proxy, semaphore, proxy_list)


async def process_category(category_id, cat_shard, cat_query, headers, proxy, proxy_list):
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    unic = []
    async with aiohttp.ClientSession() as session:
        tasks = []
        for page in range(1, 51):
            url = f"https://catalog.wb.ru/catalog/{cat_shard}/v2/catalog?ab_testing=false&appType=1&{cat_query}&curr=rub&dest=-365403&lang=ru&page={page}&sort=popular&spp=30&uclusters=3&uiv=8&uv=QIPAIUWVuZM4JDP0OG63lUJ4P6i9eUN1N3PA-cUsPc237y-PvwlER0UAOeLDPMVPPMzBQCtAwKS9ZEEJNfw-KToivZO6HsSxwfO5Zjb8P-lAPT9YQFe8mcQxu7w-7LyVw3XETUPrw3hBtEJnsgdByT-xwRQ7e0QhuYfFazKcPL8_2L7fPXnCCj4rLjdC0MALtoQ_jbhpQhe4IUB1upg6mT9_wO26SL9hP7qtiLSJPiE8vEFFulFAF777wYy528R6PEPDZLTIuuQekTtiQHm0MTpQvIOwT0BUwLBDeMTfvzAwL0QLPNg_VZmqsU24N6_2wUi5v8CVPkU_2DKKQMZBmg"
            tasks.append(process_page(session, url, headers,
                         proxy, semaphore, proxy_list))

        results = await asyncio.gather(*tasks)
        for products in results:
            for product in products:
                unic.append(product.get("id"))
    return unic


async def database(unic):
    double_check = f"""
        SELECT sku FROM all_sku WHERE sku IN({', '.join(map(str, unic))})
    """
    result = client.execute(double_check)
    existing_skus = set([sku[0] for sku in result])

    double_check = list(set(unic)-existing_skus)
    updated_list = [(item, datetime.now(), 1) for item in double_check]

    client.execute(
        'INSERT INTO all_sku (sku, date_add, status) VALUES', updated_list)


async def category_parser(categorys):
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

    if not proxy_list:
        raise ValueError(
            "Список прокси пуст. Проверьте файл configs/proxy.py.")

    for counter, (category_id, category_data) in enumerate(categorys.items(), start=1):
        cat_shard = list(category_data.keys())[0]
        cat_query = list(category_data.values())[0]
        if cat_query != "null" and cat_shard != "null":
            proxy = random.choice(proxy_list)  # Выбираем случайный прокси
            unic = await process_category(category_id, cat_shard, cat_query, headers, proxy, proxy_list)
            await database(unic)
            # Выводим номер и ID категории
            print(f"Категория {counter} завершена (ID: {category_id})")
            await asyncio.sleep(7)  # Задержка между категориями
    print("Работа выполнена")


if __name__ == '__main__':
    asyncio.run(category_parser(items_check(get_category())))
