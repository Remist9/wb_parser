import requests
import time
from time import sleep
import json
from configs.proxy import proxy


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
    with open("categorys.json", "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)  # обнуление

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

    with open("categorys.json", "w", encoding="utf-8") as f:
        json.dump(categorys, f, ensure_ascii=False, indent=4)

    return


def category_parser():
    with open("categorys.json", "r", encoding="utf-8") as f:
        categorys = json.load(f)

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

    unic = {}

    for i in categorys:
        cat_shard = list(categorys[i].keys())[0]
        cat_query = list(categorys[i].values())[0]
        if cat_query != "null" and cat_shard != "null":
            for page in range(1, 51):
                url = f"https://catalog.wb.ru/catalog/{cat_shard}/v2/catalog?ab_testing=false&appType=1&{cat_query}&curr=rub&dest=-365403&lang=ru&page={page}&sort=popular&spp=30&uclusters=3&uiv=8&uv=QIPAIUWVuZM4JDP0OG63lUJ4P6i9eUN1N3PA-cUsPc237y-PvwlER0UAOeLDPMVPPMzBQCtAwKS9ZEEJNfw-KToivZO6HsSxwfO5Zjb8P-lAPT9YQFe8mcQxu7w-7LyVw3XETUPrw3hBtEJnsgdByT-xwRQ7e0QhuYfFazKcPL8_2L7fPXnCCj4rLjdC0MALtoQ_jbhpQhe4IUB1upg6mT9_wO26SL9hP7qtiLSJPiE8vEFFulFAF777wYy528R6PEPDZLTIuuQekTtiQHm0MTpQvIOwT0BUwLBDeMTfvzAwL0QLPNg_VZmqsU24N6_2wUi5v8CVPkU_2DKKQMZBmg"

                while True:
                    try:
                        req = requests.get(
                            url=url, headers=headers, proxies=proxy)
                        if req.status_code == 426:
                            print(
                                f"Ошибка 426 на странице {page}. Попробую снова через 5 секунд.")
                            time.sleep(5)
                            continue
                        if req.status_code == 200:
                            try:
                                data = req.json()
                                print("Статус-код:", req.status_code)
                                for i in data.get("data").get("products"):
                                    item_id = i.get("id")
                                    if item_id not in unic:
                                        unic[item_id] = 1
                                    else:
                                        unic[item_id] = 0
                                print(f"Старница {page} завершена")
                                break
                            except requests.exceptions.JSONDecodeError:
                                print(
                                    f"Ошибка при парсинге JSON на странице {page}. Ответ: {req.text}")
                                time.sleep(5)
                                continue
                        else:
                            print(
                                f"Ошибка при запросе на страницу {page}: {req.status_code}")
                            time.sleep(5)
                            continue

                    except requests.exceptions.RequestException as e:
                        print(f"Произошла ошибка на странице {page}: {e}")
                        time.sleep(5)
                        continue

            print("Категория завершена")
    print("Работа выполнена")

    with open("articul.json", "w", encoding="utf-8") as f:
        json.dump(unic, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    # items_check(get_category())
    category_parser()
    pass
