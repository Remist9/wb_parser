import requests
import time
from time import sleep
from bs4 import BeautifulSoup
import json


def get_category():
    print("hello")
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

    with open("response.json", "w", encoding="utf-8") as f:
        # буфер что-бы не спамить сайт ВБ запросами
        json.dump(response.json(), f, ensure_ascii=False, indent=4)
    return


def items_check():
    with open("response.json", "r", encoding="utf-8") as f:
        response = json.load(f)

    with open("categorys.json", "w", encoding="utf-8") as f:
        json.dump({}, f, ensure_ascii=False, indent=4)
    categorys = {}

    for i in response:
        if "childs" in i:
            category_name = i.get("name")
            sub_categories = i["childs"]
            for z in sub_categories:
                if "seo" in z:
                    sub_category_name = z.get("seo")
                else:
                    sub_category_name = category_name + "->" + z.get("name")
                sub_category_url = "https://www.wildberries.ru" + z.get("url")
                categorys[sub_category_name] = sub_category_url

    with open("categorys.json", "w", encoding="utf-8") as f:
        json.dump(categorys, f, ensure_ascii=False, indent=4)

    return


if __name__ == '__main__':
    # get_category() #спарсили json где все категории и суб-категории
    items_check()
    pass
