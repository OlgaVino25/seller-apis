import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает пагинированный список товаров через озон Seller API.

    В случае ошибки HTTP поднимает исключение requests.HTTPError.

    Args:
        last_id (str): Идентификатор последнего полученного товара.
        client_id (str): Идентификатор клиента озон.
        seller_token (str): API-ключ продавца.
    
    Returns:
        dict: Словарь с результатом из ответа API:
            {
                "items": list[dict],  # Список товаров
                "total": int,         # Всего товаров
                "last_id": str        # Идентификатор последнего значения на странице
            }
    
    Raises:
        requests.HTTPError: В случае ошибки HTTP-запроса
    
    Examples:
        Некорректный API-ключ:
            >>> get_product_list("", "valid_client", "invalid_token")
            Traceback (most recent call last):
            ...
            requests.HTTPError: 403 Client Error: Forbidden
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает список всех артикулов товаров магазина озон.
    
    Использует get_product_list() для полного обхода товаров через пагинацию.

    Args:
        client_id (str): Идентификатор клиента озон.
        seller_token (str): API-ключ продавца.
    
    Returns:
        list[str]: Список артикулов товаров (offer_id).
        Возвращает пустой список, если товаров нет или произошла ошибка.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров в магазине озон.

    Отправляет новые цены для одного или нескольких товаров.

    Returns:
        dict: Словарь с ответом API после обработки запроса.
    
    Raises:
        requests.HTTPError: В случае ошибки HTTP-запроса
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров в магазине озон.

    Отправляет новые остатки для одного или нескольких товаров.

    Returns:
        dict: Словарь с ответом API после обработки запроса.
    
    Raises:
        requests.HTTPError: В случае ошибки HTTP-запроса
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл ostatki с сайта casio.
    
    Скачивает zip-архив с актуальными остатками с сайта timeworld.ru.
    Извлекает файл 'ostatki.xls' и читает данные из excel-файла.
    Преобразует данные в список словарей.
    Удаляет временные файлы.

    Returns:
        list[dict]: Список словарей с информацией о часах.
    
    Raises:
        requests.HTTPError: В случае ошибки HTTP-запроса
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список остатков товаров для обновления озон.
    
    Обрабатывает данные о наличии часов из файла дистрибьютора и сопоставляет их
    с артикулами товаров в личном кабинете озон.

    Args:
        watch_remnants (list[dict]): Список словарей с данными о наличии часов.
            Каждый словарь должен содержать ключи:
            - "Код": артикул товара
            - "Количество": количество на складе
        offer_ids (list[str]): Список артикулов товаров из озон.
    
    Returns:
        list[dict]: Список словарей в формате для озон:
            [{
                "offer_id": "артикул товара",
                "stock": количество (0-100)
            }]
    
    Notes:
        - Преобразует значение ">10" в 100
        - Значение "1" преобразуется в 0
        - Товары из offer_ids, отсутствующие в watch_remnants, добавляются с stock=0
        - Удаляет обработанные offer_ids из входного списка
    
    Examples:
        >>> remnants = [{"Код": "CASIO-123", "Количество": "5"}]
         >>> ids = ["CASIO-123", "CASIO-456"]
         >>> create_stocks(remnants, ids)
         [
            {"offer_id": "CASIO-123", "stock": 5},
            {"offer_id": "CASIO-456", "stock": 0}
         ]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Формирует список цен для обновления в озон.

    Преобразует данные о ценах из файла дистрибьютора в формат,
    необходимый для загрузки цен на озн.

    Args:
        watch_remnants (list[dict]): Список словарей с данными о наличии часов.
            Каждый словарь должен содержать ключи:
            - "Код": артикул товара
            - "Количество": количество на складе
        offer_ids (list[str]): Список артикулов товаров из озон.

    Returns:
        list[dict]: Список словарей в формате озон:
            [{
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "артикул",
                "old_price": "0",
                "price": "новая цена"
            }]

    Notes:
        - Использует функцию price_conversion() для преобразования формата цены
        - Включает только товары, присутствующие в обоих списках
        - Устанавливает old_price="0" (без старой цены)
        - Валюта всегда RUB

    Examples:
        >>> remnants = [{"Код": "CASIO-123", "Цена": "6'300.50 руб."}]
        >>> ids = ["CASIO-123"]
        >>> create_prices(remnants, ids)
        [{
            "auto_action_enabled": "UNKNOWN",
            "currency_code": "RUB",
            "offer_id": "CASIO-123",
            "old_price": "0",
            "price": "6300"
        }]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует цену в формат для озон.
    
    Убирает из цены все символы, кроме цифр от 0 до 9.

    Args:
        price(str): Строка с ценой из файла дистрибьютора. Например: "5'990.00 руб."
    
    Returns:
        str: Цена без лишних символов, только цифры. Например: "5990"
    
    Examples:
        Пример корректного исполнения функции:
        >>> price_conversion("13'899.99 руб.")
        "13899"

        Пример некорректного исполнения функции:
        >>> price_conversion(6300.50)  # число вместо строки
        Traceback (most recent call last):
        ...
        AttributeError: 'float' object has no attribute 'split'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Генератор, который выдаёт элементы списка lst по частям по n элементов.
    
    Выполняет итерацию элементов списка lst с шагом n.

    Цикл for в сочетании с функцией range() позволяет получать доступ к элементам списка по их индексу. Третий аргумент функции range() определяет шаг, с которым будет двигаться цикл.
    По умолчанию шаг равен 1.

    Args:
        lst (list): Исходный список для разделения.

    Yields:
        list: Список элементов длиной не более `n`.

    Examples:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

        Некорректный размер:
        >>> list(divide([1, 2, 3], 0))
        Traceback (most recent call last):
        ...
        ValueError: n must be greater than 0
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет цены товаров в озон и возвращает результаты.
    
    Процесс работы:
    - Получает список всех артикулов из озон
    - Формирует данные для обновления цен на основе прайса дистрибьютора и актуальных артикулов магазина
    - Обновляет цены по 1000 товаров
    - Возвращает сформированные данные о ценах

    Args:
        watch_remnants (list[dict]): Список словарей с данными о наличии часов.
            Каждый словарь должен содержать ключи:
            - "Код": артикул товара
            - "Количество": количество на складе
        client_id (str): Идентификатор клиента озон.
        seller_token (str): API-ключ продавца.

    Returns:
        list[dict]: Список всех сформированных цен в формате озон:
            [{
                "offer_id": "артикул",
                "price": "цена",
                "currency_code": "RUB",
                "auto_action_enabled": "UNKNOWN",
                "old_price": "0"
            }]
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет остатки товаров в озон и возвращает результаты.
    
    Процесс работы:
    - Получает список всех артикулов из озон
    - Формирует данные об остатках на основе файла дистрибьютора
    - Обновляет остатки по 100 товаров
    - Фильтрует и возвращает результаты

    Args:
        watch_remnants (list[dict]): Список словарей с данными о наличии часов.
            Каждый словарь должен содержать ключи:
            - "Код": артикул товара
            - "Количество": количество на складе
        client_id (str): Идентификатор клиента озон.
        seller_token (str): API-ключ продавца.

    Returns:
        tuple[list[dict], list[dict]]: Кортеж из двух списков:
            - not_empty: товары с ненулевым остатком
            - stocks: все обновленные товары с остатками
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
