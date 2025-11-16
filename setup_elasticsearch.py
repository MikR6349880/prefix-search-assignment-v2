# setup_elasticsearch.py для v2 проекта (с исправленным bulk, таймаутом и проверкой)

import xml.etree.ElementTree as ET
# import pandas as pd # Не используется
import re
from typing import Dict, List
import logging
import requests # Импортируем requests для OpenSearch
import time
import json # Импортируем json для работы с телом bulk-запроса

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Конфигурация OpenSearch (для v2)
OPENSEARCH_HOST = "http://localhost:9200" # Убедимся, что это localhost
OPENSEARCH_INDEX_NAME = "catalog_products" # Имя индекса

# Определение маппинга индекса (адаптировано из v1, возможно, без русской морфологии)
INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                # Упрощённый анализатор, использующий стандартные фильтры OpenSearch
                "default_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        # "russian_morphology", # Убираем, если не установлен плагин
                        "stop", # Фильтр стоп-слов (использует встроенные, например, _english_)
                        "snowball" # Лемматизация (использует встроенные, например, English)
                    ],
                    "char_filter": [
                        "html_strip"
                    ]
                },
                # Аналайзер для префиксного поиска (ngram) на основе стандартных токенов
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "keyword", # Используем keyword для ngram
                    "filter": [
                        "lowercase",
                        "edge_ngram_filter" # Переименовали из 'edge_ngram' для ясности
                    ]
                }
            },
            "filter": {
                "edge_ngram_filter": { # Переименовали из 'edge_ngram'
                    "type": "edge_ngram",
                    "min_gram": 1,
                    "max_gram": 20,
                    "token_chars": ["letter"]
                },
                "stop": {
                    "type": "stop",
                    # Используем встроенный список стоп-слов. OpenSearch не всегда имеет _russian_.
                    # Для русского языка может потребоваться плагин или кастомный список.
                    # Пока используем _english_ как пример или оставим пустым.
                    # "stopwords": "_russian_" # <- Может не работать без плагина
                    "stopwords": "_english_" # <- Используем английские стоп-слова как пример
                },
                "snowball": {
                    "type": "snowball",
                    # Язык для лемматизации. OpenSearch не всегда имеет Russian.
                    # "language": "Russian" # <- Может не работать без плагина
                    "language": "English" # <- Используем English как пример
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "name": {
                "type": "text",
                "analyzer": "default_analyzer", # Используем упрощённый
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer"
                    },
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "brand": {
                "type": "text",
                "analyzer": "default_analyzer",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer"
                    },
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "category": {
                "type": "text",
                "analyzer": "default_analyzer",
                "fields": {
                    "autocomplete": {
                        "type": "text",
                        "analyzer": "autocomplete_analyzer"
                    },
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "price": {
                "type": "double"
            },
            "url": {
                "type": "keyword"
            },
            "store": {
                "type": "keyword"
            }
        }
    }
}


def wait_for_opensearch():
    """Ждём, пока OpenSearch станет доступен."""
    logger.info("Ожидание запуска OpenSearch...")
    for _ in range(120): # Попробуем 120 раз с интервалом 5 секунд = 10 мин
        try:
            # --- ДОБАВЛЕН ТАЙМАУТ ---
            response = requests.get(OPENSEARCH_HOST, timeout=10) # Добавляем таймаут 10 секунд
            if response.status_code == 200:
                logger.info("OpenSearch доступен.")
                return True
        except requests.exceptions.RequestException as e:
            logger.debug(f"Попытка подключения к OpenSearch: {e}") # Логируем ошибку, если нужно, чтобы видеть, что происходит
            pass
        time.sleep(5)
    logger.error("OpenSearch не стал доступен за отведённое время.")
    return False


def create_index():
    """Создаёт индекс с заданным маппингом."""
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX_NAME}"
    response = requests.put(url, json=INDEX_MAPPING, headers={"Content-Type": "application/json"})
    if response.status_code in [200, 201]:
        logger.info(f"Индекс '{OPENSEARCH_INDEX_NAME}' создан или уже существует.")
        return True
    else:
        logger.error(f"Ошибка при создании индекса: {response.status_code} - {response.text}")
        return False


def load_catalog_to_opensearch(xml_path: str):
    """Загружает товары из XML в OpenSearch."""
    logger.info(f"Загрузка каталога из {xml_path} в OpenSearch...")
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # Собираем строки для bulk-запроса в список
        bulk_lines = []
        for product_elem in root.findall('product'):
            product_data = {}
            for child in product_elem:
                product_data[child.tag] = child.text

            # Формируем строку действия индексации и добавляем её в список с \n
            action = {"index": {"_index": OPENSEARCH_INDEX_NAME}}
            # Используем json.dumps с ensure_ascii=False и без лишних пробелов
            action_line = json.dumps(action, ensure_ascii=False, separators=(',', ':'))
            bulk_lines.append(action_line) # Добавляем строку БЕЗ \n сюда

            # Формируем строку с данными документа и добавляем её в список с \n
            doc_line = json.dumps(product_data, ensure_ascii=False, separators=(',', ':'))
            bulk_lines.append(doc_line) # Добавляем строку БЕЗ \n сюда

                # --- ПРАВИЛЬНАЯ ПРОВЕРКА: Проверяем, есть ли что отправлять ---
        if bulk_lines: # Проверяем, что список строк не пуст
            # Объединяем все строки с \n и ДОБАВЛЯЕМ финальный \n
            # join добавляет \n между элементами списка.
            # bulk_lines = ["action1", "doc1", "action2", "doc2"] -> "action1\ndoc1\naction2\ndoc2\n"
            bulk_body = '\n'.join(bulk_lines) + '\n'

            bulk_url = f"{OPENSEARCH_HOST}/_bulk"
            # Теперь bulk_body гарантированно заканчивается на \n (если bulk_lines не был пуст)

            # --- ОТЛАДКА: Выводим последние 100 символов bulk_body ---
            print(f"DEBUG: Last 100 chars of bulk_body: {repr(bulk_body[-100:])}")
            # --- ПРАВИЛЬНАЯ проверка на завершающий \n ---
            print(f"DEBUG: Does bulk_body end with \\n? {bulk_body.endswith(chr(10))}") # chr(10) - символ новой строки
            # --- /ОТЛАДКА ---

            # --- ОТЛАДКА: Сохраняем bulk_body в файл ---
            print("DEBUG: Saving bulk_body to bulk_request.ndjson for manual inspection/curl test.")
            with open("bulk_request.ndjson", "wb") as f:
                # Записываем тело как байты, чтобы точно сохранить \n
                f.write(bulk_body.encode('utf-8'))
            # --- /ОТЛАДКА ---

            response = requests.post(bulk_url, data=bulk_body, headers={"Content-Type": "application/x-ndjson"})

            if response.status_code == 200:
                # Подсчитываем количество загруженных документов (каждая запись = 2 строки: action + doc)
                # len(bulk_lines) - это общее количество строк (действий и документов)
                num_docs = len(bulk_lines) // 2
                logger.info(f"Загружено {num_docs} товаров в OpenSearch.")
                # Принудительно обновляем индекс, чтобы изменения стали видны
                refresh_url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX_NAME}/_refresh"
                requests.post(refresh_url)
                logger.info("Индекс обновлён.")
                return True
            else:
                logger.error(f"Ошибка при bulk-загрузке: {response.status_code} - {response.text}")
                return False
        else:
            logger.warning("Нет данных для загрузки (bulk_lines пуст).")
            # Возвращаем True, если пустой каталог - это не ошибка, иначе False
            return True # Или False, если считать пустой каталог ошибкой

    except ET.ParseError as e:
        logger.error(f"Ошибка парсинга XML: {e}")
        return False
    except FileNotFoundError:
        logger.error(f"Файл не найден: {xml_path}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке каталога: {e}")
        return False


if __name__ == "__main__":
    if not wait_for_opensearch():
        exit(1)

    if not create_index():
        exit(1)

    if not load_catalog_to_opensearch("data/catalog_products.xml"):
        exit(1)

    logger.info("Настройка OpenSearch v2 завершена.")