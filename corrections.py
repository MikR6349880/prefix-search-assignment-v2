# corrections.py

# Словарь для исправления опечаток, транслитерации и нормализации запросов
# Ключ - оригинальный запрос или его часть, значение - исправленный/нормализованный вариант
CORRECTIONS = {
    # Опечатки
    "xfq": "чай",
    "xfqyfz": "сервиз",
    # Транслитерация
    "холс": "hols", # или "холод", если это слово
    "san pelle": "san pellegrino",
    "sanpelle": "san pellegrino",
    "san pel": "san pellegrino",
    "san pellegr": "san pellegrino",
    # Часто встречающиеся сокращения/альтернативы (примеры)
    "адаптер": "adapter",
    "адаптер для": "adapter for",
    # Пример с числовым атрибутом (пока просто как есть, но можно улучшить)
    # "10л": "10 liters" # Не будем пока менять числа, это сложнее
}

# Функция для применения корректировок
def apply_corrections(query: str) -> str:
    """
    Применяет правила корректировки к запросу.
    Пока простая реализация: заменяет точные совпадения токенов или сам запрос целиком.
    """
    # Приводим к нижнему регистру для сопоставления
    query_lower = query.lower()
    # Проверяем, есть ли весь запрос в словаре
    if query_lower in CORRECTIONS:
        return CORRECTIONS[query_lower]

    # Проверяем и заменяем части запроса (токены)
    # Это простая версия, заменяет по одному вхождению за раз
    corrected_query = query_lower
    for wrong, correct in CORRECTIONS.items():
        # Заменяем *точное* вхождение wrong на correct в corrected_query
        # Это может не сработать, если wrong является частью другого слова
        # или если слова в неправильном порядке.
        # Для более сложных случаев нужен NLP.
        corrected_query = corrected_query.replace(wrong, correct)

    return corrected_query

# Пример использования (можно закомментировать или удалить после тестирования)
# if __name__ == "__main__":
#     test_queries = ["xfq", "san pelle", "adapter usb c", "холс"]
#     for q in test_queries:
#         corrected = apply_corrections(q)
#         print(f"Original: '{q}' -> Corrected: '{corrected}'")