FROM python:3.12-slim

WORKDIR /app

# Устанавливаем зависимости (requests)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все файлы проекта
COPY . .

# (Опционально) Устанавливаем git, curl, docker-compose внутри контейнера (если нужно для setup_elasticsearch.py)
# RUN apt-get update && apt-get install -y git curl && rm -rf /var/lib/apt/lists/*

# CMD для запуска основного скрипта (предполагается, что setup_elasticsearch.py запускается внутри search_engine.py или отдельно)
# CMD ["python", "search_engine.py"]
# Или, если нужно сначала загрузить данные:
# CMD ["sh", "-c", "python setup_elasticsearch.py && python search_engine.py"]
# Лучше, если setup и search запускаются отдельно или через скрипт.
# CMD ["python", "search_engine.py"]
# Для задачи, где OpenSearch запускается отдельно, CMD может быть просто запуском оценки.
# Убедитесь, что OpenSearch доступен по http://localhost:9200 или через другое имя сервиса (например, http://opensearch:9200), если используется docker-compose.
# CMD ["python", "search_engine.py"]
# Или, если у тебя есть скрипт для запуска всей цепочки (setup, search, evaluate):
# COPY run_all.py . # Предположим, у тебя есть такой скрипт
# CMD ["python", "run_all.py"]
# Но для простоты, пусть основной скрипт будет search_engine.py, а setup запускается вручную или в другом контексте.
# CMD ["python", "search_engine.py"]
# Правильный CMD зависит от того, как ты хочешь использовать образ.
# Если образ предназначен для *запуска оценки* при условии, что OpenSearch уже запущен и доступен (например, в той же сети docker-compose), то:
# CMD ["python", "search_engine.py"]
# ВАЖНО: Если ты хочешь, чтобы контейнер *подождал* OpenSearch перед выполнением, логика ожидания должна быть в Python-скрипте или в shell-скрипте, который выз