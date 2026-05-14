# Message Filter System (Faust + Kafka)

Система потоковой обработки сообщений с блокировкой пользователей и цензурой слов.  
Реализована с использованием **Faust** (библиотека потоковой обработки для Python) и **Kafka** (кластер из 3 брокеров в режиме KRaft).

## 📦 Содержание

- [Архитектура](#архитектура)
- [Файлы проекта](#файлы-проекта)
- [Инструкция по запуску](#инструкция-по-запуску)
- [Тестирование](#тестирование)
- [Описание логики](#описание-логики)

---

## Архитектура

- **3 брокера Kafka** (KRaft, без ZooKeeper) – топики: `messages`, `filtered_messages`, `blocked_users`, `banned_words`.
- **Faust‑воркер** – подписывается на топики, реализует:
  - блокировку нежелательных пользователей (персистентное хранилище `shelve`);
  - цензуру запрещённых слов (динамическое обновление списка).
- **Kafka UI** – веб‑интерфейс для просмотра топиков и сообщений (порт 8080).
- **Тестовый клиент** – отправляет тестовые данные для проверки.

---

## Файлы проекта

Проект состоит из следующих файлов (все они приведены в приложении к заданию):

- `docker-compose.yml` – оркестрация всех сервисов.
- `Dockerfile` – сборка образа Faust‑воркера.
- `Dockerfile.test` – сборка образа для тестового клиента.
- `patch_nullcontext.py` – патч для совместимости Faust.
- `app.py` – основная логика блокировки и цензуры.
- `tests/test_data.py` – тестовые данные.

Создайте эти файлы в соответствующих папках согласно инструкциям ниже.

---

## Инструкция по запуску

### 1. Подготовка окружения

Установите Docker и Docker Compose (версия ≥ 2.0). Убедитесь, что порты `9092`, `9093`, `9095`, `8080` свободны.

### 2. Создание файлов

Скопируйте содержимое каждого файла из приложения к заданию в соответствующий файл в вашей локальной папке. Структура:

message-filter-system/
├── docker-compose.yml
├── Dockerfile
├── Dockerfile.test
├── patch_nullcontext.py
├── app.py
└── tests/
└── test_data.py


### 3. Запуск всех сервисов

```bash
cd message-filter-system
docker-compose up -d

### 4. Просмотр логов процессора
bash
docker-compose logs -f processor
При успешном запуске вы увидите сообщения о старте Faust и подписке на топики.

### 5. Запуск тестов
В отдельном терминале:

bash
docker-compose run --rm test
Тесты отправят в Kafka тестовые сообщения, команды блокировки и запрещённые слова. В логах процессора появятся соответствующие записи о блокировке и цензуре.

### 6. Проверка через Kafka UI
Откройте браузер: http://localhost:8080. Выберите топик filtered_messages – вы увидите обработанные сообщения (с замаскированными словами).

### 7. Остановка системы
bash
docker-compose down -v



Тестирование
Автоматический тест (test_data.py) отправляет следующие данные:
Обычное сообщение
Топик messages: {"from_user": "Alice", "to_user": "Bob", "content": "Hello Bob!"}
Ожидание: сообщение появляется в filtered_messages без изменений.

Блокировка
Топик blocked_users: {"user_id": "Bob", "blocked_user": "Charlie", "action": "block"}
Ожидание: обновляется список блокировок.

Сообщение от заблокированного
Топик messages: {"from_user": "Charlie", "to_user": "Bob", "content": "Hi Bob, can you see me?"}
Ожидание: сообщение не попадает в filtered_messages.

Добавление запрещённого слова
Топик banned_words: {"action": "add", "word": "spam"}

Сообщение с запрещённым словом
Топик messages: {"from_user": "Alice", "to_user": "Bob", "content": "This is spam message"}
Ожидание: в filtered_messages слово spam заменяется на ****.

Разблокировка
Топик blocked_users: {"user_id": "Bob", "blocked_user": "Charlie", "action": "unblock"}

Сообщение после разблокировки
Топик messages: {"from_user": "Charlie", "to_user": "Bob", "content": "Now you can see me!"}
Ожидание: сообщение проходит и попадает в filtered_messages.

Если топика filtered_messages нет, создайте его вручную:

bash
docker exec kafka-node1 kafka-topics --create --topic filtered_messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1



Ручное тестирование (опционально)
Отправка сообщения через консоль Kafka:

bash
docker exec -it kafka-node1 kafka-console-producer --bootstrap-server localhost:9092 --topic messages
> {"from_user":"Test","to_user":"Admin","content":"test message"}
Просмотр обработанных сообщений:

bash
docker exec -it kafka-node1 kafka-console-consumer --bootstrap-server localhost:9092 --topic filtered_messages --from-beginning


Описание логики
app.py (Faust приложение)
blocked_users – словарь, где ключ – получатель, значение – множество отправителей, которые заблокированы этим получателем. Хранится в shelve (персистентно).

banned_words – множество запрещённых слов (также сохраняется в shelve).

Агенты:

process_messages

Читает топик messages.

Проверяет, не заблокирован ли отправитель получателем. Если заблокирован – сообщение отбрасывается.

Заменяет все запрещённые слова в тексте на * (маскировка).

Отправляет обработанное сообщение в топик filtered_messages.

process_blocks

Читает топик blocked_users.

Обновляет blocked_users (добавляет/удаляет блокировки) и сохраняет в shelve.

process_banned

Читает топик banned_words.

Обновляет banned_words (добавляет/удаляет слова) и сохраняет в shelve.

Персистентность:
Используется shelve – встроенная в Python библиотека для хранения словарей и множеств на диске. Это соответствует требованию задания "Python: table".


