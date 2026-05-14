#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Message Filter System (Faust + Kafka)
Единый файл, содержащий:
- Модели данных (Message, BlockEvent)
- Персистентное хранилище на shelve
- Цензуру с динамическим списком слов
- Faust‑приложение с агентами для блокировки и цензуры
"""

import faust
import os
import time
import shelve
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Set, Dict

# =========================== МОДЕЛИ ДАННЫХ ===========================

@dataclass
class Message:
    message_id: str
    from_user: str
    to_user: str
    content: str
    timestamp: datetime
    is_censored: bool = False
    original_content: Optional[str] = None

    def to_json(self) -> dict:
        return {
            'message_id': self.message_id,
            'from_user': self.from_user,
            'to_user': self.to_user,
            'content': self.content,
            'timestamp': self.timestamp.isoformat(),
            'is_censored': self.is_censored,
            'original_content': self.original_content
        }

    @classmethod
    def from_json(cls, data: dict):
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

@dataclass
class BlockEvent:
    user_id: str
    blocked_user: str
    action: str          # 'block' или 'unblock'
    timestamp: datetime

    def to_json(self) -> dict:
        return {
            'user_id': self.user_id,
            'blocked_user': self.blocked_user,
            'action': self.action,
            'timestamp': self.timestamp.isoformat()
        }

    @classmethod
    def from_json(cls, data: dict):
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

# =========================== ПЕРСИСТЕНТНОЕ ХРАНИЛИЩЕ ===========================

class ShelveStorage:
    """Обёртка над shelve для хранения множеств и словарей."""
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def get(self, key: str, default=None):
        with shelve.open(self.path) as db:
            return db.get(key, default)

    def set(self, key: str, value):
        with shelve.open(self.path) as db:
            db[key] = value

    def update_blocked(self, user: str, target: str, block: bool):
        """Добавить или удалить блокировку."""
        with shelve.open(self.path) as db:
            blocked = db.get('blocked', {})
            if user not in blocked:
                blocked[user] = set()
            if block:
                blocked[user].add(target)
            else:
                blocked[user].discard(target)
            db['blocked'] = blocked

    def is_blocked(self, user: str, target: str) -> bool:
        with shelve.open(self.path) as db:
            blocked = db.get('blocked', {})
            return target in blocked.get(user, set())

    def get_banned_words(self) -> Set[str]:
        with shelve.open(self.path) as db:
            return db.get('banned', set())

    def add_banned_word(self, word: str):
        with shelve.open(self.path) as db:
            words = db.get('banned', set())
            words.add(word.lower().strip())
            db['banned'] = words

    def remove_banned_word(self, word: str):
        with shelve.open(self.path) as db:
            words = db.get('banned', set())
            words.discard(word.lower().strip())
            db['banned'] = words

# =========================== ЦЕНЗУРА ===========================

def censor_text(text: str, banned_words: Set[str]) -> str:
    """Заменяет все запрещённые слова на звёздочки."""
    if not banned_words:
        return text
    result = text
    for w in banned_words:
        # Ищем целое слово (не часть другого) и заменяем
        pattern = rf'\b{re.escape(w)}\b'
        result = re.sub(pattern, '*' * len(w), result, flags=re.IGNORECASE)
    return result

# =========================== FAUST ПРИЛОЖЕНИЕ ===========================

# Задержка для ожидания готовности Kafka
time.sleep(5)

broker_list = os.getenv('KAFKA_BROKERS', 'kafka1:9093,kafka2:9092,kafka3:9092').split(',')

app = faust.App(
    'message-filter-app',
    broker=broker_list,
    consumer_auto_offset_reset='earliest',
    value_serializer='json',
)

# Топики
messages_topic = app.topic('messages')
filtered_topic = app.topic('filtered_messages')
blocked_topic = app.topic('blocked_users')
banned_topic = app.topic('banned_words')

# Инициализация хранилищ (пути к файлам в томе /data)
blocked_storage = ShelveStorage('/data/blocked_storage')
banned_storage = ShelveStorage('/data/banned_storage')

# Предзагрузка (необязательно)
blocked_storage.get('blocked', {})
banned_storage.get('banned', set())

# -------------------------------------------------------------------
# Агент обработки сообщений
# -------------------------------------------------------------------
@app.agent(messages_topic)
async def process_messages(stream):
    async for msg_data in stream:
        message = Message.from_json(msg_data)
        print(f"[→] {message.from_user} → {message.to_user}: {message.content[:50]}")

        # Проверка блокировки (получатель заблокировал отправителя)
        if blocked_storage.is_blocked(message.to_user, message.from_user):
            print(f"[🚫] БЛОК: {message.from_user} не может писать {message.to_user}")
            continue

        # Цензура
        banned = banned_storage.get_banned_words()
        censored = censor_text(message.content, banned)
        if censored != message.content:
            message.original_content = message.content
            message.content = censored
            message.is_censored = True
            print(f"[🔞] ЦЕНЗУРА: {message.original_content[:50]} → {censored[:50]}")

        await filtered_topic.send(value=message.to_json())
        print(f"[✓] Отправлено {message.from_user} → {message.to_user}")

# -------------------------------------------------------------------
# Агент обработки событий блокировки
# -------------------------------------------------------------------
@app.agent(blocked_topic)
async def process_blocks(stream):
    async for ev_data in stream:
        ev = BlockEvent.from_json(ev_data)
        if ev.action == 'block':
            blocked_storage.update_blocked(ev.user_id, ev.blocked_user, block=True)
            print(f"[🔒] {ev.user_id} заблокировал {ev.blocked_user}")
        else:
            blocked_storage.update_blocked(ev.user_id, ev.blocked_user, block=False)
            print(f"[🔓] {ev.user_id} разблокировал {ev.blocked_user}")

# -------------------------------------------------------------------
# Агент обновления списка запрещённых слов
# -------------------------------------------------------------------
@app.agent(banned_topic)
async def process_banned(stream):
    async for ev_data in stream:
        action = ev_data.get('action')
        word = ev_data.get('word', '').lower().strip()
        if action == 'add':
            banned_storage.add_banned_word(word)
            print(f"[🚫] Запрещено слово: {word}")
        elif action == 'remove':
            banned_storage.remove_banned_word(word)
            print(f"[✅] Разрешено слово: {word}")

# -------------------------------------------------------------------
# Точка входа (для прямого запуска python app.py)
# -------------------------------------------------------------------
if __name__ == '__main__':
    app.main()