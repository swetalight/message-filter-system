"""
Тестовый скрипт для проверки системы.

Отправляет тестовые сообщения в Kafka для проверки:
- Блокировки пользователей
- Цензуры сообщений
- Комплексных сценариев
"""

import time
import json
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Адреса Kafka брокеров
KAFKA_SERVERS = ['kafka1:9093', 'kafka2:9092', 'kafka3:9092']


def get_producer():
    """
    Создает и возвращает Kafka producer.
    
    Returns:
        KafkaProducer: Настроенный producer для отправки сообщений
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def send_message(producer, from_user: str, to_user: str, content: str):
    """
    Отправляет сообщение в топик 'messages'.
    
    Args:
        producer: Kafka producer
        from_user: Отправитель
        to_user: Получатель
        content: Текст сообщения
    """
    message = {
        'message_id': str(uuid.uuid4()),
        'from_user': from_user,
        'to_user': to_user,
        'content': content,
        'timestamp': datetime.now().isoformat(),
        'is_censored': False,
        'original_content': None
    }
    
    future = producer.send('messages', value=message)
    result = future.get(timeout=10)
    print(f"📤 Отправлено: {from_user} → {to_user}: {content[:50]}")
    return result


def block_user(producer, user_id: str, blocked_user: str, action: str = 'block'):
    """
    Отправляет событие блокировки/разблокировки.
    
    Args:
        producer: Kafka producer
        user_id: Пользователь, выполняющий действие
        blocked_user: Пользователь, которого блокируют/разблокируют
        action: 'block' или 'unblock'
    """
    event = {
        'user_id': user_id,
        'blocked_user': blocked_user,
        'action': action,
        'timestamp': datetime.now().isoformat()
    }
    
    future = producer.send('blocked_users', value=event)
    result = future.get(timeout=10)
    emoji = "🔒" if action == 'block' else "🔓"
    print(f"{emoji} {user_id} {action}s {blocked_user}")
    return result


def add_banned_word(producer, word: str):
    """Добавляет слово в список запрещенных."""
    event = {'action': 'add', 'word': word}
    future = producer.send('banned_words', value=event)
    result = future.get(timeout=10)
    print(f"🚫 Добавлено слово: {word}")
    return result


def run_tests():
    """Запускает все тесты."""
    print("=" * 60)
    print("НАЧАЛО ТЕСТИРОВАНИЯ СИСТЕМЫ")
    print("=" * 60)
    
    # Создаем producer
    producer = get_producer()
    
    # Даем время на запуск Kafka и Faust
    print("⏳ Ожидание запуска сервисов...")
    time.sleep(10)
    
    # ===== ТЕСТ 1: Базовая отправка =====
    print("\n📝 ТЕСТ 1: Базовая отправка сообщений")
    print("-" * 40)
    send_message(producer, "Alice", "Bob", "Hello Bob!")
    send_message(producer, "Bob", "Alice", "Hi Alice!")
    send_message(producer, "Charlie", "Alice", "Hey Alice!")
    time.sleep(2)
    
    # ===== ТЕСТ 2: Блокировка пользователей =====
    print("\n🔒 ТЕСТ 2: Блокировка пользователей")
    print("-" * 40)
    
    # Bob блокирует Charlie
    block_user(producer, "Bob", "Charlie", "block")
    time.sleep(2)
    
    # Отправляем сообщение от заблокированного пользователя
    send_message(producer, "Charlie", "Bob", "Hello Bob, can you hear me?")
    send_message(producer, "Charlie", "Alice", "Bob blocked me!")
    time.sleep(2)
    
    # Разблокируем
    block_user(producer, "Bob", "Charlie", "unblock")
    time.sleep(2)
    
    # Отправляем снова
    send_message(producer, "Charlie", "Bob", "Now you can see me!")
    time.sleep(3)
    
    # ===== ТЕСТ 3: Цензура =====
    print("\n📝 ТЕСТ 3: Цензура сообщений")
    print("-" * 40)
    
    # Добавляем запрещенные слова
    add_banned_word(producer, "spam")
    add_banned_word(producer, "badword")
    add_banned_word(producer, "offensive")
    time.sleep(2)
    
    # Отправляем сообщения с запрещенными словами
    send_message(producer, "Alice", "Bob", "This is spam message")
    send_message(producer, "Bob", "Alice", "Stop sending badword content")
    send_message(producer, "Charlie", "Alice", "That's offensive!")
    send_message(producer, "Alice", "Bob", "Clean message without problems")
    time.sleep(3)
    
    # Удаляем слово
    print("\n🗑️ Удаляем слово 'spam' из списка")
    event = {'action': 'remove', 'word': 'spam'}
    producer.send('banned_words', value=event).get(timeout=10)
    time.sleep(2)
    
    # Отправляем снова
    send_message(producer, "Alice", "Bob", "This spam is no longer banned")
    time.sleep(5)
    
    print("\n" + "=" * 60)
    print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 60)
    print("\n💡 Проверьте результаты в Kafka UI: http://localhost:8080")
    print("💡 Топик 'filtered_messages' содержит обработанные сообщения")
    
    producer.close()


if __name__ == '__main__':
    run_tests()