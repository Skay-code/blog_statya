import sqlite3 
from typing import Optional, Tuple

def set_topic_author(db_connection, topic_id: int, author_name: str) -> str:
    """
    Устанавливает автора для темы, обрабатывая все возможные случаи
    
    Args:
        db_connection: Подключение к базе данных
        topic_id: ID темы в Telegram
        author_name: Имя автора
    
    Returns:
        str: Описание выполненного действия
    """
    cursor = db_connection.cursor()
    
    try:
        # Ищем существующего автора с таким именем
        cursor.execute(
            "SELECT author_id, telegram_topic_id FROM authors WHERE name = ?", 
            (author_name,)
        )
        author_data = cursor.fetchone()
        existing_author_id = author_data[0] if author_data else None
        author_current_topic_id = author_data[1] if author_data else None
        
        # Ищем автора для данной темы
        cursor.execute(
            "SELECT author_id FROM authors WHERE telegram_topic_id = ?", 
            (topic_id,)
        )
        topic_author_data = cursor.fetchone()
        topic_current_author_id = topic_author_data[0] if topic_author_data else None
        
        # Случай 1: Ни автора, ни темы нет в БД
        if existing_author_id is None and topic_current_author_id is None:
            cursor.execute(
                "INSERT INTO authors (name, telegram_topic_id) VALUES (?, ?)",
                (author_name, topic_id)
            )
            db_connection.commit()
            return f"✅ Создан новый автор '{author_name}' для темы {topic_id}"

        # Случай 2: Автор есть, но возможно с другой темой
        elif existing_author_id is not None and topic_current_author_id is None:
            cursor.execute(
                "UPDATE authors SET telegram_topic_id = ? WHERE author_id = ?",
                    (topic_id, existing_author_id)
                )
            db_connection.commit()
            return f"✅ Автор '{author_name}' перемещен на тему {topic_id}. Старая тема ({author_current_topic_id}) удалится и все книги автора будут перемещены в эту тему."
        
        # Случай 3: Тема есть с другим автором, но указанного автора нет в БД
        elif existing_author_id is None and topic_current_author_id is not None:
            # Получаем имя старого автора для информации
            cursor.execute(
                "SELECT name FROM authors WHERE author_id = ?", 
                (topic_current_author_id,)
            )
            old_author_name = cursor.fetchone()[0]
            
            # Обновляем имя автора
            cursor.execute(
                "UPDATE authors SET name = ? WHERE author_id = ?",
                (author_name, topic_current_author_id)
            )
            db_connection.commit()
            return f"✅ Заменен автор темы {topic_id}: '{old_author_name}' → '{author_name}'. Все книги автора перешли к новому автору."
        
        # Случай 4: И автор есть, и тема есть (возможно с разными связями)
        elif existing_author_id is not None and topic_current_author_id is not None:
            if existing_author_id != topic_current_author_id:
                # Получаем имя старого автора темы
                cursor.execute(
                    "SELECT name FROM authors WHERE author_id = ?", 
                    (topic_current_author_id,)
                )
                old_author_name = cursor.fetchone()[0]
                
                return f"❌ Ошибка при установке автора. Автор {author_name} привязан к теме {author_current_topic_id}. А к данной теме привязан автор {old_author_name}."
            else:
                return f"ℹ️ Автор '{author_name}' уже привязан к теме {topic_id}"
                
    except Exception as e:
        db_connection.rollback()
        return f"❌ Ошибка при установке автора: {str(e)}"
# Пример использования в телеграм боте
@router.message(Command("set_author"))
async def handle_set_author_command(message, bot):
    """
    Обработчик команды /set_author в телеграм боте
    Формат: /set_author Имя Автора
    """
    try:
        # Извлекаем имя автора из сообщения
        command_parts = message.text.split(maxsplit=1)
        if len(command_parts) < 2:
            await bot.send_message(
                message.chat.id, 
                "❌ Использование: /set_author Имя Автора",
                message_thread_id=message.message_thread_id
            )
            return
        
        author_name = command_parts[1].strip()
        topic_id = message.message_thread_id
        
        if not topic_id:
            await bot.send_message(
                message.chat.id, 
                "❌ Команда должна быть отправлена в теме"
            )
            return
        
        # Подключение к БД (замените на ваш способ подключения)
        with sqlite3.connect('bot_database.db') as conn:
            result = set_topic_author(conn, topic_id, author_name)
            
        await bot.send_message(
            message.chat.id,
            result,
            message_thread_id=topic_id
        )
        
    except Exception as e:
        await bot.send_message(
            message.chat.id,
            f"❌ Произошла ошибка: {str(e)}",
            message_thread_id=message.message_thread_id
        )
