# migrate_db.py
import sqlite3
import os

def migrate_database():
    """Добавляет новые поля в существующую базу данных"""
    
    # Путь к вашей базе данных
    db_path = "calls.db"
    
    if not os.path.exists(db_path):
        print(f"❌ База данных {db_path} не найдена")
        return
    
    # Подключаемся к базе
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("🔄 Начинаем миграцию базы данных...")
    
    # Проверяем и добавляем поле call_duration в таблицу evaluations
    try:
        cursor.execute("SELECT call_duration FROM evaluations LIMIT 1")
        print("✅ Поле call_duration уже существует в таблице evaluations")
    except sqlite3.OperationalError:
        print("➕ Добавляем поле call_duration в таблицу evaluations...")
        cursor.execute("ALTER TABLE evaluations ADD COLUMN call_duration INTEGER")
        print("✅ Поле добавлено")
    
    # Проверяем и добавляем поле avg_call_duration в таблицу operators
    try:
        cursor.execute("SELECT avg_call_duration FROM operators LIMIT 1")
        print("✅ Поле avg_call_duration уже существует в таблице operators")
    except sqlite3.OperationalError:
        print("➕ Добавляем поле avg_call_duration в таблицу operators...")
        cursor.execute("ALTER TABLE operators ADD COLUMN avg_call_duration INTEGER DEFAULT 0")
        print("✅ Поле добавлено")
    
    # Проверяем и добавляем поле avg_call_duration в таблицу quality_history
    try:
        cursor.execute("SELECT avg_call_duration FROM quality_history LIMIT 1")
        print("✅ Поле avg_call_duration уже существует в таблице quality_history")
    except sqlite3.OperationalError:
        print("➕ Добавляем поле avg_call_duration в таблицу quality_history...")
        cursor.execute("ALTER TABLE quality_history ADD COLUMN avg_call_duration INTEGER DEFAULT 0")
        print("✅ Поле добавлено")
    
    # Сохраняем изменения
    conn.commit()
    conn.close()
    
    print("\n✅ Миграция успешно завершена!")
    print("Новые поля добавлены:")
    print("  - evaluations.call_duration")
    print("  - operators.avg_call_duration")
    print("  - quality_history.avg_call_duration")

if __name__ == "__main__":
    migrate_database()