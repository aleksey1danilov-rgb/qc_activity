# add_topic_column.py
from database import engine, Base, Evaluation
from sqlalchemy import text

def add_topic_column():
    try:
        with engine.connect() as conn:
            # Проверяем, существует ли уже колонка
            result = conn.execute(text("PRAGMA table_info(evaluations)"))
            columns = [row[1] for row in result]
            
            if 'topic_id' not in columns:
                print("Добавляем колонку topic_id...")
                conn.execute(text("ALTER TABLE evaluations ADD COLUMN topic_id INTEGER REFERENCES topics(id)"))
                conn.commit()
                print("✅ Колонка успешно добавлена")
            else:
                print("✅ Колонка topic_id уже существует")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    add_topic_column()