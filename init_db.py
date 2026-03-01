from database import SessionLocal, Base, engine, Project, User, MetricBlock, Metric
from auth import hash_password
from sqlalchemy import inspect

# Создаем таблицы
print("🚀 Создаем таблицы...")
Base.metadata.create_all(bind=engine)

# Проверяем созданные таблицы
inspector = inspect(engine)
tables = inspector.get_table_names()
print("✅ Созданные таблицы:")
for table in tables:
    print(f"  - {table}")

# Открываем сессию
db = SessionLocal()

try:
    # Создаем проекты
    print("\n📁 Создаем проекты...")
    project1 = Project(name="Re: Развитие", description="Развитие, фермеры, недвижимость")
    project2 = Project(name="Хантеры Travel", description="Хантеры путешествия")
    db.add_all([project1, project2])
    db.commit()
    print("✅ Проекты созданы")

    # Создаем админа
    print("\n👤 Создаем пользователей...")
    if db.query(User).filter(User.login == "admin").count() == 0:
        admin = User(
            full_name="Администратор",
            login="admin",
            password_hash=hash_password("admin123"),
            role="admin",
            avatar_letters="АД"  # ← вернул avatar_letters
        )
        db.add(admin)
        print("  - Админ создан (АД)")

    # Создаем Яну
    if db.query(User).filter(User.login == "yana.control").count() == 0:
        yana = User(
            full_name="Яна Контролер",
            login="yana.control",
            password_hash=hash_password("yana123"),
            role="controller",
            avatar_letters="ЯК"  # ← вернул avatar_letters
        )
        db.add(yana)
        print("  - Яна создана (ЯК)")
    
    db.commit()
    print("✅ Пользователи созданы")

    # Показываем что создалось
    print("\n📊 Итог:")
    projects = db.query(Project).all()
    print(f"  Проектов: {len(projects)}")
    for p in projects:
        print(f"    - {p.name}")
    
    users = db.query(User).all()
    print(f"  Пользователей: {len(users)}")
    for u in users:
        print(f"    - {u.full_name} ({u.login}, {u.role}, инициалы: {u.avatar_letters})")

except Exception as e:
    print(f"❌ Ошибка: {e}")
    db.rollback()
finally:
    db.close()

print("\n🚀 Готово! Метрики добавляй через интерфейс!")