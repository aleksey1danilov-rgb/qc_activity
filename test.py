from database import SessionLocal, Topic, Operator
db = SessionLocal()

# Посмотрим оператора
operator = db.query(Operator).first()
print(f"Оператор: {operator.full_name}, проект ID: {operator.project_id}")

# Посмотрим темы для этого проекта
topics = db.query(Topic).filter(
    Topic.project_id == operator.project_id,
    Topic.is_active == True
).all()
print(f"Найдено тем: {len(topics)}")
for t in topics:
    print(f"ID: {t.id}, Название: {t.name}, Проект: {t.project_id}, Активна: {t.is_active}")

db.close()