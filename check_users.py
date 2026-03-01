from database import SessionLocal, User
from auth import hash_password

db = SessionLocal()
users = db.query(User).all()
print("Текущие пользователи в БД:")
for user in users:
    print(f"ID: {user.id}, Логин: {user.login}, Роль: {user.role}, Активен: {user.is_active}")
db.close()