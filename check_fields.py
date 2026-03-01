# check_fields.py
from database import SessionLocal, Evaluation, Operator, QualityHistory

db = SessionLocal()

print("🔍 Проверка новых полей в базе данных:\n")

# Проверяем Evaluation
try:
    eval_sample = db.query(Evaluation).first()
    if eval_sample:
        print(f"✅ Evaluation: поле call_duration = {eval_sample.call_duration}")
    else:
        print("📝 Evaluation: таблица пуста, но поле должно быть")
except Exception as e:
    print(f"❌ Evaluation: ошибка - {e}")

# Проверяем Operator
try:
    op_sample = db.query(Operator).first()
    if op_sample:
        print(f"✅ Operator: поле avg_call_duration = {op_sample.avg_call_duration}")
    else:
        print("📝 Operator: таблица пуста, но поле должно быть")
except Exception as e:
    print(f"❌ Operator: ошибка - {e}")

# Проверяем QualityHistory
try:
    hist_sample = db.query(QualityHistory).first()
    if hist_sample:
        print(f"✅ QualityHistory: поле avg_call_duration = {hist_sample.avg_call_duration}")
    else:
        print("📝 QualityHistory: таблица пуста, но поле должно быть")
except Exception as e:
    print(f"❌ QualityHistory: ошибка - {e}")

db.close()