from sqlalchemy import func, and_, desc
from datetime import datetime, timedelta, date
from database import SessionLocal, Operator, Project, Evaluation, Metric, EvaluationMetric, User, QualityHistory
import calendar



def get_operator_stats(operator_id, period_days=30):
    """Статистика по оператору за период"""
    db = SessionLocal()
    try:
        cutoff_date = date.today() - timedelta(days=period_days)
        
        evaluations = db.query(Evaluation).filter(
            Evaluation.operator_id == operator_id,
            Evaluation.call_date >= cutoff_date
        ).all()
        
        if not evaluations:
            return None
        
        avg_quality = sum(e.quality_percent for e in evaluations) / len(evaluations)
        
        # Динамика (сравнение с предыдущим периодом)
        prev_cutoff = cutoff_date - timedelta(days=period_days)
        prev_evaluations = db.query(Evaluation).filter(
            Evaluation.operator_id == operator_id,
            Evaluation.call_date >= prev_cutoff,
            Evaluation.call_date < cutoff_date
        ).all()
        
        prev_avg = sum(e.quality_percent for e in prev_evaluations) / len(prev_evaluations) if prev_evaluations else avg_quality
        dynamic = avg_quality - prev_avg
        
        operator = db.get(Operator, operator_id)
        if not operator:
            return None
        
        return {
            "operator": operator.full_name,
            "project": operator.project.name if operator.project else "Без проекта",
            "evaluations_count": len(evaluations),
            "avg_quality": round(avg_quality, 1),
            "dynamic": round(dynamic, 1),
            "dynamic_trend": "up" if dynamic > 0 else "down" if dynamic < 0 else "stable",
            "experience_days": operator.experience_days,
            "hire_date": operator.hire_date
        }
    finally:
        db.close()  # ✅ ОБЯЗАТЕЛЬНО ЗАКРЫВАЕМ!


def get_project_stats(project_id, period_days=30):
    """Статистика по проекту за период"""
    db = SessionLocal()
    try:
        cutoff_date = date.today() - timedelta(days=period_days)
        
        # Все оценки по проекту
        evaluations = db.query(Evaluation).join(Operator).filter(
            Operator.project_id == project_id,
            Evaluation.call_date >= cutoff_date
        ).all()
        
        if not evaluations:
            return None
        
        avg_quality = sum(e.quality_percent for e in evaluations) / len(evaluations)
        
        # Операторы проекта
        operators = db.query(Operator).filter(Operator.project_id == project_id).all()
        operators_stats = []
        for op in operators:
            op_evals = [e for e in evaluations if e.operator_id == op.id]
            if op_evals:
                op_avg = sum(e.quality_percent for e in op_evals) / len(op_evals)
                operators_stats.append({
                    "name": op.full_name,
                    "avg_quality": round(op_avg, 1),
                    "evaluations": len(op_evals)
                })
        
        # Динамика проекта
        prev_cutoff = cutoff_date - timedelta(days=period_days)
        prev_evaluations = db.query(Evaluation).join(Operator).filter(
            Operator.project_id == project_id,
            Evaluation.call_date >= prev_cutoff,
            Evaluation.call_date < cutoff_date
        ).all()
        
        prev_avg = sum(e.quality_percent for e in prev_evaluations) / len(prev_evaluations) if prev_evaluations else avg_quality
        dynamic = avg_quality - prev_avg
        
        project = db.get(Project, project_id)
        if not project:
            return None
        
        return {
            "project": project.name,
            "evaluations_count": len(evaluations),
            "avg_quality": round(avg_quality, 1),
            "dynamic": round(dynamic, 1),
            "operators_count": len(operators),
            "operators_stats": sorted(operators_stats, key=lambda x: x["avg_quality"], reverse=True)
        }
    finally:
        db.close()


def get_weekly_dynamics(project_id=None, operator_id=None):
    """Динамика по неделям для графика"""
    db = SessionLocal()
    try:
        today = date.today()
        weeks_data = []
        
        for i in range(4, -1, -1):  # Последние 5 недель
            week_end = today - timedelta(days=today.weekday() + 7*i)
            week_start = week_end - timedelta(days=6)
            
            query = db.query(Evaluation)
            if operator_id:
                query = query.filter(Evaluation.operator_id == operator_id)
            elif project_id:
                query = query.join(Operator).filter(Operator.project_id == project_id)
            
            week_evals = query.filter(
                Evaluation.call_date >= week_start,
                Evaluation.call_date <= week_end
            ).all()
            
            avg_quality = sum(e.quality_percent for e in week_evals) / len(week_evals) if week_evals else 0
            
            weeks_data.append({
                "week": f"{week_start.strftime('%d.%m')}-{week_end.strftime('%d.%m')}",
                "quality": round(avg_quality, 1),
                "count": len(week_evals)
            })
        
        return weeks_data
    finally:
        db.close()


def get_monthly_dynamics(project_id=None, operator_id=None, months=6):
    """Динамика по месяцам"""
    db = SessionLocal()
    try:
        today = date.today()
        months_data = []
        
        for i in range(months - 1, -1, -1):
            month = today.month - i
            year = today.year
            while month <= 0:
                month += 12
                year -= 1
            
            _, last_day = calendar.monthrange(year, month)
            
            month_start = date(year, month, 1)
            month_end = date(year, month, last_day)
            
            query = db.query(Evaluation)
            if operator_id:
                query = query.filter(Evaluation.operator_id == operator_id)
            elif project_id:
                query = query.join(Operator).filter(Operator.project_id == project_id)
            
            month_evals = query.filter(
                Evaluation.call_date >= month_start,
                Evaluation.call_date <= month_end
            ).all()
            
            avg_quality = sum(e.quality_percent for e in month_evals) / len(month_evals) if month_evals else 0
            
            months_data.append({
                "month": month_start.strftime('%B %Y'),
                "quality": round(avg_quality, 1),
                "count": len(month_evals)
            })
        
        return months_data
    finally:
        db.close()


# ============== ФУНКЦИЯ ДЛЯ ТЕСТИРОВАНИЯ ==============
if __name__ == "__main__":
    print("🔍 ТЕСТИРОВАНИЕ ФУНКЦИЙ ОТЧЕТОВ")
    print("=" * 50)
    
    # Статистика по оператору
    print("📊 Статистика оператора:")
    op_stats = get_operator_stats(1)
    if op_stats:
        print(f"  {op_stats['operator']} ({op_stats['project']}): {op_stats['avg_quality']}%")
        print(f"  Динамика: {op_stats['dynamic']:+}%")
        print(f"  Стаж: {op_stats['experience_days']} дней")
    else:
        print("  ❌ Нет данных по оператору 1")
    
    print()
    
    # Статистика по проекту
    print("📈 Статистика проекта:")
    proj_stats = get_project_stats(1)
    if proj_stats:
        print(f"  {proj_stats['project']}: {proj_stats['avg_quality']}%")
        print(f"  Динамика: {proj_stats['dynamic']:+}%")
        print(f"  Операторы: {proj_stats['operators_count']}")
        for op in proj_stats['operators_stats'][:3]:
            print(f"    • {op['name']}: {op['avg_quality']}%")
    else:
        print("  ❌ Нет данных по проекту 1")
    
    print()
    
    # Динамика по неделям
    print("📅 Динамика за 5 недель:")
    weeks = get_weekly_dynamics(project_id=1)
    for w in weeks:
        print(f"  {w['week']}: {w['quality']}% ({w['count']} оценок)")