# database.py - финальная версия с поддержкой PostgreSQL

import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text, Boolean, Date, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime, date, time
from enum import Enum

# Определяем URL базы данных из переменной окружения или используем SQLite по умолчанию
SQLALCHEMY_DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./calls.db")

# Настройки для разных типов баз данных
if SQLALCHEMY_DATABASE_URL.startswith("postgresql"):
    # Для PostgreSQL
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True
    )
else:
    # Для SQLite
    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"check_same_thread": False} if "sqlite" in SQLALCHEMY_DATABASE_URL else {}
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ============== СПРАВОЧНИКИ ==============

class UserRole(Enum):
    """Роли пользователей"""
    CONTROLLER = "controller"  # Оценивает звонки
    PROJECT_MANAGER = "manager"  # Смотрит отчеты
    ADMIN = "admin"  # Полный доступ


# ============== ПРОЕКТЫ ==============

class Project(Base):
    """Проекты"""
    __tablename__ = "projects"
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    description = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    operators = relationship("Operator", back_populates="project")
    metric_blocks = relationship("MetricBlock", back_populates="project", cascade="all, delete-orphan")
    managers = relationship("User", secondary="project_managers", back_populates="managed_projects")
    topics = relationship("Topic", back_populates="project", cascade="all, delete-orphan")
    
    # Связь с ProjectManager
    manager_links = relationship("ProjectManager", back_populates="project", overlaps="managers,managed_projects")


# ============== ПОЛЬЗОВАТЕЛИ ==============

class User(Base):
    """Пользователи системы"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    login = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=True)
    role = Column(String, nullable=False, default=UserRole.CONTROLLER.value)
    
    email = Column(String, nullable=True)
    avatar_letters = Column(String(2), nullable=True)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Связи
    evaluations_given = relationship("Evaluation", back_populates="evaluator")
    managed_projects = relationship("Project", secondary="project_managers", back_populates="managers")
    
    # Связь с ProjectManager
    manager_links = relationship("ProjectManager", back_populates="user", overlaps="managed_projects,managers")


class ProjectManager(Base):
    """Связь руководителей с проектами"""
    __tablename__ = "project_managers"
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"))
    assigned_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    user = relationship("User", back_populates="manager_links", overlaps="managed_projects,managers")
    project = relationship("Project", back_populates="manager_links", overlaps="managed_projects,managers")


# ============== ОПЕРАТОРЫ ==============

class Operator(Base):
    """Операторы — кого оцениваем"""
    __tablename__ = "operators"
    
    id = Column(Integer, primary_key=True)
    full_name = Column(String, nullable=False)
    login = Column(String, unique=True, nullable=False)
    avatar_letters = Column(String(2))
    
    # Стаж
    hire_date = Column(Date, nullable=True)
    experience_days = Column(Integer, default=0)
    
    # Статистика (обновляется автоматически)
    total_evaluations = Column(Integer, default=0)
    avg_quality = Column(Float, default=0.0)
    avg_call_duration = Column(Integer, default=0)  # Средняя длительность звонка в секундах
    
    # Проект
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    is_active = Column(Boolean, default=True)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    project = relationship("Project", back_populates="operators")
    evaluations = relationship("Evaluation", back_populates="operator")


# ============== ТЕМЫ ЗВОНКОВ ==============

class Topic(Base):
    """Темы звонков для проекта"""
    __tablename__ = "topics"
    
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    
    name = Column(String, nullable=False)  # Название темы
    description = Column(String, nullable=True)  # Описание
    
    # Порядок отображения
    display_order = Column(Integer, default=0)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    project = relationship("Project", back_populates="topics")
    evaluations = relationship("Evaluation", back_populates="topic")  # Связь с оценками


# ============== МЕТРИКИ И ПОДБЛОКИ ==============

class MetricBlock(Base):
    """Подблоки метрик (например: 'Оценка звонка', 'Работа в CRM')"""
    __tablename__ = "metric_blocks"
    
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey("projects.id", ondelete="CASCADE"), nullable=False)
    
    name = Column(String, nullable=False)  # Название подблока
    description = Column(String, nullable=True)
    
    # Порядок отображения
    display_order = Column(Integer, default=0)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    project = relationship("Project", back_populates="metric_blocks")
    metrics = relationship("Metric", back_populates="block", cascade="all, delete-orphan")


class Metric(Base):
    """Метрики оценки — принадлежат подблоку"""
    __tablename__ = "metrics"
    
    id = Column(Integer, primary_key=True)
    block_id = Column(Integer, ForeignKey("metric_blocks.id", ondelete="CASCADE"), nullable=False)
    
    name = Column(String, nullable=False)  # Приветствие, Скрипт и т.д.
    description = Column(String, nullable=True)
    
    # Максимальный балл по метрике
    max_score = Column(Integer, nullable=False, default=5)
    
    # Тип метрики
    is_critical = Column(Boolean, default=False)  # Критичная ли метрика
    is_global_critical = Column(Boolean, default=False)  # Глобальная критическая ошибка (обнуляет все блоки)
    allow_na = Column(Boolean, default=True)  # Разрешено ли Н/О для этой метрики
    
    # Для критичных метрик — настройки штрафа
    penalty_type = Column(String, nullable=True)  # "block", "total" или "global"
    penalty_value = Column(Float, nullable=True)  # Размер штрафа в %
    
    # Обнуление подблока при невыполнении
    resets_block = Column(Boolean, default=False)  # Если True и метрика = 0 -> весь блок = 0
    
    # Порядок отображения внутри блока
    display_order = Column(Integer, default=0)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    block = relationship("MetricBlock", back_populates="metrics")
    evaluations = relationship("EvaluationMetric", back_populates="metric")


# ============== ОЦЕНКИ ==============

class Evaluation(Base):
    """Оценка звонка — главная таблица"""
    __tablename__ = "evaluations"
    
    id = Column(Integer, primary_key=True)
    
    # Кого оценили
    operator_id = Column(Integer, ForeignKey("operators.id"), nullable=False)
    
    # Кто оценил
    evaluator_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Тема звонка (новая связь)
    topic_id = Column(Integer, ForeignKey("topics.id"), nullable=True)
    
    # Данные звонка
    call_link = Column(String, nullable=True)  # Ссылка на запись
    call_date = Column(Date, nullable=False)  # Дата звонка
    
    # Длительность звонка (НОВОЕ ПОЛЕ)
    call_duration = Column(Integer, nullable=True)  # Длительность в секундах
    
    # Дата оценки (когда оценили)
    evaluation_date = Column(Date, default=date.today)  # Дата проведения оценки
    
    # Итоговые показатели
    total_score = Column(Float, default=0.0)  # Набранные баллы (сумма по всем метрикам)
    max_possible_score = Column(Float, default=0.0)  # Максимально возможные баллы
    base_percent = Column(Float, default=0.0)  # Базовый процент до штрафов
    penalty_percent = Column(Float, default=0.0)  # Суммарный штраф
    quality_percent = Column(Float, default=0.0)  # Итоговый процент (после штрафов)
    
    # Комментарий
    comment = Column(Text, nullable=True)
    
    # Метаданные
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    operator = relationship("Operator", back_populates="evaluations")
    evaluator = relationship("User", back_populates="evaluations_given")
    topic = relationship("Topic", back_populates="evaluations")  # Связь с темой
    metrics = relationship("EvaluationMetric", back_populates="evaluation", cascade="all, delete-orphan")
    block_results = relationship("EvaluationBlockResult", back_populates="evaluation", cascade="all, delete-orphan")


class EvaluationMetric(Base):
    """Детализация оценки по каждой метрике"""
    __tablename__ = "evaluation_metrics"
    
    id = Column(Integer, primary_key=True)
    
    evaluation_id = Column(Integer, ForeignKey("evaluations.id", ondelete="CASCADE"), nullable=False)
    metric_id = Column(Integer, ForeignKey("metrics.id"), nullable=False)
    
    # Баллы
    earned_score = Column(Integer, nullable=True)  # Что поставили (NULL если Н/О)
    max_score = Column(Integer, nullable=False)  # Максимум по метрике (на момент оценки)
    
    # Комментарий к конкретной метрике (почему не выполнено)
    comment = Column(Text, nullable=True)
    
    # Флаг "не оценивалось"
    is_not_evaluated = Column(Boolean, default=False)
    
    # Флаг применения штрафа
    penalty_applied = Column(Boolean, default=False)
    
    # Связи
    evaluation = relationship("Evaluation", back_populates="metrics")
    metric = relationship("Metric", back_populates="evaluations")


class EvaluationBlockResult(Base):
    """Результаты по подблокам для каждой оценки"""
    __tablename__ = "evaluation_block_results"
    
    id = Column(Integer, primary_key=True)
    
    evaluation_id = Column(Integer, ForeignKey("evaluations.id", ondelete="CASCADE"), nullable=False)
    block_id = Column(Integer, ForeignKey("metric_blocks.id"), nullable=False)
    
    # Результаты
    earned_score = Column(Float, default=0.0)  # Набранные баллы в блоке
    max_score = Column(Float, default=0.0)  # Максимальные баллы в блоке
    percent = Column(Float, default=0.0)  # Процент качества по блоку
    
    # Флаги
    was_reset = Column(Boolean, default=False)  # Был ли обнулен
    critical_failures = Column(Integer, default=0)  # Количество критичных ошибок
    
    # Связи
    evaluation = relationship("Evaluation", back_populates="block_results")
    block = relationship("MetricBlock")


# ============== ИСТОРИЯ ==============

class QualityHistory(Base):
    """История качества для графиков"""
    __tablename__ = "quality_history"
    
    id = Column(Integer, primary_key=True)
    
    operator_id = Column(Integer, ForeignKey("operators.id"), nullable=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=True)
    block_id = Column(Integer, ForeignKey("metric_blocks.id"), nullable=True)
    
    # Период
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    week = Column(Integer, nullable=True)
    
    # Показатели
    avg_quality = Column(Float, default=0.0)
    avg_call_duration = Column(Integer, default=0)  # Средняя длительность звонка
    evaluations_count = Column(Integer, default=0)
    critical_errors_count = Column(Integer, default=0)
    blocks_reset_count = Column(Integer, default=0)
    
    # Динамика
    change_from_previous = Column(Float, default=0.0)
    
    created_at = Column(DateTime, default=datetime.utcnow)


# ============== ТИКЕТЫ ==============

class TicketStatus(Enum):
    """Статусы тикетов"""
    NEW = "new"  # Новый
    IN_PROGRESS = "in_progress"  # В работе
    COMPLETED = "completed"  # Выполнен
    CANCELLED = "cancelled"  # Отменен


class TicketPriority(Enum):
    """Приоритеты тикетов"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TicketTopic(Enum):
    """Темы тикетов"""
    EVALUATION = "evaluation"  # По результатам оценки
    GENERAL = "general"  # Общая проработка
    METRIC = "metric"  # По конкретной метрике
    BLOCK = "block"  # По блоку метрик


class Ticket(Base):
    """Тикеты на проработку"""
    __tablename__ = "tickets"
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)  # Заголовок тикета
    description = Column(Text, nullable=True)  # Описание
    
    # Связи
    evaluation_id = Column(Integer, ForeignKey("evaluations.id"), nullable=True)  # Связь с оценкой
    operator_id = Column(Integer, ForeignKey("operators.id"), nullable=False)  # Оператор
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)  # Проект
    created_by = Column(Integer, ForeignKey("users.id"), nullable=False)  # Кто создал
    assigned_to = Column(Integer, ForeignKey("users.id"), nullable=True)  # Кому назначен
    
    # Тема и приоритет
    topic = Column(String, nullable=False, default=TicketTopic.EVALUATION.value)
    priority = Column(String, nullable=False, default=TicketPriority.MEDIUM.value)
    
    # Статус и SLA
    status = Column(String, nullable=False, default=TicketStatus.NEW.value)
    sla_deadline = Column(Date, nullable=True)  # Дедлайн по SLA (2 рабочих дня)
    
    # Даты
    created_at = Column(DateTime, default=datetime.utcnow)
    taken_at = Column(DateTime, nullable=True)  # Когда взяли в работу
    completed_at = Column(DateTime, nullable=True)  # Когда завершили
    
    # Дополнительные поля для проработки
    block_id = Column(Integer, ForeignKey("metric_blocks.id"), nullable=True)  # Фильтр по блоку
    metric_id = Column(Integer, ForeignKey("metrics.id"), nullable=True)  # Фильтр по метрике
    meeting_link = Column(String, nullable=True)  # Ссылка на встречу
    work_comment = Column(Text, nullable=True)  # Комментарий по проработке
    
    # Связи
    evaluation = relationship("Evaluation", backref="tickets")
    operator = relationship("Operator", backref="tickets")
    project = relationship("Project", backref="tickets")
    creator = relationship("User", foreign_keys=[created_by], backref="created_tickets")
    assignee = relationship("User", foreign_keys=[assigned_to], backref="assigned_tickets")
    block = relationship("MetricBlock", backref="tickets")
    metric = relationship("Metric", backref="tickets")


class TicketHistory(Base):
    """История изменений тикета"""
    __tablename__ = "ticket_history"
    
    id = Column(Integer, primary_key=True)
    ticket_id = Column(Integer, ForeignKey("tickets.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    action = Column(String, nullable=False)  # created, assigned, completed, etc.
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Связи
    ticket = relationship("Ticket", backref="history")
    user = relationship("User", backref="ticket_actions")

# ============== ФУНКЦИИ ==============

def init_db():
    """Создание всех таблиц"""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Получение сессии БД"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def calculate_evaluation_scores(evaluation, db_session):
    """
    Пересчет всех показателей для оценки
    """
    try:
        print(f"\n{'='*50}")
        print(f"🔄 Пересчет оценки ID: {evaluation.id}")
        
        # Очищаем старые результаты по блокам
        for br in evaluation.block_results:
            db_session.delete(br)
        
        db_session.flush()
        
        # Получаем все метрики оценки
        eval_metrics = evaluation.metrics
        print(f"📊 Всего метрик: {len(eval_metrics)}")
        
        # Группируем по блокам
        metrics_by_block = {}
        for em in eval_metrics:
            if em.metric and em.metric.block:
                block_id = em.metric.block_id
                if block_id not in metrics_by_block:
                    metrics_by_block[block_id] = []
                metrics_by_block[block_id].append(em)
                print(f"  - Метрика '{em.metric.name}': earned={em.earned_score}, max={em.max_score}")
        
        if not metrics_by_block:
            print("❌ Нет метрик, привязанных к блокам!")
            evaluation.quality_percent = 0
            return 0
        
        total_penalty = 0
        block_percents = []
        
        # Обрабатываем каждый блок
        for block_id, metrics_list in metrics_by_block.items():
            block = metrics_list[0].metric.block
            
            block_earned = 0
            block_max = 0
            block_reset = False
            critical_failures = 0
            
            print(f"\n📦 Блок: {block.name} (ID: {block_id})")
            
            # Проверяем на обнуление
            for em in metrics_list:
                if not em.is_not_evaluated and em.earned_score == 0:
                    if em.metric.resets_block:
                        block_reset = True
                        print(f"  ⚠️ Метрика '{em.metric.name}' обнуляет блок!")
            
            # Если блок не обнулен, считаем баллы
            if not block_reset:
                for em in metrics_list:
                    if not em.is_not_evaluated and em.earned_score is not None:
                        block_earned += em.earned_score
                        block_max += em.max_score
                        print(f"  ✓ {em.metric.name}: {em.earned_score}/{em.max_score}")
                        
                        # Проверяем критичные метрики
                        if em.earned_score == 0 and em.metric.is_critical:
                            critical_failures += 1
                            em.penalty_applied = True
                            
                            if em.metric.penalty_type == 'total':
                                total_penalty += em.metric.penalty_value or 0
                                print(f"  ⚠️ Критичная метрика, штраф: {em.metric.penalty_value}%")
                        else:
                            em.penalty_applied = False
            else:
                print(f"  ❌ Блок обнулен, все метрики = 0")
                block_earned = 0
                block_max = sum(em.max_score for em in metrics_list)
            
            # Рассчитываем процент по блоку
            if block_max > 0:
                block_percent = (block_earned / block_max) * 100
            else:
                block_percent = 0
            
            block_percents.append(block_percent)
            print(f"  📈 Процент блока: {block_percent:.1f}%")
            
            # Сохраняем результат блока
            block_result = EvaluationBlockResult(
                evaluation_id=evaluation.id,
                block_id=block_id,
                earned_score=block_earned,
                max_score=block_max,
                percent=block_percent,
                was_reset=block_reset,
                critical_failures=critical_failures
            )
            db_session.add(block_result)
        
        # Рассчитываем общий процент как среднее арифметическое по блокам
        if block_percents:
            base_percent = sum(block_percents) / len(block_percents)
            print(f"\n📊 Среднее по блокам: {base_percent:.1f}%")
            print(f"   Проценты блоков: {[round(p, 1) for p in block_percents]}")
        else:
            base_percent = 0
            print(f"\n📊 Нет блоков, base_percent = 0")
        
        # Применяем штрафы
        print(f"💰 Суммарный штраф: {total_penalty}%")
        final_percent = base_percent * (1 - total_penalty / 100)
        final_percent = max(0, min(100, final_percent))
        
        print(f"🎯 Итоговое качество: {final_percent:.1f}%")
        
        # Обновляем оценку
        evaluation.total_score = sum(em.earned_score or 0 for em in eval_metrics)
        evaluation.max_possible_score = sum(em.max_score or 0 for em in eval_metrics)
        evaluation.base_percent = base_percent
        evaluation.penalty_percent = total_penalty
        evaluation.quality_percent = final_percent
        
        db_session.flush()
        
        print(f"{'='*50}\n")
        return evaluation.quality_percent
        
    except Exception as e:
        print(f"❌ Ошибка в calculate_evaluation_scores: {str(e)}")
        import traceback
        traceback.print_exc()
        evaluation.quality_percent = 0
        return 0


def update_operator_avg_quality(operator_id, db_session):
    """Обновляет среднее качество оператора и среднюю длительность звонков"""
    evaluations = db_session.query(Evaluation).filter_by(operator_id=operator_id).all()
    
    if evaluations:
        # Среднее качество
        avg_quality = sum(e.quality_percent for e in evaluations) / len(evaluations)
        
        # Средняя длительность звонка (только для оценок с указанной длительностью)
        durations = [e.call_duration for e in evaluations if e.call_duration is not None]
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        operator = db_session.query(Operator).get(operator_id)
        operator.avg_quality = avg_quality
        operator.avg_call_duration = int(avg_duration)  # Сохраняем как целое число секунд
        operator.total_evaluations = len(evaluations)


def calculate_evaluation_scores(evaluation, db_session):
    """
    Пересчет всех показателей для оценки
    """
    try:
        print(f"\n{'='*50}")
        print(f"🔄 Пересчет оценки ID: {evaluation.id}")
        
        # Очищаем старые результаты по блокам
        for br in evaluation.block_results:
            db_session.delete(br)
        
        db_session.flush()
        
        # Получаем все метрики оценки
        eval_metrics = evaluation.metrics
        print(f"📊 Всего метрик: {len(eval_metrics)}")
        
        # Проверяем глобальные критические ошибки
        global_reset = False
        global_reset_metric = None
        
        for em in eval_metrics:
            if em.metric and em.metric.is_global_critical and not em.is_not_evaluated and em.earned_score == 0:
                global_reset = True
                global_reset_metric = em.metric.name
                print(f"💥 ГЛОБАЛЬНАЯ КРИТИЧЕСКАЯ ОШИБКА: '{em.metric.name}' - все блоки обнулены!")
                break
        
        # Группируем по блокам
        metrics_by_block = {}
        for em in eval_metrics:
            if em.metric and em.metric.block:
                block_id = em.metric.block_id
                if block_id not in metrics_by_block:
                    metrics_by_block[block_id] = []
                metrics_by_block[block_id].append(em)
                print(f"  - Метрика '{em.metric.name}': earned={em.earned_score}, max={em.max_score}")
        
        if not metrics_by_block:
            print("❌ Нет метрик, привязанных к блокам!")
            evaluation.quality_percent = 0
            return 0
        
        total_penalty = 0
        block_percents = []
        
        # Обрабатываем каждый блок
        for block_id, metrics_list in metrics_by_block.items():
            block = metrics_list[0].metric.block
            
            block_earned = 0
            block_max = 0
            block_reset = False
            critical_failures = 0
            
            print(f"\n📦 Блок: {block.name} (ID: {block_id})")
            
            # Если глобальная критическая ошибка - все блоки обнулены
            if global_reset:
                print(f"  💥 Блок обнулен из-за глобальной критической ошибки!")
                block_reset = True
                block_earned = 0
                block_max = sum(em.max_score for em in metrics_list)
            else:
                # Проверяем на обнуление внутри блока
                for em in metrics_list:
                    if not em.is_not_evaluated and em.earned_score == 0:
                        if em.metric.resets_block:
                            block_reset = True
                            print(f"  ⚠️ Метрика '{em.metric.name}' обнуляет блок!")
                
                # Если блок не обнулен, считаем баллы
                if not block_reset:
                    for em in metrics_list:
                        if not em.is_not_evaluated and em.earned_score is not None:
                            block_earned += em.earned_score
                            block_max += em.max_score
                            print(f"  ✓ {em.metric.name}: {em.earned_score}/{em.max_score}")
                            
                            # Проверяем критичные метрики
                            if em.earned_score == 0 and em.metric.is_critical and not em.metric.is_global_critical:
                                critical_failures += 1
                                em.penalty_applied = True
                                
                                if em.metric.penalty_type == 'total':
                                    total_penalty += em.metric.penalty_value or 0
                                    print(f"  ⚠️ Критичная метрика, штраф: {em.metric.penalty_value}%")
                            else:
                                em.penalty_applied = False
                else:
                    print(f"  ❌ Блок обнулен, все метрики = 0")
                    block_earned = 0
                    block_max = sum(em.max_score for em in metrics_list)
            
            # Рассчитываем процент по блоку
            if block_max > 0:
                block_percent = (block_earned / block_max) * 100
            else:
                block_percent = 0
            
            block_percents.append(block_percent)
            print(f"  📈 Процент блока: {block_percent:.1f}%")
            
            # Сохраняем результат блока
            block_result = EvaluationBlockResult(
                evaluation_id=evaluation.id,
                block_id=block_id,
                earned_score=block_earned,
                max_score=block_max,
                percent=block_percent,
                was_reset=block_reset,
                critical_failures=critical_failures
            )
            db_session.add(block_result)
        
        # Рассчитываем общий процент как среднее арифметическое по блокам
        if block_percents:
            base_percent = sum(block_percents) / len(block_percents)
            print(f"\n📊 Среднее по блокам: {base_percent:.1f}%")
            print(f"   Проценты блоков: {[round(p, 1) for p in block_percents]}")
        else:
            base_percent = 0
            print(f"\n📊 Нет блоков, base_percent = 0")
        
        # Применяем штрафы
        print(f"💰 Суммарный штраф: {total_penalty}%")
        final_percent = base_percent * (1 - total_penalty / 100)
        final_percent = max(0, min(100, final_percent))
        
        if global_reset:
            print(f"💥 Итоговое качество: 0% (глобальная критическая ошибка)")
            final_percent = 0
        
        print(f"🎯 Итоговое качество: {final_percent:.1f}%")
        
        # Обновляем оценку
        evaluation.total_score = sum(em.earned_score or 0 for em in eval_metrics)
        evaluation.max_possible_score = sum(em.max_score or 0 for em in eval_metrics)
        evaluation.base_percent = base_percent
        evaluation.penalty_percent = total_penalty
        evaluation.quality_percent = final_percent
        
        db_session.flush()
        
        print(f"{'='*50}\n")
        return evaluation.quality_percent
        
    except Exception as e:
        print(f"❌ Ошибка в calculate_evaluation_scores: {str(e)}")
        import traceback
        traceback.print_exc()
        evaluation.quality_percent = 0
        return 0