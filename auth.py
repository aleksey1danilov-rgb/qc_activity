from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional
import hashlib
import hmac
from datetime import datetime, date  # Добавил date

from database import (
    SessionLocal, get_db, User, Project, Evaluation,
    MetricBlock, Metric, EvaluationMetric, EvaluationBlockResult,
    ProjectManager, QualityHistory, UserRole, Ticket, TicketStatus  # Добавил Ticket и TicketStatus
)

router = APIRouter(prefix="/auth", tags=["auth"])

# Шаблоны
templates = Jinja2Templates(directory="templates")

# ============== ХЕШИРОВАНИЕ ПАРОЛЕЙ ==============

def hash_password(password: str) -> str:
    """Хеширование пароля (SHA256)"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Проверка пароля"""
    return hmac.compare_digest(hash_password(plain_password), hashed_password)


# ============== ЗАВИСИМОСТИ ==============

async def get_current_user(
    request: Request,
    db: Session = Depends(get_db)
):
    """Получение текущего пользователя из куки"""
    user_id = request.cookies.get("user_id")
    if not user_id:
        raise HTTPException(
            status_code=401,
            detail="Не авторизован",
            headers={"Location": "/auth/login"}
        )

    user = db.get(User, int(user_id))
    if not user or not user.is_active:
        raise HTTPException(status_code=403, detail="Пользователь не найден или заблокирован")

    request.state.user = user
    return user


async def get_optional_user(
    request: Request,
    db: Session = Depends(get_db)
):
    """Получение пользователя если есть кука"""
    user_id = request.cookies.get("user_id")
    if user_id:
        user = db.get(User, int(user_id))
        if user and user.is_active:
            request.state.user = user
            return user
    return None


def require_role(*roles):
    """Декоратор для проверки роли пользователя"""
    async def role_checker(current_user: User = Depends(get_current_user)):
        # Преобразуем Enum в значения для сравнения
        role_values = []
        for role in roles:
            if hasattr(role, 'value'):  # если это Enum
                role_values.append(role.value)
            else:
                role_values.append(role)
        
        # Проверяем, есть ли роль пользователя в списке разрешенных
        if current_user.role not in role_values:
            raise HTTPException(
                status_code=403,
                detail=f"Недостаточно прав. Ваша роль: {current_user.role}, требуется: {role_values}"
            )
        return current_user
    return role_checker

# ============== РЕГИСТРАЦИЯ ==============

@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    """Страница регистрации"""
    return templates.TemplateResponse(
        "register.html",
        {"request": request}
    )


@router.post("/register")
async def register(
    request: Request,
    db: Session = Depends(get_db)
):
    """Обработка регистрации нового пользователя"""
    form = await request.form()

    full_name = form.get("full_name")
    login = form.get("login")
    password = form.get("password")
    password_confirm = form.get("password_confirm")
    email = form.get("email")
    role = form.get("role", UserRole.CONTROLLER)

    # Валидация
    errors = {}

    existing_user = db.query(User).filter(User.login == login).first()
    if existing_user:
        errors["login"] = "Пользователь с таким логином уже существует"

    if email:
        existing_email = db.query(User).filter(User.email == email).first()
        if existing_email:
            errors["email"] = "Email уже используется"

    if len(password) < 6:
        errors["password"] = "Пароль должен быть минимум 6 символов"

    if password != password_confirm:
        errors["password_confirm"] = "Пароли не совпадают"

    if errors:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "errors": errors,
                "form_data": {
                    "full_name": full_name,
                    "login": login,
                    "email": email,
                    "role": role
                }
            }
        )

    # Создаем пользователя
    avatar_letters = ""
    if full_name:
        name_parts = full_name.split()
        if len(name_parts) >= 2:
            avatar_letters = name_parts[0][0] + name_parts[1][0]
        else:
            avatar_letters = full_name[:2].upper()

    new_user = User(
        full_name=full_name,
        login=login,
        password_hash=hash_password(password),
        email=email,
        role=role,
        avatar_letters=avatar_letters.upper() if avatar_letters else None,
        is_active=True
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    return RedirectResponse(
        url="/auth/login?registered=1",
        status_code=302
    )


# ============== АВТОРИЗАЦИЯ ==============

@router.get("/login", response_class=HTMLResponse)
async def login_page(
    request: Request,
    registered: Optional[str] = None,
    error: Optional[str] = None
):
    """Страница входа"""
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "registered": registered == "1",
            "error": error
        }
    )


@router.post("/login")
async def login(
    request: Request,
    db: Session = Depends(get_db)
):
    """Обработка входа"""
    form = await request.form()
    username = form.get("username")
    password = form.get("password")

    user = db.query(User).filter(User.login == username).first()

    if not user or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Неверный логин или пароль"
            },
            status_code=401
        )

    if not user.is_active:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Пользователь заблокирован"
            },
            status_code=403
        )

    # СОЗДАЕМ СЕССИЮ ЧЕРЕЗ COOKIE
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key="user_id",
        value=str(user.id),
        httponly=True,
        max_age=3600 * 24,  # 24 часа
        samesite="lax",
        path="/"
    )
    return response


@router.get("/logout")
async def logout():
    """Выход из системы"""
    response = RedirectResponse(url="/auth/login", status_code=302)
    response.delete_cookie("user_id", path="/")
    return response


@router.get("/profile", response_class=HTMLResponse)
async def profile_page(
    request: Request,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Страница профиля пользователя"""
    
    # Получаем все проекты для отображения
    all_projects = db.query(Project).filter(Project.is_active == True).all()
    
    # Получаем оценки пользователя (для контролера)
    evaluations = []
    if current_user.role == 'controller':
        evaluations = db.query(Evaluation).filter(
            Evaluation.evaluator_id == current_user.id
        ).order_by(Evaluation.created_at.desc()).limit(10).all()
    
    # Количество оценок пользователя
    evaluations_count = db.query(Evaluation).filter(
        Evaluation.evaluator_id == current_user.id
    ).count()
    
    # ============== СТАТИСТИКА ТИКЕТОВ ==============
    
    # Получаем проекты, доступные пользователю
    user_projects = []
    if current_user.role == 'admin':
        # Админ видит все проекты
        projects_query = db.query(Project.id).filter(Project.is_active == True)
        user_projects = [p[0] for p in projects_query.all()]
    elif current_user.role == 'manager':
        # Менеджер видит свои проекты
        user_projects = [pm.project_id for pm in current_user.manager_links]
    elif current_user.role == 'controller':
        # Контролер видит все проекты (можно изменить при необходимости)
        projects_query = db.query(Project.id).filter(Project.is_active == True)
        user_projects = [p[0] for p in projects_query.all()]
    
    # Базовый запрос для тикетов
    tickets_query = db.query(Ticket)
    
    # Фильтруем по проектам
    if user_projects:
        tickets_query = tickets_query.filter(Ticket.project_id.in_(user_projects))
    elif current_user.role not in ['admin', 'controller']:
        # Если нет проектов и не админ/контролер, то тикетов нет
        tickets_query = tickets_query.filter(False)
    
    # Статистика по тикетам
    ticket_stats = {
        "new": tickets_query.filter(Ticket.status == TicketStatus.NEW.value).count(),
        "in_progress": tickets_query.filter(Ticket.status == TicketStatus.IN_PROGRESS.value).count(),
        "overdue": tickets_query.filter(
            Ticket.status.in_([TicketStatus.NEW.value, TicketStatus.IN_PROGRESS.value]),
            Ticket.sla_deadline < date.today()
        ).count()
    }
    
    # Последние 5 тикетов
    recent_tickets = tickets_query.order_by(Ticket.created_at.desc()).limit(5).all()
    
    # Общее количество активных тикетов для бейджа
    tickets_count = tickets_query.filter(
        Ticket.status.in_([TicketStatus.NEW.value, TicketStatus.IN_PROGRESS.value])
    ).count()
    
    # ================================================
    
    return templates.TemplateResponse(
        "profile.html",
        {
            "request": request,
            "user": current_user,
            "projects": all_projects,
            "evaluations": evaluations,
            "evaluations_count": evaluations_count,
            "ticket_stats": ticket_stats,
            "recent_tickets": recent_tickets,
            "tickets_count": tickets_count,
            "date": date,
            "password_changed": request.query_params.get("password_changed", False)
            # Убрал обращение к request.session
        }
    )


# ============== ОТЛАДКА РОЛИ ==============

@router.get("/debug-role")
async def debug_role(current_user: User = Depends(get_current_user)):
    """Отладка роли пользователя"""
    return {
        "user_id": current_user.id,
        "login": current_user.login,
        "role": current_user.role,
        "role_type": str(type(current_user.role)),
        "is_admin": current_user.role == "admin",
        "is_admin_strict": current_user.role == UserRole.ADMIN,
        "admin_value": UserRole.ADMIN,
        "admin_value_type": str(type(UserRole.ADMIN))
    }


# ============== УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ (ТОЛЬКО АДМИН) ==============

@router.get("/admin/users", response_class=HTMLResponse)
async def admin_users(
    request: Request,
    db: Session = Depends(get_db)
):
    """Список пользователей (только админ) - упрощенная версия"""
    # Получаем пользователя напрямую из куки
    user_id = request.cookies.get("user_id")
    if not user_id:
        return RedirectResponse(url="/auth/login", status_code=302)
    
    user = db.get(User, int(user_id))
    if not user or not user.is_active:
        return RedirectResponse(url="/auth/login", status_code=302)
    
    # Проверяем роль простым сравнением
    if user.role != "admin":
        return HTMLResponse(
            content=f"<h1>Доступ запрещен</h1><p>Ваша роль: {user.role}. Требуется роль: admin</p>",
            status_code=403
        )
    
    users = db.query(User).all()
    projects = db.query(Project).filter(Project.is_active == True).all()
    
    return templates.TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "user": user,  # передаем текущего пользователя
            "users": users,
            "projects": projects
        }
    )

@router.post("/admin/users/create")
async def admin_create_user(
    request: Request,
    current_user: User = Depends(require_role(UserRole.ADMIN)),  # теперь будет работать!
    db: Session = Depends(get_db)
):
    """Создание пользователя администратором"""
    data = await request.json()

    # Проверяем уникальность логина
    if db.query(User).filter(User.login == data.get("login")).first():
        raise HTTPException(status_code=400, detail="Логин уже занят")
    
    # Генерируем инициалы
    avatar_letters = None
    full_name = data.get("full_name", "")
    if full_name:
        name_parts = full_name.split()
        if len(name_parts) >= 2:
            avatar_letters = name_parts[0][0] + name_parts[1][0]
        else:
            avatar_letters = full_name[:2].upper()
    
    new_user = User(
        full_name=full_name,
        login=data.get("login"),
        password_hash=hash_password(data.get("password", "123456")),
        email=data.get("email"),
        role=data.get("role"),
        avatar_letters=avatar_letters,
        is_active=data.get("is_active", True)
    )
    db.add(new_user)
    db.flush()

    # Здесь тоже используем строковое сравнение
    if data.get("role") == "manager":  # Строка, а не Enum
        for project_id in data.get("projects", []):
            pm = ProjectManager(
                user_id=new_user.id,
                project_id=project_id
            )
            db.add(pm)

    db.commit()

    return JSONResponse({
        "status": "success",
        "user_id": new_user.id,
        "message": f"Пользователь {new_user.full_name} создан"
    })

@router.post("/admin/users/{user_id}/toggle-block")
async def admin_toggle_block(
    user_id: int,
    current_user: User = Depends(require_role(UserRole.ADMIN)),
    db: Session = Depends(get_db)
):
    """Блокировка/разблокировка пользователя"""
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    if user.id == current_user.id:
        raise HTTPException(status_code=400, detail="Нельзя заблокировать себя")

    user.is_active = not user.is_active
    db.commit()

    return JSONResponse({
        "status": "success",
        "is_active": user.is_active
    })


@router.delete("/admin/users/{user_id}")
async def admin_delete_user(
    user_id: int,
    current_user: User = Depends(require_role(UserRole.ADMIN)),
    db: Session = Depends(get_db)
):
    """Удаление пользователя"""
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    if user.id == current_user.id:
        raise HTTPException(status_code=400, detail="Нельзя удалить себя")

    db.delete(user)
    db.commit()

    return JSONResponse({
        "status": "success",
        "message": f"Пользователь {user.full_name} удален"
    })