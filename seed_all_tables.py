# seed_all_tables.py
import psycopg2
from psycopg2 import sql
from datetime import date, timedelta, datetime
import logging
import random
import string

# === Настройка логирования ===
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()

# === Конфигурация подключений ===
GLOBAL_CONN = {
    "host": "192.168.64.4",
    "port": 5432,
    "dbname": "global_tech",
    "user": "global_user",
    "password": "123"
}

BRANCH_CONNS = {
    1: {"host": "192.168.64.5", "port": 5432, "dbname": "global_tech", "user": "branch1_user", "password": "123"},
    2: {"host": "192.168.64.6", "port": 5432, "dbname": "global_tech", "user": "branch2_user", "password": "123"},
    # Можно легко добавить 3, 4 и т.д.
}

# === Вспомогательные данные ===
ROLES = [
    (1, 'developer', 'Software developer'),
    (2, 'manager', 'Project manager'),
    (3, 'analyst', 'Data analyst'),
    (4, 'qa', 'Quality assurance engineer'),
    (5, 'devops', 'DevOps engineer')
]

SKILLS = [
    'Python', 'JavaScript', 'SQL', 'PostgreSQL', 'Docker', 'Kubernetes',
    'React', 'Vue', 'FastAPI', 'Django', 'Pandas', 'TensorFlow',
    'CI/CD', 'Linux', 'Git', 'AWS', 'Yandex Cloud', 'Prometheus', 'Grafana'
]

BRANCHES = [
    (1, 'Moscow HQ', 'Russia, Moscow', 'Europe/Moscow'),
    (2, 'Berlin Office', 'Germany, Berlin', 'Europe/Berlin')
]

DEPARTMENTS_BY_BRANCH = {
    1: ['Backend', 'Frontend', 'DevOps', 'QA'],
    2: ['Data Science', 'ML Engineering', 'Analytics', 'Product']
}

FIRST_NAMES = {
    1: ['Иван', 'Алексей', 'Дмитрий', 'Сергей', 'Андрей', 'Максим', 'Егор', 'Артём'],
    2: ['Anna', 'Lukas', 'Sophie', 'Max', 'Leon', 'Mia', 'Jonas', 'Lena']
}
LAST_NAMES = {
    1: ['Петров', 'Сидоров', 'Кузнецов', 'Смирнов', 'Попов', 'Волков', 'Морозов'],
    2: ['Schmidt', 'Weber', 'Fischer', 'Wagner', 'Becker', 'Hoffmann', 'Schulz']
}
DOMAINS = {1: 'global.tech', 2: 'global.tech'}

TASK_TEMPLATES = [
    (1, 'Реализовать API-эндпоинт', 'Создать RESTful endpoint с валидацией', 8, 'high'),
    (2, 'Написать unit-тесты', 'Покрыть ядро логики pytest', 4, 'medium'),
    (3, 'Спроектировать UI-компонент', 'Figma → React компонент', 6, 'high'),
    (4, 'Настроить CI/CD пайплайн', 'Автоматизировать сборку и деплой', 10, 'critical'),
    (5, 'Анализ данных', 'Провести EDA и подготовить отчёт', 5, 'medium')
]

PROJECTS_PER_BRANCH = {
    1: [
        ('Auth Microservice', 'OAuth2 + JWT сервис аутентификации'),
        ('Billing API', 'Система выставления счетов'),
        ('Internal CRM', 'CRM для внутреннего использования'),
    ],
    2: [
        ('Analytics Dashboard', 'Дашборд поведения пользователей в реальном времени'),
        ('ML Model Training', 'Обучение модели распознавания мошенничества'),
        ('Data Pipeline', 'ETL-пайплайн для логов'),
    ]
}

def get_conn(config):
    return psycopg2.connect(**config)

# === Генерация случайного email ===
def generate_email(first, last, branch_id):
    domain = DOMAINS[branch_id]
    suffix = ''.join(random.choices(string.digits, k=2))
    return f"{first.lower()}.{last.lower()}{suffix}@{domain}"

# === Основной сидинг глобальной БД ===
def seed_global():
    logger.info("Заполнение ГЛОБАЛЬНОЙ базы данных...")
    try:
        with get_conn(GLOBAL_CONN) as conn:
            cur = conn.cursor()

            # branches
            cur.executemany("""
                INSERT INTO global.branches (id, name, location, timezone)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, BRANCHES)

            # roles
            cur.executemany("""
                INSERT INTO global.roles (id, name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, ROLES)

            # skills
            skill_rows = [(i+1, name) for i, name in enumerate(SKILLS)]
            cur.executemany("""
                INSERT INTO global.skills (id, name)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING
            """, skill_rows)

            # departments
            dept_id = 1
            dept_data = []
            for branch_id, names in DEPARTMENTS_BY_BRANCH.items():
                for name in names:
                    dept_data.append((dept_id, name, branch_id))
                    dept_id += 1
            cur.executemany("""
                INSERT INTO global.departments (id, name, branch_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, dept_data)

            # employees
            emp_id = 1
            employees = []
            emp_skills = []
            emp_deps = []

            for branch_id in BRANCHES:
                bid = branch_id[0]
                for _ in range(12):  # 12 сотрудников на филиал → 24 всего
                    first = random.choice(FIRST_NAMES[bid])
                    last = random.choice(LAST_NAMES[bid])
                    full_name = f"{first} {last}"
                    email = generate_email(first, last, bid)
                    role_id = random.choice([1, 3, 4, 5])  # developer, analyst, qa, devops
                    hired = date.today() - timedelta(days=random.randint(30, 730))
                    employees.append((emp_id, full_name, email, role_id, bid, hired))

                    # Навыки (2–4 на сотрудника)
                    chosen_skills = random.sample(range(1, len(SKILLS)+1), k=random.randint(2, 4))
                    for sid in chosen_skills:
                        level = random.choice(['beginner', 'intermediate', 'expert'])
                        emp_skills.append((emp_id, sid, level))

                    # Отдел (один на сотрудника)
                    branch_depts = [d for d in dept_data if d[2] == bid]
                    if branch_depts:
                        dept = random.choice(branch_depts)
                        is_head = False
                        emp_deps.append((emp_id, dept[0], is_head))

                    emp_id += 1

            # Добавим явно менеджеров (по одному на филиал)
            for bid in [1, 2]:
                first = random.choice(FIRST_NAMES[bid])
                last = random.choice(LAST_NAMES[bid])
                full_name = f"{first} {last}"
                email = generate_email(first, last, bid)
                employees.append((emp_id, full_name, email, 2, bid, date.today() - timedelta(days=100)))
                emp_deps.append((emp_id, [d for d in dept_data if d[2] == bid][0][0], True))
                emp_id += 1

            # Вставка сотрудников с уникальностью по email
            cur.executemany("""
                INSERT INTO global.employees (id, full_name, email, role_id, branch_id, hired_at, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, true)
                ON CONFLICT (email) DO NOTHING
            """, employees)

            cur.executemany("""
                INSERT INTO global.employee_skills (employee_id, skill_id, proficiency_level)
                VALUES (%s, %s, %s)
                ON CONFLICT (employee_id, skill_id) DO NOTHING
            """, emp_skills)

            cur.executemany("""
                INSERT INTO global.employee_departments (employee_id, department_id, is_head)
                VALUES (%s, %s, %s)
                ON CONFLICT (employee_id, department_id) DO NOTHING
            """, emp_deps)

            # holidays
            cur.execute("""
                INSERT INTO global.holidays (branch_id, name, date_from, date_to)
                VALUES 
                  (1, 'Новогодние каникулы', '2025-12-30', '2026-01-08'),
                  (2, 'День германского единства', '2025-10-03', '2025-10-03'),
                  (NULL, 'Международный день труда', '2025-05-01', '2025-05-01')
                ON CONFLICT DO NOTHING
            """)

            # task_templates
            cur.executemany("""
                INSERT INTO global.task_templates (id, title, description, estimated_hours, priority_level)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, TASK_TEMPLATES)

            conn.commit()
        logger.info(" Глобальная БД успешно заполнена.")
    except Exception as e:
        logger.error(f" Ошибка при заполнении глобальной БД: {e}")
        raise

# === Заполнение филиальной БД ===
def seed_branch(branch_id, config):
    logger.info(f"Заполнение ФИЛИАЛА {branch_id} ({config['host']})...")
    try:
        with get_conn(config) as conn:
            cur = conn.cursor()

            # Получим менеджера этого филиала
            with get_conn(GLOBAL_CONN) as gconn:
                gcur = gconn.cursor()
                gcur.execute("SELECT id FROM global.employees WHERE branch_id = %s AND role_id = 2", (branch_id,))
                manager_row = gcur.fetchone()
                if not manager_row:
                    raise ValueError(f"Менеджер для филиала {branch_id} не найден!")
                manager_id = manager_row[0]

            # projects
            project_ids = []
            for i, (name, desc) in enumerate(PROJECTS_PER_BRANCH[branch_id], start=1):
                pid = branch_id * 100 + i
                project_ids.append(pid)
                start = date.today() - timedelta(days=30)
                end = start + timedelta(days=random.randint(60, 120))
                cur.execute("""
                    INSERT INTO branch.projects (id, name, description, manager_id, start_date, end_date, status)
                    VALUES (%s, %s, %s, %s, %s, %s, 'active')
                    ON CONFLICT (id) DO NOTHING
                """, (pid, name, desc, manager_id, start, end))

            # tasks
            task_id = branch_id * 1000
            tasks = []
            comments = []
            attachments = []
            time_logs = []
            notifications = []

            for pid in project_ids:
                for _ in range(15):  # ~15 задач на проект → ~45 на филиал
                    title = f"Задача {task_id}"
                    assignee_id = random.choice([e[0] for e in get_employees_in_branch(branch_id) if e[3] != 2])  # не менеджер
                    template_id = random.choice([t[0] for t in TASK_TEMPLATES])
                    status = random.choice(['todo', 'in_progress', 'done'])
                    priority = random.choice(['low', 'medium', 'high', 'critical'])
                    due = date.today() + timedelta(days=random.randint(1, 30))
                    tasks.append((task_id, title, '', assignee_id, manager_id, template_id, pid, status, priority, due))

                    # comment
                    comments.append((task_id * 10 + 1, task_id, assignee_id, f"Начал работу над задачей {task_id}"))

                    # attachment
                    attachments.append((task_id, task_id, f"doc_{task_id}.pdf", f"/files/doc_{task_id}.pdf", assignee_id))

                    # time log (если статус != todo)
                    if status != 'todo':
                        hours = round(random.uniform(1.0, 8.0), 1)
                        worked = date.today() - timedelta(days=random.randint(0, 5))
                        time_logs.append((task_id, task_id, assignee_id, hours, worked, f"Работа по задаче {task_id}"))

                    # notification
                    notifications.append((task_id, assignee_id, f"Вам назначена задача: {title}", False, task_id))

                    task_id += 1

            cur.executemany("""
                INSERT INTO branch.tasks (id, title, description, assignee_id, reporter_id, template_id, project_id, status, priority, due_date, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
            """, tasks)

            cur.executemany("""
                INSERT INTO branch.comments (id, task_id, author_id, content, created_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
            """, comments)

            cur.executemany("""
                INSERT INTO branch.attachments (id, task_id, file_name, file_path, uploaded_by, uploaded_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
            """, attachments)

            cur.executemany("""
                INSERT INTO branch.time_logs (id, task_id, employee_id, hours_spent, date_worked, description)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, time_logs)

            cur.executemany("""
                INSERT INTO branch.notifications (id, employee_id, message, is_read, related_task_id, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO NOTHING
            """, notifications)

            conn.commit()
        logger.info(f"Филиал {branch_id} успешно заполнен.")
    except Exception as e:
        logger.error(f"Ошибка при заполнении филиала {branch_id}: {e}")
        raise

# === Вспомогательная функция: получить сотрудников филиала ===
def get_employees_in_branch(branch_id):
    with get_conn(GLOBAL_CONN) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, full_name, email, role_id FROM global.employees WHERE branch_id = %s", (branch_id,))
        return cur.fetchall()

# === Запуск ===
if __name__ == "__main__":
    try:
        seed_global()
        for bid, config in BRANCH_CONNS.items():
            seed_branch(bid, config)
        logger.info("\nВсе таблицы успешно заполнены разнообразными демо-данными!")
        logger.info("Объём данных: ~26 сотрудников, ~6 проектов, ~90 задач, ~90 комментариев и уведомлений.")
    except Exception as e:
        logger.error(f"\n Критическая ошибка: {e}")
        exit(1)