# task_manager_cli.py
import psycopg2
from datetime import date

# === Конфигурация ===
GLOBAL_CONN = {
    "host": "192.168.64.4",
    "port": 5432,
    "dbname": "global_tech",
    "user": "global_user",
    "password": "123"
}

BRANCH_CONNS = {
    1: {"host": "192.168.64.4", "port": 5433, "dbname": "global_tech", "user": "branch1_user", "password": "123"},
    2: {"host": "192.168.64.4", "port": 5434, "dbname": "global_tech", "user": "branch2_user", "password": "123"}
}

def get_conn(config):
    return psycopg2.connect(**config)

def get_employee(emp_id):
    with get_conn(GLOBAL_CONN) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, full_name, branch_id FROM global.employees WHERE id = %s AND is_active = true", (emp_id,))
        return cur.fetchone()

def get_template(template_id):
    with get_conn(GLOBAL_CONN) as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM global.task_templates WHERE id = %s", (template_id,))
        return cur.fetchone()

def create_task():
    print("\n Сценарий 1: Создание задачи")
    try:
        reporter_id = int(input("ID менеджера (reporter): "))
        assignee_id = int(input("ID исполнителя (assignee): "))
        template_id = int(input("ID шаблона задачи (1=API, 2=Tests, 3=UI): ") or 1)
        due_date = input("Срок выполнения (YYYY-MM-DD, например 2025-11-30): ") or "2025-11-30"
        title = input("Название задачи: ") or "Разработать API для оплаты"

        # Проверка сотрудников
        reporter = get_employee(reporter_id)
        assignee = get_employee(assignee_id)
        if not assignee:
            print(" Исполнитель не найден или неактивен")
            return
        if not reporter:
            print(" Менеджер не найден")
            return
        if reporter[2] != assignee[2]:
            print(" Менеджер и исполнитель должны быть из одного филиала!")
            return

        template = get_template(template_id)
        if not template:
            print(" Шаблон не найден, создаём без шаблона")
            template_id = None

        branch_id = assignee[2]
        # Найдём проект в этом филиале (берём первый)
        with get_conn(BRANCH_CONNS[branch_id]) as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM branch.projects WHERE manager_id = %s LIMIT 1", (reporter_id,))
            project = cur.fetchone()
            if not project:
                print(" Не найден проект, управляемый этим менеджером")
                return
            project_id = project[0]

            # Создаём задачу
            cur.execute("""
                INSERT INTO branch.tasks (title, assignee_id, reporter_id, template_id, project_id, status, priority, due_date, created_at)
                VALUES (%s, %s, %s, %s, %s, 'todo', 'high', %s, NOW())
                RETURNING id
            """, (title, assignee_id, reporter_id, template_id, project_id, due_date))
            task_id = cur.fetchone()[0]

            # Создаём уведомление
            cur.execute("""
                INSERT INTO branch.notifications (employee_id, message, is_read, related_task_id, created_at)
                VALUES (%s, %s, false, %s, NOW())
            """, (assignee_id, f"Вам назначена задача: {title}", task_id))

            conn.commit()
            print(f" Задача создана! ID={task_id}, филиал={branch_id}")
            print(f" Уведомление отправлено сотруднику {assignee[1]}")
    except Exception as e:
        print(f" Ошибка: {e}")

def log_time():
    print("\n Сценарий 2: Отчёт по времени")
    try:
        employee_id = int(input("Ваш ID сотрудника: "))
        task_id = int(input("ID задачи: "))
        hours = float(input("Часов потрачено (например, 3.5): "))
        work_date = input("Дата работы (YYYY-MM-DD): ") or str(date.today())
        description = input("Описание: ") or "Работа по задаче"

        emp = get_employee(employee_id)
        if not emp:
            print("Сотрудник не найден")
            return

        branch_id = emp[2]
        with get_conn(BRANCH_CONNS[branch_id]) as conn:
            cur = conn.cursor()
            # Проверим, что задача существует и назначена этому сотруднику
            cur.execute("SELECT id FROM branch.tasks WHERE id = %s AND assignee_id = %s", (task_id, employee_id))
            if not cur.fetchone():
                print("Задача не найдена или не назначена вам")
                return

            cur.execute("""
                INSERT INTO branch.time_logs (task_id, employee_id, hours_spent, date_worked, description)
                VALUES (%s, %s, %s, %s, %s)
            """, (task_id, employee_id, hours, work_date, description))
            conn.commit()
            print("Время учтено!")
    except Exception as e:
        print(f"Ошибка: {e}")

def add_employee():
    print("\nСценарий 3: Добавление сотрудника (HR)")
    try:
        full_name = input("ФИО: ") or "Alexei Ivanov"
        email = input("Email (уникальный): ") or "alexei.ivanov@global.tech"
        branch_id = int(input("Филиал (1=Moscow, 2=Berlin): ") or 2)
        role_id = int(input("Роль (1=developer, 2=manager, 3=analyst): ") or 1)
        skill_id = int(input("Навык (1=Python, 2=SQL, 3=Docker...): ") or 1)
        proficiency = input("Уровень (beginner/intermediate/expert): ") or "intermediate"

        with get_conn(GLOBAL_CONN) as conn:
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO global.employees (full_name, email, role_id, branch_id, hired_at, is_active)
                VALUES (%s, %s, %s, %s, %s, true)
                RETURNING id
            """, (full_name, email, role_id, branch_id, date.today()))
            emp_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO global.employee_skills (employee_id, skill_id, proficiency_level)
                VALUES (%s, %s, %s)
                ON CONFLICT (employee_id, skill_id) 
                DO UPDATE SET proficiency_level = EXCLUDED.proficiency_level
            """, (emp_id, skill_id, proficiency))

            cur.execute("SELECT id FROM global.departments WHERE branch_id = %s LIMIT 1", (branch_id,))
            dept = cur.fetchone()
            if dept:
                cur.execute("""
                    INSERT INTO global.employee_departments (employee_id, department_id, is_head)
                    VALUES (%s, %s, false)
                    ON CONFLICT DO NOTHING
                """, (emp_id, dept[0]))

            conn.commit()
            print(f"Сотрудник {full_name} (ID={emp_id}) добавлен в филиал {branch_id}!")
    except Exception as e:
        print(f"Ошибка: {e}")

        
def view_my_tasks():
    print("\nСценарий 4: Мои задачи")
    try:
        emp_id = int(input("Ваш ID: "))
        emp = get_employee(emp_id)
        if not emp:
            print("Сотрудник не найден")
            return

        branch_id = emp[2]
        with get_conn(BRANCH_CONNS[branch_id]) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT t.id, t.title, t.status, t.due_date, p.name AS project
                FROM branch.tasks t
                JOIN branch.projects p ON t.project_id = p.id
                WHERE t.assignee_id = %s
                ORDER BY t.created_at DESC
            """, (emp_id,))
            tasks = cur.fetchall()

        if not tasks:
            print("У вас нет задач")
            return

        print(f"\nЗадачи сотрудника {emp[1]} (филиал {branch_id}):")
        for t in tasks:
            print(f"  • [{t[2]}] {t[1]} (до {t[3]}) — проект: {t[4]} (ID={t[0]})")
    except Exception as e:
        print(f"Ошибка: {e}")

def delete_task():
    print("\nСценарий 5: Удаление задачи")
    try:
        task_id = int(input("ID задачи для удаления: "))
        # Найдём филиал по задаче (нужно сначала определить, где она хранится)
        found = False
        for bid, cfg in BRANCH_CONNS.items():
            with get_conn(cfg) as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM branch.tasks WHERE id = %s", (task_id,))
                if cur.fetchone():
                    # Удаляем связанные данные
                    cur.execute("DELETE FROM branch.comments WHERE task_id = %s", (task_id,))
                    cur.execute("DELETE FROM branch.time_logs WHERE task_id = %s", (task_id,))
                    cur.execute("DELETE FROM branch.attachments WHERE task_id = %s", (task_id,))
                    cur.execute("DELETE FROM branch.notifications WHERE related_task_id = %s", (task_id,))
                    cur.execute("DELETE FROM branch.tasks WHERE id = %s", (task_id,))
                    conn.commit()
                    print(f"Задача {task_id} удалена из филиала {bid}")
                    found = True
                    break
        if not found:
            print("Задача не найдена")
    except Exception as e:
        print(f"Ошибка: {e}")

def main():
    while True:
        print("\n" + "="*50)
        print("РАСПРЕДЕЛЁННАЯ СИСТЕМА УПРАВЛЕНИЯ ЗАДАЧАМИ")
        print("="*50)
        print("1. Создать задачу (Сценарий 1)")
        print("2. Отчитаться по времени (Сценарий 2)")
        print("3. Добавить сотрудника (Сценарий 3)")
        print("4. Посмотреть свои задачи (Сценарий 4)")
        print("5. Удалить задачу (Сценарий 5)")
        print("0. Выход")
        choice = input("\nВыберите действие: ").strip()

        if choice == "1":
            create_task()
        elif choice == "2":
            log_time()
        elif choice == "3":
            add_employee()
        elif choice == "4":
            view_my_tasks()
        elif choice == "5":
            delete_task()
        elif choice == "0":
            print("До свидания!")
            break
        else:
            print("Неверный выбор")

if __name__ == "__main__":
    main()