import psycopg2
from psycopg2 import sql

# === CONNECTION CONFIG ===
GLOBAL_DB = {
    "host": "192.168.64.4",
    "port": 5432,
    "dbname": "global_tech",
    "user": "global_user",
    "password": "123"
}

BRANCH_DBS = [
    {
        "host": "192.168.64.4",
        "port": 5433,
        "dbname": "global_tech",
        "user": "branch1_user",
        "password": "123",
        "name": "Branch 1"
    },
    {
        "host": "192.168.64.4",
        "port": 5434,
        "dbname": "global_tech",
        "user": "branch2_user",
        "password": "123",
        "name": "Branch 2"
    }
]

# === GLOBAL TABLES (schema: global) ===
GLOBAL_DDL = """
CREATE SCHEMA IF NOT EXISTS global;

CREATE TABLE IF NOT EXISTS global.employees (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    role_id INTEGER NOT NULL,
    branch_id INTEGER NOT NULL,
    hired_at DATE,
    is_active BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS global.roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS global.branches (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    location VARCHAR(255),
    timezone VARCHAR(50) DEFAULT 'UTC'
);

CREATE TABLE IF NOT EXISTS global.task_templates (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    estimated_hours INTEGER,
    priority_level VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS global.skills (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS global.employee_skills (
    employee_id INTEGER NOT NULL,
    skill_id INTEGER NOT NULL,
    proficiency_level VARCHAR(20),
    PRIMARY KEY (employee_id, skill_id)
);

CREATE TABLE IF NOT EXISTS global.departments (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    branch_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS global.employee_departments (
    employee_id INTEGER NOT NULL,
    department_id INTEGER NOT NULL,
    is_head BOOLEAN DEFAULT false,
    PRIMARY KEY (employee_id, department_id)
);

CREATE TABLE IF NOT EXISTS global.holidays (
    id SERIAL PRIMARY KEY,
    branch_id INTEGER,
    name VARCHAR(100) NOT NULL,
    date_from DATE NOT NULL,
    date_to DATE NOT NULL
);
"""

# === BRANCH TABLES (schema: branch) ===
BRANCH_DDL = """
CREATE SCHEMA IF NOT EXISTS branch;

CREATE TABLE IF NOT EXISTS branch.projects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    manager_id INTEGER NOT NULL,
    start_date DATE,
    end_date DATE,
    status VARCHAR(20) DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS branch.tasks (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    assignee_id INTEGER NOT NULL,
    reporter_id INTEGER,
    template_id INTEGER,
    project_id INTEGER NOT NULL,
    status VARCHAR(20) DEFAULT 'todo',
    priority VARCHAR(20),
    due_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS branch.comments (
    id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS branch.attachments (
    id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(512) NOT NULL,
    uploaded_by INTEGER NOT NULL,
    uploaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS branch.time_logs (
    id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    hours_spent DECIMAL(5,2) NOT NULL,
    date_worked DATE NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS branch.notifications (
    id SERIAL PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    message TEXT NOT NULL,
    is_read BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    related_task_id INTEGER
);
"""

def execute_sql(conn_params, sql_statements, label):
    try:
        print(f"Connecting to {label} at {conn_params['host']}...")
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(sql_statements)
        cur.close()
        conn.close()
        print(f"Schema initialized successfully on {label}")
    except Exception as e:
        print(f" Failed to initialize {label}: {e}")

if __name__ == "__main__":
    execute_sql(GLOBAL_DB, GLOBAL_DDL, "Global DB (vm1)")

    for branch in BRANCH_DBS:
        config = {
            "host": branch["host"],
            "port": branch["port"],
            "dbname": branch["dbname"],
            "user": branch["user"],
            "password": branch["password"]
        }
        execute_sql(config, BRANCH_DDL, branch["name"])