# Job Market ETL

Сквозной ETL-пайплайн для анализа рынка вакансий: сбор данных из нескольких источников, трансформация, загрузка в Greenplum DWH, построение витрин и визуализация в Grafana через ClickHouse.

## Архитектура:

┌─────────────┐ ┌─────────────┐ 
│ TheirStack  │ │   hh.ru     │ 
│ API         │ │(hh_data.xlsx│ 
└──────┬──────┘ └──────┬──────┘ 
       │               │               
       ▼               ▼               
┌─────────────────────────────────────────────────┐
│                   MinIO (S3)                    │
│                 raw data lake                   │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│                 Greenplum (DWH)                 │
│     staging.vacancies_raw → core.* → marts.*    │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│                ClickHouse (OLAP)                │
│                   facts.*                       │
└─────────────────────┬───────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────┐
│             Grafana (Dashboards)                │
└─────────────────────────────────────────────────┘

## Стек технологий

|    Компонент    |           Технология               |
|-----------------|------------------------------------|
| Оркестрация     | Apache Airflow 2.8                 |
| Data Lake       | MinIO (S3-совместимый)             |
| DWH             | Greenplum (PostgreSQL-совместимый) |
| OLAP            | ClickHouse                         |
| Визуализация    | Grafana                            |
| Контейнеризация | Docker, Docker Compose             |

## Структура проекта

job-market-etl/
├── airflow/                           # Airflow DAG'и, логи, плагины
│ └── dags/                            # DAG-файлы
│ ├── theirstack_extraction_dag.py     # Выгрузка из TheirStack API
│ ├── synthetic_hh_dag.py              # Загрузка синтетических данных hh.ru (hh ограничил доступ к вакансиям через api :c )
│ ├── gp_transform_dag.py              # Трансформация staging → core
│ ├── gp_build_marts_dag.py            # Построение витрин
│ ├── load_clickhouse_dag.py           # Загрузка в ClickHouse
│ └── trigger_transform_sensor.py      # Сенсор для координации DAG'ов
├── src/
│ ├── extractors/                      # Экстракторы данных из API 
│ │ ├── theirstack_extractor.py
│ │ └── base_extractor.py
│ ├── loaders/                         # Загрузчики данных
│ │ ├── base_loader.py
│ │ ├── s3_loader.py
│ │ ├── gp_loader.py
│ │ └── ch_loader.py
│ ├── transformers/                    # Трансформация данных
│ │ └── excel_to_hh.py
│ └── utils/                           # Вспомогательные модули
│ ├── config.py
│ ├── gp_connection.py
│ └── ch_connection.py
├── scripts/                           # SQL-скрипты
│ ├── create_tables.sql                # DDL таблиц
│ ├── gp_transform_vacancies_raw.sql   # Трансформация staging → core
│ ├── gp_rebuild_marts.sql             # Перестроение витрин
│ └── init_clickhouse.sql              # Инициализация ClickHouse
├── Dockerfile.airflow                 # Кастомный образ Airflow
├── docker-compose.yml                 # Конфигурация сервисов
├── .env                               # Переменные окружения
└── generate_synthetic_hh.py           # Генератор синтетических данных


## Быстрый старт

### 1. Настройка окружения

```bash
# Клонировать репозиторий
git clone https://github.com/algusarov32/job-market-etl/
cd job-market-etl

# Создать .env файл с переменными окружения
touch .env
# Отредактировать .env — указать свои ключи API и пароли


2. Переменные окружения (.env)

# ------------------------------------------------------------
# MinIO
# ------------------------------------------------------------
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_password
MINIO_ENDPOINT_URL=http://minio:your_port
MINIO_BUCKET_RAW=raw-data
MINIO_REGION=us-east-1
# Ports (host-side)
MINIO_API_PORT=your_port
MINIO_CONSOLE_PORT=your_port

# ------------------------------------------------------------
# Greenplum
# ------------------------------------------------------------
GP_HOST=your_host
GP_PORT=your_port
GP_DATABASE=vacancies_db
GP_USER=gpadmin
GP_PASSWORD=your_password

# ------------------------------------------------------------
# ClickHouse
# ------------------------------------------------------------
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_NATIVE_PORT=your_port
CLICKHOUSE_HTTP_PORT=your_port
CLICKHOUSE_HOST_PORT=your_port
CLICKHOUSE_DATABASE=facts
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password

# ------------------------------------------------------------
# Airflow metadata Postgres
# ------------------------------------------------------------
AIRFLOW_POSTGRES_USER=afadmin
AIRFLOW_POSTGRES_PASSWORD=your_password
AIRFLOW_POSTGRES_DB=airflow
AIRFLOW_POSTGRES_PORT=your_port

# ------------------------------------------------------------
# Airflow itself
# ------------------------------------------------------------
AIRFLOW_FERNET_KEY=your_key
AIRFLOW_ADMIN_USER=afadmin
AIRFLOW_ADMIN_PASSWORD=your_password
AIRFLOW_WEBSERVER_PORT=your_port

# ------------------------------------------------------------
# Grafana
# ------------------------------------------------------------
GRAFANA_ADMIN_USER=gadmin
GRAFANA_ADMIN_PASSWORD=your_password
GRAFANA_PORT=your_port

# ------------------------------------------------------------
# TheirStack API
# ------------------------------------------------------------
THEIRSTACK_API_KEY=your_key
THEIRSTACK_API_BASE_URL=https://api.theirstack.com/v1
THEIRSTACK_API_RATE_LIMIT_DELAY=1.0
THEIRSTACK_API_MAX_PER_PAGE=500

3. Сборка и запуск

# Забилдить и запустить все сервисы
docker compose up -d --build

# Проверить статус
docker compose ps
Все сервисы должны быть в статусе healthy:
  - airflow-webserver 
  - airflow-scheduler
  - minio 
  - clickhouse 
  - grafana 

4. Инициализация Greenplum
Я ставил отдельно от контейнера на bare metal

# Подключиться к Greenplum
psql -h localhost -U gpadmin -d vacancies_db

# Выполнить DDL
\i scripts/create_tables.sql

5.S3 (Minio)
Необходимо создать bucket raw-data (я делал через UI), в нем подпапки theirstack и HH

6. Запуск DAG'ов
Откройте Airflow UI

theirstack_extraction_dag — запустить вручную
synthetic_hh_dag — запустить вручную
gp_transform_dag — запустится автоматически через сенсор после выполнения theirstack_extraction_dag, если theirstack_extraction_dag и synthetic_hh_dag завершились успешно с ds > now() - timedelta(minutes=5)
gp_build_marts_dag — запустится после трансформации
load_clickhouse_dag — запустится после витрин

7. Grafana
Data Source: ClickHouse (уже настроен через GF_INSTALL_PLUGINS)


# DAG'и и их зависимости
text
theirstack_extraction_dag (02:00) ──┐
                                     ├── trigger_transform_sensor ──┐
synthetic_hh_dag (manual) ──────────┘                               │
                                                                     ▼
                                                          gp_transform_dag
                                                                     │
                                                                     ▼
                                                          gp_build_marts_dag
                                                                     │
                                                                     ▼
                                                          load_clickhouse_dag
Параллельные DAG'и
theirstack_extraction_dag и synthetic_hh_dag работают параллельно — демонстрирует оркестрацию из нескольких источников. После завершения обоих срабатывает сенсор и запускает трансформацию.

Остановка
docker compose down

Для полной очистки (включая тома):
docker compose down -v

