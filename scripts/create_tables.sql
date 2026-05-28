-- ============================================================
-- Schemas
-- ============================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================
-- STAGING
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.vacancies_raw (
    vacancy_id  TEXT       NOT NULL,
    source      TEXT       NOT NULL DEFAULT 'source_error',
    raw_json    JSONB      NOT NULL,
    loaded_at   TIMESTAMP  NOT NULL DEFAULT NOW(),
    UNIQUE (vacancy_id, source)
) DISTRIBUTED BY (vacancy_id);

-- ============================================================
-- CORE
-- ============================================================

-- Справочник компаний
CREATE TABLE IF NOT EXISTS core.companies (
    company_id  BIGSERIAL,
    name        VARCHAR(200)      NOT NULL,
    industry    VARCHAR(200),
    created_at  TIMESTAMP  NOT NULL DEFAULT NOW(),
    UNIQUE (company_id, name)  -- включает company_id для Greenplum
) DISTRIBUTED BY (company_id);

-- Справочник городов
CREATE TABLE IF NOT EXISTS core.cities (
    city_id    BIGSERIAL,
    city_name  VARCHAR(40)        NOT NULL,
    region     VARCHAR(100),
    UNIQUE (city_id, city_name)  -- включает city_id для Greenplum
) DISTRIBUTED BY (city_id);

-- Справочник навыков
CREATE TABLE IF NOT EXISTS core.skills (
    skill_id    BIGSERIAL,
    skill_name  VARCHAR(100)      NOT NULL,
    UNIQUE (skill_id, skill_name)  -- включает skill_id для Greenplum
) DISTRIBUTED BY (skill_id);

-- Основная таблица вакансий
CREATE TABLE IF NOT EXISTS core.vacancies (
    vacancy_id        BIGINT        NOT NULL,
    source            TEXT          NOT NULL DEFAULT 'source_error',
    title             TEXT          NOT NULL,
    company_id        BIGINT,
    city_id           BIGINT,
    salary_from       NUMERIC(12,2),
    salary_to         NUMERIC(12,2),
    salary_currency   VARCHAR(6),
    salary_gross      BOOLEAN       DEFAULT FALSE,
    salary_from_rub   NUMERIC(12,2),
    salary_to_rub     NUMERIC(12,2),
    experience_level  VARCHAR(100),
    employment_type   VARCHAR(100),
    schedule          TEXT,
    remote_possible   BOOLEAN       DEFAULT FALSE,
    published_at      TIMESTAMP,
    is_active         BOOLEAN       DEFAULT TRUE,
    created_at        TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP     NOT NULL DEFAULT NOW(),
    UNIQUE (vacancy_id, source)
) DISTRIBUTED BY (vacancy_id);

-- Таблица связки вакансий и навыков
CREATE TABLE IF NOT EXISTS core.vacancy_skills (
    vacancy_id  BIGINT  NOT NULL,
    skill_id    BIGINT  NOT NULL,
    UNIQUE (vacancy_id, skill_id)
) DISTRIBUTED BY (vacancy_id);

-- ============================================================
-- MARTS
-- ============================================================

-- Для витрин с DISTRIBUTED RANDOMLY убираем UNIQUE
-- Защита от дубликатов — через DELETE перед INSERT

CREATE TABLE IF NOT EXISTS marts.skills_stats (
    stat_date        DATE           NOT NULL,
    skill_name       VARCHAR(100)   NOT NULL,
    city_name        VARCHAR(40)    NOT NULL DEFAULT 'All Cities',
    vacancy_count    INTEGER        NOT NULL DEFAULT 0,
    avg_salary_from  NUMERIC(12,2),
    avg_salary_to    NUMERIC(12,2),
    median_salary    NUMERIC(12,2),
    min_salary       NUMERIC(12,2),
    max_salary       NUMERIC(12,2),
    junior_count     INTEGER       DEFAULT 0,
    middle_count     INTEGER       DEFAULT 0,
    senior_count     INTEGER       DEFAULT 0,
    remote_count     INTEGER       DEFAULT 0,
    office_count     INTEGER       DEFAULT 0
) DISTRIBUTED RANDOMLY
  PARTITION BY RANGE (stat_date)
  (START (DATE '2024-01-01') END (DATE '2027-01-01') EVERY (INTERVAL '1 month'));

CREATE TABLE IF NOT EXISTS marts.company_stats (
    stat_date         DATE          NOT NULL,
    company_name      VARCHAR(200)  NOT NULL,
    active_vacancies  INTEGER       DEFAULT 0,
    avg_salary        NUMERIC(12,2),
    max_salary        NUMERIC(12,2),
    cities_count      INTEGER       DEFAULT 0
) DISTRIBUTED RANDOMLY;

CREATE TABLE IF NOT EXISTS marts.market_dynamics (
    stat_date         DATE          NOT NULL,
    city_name         VARCHAR(40)   NOT NULL DEFAULT 'All Cities',
    total_vacancies   INTEGER  DEFAULT 0,
    active_companies  INTEGER  DEFAULT 0,
    avg_salary        NUMERIC(12,2),
    median_salary     NUMERIC(12,2),
    remote_vacancies  INTEGER  DEFAULT 0,
    office_vacancies  INTEGER  DEFAULT 0,
    junior_vacancies  INTEGER  DEFAULT 0,
    middle_vacancies  INTEGER  DEFAULT 0,
    senior_vacancies  INTEGER  DEFAULT 0
) DISTRIBUTED RANDOMLY;

-- ============================================================
-- Permissions
-- ============================================================

GRANT USAGE ON SCHEMA staging TO gpadmin;
GRANT USAGE ON SCHEMA core    TO gpadmin;
GRANT USAGE ON SCHEMA marts   TO gpadmin;

GRANT ALL ON ALL TABLES    IN SCHEMA staging TO gpadmin;
GRANT ALL ON ALL TABLES    IN SCHEMA core    TO gpadmin;
GRANT ALL ON ALL TABLES    IN SCHEMA marts   TO gpadmin;

GRANT ALL ON ALL SEQUENCES IN SCHEMA core    TO gpadmin;

SELECT 'Tables created successfully!' AS status;