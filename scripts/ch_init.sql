-- ============================================================
-- ch_init.sql
-- Auto-executed when the ClickHouse container starts for the
-- first time (mounted at /docker-entrypoint-initdb.d/).
--
-- Design: pre-aggregated data from Greenplum marts.* loaded
-- as-is. Grafana aggregates on the fly.
-- ============================================================

CREATE DATABASE IF NOT EXISTS facts;

-- ── Skills stats ─────────────────────────────────────────────
-- Pre-aggregated by skill × city × date from marts.skills_stats
CREATE TABLE IF NOT EXISTS facts.skills_stats
(
    stat_date        Date,
    skill_name       LowCardinality(String),
    city_name        LowCardinality(String),
    vacancy_count    UInt32,
    avg_salary_from  Nullable(Float64),
    avg_salary_to    Nullable(Float64),
    median_salary    Nullable(Float64),
    min_salary       Nullable(Float64),
    max_salary       Nullable(Float64),
    junior_count     UInt32,
    middle_count     UInt32,
    senior_count     UInt32,
    remote_count     UInt32,
    office_count     UInt32,
    loaded_at        DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY (stat_date, skill_name, city_name)
SETTINGS index_granularity = 8192;

-- ── Company stats ────────────────────────────────────────────
-- Pre-aggregated by company × date from marts.company_stats
CREATE TABLE IF NOT EXISTS facts.company_stats
(
    stat_date         Date,
    company_name      String,
    active_vacancies  UInt32,
    avg_salary        Nullable(Float64),
    max_salary        Nullable(Float64),
    cities_count      UInt32,
    loaded_at         DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY (stat_date, company_name)
SETTINGS index_granularity = 8192;

-- ── Market dynamics ──────────────────────────────────────────
-- Pre-aggregated by city × date from marts.market_dynamics
CREATE TABLE IF NOT EXISTS facts.market_dynamics
(
    stat_date          Date,
    city_name          LowCardinality(String),
    total_vacancies    UInt32,
    active_companies   UInt32,
    avg_salary         Nullable(Float64),
    median_salary      Nullable(Float64),
    remote_vacancies   UInt32,
    office_vacancies   UInt32,
    junior_vacancies   UInt32,
    middle_vacancies   UInt32,
    senior_vacancies   UInt32,
    loaded_at          DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(stat_date)
ORDER BY (stat_date, city_name)
SETTINGS index_granularity = 8192;