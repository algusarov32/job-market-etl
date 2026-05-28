-- ============================================================
-- gp_transform_vacancies_raw.sql
-- Moves data from staging.vacancies_raw → core.*
-- Supports: theirstack, synthetic_hh
-- Idempotent: WHERE NOT EXISTS / DELETE + INSERT
-- ============================================================

BEGIN;

-- ── 1. Companies ─────────────────────────────────────────────
INSERT INTO core.companies (name, industry)
SELECT DISTINCT
    COALESCE(
        (raw_json ->> 'company'),                   -- TheirStack: "IMENDO"
        (raw_json -> 'employer' ->> 'name')         -- hh.ru:     {"employer": {"name": "Yandex"}}
    ) AS name,
    COALESCE(
        (raw_json -> 'company_object' ->> 'industry'),  -- TheirStack: {"company_object": {"industry": "IT"}}
        (raw_json -> 'employer' ->> 'industry')         -- hh.ru:     {"employer": {"industry": "IT"}}
    ) AS industry
FROM staging.vacancies_raw s
WHERE COALESCE(
          (raw_json ->> 'company'),                   -- TheirStack
          (raw_json -> 'employer' ->> 'name')         -- hh.ru
      ) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.companies c
      WHERE c.name = COALESCE(
                         (s.raw_json ->> 'company'),
                         (s.raw_json -> 'employer' ->> 'name')
                     )
  );

-- ── 2. Cities ────────────────────────────────────────────────
INSERT INTO core.cities (city_name, region)
SELECT DISTINCT
    COALESCE(
        (raw_json -> 'locations' -> 0 ->> 'city'),      -- TheirStack: {"locations": [{"city": "Berlin"}]}
        (raw_json -> 'area' ->> 'name')                 -- hh.ru:     {"area": {"name": "Москва"}}
    ) AS city_name,
    COALESCE(
        (raw_json -> 'locations' -> 0 ->> 'state'),     -- TheirStack: {"locations": [{"state": "Berlin"}]}
        (raw_json -> 'area' -> 'parent' ->> 'name')     -- hh.ru:     {"area": {"parent": {"name": "Россия"}}}
    ) AS region
FROM staging.vacancies_raw s
WHERE COALESCE(
          (raw_json -> 'locations' -> 0 ->> 'city'),     -- TheirStack
          (raw_json -> 'area' ->> 'name')                -- hh.ru
      ) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.cities ct
      WHERE ct.city_name = COALESCE(
                               (s.raw_json -> 'locations' -> 0 ->> 'city'),
                               (s.raw_json -> 'area' ->> 'name')
                           )
  );

-- ── 3. Skills ────────────────────────────────────────────────
INSERT INTO core.skills (skill_name)
SELECT DISTINCT
    skill_value AS skill_name
FROM staging.vacancies_raw s
CROSS JOIN LATERAL jsonb_array_elements_text(
    COALESCE(
        s.raw_json -> 'technology_slugs',               -- TheirStack: ["databricks", "azure"]
        s.raw_json -> 'key_skills',                     -- hh.ru:     [{"name": "Python"}, {"name": "SQL"}]
        '[]'::jsonb
    )
) AS skill_value
WHERE skill_value IS NOT NULL
  AND skill_value != ''
  AND NOT EXISTS (
      SELECT 1 FROM core.skills sk
      WHERE sk.skill_name = skill_value
  );

-- ── 4. Vacancies ─────────────────────────────────────────────
DELETE FROM core.vacancies
WHERE (vacancy_id, source) IN (
    SELECT (raw_json ->> 'id')::BIGINT, s.source
    FROM staging.vacancies_raw s
    WHERE (raw_json ->> 'id') IS NOT NULL
);

INSERT INTO core.vacancies (
    vacancy_id, source, title,
    company_id, city_id,
    salary_from, salary_to, salary_currency, salary_gross,
    salary_from_rub, salary_to_rub,
    experience_level, employment_type, schedule,
    remote_possible, published_at
)
SELECT
    (raw_json ->> 'id')::BIGINT                              AS vacancy_id,
    s.source,
    -- ============================================================
    -- title
    -- ============================================================
    COALESCE(
        (raw_json ->> 'job_title'),                     -- TheirStack: "Data & AI Engineer"
        (raw_json ->> 'name')                           -- hh.ru:     "Data Engineer"
    )                                                        AS title,
    c.company_id,
    ct.city_id,
    -- ============================================================
    -- salary_from (MONTHLY)
    -- ============================================================
    COALESCE(
        ((raw_json ->> 'min_annual_salary')::NUMERIC / 12),      -- TheirStack: annual → monthly
        ((raw_json ->> 'min_annual_salary_usd')::NUMERIC / 12),  -- TheirStack: USD annual → monthly
        (raw_json -> 'salary' ->> 'from')::NUMERIC               -- hh.ru:     already monthly
    )                                                        AS salary_from,
    -- ============================================================
    -- salary_to (MONTHLY)
    -- ============================================================
    COALESCE(
        ((raw_json ->> 'max_annual_salary')::NUMERIC / 12),      -- TheirStack: annual → monthly
        ((raw_json ->> 'max_annual_salary_usd')::NUMERIC / 12),  -- TheirStack: USD annual → monthly
        (raw_json -> 'salary' ->> 'to')::NUMERIC                 -- hh.ru:     already monthly
    )                                                        AS salary_to,
    -- ============================================================
    -- salary_currency
    -- ============================================================
    COALESCE(
        (raw_json ->> 'salary_currency'),                   -- TheirStack: "EUR"
        (raw_json -> 'salary' ->> 'currency')               -- hh.ru:     {"salary": {"currency": "RUR"}}
    )                                                        AS salary_currency,
    -- ============================================================
    -- salary_gross
    -- ============================================================
    COALESCE(
        FALSE,                                              -- TheirStack: нет поля gross
        ((raw_json -> 'salary' ->> 'gross')::BOOL)          -- hh.ru:     {"salary": {"gross": true}}
    )                                                        AS salary_gross,
    -- ============================================================
    -- salary_from_rub (MONTHLY)
    -- ============================================================
    CASE 
        WHEN COALESCE((raw_json ->> 'salary_currency'), (raw_json -> 'salary' ->> 'currency')) IN ('RUR', 'RUB')
            THEN COALESCE(
                     ((raw_json ->> 'min_annual_salary')::NUMERIC / 12),
                     (raw_json -> 'salary' ->> 'from')::NUMERIC
                 )
        WHEN COALESCE((raw_json ->> 'salary_currency'), (raw_json -> 'salary' ->> 'currency')) = 'EUR'
            THEN COALESCE(
                     ((raw_json ->> 'min_annual_salary')::NUMERIC / 12),
                     (raw_json -> 'salary' ->> 'from')::NUMERIC
                 ) * 100  -- EUR → RUB rate
        ELSE COALESCE(
                 ((raw_json ->> 'min_annual_salary_usd')::NUMERIC / 12),
                 ((raw_json ->> 'min_annual_salary')::NUMERIC / 12),
                 (raw_json -> 'salary' ->> 'from')::NUMERIC
             ) * 90  -- USD → RUB rate (default)
    END                                                      AS salary_from_rub,
    -- ============================================================
    -- salary_to_rub (MONTHLY)
    -- ============================================================
    CASE 
        WHEN COALESCE((raw_json ->> 'salary_currency'), (raw_json -> 'salary' ->> 'currency')) IN ('RUR', 'RUB')
            THEN COALESCE(
                     ((raw_json ->> 'max_annual_salary')::NUMERIC / 12),
                     (raw_json -> 'salary' ->> 'to')::NUMERIC
                 )
        WHEN COALESCE((raw_json ->> 'salary_currency'), (raw_json -> 'salary' ->> 'currency')) = 'EUR'
            THEN COALESCE(
                     ((raw_json ->> 'max_annual_salary')::NUMERIC / 12),
                     (raw_json -> 'salary' ->> 'to')::NUMERIC
                 ) * 100
        ELSE COALESCE(
                 ((raw_json ->> 'max_annual_salary_usd')::NUMERIC / 12),
                 ((raw_json ->> 'max_annual_salary')::NUMERIC / 12),
                 (raw_json -> 'salary' ->> 'to')::NUMERIC
             ) * 90
    END                                                      AS salary_to_rub,
    -- ============================================================
    -- experience_level
    -- ============================================================
    COALESCE(
        (raw_json ->> 'seniority'),                         -- TheirStack: "mid_level"
        (raw_json -> 'experience' ->> 'id')                 -- hh.ru:     {"experience": {"id": "between3And6"}}
    )                                                        AS experience_level,
    -- ============================================================
    -- employment_type  (оба TEXT)
    -- ============================================================
    COALESCE(
        (raw_json -> 'employment_statuses' ->> 0),           -- TheirStack: text (->>)
        (raw_json -> 'employment' ->> 'id')                 -- hh.ru:     text (->>)
    )                                                        AS employment_type,
    -- ============================================================
    -- schedule  (оба TEXT)
    -- ============================================================
    COALESCE(
        CASE 
            WHEN (raw_json ->> 'remote')::BOOL THEN 'remote'     -- TheirStack: "remote": true
            WHEN (raw_json ->> 'hybrid')::BOOL THEN 'hybrid'     -- TheirStack: "hybrid": true
            ELSE 'office'
        END,                                                    -- CASE уже возвращает text
        (raw_json -> 'schedule' ->> 'id')                        -- hh.ru: text (->>)
    )                                                        AS schedule,
    -- ============================================================
    -- remote_possible
    -- ============================================================
    COALESCE(
        (raw_json ->> 'remote')::BOOL,                      -- TheirStack: "remote": true/false
        (raw_json -> 'schedule' ->> 'id') = 'remote'        -- hh.ru:     schedule.id == 'remote'
    )                                                        AS remote_possible,
    -- ============================================================
    -- published_at
    -- ============================================================
    COALESCE(
        (raw_json ->> 'date_posted')::DATE::TIMESTAMP,      -- TheirStack: "2026-05-21"
        (raw_json ->> 'published_at')::TIMESTAMP            -- hh.ru:     "2024-01-15T10:00:00+0300"
    )                                                        AS published_at
FROM staging.vacancies_raw s
LEFT JOIN core.companies c
    ON c.name = COALESCE(
                   (s.raw_json ->> 'company'),               -- TheirStack
                   (s.raw_json -> 'employer' ->> 'name')     -- hh.ru
               )
LEFT JOIN core.cities ct
    ON ct.city_name = COALESCE(
                          (s.raw_json -> 'locations' -> 0 ->> 'city'),  -- TheirStack
                          (s.raw_json -> 'area' ->> 'name')             -- hh.ru
                      )
WHERE (raw_json ->> 'id') IS NOT NULL;

-- ── 5. Vacancy ↔ Skills ───────────────────────────────────────
DELETE FROM core.vacancy_skills
WHERE vacancy_id IN (
    SELECT (raw_json ->> 'id')::BIGINT
    FROM staging.vacancies_raw
    WHERE (raw_json ->> 'id') IS NOT NULL
);

INSERT INTO core.vacancy_skills (vacancy_id, skill_id)
SELECT DISTINCT
    (s.raw_json ->> 'id')::BIGINT  AS vacancy_id,
    sk.skill_id
FROM staging.vacancies_raw s
CROSS JOIN LATERAL jsonb_array_elements_text(
    COALESCE(
        s.raw_json -> 'technology_slugs',                   -- TheirStack: ["databricks", "azure"]
        s.raw_json -> 'key_skills',                         -- hh.ru:     [{"name": "Python"}, {"name": "SQL"}]
        '[]'::jsonb
    )
) AS skill_value
JOIN core.skills sk
    ON sk.skill_name = skill_value
WHERE (s.raw_json ->> 'id') IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM core.vacancy_skills vs
      WHERE vs.vacancy_id = (s.raw_json ->> 'id')::BIGINT
        AND vs.skill_id = sk.skill_id
  );

COMMIT;

-- ── Sanity check ─────────────────────────────────────────────
SELECT 'companies'      AS tbl, COUNT(*) AS rows FROM core.companies
UNION ALL
SELECT 'cities',                COUNT(*)          FROM core.cities
UNION ALL
SELECT 'skills',                COUNT(*)          FROM core.skills
UNION ALL
SELECT 'vacancies',             COUNT(*)          FROM core.vacancies
UNION ALL
SELECT 'vacancy_skills',        COUNT(*)          FROM core.vacancy_skills
ORDER BY tbl;