-- ============================================================
-- gp_rebuild_marts.sql
-- Rebuilds all mart tables from core.* for today's date.
-- Safe to re-run: DELETE + INSERT ensures idempotency.
-- ============================================================

BEGIN;

-- ── Skills stats ─────────────────────────────────────────────
DELETE FROM marts.skills_stats WHERE stat_date = CURRENT_DATE;

INSERT INTO marts.skills_stats (
    stat_date, skill_name, city_name,
    vacancy_count,
    avg_salary_from, avg_salary_to, median_salary,
    min_salary, max_salary,
    junior_count, middle_count, senior_count,
    remote_count, office_count
)
SELECT
    CURRENT_DATE                                                           AS stat_date,
    s.skill_name,
    COALESCE(c.city_name, 'Unknown')                                       AS city_name,
    COUNT(*)                                                               AS vacancy_count,
    ROUND(AVG(v.salary_from_rub), 2)                                       AS avg_salary_from,
    ROUND(AVG(v.salary_to_rub),   2)                                       AS avg_salary_to,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
              ORDER BY COALESCE(v.salary_from_rub, v.salary_to_rub)
          ))::numeric(15,2), 2)                                            AS median_salary,
    MIN(v.salary_from_rub)                                                 AS min_salary,
    MAX(v.salary_to_rub)                                                   AS max_salary,
    -- TheirStack seniority values
    COUNT(*) FILTER (WHERE v.experience_level IN ('entry_level'))
                                                                           AS junior_count,
    COUNT(*) FILTER (WHERE v.experience_level IN ('mid_level'))
                                                                           AS middle_count,
    COUNT(*) FILTER (WHERE v.experience_level IN ('senior', 'manager', 'executive'))
                                                                           AS senior_count,
    COUNT(*) FILTER (WHERE v.remote_possible = TRUE)                       AS remote_count,
    COUNT(*) FILTER (WHERE v.remote_possible = FALSE)                      AS office_count
FROM core.vacancies v
JOIN core.vacancy_skills vs ON v.vacancy_id = vs.vacancy_id
JOIN core.skills          s  ON vs.skill_id  = s.skill_id
LEFT JOIN core.cities      c  ON v.city_id   = c.city_id
WHERE v.is_active = TRUE
GROUP BY s.skill_name, c.city_name;

-- ── Company stats ────────────────────────────────────────────
DELETE FROM marts.company_stats WHERE stat_date = CURRENT_DATE;

INSERT INTO marts.company_stats (
    stat_date, company_name,
    active_vacancies, avg_salary, max_salary, cities_count
)
SELECT
    CURRENT_DATE                                             AS stat_date,
    co.name                                                  AS company_name,
    COUNT(*)                                                 AS active_vacancies,
    ROUND(AVG((v.salary_from_rub + v.salary_to_rub) / 2), 2) AS avg_salary,
    MAX(v.salary_to_rub)                                      AS max_salary,
    COUNT(DISTINCT v.city_id)                                 AS cities_count
FROM core.vacancies  v
JOIN core.companies co ON v.company_id = co.company_id
WHERE v.is_active = TRUE
  AND v.published_at >= NOW() - INTERVAL '90 days'
GROUP BY co.name;

-- ── Market dynamics ──────────────────────────────────────────
DELETE FROM marts.market_dynamics WHERE stat_date = CURRENT_DATE;

INSERT INTO marts.market_dynamics (
    stat_date, city_name,
    total_vacancies, active_companies, avg_salary, median_salary,
    remote_vacancies, office_vacancies,
    junior_vacancies, middle_vacancies, senior_vacancies
)
SELECT
    CURRENT_DATE                                                    AS stat_date,
    COALESCE(c.city_name, 'All Cities')                             AS city_name,
    COUNT(*)                                                        AS total_vacancies,
    COUNT(DISTINCT v.company_id)                                    AS active_companies,
    ROUND(AVG((v.salary_from_rub + v.salary_to_rub) / 2), 2)       AS avg_salary,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
              ORDER BY COALESCE(v.salary_from_rub, v.salary_to_rub)
          ))::numeric(15,2), 2)                                     AS median_salary,
    COUNT(*) FILTER (WHERE v.remote_possible = TRUE)                AS remote_vacancies,
    COUNT(*) FILTER (WHERE v.remote_possible = FALSE)               AS office_vacancies,
    -- TheirStack seniority values
    COUNT(*) FILTER (WHERE v.experience_level IN ('entry_level'))
                                                                    AS junior_vacancies,
    COUNT(*) FILTER (WHERE v.experience_level IN ('mid_level'))
                                                                    AS middle_vacancies,
    COUNT(*) FILTER (WHERE v.experience_level IN ('senior', 'manager', 'executive'))
                                                                    AS senior_vacancies
FROM core.vacancies v
LEFT JOIN core.cities c ON v.city_id = c.city_id
WHERE v.is_active = TRUE
GROUP BY GROUPING SETS ((c.city_name), ());

COMMIT;

-- ── Summary ──────────────────────────────────────────────────
SELECT 'skills_stats'     AS mart, COUNT(*) AS rows FROM marts.skills_stats    WHERE stat_date = CURRENT_DATE
UNION ALL
SELECT 'company_stats',            COUNT(*)          FROM marts.company_stats   WHERE stat_date = CURRENT_DATE
UNION ALL
SELECT 'market_dynamics',          COUNT(*)          FROM marts.market_dynamics WHERE stat_date = CURRENT_DATE
ORDER BY mart;