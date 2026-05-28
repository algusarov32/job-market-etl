CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Тестовая таблица для проверки подключения
CREATE TABLE IF NOT EXISTS staging.test_table (
    id SERIAL,
    message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED RANDOMLY;

-- Вставляем тестовую запись
INSERT INTO staging.test_table (message) 
VALUES ('Greenplum is running!');

-- Предоставляем права
GRANT ALL ON SCHEMA staging TO gpadmin;
GRANT ALL ON SCHEMA analytics TO gpadmin;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO gpadmin;

-- Логируем успешную инициализацию
SELECT 'Database initialized successfully!' AS status;