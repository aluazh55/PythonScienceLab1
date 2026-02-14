-- 1. Таблица транзакций
CREATE TABLE IF NOT EXISTS transactions (
    id              SERIAL PRIMARY KEY,
    tx_date         DATE NOT NULL,
    account_number  VARCHAR(20) NOT NULL,
    operation_type  VARCHAR(10) NOT NULL CHECK (operation_type IN ('приход', 'расход')),
    amount          NUMERIC(15, 2) NOT NULL
);

-- 2. Генерация минимум 300 записей (можно больше)
DO $$
DECLARE
    i INT;
    acc VARCHAR(20);
    types TEXT[] := ARRAY['приход', 'расход'];
BEGIN
    FOR i IN 1..350 LOOP
        acc := '40817810' || lpad((1000 + (i % 50))::text, 6, '0');  -- разные счета

        INSERT INTO transactions (tx_date, account_number, operation_type, amount)
        VALUES (
            CURRENT_DATE - (random() * 365)::int,
            acc,
            types[CASE WHEN random() < 0.5 THEN 1 ELSE 2 END],
            (random() * 100000 + 100)::numeric(15,2)
        );
    END LOOP;
END $$;

-- 3. Функция daily_report
CREATE OR REPLACE FUNCTION daily_report(
    p_account_number VARCHAR,
    p_start_date     DATE,
    p_end_date       DATE
)
RETURNS TABLE (
    report_date     DATE,
    income          NUMERIC(15,2),
    expense         NUMERIC(15,2),
    balance         NUMERIC(15,2)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH daily AS (
        SELECT
            tx_date,
            SUM(CASE WHEN operation_type = 'приход' THEN amount ELSE 0 END) AS income,
            SUM(CASE WHEN operation_type = 'расход' THEN amount ELSE 0 END) AS expense
        FROM transactions
        WHERE account_number = p_account_number
          AND tx_date BETWEEN p_start_date AND p_end_date
        GROUP BY tx_date
    ),
    running AS (
        SELECT
            report_date,
            income,
            expense,
            SUM(income - expense) OVER (ORDER BY report_date) AS balance
        FROM daily
    )
    SELECT * FROM running
    ORDER BY report_date;
END;
$$;

-- 4. Функция monthly_report
CREATE OR REPLACE FUNCTION monthly_report(
    p_account_number VARCHAR,
    p_start_date     DATE,
    p_end_date       DATE
)
RETURNS TABLE (
    report_month    DATE,           -- первое число месяца
    income          NUMERIC(15,2),
    expense         NUMERIC(15,2),
    balance         NUMERIC(15,2)
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH monthly AS (
        SELECT
            DATE_TRUNC('month', tx_date)::DATE AS month_start,
            SUM(CASE WHEN operation_type = 'приход' THEN amount ELSE 0 END) AS income,
            SUM(CASE WHEN operation_type = 'расход' THEN amount ELSE 0 END) AS expense
        FROM transactions
        WHERE account_number = p_account_number
          AND tx_date BETWEEN p_start_date AND p_end_date
        GROUP BY month_start
    ),
    running AS (
        SELECT
            month_start AS report_month,
            income,
            expense,
            SUM(income - expense) OVER (ORDER BY month_start) AS balance
        FROM monthly
    )
    SELECT * FROM running
    ORDER BY report_month;
END;
$$;