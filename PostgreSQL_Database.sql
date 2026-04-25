CREATE TABLE stock_prices (
    clean_date      DATE,
    month_name      VARCHAR(20),
    company         VARCHAR(10) NOT NULL,
    open_price      NUMERIC(12,4),
    high_price      NUMERIC(12,4),
    low_price       NUMERIC(12,4),
    close_price     NUMERIC(12,4),
    volume          BIGINT,
    year            INTEGER,
    month           INTEGER
);

CREATE TABLE stock_calculated AS
WITH daily AS (
    SELECT 
        *,
        LAG(close_price) OVER (PARTITION BY company ORDER BY clean_date) AS prev_close
    FROM stock_prices
)
SELECT 
    clean_date,
    month_name,
    company,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    year,
    month,

    -- Daily Return (first row = 0)
    COALESCE(
        ROUND( ((close_price - prev_close) / prev_close)::numeric , 6),
        0
    ) AS daily_return,

    -- Cumulative Return (starts at 0)
    ROUND(
        (EXP(
            SUM(
                COALESCE(LN(1 + (close_price - prev_close)::numeric / prev_close), 0)
            ) OVER (PARTITION BY company ORDER BY clean_date)
        ) - 1)::numeric , 
    6) AS cumulative_return,

    -- 20-day Rolling Volatility (only 20 NULLs per stock instead of 30)
    ROUND(
        COALESCE(
            STDDEV_SAMP(
                (close_price - prev_close)::numeric / prev_close
            ) OVER (PARTITION BY company ORDER BY clean_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
            0
        )::numeric , 
    6) AS volatility_20d,

    -- Annualized Volatility
    ROUND(
        COALESCE(
            STDDEV_SAMP(
                (close_price - prev_close)::numeric / prev_close
            ) OVER (PARTITION BY company ORDER BY clean_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
            * SQRT(252),
            0
        )::numeric , 
    6) AS annualized_volatility

FROM daily;

select * from stock_calculated;

----- Stock Scorecard
SELECT
    company,
    ROUND(MAX(cumulative_return) * 100, 2) AS total_return_pct,
    RANK() OVER (ORDER BY MAX(cumulative_return) DESC) AS return_rank,
    ROUND(AVG(annualized_volatility) * 100, 2) AS avg_volatility_pct,
	ROUND((AVG(daily_return)::numeric / NULLIF(STDDEV(daily_return)::numeric, 0)) * SQRT(252)::numeric,2) AS sharpe_ratio,
    ROUND(AVG(daily_return) * 100, 4) AS avg_daily_return_pct,
    COUNT(*) AS trading_days
FROM stock_calculated
GROUP BY company
ORDER BY return_rank;


----- Year-by-Year Return Table
SELECT
    company,
    year,
    ROUND((AVG(daily_return) * 252 * 100)::NUMERIC, 2) AS annual_return_pct
FROM stock_calculated
WHERE year BETWEEN 2016 AND 2025
GROUP BY company, year
ORDER BY company, year;

---- Monthly Seasonality
SELECT
    company,
    month_name,
    month,
    ROUND((AVG(daily_return) * 100)::NUMERIC, 4) AS avg_daily_return_pct,
    COUNT(*) AS trading_days
FROM stock_calculated
GROUP BY company, month_name, month
ORDER BY company, month;