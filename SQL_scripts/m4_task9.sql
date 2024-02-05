WITH CTE1 AS (
  SELECT
    TO_TIMESTAMP(CONCAT(day, ' ', month, ' ', year, ' ', timestamp), 'DD MM YYYY HH24:MI:SS') AS event_time
  FROM
    dim_date_times
),

CTE2 AS (
  SELECT
    event_time,
    LEAD(event_time) OVER (ORDER BY event_time) AS next_event_time
  FROM
    CTE1
),

CTE3 AS (
  SELECT
    EXTRACT(YEAR FROM event_time) AS year,
    EXTRACT(EPOCH FROM (next_event_time - event_time)) AS time_difference
  FROM
    CTE2
)

SELECT
  year,
  MAKE_INTERVAL(secs := AVG(time_difference)) AS actual_time_taken
FROM
  CTE3
GROUP BY
  year
ORDER BY
  actual_time_taken DESC
  LIMIT 5;


