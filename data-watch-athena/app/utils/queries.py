create_history_table_q = """
CREATE TABLE ods.datawatch_athena_history (
  dt_day DATE NOT NULL,
  kpi VARCHAR(256) NOT NULL,
  variation VARCHAR(256) NOT NULL,
  value INT NOT NULL,
  anomaly BOOLEAN NOT NULL DEFAULT FALSE,
  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  report_day DATE NOT NULL DEFAULT (CURRENT_TIMESTAMP - INTERVAL '1 day')::DATE
)"""

delete_query = """
DELETE FROM ods.datawatch_athena_history
WHERE
    report_day = %(report_day)s
    AND kpi = %(kpi)s
"""

insert_query = """
INSERT INTO ods.datawatch_athena_history
(dt_day, kpi, variation, value, report_day)
VALUES (%(dt_day)s, %(kpi)s, %(variation)s, %(value)s, %(report_day)s)
"""

get_kpi_values_ignored_q = """
SELECT
    dt_day,
    report_day,
    kpi,
    kpi||'-'||variation as kpi_variation,
    value,
    CASE WHEN dt_day = %(day)s THEN FALSE ELSE anomaly END AS anomaly
FROM ods.datawatch_athena_history
WHERE
    report_day >= %(day)s - INTERVAL '28 days'
    AND report_day <= %(day)s
    AND kpi NOT IN %(ignored)s
ORDER BY 1,2,3
"""
get_kpi_values_selected_q = """
SELECT
    dt_day,
    report_day,
    kpi,
    kpi||'-'||variation as kpi_variation,
    value,
    CASE WHEN dt_day = %(day)s THEN FALSE ELSE anomaly END AS anomaly
FROM ods.datawatch_athena_history
WHERE
    report_day >= %(day)s - INTERVAL '28 days'
    AND report_day <= %(day)s
    AND kpi IN %(selected)s
ORDER BY 1,2,3
"""


