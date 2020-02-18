create_history_table_q = """
CREATE TABLE ods.datawatch_history (
  dt_day DATE NOT NULL,
  kpi VARCHAR(256) NOT NULL,
  variation VARCHAR(256) NOT NULL,
  value INT NOT NULL,
  anomaly BOOLEAN NOT NULL DEFAULT FALSE,
  insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  report_day DATE NOT NULL DEFAULT (CURRENT_TIMESTAMP - INTERVAL '1 day')::DATE
)"""

# stale_data_q = """
# SELECT date_trunc('day', insert_time) AS insert_time,
#   count(*) AS rows
# FROM ods.datawatch_history
# GROUP BY 1
# HAVING count(*) > 70
# ORDER BY 1 DESC
# LIMIT 1
# """
delete_query = """
DELETE FROM ods.datawatch_history
WHERE
    report_day = %(report_day)s
    AND kpi = %(kpi)s
"""

insert_query = """
INSERT INTO ods.datawatch_history
(dt_day, kpi, variation, value, report_day)
VALUES (%(dt_day)s, %(kpi)s, %(variation)s, %(value)s, %(report_day)s)
"""

# get_kpis_to_check_q = """
# SELECT kpi, max(dt_day) AS last_day, max(insert_time) AS last_insert
# FROM ods.datawatch_history
# WHERE dt_day > CURRENT_TIMESTAMP - INTERVAL '15 days'
# GROUP BY 1
# """

# 'anomaly' should be always false when is the last day extracted (to re-run past tests)
# get_kpi_values_q = """
# SELECT
#     dt_day,
#     report_day,
#     kpi,
#     kpi||'-'||variation as kpi_variation,
#     value,
#     CASE WHEN dt_day = %(day)s THEN FALSE ELSE anomaly END AS anomaly
# FROM ods.datawatch_history
# WHERE
#     report_day >= %(day)s - INTERVAL '%(history_length)s days'
#     AND report_day <= %(day)s
# ORDER BY 1,2,3
# """
get_kpi_values_ignored_q = """
SELECT
    dt_day,
    report_day,
    kpi,
    kpi||'-'||variation as kpi_variation,
    value,
    CASE WHEN dt_day = %(day)s THEN FALSE ELSE anomaly END AS anomaly
FROM ods.datawatch_history
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
FROM ods.datawatch_history
WHERE
    report_day >= %(day)s - INTERVAL '28 days'
    AND report_day <= %(day)s
    AND kpi IN %(selected)s
ORDER BY 1,2,3
"""


