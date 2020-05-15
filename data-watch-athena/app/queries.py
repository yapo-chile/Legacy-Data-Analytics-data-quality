"""
List of queries to be used in datawatch, this file will be uploaded to
s3 yapo-data bucket.
"""
QUERIES ={
    "Test": """select date(dt_metric) as dt_day,
		event_type || ' - ' || event_name || ' - ' || product_type as variation,
		SUM(metric_value) as value
    from yapocl_databox.insights_events_behavioral_counts_365d 
    where event_name = 'Delivery Attempted' 
    and cast(date_parse(cast(year as varchar) || '-' ||
        cast(month as varchar) || '-' || cast(day as varchar),
        '%Y-%c-%e') as date) = date '2020-05-13'
    group by 1,2;""",

    }
