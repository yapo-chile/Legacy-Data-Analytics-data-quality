"""
List of queries to be used in datawatch, this file will be uploaded to
s3 yapo-data bucket.
"""
QUERIES ={
    "NAA": """select
        (now()::date - 1)::varchar as dt_day,
        'NAA' as variation,
        count(*) as value
        from ods.ad
        where approval_date::date = now()::date - 1
        and ad.platform_id_fk in (1,2,7,8)
        group by approval_date::date""",
    "NIA": """select
        (now()::date - 1)::varchar as dt_day,
        'NIA' as variation,
        count(*) as value
        from ods.ad
        where creation_date::date = now()::date - 1
        and ad.platform_id_fk in (1,2,7,8)
        group by creation_date::date""",
    "DAU": """
            select
            (now()::date - 1)::varchar as dt_day,
            'DAU' as variation,
            SUM(visitors)::int as value
            from
                    ods.fact_day_xiti
            where fecha::date = now()::date - 1
            and site_name in ('Desktop v2',
                                                    'Mobile v2',
                                                    'NGA Android App',
                                                    'NGA Ios App')
            group by fecha::date""",
    "VISITS": """select
        (now()::date - 1)::varchar as dt_day,
        'VISITS' as variation,
        SUM(visits)::int as value
        from ods.fact_day_xiti
        where fecha::date = now()::date - 1
        and site_name in ('Desktop v2',
                                                'Mobile v2',
                                                'NGA Android App',
                                                'NGA Ios App')
        group by fecha::date""",
    "LEADS": """
        select
        (now()::date - 1)::varchar as dt_day,
        'LEADS' as variation,
        sum(leads)*0.79 + sum(leads)*0.21 as value
        from ods.leads_daily
        where fecha::date = now()::date - 1
        and site in ('Desktop v2',
                    'Mobile v2',
                    'NGA Android App',
                    'NGA Ios App')
        group by fecha::date""",
    "EVENTS_LEADS": """select
        event_date::date as dt_day,
        event_type || ' - ' || event_name as variation,
        SUM(amount_events) as value
        from dm_pulse.monitoring_events
        where event_name in ('Ad phone number displayed', 'Ad phone number called', 'Ad reply submitted')
        and event_date::date = now()::date - 1
        group by 1,2""",
    "EVENTS_ADS": """select
        event_date::varchar dt_day,
        event_type || ' - ' || event_name as variation,
        sum(amount_events) as value
        from dm_pulse.monitoring_events
        where
        event_name in ( 'Ad insertion confirmed',
        	'Ad detail viewed',
        	'Ad insertion error',
        	'Ad insertion form')
        and event_date::date = now()::date - 1
        group by 1,2""",
    "EVENTS_LISTING": """ select
        event_date::varchar dt_day,
        event_type || ' - ' || event_name as variation,
        sum(amount_events) as value
        from dm_pulse.monitoring_events
        where
        event_name in ( 'Listing viewed')
        and event_date::date = now()::date - 1
        group by 1,2""",

    }
