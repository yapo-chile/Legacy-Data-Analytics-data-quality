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
    "NAA_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'NAA' || ' - ' || platform as variation,
	        SUM(new_ads) as value
        from 
	        dm_peak.content
        where
            vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "NAA_PRI_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'NAA_PRI' || ' - ' || platform as variation,
	        SUM(naa_pri) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "NAA_PRO_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'NAA_PRO' || ' - ' || platform as variation,
	        SUM(naa_pro) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "SELLERS_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'SELLERS' || ' - ' || platform as variation,
	        SUM(sellers) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "SELLERS_PRI_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'SELLERS_PRI' || ' - ' || platform as variation,
	        SUM(sellers_pri) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "SELLERS_PRO_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'SELLERS_PRO' || ' - ' || platform as variation,
	        SUM(sellers_pro) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "NIA_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'NIA' || ' - ' || platform as variation,
	        SUM(new_inserted_ads) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "NIA_PRI_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'NIA_PRI' || ' - ' || platform as variation,
	        SUM(nia_pri) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "NIA_PRO_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'NIA_PRO' || ' - ' || platform as variation,
	        SUM(nia_pro) as value
        from 
	        dm_peak.content
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "DAU_XITI_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'DAU_XITI' || ' - ' || platform as variation,
	        SUM(dau_xiti) as value
        from 
	        dm_peak.traffic
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "LEADS_XITI_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'LEADS_XITI' || ' - ' || platform as variation,
	        SUM(leads_xiti) as value
        from 
	        dm_peak.traffic
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "LEADS_XITI_BY_TYPE_AGG" : """
		select	
			fecha as dt_day,
			lower(click_name_1) as variation,
			count(click_name_1) as value
		from 
			ods.fact_day_leads_geolocation_xiti 
		where 
			((site = 557229 and lower(click_name_1) in ('call_mobile','sms_mobile')) -- only call and sms ios leads
			or (site = 557231 and lower(click_name_1) in ('sent_email_confirm','call_mobile','sms_mobile')) -- only android leads
			or (site = 535162 and lower(click_name_1) = 'sent_email_confirm')) -- only email desktop lead
			and fecha = now()::date - 1
			group by fecha, lower(click_name_1)""",
    "LEADS_XITI_BY_TYPE_DETAIL": """
        select	
            fecha as dt_day,
            case
                when site = 557231 then concat(lower(click_name_1), ' - Yapo.cl NGA Android app')
                when site = 535162 then concat(lower(click_name_1), ' - Yapo.cl v2')
                when site = 557229 then concat(lower(click_name_1), ' - Yapo.cl NGA IOS app')

            end as variation,
            count(click_name_1) as value
        from 
            ods.fact_day_leads_geolocation_xiti 
        where 
            ((site = 557229 and lower(click_name_1) in ('call_mobile','sms_mobile')) -- only call and sms ios leads
            or (site = 557231 and lower(click_name_1) in ('sent_email_confirm','call_mobile','sms_mobile')) -- only android leads
            or (site = 535162 and lower(click_name_1) = 'sent_email_confirm')) -- only email desktop lead
            and fecha = now()::date - 1
            group by fecha, variation""",
    "DAU_PULSE_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'DAU_PULSE' || ' - ' || platform as variation,
	        SUM(dau_pulse) as value
        from 
	        dm_peak.traffic
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "BROWSER_PULSE_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'BROWSER_PULSE' || ' - ' || platform as variation,
	        SUM(browsers_pulse) as value
        from 
	        dm_peak.traffic
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "BUYERS_PULSE_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'BUYERS_PULSE' || ' - ' || platform as variation,
	        SUM(buyers_pulse) as value
        from 
	        dm_peak.traffic
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "UNIQUE_LEADS_PULSE_BY_PLATFORM": """
        select
	        timedate::varchar as dt_day,
	        'UNIQUE_LEADS_PULSE' || ' - ' || platform as variation,
	        SUM(unique_leads_pulse) as value
        from 
	        dm_peak.traffic
        where vertical not in ('All Yapo')
	        and timedate::date = now()::date - 1
        group by 1,2""",
    "REVENUES_PP_BY_VERTICAL": """
        select
	        date_id::varchar as dt_day,
	        'REVENUES_PP' || ' - ' || vertical as variation,
	        SUM(amount) as value
        from 
	        dm_peak.revenues
        where
	        date_id::date = now()::date - 1
	        and revenue_type in ('Premium Products')
        group by 1,2""",
    "REVENUES_PACKS_BY_VERTICAL": """
        select
	        date_id::varchar as dt_day,
	        'REVENUES_PACKS' || ' - ' || vertical as variation,
	        SUM(amount) as value
        from 
	        dm_peak.revenues
        where
	        date_id::date = now()::date - 1
	        and revenue_type in ('Packs')
        group by 1,2""",
    "REVENUES_IF_BY_VERTICAL": """
        select
	        date_id::varchar as dt_day,
	        'REVENUES_IF' || ' - ' || vertical as variation,
	        SUM(amount) as value
        from 
	        dm_peak.revenues
        where
	        date_id::date = now()::date - 1
	        and revenue_type in ('Insertion Fee')
        group by 1,2""",
    "EVENTS_LEADS": """select
        event_date::date as dt_day,
        event_type || ' - ' || event_name as variation,
        SUM(amount_events) as value
        from (
        	select distinct *
        	from dm_pulse.monitoring_events
        	where
        	event_name in ('Ad phone number displayed',
            'Ad phone number called',
            'Ad reply submitted',
            'View AdReplyForm',
            'Ad phone whatsapp number contacted',
            'Ad View Similar Ads Interaction Button')
            and event_type not in ('Click')
        	and event_date::date = now()::date - 1) a
        group by 1,2""",
    "EVENTS_ADS": """select
        event_date::varchar dt_day,
        event_type || ' - ' || event_name as variation,
        sum(amount_events) as value
        from (
        	select distinct *
        	from dm_pulse.monitoring_events
        	where
        	event_name in ('Ad insertion confirmed',
            'Ad detail viewed',
            'Ad insertion error',
            'Ad insertion form',
            'Ad saved',
            'Ad unsaved',
            'Ad published',
            'Ad deleted') 
        	and event_date::date = now()::date - 1) a
        group by 1,2""",
    "EVENTS_MESSAGING_CENTER": """select
        event_date::varchar dt_day,
        event_type || ' - ' || event_name as variation,
        sum(amount_events) as value
        from (
        	select distinct *
        	from dm_pulse.monitoring_events
        	where
        	event_name in ('Ad sms app opened',
            'Bulk conversation delete',
            'Click system message link',
            'Conversation Close',
            'Conversation View',
            'Message sent',
            'User click in send button reply conversation',
            'User sends a message',
            'Block user',
            'Unblock user') 
        	and event_date::date = now()::date - 1) a
        group by 1,2""",
    "EVENTS_RECOMMENDATIONS": """select
        event_date::varchar dt_day,
        event_type || ' - ' || event_name as variation,
        sum(amount_events) as value
        from (
        	select distinct *
        	from dm_pulse.monitoring_events
        	where
        	event_name in ('Recommendation widget viewable impression',
            'Recommendation widget impression',
            'Recommendation widget clicked') 
        	and event_date::date = now()::date - 1) a
        group by 1,2""",
    "EVENTS_VIEWS": """select
        event_date::date as dt_day,
        event_type || ' - ' || event_name || ' - ' || object_type  as variation,
        SUM(amount_events) as value
        from (
        	select distinct *
        	from dm_pulse.monitoring_events
        	where
        	event_name in ('Inbox View', 'Page viewed', 'Inbox view') 
        	and event_date::date = now()::date - 1) a
        group by 1,2""",
    "EVENTS_LISTING": """select
        event_date::varchar dt_day,
        event_type || ' - ' || event_name as variation,
        sum(amount_events) as value
        from (
        	select distinct *
        	from dm_pulse.monitoring_events
        	where
        	event_name in ('Listing viewed', 'Listing unsaved', 'Listing saved') 
        	and event_date::date = now()::date - 1) a
        group by 1,2""",

    }
