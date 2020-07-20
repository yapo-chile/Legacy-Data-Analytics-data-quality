from infraestructure.conf import getConf
from utils.read_params import ReadParams


class Query:
    """
    Class that store all querys
    """
    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def query_base_postgresql(self) -> str:
        """
        Method return str with query
        """
        query = """
                select cast((now() - interval '1 day')::date as varchar)
                    as timedate,
	            version()  as current_version;
            """
        return query

    def query_base_athena(self) -> str:
        """
        Method return str with query
        """
        query = """
                select substr(
                        cast((cast(now() as timestamp) - interval '1' day)
                    as varchar), 1, 10) as timedate,
                'Athena' as current_version
            """
        return query

    def query_athena_ts_ad_phone_number_called(self) -> str:
        """
        Method return str with query
        """
        query = """
                WITH
                DATASET AS (
	                SELECT 
                        ARRAY[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30] AS items
                ),
                DATES_RANGE_ARRAY AS (
	                SELECT 
		                array_agg(date_add('day', -i, DATE('{0}'))) AS array_items
	                FROM DATASET
	                CROSS JOIN UNNEST(items) AS t(i)
                ),
                DATES_RANGE_COLUMN AS (
	                SELECT 
		                day_range
	                FROM DATES_RANGE_ARRAY
	                CROSS JOIN UNNEST(array_items) AS k(day_range)
	                ORDER BY day_range
                )

                -- Q registros
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity,
                        'q_registros'||'-'||product_type as entity_var,
                        count(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Listing viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,3
                    
                    UNION all
                    
                -- Q event type 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        case
                        	when event_type = 'Call' then 'q_event_type_call'||'-'||product_type
                        	when event_type = 'Show' then 'q_event_type_show'||'-'||product_type
                        	when event_type = 'Send' then 'q_event_type_Send'||'-'||product_type
                            when event_type = 'View' then 'q_event_type_view'||'-'||product_type
                            when event_type = 'Create' then 'q_event_type_create'||'-'||product_type
                            when event_type is null then 'q_event_type_null'||'-'||product_type
                            else 'q_event_type_others'||'-'||product_type
                        end entity_var,
                        sum(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Listing viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,
                        case
                        	when event_type = 'Call' then 'q_event_type_call'||'-'||product_type
                        	when event_type = 'Show' then 'q_event_type_show'||'-'||product_type
                        	when event_type = 'Send' then 'q_event_type_Send'||'-'||product_type
                            when event_type = 'View' then 'q_event_type_view'||'-'||product_type
                            when event_type = 'Create' then 'q_event_type_create'||'-'||product_type
                            when event_type is null then 'q_event_type_null'||'-'||product_type
                            else 'q_event_type_others'||'-'||product_type
                        end
                        
                    UNION all
                    
                -- Q object type 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        case
                        	when object_type = 'PhoneContact' then 'q_object_type_phoneContact'||'-'||product_type
                        	when object_type = 'Message' then 'q_object_type_message'||'-'||product_type
                        	when object_type = 'ClassifiedAd' then 'q_object_type_classifiedAd'||'-'||product_type
                            when object_type = 'Listing' then 'q_object_type_listing'||'-'||product_type
                            when object_type = 'Confirmation' then 'q_object_type_confirmation'||'-'||product_type
                            when object_type is null then 'q_object_type_null'||'-'||product_type
                            else 'q_object_type_others'||'-'||product_type
                        end entity_var,
                        sum(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Listing viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,
                            case
                            	when object_type = 'PhoneContact' then 'q_object_type_phoneContact'||'-'||product_type
                            	when object_type = 'Message' then 'q_object_type_message'||'-'||product_type
                            	when object_type = 'ClassifiedAd' then 'q_object_type_classifiedAd'||'-'||product_type
                                when object_type = 'Listing' then 'q_object_type_listing'||'-'||product_type
                                when object_type = 'Confirmation' then 'q_object_type_confirmation'||'-'||product_type
                                when object_type is null then 'q_object_type_null'||'-'||product_type
                                else 'q_object_type_others'||'-'||product_type
                            end
                            
                    UNION all
                    
                 -- Q ad id 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        'q_ad_id'||'-'||product_type entity_var,
                        count(distinct ad_id ) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,3
                    
                    UNION ALL    
                    
                -- Q local main category 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        case
                            when local_main_category = 'vehiculos' or local_main_category = 'vehículos' then 'q_main_category_vehiculos'||'-'||product_type
                            when local_main_category = 'inmuebles' then 'q_main_category_inmuebles'||'-'||product_type
                            when local_main_category is null then 'q_main_category_null'||'-'||product_type
                            else 'q_object_type_others'||'-'||product_type
                        end entity_var,
                        sum(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Listing viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,
                        case
                            when local_main_category = 'vehiculos' or local_main_category = 'vehículos' then 'q_main_category_vehiculos'||'-'||product_type
                            when local_main_category = 'inmuebles' then 'q_main_category_inmuebles'||'-'||product_type
                            when local_main_category is null then 'q_main_category_null'||'-'||product_type
                            else 'q_object_type_others'||'-'||product_type
                        end
                        
                    UNION all
                    
                -- Q local category level 1 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        case
                            when local_category_level1 = 'comprar' then 'q_category_level1_comprar'||'-'||product_type
                            when local_category_level1 = 'arrendar' then 'q_category_level1_arrendar'||'-'||product_type
                            when local_category_level1 = 'arriendo de temporada' then 'q_category_level1_arriendo_temporada'||'-'||product_type
                            when local_category_level1 = 'autos, camionetas y 4x4' or local_category_level1 = 'autos camionetas y 4x4' then 'q_category_level1_autos_camionetas_4x4'||'-'||product_type
                            when local_category_level1 = 'motos' then 'q_category_level1_motos'||'-'||product_type
                            when local_category_level1 = 'camiones y furgones' then 'q_category_level1_camiones_furgones'||'-'||product_type
                            when local_category_level1 = 'barcos lanchas y aviones' then 'q_category_level1_barcos_lanchas_aviones'||'-'||product_type
                            when local_category_level1 = 'otros vehiculos' then 'q_category_level1_otros_vehiculos'||'-'||product_type
                            when local_category_level1 is null then 'q_category_level1_null'||'-'||product_type
                            else 'q_category_level1_others'||'-'||product_type
                        end entity_var,
                        sum(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Listing viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,
                        case
                            when local_category_level1 = 'comprar' then 'q_category_level1_comprar'||'-'||product_type
                            when local_category_level1 = 'arrendar' then 'q_category_level1_arrendar'||'-'||product_type
                            when local_category_level1 = 'arriendo de temporada' then 'q_category_level1_arriendo_temporada'||'-'||product_type
                            when local_category_level1 = 'autos, camionetas y 4x4' or local_category_level1 = 'autos camionetas y 4x4' then 'q_category_level1_autos_camionetas_4x4'||'-'||product_type
                            when local_category_level1 = 'motos' then 'q_category_level1_motos'||'-'||product_type
                            when local_category_level1 = 'camiones y furgones' then 'q_category_level1_camiones_furgones'||'-'||product_type
                            when local_category_level1 = 'barcos lanchas y aviones' then 'q_category_level1_barcos_lanchas_aviones'||'-'||product_type
                            when local_category_level1 = 'otros vehiculos' then 'q_category_level1_otros_vehiculos'||'-'||product_type
                            when local_category_level1 is null then 'q_category_level1_null'||'-'||product_type
                            else 'q_category_level1_others'||'-'||product_type
                        end
                        
                    UNION all
                    
                -- Q local vertical 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        case
                            when local_vertical = 'real estate' then 'q_local_vertical_real_estate'||'-'||product_type
                            when local_vertical = 'motor' then 'q_local_vertical_motor'||'-'||product_type
                            when local_vertical is null then 'q_local_vertical_null'||'-'||product_type
                            else 'q_local_vertical_others'||'-'||product_type
                        end entity_var,
                        sum(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Listing viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,
                        case
                            when local_vertical = 'real estate' then 'q_local_vertical_real_estate'||'-'||product_type
                            when local_vertical = 'motor' then 'q_local_vertical_motor'||'-'||product_type
                            when local_vertical is null then 'q_local_vertical_null'||'-'||product_type
                            else 'q_local_vertical_others'||'-'||product_type
                        end
                        
                    UNION all
                    
                -- Q local ad type 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        case
                            when local_ad_type = 'sell' then 'q_local_ad_type_sell'||'-'||product_type
                            when local_ad_type = 'rent' then 'q_local_ad_type_rent'||'-'||product_type
                            when local_ad_type = 'buy' then 'q_local_ad_type_buy'||'-'||product_type
                            when local_ad_type is null then 'q_local_ad_type_null'||'-'||product_type
                            else 'q_local_ad_type_others'||'-'||product_type
                        end entity_var,
                        sum(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted', 'Ad detail viewed', 'Listing viewed', 'Ad insertion confirmed')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,
                        case
                            when local_ad_type = 'sell' then 'q_local_ad_type_sell'||'-'||product_type
                            when local_ad_type = 'rent' then 'q_local_ad_type_rent'||'-'||product_type
                            when local_ad_type = 'buy' then 'q_local_ad_type_buy'||'-'||product_type
                            when local_ad_type is null then 'q_local_ad_type_null'||'-'||product_type
                            else 'q_local_ad_type_others'||'-'||product_type
                        end
                        
                   UNION all
                   
                -- Q lead type 
                    SELECT
                        DATE(SUBSTR(event_date, 1,10)) as fecha,
                        event_name as entity, 
                        case
                            when lead_type = 'Ad phone number called' then 'q_lead_type_Ad_phone_number_called'||'-'||product_type
                            when lead_type = 'Ad phone number displayed' then 'q_lead_type_Ad_phone_number_displayed'||'-'||product_type
                            when lead_type = 'Ad reply submited' then 'q_lead_type_Ad_reply_submited'||'-'||product_type
                            when lead_type is null then 'q_lead_type_null'||'-'||product_type
                            else 'q_lead_type_others'||'-'||product_type
                        end entity_var,
                        sum(1) as mtr_var
                    FROM yapocl_databox.insights_events_behavioral_fact_layer_365d
                    WHERE 
                        DATE(SUBSTR(event_date, 1,10)) BETWEEN (SELECT min(day_range) as date_from FROM DATES_RANGE_COLUMN) AND (SELECT max(day_range) as date_to FROM DATES_RANGE_COLUMN)
                        and event_name in ('Ad phone number called', 'Ad phone number displayed', 'Ad reply submitted')
                        and client_id = 'yapocl'
                    GROUP BY 1,2,
                        case
                            when lead_type = 'Ad phone number called' then 'q_lead_type_Ad_phone_number_called'||'-'||product_type
                            when lead_type = 'Ad phone number displayed' then 'q_lead_type_Ad_phone_number_displayed'||'-'||product_type
                            when lead_type = 'Ad reply submited' then 'q_lead_type_Ad_reply_submited'||'-'||product_type
                            when lead_type is null then 'q_lead_type_null'||'-'||product_type
                            else 'q_lead_type_others'||'-'||product_type
                        end
                        
                   ORDER BY 1,2,3
                """.format(self.params.get_date_to())
        return query

    def query_insert_output_dw(self) -> str:
        """
        Method return str with query
        """
        query = """
                INSERT INTO dm_analysis.temp_time_series_data_quality
                            (timedate,
                             entity,
                             entity_var,
                             w_rule_1,
                             w_rule_2,
                             w_rule_3,
                             w_rule_4,
                             eval_rank)
                VALUES %s;"""
        return query

    def delete_base_output_eval(self) -> str:
        """
        Method that returns events of the day
        """
        command = """
                    delete from dm_analysis.temp_time_series_data_quality where 
                    timedate::date = 
                    '""" + self.params.get_date_from() + """'::date """

        return command
