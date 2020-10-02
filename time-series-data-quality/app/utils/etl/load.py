import logging
import pandas as pd
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams


class Load:

    # Write data to data warehouse
    def write_data_dwh_output_eval(self,
                                   config: getConf,
                                   params: ReadParams,
                                   data_dict: pd.DataFrame) -> None:
        query = Query(config, params)
        DB_WRITE = Database(conf=config.DWConf)
        DB_WRITE.execute_command(query.delete_base_output_eval())
        for row in data_dict.itertuples():
            data_row = [(row.timedate, row.entity, row.entity_var, row.w_rule_1,
                         row.w_rule_2, row.w_rule_3, row.w_rule_4, row.eval_rank)]
            DB_WRITE.insert_data(query.query_insert_output_dw(), data_row)
        logging.info('INSERT dm_analysis.temp_time_series_data_quality COMMIT.')
        DB_WRITE.close_connection()

    # Write data to data warehouse
    def write_data_timelines_in_dwh(self,
                                    config: getConf,
                                    params: ReadParams,
                                    data: pd.DataFrame) -> None:
        query = Query(config, params)
        db = Database(conf=config.DWConf)
        db.execute_command(query.delete_base_output_average())
        db.insert_copy('dm_analysis', 'time_series_data_quality_average', data)
        db.close_connection()
