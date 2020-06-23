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
                                   data_dwh: pd.DataFrame) -> None:
        query = Query(config, params)
        DB_WRITE = Database(conf=config.DWConf)
        DB_WRITE.execute_command(query.delete_base_output_eval())
        DB_WRITE.insert_data_output_eval(data_dwh)
        DB_WRITE.close_connection()
