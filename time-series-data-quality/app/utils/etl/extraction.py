from infraestructure.athena import Athena
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.query import Query
from utils.read_params import ReadParams

class Extraction:

    def __init__(self,
                 conf: getConf,
                 params: ReadParams) -> None:
        self.params = params
        self.conf = conf

    def get_data_last_day(self):
        query = Query(self.conf, self.params)
        db_source = Database(conf=self.conf.DWConf)
        data_last_day = db_source.select_to_dict(
            query.query_data_last_day())
        db_source.close_connection()
        return data_last_day

    # Query data from Pulse bucket
    def get_source_data_pulse(self):
        athena = Athena(conf=self.conf.athenaConf)
        query = Query(self.conf, self.params)
        data_athena = athena.get_data(query.query_base_pulse())
        athena.close_connection()
        return data_athena

    # Query data from data warehouse
    def get_source_data_dwh(self):
        query = Query(self.conf, self.params)
        db_source = Database(conf=self.conf.DWConf)
        data_dwh = db_source.select_to_dict(query.query_base_postgresql_dw())
        db_source.close_connection()
        return data_dwh

    # Query data from blocket DB
    def get_source_data_blocket(self):
        query = Query(self.conf, self.params)
        db_source = Database(conf=self.conf.blocketConf)
        data_blocket = db_source.select_to_dict( \
            query.query_base_postgresql_blocket())
        db_source.close_connection()
        return data_blocket

    # Query data from Pulse bucket
    def get_data_pulse_ts_ad_phone_number_called(self):
        athena = Athena(conf=self.conf.athenaConf)
        query = Query(self.conf, self.params)
        data_athena = athena.get_data(query.query_athena_ts_ad_phone_number_called())
        athena.close_connection()
        return data_athena
