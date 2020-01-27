# pylint: disable=no-member
# utf-8
import sys
import logging
import pandas as pd
from infraestructure.conf import getConf
from infraestructure.psql import Database
from utils.api_request import get_projects
from utils.query import Query
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution

# Query data from Pulse bucket
def source_data_rundeck(params: ReadParams,
                        config: getConf):
    data_rundeck = get_projects(params, config)
    return data_rundeck

# Write data to data warehouse
def write_data_dwh(params: ReadParams,
                   config: getConf,
                   data_rundeck: pd.DataFrame) -> None:
    query = Query(config, params)
    DB_WRITE = Database(conf=config.db)
    DB_WRITE.execute_command(query.delete_data_rundeck())
    DB_WRITE.insert_data(data_rundeck)
    DB_WRITE.close_connection()

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('rundeck-stat')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    DATA_RUNDECK = source_data_rundeck(PARAMS, CONFIG)
    write_data_dwh(PARAMS, CONFIG, DATA_RUNDECK)
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
