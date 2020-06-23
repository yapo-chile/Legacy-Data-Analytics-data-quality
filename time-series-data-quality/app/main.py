# pylint: disable=no-member
# utf-8
import sys
import logging
from infraestructure.conf import getConf
from utils.read_params import ReadParams
from utils.time_execution import TimeExecution
from utils.statistical import Statistical

if __name__ == '__main__':
    CONFIG = getConf()
    TIME = TimeExecution()
    LOGGER = logging.getLogger('time_series_data_quality')
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    PARAMS = ReadParams(sys.argv)
    # Calling generate statistical evaluation
    Statistical(CONFIG, PARAMS).generate()
    # End process
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
