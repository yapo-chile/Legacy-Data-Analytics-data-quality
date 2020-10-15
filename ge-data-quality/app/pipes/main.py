# pylint: disable=no-member
# utf-8
import sys
import logging
import datetime
from exp_suite.suite_metrics_time_series_average import SuiteMetricsTimeSeriesAverages
from infraestructure.conf import set_conf
from utils.time_execution import TimeExecution

if __name__ == '__main__':
    LOGGER = logging.getLogger('time_series_data_quality')
    TIME = TimeExecution()
    DATE_FORMAT = """%(asctime)s,%(msecs)d %(levelname)-2s """
    INFO_FORMAT = """[%(filename)s:%(lineno)d] %(message)s"""
    LOG_FORMAT = DATE_FORMAT + INFO_FORMAT
    set_conf()
    SuiteMetricsTimeSeriesAverages().execute_suite()
    TIME.get_time()
    LOGGER.info('Process ended successfully.')
