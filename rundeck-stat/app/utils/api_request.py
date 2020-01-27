import logging
import pandas as pd
from rundeck.client import Rundeck
from infraestructure.conf import getConf
from utils.read_params import ReadParams

def create_dataframe() -> pd.DataFrame:
    """
    Function that create a empty panda Dataframe.
    Return Dataframe
    """
    execution_data: dict = {}
    execution_data['project_name'] = 'null'
    execution_data['status'] = 'null'
    execution_data['job_name'] = 'null'
    execution_data['date_init'] = ''
    execution_data['date_end'] = ''
    execution_data['summary'] = 'null'
    execution_data['id_excution'] = 0
    execution_data['permalink'] = 'null'
    execution_data['time_execution'] = -1

    df_main = pd.DataFrame([execution_data], columns=execution_data.keys())
    df_main = df_main.drop(df_main.index, inplace=True)
    return df_main

def get_projects(params: ReadParams, config: getConf) -> pd.DataFrame:
    """
    Function that get jobs that run in Rundeck.
    Returns a pandas Dataframe.
    """
    logger = logging.getLogger('api-request')
    date_format = """%(asctime)s,%(msecs)d %(levelname)-2s """
    info_format = """[%(filename)s:%(lineno)d] %(message)s"""
    log_format = date_format + info_format
    logging.basicConfig(format=log_format, level=logging.INFO)
    pd_data = create_dataframe()
    rd = Rundeck(config.rundeck.host, api_token=config.rundeck.token)
    projects = rd.list_projects()
    date_from: str = params.get_date_from() + 'T00:00:00Z'
    date_to: str = params.get_date_to() + 'T23:59:59Z'
    logger.info('GET Data host: %s', config.rundeck.host)
    for project in projects:
        jobs = rd.get_project_history(project['name'],
                                      max=10000,
                                      begin=date_from,
                                      end=date_to)
        for job in jobs:
            execution_data = {}
            execution = job['execution']
            execution_data['project_name'] = project['name']
            execution_data['status'] = job['status']
            execution_data['job_name'] = job['title']
            execution_data['date_init'] = job['date-started']
            execution_data['date_end'] = job['date-ended']
            execution_data['summary'] = job['summary']
            execution_data['id_excution'] = execution['id']
            execution_data['permalink'] = execution['permalink']
            execution_data['time_execution'] = \
                (job['date-ended']-job['date-started']).total_seconds()
            df_tmp = pd.DataFrame([execution_data],
                                  columns=execution_data.keys())
            pd_data = pd.concat([pd_data, df_tmp])
    return pd_data
