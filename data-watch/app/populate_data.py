#!/usr/bin/env python3
import logging
import sys
import os

from datetime import date, timedelta
import psycopg2 as pg
import argparse
import infraestructure.common as cm
import utils.queries as q
from configobj import ConfigObj
from urllib.parse import urlparse

target_day = date.today() - timedelta(1)
logger = logging.getLogger('populate_data')
logger.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_params():
    parser = argparse.ArgumentParser(
        description="Populate history table with KPIs values",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--show-kpis',
                        dest='show_kpis', default=False,
                        action='store_true',
                        help="Print available KPIs and then exit.")
    parser.add_argument('--ignored-kpis',
                        dest='ignored_kpis',
                        nargs='+', help="Exclude these KPIs from population.")
    parser.add_argument('--selected-kpis',
                        dest='selected_kpis',
                        nargs='+', help="Populate only these KPIs.")
    parser.add_argument('--date',
                        dest='day', default=None,
                        type=cm.valid_date,
                        help="The day for which (re)calculate all KPIs. Default: yesterday")
    parser.add_argument('--from-date',
                        dest='from_day', default=None,
                        type=cm.valid_date,
                        help="Start of (re)calculation period.")
    parser.add_argument('--to-date',
                        dest='to_day', default=None,
                        type=cm.valid_date,
                        help="End of (re)calculation period.")
    options = parser.parse_args()
    if options.ignored_kpis and options.selected_kpis:
        raise argparse.ArgumentTypeError("You cannot specify both ignored and selected KPIs")
    return options


def populate_kpi_day(kpi, query, day, source, history_source):
    conn = pg.connect(**params_from_uri(source))
    conn_write = pg.connect(**params_from_uri(history_source))
    with conn, conn_write:
        with conn.cursor() as curs, conn_write.cursor() as curs_write:
            curs.execute(query, vars=dict(day=day))
            results = curs.fetchall()
            curs_write.execute(q.delete_query, vars=dict(report_day=day, kpi=kpi))
            params = [dict(dt_day=r[0], kpi=kpi, variation=r[1], value=r[2], report_day=day)
                      for r in results]
            logger.debug(params)
            curs_write.executemany(q.insert_query, params)
    conn.close()
    conn_write.close()


def get_queries(cfg):
    queries = {}
    for source_name in cfg.sections:
        for kpi in cfg[source_name].sections:
            queries[kpi] = {'query': cfg[source_name][kpi]['query'],
                            'source': cfg[source_name]['connection_string'],
                            'source_name': source_name}
    return queries


def populate_day(day, cfg, selected_kpis=None, ignored_kpis=None):
    logger.info("Calculating KPIs for day %s", day.strftime('%Y-%m-%d'))
    queries = get_queries(cfg)
    history_source = cfg['history_datasource']
    if selected_kpis:
        kpis_to_populate = selected_kpis
    elif ignored_kpis:
        kpis_to_populate = [k for k in queries.keys() if k not in ignored_kpis]
    else:
        kpis_to_populate = list(queries.keys())

    for kpi in kpis_to_populate:
        logger.info("Calculating %s...", kpi)
        query = queries.get(kpi, {}).get('query')
        source = queries.get(kpi, {}).get('source')
        if query and source:
            try:
                populate_kpi_day(kpi, query, day, source, history_source)
            except Exception as exc:
                raise Exception("%s: %s" % (query, str(exc)))


def populate_period(from_date, to_date, cfg, selected_kpis=None, ignored_kpis=None):
    logger.info(
        "Calculating KPIs for period from %s to %s inclusive" %
        (from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d')))
    days = [from_date + timedelta(days=x) for x in range((to_date - from_date).days + 1)]
    for day in days:
        populate_day(day, cfg, selected_kpis, ignored_kpis)


def show_kpis(cfg):
    q = get_queries(cfg)
    sorted_q = sorted(list(q.keys()))

    print("\nAvailable KPIs:\n")
    for kpi in sorted_q:
        print("%s (%s)" % (kpi, q[kpi]['source_name']))
    sys.exit(0)


def config_is_valid(cfg):
    default_values = ["email_sender", "email_recipients", "default_tolerance",
                      "default_seasonality", "default_history_length", "history_datasource"]
    for default_value in default_values:
        if default_value not in cfg:
            logger.error("No value present for %s" % default_value)
            return False
    return True


def params_from_uri(uri):
    pu = urlparse(uri)
    return dict(
        host=pu.hostname,
        user=pu.username,
        database=pu.path.strip(' /'),
        port=pu.port,
        password=pu.password
    )


def get_config():
    conf_dir = os.path.dirname(os.path.realpath(__file__))
    cfg = ConfigObj(os.environ.get('DATA_WATCH_CONF'), interpolation=False)
    assert config_is_valid(cfg)
    return cfg


if __name__ == '__main__':
    opts = get_params()
    config = get_config()

    if opts.show_kpis:
        show_kpis(config)

    try:
        if opts.day:
            populate_day(opts.day, config, opts.selected_kpis, opts.ignored_kpis)
        elif opts.from_day or opts.to_day:
            if not (opts.from_day and opts.to_day) or (opts.from_day > opts.to_day):
                raise Exception("Missing/wrong either from-date or to-date arguments")
            populate_period(opts.from_day, opts.to_day, config, opts.selected_kpis,
                            opts.ignored_kpis)
        else:
            yesterday = date.today() - timedelta(1)
            populate_day(yesterday, config, opts.selected_kpis, opts.ignored_kpis)
    except Exception as e:
        cm.send_mail(
            send_from=config['email_sender'],
            send_to=config['email_recipients'],
            subject="DataWatch Populate just died.",
            text=str(e)
        )
        raise e
