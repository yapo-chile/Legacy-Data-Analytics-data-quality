#!/usr/bin/env python
from collections import defaultdict
import pandas as pd
import infraestructure.common as cm
import argparse
import matplotlib
from utils.queries import get_kpi_values_ignored_q, get_kpi_values_selected_q
from sklearn import linear_model
from sklearn.preprocessing import OneHotEncoder
from datetime import timedelta, date
import tempfile
import os
import logging
import sys
from populate_data import get_config, show_kpis, get_queries

logger = logging.getLogger('datawatch')
logger.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stderr)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def too_far_from_expected(real, expected, tolerance):
    if expected < 0:
        expected = 0
    if expected == 0 and real == 0:
        return False
    try:
        ratio1 = abs(expected - real) / expected
    except ZeroDivisionError:
        ratio1 = 0
    try:
        ratio2 = abs(expected - real) / real
    except ZeroDivisionError:
        ratio2 = 0

    if ratio1 > tolerance or ratio2 > tolerance:
        return True


def find_anomalies(kpi_data, kpi_configs, ignored_kpi_variations=()):
    # we specify parameters per kpi, so we need a way to get the kpi for each variation:
    orig_kpi_map = {t[0]: t[1] for i, t in kpi_data[['kpi_variation', 'kpi']].drop_duplicates().iterrows()}
    hp = kpi_data.pivot(values='value', columns='kpi_variation', index='report_day').fillna(0)
    # old anomalies, we exclude them from the training
    past_anomalies = kpi_data.pivot(values='anomaly', columns='kpi_variation',
                                    index='report_day').fillna(False)
    # zeroes never preceded by not-zero values must be ignored
    allzeroes = hp.cumsum() == 0
    days_to_consider = ~past_anomalies & ~allzeroes
    anomalies = {}

    for kpi_variation in hp.columns:
        if kpi_variation in ignored_kpi_variations:
            logger.debug("kpi_variation %s is ignored.", kpi_variation)
            continue
        logger.debug("analyzing %s kpi_variation...", kpi_variation)
        orig_kpi = orig_kpi_map[kpi_variation]
        kpi_config = kpi_configs[orig_kpi]
        kpi_data = hp[[kpi_variation]][days_to_consider[kpi_variation]].rename(columns={kpi_variation: 'y'}).copy()
        try:
            y_truth = kpi_data['y'].tail(1).values[0]
        except IndexError:
            logger.warning("No significant data present for kpi_variation %s." % kpi_variation)
            continue
        try:
            y_hat = linear_model_check(kpi_data, kpi_config)
            if too_far_from_expected(y_truth, y_hat, kpi_config['tolerance']):
                report = hp[[kpi_variation]].copy()
                expected = report.tail(2).copy()
                expected.at[report.tail(1).index, kpi_variation] = y_hat
                report.at[expected.index, '%s_expected' % kpi_variation] = \
                    expected.values.transpose()[0]
                anomalies[kpi_variation] = report
        except NotEnoughData:
            logger.warning("Not enough data to predict %s" % kpi_variation)
    return anomalies


class NotEnoughData(Exception):
    pass


def linear_model_check(kpi_data, kpi_config):
    ds = kpi_data.tail(kpi_config['history_length'] + 1).copy()
    # ds = hp[[kpi_variation]][days_to_consider[kpi_variation]].tail(kpi_config['history_length'] + 1)
    if len(ds) < 3:
        raise NotEnoughData("Days present: %s" % len(ds))
    # days of week is used to build the seasonality
    ds['season'] = (ds.index - ds.index.min()).days % kpi_config['seasonality']
    ds['progressive'] = (ds.index - ds.index.min()).days
    ohe = OneHotEncoder(n_values=[kpi_config['seasonality']], categorical_features=[0])
    x = ohe.fit_transform(ds.head(-1)[['season', 'progressive']].values)
    x_hat = ohe.transform(ds.tail(1)[['season', 'progressive']].values)
    y = ds['y'].head(-1)
    # y_truth = ds[kpi_variation].tail(1).values[0]
    m = linear_model.LinearRegression()
    m.fit(x, y)
    y_hat = m.predict(x_hat)[0]
    if y_hat < 0:
        y_hat = 0
    return y_hat


def send_alert_email(anomalies, recipients, sender, stale_data=None, print_only=False):
    email_text = """These KPIs fluctuated too much:"""
    graphs = []
    for kpi_variation, data in anomalies.items():
        if data is not None:
            email_text += "\n- %s" % kpi_variation
            graph_filename = os.path.join(tempfile.gettempdir(), "%s.png" % kpi_variation)
            data.plot().get_figure().savefig(graph_filename)
            graphs.append(graph_filename)
        else:
            # anomaly because there's not enough data
            # (no graph necessary)
            email_text += "\n- %s (not enough data)" % kpi_variation

    subject = compose_alert_subject(list(anomalies.keys()), stale_data)

    cm.send_mail(
        send_from=sender,
        send_to=recipients,
        subject=subject,
        text=email_text,
        files=graphs,
        print_only=print_only
    )


def compose_alert_subject(anomalies_labels, stale_data):
    default_expansion = 3
    stretch = 2
    if len(anomalies_labels) > default_expansion + stretch:
        split = default_expansion
    else:
        split = default_expansion + stretch
    expanded = anomalies_labels[:split]
    remainder = anomalies_labels[split:]
    subject = 'The %s KPI%s %sfluctuated too much' % (
        ', '.join(expanded),
        's' if len(expanded) > 1 else '',
        '' if not remainder else 'and %s others ' % len(remainder)
    )
    if stale_data:
        subject += ' (stale data)'
    return subject


def send_ok_email(recipients, sender, kpis, stale_data=None, print_only=False):
    if len(kpis) > 3:
        subject = "All KPIs (%s) look OK" % len(kpis)
    else:
        singular = len(kpis) == 1
        subject = "%s KPI%s look%s OK" % (', '.join(kpis), '' if singular else 's', 's' if singular else '')
    if stale_data:
        subject += ' (stale data)'
    body = """KPIs checked:\n%s""" % '\n'.join(kpis)
    cm.send_mail(
        send_from=sender,
        send_to=recipients,
        subject=subject,
        text=body,
        print_only=print_only
    )


def get_options():
    parser = argparse.ArgumentParser(
        description="Checks if some KPIs fluctuated too much.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--day',
                        dest='set_day',
                        default=date.today() - timedelta(1),
                        type=cm.valid_date,
                        help='Day to check (default: yesterday)')
    parser.add_argument('--ignored-kpis',
                        dest='ignored_kpis',
                        nargs='+', help="Check all KPIs but these.")
    parser.add_argument('--recipients',
                        dest='recipients',
                        nargs='+', help="Send email to these addresses")
    parser.add_argument('--selected-kpis',
                        dest='selected_kpis',
                        nargs='+', help="Check only these KPIs.")
    parser.add_argument('--show-kpis',
                        dest='show_kpis', default=False,
                        action='store_true',
                        help="Print available KPIs and then exit.")
    parser.add_argument('--alerts-from',
                        dest='alerts_from',
                        default=None,
                        help='Count alerts in a period',
                        type=cm.valid_date)
    parser.add_argument('--alerts-to',
                        dest='alerts_to',
                        default=None,
                        help='Count alerts in a period',
                        type=cm.valid_date)
    parser.add_argument('--test-run',
                        dest='test_run', default=False,
                        action='store_true',
                        help='Print report email without sending it')
    opts = parser.parse_args()
    if opts.ignored_kpis and opts.selected_kpis:
        raise argparse.ArgumentTypeError("You cannot specify both ignored and selected KPIs")
    return opts


def get_kpi_values(day_to_check, connection_string, kpis_to_check, history_length):
    query_params = dict(day=day_to_check,
                        history_length=history_length,
                        selected=tuple(kpis_to_check))
    return pd.read_sql(get_kpi_values_selected_q,
                       connection_string,
                       parse_dates=['dt_day', 'report_day'],
                       params=query_params)


def get_max_history_length(cfg, kpis_to_check):
    return max(int(cfg[data_source][kpi].get('history_length', cfg['default_history_length']))
               for data_source in cfg.sections
               for kpi in cfg[data_source].sections if kpi in kpis_to_check)


def get_kpi_configs(cfg, kpis_to_check):
    tolerances = {kpi: float(cfg[datasource][kpi].get('tolerance', cfg['default_tolerance']))
                  for datasource in cfg.sections
                  for kpi in cfg[datasource].sections}
    seasonalities = {kpi: int(cfg[datasource][kpi].get('seasonality', cfg['default_seasonality']))
                     for datasource in cfg.sections
                     for kpi in cfg[datasource].sections}
    history_lengths = {kpi: int(cfg[datasource][kpi].get('history_length', cfg['default_history_length']))
                       for datasource in cfg.sections
                       for kpi in cfg[datasource].sections}
    return {kpi: dict(
        tolerance=tolerances[kpi],
        seasonality=seasonalities[kpi],
        history_length=history_lengths[kpi]) for kpi in kpis_to_check}


def main(cfg):
    options = get_options()

    if options.show_kpis:
        show_kpis(cfg)

    all_kpis = list(get_queries(cfg).keys())
    if options.selected_kpis:
        kpis_to_check = list(set(options.selected_kpis).intersection(set(all_kpis)))
    elif options.ignored_kpis:
        kpis_to_check = list(set(all_kpis).difference(set(options.ignored_kpis)))
    else:
        kpis_to_check = all_kpis

    kpi_configs = get_kpi_configs(cfg, kpis_to_check)

    if options.alerts_from:
        alerts_history(options.alerts_from, options.alerts_to, kpi_configs,
                       cfg['history_datasource'], cfg['unstable_kpis'])

    kpi_values = get_kpi_values(options.set_day,
                                cfg['history_datasource'],
                                kpis_to_check,
                                get_max_history_length(cfg, kpis_to_check))

    kpi_values.to_csv('data.csv', index = False)
    anomalies = find_anomalies(
        kpi_values,
        kpi_configs,
        ignored_kpi_variations=cfg['unstable_kpis']
    )

    recipents = options.recipients or cfg['email_recipients']

    # stale data needs to be thought over
    stale_data = False
    if anomalies:
        send_alert_email(
            anomalies,
            recipents,
            cfg['email_sender'],
            stale_data=stale_data,
            print_only=options.test_run)
    else:
        send_ok_email(recipents,
                      sender=cfg['email_sender'],
                      kpis=kpis_to_check,
                      stale_data=stale_data,
                      print_only=options.test_run)


def day_anomalies(day, kpi_configs, history_datasource):
    kpi_values = get_kpi_values(day,
                                history_datasource,
                                kpis_to_check=kpi_configs.keys(),
                                history_length=max(kpi_configs[kpi]['history_length'] for kpi in kpi_configs))
    return find_anomalies(
        kpi_values,
        kpi_configs)


def alerts_history(day_from, day_to, kpi_configs, history_datasource, ignored_unstable):
    print("SIMULATE ALERTS FROM %s TO %s for %s kpis" % (day_from, day_to, len(kpi_configs)))
    alert_counter = defaultdict(int)
    for day in pd.date_range(day_from, day_to):
        alerts = day_anomalies(day, kpi_configs, history_datasource)
        for kpi in alerts:
            alert_counter[kpi] += 1
    sorted_alerts = sorted(alert_counter.items(), key=lambda x: x[1], reverse=True)
    for kpi_alerted, times in sorted_alerts:
        if kpi_alerted not in ignored_unstable:
            print("Kpi-variation %s alert triggered %s times." % (kpi_alerted, times))
    for kpi_alerted, times in sorted_alerts:
        if kpi_alerted in ignored_unstable:
            print("Kpi-variation %s alert triggered %s times (but silenced)." % (kpi_alerted, times))
    sys.exit(0)


if __name__ == '__main__':
    config = get_config()
    try:
        matplotlib.use('agg')
        main(config)
    except Exception as e:
        print(e)
        print("Sending email")
        cm.send_mail(
            send_from=config['email_sender'],
            send_to=config['email_recipients'],
            subject="DataWatch just died.",
            text=str(e)
        )
        raise e
