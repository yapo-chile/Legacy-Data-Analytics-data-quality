from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import re
import os
import socket
from collections import defaultdict
import smtplib
from datetime import datetime
import argparse


def netcat(host, port, content):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, int(port)))
    s.sendall(content)
    s.shutdown(socket.SHUT_WR)
    all_data = b''
    while True:
        data = s.recv(4096)
        if not data:
            break
        all_data += data
    s.close()
    return all_data


br = re.compile("<br />")


def parse_search_res(res):
    header_end = False
    sep = b'\t'
    encoding = 'latin-1'
    ads = []
    for line in res.splitlines():
        if not header_end:
            if not line.startswith(b'info:'):
                header = list(h.decode(encoding) for h in line.split(sep))
                header_end = True
            continue
        ad = dict(zip(header, (v.decode(encoding) for v in line.split(sep))))
        if ad:
            ad['body'] = ' '.join(br.split(ad.get('body', '')))
            ads.append(ad)
    return ads


def ignore_exceptions(some_function):
    def wrapper(*args, **kwargs):
        try:
            some_function(*args, **kwargs)
        except Exception as e:
            print(e)
            pass

    return wrapper


def get_conf(confd='kbconfdvirtual.subito.int', port=5758):
    return netcat(confd, port,
                  b'cmd:bconf\nappl:phoenix\nhost:pippo\ncommit:1\nend\n').decode(
        'latin1').splitlines()


def init_addresses(conf):
    global REDIS_PAGES_HOST, REDIS_PAGES_PORT, REDIS_PAGES_DB, REDIS_EVENTS_HOST, \
        REDIS_EVENTS_PORT, REDIS_EVENTS_DB, SEARCH_HOST, SEARCH_PORT

    search_host_re = re.compile("^conf:search.host=(.*)")
    search_port_re = re.compile("^conf:search.port=([0-9]*)")
    redispages_host_re = re.compile("^conf:common.redispages.host.m.name=(.*)")
    redispages_port_re = re.compile("^conf:common.redispages.host.m.port=([0-9]*)")
    redispages_db_re = re.compile("^conf:common.redispages.db=([0-9]*)")
    redisevents_host_re = re.compile("^conf:common.redis.host.m.name=(.*)")
    redisevents_port_re = re.compile("^conf:common.redis.host.m.port=([0-9]*)")
    redisevents_db_re = re.compile("^conf:common.redis.db=([0-9]*)")

    for line in conf:
        m = search_host_re.match(line)
        if m:
            SEARCH_HOST = m.groups()[0]
        m = search_port_re.match(line)
        if m:
            SEARCH_PORT = int(m.groups()[0])

        m = redispages_host_re.match(line)
        if m:
            REDIS_PAGES_HOST = m.groups()[0]
        m = redispages_port_re.match(line)
        if m:
            REDIS_PAGES_PORT = int(m.groups()[0])
        m = redispages_db_re.match(line)
        if m:
            REDIS_PAGES_DB = int(m.groups()[0])

        m = redisevents_host_re.match(line)
        if m:
            REDIS_EVENTS_HOST = m.groups()[0]
        m = redisevents_port_re.match(line)
        if m:
            REDIS_EVENTS_PORT = int(m.groups()[0])
        m = redisevents_db_re.match(line)
        if m:
            REDIS_EVENTS_DB = int(m.groups()[0])


def get_double_lookup(conf, regex):
    lookup = defaultdict(lambda: defaultdict(lambda: None))
    for line in conf:
        m = regex.match(line)
        if not m:
            continue
        first_key, encoded_value, decoded_value = m.groups()
        lookup[first_key][encoded_value] = decoded_value
    return lookup


def get_lookup(conf, regex):
    lookup = defaultdict(lambda: None)
    for line in conf:
        m = regex.match(line)
        if m:
            code, label = m.groups()
            lookup[code] = label
    return lookup


param_decode = re.compile('^conf:common\.([^.]*)\.([^.]*)=(.*)')
cities_decode = re.compile('^conf:common\.region\.([0-9]*)\.cities\.([0-9]*)=(.*)')
carmodel_decode = re.compile('^conf:common\.carmodel\.[0-9]*\.([0-9]*)=(.*)')
bikemodel_decode = re.compile('^conf:common\.bikemodel\.[0-9]*\.([0-9]*)=(.*)')
category_decode = re.compile('^conf:common\.category\.([0-9]*)\.name=(.*)')
region_decode = re.compile('^conf:common\.region\.([0-9]*)\.name=(.*)')
adtype_decode = re.compile('^conf:common\.type\.list\.([a-z])=(.*)')
servicename_decode = re.compile('^conf:common\.servicename\.([0-9]*)\.([0-9]*)=(.*)')
town_decode = re.compile('^conf:common\.town\.[0-9]*\.[0-9]*\.([0-9]*)=(.*)')
townzone_decode = re.compile('^conf:common\.townzone\.([0-9]*)\.([0-9]*)=(.*)')
zone_decode = re.compile('^conf:common\.zone\.([0-9]*)\.([0-9]*)=(.*)')


def init_lookups(conf):
    global params_lookup, city_lookup, servicename_lookup
    global townzone_lookup, zone_lookup
    global integer_fields, passthrough_fields, boolean_fields
    global forced_int, special_fields

    params_lookup = get_double_lookup(conf, param_decode)
    params_lookup['category'] = get_lookup(conf, category_decode)
    params_lookup['carmodel'] = get_lookup(conf, carmodel_decode)
    params_lookup['bikemodel'] = get_lookup(conf, bikemodel_decode)
    params_lookup['region'] = get_lookup(conf, region_decode)
    params_lookup['type'] = get_lookup(conf, adtype_decode)
    params_lookup['town'] = get_lookup(conf, town_decode)

    city_lookup = get_double_lookup(conf, cities_decode)
    servicename_lookup = get_double_lookup(conf, servicename_decode)
    townzone_lookup = get_double_lookup(conf, townzone_decode)
    zone_lookup = get_double_lookup(conf, zone_decode)


def send_mail(send_from, send_to, subject, text, files=None,
              server=os.environ.get('DATA_WATCH_SERVER_EMAIL'), print_only=False):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text))

    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
        msg.attach(part)

    if print_only:
        print(msg)
    else:
        smtp = smtplib.SMTP(server, 25)
        smtp.sendmail(send_from, send_to, msg.as_string())
        smtp.close()


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        msg = "Not a valid date: '{0}'; use YYYY-MM-DD.".format(s)
        raise argparse.ArgumentTypeError(msg)
