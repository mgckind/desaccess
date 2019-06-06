""" Settings for application"""
import os
import random
import string
import logging
import yaml
DEBUG = True
DIRNAME = os.path.dirname(__file__)
TOKEN_TTL = 3600
STATIC_PATH = os.path.join(DIRNAME, 'easyweb/static')
URLPATH = 'http://desdr-server.ncsa.illinois.edu/easyweb'
try:
    LOG_PATH = os.path.join(DIRNAME, 'logs', os.environ['POD_ID'])
except:
    LOG_PATH = os.path.join(DIRNAME, 'logs')

if not os.path.exists(LOG_PATH):
    os.makedirs(LOG_PATH)
TEMPLATE_PATH = os.path.join(DIRNAME, 'templates')
WORKDIR = os.path.join(STATIC_PATH, "workdir/")
PLTDATA = os.path.join(STATIC_PATH, "plotdata/")
DBFILE = os.path.join(STATIC_PATH, "workdir/admin/users.db")
DBFILE2 = os.path.join(STATIC_PATH, "workdir/admin/users.db")
ROOT_URL = 'https://des.ncsa.illinois.edu'
#ROOT_URL = 'http://localhost:8080'
LOGFILE = os.path.join(LOG_PATH, "access.log")
LOG_GENERALFILE = os.path.join(LOG_PATH, "general.log")
LOG_APPFILE = os.path.join(LOG_PATH, "app.log")
WORKERS = os.path.join(DIRNAME, 'workers')
# TODO: read from file since there will be a race conition between pods
with open('config/desaccess.yaml', 'r') as cfile:
    conf = yaml.load(cfile)['secrets']
COOKIE_SECRET = conf['cookie']
SKEY = conf['skey']
# COOKIE_SECRET = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
# SKEY = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)-8s - %(message)s',
                              datefmt='%d/%m/%Y %H:%M:%S')

access_log = logging.getLogger('tornado.access')
access_log.setLevel(logging.DEBUG)
handler_access = logging.FileHandler(LOGFILE)
handler_access.setFormatter(formatter)
access_log.addHandler(handler_access)

general_log = logging.getLogger('tornado.general')
general_log.setLevel(logging.DEBUG)
handler_general = logging.FileHandler(LOG_GENERALFILE)
handler_general.setFormatter(formatter)
general_log.addHandler(handler_general)

app_log = logging.getLogger('tornado.application')
app_log.setLevel(logging.DEBUG)
handler_app = logging.FileHandler(LOG_APPFILE)
handler_app.setFormatter(formatter)
app_log.addHandler(handler_app)


class dbConfig(object):
    def __init__(self):
        # self.host = 'desdb-dr.ncsa.illinois.edu'
        self.host = 'desdb.ncsa.illinois.edu'
        self.port = '1521'
