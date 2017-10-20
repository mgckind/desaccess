""" Settings for application"""
import os
import random
import string
import logging
DEBUG = True
DIRNAME = os.path.dirname(__file__)
STATIC_PATH = os.path.join(DIRNAME, 'easyweb/static')
LOG_PATH = os.path.join(DIRNAME, 'logs')
if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)
TEMPLATE_PATH = os.path.join(DIRNAME, 'templates')
WORKDIR = os.path.join(STATIC_PATH, "workdir/")
#ROOT_URL = 'http://desrelease.cosmology.illinois.edu'
ROOT_URL = 'http://localhost:8080'
LOGFILE = os.path.join(LOG_PATH, "access.log")
LOG_GENERALFILE = os.path.join(LOG_PATH, "general.log")
LOG_APPFILE = os.path.join(LOG_PATH, "app.log")
WORKERS = os.path.join(DIRNAME, 'workers')
# TODO: read from file since there will be a race conition between pods
with open('config/ranC.tk', 'r') as fi:
    COOKIE_SECRET = fi.readline().strip()
    SKEY = fi.readline().strip()
# COOKIE_SECRET = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
# SKEY = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(16))
access_log = logging.getLogger('tornado.access')
access_log.setLevel(logging.DEBUG)
handler_access = logging.FileHandler(LOGFILE)
formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)-8s - %(message)s',
                              datefmt='%d/%m/%Y %Hh%Mm%Ss')
handler_access.setFormatter(formatter)
access_log.addHandler(handler_access)

class dbConfig(object):
    def __init__(self):
        self.host = 'desdb-dr.ncsa.illinois.edu'
        self.port = '1521'
