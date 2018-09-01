import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import Settings
import cx_Oracle
import os
import yaml
from version import __version__

dbConfig0 = Settings.dbConfig()
app_log = Settings.app_log

class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")


class CountHandler(BaseHandler):
    def get(self):
        kwargs = {'host': dbConfig0.host, 'port': dbConfig0.port, 'service_name': 'desdr'}
        dsn = cx_Oracle.makedsn(**kwargs)
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['oracle']
        user_manager = conf['user']
        pass_manager = conf['passwd']
        del conf
        dbh = cx_Oracle.connect(user_manager, pass_manager, dsn=dsn)
        cursor = dbh.cursor()
        try:
            cc = cursor.execute('select count(*) from des_admin.des_users').fetchone()
        except:
            cc = ('')
        cursor.close()
        dbh.close()
        self.write('<br><br><h1>Count = {}</h1>'.format(cc[0]-49))
