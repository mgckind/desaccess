import tornado.ioloop
"""Easyaccess Web application."""
import tornado.web
import os
import pusher
import queries
import login_public as login
import api
import MySQLdb as mydb
from tornado.options import define, options
import Settings
import yaml
import backup
import cutout
from version import __version__

define("port", default=8080, help="run on the given port", type=int)

def create_db(delete=False):
    with open('config/mysqlconfig.yaml', 'r') as cfile:
        conf = yaml.load(cfile)['mysql']
    conf.pop('db', None)
    con = mydb.connect(**conf)
    try:
        con.select_db('des')
    except:
        backup.restore()
        con.commit()
        con.select_db('des')
    cur = con.cursor()
    if delete:
        cur.execute("DROP TABLE IF EXISTS Jobs")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Jobs(
    user text,
    job text,
    name mediumtext,
    status text,
    time datetime,
    type text,
    query mediumtext,
    files text,
    sizes text
    )""")
    con.commit()
    con.close()


class MyStaticFileHandler(tornado.web.StaticFileHandler):
    def write_error(self, status_code, *args, **kwargs):
        if status_code in [404]:
            self.render('404.html', version=__version__, errormessage='404: Page Not Found', username='')
        else:
            super().write_error(status_code, *args, **kwargs)

class My404Handler(tornado.web.RequestHandler):
    def prepare(self):
        self.set_status(404)
        self.render('404.html', version=__version__, errormessage='404: Page Not Found', username='')


class Application(tornado.web.Application):
    """
    The tornado application  class
    """

    def __init__(self):
        handlers = [
            (r"/", login.MainHandler),
            (r"/easyweb/?", login.MainHandler),
            (r"/easyweb/db-schema/?", login.MainHandler),
            (r"/easyweb/db-access/?", login.MainHandler),
            (r"/easyweb/db-examples/?", login.MainHandler),
            (r"/easyweb/cutouts/?", login.MainHandler),
            (r"/easyweb/footprint/?", login.MainHandler),
            (r"/easyweb/my-jobs/?", login.MainHandler),
            (r"/easyweb/help-form/?", login.MainHandler),
            (r"/easyweb/login/?", login.AuthLoginHandler),
            (r"/easyweb/changepass/?", login.ChangeAuthHandler),
            (r"/easyweb/changeinfo/?", login.UpdateInfoHandler),
            (r"/easyweb/logout/?", login.AuthLogoutHandler),
            (r"/easyweb/myjobs/?", api.MyJobsHandler),
            (r"/easyweb/mylogs/?", api.MyLogsHandler),
            (r"/easyweb/myexamples/?", api.MyExamplesHandler),
            (r"/easyweb/myresponse/?", api.MyResponseHandler),
            (r"/easyweb/mytables/?", api.MyTablesHandler),
            (r"/easyweb/desctables/?", api.DescTablesHandler),
            (r"/easyweb/alltables/?", api.AllTablesHandler),
            (r'/easyweb/websocket/?', pusher.WebSocketHandler),
            (r'/easyweb/pusher/?', pusher.PusherHandler),
            (r"/easyweb/query/?", queries.QueryHandler),
            (r"/easyweb/reset/(\w+)", login.ResetHandler),
            (r"/easyweb/email/(.+)", login.EmailHandler),
            (r"/easyweb/reset/", login.ResetHandler),
            (r"/easyweb/activate/(\w+)", login.ActivateHandler),
            (r"/easyweb/signup/?", login.SignupHandler),
            (r"/easyweb/cutout/", cutout.FileHandler),
            (r"/easyweb/delete/", api.DeleteHandler),
            (r"/easyweb/change/?", api.ChangeHandler),
            (r"/easyweb/gettile/?", api.GetTileHandler),
            (r"/easyweb/help/?", api.HelpHandler),
            (r"/easyweb/files/dr1/(.*)", MyStaticFileHandler,
             {'path': '/des004/despublic/dr1_tiles/'}),
        ]
        settings = {
            "template_path": Settings.TEMPLATE_PATH,
            "static_path": Settings.STATIC_PATH,
            "debug": Settings.DEBUG,
            "cookie_secret": Settings.COOKIE_SECRET,
            "login_url": "/easyweb/login/",
            "static_url_prefix": "/easyweb/static/",
            "default_handler_class": My404Handler,
        }
        tornado.web.Application.__init__(self, handlers, **settings)


def main():
    """
    The main function
    """
    if not os.path.exists(Settings.WORKDIR):
        os.mkdir(Settings.WORKDIR)
    create_db()
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
