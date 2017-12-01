import tornado.ioloop
"""Easyaccess Web application."""
import tornado.web
from tornado.options import define, options
from version import __version__
import Settings

define("port", default=8080, help="run on the given port", type=int)


class DownHandler(tornado.web.RequestHandler):
    def get(self):
        self.set_status(404)
        self.render('service-down.html', version=__version__, errormessage='These services are down for the moment', username='')

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        #self.set_status(404)
        self.render('service-down.html', errormessage='These services are down for the moment', username='')

class Application(tornado.web.Application):
    """
    The tornado application  class
    """

    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/easyweb/", MainHandler),
        ]
        settings = {
            "template_path": Settings.TEMPLATE_PATH,
            "static_path": Settings.STATIC_PATH,
            "debug": Settings.DEBUG,
            "static_url_prefix": "/easyweb/static/",
            "default_handler_class": DownHandler,
        }
        tornado.web.Application.__init__(self, handlers, **settings)


def main():
    """
    The main function
    """
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
