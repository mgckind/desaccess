import tornado.web
import tornado.websocket
import json

open_sockets = set()


def SendMessage(msg):
    closed = set()
    for socket in open_sockets:
        if not socket.ws_connection or not socket.ws_connection.stream.socket:
            closed.add(socket)
        else:
            socket.write_message(msg)
    for socket in closed:
        open_sockets.remove(socket)


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        print("WebSocket opened")
        open_sockets.add(self)

    def on_message(self, message):
        pass

    def on_close(self):
        print("WebSocket closed")


class PusherHandler(tornado.web.RequestHandler):
    """pusher handler."""

    def post(self):
        data = self.get_argument('jobid')
        try:
            data2 = json.loads(data)
        except:
            data2 = data
        msg = {'out': data2}
        SendMessage(json.dumps(msg))
