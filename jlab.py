"""DES Jupyter Labs """
import tornado.ioloop
import tornado.web
import uuid
import json
import requests

class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")

class LabLaunchHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        print(user)
        print('deploying Lab')
        r = requests.post('http://localhost:8888/labs/api/v1/deploy', data={'user': user})
        print(r.json()['token'])
        temp = json.dumps({'status': 'deploying'}, indent=4)
        self.write(temp)

class LabStatusHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        r = requests.post('http://localhost:8888/labs/api/v1/status', data={'user': user})
        status = r.json()['msg']
        ready = r.json()['err']
        print('status Lab', status)
        temp = json.dumps({'status': status, 'ready': ready}, indent=4)
        self.write(temp)

class LabGotoHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        r = requests.post('http://localhost:8888/labs/api/v1/token', data={'user': user})
        token = r.json()['token']
        temp = json.dumps({'status': token}, indent=4)
        self.write(temp)
        self.redirect('/easyweb/deslabs/labs/{user}/?token={token}'.format(user=user, token=token))

class LabDeleteHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        r = requests.post('http://localhost:8888/labs/api/v1/delete', data={'user': user})
        print('deleting Lab')
        temp = json.dumps({'status': 'deleting'}, indent=4)
        self.write(temp)
