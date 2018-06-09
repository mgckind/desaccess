"""DES Jupyter Labs """
import tornado.ioloop
import tornado.web
import uuid
import json

class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")

class LaunchHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        token = str(uuid.uuid4())
        print(user, token)
        print('deploying Lab')
        #deploy.main(action='deploy',user=user,token=token)
        #insert_token(user, token)
        temp = json.dumps({'status': 'done'}, indent=4)
        self.write(temp)

class LabStatusHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        token = str(uuid.uuid4())
        print('status Lab', token[0:10])
        #deploy.main(action='deploy',user=user,token=token)
        #insert_token(user, token)
        temp = json.dumps({'status': token[0:10]}, indent=4)
        self.write(temp)
