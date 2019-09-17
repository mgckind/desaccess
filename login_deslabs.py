import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import Settings
from oracle import db_utils
from smtp import email_utils
import cx_Oracle
import os
import json
import yaml
from version import __version__

dbConfig0 = Settings.dbConfig()
app_log = Settings.app_log

class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")





class ResetHandler(BaseHandler):
    def get(self, slug):
        username, msg = db_utils.valid_url(slug, 24*3600)
        if username is not None:
            self.render('reset.html', errormessage='', version=__version__, toast='no', url=slug, user=username)
        else:
            self.render('activate.html', version=__version__, errormessage=msg, username='')

    def post(self):
        email = self.get_argument("email", "").lower()
        print(email)
        print('Reset Password')
        app_log.info('Reset Password')
        check, url, checkuser = db_utils.create_reset_url(email)
        print(url)
        if check:
            email_utils.send_reset(email, checkuser, url)
            app_log.info('{},{},{}'.format(email, url, checkuser))
            self.write(json.dumps({'msg': 'Reset email sent!', 'errno': '0'}))
        else:
            self.write(json.dumps({'msg': '{}'.format(url), 'errno': '1'}))

    def put(self):
        username = self.get_argument("username", "")
        password = self.get_argument("password", "")
        valid = len(password) >= 6
        url = self.get_argument("url", "")
        print(username, password, url)
        username2, msg = db_utils.valid_url(url, 24.5*3600)
        print(username2, msg)
        if username == username2:
            print('valid', valid)
            if valid:
                check, msg = db_utils.update_password(username, password)
                if check:
                    db_utils.unlock_user(username)
                    self.write(json.dumps({'msg': 'Password updated', 'errno': '0'}))
                else:
                    self.write(json.dumps({'msg': msg + '. Try again', 'errno': '1'}))
            else:
                msg = 'Password minimum length is 6.'
                self.write(json.dumps({'msg': msg, 'errno': '1'}))
        else:
            self.write(json.dumps({'msg': msg, 'errno': '1'}))


class ActivateHandler(BaseHandler):
    def get(self, slug):
        username, msg = db_utils.valid_url(slug, 24*3600)
        if username is not None:
            db_utils.unlock_user(username)
            msg = 'Thanks for activating your account'
            self.render('activate.html', version=__version__, errormessage=msg, username='')
        else:
            self.render('activate.html', version=__version__, errormessage=msg, username='')


class MainHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        loc_db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
        newfolder = os.path.join(Settings.WORKDIR, loc_user)
        if not os.path.exists(newfolder):
            os.mkdir(newfolder)
        kwargs = {'host': dbConfig0.host, 'port': dbConfig0.port, 'service_name': 'desoper'}
        dsn = cx_Oracle.makedsn(**kwargs)
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['oracle']
        user_manager = conf['user']
        pass_manager = conf['passwd']
        del conf
        dbh = cx_Oracle.connect(user_manager, pass_manager, dsn=dsn)
        cursor = dbh.cursor()
        try:
            cc = cursor.execute('select firstname,lastname,email from des_users where '
                                'upper(username) = \'%s\'' % loc_user.upper()).fetchone()
        except:
            cc = ('', '', '')
        try:
            ee = cursor.execute('select apps from MCARRAS2.USER_APPS where '
                                'upper(username) = \'%s\'' % loc_user.upper()).fetchone()
        except:
            ee = ('jlab fchart analysis')
        cursor.close()
        dbh.close()
        try:
            self.render("main-public.html", name=cc[0], lastname=cc[1],
                        email=cc[2], username=loc_user, version=__version__, db=loc_db, eapps=ee[0])
        except:
            self.render("login-public.html", errormessage='',
                        version=__version__, update='no', toast='no', db='', next='')


class AuthLoginHandler(BaseHandler):
    def get(self):
        update = 'no'
        toast = 'no'
        next = self.get_argument('next', u'/easyweb/')
        try:
            errormessage = self.get_argument("error")
            print(errormessage)
            if '28001' in errormessage:
                update = 'yes'
                errormessage += ". Please enter new password."
        except:
            errormessage = ""
        try:
            db = self.get_argument("db")
            print(db)
        except:
            db = ""
        self.render("login-public.html", errormessage=errormessage, version=__version__,
                    update=update, toast=toast, db=db, next=next)

    def check_permission(self, password, username, db):
        kwargs = {'host': dbConfig0.host, 'port': dbConfig0.port, 'service_name': db}
        dsn = cx_Oracle.makedsn(**kwargs)
        try:
            dbh = cx_Oracle.connect(username, password, dsn=dsn)
            dbh.close()
            return True, ""
        except Exception as e:
            error = str(e).strip()
            return False, error

    def post(self):
        username = self.get_argument("username", "")
        password = self.get_argument("password", "")
        db = self.get_argument("database", "")
        auth, err = self.check_permission(password, username, db)
        if auth:
            self.set_current_user(username, password, db)
            newfolder = os.path.join(Settings.WORKDIR, username)
            if not os.path.exists(newfolder):
                os.mkdir(newfolder)
            # Add to DB for stats
            self.redirect(self.get_argument("next", u"/easyweb/"))
        else:
            error_msg = u"?error=" + tornado.escape.url_escape(err)
            db_msg = u";db=" + tornado.escape.url_escape(db)
            self.redirect(u"/easyweb/login/" + error_msg + db_msg)

    def set_current_user(self, user, passwd, db):
        if user:
            self.set_secure_cookie("usera", tornado.escape.json_encode(user), expires_days=5)
            self.set_secure_cookie("userb", tornado.escape.json_encode(passwd), expires_days=5)
            self.set_secure_cookie("userdb", tornado.escape.json_encode(db), expires_days=5)
        else:
            self.clear_cookie("usera")
            self.clear_cookie("userb")
            self.clear_cookie("userdb")


class UpdateInfoHandler(BaseHandler):
    def post(self):
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['oracle']
        user_manager = conf['user']
        pass_manager = conf['passwd']
        del conf
        username = self.get_argument("username", "")
        firstname = self.get_argument("firstname", "")
        lastname = self.get_argument("lastname", "")
        email = self.get_argument("email", "")
        err = ''
        db_utils.update_info(username, firstname, lastname, email, user_manager, pass_manager, db='desoper')
        return self.write(json.dumps({'msg': err, 'errno': '0'}))


class ChangeAuthHandler(BaseHandler):

    def check_permission_new(self, oldpassword, password, username, db):
        kwargs = {'host': dbConfig0.host, 'port': dbConfig0.port, 'service_name': db}
        dsn = cx_Oracle.makedsn(**kwargs)
        try:
            dbh = cx_Oracle.connect(username, oldpassword, dsn=dsn, newpassword=password)
            dbh.close()
            return True, ""
        except Exception as e:
            error = str(e).strip()
            return False, error

    def post(self):
        username = self.get_argument("username", "")
        oldpassword = self.get_argument("oldpassword", "")
        password = self.get_argument("password", "")
        db = self.get_argument("database", "")
        valid = len(password) >= 6
        if valid:
            auth, err = self.check_permission_new(oldpassword, password, username, db)
        else:
            auth = False
            err = 'Password minimum length is 6.'
        if auth:
            self.clear_cookie("usera")
            self.clear_cookie("userb")
            self.clear_cookie("userdb")
            return self.write(json.dumps({'msg': err, 'errno': '0'}))
        else:
            return self.write(json.dumps({'msg': err, 'errno': '1'}))

    def set_current_user(self, user, passwd, db):
        if user:
            self.set_secure_cookie("usera", tornado.escape.json_encode(user), expires_days=5)
            self.set_secure_cookie("userb", tornado.escape.json_encode(passwd), expires_days=5)
            self.set_secure_cookie("userdb", tornado.escape.json_encode(db), expires_days=5)
        else:
            self.clear_cookie("usera")
            self.clear_cookie("userb")
            self.clear_cookie("userdb")


class AuthLogoutHandler(BaseHandler):
    def get(self):
        self.clear_cookie("usera")
        self.clear_cookie("userb")
        self.clear_cookie("userdb")
        self.redirect(self.get_argument("next", "/easyweb/"))
