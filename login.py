import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import Settings
import cx_Oracle
import os
import time
from version import __version__

dbConfig0 = Settings.dbConfig()


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")


class MainHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        #self.render("main.html", name='Matias', email='', username='mcarras2') #TODO
        loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"', '')
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        loc_db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
        newfolder = os.path.join(Settings.WORKDIR, loc_user)
        if not os.path.exists(newfolder):
            os.mkdir(newfolder)
        kwargs = {'host': dbConfig0.host, 'port': dbConfig0.port, 'service_name': loc_db}
        dsn = cx_Oracle.makedsn(**kwargs)
        dbh = cx_Oracle.connect(loc_user, loc_passw, dsn=dsn)
        cursor = dbh.cursor()
        try:
            cc = cursor.execute('select firstname,email from des_users where '
                                'upper(username) = \'%s\'' % loc_user.upper()).fetchone()
        except:
            cc = ('', '')
        cursor.close()
        dbh.close()
        self.render("main.html", name=cc[0], email=cc[1], username=loc_user, db=loc_db)


class AuthLoginHandler(BaseHandler):
    def get(self):
        update = 'no'
        toast = 'no'
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
        self.render("login.html", errormessage=errormessage, version=__version__, update=update,
                    toast=toast, db=db)

    def check_permission(self, password, username, db):
        #return True, "" #TODO
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


class ChangeAuthHandler(BaseHandler):

    def check_permission_new(self, oldpassword, password, username, db):
        #return True, "" #TODO
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
        # print(oldpassword, password)
        auth, err = self.check_permission_new(oldpassword, password, username, db)
        if auth:
            self.clear_cookie("usera")
            self.clear_cookie("userb")
            self.clear_cookie("userdb")
            self.render("login.html", errormessage="", version=__version__, update='no',
                        toast='yes', db=db)
        else:
            self.render("login.html", errormessage=err, version=__version__, update='yes',
                        toast='no', db=db)
            # error_msg = u"?error=" + tornado.escape.url_escape(err)
            # self.redirect(u"/easyweb/login/" + error_msg)

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
