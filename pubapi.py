import Settings
import tornado.web
import tornado.websocket
import base64
import os
import datetime
import MySQLdb as mydb
import yaml
import uuid
from Crypto.Cipher import AES
import hashlib
import ea_tasks
import pandas as pd
import numpy as np
import backup
import cx_Oracle
from api import humantime

dbConfig0 = Settings.dbConfig()
app_log = Settings.app_log


def check_permission(password, username, db):
    kwargs = {'host': dbConfig0.host, 'port': dbConfig0.port, 'service_name': db}
    dsn = cx_Oracle.makedsn(**kwargs)
    try:
        dbh = cx_Oracle.connect(username, password, dsn=dsn)
        dbh.close()
        return True, ""
    except Exception as e:
        error = str(e).strip()
        return False, error


def create_token_table(delete=False):
    with open('config/desaccess.yaml', 'r') as cfile:
        conf = yaml.load(cfile)['mysql']
    conf.pop('db', None)
    con = mydb.connect(**conf)
    try:
        con.select_db('des')
    except Exception:
        backup.restore()
        con.commit()
        con.select_db('des')
    cur = con.cursor()
    if delete:
        cur.execute("DROP TABLE IF EXISTS Tokens")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS Tokens(
    user varchar(50),
    token varchar(50),
    time datetime,
    UNIQUE(user)
    )""")
    con.commit()
    con.close()


def create_token(user):
        token = hashlib.sha1(os.urandom(64)).hexdigest()
        now = datetime.datetime.now()
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        nows = now.strftime('%Y-%m-%d %H:%M:%S')
        tup = tuple([user, token, nows])
        with con:
            cur = con.cursor()
            cur.execute("REPLACE INTO Tokens VALUES {0}".format(tup))
        con.close()
        return token


def check_token(token, ttl=Settings.TOKEN_TTL):
        now = datetime.datetime.now()
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        cur = con.cursor()
        cur.execute("SELECT *  from Tokens where token = '{0}'".format(token))
        try:
            cc = cur.fetchone()
            now = datetime.datetime.now()
            dt = (now - cc[2]).total_seconds()
            left = ttl - dt
            user = cc[0]
        except Exception:
            left = None
            user = None
        con.close()
        return left, user


class ApiTokenHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        arguments = {k.lower(): self.get_argument(k) for k in self.request.arguments}
        response = {'status': 'error'}
        if 'token' in arguments:
            ttl, user = check_token(arguments['token'])
            if ttl is None:
                response['message'] = 'Token does not exist'
                self.set_status(404)
            elif ttl < 0:
                response['message'] = 'Token is expired, please create a new one'
                self.set_status(404)
            else:
                response['status'] = 'ok'
                response['message'] = 'Token is valid for %s' % humantime(round(ttl))
                self.set_status(200)
        else:
            response['message'] = 'no token argument found!'
            self.set_status(400)
        self.write(response)
        self.flush()
        self.finish()

    @tornado.web.asynchronous
    def post(self):
        arguments = {k.lower(): self.get_argument(k) for k in self.request.arguments}
        response = {'status': 'error'}
        if 'username' in arguments:
            if 'password' not in arguments:
                response['message'] = 'Need password'
                self.set_status(400)
            else:
                user = arguments['username']
                passwd = arguments['password']
                check, msg = check_permission(passwd, user, 'dessci')
                if check:
                    response['status'] = 'ok'
                    newfolder = os.path.join(Settings.WORKDIR, user)
                    if not os.path.exists(newfolder):
                        os.mkdir(newfolder)
                    token = create_token(user)
                else:
                    self.set_status(403)
                    response['message'] = msg
        else:
            response['message'] = 'Need username'
            self.set_status(400)
        if response['status'] == 'ok':
            response['message'] = 'Token created, expiration time: %s' % humantime(Settings.TOKEN_TTL)
            response['token'] = token
            self.set_status(200)
        self.write(response)
        self.flush()
        self.finish()


class ApiCutoutHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        listargs = ['token', 'username', 'password', 'ra', 'dec', 'xsize', 'ysize', 'list_only','email', 'jobname']
        response = {'status': 'error'}
        arguments = {k.lower(): self.get_argument(k) for k in self.request.arguments}
        for l in listargs:
            if l not in arguments:
                msg = 'Missing {0}'.format(l)
                response['msg'] = msg
                self.set_status(400)
                self.write(response)
                self.finish()
                return
        token = arguments["token"]
        loc_user = arguments["username"]
        loc_passw = arguments["password"]
        email = arguments["email"]
        send_email = False
        if email != '':
            send_email = True
        jobname = arguments["jobname"]
        list_only = arguments["list_only"] == 'true'
        ra = [float(i) for i in arguments['ra'].replace('[', '').replace(']', '').split(',')]
        dec = [float(i) for i in arguments['dec'].replace('[', '').replace(']', '').split(',')]
        xs = np.ones(len(ra))
        ys = np.ones(len(ra))
        xs_read = [float(i) for i in arguments['xsize'].replace('[', '').replace(']', '').split(',')]
        if len(xs_read) == 1:
            xs = xs*xs_read
        if len(xs) >= len(xs_read):
            xs[0:len(xs_read)] = xs_read
        else:
            xs = xs_read[0:len(xs)]
        ys_read = [float(i) for i in arguments['ysize'].replace('[', '').replace(']', '').split(',')]
        if len(ys_read) == 1:
            ys = ys*ys_read
        if len(ys) >= len(ys_read):
            ys[0:len(ys_read)] = ys_read
        else:
            ys = ys_read[0:len(ys)]
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        try:
            cur = con.cursor()
            cur.execute("SELECT *  from Tokens where token = '{0}'".format(token))
            cc = cur.fetchone()
            now = datetime.datetime.now()
            dt = (now-cc[1]).total_seconds()
            msg = 'Token {0} times {1}'.format(token, dt)
            response['msg'] = msg
        except Exception as e:
            msg = 'Token not valid or expired'
            response['msg'] = msg
            self.set_status(403)
            print(str(e))
            self.write(response)
            self.finish()
            return
        con.close()
        cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
        lp = base64.b64encode(cipher.encrypt(loc_passw.rjust(32)))
        user_folder = os.path.join(Settings.WORKDIR, loc_user)+'/'
        response['user'] = loc_user
        response['elapsed'] = 0
        jobid = str(uuid.uuid4())
        df = pd.DataFrame(np.array([ra, dec, xs, ys]).T, columns=['RA', 'DEC', 'XSIZE', 'YSIZE'])
        input_csv = user_folder + jobid + '.csv'
        df.to_csv(input_csv, sep=',', index=False)
        del df
        folder2 = user_folder+jobid+'/'
        os.system('mkdir -p '+folder2)
        now = datetime.datetime.now()
        run = ea_tasks.desthumb.apply_async(args=[input_csv, loc_user, lp.decode(),
                                                  folder2, '', '', jobid, list_only,
                                                  send_email, email], retry=True, task_id=jobid)
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jobid, jobname, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
                     'cutout', '', '', '', -1])
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))
        con.close()
        response['msg'] = 'Job {0} submitted'.format(jobid)
        response['status'] = 'ok'
        response['kind'] = 'cutout'
        response['jobid'] = jobid
        self.write(response)
        self.flush()
        self.finish()


class ApiQueryHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        listargs = ['token', 'query', 'output', 'compression', 'email', 'jobname', 'db']
        response = {'status': 'error'}
        arguments = {k.lower(): self.get_argument(k) for k in self.request.arguments}
        for l in listargs:
            if l not in arguments:
                msg = 'Missing {0}'.format(l)
                response['msg'] = msg
                self.set_status(400)
                self.write(response)
                self.finish()
                return
        token = arguments["token"]
        query = arguments["query"]
        db = arguments["db"]
        query = query.replace(';', '')
        lines = query.split('\n')
        newquery = ''
        for line in lines:
            line = line.lstrip()
            if line.startswith('--'):
                continue
            newquery += ' ' + line.split('--')[0]
        query = newquery
        filename = arguments['output']
        if filename == '':
            self.set_status(400)
            response['msg'] = 'Filename cannot be empty'
            self.write(response)
            self.finish()
            return
        elif not filename.endswith('.csv') and not filename.endswith('.fits') and not filename.endswith('.h5'):
            self.set_status(400)
            response['msg'] = 'ERROR: File format allowed : .csv, .fits and .h5'
            self.write(response)
            self.finish()
            return
        compression = arguments["compression"].lower() == 'yes'
        email = arguments["email"]
        jobname = arguments["jobname"]
        ttl, user = check_token(token)
        if ttl is None or ttl < 0:
            response['msg'] = 'Token not valid or expired, create a new one'
            self.set_status(403)
            self.write(response)
            self.finish()
            return
        cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
        lp = base64.b64encode(cipher.encrypt('TODO'.rjust(32)))
        response['user'] = user
        response['elapsed'] = 0
        jobid = str(uuid.uuid4())

        run_check = ea_tasks.check_query(query, db, 'TODO', lp.decode())
        if run_check['status'] == 'error':
            response['msg'] = run_check['data']
            self.set_status(400)
            self.write(response)
            self.finish()
            return
        now = datetime.datetime.now()
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        tup = tuple([user, jobid, jobname, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
                     'query', query, '', '', -1])
        cur = con.cursor()
        cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))
        con.commit()
        con.close()
        try:
            run = ea_tasks.run_query.apply_async(args=[query, filename, db,
                                                 'TODO', lp.decode(), jobid, email, compression], retry=True, task_id=jobid)
        except Exception as e:
            self.set_status(400)
            response['msg'] = 'Unexpected Error'
            self.write(response)
            self.finish()
        response['jobid'] = jobid
        response['msg'] = 'Job {0} submitted'.format(jobid)
        response['status'] = 'ok'
        response['kind'] = 'query'

        self.write(response)
        self.finish()

class ApiJobHandler(tornado.web.RequestHandler):

    def post(self):
        response = {'status': 'error'}
        token = self.get_argument("token", "none")
        jobid = self.get_argument("jobid", "none")
        if jobid == "none":
            self.set_status(400)
            response['msg'] = 'Need Job Id'
            self.write(response)
            self.finish()
            return
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        try:
            cur = con.cursor()
            cur.execute("SELECT *  from Tokens where token = '{0}'".format(token))
            cc = cur.fetchone()
            now = datetime.datetime.now()
            dt = (now-cc[1]).total_seconds()
            msg = 'Token {0} times {1}'.format(token, dt)
            response['msg'] = msg
        except Exception as e:
            msg = 'Token not valid or expired'
            response['msg'] = msg
            self.set_status(403)
            print(str(e))
            con.close()
            self.write(response)
            self.finish()
            return
        try:
            cur = con.cursor()
            cur.execute("SELECT * from Jobs where job = '{0}'".format(jobid))
            cc = cur.fetchone()
            files = cc[7]
            ff = files[1:-1].replace('"', '').split(',')
            host = 'http://desdr-server.ncsa.illinois.edu/workdir/{0}/{1}/'.format(cc[0], jobid)
            final = [host+f.replace(' ','') for f in ff]
            response['msg'] = 'Job summary'
            response['job_status'] = cc[3]
            response['files'] = final
            response['job_runtime'] = cc[9]
            response['job_type'] = cc[5]
        except Exception as e:
            msg = 'Job id not valid or does not exist'
            response['msg'] = msg
            self.set_status(400)
            print(str(e))
            con.close()
            self.write(response)
            self.finish()
            return
        con.close()
        response['status'] = 'ok'
        self.write(response)
        self.flush()
        self.finish()
