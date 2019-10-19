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
import io

dbConfig0 = Settings.dbConfig()
app_log = Settings.app_log


def check_permission(password, username, db):
    kwargs = {'host': dbConfig0.host, 'port': dbConfig0.port, 'service_name': db}
    dsn = cx_Oracle.makedsn(**kwargs)
    app_log.info('Checking permissions for {}'.format(username))
    try:
        dbh = cx_Oracle.connect(username, password, dsn=dsn)
        dbh.close()
        return True, ""
    except Exception as e:
        error = str(e).strip()
        app_log.error(error)
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
    cp varchar(60),
    UNIQUE(user)
    )""")
    con.commit()
    con.close()


def create_token(user, cp):
        token = hashlib.sha1(os.urandom(64)).hexdigest()
        now = datetime.datetime.now()
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        nows = now.strftime('%Y-%m-%d %H:%M:%S')
        tup = tuple([user, token, nows, cp.decode()])
        cur = con.cursor()
        cur.execute("REPLACE INTO Tokens VALUES {0}".format(tup))
        con.commit()
        con.close()
        app_log.info('Adding Token to  {}'.format(user))
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
            lp = cc[3]
            app_log.info('Token {} is valid'.format(token))
        except Exception:
            left = None
            user = None
            lp = None
            app_log.warning('Token {} is invalid'.format(token))
        con.close()
        return left, user, lp


class ApiTokenHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def get(self):
        arguments = {k.lower(): self.get_argument(k) for k in self.request.arguments}
        response = {'status': 'error'}
        if 'token' in arguments:
            ttl, user, lp = check_token(arguments['token'])
            if ttl is None:
                response['message'] = 'Token does not exist'
                self.set_status(404)
            elif ttl < 0:
                response['message'] = 'Token is expired, please create a new one'
                app_log.warning(response['message'])
                self.set_status(404)
            else:
                response['status'] = 'ok'
                response['message'] = 'Token is valid for %s' % humantime(round(ttl))
                app_log.info(response['message'])
                self.set_status(200)
        else:
            response['message'] = 'no token argument found!'
            app_log.warning(response['message'])
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
                    cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
                    lp = base64.b64encode(cipher.encrypt(passwd.rjust(32)))
                    token = create_token(user, lp)
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


class ApiChartHandler(tornado.web.RequestHandler):
    def missingargs(self, response, msg):
        response['msg'] = msg
        self.set_status(400)
        app_log.warning(response['msg'])
        self.write(response)
        self.finish()

    @tornado.web.asynchronous
    def post(self):
        response = {'status': 'error'}

        listargs = ['token','xsize','ysize','jobname','colors','mag_limit','return_cut']

        arguments = {k.lower(): self.get_argument(k, '') for k in self.request.arguments}

        for l in listargs:
            if l not in arguments:
                msg = 'Missing {0}'.format(l)
                return self.missingargs(response, msg)

        if 'csvfile' in arguments:
            fileinfo = self.request.files['csvfile'][0]
            df = pd.DataFrame(pd.read_csv(io.BytesIO(fileinfo['body'])))
        elif 'ra' in arguments and 'dec' in arguments:
            ra = [float(i) for i in arguments['ra'].replace('[', '').replace(']', '').split(',')]
            dec = [float(i) for i in arguments['dec'].replace('[', '').replace(']', '').split(',')]
        else:
            msg = 'Missing input data.'
            return self.missingargs(response, msg)

        token = arguments['token']
        ttl, user, lp = check_token(token)
        if ttl is None or ttl < 0:
            response['msg'] = 'Token not valid or expired, create a new one'
            self.set_status(403)
            self.write(response)
            self.finish()
            return

        xsize = float(arguments['xsize'])
        ysize = float(arguments['ysize'])
        mag = arguments['mag_limit']
        jobname = arguments['jobname']

        try:
            email = arguments['email']
        except:
            email = ''
            send_email = False
        else:
            send_email = True

        user_folder = os.path.join(Settings.WORKDIR, user) + '/'
        response['user'] = user
        response['elapsed'] = 0
        jobid = str(uuid.uuid4()).replace("-", "_")

        db = 'dessci'
        input_csv = user_folder + jobid + '.csv'
        if 'ra' in arguments:
            df = pd.DataFrame(np.array([ra, dec]).T, columns=['RA','DEC'])
        df.to_csv(input_csv, sep=',', index=False)

        del df

        folder2 = user_folder + jobid + '/'
        os.system('mkdir -p ' + folder2)

        colors = str(arguments['colors'])
        return_cut = True if 'return_cut' in arguments and arguments['return_cut'].upper() == 'TRUE' else False

        run = ea_tasks.make_chart.apply_async(args=[input_csv, user, lp, folder2, db, xsize, ysize, jobid, return_cut, send_email, email, colors, mag], retry=True, task_id=jobid)

        app_log.info('Job Finding Chart {} submitted'.format(run))

        with open('config/desaccess.yaml','r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tup = tuple([user, jobid, jobname, 'PENDING', now, 'coadd', '', '', '', -1])

        cur = con.cursor()
        cur.execute("INSERT INTO Jobs VALUES{0}".format(tup))
        con.commit()
        con.close()
        response['msg'] = 'Job {0} submitted'.format(jobid)
        response['status'] = 'ok'
        response['kind'] = 'cutout'
        response['jobid'] = jobid
        self.write(response)
        self.flush()
        self.finish()


class ApiCutoutHandler(tornado.web.RequestHandler):
    def missingargs(self, response, msg):
        response['msg'] = msg
        self.set_status(400)
        app_log.warning(response['msg'])
        self.write(response)
        self.finish()

    @tornado.web.asynchronous
    def post(self):
        SMALL_QUEUE, SMALL_QUEUE_MAX_CPUS = 300, 1
        MEDIUM_QUEUE, MEDIUM_QUEUE_MAX_CPUS = 1000, 4
        LARGE_QUEUE, LARGE_QUEUE_MAX_CPUS = 10000, 6

        response = {'status': 'error'}

        listargs = ['token','xsize','ysize','jobname']  # required
        jtasks = ['make_stiff_rgb','make_lupton_rgb','make_fits']

        arguments = {k.lower(): self.get_argument(k, '') for k in self.request.arguments}

        for l in listargs:
            if l not in arguments:
                msg = 'Missing {0}'.format(l)
                return self.missingargs(response, msg)

        if 'csvfile' in arguments:
            fileinfo = self.request.files['csvfile'][0]
            df = pd.DataFrame(pd.read_csv(io.BytesIO(fileinfo['body'])))              # will this work?
            #df = pd.DataFrame(fileinfo['body'].pd.Series.str.decode('ascii'))        # will this work?
            #df.columns = [x.upper() for x in df.columns]                             # capitalizes columns headers
        elif 'ra' in arguments and 'dec' in arguments:
            ra = [float(i) for i in arguments['ra'].replace('[', '').replace(']', '').split(',')]
            dec = [float(i) for i in arguments['dec'].replace('[', '').replace(']', '').split(',')]
        elif 'coadd' in arguments:
            coadd = [int(i) for i in arguments['coadd'].replace('[','').replace(']','').split(',')]
        else:
            msg = 'Missing input data.'
            return self.missingargs(response, msg)

        if 'make_stiff_rgb' not in arguments and 'make_lupton_rgb' not in arguments and 'make_fits' not in arguments:
            msg = 'Missing job task. Select at least 1 from {}.'.format(jtasks)
            return self.missingargs(response, msg)

        if 'make_fits' in arguments and 'bands' not in arguments:
            msg = 'Missing color band(s) for make_fits.'
            return self.missingargs(response, msg)

        token = arguments["token"]
        ttl, user, lp = check_token(token)
        if ttl is None or ttl < 0:
            response['msg'] = 'Token not valid or expired, create a new one'
            self.set_status(403)
            self.write(response)
            self.finish()
            return

        xsize = arguments['xsize']
        ysize = arguments['ysize']

        try:
            email = arguments["email"]
        except:
            email = ''
            send_email = False
        else:
            send_email = True

        jobname = arguments["jobname"]

        user_folder = os.path.join(Settings.WORKDIR, user)+'/'
        response['user'] = user
        response['elapsed'] = 0
        jobid = str(uuid.uuid4()).replace("-", "_")

        input_csv = user_folder + jobid + '.csv'
        if 'ra' in arguments:
            #df = pd.DataFrame(np.array([ra, dec, xsize, ysize]).T, columns=['RA', 'DEC', 'XSIZE', 'YSIZE'])
            df = pd.DataFrame(np.array([ra, dec]).T, columns=['RA', 'DEC'])
        elif 'coadd' in arguments:
            #df = pd.DataFrame(np.array([coadd, xsize, ysize]).T, columns=['COADD_OBJECT_ID', 'XSIZE', 'YSIZE'])
            df = pd.DataFrame(np.array([coadd]).T, columns=['COADD_OBJECT_ID'])
        df.to_csv(input_csv, sep=',', index=False)

        dftemp_rows = len(df.index)
        if dftemp_rows <= SMALL_QUEUE:
            job_size = 'small'
            nprocs = SMALL_QUEUE_MAX_CPUS
        if dftemp_rows > SMALL_QUEUE and dftemp_rows <= MEDIUM_QUEUE:
            job_size = 'medium'
            nprocs = MEDIUM_QUEUE_MAX_CPUS
        if dftemp_rows > MEDIUM_QUEUE and dftemp_rows <= LARGE_QUEUE:
            job_size = 'large'
            nprocs = LARGE_QUEUE_MAX_CPUS
        if dftemp_rows > LARGE_QUEUE:
            job_size = 'manual'
            nprocs = LARGE_QUEUE_MAX_CPUS

        if dftemp_rows > LARGE_QUEUE:
            too_large = 'You can only submit up to {} objects at a time'.format(LARGE_QUEUE)
            response['msg'] = too_large
            self.set_status(400)
            self.write(response)
            self.finish()
            return

        del df

        folder2 = user_folder + jobid + '/'
        os.system('mkdir -p ' + folder2)

        db = 'dessci'
        if 'release' in arguments:
            release = arguments['release'].upper()
        else:
            release = 'Y6A1'
        tiffs = True if 'make_stiff_rgb' in arguments and arguments['make_stiff_rgb'].upper() == 'TRUE' else False
        pngs = True if 'make_lupton_rgb' in arguments and arguments['make_lupton_rgb'].upper() == 'TRUE' else False
        fits = True if 'make_fits' in arguments and arguments['make_fits'].upper() == 'TRUE' else False
        rgb = False
        if 'rgb_values' in arguments:
            rgb_values = arguments['rgb_values'].lower()
        else:
            rgb_values = 'i,r,g'



        if not tiffs and not pngs and not fits and not rgb:
            msg = 'At least 1 job task selected must be true: {}'.format(jtasks)
            return self.missingargs(response, msg)

        colors = ''
        if fits:
            colors = str(arguments['bands'])

        return_list = True if 'return_list' in arguments and arguments['return_list'].upper() == 'TRUE' else False

        run = ea_tasks.bulktasks.apply_async(args=[job_size, nprocs, input_csv, user, lp, jobid, folder2, db, release, tiffs, pngs, fits, rgb_values, colors, xsize, ysize, return_list, send_email, email], retry=True, task_id=jobid, queue='bulk-queue')

        app_log.info('Job Cutouts {} submitted'.format(run))

        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tup = tuple([user, jobid, jobname, 'PENDING', now, 'coadd', '', '', '', -1])

        cur = con.cursor()
        cur.execute("INSERT INTO Jobs VALUES{0}".format(tup))
        con.commit()
        con.close()
        response['msg'] = 'Job {0} submitted'.format(jobid)
        response['status'] = 'ok'
        response['kind'] = 'cutout'
        response['jobid'] = jobid
        self.write(response)
        self.flush()
        self.finish()


class ApiEpochHandler(tornado.web.RequestHandler):
    def missingargs(self, response, msg):
        response['msg'] = msg
        self.set_status(400)
        app_log.warning(response['msg'])
        self.write(response)
        self.finish()

    @tornado.web.asynchronous
    def post(self):
        response = {'status':'error'}

        listargs = ['token','xsize','ysize','jobname','make_fits','colors'] # required
        optargs = ['return_list','airmass','fwhm'] # optional

        arguments = {k.lower(): self.get_argument(k, '') for k in self.request.arguments}
        for l in listargs:
            if l not in arguments:
                msg = 'Missing{0}'.format(l)
                return self.missingargs(response, msg)

        if 'csvfile' in arguments:
            fileinfo = self.request.files['csvfile'][0]
            df = pd.DataFrame(pd.read_csv(io.BytesIO(fileinfo['body'])))
        elif 'ra' in arguments and 'dec' in arguments:
            ra = [float(i) for i in arguments['ra'].replace('[', '').replace(']', '').split(',')]
            dec = [float(i) for i in arguments['dec'].replace('[', '').replace(']', '').split(',')]
        else:
            msg = 'Missing input data.'
            return self.missingargs(response, msg)

        # Check token status
        token = arguments['token']
        ttl, user, lp = check_token(token)
        if ttl is None or ttl < 0:
            response['msg'] = 'Token not valid or expired, create a new one.'
            self.set_status(403)
            self.write(response)
            self.finish()

        # Required arguments
        xsize = float(arguments['xsize'])
        ysize = float(arguments['ysize'])
        colors = str(arguments['colors'])
        jobname = arguments['jobname']
        db = 'dessci'

        # Optional arguments
        return_list = True if 'return_list' in arguments and arguments['return_list'].upper() == 'TRUE' else False

        data_options = {'airmass':None, 'psffwhm':None}
        if 'airmass' in arguments:
            data_options['airmass'] = float(arguments['airmass'])
        if 'psffwhm' in arguments:
            data_options['psffwhm'] = float(arguments['psffwhm'])

        try:
            email = arguments['email']
        except:
            email = ''
            send_email = False
        else:
            send_email = True

        # Next section
        user_folder = os.path.join(Settings.WORKDIR, user)+'/'
        response['user'] = user
        response['elapsed'] = 0
        jobid = str(uuid.uuid4()).replace("-", "_")

        input_csv = user_folder + jobid + '.csv'
        if 'ra' in arguments:
            df = pd.DataFrame(np.array([ra, dec]).T, columns=['RA','DEC'])
        df.to_csv(input_csv, sep=',', index=False)
        del df

        folder2 = user_folder + jobid + '/'
        os.system('mkdir -p ' + folder2)

        run = ea_tasks.epochtasks.apply_async(args=[input_csv,
                                                    user,
                                                    lp,
                                                    jobid,
                                                    folder2,
                                                    db,
                                                    data_options,
                                                    colors,
                                                    xsize,
                                                    ysize,
                                                    return_list,
                                                    send_email,
                                                    email], 
                                              retyr=True,
                                              task_id=jobid)

        app_log.info('Job Single Epoch Cutouts {} submitted'.format(run))

        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        tup = tuple([user, jobid, jobname, 'PENDING', now, 'coadd', '', '', '', -1])

        cur = con.cursor()
        cur.execute("INSERT INTO Jobs VALUES{0}".format(tup))
        con.commit()
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
        fi = filename
        if filename == '':
            self.set_status(400)
            response['msg'] = 'Filename cannot be empty'
            self.write(response)
            self.finish()
            return
        elif not fi.endswith('.csv') and not fi.endswith('.fits') and not fi.endswith('.h5'):
            self.set_status(400)
            response['msg'] = 'ERROR: File format allowed : .csv, .fits and .h5'
            self.write(response)
            self.finish()
            return
        compression = arguments["compression"].lower() == 'yes'
        email = arguments["email"]
        jobname = arguments["jobname"]
        ttl, user, lp = check_token(token)
        if ttl is None or ttl < 0:
            response['msg'] = 'Token not valid or expired, create a new one'
            self.set_status(403)
            self.write(response)
            self.finish()
            return
        response['user'] = user
        response['elapsed'] = 0
        jobid = str(uuid.uuid4())

        run_check = ea_tasks.check_query(query, db, user, lp)
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
                                                 user, lp, jobid, email, compression],
                                                 retry=True, task_id=jobid)
            app_log.info('Job {} submitted...'.format(run))
        except Exception as e:
            self.set_status(400)
            response['msg'] = 'Unexpected Error: ' + repr(e)
            self.write(response)
            self.finish()
        response['jobid'] = jobid
        response['msg'] = 'Job {0} submitted'.format(jobid)
        response['status'] = 'ok'
        response['kind'] = 'query'

        self.write(response)
        self.finish()


class ApiJobHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    def post(self):
        response = {'status': 'error'}
        token = self.get_argument("token", "none")
        jobid = self.get_argument("jobid", "none")
        if jobid == "none":
            self.set_status(400)
            response['msg'] = 'Need Job Id, use all to see all jobs'
            self.write(response)
            self.finish()
            return
        ttl, user, lp = check_token(token)
        if ttl is None or ttl < 0:
            response['msg'] = 'Token not valid or expired, create a new one'
            self.set_status(403)
            self.write(response)
            self.finish()
            return
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        try:
            cur = con.cursor()
            if jobid.lower() == 'all':
                cur.execute("SELECT * from Jobs \
                            where user = '{0}' order by time DESC".format(user))
                cc = cur.fetchall()
                response['msg'] = 'List of all Jobs'
                response['job_id'] = [j[1] for j in cc]
                response['job_name'] = [j[2] for j in cc]
                response['job_status'] = [j[3] for j in cc]
                response['job_type'] = [j[5] for j in cc]
                response['job_creation'] = [str(j[4]) for j in cc]
                response['job_runtime'] = [j[9] for j in cc]
            else:
                cur.execute("SELECT * from Jobs where job = '{0}'".format(jobid))
                cc = cur.fetchone()
                files = cc[7]
                ff = files[1:-1].replace('"', '').split(',')
                host = Settings.URLPATH + '/workdir/{0}/{1}/'.format(cc[0], jobid)
                final = [host+f.replace(' ', '') for f in ff]
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

    @tornado.web.asynchronous
    def delete(self):
        response = {'status': 'error'}
        token = self.get_argument("token", "none")
        jobid = self.get_argument("jobid", "none")
        if jobid == "none":
            self.set_status(400)
            response['msg'] = 'Need Job Id'
            self.write(response)
            self.finish()
            return
        ttl, user, lp = check_token(token)
        if ttl is None or ttl < 0:
            response['msg'] = 'Token not valid or expired, create a new one'
            self.set_status(403)
            self.write(response)
            self.finish()
            return
        user_folder = os.path.join(Settings.WORKDIR, user)+'/'
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        try:
            cur = con.cursor()
            cur.execute("SELECT * from Jobs where job = '{0}'".format(jobid))
            cc = cur.fetchone()
            check_id = cc[1]
            q = "DELETE from Jobs where job = '{}' and user = '{}'".format(jobid, user)
            cur.execute(q)
            try:
                os.system('rm -rf ' + user_folder + jobid + '*')
            except Exception as e:
                print(e)
            response['msg'] = 'Job {} was deleted'.format(jobid)
        except Exception as e:
            msg = 'Job id not valid or does not exist'
            response['msg'] = msg
            self.set_status(400)
            print(str(e))
            con.close()
            self.write(response)
            self.finish()
            return
        con.commit()
        con.close()
        response['status'] = 'ok'
        self.write(response)
        self.flush()
        self.finish()
