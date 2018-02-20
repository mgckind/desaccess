import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
from Crypto.Cipher import AES
import base64
import os
import uuid
import Settings
import datetime
import datetime as dt
import MySQLdb as mydb
import yaml
import ea_tasks
import requests
import re
import config.descut as des

from time import sleep

def create_token():
    req = requests.post('https://descut.cosmology.illinois.edu/api/token/',
                        data={'username': des.USERNAME, 'password': des.PASSWORD}, verify=False)
    return req

class infoP(object):
    def __init__(self, uu, pp):
        self._uu=uu
        self._pp=pp

def dt_t(entry):
    t = dt.datetime.strptime(entry['time'], '%a %b %d %H:%M:%S %Y')
    return t.strftime('%Y-%m-%d %H:%M:%S')


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")


class FileHandler(BaseHandler):
    @tornado.web.asynchronous
    @tornado.web.authenticated
    def post(self):
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"', '')
        user_folder = os.path.join(Settings.WORKDIR, loc_user)+'/'
        cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
        lp = base64.b64encode(cipher.encrypt(loc_passw.rjust(32)))
        xs = float(self.get_argument("xsize"))
        ys = float(self.get_argument("ysize"))
        list_only = self.get_argument("list_only") == 'true'
        send_email = self.get_argument("send_email") == 'true'
        email = self.get_argument("email")
        name = self.get_argument("name")
        tag = self.get_argument("tag")
        stype = self.get_argument("submit_type")
        values = self.get_argument("values")

        print('**************')
        print(xs, ys, 'sizes')
        print(stype, 'type')
        print(list_only, 'list_only')
        print(send_email, 'send_email')
        print(email, 'email')
        print(name, 'name')
        print(name, '<<== tag')


        jobid = str(uuid.uuid4())
        #if name == '':
        #    name = jobid
        if stype == "manual":
            values = self.get_argument("values")
            print(values)
            filename = user_folder+jobid+'.csv'
            F = open(filename, 'w')
            F.write("RA,DEC\n")
            F.write(values)
            F.close()
        if stype == "csvfile":
            fileinfo = self.request.files["csvfile"][0]
            fname = fileinfo['filename']
            extn = os.path.splitext(fname)[1]
            print(fname)
            print(fileinfo['content_type'])
            filename = user_folder+jobid+extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        print('**************')
        folder2 = user_folder+jobid+'/'
        os.system('mkdir -p '+folder2)
        now = datetime.datetime.now()
        input_csv = user_folder + jobid + '.csv'

        f = open(input_csv, 'r+')
        ra = []
        dec = []
        count = 0
        for line in f:
            if count == 0:
                count += 1
                continue
            count += 1
            l = line.split(',')
            r = float(l[0])
            d = float(l[1])
            ra.append(r)
            dec.append(d)

        f.close()


        # create token
        req = create_token()
        print(req)
        print(req.text)
        response = req.json()
        status = response['status']
        if status == "error":
            print("PASSWORD ERROR")
            self.set_status(403)
            self.flush()
            self.finish()
        token = response['token']


        # create job query body
        # create body of request
        body = {
            'token': token,  # required
            'ra': str(ra),  # required
            'dec': str(dec),  # required
            'job_type': 'coadd',  # required 'coadd' or 'single'
            'xsize': float(xs),  # optional (default : 1.0)
            'ysize': float(ys),  # optional (default : 1.0)
            'tag': tag,
        # optional for 'coadd' jobs (default: Y3A1_COADD, see Coadd Help page for more options)
            'no_blacklist': 'false',
        # optional for 'single' epochs jobs (default: 'false'). return or not blacklisted exposures
            'list_only': list_only,  # optional (default : 'false') 'true': will not generate pngs (faster)
            # 'comment': ''
        }

        if send_email:
            body['email'] = email

        # create body for files if needed
        body_files = {'csvfile': open(input_csv, 'rb')}  # To load csv file as part of request
        # To include files
        req = requests.post('https://descut.cosmology.illinois.edu/api/jobs/', data=body, files=body_files, verify=False)
        print(req)
        print(req.text)
        response = req.json()
        status = response['status']
        if status == 'error':
            self.set_status(403)
            self.flush()
            self.finish()
        jid =  response['job']
        msg = response['message']
        print (msg)
        result = ''

        while(1):
            # get job result
            url = "https://descut.cosmology.illinois.edu/api/jobs/?token={0}&jobid={1}".format(token, jid)
            req = requests.get(url, verify=False)
            print(req.text)
            response = req.json()
            status = response['status']
            if status == 'ok':
                result = response
                break

        # run = ea_tasks.desthumb.apply_async(args=[input_csv, loc_user, lp.decode(),
        #                                           folder2, xs, ys, jobid, list_only,
        #                                           send_email, email], retry=True, task_id=jobid)
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
                     'coadd', '', '', '', -1])
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))

        con.close()

        print("Process id before forking: {}".format(os.getpid()))

        try:
            pid = os.fork()
        except OSError:
            exit("Could not create a child process")

        if pid == 0:
            with open('config/mysqlconfig.yaml', 'r') as cfile:
                conf = yaml.load(cfile)['mysql']
            con = mydb.connect(**conf)
            print("In the child process that has the PID {}".format(os.getpid()))
            while (1):
                sleep(1)
                # get job result
                url = "https://descut.cosmology.illinois.edu/api/jobs/?token={0}&jobid={1}".format(token, jid)
                req = requests.get(url, verify=False)
                print(req.text)
                response = req.json()
                msg = response['message']
                if msg.find("PENDING") != -1:
                    continue
                if msg.find("SUCCESS") != -1:
                    with con:
                        cur = con.cursor()
                        q = "UPDATE Jobs SET status='{0}' where job = '{1}'".format('SUCCESS', jid)
                        cur.execute(q)
                    break
                if msg.find("SUCCESS") == -1 and msg.find("PENDING") == -1:
                    with con:
                        cur = con.cursor()
                        q = "UPDATE Jobs SET status='{0}' where job = '{1}'".format('FAILED', jid)
                        cur.execute(q)
                    break

            # copy the file to local
            # req = requests.get('https://descut.cosmology.illinois.edu/api/jobs/?token={0}&jobid={1}'.format(token, jid))
            # print(req.text)
            # links = req.json()['links']
            # k = 0
            # for l in links:
            #     # only use png
            #     if l.endswith('png'):
            #         print(l)
            #         r = requests.get(l, stream=True)
            #         if r.status_code == 200:
            #             img = "image_%d.png" % (k)
            #             fp = folder2 + img
            #             f = open(fp, "wb")
            #             for chunk in r:
            #                 f.write(chunk)
            #         k += 1
            #
            print("Goodbye, this cruel world.")
            con.close()
            exit()

        print("In the parent process after forking the child {}".format(pid))
        # not wait for the child, go fly
        # finished = os.waitpid(0, 0)
        # print(finished)


        self.set_status(200)
        self.flush()
        self.finish()




class FileHandlerS(BaseHandler):
    @tornado.web.asynchronous
    @tornado.web.authenticated
    def post(self):
        print("REACH ME")
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"','')
        loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"','')
        user_folder = os.path.join(Settings.WORKDIR,loc_user)+'/'

        cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
        lp = base64.b64encode(cipher.encrypt(loc_passw.rjust(32)))

        xs = float(self.get_argument("xsize"))
        ys = float(self.get_argument("ysize"))
        list_only = self.get_argument("list_only") == 'true'
        send_email = self.get_argument("send_email") == 'true'
        noBlacklist = self.get_argument('noBlacklist') == 'true'
        bands = self.get_argument('bands')
        email = self.get_argument("email")
        # comment = self.get_argument("comment")
        stype = self.get_argument("submit_type")
        name = self.get_argument("name")
        print('**************')
        print(xs,ys,'sizes')
        print(stype,'type')
        print(list_only,'list_only')
        print(send_email,'send_email')
        print(email,'email')
        # print(comment,'comment')
        print(bands, 'bands')
        print(noBlacklist, 'noBlacklist')
        print(name, 'name')
        jobid = str(uuid.uuid4())
        if bands == "all":
            bands = ["g", "r", "i", "z", "Y"]
        else:
            bands = list(bands.split(" "))
        print("bands ==> ", bands)

        if stype=="manual":
            values = self.get_argument("values")
            print(values)
            filename = user_folder+jobid+'.csv'
            F=open(filename,'w')
            F.write("RA,DEC\n")
            F.write(values)
            F.close()
        if stype=="csvfile":
            fileinfo = self.request.files["csvfile"][0]
            fname = fileinfo['filename']
            extn = os.path.splitext(fname)[1]
            print(fname)
            print(fileinfo['content_type'])
            filename = user_folder+jobid+extn
            with open(filename,'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        # print('**************')
        # job_dir=user_folder+'results/'+jobid+'/'
        # os.system('mkdir -p '+job_dir)
        # infP=infoP(loc_user,loc_passw)
        # now = datetime.datetime.now()
        # tiid = loc_user+'__'+jobid+'_{'+now.strftime('%a %b %d %H:%M:%S %Y')+'}'

        print('**************')
        folder2 = user_folder + jobid + '/'
        os.system('mkdir -p ' + folder2)
        now = datetime.datetime.now()
        input_csv = user_folder + jobid + '.csv'

        f = open(input_csv, 'r+')
        ra = []
        dec = []
        count = 0
        for line in f:
            if count == 0:
                count += 1
                continue
            count += 1
            l = line.split(',')
            r = float(l[0])
            d = float(l[1])
            ra.append(r)
            dec.append(d)

        f.close()

        # create token
        req = create_token()
        print(req)
        print(req.text)
        response = req.json()
        status = response['status']
        if status == "error":
            print("PASSWORD ERROR")
            self.set_status(403)
            self.flush()
            self.finish()
        token = response['token']

        # create job query body
        # create body of request
        body = {
            'token': token,  # required
            'ra': str(ra),  # required
            'dec': str(dec),  # required
            'job_type': 'single',  # required 'coadd' or 'single'
            'xsize': float(xs),  # optional (default : 1.0)
            'ysize': float(ys),  # optional (default : 1.0)
            'band': bands,  # optional for 'single' epochs jobs (default: all bands)
            'no_blacklist': noBlacklist,
            # optional for 'single' epochs jobs (default: 'false'). return or not blacklisted exposures
            'list_only': list_only,  # optional (default : 'false') 'true': will not generate pngs (faster)
            # 'comment': ''
        }

        if send_email:
            body['email'] = email

        # create body for files if needed
        body_files = {'csvfile': open(input_csv, 'rb')}  # To load csv file as part of request
        # To include files
        req = requests.post('https://descut.cosmology.illinois.edu/api/jobs/', data=body, files=body_files,
                            verify=False)
        print(req)
        print(req.text)
        response = req.json()
        status = response['status']
        if status == 'error':
            self.set_status(403)
            self.flush()
            self.finish()
        jid = response['job']
        msg = response['message']
        print (msg)
        result = ''

        while (1):
            # get job result
            url = "https://descut.cosmology.illinois.edu/api/jobs/?token={0}&jobid={1}".format(token, jid)
            req = requests.get(url, verify=False)
            print(req.text)
            response = req.json()
            status = response['status']
            if status == 'ok':
                result = response
                break



        # if send_email:
        #     print('Sending email to %s' % email)
        #     run=dtasks.mkcut.apply_async(args=[filename, loc_user, loc_passw, job_dir, xs, ys, bands, jobid, noBlacklist, tiid, list_only], \
        #         task_id=tiid, link=dtasks.send_note.si(loc_user, tiid, toemail))
        # else:
        #     print('Not sending email')
        #     run=dtasks.mkcut.apply_async(args=[filename, loc_user, loc_passw, job_dir, xs, ys, bands, jobid, noBlacklist, tiid, list_only], \
        #         task_id=tiid)

        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
                     'epoch', '', '', '', -1])
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))
        con.close()

        print("Process id before forking: {}".format(os.getpid()))

        try:
            pid = os.fork()
        except OSError:
            exit("Could not create a child process")

        if pid == 0:
            with open('config/mysqlconfig.yaml', 'r') as cfile:
                conf = yaml.load(cfile)['mysql']
            con = mydb.connect(**conf)
            print("In the child process that has the PID {}".format(os.getpid()))
            while (1):
                sleep(1)
                # get job result
                url = "https://descut.cosmology.illinois.edu/api/jobs/?token={0}&jobid={1}".format(token, jid)
                req = requests.get(url, verify=False)
                print(req.text)
                response = req.json()
                msg = response['message']
                if msg.find("PENDING") != -1:
                    continue
                if msg.find("SUCCESS") != -1:
                    with con:
                        cur = con.cursor()
                        q = "UPDATE Jobs SET status='{0}' where job = '{1}'".format('SUCCESS', jid)
                        cur.execute(q)
                    break
                if msg.find("SUCCESS") == -1 and msg.find("PENDING") == -1:
                    with con:
                        cur = con.cursor()
                        q = "UPDATE Jobs SET status='{0}' where job = '{1}'".format('FAILED', jid)
                        cur.execute(q)
                    break
            print("Goodbye, this cruel world.")
            con.close()
            exit()

        print("In the parent process after forking the child {}".format(pid))
        # not wait for the child, go fly
        # finished = os.waitpid(0, 0)
        # print(finished)




        self.set_status(200)
        self.flush()
        self.finish()
