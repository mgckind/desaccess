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

        stype = self.get_argument("submit_type")
        print('**************')
        print(xs, ys, 'sizes')
        print(stype, 'type')
        print(list_only, 'list_only')
        print(send_email, 'send_email')
        print(email, 'email')
        print(name, 'name')
        jobid = str(uuid.uuid4())
        #if name == '':
        #    name = jobid
        if xs == 0.0:
            xs = ''
        if ys == 0.0:
            ys = ''
        if stype == "manual":
            # read in values and seperate ra, dec
            values = self.get_argument("values")
            print(values)

            ra_dec_pairs = value.split('\n')
            ra = []
            dec = []
            for i in range(len(ra_dec_pairs)):
                tup = ra_dec_pairs[i].split(',')
                ra.append(tup[0])
                dec.append(tup[1])

            # create body of request
            body = {
                'token': 'aaa...',  # TODO: what does token mean required
                'ra': str(ra),  # required
                'dec': str(dec),  # required
                'job_type': 'coadd',  # required 'coadd' or 'single'
                'xsize': str(xs),  # optional (default : 1.0)
                'ysize': str(ys),  # optional (default : 1.0)
                'tag': 'Y3A1_COADD',
                'band': 'g,r,i',  # optional for 'single' epochs jobs (default: all bands)
                'no_blacklist': 'false',
                'list_only': 'false',  # optional (default : 'false') 'true': will not generate pngs (faster)
                'email': email  # optional will send email when job is finished
            }

            # create body for files if needed
            req = requests.post('https://descut.cosmology.illinois.edu/api/jobs/', data=body)
            print(req)
            print(req.text)
            print(req.json()['job'])

            # TODO: Do we need to keep this?
            filename = user_folder+jobid+'.csv'
            F = open(filename, 'w')
            F.write("RA,DEC\n")
            F.write(values)
            F.close()

        if stype == "csvfile":
            # Do we need to keep this?
            fileinfo = self.request.files["csvfile"][0]
            fname = fileinfo['filename']
            extn = os.path.splitext(fname)[1]
            print(fname)
            print(fileinfo['content_type'])
            filename = user_folder+jobid+extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))

            # TODO: Do we need to parse file to get the ra, dec list
            body = {
                'token': 'aaa...',  # required
                'ra': str(ra),  # required
                'dec': str(dec),  # required
                'job_type': 'coadd',  # required 'coadd' or 'single'
                'xsize': str(xs),  # optional (default : 1.0)
                'ysize': str(ys),  # optional (default : 1.0)
                'tag': 'Y3A1_COADD',
            # optional for 'coadd' jobs (default: Y3A1_COADD, see Coadd Help page for more options)
                'band': 'g,r,i',  # optional for 'single' epochs jobs (default: all bands)
                'no_blacklist': 'false',
            # optional for 'single' epochs jobs (default: 'false'). return or not blacklisted exposures
                'list_only': 'false',  # optional (default : 'false') 'true': will not generate pngs (faster)
                'email': email # optional will send email when job is finished
            }
            # create body for files if needed
            body_files = {'csvfile': open(fname, 'rb')}  # To load csv file as part of request
            # To include files
            req = requests.post('https://descut.cosmology.illinois.edu/api/jobs/', data=body, files=body_files)






        print('**************')
        folder2 = user_folder+jobid+'/'
        os.system('mkdir -p '+folder2)
        now = datetime.datetime.now()
        input_csv = user_folder + jobid + '.csv' # TODO: What does import csv mean?
        # run = ea_tasks.desthumb.apply_async(args=[input_csv, loc_user, lp.decode(),
                                                  folder2, xs, ys, jobid, list_only,
                                                  send_email, email], retry=True, task_id=jobid)




        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jobid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
                     'cutout', '', '', '', -1])
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))
        con.close()
        self.set_status(200)
        self.flush()
        self.finish()
