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
import pandas as pd


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
        user_folder = os.path.join(Settings.WORKDIR, loc_user) + '/'
        cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
        lp = base64.b64encode(cipher.encrypt(loc_passw.rjust(32)))
        db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')

        airmass = float(self.get_argument("se_airmass"))
        psffwhm = float(self.get_argument("se_psffwhm"))
        blacklist = self.get_argument("se_blacklist") == 'true'

        gband = self.get_argument("se_gband") == 'true'
        rband = self.get_argument("se_rband") == 'true'
        iband = self.get_argument("se_iband") == 'true'
        zband = self.get_argument("se_zband") == 'true'
        yband = self.get_argument("se_Yband") == 'true'
        allbands = self.get_argument("se_all_toggle") == 'true'

        xsize = float(self.get_argument("se_xsize"))
        ysize = float(self.get_argument("se_ysize"))

        return_list = self.get_argument("se_returnList") == 'true'
        send_email = self.get_argument("se_send_email") == 'true'
        email = self.get_argument("se_email")
        name = self.get_argument("se_name")
        stype = self.get_argument("se_submit_type")

        if allbands:
            gband = True
            rband = True
            iband = True
            zband = True
            yband = True
        colors = ''
        if gband:
            colors = (',').join((colors, 'g'))
        if rband:
            colors = (',').join((colors, 'r'))
        if iband:
            colors = (',').join((colors, 'i'))
        if zband:
            colors = (',').join((colors, 'z'))
        if yband:
            colors = (',').join((colors, 'y'))
        colors = colors.strip(',')

        print('**************')
        print(airmass, 'airmass')
        print(psffwhm, 'psf_fwhm')
        print(gband, rband, iband, zband, yband, 'bands')
        print(xsize, ysize, 'sizes')
        print(return_list, 'return list of tiles with objects')
        print(send_email, 'send_email')
        print(email, 'email')
        print(name, 'name')
        print(stype, 'type')

        jobid = str(uuid.uuid4()).replace("-", "_")

        if xsize == 0.0:
            xsize = 1.0
        if ysize == 0.0:
            ysize = 1.0

        filename = user_folder + jobid + '.csv'
        if stype == 'csvfileAB':
            fileinfo = self.request.files['csvfile'][0]
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        if stype == 'manualAB':
            values = self.get_argument('se_positions')
            F = open(filename, 'w')
            F.write(values)
            F.close()
        print('**************')

        folder2 = user_folder + jobid + '/'
        os.system('mkdir -p ' + folder2)
        now = datetime.datetime.now()
        input_csv = user_folder + jobid + '.csv'

        run = ea_tasks.epochtasks.apply_async(args=[input_csv, loc_user, lp.decode(),
            jobid, folder2, db, airmass, psffwhm, blacklist, colors, xsize, ysize, return_list,
            send_email, email], retry=True, task_id=jobid)

        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jobid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'), 'single epoch', '', '', '', -1])

        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Hobs VALUES{0}".format(tup))
        con.close()
        self.set_status(200)
        self.flush()
        self.finish()