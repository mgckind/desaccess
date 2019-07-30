"""
Written by Landon Gelman for use by DES Data Management, 2017-2018.
"""

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
from Settings import app_log


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
        db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
        xs = float(self.get_argument("fc_xsize"))
        ys = float(self.get_argument("fc_ysize"))
        gband = self.get_argument("fc_gband") == 'true'
        rband = self.get_argument("fc_rband") == 'true'
        iband = self.get_argument("fc_iband") == 'true'
        zband = self.get_argument("fc_zband") == 'true'
        yband = self.get_argument("fc_yband") == 'true'
        allbands = self.get_argument("fc_all_toggle") == 'true'
        mag = float(self.get_argument("fc_mag"))
        return_cut = self.get_argument("return_cutout_png") == 'true'
        send_email = self.get_argument("fc_send_email") == 'true'
        email = self.get_argument("fc_email")
        name = self.get_argument("fc_name")
        stype = self.get_argument("fc_submit_type")

        #if return_cut:
        #    list_only = False
        #else:
        #    list_only = True

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

        jobid = str(uuid.uuid4()).replace("-","_")    #'57b54f4f-ab85-4e4e-b366-1557c4b3ca0b' #str(uuid.uuid4())
        app_log.info('***** JOB *****')
        app_log.info('Cutouts Job: {} by {}'.format(jobid, loc_user))
        app_log.info('{} {} sizes'.format(xs,ys))
        app_log.info(' {} {} {} {} {} bands'.format(gband, rband, iband, zband, yband))
        app_log.info('magnitude limit: {}'.format(mag))
        app_log.info('type {} '.format(stype))
        app_log.info('return_cut: {}'.format(return_cut))
        app_log.info('send_email: {}'.format(send_email))
        app_log.info('email: {}'.format(email))
        app_log.info('name: {}'.format(name))
        if xs == 0.0:
            xs = 1.0
        if ys == 0.0:
            ys = 1.0
        filename = user_folder + jobid + '.csv'
        if stype == "manual":
            values = self.get_argument("fc_values")
            F = open(filename, 'w')
            F.write(values.upper())
            F.close()
        if stype == "csvfile":
            fileinfo = self.request.files['csvfile'][0]
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
            app_log.info(fileinfo['content_type'])
        app_log.info('**************')
        folder2 = user_folder+jobid+'/'
        os.system('mkdir -p '+folder2)
        now = datetime.datetime.now()
        input_csv = user_folder + jobid + '.csv'

        run = ea_tasks.make_chart.apply_async(args=[input_csv, loc_user, lp.decode(), folder2, db, xs, ys, jobid, return_cut, send_email, email, colors, mag], retry=True, task_id=jobid)

        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jobid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
                     'finding chart', '', '', '', -1])

        cur = con.cursor()
        cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))
        con.commit()
        con.close()
        self.set_status(200)
        self.flush()
        self.finish()
