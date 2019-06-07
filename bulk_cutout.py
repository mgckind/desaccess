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
        tiffs = self.get_argument("make_tiffs") == 'true'
        pngs = self.get_argument("make_pngs") == 'true'
        fits = self.get_argument("make_fits") == 'true'
        rgb = self.get_argument("make_rgb") == 'true'
        rgb_values = self.get_argument("rgb_values").split(' ')
        #rgb_minimum = float(self.get_argument("bc_rgb_minimum"))
        #rgb_stretch = float(self.get_argument("bc_rgb_stretch"))
        #rgb_asinh = float(self.get_argument("bc_rgb_asinh"))
        gband = self.get_argument("bc_gband") == 'true'
        rband = self.get_argument("bc_rband") == 'true'
        iband = self.get_argument("bc_iband") == 'true'
        zband = self.get_argument("bc_zband") == 'true'
        yband = self.get_argument("bc_Yband") == 'true'
        allbands = self.get_argument("bc_all_toggle") == 'true'
        xsize = float(self.get_argument("bc_xsize"))
        ysize = float(self.get_argument("bc_ysize"))
        return_list = self.get_argument("bc_returnList") == 'true'
        send_email = self.get_argument("bc_send_email") == 'true'
        email = self.get_argument("bc_email")
        name = self.get_argument("bc_name")
        stype = self.get_argument("bc_submit_type")

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

        jobid = str(uuid.uuid4()).replace("-", "_")
        app_log.info('***** JOB *****')
        app_log.info('Cutouts Job: {} by {}'.format(jobid, loc_user))
        app_log.info('{} make tiff'.format(tiffs))
        app_log.info('{} make png'.format(pngs))
        app_log.info('{} make fits'.format(fits))
        if fits:
            app_log.info(gband, rband, iband, zband, yband, 'bands for fits')
        app_log.info('{} make rgb'.format(rgb))
        if rgb:
            app_log.info((';').join(rgb_values) + ', rgb bands')
        app_log.info('{} , {} sizes'.format(xsize, ysize))
        app_log.info('{}, return list of tiles with objects'.format(return_list))
        app_log.info('{} send_email'.format(send_email))
        app_log.info('{} email'.format(email))
        app_log.info('{} : name'.format(name))
        app_log.info('{} type'.format(stype))


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
            values = self.get_argument('bc_positions')
            F = open(filename, 'w')
            F.write(values.upper())
            F.close()

        """
        if stype == 'manualCoadds':
            values = self.get_argument('bc_coadds')
            #filename = user_folder + jobid + '.csv'
            F = open(filename, 'w')
            F.write("COADD_OBJECT_ID\n")
            F.write(values)
            F.close()
        if stype == 'manualCoords':
            values = self.get_argument('bc_coords')
            #filename = user_folder + jobid + '.csv'
            F = open(filename, 'w')
            F.write('RA,DEC\n')
            F.write(values)
            F.close()
        if stype == 'coaddfile':
            fileinfo = self.request.files['csvfile1'][0]
            #fname = fileinfo['filename']
            #extn = os.path.splitext(fname)[1]
            #filename = user_folder + jobid + extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        if stype == 'coordfile':
            fileinfo = self.request.files['csvfile2'][0]
            #fname = fileinfo['filename']
            #extn = os.path.splitext(fname)[1]
            #filename = user_folder + jobid + extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        """
        app_log.info('**************')

        folder2 = user_folder + jobid + '/'
        os.system('mkdir -p '+folder2)
        now = datetime.datetime.now()
        input_csv = user_folder + jobid + '.csv'

        SMALL_QUEUE = 300
        MEDIUM_QUEUE = 1000
        LARGE_QUEUE = 10000
        SMALL_QUEUE_MAX_CPUS = 1
        MEDIUM_QUEUE_MAX_CPUS = 4
        LARGE_QUEUE_MAX_CPUS = 6
        job_size = ''

        dftemp = pd.DataFrame(pd.read_csv(input_csv))
        dftemp_rows = len(dftemp.index)

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

        run = ea_tasks.bulktasks.apply_async(args=[job_size, nprocs, input_csv, loc_user, lp.decode(), jobid, folder2, db, tiffs, pngs, fits, rgb, rgb_values, colors, xsize, ysize, return_list, send_email, email], retry=True, task_id=jobid, queue='bulk-queue')

        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jobid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'), 'coadd', '', '', '', -1])

        cur = con.cursor()
        cur.execute("INSERT INTO Jobs VALUES{0}".format(tup))
        con.commit()
        con.close()
        self.set_status(200)
        self.flush()
        self.finish()
