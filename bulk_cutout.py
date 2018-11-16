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

        print('**************')
        print(tiffs, 'make tiff')
        print(pngs, 'make png')
        print(fits, 'make fits')
        if fits:
            print(gband, rband, iband, zband, yband, 'bands for fits')
        print(rgb, 'make rgb')
        if rgb:
            print((';').join(rgb_values), 'rgb bands')
        print(xsize, ysize, 'sizes')
        print(return_list, 'return list of tiles with objects')
        print(send_email, 'send_email')
        print(email, 'email')
        print(name, 'name')
        print(stype, 'type')

        jobid = str(uuid.uuid4()).replace("-", "_")

        if xsize == 0.0:
            xsize = ''
        if ysize == 0.0:
            ysize = ''

        if stype == 'manualCoadds':
            values = self.get_argument('bc_coadds')
            filename = user_folder + jobid + '.csv'
            F = open(filename, 'w')
            F.write("COADD_OBJECT_ID\n")
            F.write(values)
            F.close()
        if stype == 'manualCoords':
            values = self.get_argument('bc_coords')
            filename = user_folder + jobid + '.csv'
            F = open(filename, 'w')
            F.write('RA,DEC\n')
            F.write(values)
            F.close()
        if stype == 'coaddfile':
            fileinfo = self.request.files['csvfile1'][0]
            fname = fileinfo['filename']
            extn = os.path.splitext(fname)[1]
            filename = user_folder + jobid + extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        if stype == 'coordfile':
            fileinfo = self.request.files['csvfile2'][0]
            fname = fileinfo['filename']
            extn = os.path.splitext(fname)[1]
            filename = user_folder + jobid + extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        print('**************')

        folder2 = user_folder + jobid + '/'
        os.system('mkdir -p '+folder2)
        now = datetime.datetime.now()
        input_csv = user_folder + jobid + '.csv'

        run = ea_tasks.bulktasks.apply_async(args=[input_csv, loc_user, lp.decode(), jobid, folder2, db, tiffs, pngs, fits, rgb, rgb_values, gband, rband, iband, zband, yband, xsize, ysize, return_list, send_email, email], retry=True, task_id=jobid, queue='bulk-queue')

        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)

        tup = tuple([loc_user, jobid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'), 'cutoutb', '', '', '', -1])

        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Jobs VALUES{0}".format(tup))
        con.close()
        self.set_status(200)
        self.flush()
        self.finish()
