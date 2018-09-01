import tornado.websocket
from Crypto.Cipher import AES
import datetime
import datetime as dt
import glob
import inspect
import json
import os
import subprocess
import time
import uuid
from time import sleep

import MySQLdb as mydb
import config.descut as des
import requests
import tornado.websocket
import yaml
import base64
from Crypto.Cipher import AES

import Settings


def __line__():
    return inspect.currentframe().f_back.f_lineno

def get_filesize(filename):
    size = os.path.getsize(filename)
    size = size * 1. / 1024.
    if size > 1024. * 1024:
        size = '%.2f GB' % (1. * size / 1024. / 1024)
    elif size > 1024.:
        size = '%.2f MB' % (1. * size / 1024.)
    else:
        size = '%.2f KB' % (size)
    return size


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


        # jobid = str(uuid.uuid4())
        #if name == '':
        #    name = jobid
        if stype == "manual":
            values = self.get_argument("values")
            print(values)
            filename = user_folder+'cutouts.csv'
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
            filename = user_folder + "cutouts" + extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        print('**************')
        now = datetime.datetime.now()
        input_csv = user_folder + "cutouts" + '.csv'

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

        # submit job
        t1 = time.time()
        req = requests.post('https://descut.cosmology.illinois.edu/api/jobs/', data=body, files=body_files, verify=False)

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

        # write running to the log
        target_folder = user_folder + jid + '/'
        if not os.path.exists(os.path.join(user_folder, jid)):
            os.mkdir(os.path.join(user_folder, jid))
        jsonfile = os.path.join(target_folder, jid + ".json")
        js = open(jsonfile, "w")
        with open(target_folder + 'log.log', 'w') as logfile:
            logfile.write('Running...')
        cmd = "cp " + user_folder + "cutouts.csv " + user_folder + jid + ".csv"
        os.system(cmd)
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
        with open('config/desaccess.yaml', 'r') as cfile:
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
            with open('config/desaccess.yaml', 'r') as cfile:
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
            req = requests.get('https://descut.cosmology.illinois.edu/api/jobs/?token={0}&jobid={1}'.format(token, jid), verify=False)
            print(req.text)
            links = req.json()['links']
            l = links[0]
            print("==>> PATH", l)
            # if l.endswith('tar.gz'):
            # print(l)
            l = l.replace(os.path.basename(l), jid + ".tar.gz")
            print("==>> NEW L", l)
            r = requests.get(l, stream=True, verify=False)

            # writing json
            response = {}
            response['user'] = loc_user
            response['elapsed'] = 0
            response['jobid'] = jid
            response['files'] = None
            response['sizes'] = None
            response['email'] = 'no'

            if r.status_code == 200:
                fp = target_folder + jid + ".tar.gz"
                print("==>> FILEPATH", fp)
                f = open(fp, "wb")
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()
                # uncompress file
                os.chdir(target_folder)
                os.system("tar -zxf {}.tar.gz".format(jid))
                os.chdir(target_folder + "results/" + jid + "/")
                os.system("cp * ../../")
                os.chdir(target_folder)
                print(os.listdir(target_folder))
                os.system("rm -rf {}/results/".format(target_folder))
                os.chdir(os.path.dirname(__file__))
                if os.path.exists(target_folder + "list.json"):
                    os.remove(target_folder + "list.json")
                outfile = open(target_folder + "list.json", "w")


                if list_only:
                    json.dump('', outfile, indent=4)
                else:
                    tiffiles = glob.glob(target_folder + '*.tif')
                    titles = []
                    pngfiles = []
                    Ntiles = len(tiffiles)
                    for f in tiffiles:
                        title = f.split('/')[-1][:-4]
                        print(title)
                        # TODO: CANNOT RUN CONVERT IN MAC
                        subprocess.check_output(["convert %s %s.png" % (f, f)], shell=True)
                        titles.append(title)
                        pngfiles.append(target_folder + title + '.tif.png')
                    for ij in range(Ntiles):
                        pngfiles[ij] = pngfiles[ij][pngfiles[ij].find('/easyweb'):]
                    json.dump([dict(name=pngfiles[i], title=titles[i],
                                    size=Ntiles) for i in range(len(pngfiles))], outfile, indent=4)

                # writing files for wget
                allfiles = glob.glob(target_folder + '*.*')
                response['files'] = [os.path.basename(i) for i in allfiles]
                response['sizes'] = [get_filesize(i) for i in allfiles]
                Fall = open(target_folder + 'list_all.txt', 'w')
                prefix = 'URLPATH' + '/static'
                for ff in allfiles:
                    if (ff.find(jid + '.tar.gz') == -1 & ff.find('list.json') == -1):
                        Fall.write(prefix + ff.split('static')[-1] + '\n')
                Fall.close()
                response['status'] = 'ok'
                t2 = time.time()
                response['elapsed'] = t2 - t1
                json.dump(response, js)

            else:
                response['status'] = 'error'
                response['data'] = r.status_code
                response['kind'] = 'query'
                print("==>>", response)
                json.dump(response, js)



            print("Goodbye, this cruel world.")
            con.close()
            return response
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
        if bands == 'all':
            bands = ["g", "r", "i", "z", "Y"]
        else:
            bands = list(bands.split(" "))
        print("bands ==> ", bands)

        # jobid = str(uuid.uuid4())
        # if name == '':
        #    name = jobid
        if stype == "manual":
            values = self.get_argument("values")
            print(values)
            filename = user_folder + 'cutouts.csv'
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
            filename = user_folder + "cutouts" + extn
            with open(filename, 'w') as F:
                F.write(fileinfo['body'].decode('ascii'))
        print('**************')
        now = datetime.datetime.now()
        input_csv = user_folder + "cutouts" + '.csv'

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
            'band': str(bands),  # optional for 'single' epochs jobs (default: all bands)
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

        # submit job
        t1 = time.time()
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

        # write running to the log
        target_folder = user_folder + jid + '/'
        if not os.path.exists(os.path.join(user_folder, jid)):
            os.mkdir(os.path.join(target_folder))
        jsonfile = os.path.join(target_folder, jid + ".json")
        js = open(jsonfile, "w")
        with open(target_folder + 'log.log', 'w') as logfile:
            logfile.write('Running...')
        cmd = "cp " + user_folder + "cutouts.csv " + user_folder + jid + ".csv"
        os.system(cmd)
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

        # run = ea_tasks.desthumb.apply_async(args=[input_csv, loc_user, lp.decode(),
        #                                           folder2, xs, ys, jobid, list_only,
        #                                           send_email, email], retry=True, task_id=jobid)
        with open('config/desaccess.yaml', 'r') as cfile:
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
            with open('config/desaccess.yaml', 'r') as cfile:
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
            req = requests.get('https://descut.cosmology.illinois.edu/api/jobs/?token={0}&jobid={1}'.format(token, jid),
                verify=False)
            print(req.text)
            links = req.json()['links']
            l = links[0]
            idx = l.find("thumbs")
            l = l[: idx]
            l = l + jid + ".tar.gz"
            r = requests.get(l, stream=True, verify=False)

            # writing json
            response = {}
            response['user'] = loc_user
            response['elapsed'] = 0
            response['jobid'] = jid
            response['files'] = None
            response['sizes'] = None
            response['email'] = 'no'

            print("==>> REQUEST", r)

            if r.status_code == 200:
                fp = target_folder + jid + ".tar.gz"
                f = open(fp, "wb")
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
                        f.flush()
                # uncompress file
                os.chdir(target_folder)
                os.system("tar xf {0}.tar.gz -C {1} --strip-components=1".format(jid, target_folder))
                os.chdir(target_folder)
                script_dir = os.path.dirname(os.path.abspath(__file__))
                all_files = glob.glob('thumbs*/DESJ*')
                with open('list_all.txt', 'w') as list_output:
                    for file in all_files:
                        list_output.write(target_folder + file + '\n')

                os.chdir(script_dir)

                if os.path.exists(target_folder + "list.json"):
                     os.remove(target_folder + "list.json")
                outfile = open(target_folder + "list.json", "w")

                os.chdir(target_folder)

                if list_only:
                    json.dump('', outfile, indent=4)
                else:

                    # [{"RA": 0.0,
                    # "DEC": 0.0,
                    # XSIZE": 0.0,
                    # "YSIZE": 0.0,
                    # "demo_png": "thumbs_DESJ_000000.0+000000.0\/DESJ000000.0+000000.0_Y_20121109.png",
                    # "image_title": "thumbs_DESJ_000000.0+000000.0"}]


                    ls = os.listdir(target_folder)
                    image_title = []
                    demo_png = []
                    for l in ls:
                        if l.find("thumb") != -1:
                            image_title.append(l)

                    # print("==>> THUMB FILE: ", image_title)

                    for dir in image_title:
                        os.chdir(target_folder + dir + '/')
                        # print("==>>Grab demo", os.listdir(os.getcwd()))
                        fs = os.listdir(os.getcwd())
                        print(fs)
                        for p in fs:
                            if p.endswith('png'):
                                demo_png.append(p)
                                break

                    print("==>>DEMO_PNG", demo_png)
                    json.dump([dict(RA=ra[i], DEC=dec[i],
                                    XSIZE=xs, YSIZE=ys, demo_png=demo_png[i], image_title=image_title[i]) for i in range(len(image_title))], outfile, indent=4)

                # writing files for wget
                allfiles = glob.glob(target_folder + '*.*')
                response['files'] = [os.path.basename(i) for i in allfiles]
                response['sizes'] = [get_filesize(i) for i in allfiles]
                Fall = open(target_folder + 'list_all.txt', 'w')
                prefix = 'URLPATH' + '/static'
                for ff in allfiles:
                    if (ff.find(jid + '.tar.gz') == -1 & ff.find('list.json') == -1):
                        Fall.write(prefix + ff.split('static')[-1] + '\n')
                Fall.close()
                response['status'] = 'ok'
                t2 = time.time()
                response['elapsed'] = t2 - t1
                json.dump(response, js)

            else:
                response['status'] = 'error'
                response['data'] = r.status_code
                response['kind'] = 'query'
                print("==>>", response)
                json.dump(response, js)

            print("Goodbye, this cruel world.")
            con.close()
            return response
            exit()

        print("In the parent process after forking the child {}".format(pid))
        # not wait for the child, go fly
        # finished = os.waitpid(0, 0)
        # print(finished)

        self.set_status(200)
        self.flush()
        self.finish()
