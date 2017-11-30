from celery import Celery, Task
import easyaccess as ea
import requests
from Crypto.Cipher import AES
import base64
import Settings
import os
import threading
import time
import json
#import MySQLdb as mydb
import yaml
import time
import subprocess
from celery.exceptions import SoftTimeLimitExceeded
import glob
from smtp import email_utils

app = Celery('ea_tasks')
app.config_from_object('config.celeryconfig')


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


class CustomTask(Task):

    abstract = None

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        url = 'http://localhost:8080/easyweb/pusher/'
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        q0 = "UPDATE Jobs SET status='{0}' where job = '{1}'".format('REVOKE', task_id)
        cur = con.cursor()
        print('FAILED')
        print(exc)
        print(einfo)
        cur.execute(q0)
        con.commit()
        con.close()
        requests.post(url, data={'jobid': task_id}, verify=False)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print('Done?')
        try:
            test = retval['status']
        except:
            return
        url = 'http://localhost:8080/easyweb/pusher/'
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        file_list = json.dumps(retval['files'])
        size_list = json.dumps(retval['sizes'])
        if retval['status'] == 'ok':
            temp_status = 'SUCCESS'
            if retval['email'] != 'no':
                user = retval['user']
                email = retval['email']
                print('SEND EMAIL TO: ', email)
                email_utils.send_note(user, task_id, email)
            else:
                print('NO EMAIL')

        else:
            temp_status = 'FAIL'
        q0 = "UPDATE Jobs SET status='{0}' where job = '{1}'".format(temp_status, task_id)
        q1 = "UPDATE Jobs SET files='{0}' where job = '{1}'".format(file_list, task_id)
        q2 = "UPDATE Jobs SET sizes='{0}' where job = '{1}'".format(size_list, task_id)
        cur = con.cursor()
        cur.execute(q0)
        if retval['files'] is not None:
            cur.execute(q1)
            cur.execute(q2)
        con.commit()
        con.close()
        requests.post(url, data=retval, verify=False)


def check_query(query, db, username, lp):
    response = {}
    response['user'] = username
    cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
    dlp = cipher.decrypt(base64.b64decode(lp)).strip()
    try:
        connection = ea.connect(db, user=username, passwd=dlp.decode())
        cursor = connection.cursor()
    except Exception as e:
        response['status'] = 'error'
        response['data'] = str(e).strip()
        response['kind'] = 'check'
        return response
    try:
        cursor.parse(query.encode())
        response['status'] = 'ok'
        response['data'] = 'Ok!'
        response['kind'] = 'check'
    except Exception as e:
        response['status'] = 'error'
        response['data'] = str(e).strip()
        response['kind'] = 'check'
    cursor.close()
    connection.close()
    return response


@app.task(base=CustomTask)
def run_query(query, filename, db, username, lp, jid, timeout=None):
    """
    Run a query

    Parameters
    ----------
    query : str
    filename : str
        None if not output filename
    db : database
    username : username
    lp : encypted password
    jid: Job (Task) id
    timeout: int, optional
        Timeout in seconds

    Returns
    -------
    dict
        response dictionary with following keys:
        - user    : username
        - elapsed : time in seconds
        - status  : 'ok'/'error'
        - data    : json array of data or message
        - kind    : 'query' (any no select statement) / 'select' (select statement)
        - jobid   : Job id
        - files   : list of created files
        - sizes   : list of  sizes of created filenames

    """
    response = {}
    response['user'] = username
    response['elapsed'] = 0
    response['jobid'] = jid
    response['files'] = None
    response['sizes'] = None
    response['email'] = 'no'
    user_folder = os.path.join(Settings.WORKDIR, username)+'/'
    if filename is not None:
        if not os.path.exists(os.path.join(user_folder, jid)):
            os.mkdir(os.path.join(user_folder, jid))
    jsonfile = os.path.join(user_folder, jid+'.json')
    cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
    dlp = cipher.decrypt(base64.b64decode(lp)).strip()
    try:
        connection = ea.connect(db, user=username, passwd=dlp.decode())
        cursor = connection.cursor()
    except Exception as e:
            response['status'] = 'error'
            response['data'] = str(e).strip()
            response['kind'] = 'query'
            with open(jsonfile, 'w') as fp:
                json.dump(response, fp)
            return response
    if timeout is not None:
        tt = threading.Timer(timeout, connection.con.cancel)
        tt.start()
    t1 = time.time()
    if query.lower().lstrip().startswith('select'):
        response['kind'] = 'select'
        try:
            if filename is not None:
                outfile = os.path.join(user_folder, jid, filename)
                connection.query_and_save(query, outfile)
                if timeout is not None:
                    tt.cancel()
                t2 = time.time()
                job_folder = os.path.join(user_folder, jid)+'/'
                files = glob.glob(job_folder+'*')
                response['files'] = [os.path.basename(i) for i in files]
                response['sizes'] = [get_filesize(i) for i in files]
                data = 'Job {0} done'.format(jid)
                response['kind'] = 'query'
            else:
                df = connection.query_to_pandas(query)
                if timeout is not None:
                    tt.cancel()
                data = df.to_json(orient='records')
                t2 = time.time()
            response['status'] = 'ok'
            response['data'] = data
        except Exception as e:
            if timeout is not None:
                tt.cancel()
            t2 = time.time()
            response['status'] = 'error'
            response['data'] = str(e).strip()
            response['kind'] = 'query'
    else:
        response['kind'] = 'query'
        try:
            df = cursor.execute(query)
            connection.con.commit()
            if timeout is not None:
                tt.cancel()
            t2 = time.time()
            response['status'] = 'ok'
            response['data'] = 'Done!'
        except Exception as e:
            if timeout is not None:
                tt.cancel()
            t2 = time.time()
            response['status'] = 'error'
            response['data'] = str(e).strip()

    response['elapsed'] = t2 - t1
    with open(jsonfile, 'w') as fp:
        json.dump(response, fp)
    cursor.close()
    connection.close()
    return response


#@app.task(base=CustomTask, soft_time_limit=10, time_limit=20)
@app.task(base=CustomTask)
def desthumb(inputs, uu, pp, outputs, xs, ys, jobid, listonly, send_email, email):
    response = {}
    response['user'] = uu
    response['elapsed'] = 0
    response['jobid'] = jobid
    response['files'] = None
    response['sizes'] = None
    response['email'] = 'no'
    if send_email:
        response['email'] = email
    t1 = time.time()
    cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
    dlp = cipher.decrypt(base64.b64decode(pp)).strip()
    pp = dlp.decode()
    user_folder = Settings.WORKDIR+uu+"/"
    jsonfile = user_folder+jobid+'.json'
    mypath = user_folder+jobid+'/'
    with open(mypath+'log.log', 'w') as logfile:
        logfile.write('Running...')
    uu = 'demo_user'
    pp = 'demo_pass'
    com = "makeDESthumbs {0} --user {1} --password {2} --MP --outdir={3}".format(inputs, uu, pp,
                                                                                 outputs)
    if xs != "":
        com += ' --xsize %s ' % xs
    if ys != "":
        com += ' --ysize %s ' % ys
    com += " --logfile %s" % (outputs + 'log.log')
    com += " --tag Y3A1_COADD"
    # print(com)
    # time.sleep(40)
    os.chdir(mypath)
    oo = subprocess.check_output([com], shell=True)
    if listonly:
        if os.path.exists(mypath+"list.json"):
            os.remove(mypath+"list.json")
        with open(mypath+"list.json", "w") as outfile:
            json.dump('', outfile, indent=4)
    else:
        tiffiles = glob.glob(mypath+'*.tif')
        titles = []
        pngfiles = []
        Ntiles = len(tiffiles)
        for f in tiffiles:
            title = f.split('/')[-1][:-4]
            subprocess.check_output(["convert %s %s.png" % (f, f)], shell=True)
            titles.append(title)
            pngfiles.append(mypath+title+'.tif.png')

        for ij in range(Ntiles):
            pngfiles[ij] = pngfiles[ij][pngfiles[ij].find('/easyweb'):]
        os.chdir(user_folder)
        os.system("tar -zcf {0}/{0}.tar.gz {0}/".format(jobid))
        os.chdir(os.path.dirname(__file__))
        if os.path.exists(mypath+"list.json"):
            os.remove(mypath+"list.json")
        with open(mypath+"list.json", "w") as outfile:
            json.dump([dict(name=pngfiles[i], title=titles[i],
                            size=Ntiles) for i in range(len(pngfiles))], outfile, indent=4)

    # writing files for wget
    allfiles = glob.glob(mypath+'*.*')
    response['files'] = [os.path.basename(i) for i in allfiles]
    response['sizes'] = [get_filesize(i) for i in allfiles]
    Fall = open(mypath+'list_all.txt', 'w')
    prefix = 'URLPATH'+'/static'
    for ff in allfiles:
        if (ff.find(jobid+'.tar.gz') == -1 & ff.find('list.json') == -1):
            Fall.write(prefix+ff.split('static')[-1]+'\n')
    Fall.close()
    response['status'] = 'ok'
    t2 = time.time()
    response['elapsed'] = t2-t1
    with open(jsonfile, 'w') as fp:
        json.dump(response, fp)
    return response
