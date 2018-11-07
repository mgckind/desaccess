from celery import Celery, Task
from celery.result import AsyncResult, allow_join_result
import easyaccess as ea
import requests
from Crypto.Cipher import AES
import base64
import Settings
import plotutils
import bulkthumbs
import os
import threading
import time
import json
import MySQLdb as mydb
import yaml
import time
import subprocess
from celery.exceptions import SoftTimeLimitExceeded
import glob
from smtp import email_utils
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from astropy.io import fits
from astropy.wcs import WCS, _wcs

app = Celery('ea_tasks')
app.config_from_object('config.celeryconfig')
app.conf.broker_transport_options = {'visibility_timeout': 3600}

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
    #acks_late = True
    #reject_on_worker_lost = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        url = 'http://localhost:8080/easyweb/pusher/'
        with open('config/desaccess.yaml', 'r') as cfile:
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
        #requests.post(url, data={'jobid': task_id}, verify=False)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        print(einfo)
        print(status)
        print('Done?')
        failed_job = False
        try:
            test = retval['status']
        except:
            return
        url = 'http://localhost:8080/easyweb/pusher/'
        with open('config/desaccess.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        cur = con.cursor()
        cur.execute("SELECT status,name from Jobs where job = '{}'".format(task_id))
        cc = cur.fetchone()
        statusjob = cc[0]
        print(statusjob)
        namejob = cc[1]
        if namejob == '':
            namejob = task_id
        file_list = json.dumps(retval['files'])
        size_list = json.dumps(retval['sizes'])
        elapsed = int(retval['elapsed'])
        if retval['status'] == 'ok':
            if statusjob == 'REVOKE':
                temp_status = 'REVOKE'
                failed_job = True
            else:
                temp_status = 'SUCCESS'
            if retval['email'] != 'no':
                user = retval['user']
                email = retval['email']
                print('SEND EMAIL TO: ', email)
                print(namejob)
                try:
                    if failed_job:
                        email_utils.send_fail(user, namejob, email)
                    else:
                        email_utils.send_note(user, namejob, email)
                except Exception as e:
                    print(str(e).strip())
            else:
                print('NO EMAIL')

        else:
            temp_status = 'FAIL'
        q0 = "UPDATE Jobs SET status='{0}' where job = '{1}'".format(temp_status, task_id)
        q1 = "UPDATE Jobs SET files='{0}' where job = '{1}'".format(file_list, task_id)
        q2 = "UPDATE Jobs SET sizes='{0}' where job = '{1}'".format(size_list, task_id)
        q3 = "UPDATE Jobs SET runtime='{0}' where job = '{1}'".format(elapsed, task_id)
        cur.execute(q0)
        cur.execute(q3)
        print(elapsed)
        if retval['files'] is not None:
            cur.execute(q1)
            cur.execute(q2)
        con.commit()
        con.close()
        #requests.post(url, data=retval, verify=False)


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


@app.task(ignore_result=True)
def error_handler(uuid):
    result = AsyncResult(uuid)
    with allow_join_result():
        exc = result.get(propagate=False)
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(
          uuid, exc, result.traceback))


#@app.task(base=CustomTask)
@app.task(base=CustomTask, soft_time_limit=3600*23, time_limit=3600*24)
def run_query(query, filename, db, username, lp, jid, email, compression, timeout=None):
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
    try:
        t1 = time.time()
        response = {}
        response['user'] = username
        response['elapsed'] = 0
        response['jobid'] = jid
        response['files'] = None
        response['sizes'] = None
        response['email'] = 'no'
        if email != "":
            response['email'] = email
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
                print(str(e).strip())
                response['status'] = 'error'
                response['data'] = str(e).strip()
                response['kind'] = 'query'
                with open(jsonfile, 'w') as fp:
                    json.dump(response, fp)
                return response
        if timeout is not None:
            tt = threading.Timer(timeout, connection.con.cancel)
            tt.start()
        if query.lower().lstrip().startswith('select'):
            response['kind'] = 'select'
            try:
                if filename is not None:
                    outfile = os.path.join(user_folder, jid, filename)
                    if compression:
                        connection.compression = True
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
                    df.to_csv(os.path.join(user_folder, 'quickResults.csv'), index=False)
                    t2 = time.time()
                response['status'] = 'ok'
                response['data'] = data
            except Exception as e:
                if timeout is not None:
                    tt.cancel()
                t2 = time.time()
                print('query job finished')
                print(str(e).strip())
                response['status'] = 'error'
                response['data'] = str(e).strip()
                response['kind'] = 'query'
                raise
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
    except Exception as e:
        print(str(e).strip())
        print('Exiting')
        raise

def notify(jobid):
    print('*****')
    url = Settings.ROOT_URL+'/easyweb/pusher/'
    resp = {}
    resp['status'] = 'error'
    resp['data'] = 'Time Exceeded (30 sec)'
    resp['kind'] = 'query'
    resp['jobid'] = jobid
    resp['stopJob'] = 'yes'
    #requests.post(url, data=resp, verify=False)


def run_quick(query, db, username, lp):
    response = {}
    response['user'] = username
    response['elapsed'] = 0
    try:
        user_folder = os.path.join(Settings.WORKDIR, username)+'/'
        cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
        dlp = cipher.decrypt(base64.b64decode(lp)).strip()
        connection = ea.connect(db, user=username, passwd=dlp.decode())
        cursor = connection.cursor()
        tt = threading.Timer(25, connection.con.cancel)
        tt.start()
        if query.lower().lstrip().startswith('select'):
            response['kind'] = 'select'
            try:
                df = connection.query_to_pandas(query)
                df.to_csv(os.path.join(user_folder, 'quickResults.csv'), index=False)
                df = df[0:1000]
                data = df.to_json(orient='records')
                response['status'] = 'ok'
                response['data'] = data
            except Exception as e:
                print('query job finished')
                print(str(e).strip())
                response['status'] = 'error'
                err_out = str(e).strip()
                if 'ORA-01013' in err_out:
                    err_out = 'Time Exceeded (30 seconds). Please try submitting the job'
                response['data'] = err_out
                response['kind'] = 'query'
        else:
            response['kind'] = 'query'
            try:
                df = cursor.execute(query)
                connection.con.commit()
                response['status'] = 'ok'
                response['data'] = 'Done! (See results below)'
            except Exception as e:
                response['status'] = 'error'
                err_out = str(e).strip()
                if 'ORA-01013' in err_out:
                    err_out = 'Time Exceeded (30 seconds). Please try submitting this query as job'
                response['data'] = err_out
        cursor.close()
        connection.close()
    except Exception as e:
        response['status'] = 'error'
        response['data'] = str(e).strip()
        response['kind'] = 'query'
    tt.cancel()
    return response


#@app.task(base=CustomTask, soft_time_limit=10, time_limit=20)
#@app.task(base=CustomTask)
@app.task(base=CustomTask, soft_time_limit=3600*2, time_limit=3600*4)
def desthumb(inputs, uu, pp, outputs, xs, ys, jobid, listonly, send_email, email):
    with open('config/desaccess.yaml', 'r') as cfile:
        conf = yaml.load(cfile)['descut']
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
    uu = conf['username']
    pp = conf['password']
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
        os.chdir(user_folder)
        os.system("tar -zcf {0}/{0}.tar.gz {0}/".format(jobid))
        os.chdir(os.path.dirname(__file__))
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

"""
run_vistools() and make_chart() written by Landon Gelman for use by DES Data Management, 2017-2018.
"""

ARCSEC_TO_DEG = 0.000278

@app.task(base=CustomTask, soft_time_limit=3600*2, time_limit=3600*4)
def run_vistools(intype, inputs, uu, pp, outputs, db, boxsize, fluxwav, magwav, grri, gzzw1, spreadmag, addwise, addvhs, jobid, send_email, email):
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

    user_folder = Settings.WORKDIR + uu + "/"
    jsonfile = user_folder + jobid + '.json'
    mypath = user_folder + jobid + '/'

    input_df = pd.DataFrame(pd.read_csv(inputs, sep=','))

    logname = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    logfile = open(mypath + 'vistoolsLOG_' + logname + '.log', 'w')
    logfile.write('Selected Options:\n')
    logfile.write('    Flux vs. Wavelength: ........ ' + str(fluxwav) + '\n')
    logfile.write('    Magnitude vs. Wavelength: ... ' + str(magwav) + '\n')
    logfile.write('    Include WISE data: .......... ' + str(addwise) + '\n')
    logfile.write('    Include VHS data: ........... ' + str(addvhs) + '\n')
    logfile.write('    G-R vs. R-I: ................ ' + str(grri) + '\n')
    logfile.write('    G-Z vs. Z-W1: ............... ' + str(gzzw1) + '\n')
    logfile.write('    Spread vs. Magnitude: ....... ' + str(spreadmag) + '\n')

    #boxsize = (boxsize / 2 * ARCSEC_TO_DEG)
    numPlots = 0
    exten = '.png'

    start_time = time.time()

    if intype == 'coords':
        if len(input_df['RA']) != len(input_df['DEC']):
            logfile.write('ERROR - Please enter the same number of RA and DEC values.\n')
            response['status'] = 'error'
            with open(jsonfile, 'w') as fp:
                json.dump(response, fp)
            return response

    conn = ea.connect(db, user=uu, passwd=pp)
    curs = conn.cursor()

    for row in range(len(input_df[input_df.columns[0]])):
        FluxVsWavelength = fluxwav
        MagVsWavelength = magwav
        GR_RI = grri
        GZ_ZW1 = gzzw1
        SpreadVsMag = spreadmag
        wise = None
        vhs = None

        COADDID = None
        RAUSER, RAMIN, RAMAX = None, None, None
        DECUSER, DECMIN, DECMAX = None, None, None

        if intype == 'coadds':
            try:
                a = int(input_df['COADDID'][row])
            except (TypeError, ValueError):
                logfile.write('****************************************\n')
                logfile.write('ERROR - {0} is not a valid coadd ID.'.format(input_df['COADDID'][row]))
            else:
                COADDID = str(input_df['COADDID'][row])
                logfile.write('****************************************\n')
                logfile.write('Object: {0}'.format(COADDID))

        if intype == 'coords':
            try:
                a = float(input_df['RA'][row])
                b = float(input_df['DEC'][row])
            except (TypeError, ValueError):
                logfile.write('****************************************\n')
                logfile.write('ERROR - RA and DEC must be in decimal degrees.\n')
            else:
                RAUSER = float(input_df['RA'][row])
                RAMIN = str(RAUSER - (boxsize / 2 * ARCSEC_TO_DEG))
                RAMAX = str(RAUSER + (boxsize / 2 * ARCSEC_TO_DEG))
                RAUSER = str(RAUSER)
                DECUSER = float(input_df['DEC'][row])
                DECMIN = str(DECUSER - (boxsize / 2 * ARCSEC_TO_DEG))
                DECMAX = str(DECUSER + (boxsize / 2 * ARCSEC_TO_DEG))
                DECUSER = str(DECUSER)
                logfile.write('****************************************\n')
                logfile.write('User input: RA {0} DEC {1}\n'.format(RAUSER, DECUSER))

        df = None

        a = 'select * from (select y.COADD_OBJECT_ID, y.RA, y.DEC'
        b = ' from Y3A2_COADD_Object_SUMMARY y'
        c = ''
        d = ' where'
        e = ''
        f = ''

        a += ', y.MAG_AUTO_G, y.MAG_AUTO_R, y.MAG_AUTO_I, y.MAG_AUTO_Z, y.MAG_AUTO_Y, y.MAGERR_AUTO_G, y.MAGERR_AUTO_R, y.MAGERR_AUTO_I, y.MAGERR_AUTO_Z, y.MAGERR_AUTO_Y'

        a += ', y.WAVG_MAG_PSF_G, y.WAVG_MAG_PSF_R, y.WAVG_MAG_PSF_I, y.WAVG_MAG_PSF_Z, y.WAVG_MAG_PSF_Y, y.WAVG_MAGERR_PSF_G, y.WAVG_MAGERR_PSF_R, y.WAVG_MAGERR_PSF_I, y.WAVG_MAGERR_PSF_Z, y.WAVG_MAGERR_PSF_Y'

        if intype == 'coadds':
            d += ' y.COADD_OBJECT_ID = ' + COADDID + ''
            e += ')'
        if intype == 'coords':
            d += ' y.RA between ' + RAMIN + ' and ' + RAMAX + ' and y.DEC between ' + DECMIN + ' and ' + DECMAX + ' and y.MAG_AUTO_I between 10.0 and 30.0'
            e += ' order by abs('+RAUSER+' - y.RA)+abs('+DECUSER+' - y.DEC) asc)'
            f += ' where rownum = 1'

            if FluxVsWavelength or MagVsWavelength:
                #d += '  and abs(y.MAG_AUTO_G) != 99.000000 and abs(y.MAG_AUTO_R) != 99.000000 and abs(y.MAG_AUTO_I) != 99.000000 and abs(y.MAG_AUTO_Z) != 99.000000 and abs(y.MAG_AUTO_Y) != 99.000000'
                d += ' and ((y.MAG_AUTO_I >= 21.000000 and (abs(y.MAG_AUTO_G) != 99.000000 and abs(y.MAG_AUTO_R) != 99.000000 and abs(y.MAG_AUTO_Z) != 99.000000 and abs(y.MAG_AUTO_Y) != 99.000000)) or (y.MAG_AUTO_I < 21.000000 and (abs(y.WAVG_MAG_PSF_G) != 99.000000 and abs(y.WAVG_MAG_PSF_R) != 99.000000 and abs(y.WAVG_MAG_PSF_I) != 99.000000 and abs(y.WAVG_MAG_PSF_Z) != 99.000000 and abs(y.WAVG_MAG_PSF_Y) != 99.000000)))'

            elif GR_RI or GZ_ZW1:
                #d += ' and abs(y.MAG_AUTO_G) != 99.000000'
                d += ' and ((y.MAG_AUTO_I >= 21.000000 and abs(y.MAG_AUTO_G) != 99.000000) or (y.MAG_AUTO_I < 21.000000 and abs(y.WAVG_MAG_PSF_G) != 99.000000))'

                if GR_RI:
                    #d += ' and abs(y.MAG_AUTO_R) != 99.000000 and abs(y.MAG_AUTO_I) != 99.000000'
                    d += ' and ((y.MAG_AUTO_I >= 21.000000 and abs(y.MAG_AUTO_R) != 99.000000) or (y.MAG_AUTO_I < 21.000000 and (abs(y.WAVG_MAG_PSF_R) != 99.000000 and abs(y.WAVG_MAG_PSF_I) != 99.000000)))'
                if GZ_ZW1:
                    #d += ' and abs(y.MAG_AUTO_Z) != 99.000000'
                    d += ' and ((y.MAG_AUTO_I >= 21.000000 and abs(y.MAG_AUTO_Z) != 99.000000) or (y.MAG_AUTO_I < 21.000000 and abs(y.WAVG_MAG_PSF_Z) != 99.000000))'
                    if SpreadVsMag:
                        #d += ' and abs(y.MAG_AUTO_I) != 99.000000'
                        d += ' and (y.MAG_AUTO_I >= 21.000000 or (y.MAG_AUTO_I < 21.000000 and abs(y.WAVG_MAG_PSF_I) != 99.000000))'
            elif SpreadVsMag:
                #d += ' and abs(y.MAG_AUTO_I) != 99.000000'
                d += ' and (y.MAG_AUTO_I >= 21.000000 or (y.MAG_AUTO_I < 21.000000 and abs(y.WAVG_MAG_PSF_I) != 99.000000))'

        if addwise or gzzw1:
            a += ', w.W1MPRO, w.W2MPRO, w.W3MPRO, w.W4MPRO, w.W1SIGMPRO, w.W2SIGMPRO, w.W3SIGMPRO, w.W4SIGMPRO, w.W1SNR, w.W2SNR, w.W3SNR, w.W4SNR'
            c += ' left outer join WISE_DES w on y.COADD_OBJECT_ID = w.COADD_OBJECT_ID'

        if addvhs:
            a += ', v.JAPERMAG3, v.HAPERMAG3, v.KSAPERMAG3, v.JAPERMAG3ERR, v.HAPERMAG3ERR, v.KSAPERMAG3ERR'
            c += ' left outer join VHS_DES v on y.COADD_OBJECT_ID = v.COADD_OBJECT_ID'
        if SpreadVsMag:
            a += ', y.SPREAD_MODEL_G, y.SPREAD_MODEL_R, y.SPREAD_MODEL_I'

        query = a + b + c + d + e + f + ';'
        df = conn.query_to_pandas(query)

        logfile.write('Below is the query used to match object:\n' + query + '\n')

        ### "For python 3.6 and above, the columns are inserted in the order of **kwargs. For python 3.5 and earlier, since **kwargs is unordered, the columns are inserted in alphabetical order at the end of your DataFrame. Assigning multiple columns within the same assign is possible, but you cannot reference other columns created within the same assign call." - Pandas 0.22.0 documentation for pandas.DataFrame.assign ###

        if df.empty:
            if intype == 'coadds':
                logfile.write('WARNING - No object found with Coadd ID ' + COADDID + '.\n')
            if intype == 'coords':
                logfile.write('WARNING - No object found near RA ' + RAUSER + ' DEC ' + DECUSER + ' with box size ' + str(boxsize) + '\".\n')
            FluxVsWavelength = False
            MagVsWavelength = False
            GR_RI = False
            GZ_ZW1 = False
            SpreadVsMag = False
        else:
            logfile.write('Below is the result of the query:\n' + df.to_string(columns=None, header=True, index=False, justify='left') + '\n')

            filenm = 'DESJ' + plotutils.DecConverter(df['RA'][0], df['DEC'][0])

            logfile.write('Closest object to your coordinates RA ' + str(df['RA'][0]) + ' DEC ' + str(df['DEC'][0]) + ' with Coadd ID ' + str(df['COADD_OBJECT_ID'][0]) + '.\n')

            # >=21 use detmodel; <21 use wavg
            if df['MAG_AUTO_I'][0] >= 21.0:
                logfile.write('The MAG_AUTO magnitude in the I band is greater than or equal to 21. The plots will be made using MAG_AUTO magnitudes.')
                df.drop(columns=[
                        'WAVG_MAG_PSF_G', 'WAVG_MAG_PSF_R', 'WAVG_MAG_PSF_I',
                        'WAVG_MAG_PSF_Z', 'WAVG_MAG_PSF_Y',
                        'WAVG_MAGERR_PSF_G', 'WAVG_MAGERR_PSF_R', 'WAVG_MAGERR_PSF_I',
                        'WAVG_MAGERR_PSF_Z', 'WAVG_MAGERR_PSF_Y'], inplace=True)
                df.rename(columns={
                        'MAG_AUTO_G':'MAG_G', 'MAG_AUTO_R':'MAG_R',
                        'MAG_AUTO_I':'MAG_I', 'MAG_AUTO_Z':'MAG_Z',
                        'MAG_AUTO_Y':'MAG_Y',
                        'MAGERR_AUTO_G':'MAGERR_G', 'MAGERR_AUTO_R':'MAGERR_R',
                        'MAGERR_AUTO_I':'MAGERR_I', 'MAGERR_AUTO_Z':'MAGERR_Z',
                        'MAGERR_AUTO_Y':'MAGERR_Y'}, inplace=True)
            else:
                logfile.write('The MAG_AUTO magnitude in the I band is less than 21. The plots will be made using WAVG_MAG_PSF magnitudes.')
                df.drop(columns=[
                        'MAG_AUTO_G', 'MAG_AUTO_R', 'MAG_AUTO_I',
                        'MAG_AUTO_Z', 'MAG_AUTO_Y',
                        'MAGERR_AUTO_G', 'MAGERR_AUTO_R', 'MAGERR_AUTO_I',
                        'MAGERR_AUTO_Z', 'MAGERR_AUTO_Y'], inplace=True)
                df.rename(columns={
                        'WAVG_MAG_PSF_G':'MAG_G', 'WAVG_MAG_PSF_R':'MAG_R',
                        'WAVG_MAG_PSF_I':'MAG_I', 'WAVG_MAG_PSF_Z':'MAG_Z',
                        'WAVG_MAG_PSF_Y':'MAG_Y',
                        'WAVG_MAGERR_PSF_G':'MAGERR_G', 'WAVG_MAGERR_PSF_R':'MAGERR_R',
                        'WAVG_MAGERR_PSF_I':'MAGERR_I', 'WAVG_MAGERR_PSF_Z':'MAGERR_Z',
                        'WAVG_MAGERR_PSF_Y':'MAGERR_Y'}, inplace=True)

            if fluxwav:
                df = df.assign(G_FLUX=3631*(10**(-df['MAG_G']/2.5))*1000)
                df = df.assign(R_FLUX=3631*(10**(-df['MAG_R']/2.5))*1000)
                df = df.assign(I_FLUX=3631*(10**(-df['MAG_I']/2.5))*1000)
                df = df.assign(Z_FLUX=3631*(10**(-df['MAG_Z']/2.5))*1000)
                df = df.assign(Y_FLUX=3631*(10**(-df['MAG_Y']/2.5))*1000)
                df = df.assign(G_FLUXERR=df['G_FLUX']*(1-(10**(-df['MAGERR_G']/2.5))))
                df = df.assign(R_FLUXERR=df['R_FLUX']*(1-(10**(-df['MAGERR_R']/2.5))))
                df = df.assign(I_FLUXERR=df['I_FLUX']*(1-(10**(-df['MAGERR_I']/2.5))))
                df = df.assign(Z_FLUXERR=df['G_FLUX']*(1-(10**(-df['MAGERR_Z']/2.5))))
                df = df.assign(Y_FLUXERR=df['Y_FLUX']*(1-(10**(-df['MAGERR_Y']/2.5))))
                logfile.write('The following DES flux data has been calculated:\n' + df.to_string(columns=['G_FLUX','R_FLUX','I_FLUX','Z_FLUX','Y_FLUX','G_FLUXERR','R_FLUXERR','I_FLUXERR','Z_FLUXERR','Y_FLUXERR'], header=True, index=False, justify='left'))

            if addwise:
                if df['W1MPRO'][0] is None:
                    wise = False
                else:
                    wise = True

                    df = df.assign(W1FLUX=309.540*(10**(-df['W1MPRO']/2.5))*1000)
                    df = df.assign(W2FLUX=171.787*(10**(-df['W2MPRO']/2.5))*1000)
                    df = df.assign(W3FLUX=31.674*(10**(-df['W3MPRO']/2.5))*1000)
                    df = df.assign(W4FLUX=8.363*(10**(-df['W4MPRO']/2.5))*1000)
                    df = df.assign(W1FLUXERR=df['W1FLUX']*(1-(10**(-df['W1SIGMPRO']/2.5))))
                    df = df.assign(W2FLUXERR=df['W2FLUX']*(1-(10**(-df['W2SIGMPRO']/2.5))))
                    df = df.assign(W3FLUXERR=df['W3FLUX']*(1-(10**(-df['W3SIGMPRO']/2.5))))
                    df = df.assign(W4FLUXERR=df['W4FLUX']*(1-(10**(-df['W4SIGMPRO']/2.5))))
                    logfile.write('The following WISE flux data has been calculated:\n' + df.to_string(columns=['W1FLUX','W2FLUX','W3FLUX','W4FLUX','W1FLUXERR','W2FLUXERR','W3FLUXERR','W4FLUXERR'], header=True, index=False, justify='left') + '\n')

            if addvhs:
                if df['JAPERMAG3'][0] is None:
                    vhs = False
                else:
                    vhs = True

                    df = df.assign(JFLUX=1600*(10**(-df['JAPERMAG3']/2.5))*1000)
                    df = df.assign(HFLUX=1020*(10**(-df['HAPERMAG3']/2.5))*1000)
                    df = df.assign(KSFLUX=666.7*(10**(-df['KSAPERMAG3']/2.5))*1000)
                    df = df.assign(JFLUXERR=df['JFLUX']*(1-(10**(-df['JAPERMAG3ERR']/2.5))))
                    df = df.assign(HFLUXERR=df['HFLUX']*(1-(10**(-df['HAPERMAG3ERR']/2.5))))
                    df = df.assign(KSFLUXERR=df['KSFLUX']*(1-(10**(-df['KSAPERMAG3ERR']/2.5))))
                    logfile.write('The following VHS flux data has been calculated:\n' + df.to_string(columns=['JFLUX','HFLUX','KSFLUX','JFLUXERR','HFLUXERR','KSFLUXERR'], header=True, index=False, justify='left') + '\n')

            if gzzw1:
                if df['W1MPRO'][0] is None:
                    GZ_ZW1 = False
                    wise = False
                else:
                    df = df.assign(Z_W1=df['MAG_Z']-df['W1MPRO'])
                    df = df.assign(G_Z=df['MAG_G']-df['MAG_Z'])
                    logfile.write('The following color data has been calculated:\n' + df.to_string(columns=['Z_W1','G_Z'], header=True, index=False, justify='left') + '\n')

            if grri is True:
                df = df.assign(G_R=df['MAG_G']-df['MAG_R'])
                df = df.assign(R_I=df['MAG_R']-df['MAG_I'])
                logfile.write('The following color data has been calculated:\n' + df.to_string(columns=['G_R','R_I'], header=True, index=False, justify='left') + '\n')

        if addwise and not wise:
            logfile.write('WARNING - No WISE data was found for this object. The SED plot(s) will not include WISE data.\n')

        if addvhs and not vhs:
                logfile.write('WARNING - No VHS data was found for this object. The SED plot(s) will not include VHS data.\n')

        if gzzw1 and not wise:
            logfile.write('WARNING - No WISE data was found for this object. Cannot create G-Z vs. Z-W1 color plot.\n')

        if FluxVsWavelength:
            plotutils.PlotFluxVsWavelength(outputs, df, wise, vhs, filenm, exten)
            logfile.write('Figure ' + filenm + '_flux' + exten +' has been created.\n')
            numPlots += 1
        if MagVsWavelength:
            plotutils.PlotMagVsWavelength(outputs, df, wise, vhs, filenm, exten)
            logfile.write('Figure ' + filenm + '_magnitude' + exten + ' has been created.\n')
            numPlots += 1
        if GR_RI:
            plotutils.PlotGR_RI(outputs, df, filenm, exten)
            logfile.write('Figure ' + filenm + '_gr-ri' + exten + ' has been created.\n')
            numPlots += 1
        if GZ_ZW1:
            plotutils.PlotGZ_ZW1(outputs, df, filenm, exten)
            logfile.write('Figure ' + filenm + '_gz-w1' +exten + ' has been created.\n')
            numPlots += 1
        if SpreadVsMag:
            plotutils.PlotSpreadVsMag(outputs, df, filenm, exten)
            logfile.write('Figure ' + filenm + '_spreadmag' + exten + ' has been created.\n')
            numPlots += 1

    pngfiles = sorted(glob.glob(mypath + '*.png'))
    titles = []
    Ntiles = len(pngfiles)
    for p in pngfiles:
        title = p.split('/')[-1][:-4]
        title = title.split('_')[-1].upper()
        titles.append(title)
    for ij in range(Ntiles):
        pngfiles[ij] = pngfiles[ij][pngfiles[ij].find('/easyweb'):]
    os.chdir(user_folder)
    os.system("tar -zcf {0}/{0}.tar.gz {0}/".format(jobid))
    os.chdir(os.path.dirname(__file__))
    if pngfiles:
        if os.path.exists(mypath + "list.json"):
            os.remove(mypath + "list.json")
        with open(mypath + "list.json", "w") as outfile:
            json.dump([dict(name=pngfiles[i], title=titles[i], size=Ntiles) for i in range(len(pngfiles))], outfile, indent=4)

    logfile.write('****************************************\n')
    logfile.write('Number of Generated Plots: ' + str(numPlots) + '\n')
    end_time = time.time()
    difference = end_time - start_time
    logfile.write('The plots took ' + str(difference) + ' seconds to make.\n')
    logfile.write('Done.\n')
    logfile.close()

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

@app.task(base=CustomTask, soft_time_limit=3600*2, time_limit=3600*4)
def make_chart(inputs, uu, pp, outputs, db, xs, ys, jobid, listonly, send_email, email, gband, rband, iband, zband, yband, mag):
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
    jsonfile = user_folder +jobid + '.json'
    mypath = user_folder + jobid + '/'      # this is the same as "outputs"

    input_df = pd.DataFrame(pd.read_csv(inputs, sep=','))

    logname = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    logfile = open(mypath + 'DESFinderTool_' + logname + '.log', 'w')

    if len(input_df['RA']) != len(input_df['DEC']):
        logfile.write('ERROR - Please enter the same number of RA and DEC values.\n')
        response['status'] = 'error'
        with open(jsonfile, 'w') as fp:
            json.dump(response, fp)
        return response

    ralst = ','.join(input_df['RA'].apply(str).tolist())
    declst = ','.join(input_df['DEC'].apply(str).tolist())

    logfile.write('Selected Options:\n')
    logfile.write('    x size: ' + str(xs) + '\n')
    logfile.write('    y size: ' + str(ys) + '\n')
    logfile.write('    Magnitude Limit: ' + str(mag) + '\n')
    logfile.write('    G Band: {0}, R Band {1}, I Band {2}, Z Band {3}, Y Band {4}\n'.format(gband, rband, iband, zband, yband))
    logfile.write('    Return cutout too? {} \n'.format(not listonly))
    logfile.write('    Email: ' + str(email) + '\n')
    logfile.write('    Comment: ' + logname + '\n')
    logfile.write('****************************************\n')
    logfile.write('Submitted RA: ' + ralst + '\n')
    logfile.write('Submitted DEC: ' + declst + '\n')

    with open(mypath+'log.log','w') as logg:
        logg.write('Running...')
    with open('config/desaccess.yaml', 'r') as cfile:
        conf = yaml.load(cfile)['descut']
    uu1 = conf['username']
    pp1 = conf['password']
    """
    com = "makeDESthumbs {0} --user {1}1 --password {3} --MP --outdir={3}".format(inputs, uu1, pp1, outputs)

    if xs != "":
        com += ' --xsize {}'.format(xs)
    if ys != "":
        com += ' --ysize {}'.format(ys)
    com += " --logg {}".format(outputs + 'log.log')
    com += " --tag Y3A1_COADD"

    os.chdir(mypath)
    oo = subprocess.check_output([com], shell=True)

    # If no options were selected in the form, set it to use iband.
    if not gband and not rband and not iband and not zband and not yband:
        iband = True
    """
    
    bulkthumbscolors = []
    if gband:
        bulkthumbscolors.append('g')
    if rband:
        bulkthumbscolors.append('r')
    if iband:
        bulkthumbscolors.append('i')
    if zband:
        bulkthumbscolors.append('z')
    if yband:
        bulkthumbscolors.append('y')
    if not bulkthumbscolors:
        bulkthumbscolors.append('i')
    bulkthumbscolors = (',').join([str(x) for x in bulkthumbscolors])
    bulkthumbscom = "mpirun -n 1 python3 bulkthumbs.py --ra {} --dec {} --xsize {} --ysize {} --make_fits --colors {} --db {} --jobid {} --usernm {} --passwd {} --outdir {} --return_list".format(ralst, declst, xs, ys, bulkthumbscolors, "Y3A2", jobid, uu, pp, Settings.WORKDIR)
    try:
        #oo = subprocess.run([bulkthumbscom], check=True, shell=True)
        oo = subprocess.check_output([bulkthumbscom], shell=True)
    except subprocess.CalledProcessError as e:
        print(e.output)
    
    urllst = []
    dftiles = pd.DataFrame(pd.read_csv(mypath+'BTL_'+jobid.upper().replace("-","_")+'.csv'))
    for tile in dftiles['TILENAME']:
        for fileitm in os.listdir(mypath+tile+'/'):
            if fileitm.endswith("_g.fits") and gband:
                urllst.append(mypath + tile + '/' + fileitm)
            if fileitm.endswith("_r.fits") and rband:
                urllst.append(mypath + tile + '/' + fileitm)
            if fileitm.endswith("_i.fits") and iband:
                urllst.append(mypath + tile + '/' + fileitm)
            if fileitm.endswith("_z.fits") and zband:
                urllst.append(mypath + tile + '/' + fileitm)
            if fileitm.endswith("_Y.fits") and yband:
                urllst.append(mypath + tile + '/' + fileitm)
    
    conn = ea.connect(db, user=uu, passwd=pp)
    curs = conn.cursor()

    start_time = time.time()
    for row in urllst:
        logfile.write('****************************************\n')
        logfile.write('Object: ' + row[-28:-5] + '\n')

        band = ''
        filenm = ''

        makePlot = True
        helperPlot = True
        USERObject = None

        if '_g.fits' in row:
            band = 'G'
        if '_r.fits' in row:
            band = 'R'
        if '_i.fits' in row:
            band = 'I'
        if '_z.fits' in row:
            band = 'Z'
        if '_Y.fits' in row:
            band = 'Y'

        #image = urllst[row]
        ##image = fits.HDUList.fromstring(image.content)
        #image = fits.open(image)
        image = fits.open(row)

        image = image[0]
        header = image.header
        data = image.data
        data = data.copy()

        RAUSER = header['CRVAL1']            # should be the precise RA the user inputted in the form
        DECUSER = header['CRVAL2']            # should be the precise DEC the user inputted in the form
        RAUSERpixel = header['CRPIX1']        # pixel coordinate of the image corresponding to the RA
        DECUSERpixel = header['CRPIX2']        # pixel coordinate of the image corresponding to the DEC

        filenm = 'DESJ' + plotutils.DecConverter(RAUSER, DECUSER)

        RAMIN, DECMIN = plotutils.PixelstoWCS(header, RAUSERpixel+(header['NAXIS1']/2)-1, DECUSERpixel-(header['NAXIS2']/2)+1)        # minimum RA/DEC of the image field in degrees
        RAMAX, DECMAX = plotutils.PixelstoWCS(header, RAUSERpixel-(header['NAXIS1']/2)+1, DECUSERpixel+(header['NAXIS2']/2)-1)        # maximum RA/DEC of the image field in degrees
        RAMIN = str(RAMIN)
        RAMAX = str(RAMAX)
        DECMIN = str(DECMIN)
        DECMAX = str(DECMAX)
        RAUSER = str(RAUSER)
        DECUSER = str(DECUSER)

        logfile.write('RA: ' + RAUSER + '\n')
        logfile.write('DEC: ' + DECUSER + '\n')

        # FIND USER OBJECT IN THE THUMBNAIL
        query1 = 'select * from (select COADD_OBJECT_ID, ALPHAWIN_J2000, DELTAWIN_J2000, RA, DEC, WAVG_MAG_PSF_'+band+' from Y3A2_COADD_OBJECT_SUMMARY where RA between '+RAMIN+' and '+RAMAX+' and DEC between '+DECMIN+' and '+DECMAX+' order by abs('+RAUSER+' - RA) + abs('+DECUSER+' - DEC) asc) where rownum = 1'

        # FIND NEIGHBOR OBJECTS IN THE THUMBNAIL
        query2 = 'select * from (select COADD_OBJECT_ID, ALPHAWIN_J2000, DELTAWIN_J2000, RA, DEC, WAVG_MAG_PSF_'+band+' from Y3A2_COADD_OBJECT_SUMMARY where RA between '+RAMIN+' and '+RAMAX+' and DEC between '+DECMIN+' and '+DECMAX+' and WAVG_MAG_PSF_'+band+' < '+str(mag)+' and abs(WAVG_MAG_PSF_'+band+') != 99.0 order by WAVG_MAG_PSF_'+band+' asc, abs('+RAUSER+' - RA) + abs('+DECUSER+' - DEC) asc) where rownum < 11'

        # The user object will be found and thrown into a csv file. The helper object will be found, in addition to the next (up to) 9 brightest objects, and all will be appended to the same csv file.
        USERObject = conn.query_to_pandas(query1)
        logfile.write('Below is the query used to match your object:\n' + query1 + '\n')
        if USERObject.empty is True:
            makePlot = False
            logfile.write('WARNING - Could not find a catalog match for your object in this field. Cannot make plot.\n')
            continue
        else:
            makePlot = True
            filenm = 'DESJ' + plotutils.DecConverter(USERObject['RA'][0], USERObject['DEC'][0])
            logfile.write('Below is the result of the query:\n' + USERObject.to_string(columns=None, header=True, index=False, justify='left') + '\n')
            USERObject.to_csv(outputs + filenm + '_' + (band.lower() if band != 'Y' else band) + '_objects.csv', sep=',', index=False)

        df = conn.query_to_pandas(query2)
        logfile.write('Below is the query used to find a helper object:\n' + query2 + '\n')
        if df.empty is True:
            helperPlot = False
            logfile.write('WARNING - Could not find any nearby objects in this field, or your object is the brightest object in your field. Try exapnding your xsize and/or ysize.\n')
        else:
            helperPlot = True
            logfile.write('Below is the result of the query. The first object listed is the helper object.\n' + df.to_string(columns=None, header=True, index=False, justify='left') + '\n')
            with open(outputs + filenm + '_' + band + '_objects.csv', 'a') as f:
                df.to_csv(f, sep=',', index=False, header=False)

        [dataMin, dataMax] = np.percentile(data,[30,99])
        data[data < dataMin] = dataMin
        data[data > dataMax] = dataMax

        figname = outputs + filenm + '_' + band.lower() + '_chart.png' if band != 'Y' else outputs + filenm + '_' + band + '_chart.png'

        fig = plt.figure()
        ax = plotutils.CreateChart(image, header, data, xs, ys, makePlot, helperPlot, USERObject, df, filenm, band)
        fig.axes.append(ax)

        try:
            plt.savefig(figname, bbox_inches='tight', dpi=300)
        except _wcs.InconsistentAxisTypesError as e:
            logfile.write('WARNING - ' + e + '\n')
            logfile.write('There is an error in the fits header for this image. The plot cannot be created.\n')
            return None
        else:
            logfile.write('Chart exported to ' + figname + '\n')

    if listonly:
        #print('list only')
        chartfiles = glob.glob(mypath + '*.png')
        titles = []
        pngfiles = []
        Ntiles = len(chartfiles)
        for c in chartfiles:
            title = c.split('/')[-1][:-4]
            titles.append(title)
            pngfiles.append(mypath + title + '.png')    #pngfiles.append(c)
        for ij in range(Ntiles):
            pngfiles[ij] = pngfiles[ij][pngfiles[ij].find('/easyweb'):]
        os.chdir(user_folder)
        os.system("tar -zcf {0}/{0}.tar.gz {0}/".format(jobid))
        os.chdir(os.path.dirname(__file__))
        if pngfiles:
            if os.path.exists(mypath+"list.json"):
                os.remove(mypath+"list.json")
            with open(mypath+"list.json", "w") as outfile:
                json.dump([dict(name=pngfiles[i], title=titles[i],
                                size=Ntiles) for i in range(len(pngfiles))], outfile, indent=4)
    else:
        #print('make png too')
        tiffiles = glob.glob(mypath+'*.tif')
        chartfiles = glob.glob(mypath + '*.png')
        titles = []
        pngfiles = []
        Ntiles = len(tiffiles) + len(chartfiles)
        for c in chartfiles:
            title = c.split('/')[-1][:-4]
            titles.append(title)
            pngfiles.append(mypath + title + '.png')   #pngfiles.append(c)
        for f in tiffiles:
            title = f.split('/')[-1][:-4]
            subprocess.check_output(["convert %s %s.png" % (f, f)], shell=True)
            titles.append(title)
            pngfiles.append(mypath + title + '.tif.png')

        for ij in range(Ntiles):
            pngfiles[ij] = pngfiles[ij][pngfiles[ij].find('/easyweb'):]
        os.chdir(user_folder)
        os.system("tar -zcf {0}/{0}.tar.gz {0}/".format(jobid))
        os.chdir(os.path.dirname(__file__))
        if pngfiles:
            if os.path.exists(mypath+"list.json"):
                os.remove(mypath+"list.json")
            with open(mypath+"list.json", "w") as outfile:
                json.dump([dict(name=pngfiles[i], title=titles[i],
                                size=Ntiles) for i in range(len(pngfiles))], outfile, indent=4)

    logfile.write('****************************************\n')
    end_time = time.time()
    difference = end_time - start_time
    logfile.write('The plot took ' + str(difference) + ' seconds to make.\n')
    logfile.write('Have a nice day.\n')
    logfile.close()
    #conn.close()

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
