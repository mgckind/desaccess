import Settings
import sqlite3 as lite
import tornado.web
import tornado.websocket
import json
import os
import datetime
import easyaccess as ea
#import MySQLdb as mydb
from celery import Celery
import yaml


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")


def humantime(s):
    if s < 60:
        return "%d seconds" % s
    else:
        mins = s/60
        secs = s % 60
        if mins < 60:
            return "%d minutes and %d seconds" % (mins, secs)
        else:
            hours = mins/60
            mins = mins % 60
            if hours < 24:
                return "%d hours and %d minutes" % (hours, mins)
            else:
                days = hours/24
                hours = hours % 24
                return "%d days and %d hours" % (days, hours)


class MyExamplesHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        query0 = """--
-- Example Query --
-- This query selects 1% of the data
SELECT RA, DEC, MAG_AUTO_G from DR1_MAIN sample(0.0001)
"""
        queries = []
        queries.append({'desc':'Sample Basic information', 'query': query0})
        jjob = []
        jquery = []

        for i in range(len(queries)):
            jjob.append(queries[i]['desc'])
            jquery.append(queries[i]['query'])
        out_dict = [dict(job=jjob[i], jquery=jquery[i]) for i in range(len(jjob))]
        temp = json.dumps(out_dict, indent=4)
        self.write(temp)


class GetTileHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        tilename = self.get_argument('tilename', '')
        print(tilename)
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"', '')
        loc_db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
        con = ea.connect(loc_db, user=loc_user, passwd=loc_passw)
        query = """select FITS_IMAGE_G, FITS_IMAGE_R, FITS_IMAGE_I, FITS_IMAGE_Z, FITS_IMAGE_Y
                   from DR1_TILE_INFO where tilename = '{0}'""".format(tilename)
        temp_df = con.query_to_pandas(query)
        new = temp_df.transpose().reset_index()
        new.columns = ['name', 'path']
        con.close()
        self.write(new.to_json(orient='records'))

class MyLogsHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        jobid = self.get_argument('jobid')
        print(loc_user, jobid)
        log_path = os.path.join(Settings.WORKDIR, loc_user, jobid, 'log.log')
        log = ''
        with open(log_path, 'r') as logFile:
            for line in logFile:
                log += line+'<br>'
        temp = json.dumps(log)
        self.write(temp)





class MyJobsHandler(BaseHandler):
    @tornado.web.authenticated
    def delete(self):
        user = self.get_argument('username')
        jobid = self.get_argument('jobid')
        app = Celery()
        app.config_from_object('config.celeryconfig')
        app.control.revoke(jobid, terminate=True)
        app.close()
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        cur = con.cursor()
        q0 = "UPDATE Jobs SET status='{0}' where job = '{1}'".format('REVOKE', jobid)
        cur.execute(q0)
        con.commit()
        con.close()

        self.finish()


    @tornado.web.authenticated
    def get(self):
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        cur = con.cursor()
        cur.execute("SELECT * from Jobs where user = '{0}' order by time DESC".format(loc_user))
        cc = cur.fetchall()
        con.close()
        cc = list(cc)
        jjob = []
        jstatus = []
        jobtype = []
        jtime = []
        jname = []
        jelapsed = []
        jquery = []
        jfiles = []
        jsizes = []
        jfiles_bool = []
        jquery_bool = []

        for i in range(len(cc)):
            #dd = datetime.datetime.strptime(cc[i][3], '%Y-%m-%d %H:%M:%S')
            dd = cc[i][4]
            ctime = dd.strftime('%a %b %d %H:%M:%S %Y')
            jjob.append(cc[i][1])
            jname.append(cc[i][2])
            jstatus.append(cc[i][3])
            jobtype.append(cc[i][5])
            jtime.append(ctime)
            jquery.append(cc[i][6])
            jfiles.append(cc[i][7])
            jsizes.append(cc[i][8])
            if cc[i][7] == '':
                jfiles_bool.append(False)
            else:
                jfiles_bool.append(True)
            if cc[i][6] == '':
                jquery_bool.append(False)
            else:
                jquery_bool.append(True)
            jelapsed.append(humantime((datetime.datetime.now()-dd).total_seconds())+" ago")
        out_dict = [dict(job=jjob[i], status=jstatus[i], time=jtime[i], elapsed=jelapsed[i],
                    jquery=jquery[i], jfiles=jfiles[i], jbool=jfiles_bool[i], user=loc_user,
                    jsizes=jsizes[i], jname=jname[i], jobtype=jobtype[i],
                    jquerybool=jquery_bool[i]) for i in range(len(jjob))]
        temp = json.dumps(out_dict, indent=4)
        self.write(temp)


class MyTablesHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        response = {k: self.get_argument(k) for k in self.request.arguments}
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"', '')
        loc_db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
        con = ea.connect(loc_db, user=loc_user, passwd=loc_passw)
        query = """
        SELECT t.table_name, s.bytes/1024/1024/1024 SIZE_GBYTES, t.num_rows NROWS
        FROM user_segments s, user_tables t
        WHERE s.segment_name = t.table_name order by t.table_name
        """
        temp_df = con.query_to_pandas(query)
        con.close()
        self.write(temp_df.to_json(orient='records'))


class MyResponseHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        jobid = self.get_argument('jobid')
        user_folder = os.path.join(Settings.WORKDIR, loc_user)+'/'
        jsonfile = os.path.join(user_folder, jobid+'.json')
        try:
            with open(jsonfile, 'r') as data_file:
                tmp = json.load(data_file)
        except:
            tmp = ''
        self.flush()
        self.write(tmp)
        self.finish()



class DescTablesHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        response = {k: self.get_argument(k) for k in self.request.arguments}
        owner = self.get_argument('owner')
        table = self.get_argument('tablename')
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"', '')
        loc_db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
        owner = 'DES_ADMIN'
        con = ea.connect(loc_db, user=loc_user, passwd=loc_passw)
        query = """
            select atc.column_name, atc.data_type,
            case atc.data_type
            when 'NUMBER' then '(' || atc.data_precision || ',' || atc.data_scale || ')'
            when 'VARCHAR2' then atc.CHAR_LENGTH || ' characters'
            else atc.data_length || ''  end as DATA_FORMAT,
            acc.comments
            from all_tab_cols atc , all_col_comments acc, all_synonyms ass
            where ass.synonym_name = '{table}' and
            atc.owner = ass.table_owner and atc.table_name = ass.table_name
            and acc.owner = ass.table_owner and acc.table_name = ass.table_name
            and acc.column_name = atc.column_name
            order by atc.column_name
            """.format(owner=owner.upper(), table=table.upper())
        temp_df = con.query_to_pandas(query)
        con.close()
        self.write(temp_df.to_json(orient='records'))


class AllTablesHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        response = {k: self.get_argument(k) for k in self.request.arguments}
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
        loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"', '')
        loc_db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
        print(loc_db)
        con = ea.connect(loc_db, user=loc_user, passwd=loc_passw)
        query = """
        SELECT t.synonym_name as table_name, a.num_rows as NROWS
        FROM all_synonyms t, all_tables a
        where t.table_owner = 'DES_ADMIN' and t.table_name = a.table_name
        order by table_name;
        """
        temp_df = con.query_to_pandas(query)
        con.close()
        self.write(temp_df.to_json(orient='records'))



class DeleteHandler(BaseHandler):
    @tornado.web.authenticated
    def delete(self):
        response = {k: self.get_argument(k) for k in self.request.arguments}
        loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')

        user_folder = os.path.join(Settings.WORKDIR, loc_user)+'/'
        Nd = len(response)
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
            con = mydb.connect(**conf)
            cur = con.cursor()
            for j in range(Nd):
                jid = response[str(j)]
                q = "DELETE from Jobs where job = '%s' and user = '%s'" % (jid, loc_user)
                cc = cur.execute(q)
                folder = os.path.join(user_folder, jid)
                print(folder)
                try:
                    os.system('rm -rf ' + folder)
                except:
                    pass
                try:
                    os.system('rm -f ' + user_folder + jid + '.*')
                except:
                    pass
            con.commit()
            con.close()

        self.set_status(200)
        self.flush()
        self.finish()




class ChangeHandler(BaseHandler):
    @tornado.web.authenticated
    def post(self):
        user = self.get_argument('username')
        jobid = self.get_argument('jobid')
        jobname = self.get_argument('jobname')
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        cur = con.cursor()
        q0 = "UPDATE Jobs SET name='{0}' where job = '{1}'".format(jobname, jobid)
        cur.execute(q0)
        con.commit()
        con.close()

        self.finish()
