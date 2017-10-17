import Settings
import time
import schedule
#import config.mysqlconfig as ms
from subprocess import Popen, PIPE
import MySQLdb as mydb
import yaml
import sys


def dump():
    command = "mysqldump -h {host} -P {port} -u {user} -p{passwd} {db} > {sqlfile}"
    with open('config/mysqlconfig.yaml', 'r') as cfile:
        inputs = yaml.load(cfile)['mysql']
    inputs['sqlfile'] = Settings.DBFILE2
    #print(command.format(**inputs))
    print('Saving database')
    Popen(command.format(**inputs), shell=True, stdout=PIPE, stderr=PIPE)


def restore():
    try:
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        conf.pop('db', None)
        con = mydb.connect(**conf)
        cur = con.cursor()
    except:
        return False
    try:
        con.select_db('des')
        print('des Database does exists')
        return True
    except mydb.OperationalError:
        _, err, _ = sys.exc_info()
        if err.args[0] == 1049:
            print('Creating des DB')
            cur.execute('create database {0}'.format('des'))
            con.commit()
            con.close()
            print('Restoring des DB')
            command = "mysql -h {host} -P {port} -u {user} -p{passwd} {db} < {sqlfile}"
            with open('config/mysqlconfig.yaml', 'r') as cfile:
                inputs = yaml.load(cfile)['mysql']
            inputs['sqlfile'] = Settings.DBFILE2
            # print(command.format(**inputs))
            Popen(command.format(**inputs), shell=True, stdout=PIPE, stderr=PIPE)
            return False
    except Exception:
        return False


def job():
    print('Update time')
    print('-----------\n')
    conti = restore()
    if conti:
        dump()


if __name__ == '__main__':
    schedule.every().hour.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)
