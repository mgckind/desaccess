from celery import Celery
import yaml
import MySQLdb as mydb
import requests
import json


def notify():
    print('*****')
    url=Settings.ROOT_URL+'/easyweb/pusher/'   
    resp = {}
    resp['status'] = 'error'
    resp['data'] = 'Time Exceeded (30 sec)'
    resp['kind'] = 'query'
    requests.post(url, data=resp, verify=False)


def my_monitor(app):
    state = app.events.State()

    def update_revoked_tasks(event):
        state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = state.tasks.get(event['uuid'])
        jobid = task.uuid
        with open('config/mysqlconfig.yaml', 'r') as cfile:
            conf = yaml.load(cfile)['mysql']
        con = mydb.connect(**conf)
        q0 = "UPDATE Jobs SET status='{0}' where job = '{1}'".format('REVOKE', jobid)
        with con:
            cur = con.cursor()
            cur.execute(q0)
            con.commit()

        print('TASK REVOKED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))
        print(task.exception)

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-revoked': update_revoked_tasks,
            'task-failed': update_revoked_tasks,
            '*': state.event,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)


if __name__ == '__main__':
    app = Celery('ea_tasks')
    app.config_from_object('config.celeryconfig')
    my_monitor(app)
