from celery import Celery

def my_monitor(app):
    state = app.events.State()

    def on_event(event):
        print("Event for {0} : {1}".format(event['uuid'],event))

    def announce_failed_tasks(event):
        state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = state.tasks.get(event['uuid'])

        print('TASK FAILED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
                'task-failed': announce_failed_tasks,
                'task-succeeded' : on_event,
                'task-received' : on_event,
                'task-revoked' : on_event,
                '*': state.event,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == '__main__':
    app = Celery()
    app.config_from_object('config.celeryconfig')
    my_monitor(app)
