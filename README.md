# desaccess

## setup
	pip3 install -r requirements.txt

## start redis-server
	redis-server
## start celery
    celery multi start worker1 -A ea_tasks --loglevel=info
    celery worker -A ea_tasks --loglevel=info -E -c 2
## start server
	python3 public.py
	
	



Please note that this version of desaccess is using vulcanization
