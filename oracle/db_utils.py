import easyaccess as ea
import datetime as dt
import uuid


def check_username(username):
    username = username.lower()
    con = ea.connect('oldoper')
    check_username = "SELECT * from DES_ADMIN.DES_USERS where USERNAME = '{}'".format(username)
    df = con.query_to_pandas(check_username)
    con.close()
    if len(df) == 0:
        return True
    else:
        return False


def check_email(email):
    email = email.lower()
    con = ea.connect('oldoper')
    check_email = "SELECT * from DES_ADMIN.DES_USERS where EMAIL = '{}'".format(email)
    df = con.query_to_pandas(check_email)
    con.close()
    if len(df) == 0:
        return True
    else:
        return False



def create_user_manager(username, password):
    con = ea.connect('oldoper')
    dict = {}
    dict['user'] = username.lower()
    dict['passwd'] = password
    query_user = """
    CREATE USER {user} IDENTIFIED BY {passwd}
    DEFAULT TABLESPACE USERS
    """
    con.query_and_print(query_user.format(**dict), suc_arg='User {user} created'.format(**dict))
    grant_session = "GRANT CREATE SESSION to {user}".format(**dict)
    con.query_and_print(grant_session.format(**dict), suc_arg='Granted login')
    grant_tables = "GRANT SELECT,UPDATE on DES_ADMIN.DES_USERS to {user}".format(user=username)
    con.query_and_print(grant_tables, suc_arg='Granted Select on table')
    con.close()


def create_user(username, password, first, last, email, country, institution, lock=True):
    con = ea.connect('oldoper')
    dict = {}
    dict['user'] = username
    dict['passwd'] = password
    dict['first'] = first
    dict['last'] = last
    dict['email'] = email
    dict['country'] = country
    dict['inst'] = institution
    query_user = """
    CREATE USER {user} IDENTIFIED BY {passwd}
    DEFAULT TABLESPACE USERS
    """
    con.query_and_print(query_user.format(**dict), suc_arg='User {user} created'.format(**dict))
    grant_session = "GRANT CREATE SESSION to {user}".format(**dict)
    con.query_and_print(grant_session.format(**dict), suc_arg='Granted login')
    tables = ['DES_ADMIN.CACHE_TABLES', 'DES_ADMIN.CACHE_COLUMNS']
    for itable in tables:
        grant_tables = "GRANT SELECT on {table} to {user}".format(table=itable, user=username)
        con.query_and_print(grant_tables, suc_arg='Granted Select on table')
    insert_des = """
    INSERT INTO DES_ADMIN.DES_USERS VALUES (
    '{user}', '{first}', '{last}', '{email}', '{country}', '{inst}'
    )
    """.format(**dict)
    con.query_and_print(insert_des.format(**dict), suc_arg='{user} added to DES_USERS'.format(**dict))
    grant_role = "GRANT DES_READER to {user}".format(**dict)
    con.query_and_print(grant_role.format(**dict), suc_arg='Granted DES_READER role')
    if lock:
        qlock = "ALTER USER {user} account lock".format(**dict)
        con.query_and_print(qlock, suc_arg='Account locked')
    con.close()


def delete_user(username):
    con = ea.connect('oldoper')
    delete_des = "DELETE FROM DES_ADMIN.DES_USERS where username = '{}'"
    con.query_and_print(delete_des.format(username), suc_arg='{} removed from DES_USERS'.format(username))
    delete_user = "DROP USER {} CASCADE".format(username)
    con.query_and_print(delete_user, suc_arg='{} Dropped'.format(username))
    con.close()

def unlock_user(username):
    con = ea.connect('oldoper')
    qlock = "ALTER USER {0} account unlock".format(username)
    con.query_and_print(qlock, suc_arg='Account unlocked')
    con.close()

def create_reset_url(email):
    con = ea.connect('oldoper')
    check = "SELECT * from DES_ADMIN.DES_USERS where EMAIL = '{}'".format(email)
    df = con.query_to_pandas(check)
    if len(df) == 0:
        return False, 'email is not valid'
    username = df.USERNAME.ix[0]
    delete_old = "DELETE FROM DES_ADMIN.RESET_URL WHERE USERNAME = '{}'".format(username)
    con.query_and_print(delete_old, suc_arg='Delete old Url')
    now = dt.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    url = uuid.uuid4().hex
    insert_url = "INSERT INTO DES_ADMIN.RESET_URL VALUES ('{0}', '{1}', to_date('{2}' , 'yyyy/mm/dd hh24:mi:ss'))".format(username, url, now)
    con.query_and_print(insert_url, suc_arg='Add new reset URL')
    con.close()
    return True, url

def delete_url(url):
    con = ea.connect('oldoper')
    delete_old = "DELETE FROM DES_ADMIN.RESET_URL WHERE URL = '{0}'".format(url)
    con.query_and_print(delete_old, suc_arg='Delete Url')
    con.close()
    return

def update_password(username, password):
    con = ea.connect('oldoper')
    qlock = "ALTER USER {0} IDENTIFIED BY {1}".format(username, password)
    con.query_and_print(qlock, suc_arg='Password Change')
    con.close()
    return True


def update_info(username, firstname, lastname, email, user_manager='', pass_manager=''):
    username = username.lower()
    email = email.lower()
    con = ea.connect('oldoper', user=user_manager, passwd=pass_manager)
    qupdate = """
        UPDATE  DES_ADMIN.DES_USERS SET
        firstname = '{first}',
        lastname = '{last}',
        email = '{email}'
        where username = '{user}'
        """.format(first=firstname, last=lastname, email=email, user=username)
    con.query_and_print(qupdate, suc_arg='Profile Information Updated')
    con.close()
    return True

def valid_url(url, timeout = 6000):
    con = ea.connect('oldoper')
    select_url = "SELECT * FROM DES_ADMIN.RESET_URL WHERE URL = '{0}'".format(url)
    df = con.query_to_pandas(select_url)
    if len(df) == 0 :
        con.close()
        return None, ' URL does not exist!'
    created = df.CREATED.ix[0]
    username = df.USERNAME.ix[0]
    diff = (dt.datetime.now() - created).seconds
    if diff > timeout:
        print('url is not longer valid')
        con.close()
        return None, 'URL is not longer valid!'
    else:
    	#delete_old = "DELETE FROM DES_ADMIN.RESET_URL WHERE USERNAME = '{}'".format(username)
    	#con.query_and_print(delete_old, suc_arg='Delete old Url')
        con.close()
        return username, 'valid'
