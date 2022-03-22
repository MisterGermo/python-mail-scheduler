
'''
# https://mariadb.com/resources/blog/how-to-connect-python-programs-to-mariadb/
# https://docs.sqlalchemy.org/en/13/dialects/mysql.html#module-sqlalchemy.dialects.mysql.pymysql
    mysql+pymysql://<username>:<password>@<host>/<dbname>[?<options>]
# https://pymysql.readthedocs.io/en/latest/user/examples.html
# https://pymysql.readthedocs.io/en/latest/modules/connections.html#pymysql.connections.Connection.ping
    #NOTE: '$ pip3' == '$ python3.6 -m pip'
        $ python3 -m pip install PyMySQL
        $ python3.7 -m pip install PyMySQL
    '''
import pymysql.cursors
from simple_email import *
from datetime import datetime
import time
from threading import Timer, Lock

#=============================#
# set database info
#=============================#
dbHost = 'localhost'
dbUser = 'some_user'
dbPw = 'user_password'
dbName = 'databse_name'
db = None
cur = None

#=============================#
# set email info
#   (in simple_email.py as well)
#=============================#
EMAIL_SUBJ = 'my subject'
EMAIL_MSG = 'my message'

# open db connection
def open_database_connection(self):
    funcname = 'open_database_connection()'
    print('GO _', funcname)

    # Connect to DB #
    try:
        # legacy manual db connection #
        db = pymysql.connect(host=dbHost,
                             user=dbUser,
                             password=dbPw,
                             db=dbName,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
        cur = self.db.cursor()
        if ENABLE_LOGGING: print(funcname, ' >> CONNECTED >> to db successfully!')
    except Exception as e:
        print('!! Exception hit...', e, type(e), e.args, sep=' | ')
        return -1
    finally:
        return 0

# function will auto run once a week
#   (according to 'interval' set below)
def demo_check_new_email():
    err = open_database_connection()
    if err == 0:
        print(' ** SUCCESS: db connection ** ')
        tbl_name = 'reg_emails'
        col_email = 'email_addr'
        col_dt_reg = 'dt_created'
        query = f'SELECT {col_dt_reg}, {col_email} FROM {tbl_name} WHERE dt_created < now() - INTERVAL 1 WEEK;'
        rowCnt = cur.execute(query)
        rows = cur.fetchall()
        db.commit()
        db.close()
        for dict_row in rows:
            e = dict_row[col_email]
            email_send_weekly(subj=EMAIL_SUBJ, msg=EMAIL_MSG, lst_receivers=[e])
    else:
        print(' ** ERROR: db connection ** ')

def get_time_now(timeconv=False, timeonly=False):
    timenow = '%s' % int(round(time.time()))
    if timeconv:
        timenow = datetime.fromtimestamp(int(timenow))
        if timeonly:
            timenow = str(timenow).split(' ')[1]
    return timenow

#ref: https://stackoverflow.com/a/18906292/2298002
class Periodic(object):
    """
    A periodic task running in threading.Timers
    """

    def __init__(self, interval, function, *args, **kwargs):
        self._lock = Lock()
        self._timer = None
        self.function = function
        self.interval = interval
        self.args = args
        self.kwargs = kwargs
        self._stopped = True
        if kwargs.pop('autostart', True):
            self.start()

    def start(self, from_run=False):
        print('go start')
        self._lock.acquire()
        if from_run or self._stopped:
            self._stopped = False
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
        self._lock.release() # <- wrong indentation

    def _run(self):
        print('go _run')
        self.start(from_run=True)
        self.function(*self.args, **self.kwargs)

    def stop(self):
        print('go stop')
        self._lock.acquire()
        self._stopped = True
        self._timer.cancel()
        self._lock.release()
    
def call_back(*args, **kwargs):
    t = get_time_now(timeconv=True, timeonly=False)
    print(f"call_back _ time: {t}")
    print("... invoking 'demo_check_new_email()'")
    #demo_check_new_email()
    
if __name__ == "__main__":
    print('go main run')
    sec_inter_week = 60 * 60 * 24 * 7
    sec_inter_test = 5
    
    sec_inter = sec_inter_test
    per = Periodic(sec_inter, call_back, autostart=True)
    per.start()
