#!/usr/local/anaconda/bin/python
'''
Check for dep_status errors and email admins if we haven't emailed recently.
'''

import os
import sys
import argparse
import smtplib
from email.mime.text import MIMEText
import datetime as dt
import db_conn

#globals
MAX_EMAIL_SEC = 2*60*60


def main(instr=None, dev=False):

    print(f"\n{dt.datetime.now()} Running {sys.argv}")

    #cd to script dir so relative paths work
    os.chdir(sys.path[0])

    #set timezone explicit
    os.environ['TZ'] = 'Pacific/Honolulu'

    #db connect
    db = db_conn.db_conn(f'config.live.ini', configKey='DATABASE')

    #Query for last email times
    q = 'select * from dep_error_notify order by email_time desc limit 1'
    lasttime = db.query('koa', q, getOne=True, getColumn='email_time')
    if lasttime:
        now = dt.datetime.now()
        diff = now-lasttime
        if diff.seconds < MAX_EMAIL_SEC:
            return

    #query for all errors
    q = ("select instrument, count(*) as count, status_code from dep_status "
         " where status='ERROR' "
         " group by instrument, status_code order by instrument asc")
    errors = db.query('koa', q)

    #query for any records that have blank status but have status code.
    q = ("select instrument, count(*) as count, status_code from dep_status "
         " where status='COMPLETE' and status_code is not NULL and status_code != '' "
         " where status in ('COMPLETE', 'TRANSFERRED') and status_code is not NULL and status_code != '' "
         " group by instrument, status_code order by instrument asc")
    warns = db.query('koa', q)

    #query for any records that are > X minutes old and status in (PROCESSING, TRANSFERRING, etc)
    q = ("select instrument, count(*) as count from dep_status "
        " where status in ('QUEUED', 'PROCESSING', 'TRANSFERRING', 'TRANSFERRED') "
         " and creation_time < NOW() - INTERVAL 15 MINUTE "
         " group by instrument order by instrument asc")
    stuck = db.query('koa', q)

    #nada?
    if not errors and not warns and not stuck:
        return

    #msg
    msg = ''
    if errors: 
        msg += gen_table_report('errors', errors)
    if warns: 
        msg += gen_table_report('warnings', warns)
    if stuck: 
        msg += gen_table_report('stuck', stuck)

    #email and insert new record
    print(msg)
    if not dev:
        email_admin(msg, dev=dev)
        db.query('koa', 'insert into dep_error_notify set email_time=NOW()')


def gen_table_report(name, rows):
    if not rows: return
    txt = f"\n===DEP {name} summary:===\n"
    for row in rows:
        txt += row['instrument'].ljust(12)
        txt += str(row['count']).ljust(6)
        if 'status_code' in row:
            txt += row['status_code']
        txt += "\n"
    return txt


def email_admin(body, dev=False):

    subject = os.path.basename(__file__) + " report"

    msg = MIMEText(body)
    msg['From'] = 'koaadmin@keck.hawaii.edu'
    if dev:
        msg['To'] = 'jriley@keck.hawaii.edu'
        msg['Subject'] = '[TEST] ' + subject
    else:
        msg['To'] = 'koaadmin@keck.hawaii.edu'
        msg['Subject'] = subject
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()




if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--instr", type=str, default=None, help='Specific instrument to check, otherwise check all.')
    parser.add_argument("--dev", dest="dev", default=False, action="store_true")
    args = parser.parse_args()

    main(instr=args.instr, dev=args.dev)
