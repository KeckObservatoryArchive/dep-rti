#!/usr/local/anaconda/bin/python
'''
Check for koa_status errors and email admins if we haven't emailed recently.
'''

import os
import sys
import argparse
import smtplib
from email.mime.text import MIMEText
import datetime as dt
import db_conn
import socket
from getpass import getuser
from socket import gethostname
import requests

#globals
MAX_EMAIL_SEC = 2*60*60
SLACKAPP = 'https://hooks.slack.com/workflows/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'

def main(dev=False, admin_email=None, slack=False):

    print(f"\n{dt.datetime.now()} Running {sys.argv}")

    #cd to script dir so relative paths work
    os.chdir(sys.path[0])

    #set timezone explicit
    os.environ['TZ'] = 'Pacific/Honolulu'

    #db connect
    db = db_conn.db_conn(f'config.live.ini', configKey='DATABASE')

    #Query for last email times
    if not dev:
        q = 'select * from dep_error_notify order by email_time desc limit 1'
        lasttime = db.query('koa', q, getOne=True, getColumn='email_time')
        if lasttime:
            now = dt.datetime.now()
            diff = now-lasttime
            if diff.total_seconds() < MAX_EMAIL_SEC:
                print("Already sent a recent error email.")
                return

    #query for last error
    q = ("select * from koa_status where status='ERROR' and reviewed=0 order by id desc limit 1")
    lasterror = db.query('koa', q, getOne=True)

    #query for all ERRORs
    q = ("select instrument, count(*) as count, status_code, status_code_ipac from koa_status "
         " where status='ERROR' "
         " and reviewed=0 "
         " group by instrument, status_code, status_code_ipac order by instrument asc")
    errors = db.query('koa', q)

    #query for any records that have blank status but have status code.
    q = ("select instrument, count(*) as count, status_code from koa_status "
#         " where status='COMPLETE' and status_code is not NULL and status_code != '' "
         " where status in ('PROCESSING','TRANSFERRING','TRANFERRED','COMPLETE') and status_code<>'' "
         "  and reviewed=0 "
         " group by instrument, status_code order by instrument asc")
    warns = db.query('koa', q)

    #query for any records that are > X minutes old and status in (PROCESSING, TRANSFERRING, etc)
    #NOTE: creation_time is UTC
    q = ("select instrument, count(*) as count from koa_status "
        " where status in ('QUEUED', 'PROCESSING', 'TRANSFERRING', 'TRANSFERRED') "
         " and creation_time < (NOW() - INTERVAL 15 MINUTE + INTERVAL 10 HOUR) "
         " and reviewed=0 "
         " group by instrument order by instrument asc")
    stuck = db.query('koa', q)

    #nada?
    if not errors and not warns and not stuck:
        print("No errors or warnings.")
        return

    #msg
    msg = ''
    if errors: 
        msg += gen_last_error_report(lasterror)
        msg += gen_table_report('errors', errors)
    if warns: 
        msg += gen_table_report('warnings', warns)
    if stuck: 
        msg += gen_table_report('stuck', stuck)
    msg += ("\n\nReminder: This script runs once per day on cron.  Otherwise, it is triggered "
            "by new DEP errors and will only email once per hour.  You may want to manually run "
            " this script or monitor koa_status for other chronic errors in that hour window.")

    #email and insert new record
    print(msg)
    if not dev:
        db.query('koa', 'insert into dep_error_notify set email_time=NOW()')
        if admin_email:
            email_admin(msg, dev=dev, to=admin_email)
        if slack:
            data = {}
            data["user"] = f"{getuser()}@{gethostname()}"
            data["status_code"] = "ERROR" if errors else "WARNING"
            q = ("select * from koa_status where status_code<>'' and reviewed=0 order by id desc limit 1")
            lasterror = db.query('koa', q, getOne=True)
            data["message"] = f"{lasterror['instrument']}\n{lasterror['status_code']}\n{lasterror['ofname']}"
            slackMsg = requests.post(SLACKAPP, json=data)
    else:
        print("\nNOT SENDING EMAIL")


def gen_last_error_report(row):
    if not row: return
    txt = ("\n=== Most recent error ==="
            f"\n{row['instrument']}"
            f"\tid: {row['id']}"
            f"\terr: {row['status_code']}"
            f"\tkoaid: {row['koaid']}"
            f"\tofname: {row['ofname']}"
            "\n")
    return txt

def gen_table_report(name, rows):
    if not rows: return
    txt = f"\n===DEP {name} summary:===\n"
    for row in rows:
        txt += row['instrument'].ljust(12)
        txt += str(row['count']).ljust(6)
        status_code      = row.get('status_code')
        status_code_ipac = row.get('status_code_ipac')
        if status_code:      txt += "\t"+row['status_code']
        if status_code_ipac: txt += "\tIPAC:"+row['status_code_ipac']
        txt += "\n"
    return txt


def email_admin(body, dev=False, to=None):

    if not to: 
        return
    print(f"\nEmailing {to}")

    subject = os.path.basename(__file__) + " report"
    subject = subject + ' (' + socket.gethostname() + ')'
    if dev: subject = '[TEST] ' + subject

    msg = MIMEText(body)
    msg['From'] = to
    msg['To'] = to
    msg['Subject'] = subject
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--admin_email", type=str, default=None, help='Admin email to send to.')
    parser.add_argument("--dev", dest="dev", default=False, action="store_true", help="If true, will email and check/update dep_email_notify.")
    parser.add_argument("--slack", dest="slack", default=False, action="store_true", help="If true, will send output to the #koa-rti channel in Slack.")
    args = parser.parse_args()

    main(dev=args.dev, admin_email=args.admin_email, slack=args.slack)
