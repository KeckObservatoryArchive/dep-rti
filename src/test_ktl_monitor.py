#!/kroot/rel/default/bin/kpython3
'''
Monitor KTL keywords for new data to archive.
'''

#TODO: Test recovering from instrument server reboots and keyword servers being down.

#modules
import os
import sys
import argparse
import json
import yaml
import subprocess
import psutil
import datetime as dt
import ktl
from pathlib import Path
import traceback
import smtplib
from email.mime.text import MIMEText
import threading

#Init a global log ojbect so we can just type 'log' in all code below
import logging
log = logging.getLogger('koamonitor')


#Map needed keywords per instrument to standard key names
#todo: This json layout may need to be tweaked after we look at all the instruments.
#todo: This could be put in each of the instr subclasses.
instr_keys = {
    'KCWI': [
        {
            'service':   'kfcs',
            'lastfile':  'lastfile',
            'outdir':    'outdir',
            'outfile':   'outfile',
            'sequence':  'sequence'
        },
        {
            'service':   'kbds',
            'lastfile':  'loutfile',
            'outdir':    'outdir',
            'outfile':   'outfile',
            'sequence':  'frameno'
        }
    ]
}


def main():

    # define arg parser 
    parser = argparse.ArgumentParser()
    parser.add_argument("instr", type=str, help="Instrument to monitor.")

    #parse args
    args = parser.parse_args()
    instr = args.instr.upper()

    #cd to script dir so relative paths work
    os.chdir(sys.path[0])

    #load config file
    with open('config.live.ini') as f: 
        config = yaml.safe_load(f)

    #create logger
    global log
    log = create_logger('koaktlmonitor', config[instr]['ROOTDIR'], instr)
    log.info("Starting KOA KTL Monitor: " + ' '.join(sys.argv[0:]))

    #run monitor for each group defined for instr
    for keys in instr_keys[instr]:
        monitor = KtlMonitor(instr, keys)
        monitor.start()

    #stay alive forever (control-C to exit)
    while True:
        pass


def handle_fatal_error():

    #form subject and msg (and log as well)
    subject = f'KOA KTL MONITOR ERROR: {sys.argv}'
    msg = traceback.format_exc()
    if log: log.error(subject + ' ' + msg)
    else: print(msg)

    #get admin email.  Return if none.
    with open('config.live.ini') as f: config = yaml.safe_load(f)
    adminEmail = config['REPORT']['ADMIN_EMAIL']
    if not adminEmail: return
    
    # Construct email message
    msg = MIMEText(msg)
    msg['Subject'] = subject
    msg['To']      = adminEmail
    msg['From']    = adminEmail
    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()


def create_logger(name, rootdir, instr):
    '''Creates a logger based on rootdir and instr.'''

    #create directory if it does not exist
    processDir = f'{rootdir}/{instr.upper()}'
    logFile =  f'{processDir}/koa_monitor_{instr.upper()}.log'
    Path(processDir).mkdir(parents=True, exist_ok=True)

    # Create logger object
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    # Create a file handler
    handle = logging.FileHandler(logFile)
    handle.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    handle.setFormatter(formatter)
    log.addHandler(handle)

    #add stdout to output so we don't need both log and print statements(>= warning only)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    log.addHandler(sh)
    
    #init message and return
    log.info(f'logger created for {name} at {logFile}')
    return log


class KtlMonitor():
    '''
    Class to handle monitoring a distinct set of keywords for an instrument to 
    determine when a new image has been written.
    '''

    def __init__(self, instr, keys):
        log.info(f"KtlMonitor: instr: {instr}, service: {keys['service']}")
        self.instr = instr
        self.keys = keys


    def start(self):
        '''Start monitoring lastfile keyword for new files.'''

        #These cache calls can throw exceptions (if instr server is down for example)
        #So, we should catch and retry until successful.  Be careful not to multi-register the callback
        try:
            #create keyword objects for easy reads later
            keys = self.keys
            self.service = ktl.cache(keys['service'])
            self.kw_outdir   = self.service[keys['outdir']]
            self.kw_outfile  = self.service[keys['outfile']]
            self.kw_sequence = self.service[keys['sequence']]

            #monitor keyword that indicates new file
            kw = ktl.cache(keys['service'], keys['lastfile'])
            kw.callback(self.on_new_file)
            kw.monitor()

        except Exception as e:
            log.error("Could not start KTL monitoring.  Retrying in 60 seconds.")
            threading.Timer(60.0, self.start).start()
            return


    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''

        #todo: What is the best way to handle error/crashes in the callback?  Do we want the monitor to continue?
        try:
            if kw['populated'] == False:
                log.warning(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            #todo: form full file path
            outdir = self.kw_outdir.read()
            outfile = self.kw_outfile.read()
            sequence = self.kw_sequence.read()
            lastfile = keyword.ascii

            #check for blank lastfile
            if not lastfile or not lastfile.strip():
                log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                return

            #log it
            log.info(f"NEW_FILE\t{self.instr}\t{keyword.service}\t{lastfile}\t{outdir}\t{outfile}\t{sequence}")

        except Exception as e:
            handle_fatal_error()


#--------------------------------------------------------------------------------
# main command line entry
#--------------------------------------------------------------------------------
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        handle_fatal_error()