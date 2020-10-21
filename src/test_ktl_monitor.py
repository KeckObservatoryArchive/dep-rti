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
import time

#Init a global log ojbect so we can just type 'log' in all code below
import logging
log = logging.getLogger('testktlmonitor')


#Map needed keywords per instrument to standard key names
#todo: This json layout may need to be tweaked after we look at all the instruments.
#todo: This could be put in each of the instr subclasses.
instr_keys = {
    # 'KCWI': [
    #     {
    #         'service':   'kfcs',
    #         'lastfile':  'uptime',
    #     },
    #     {
    #         'service':   'kfcs',
    #         'lastfile':  'iteration',
    #     }
    # ],
    'KCWI': [
        {
            'service':   'kfcs',
            'lastfile':  'lastfile',
        },
        {
            'service':   'kbds',
            'lastfile':  'loutfile',
        }
    ],
    'NIRES': [
        {
            'service':   '???',
            'lastfile':  '???',
            'outdir':    '???',
            'outfile':   '???',
            'sequence':  '???'
        },
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
    print(config)
    log = create_logger('testktlmonitor', config[instr]['ROOTDIR'], instr)
    log.info("Starting KOA KTL Monitor: " + ' '.join(sys.argv[0:]))

    #run monitor for each group defined for instr
    monitors = []
    for keys in instr_keys[instr]:
        mon = KtlMonitor(instr, keys)
        mon.start()
        monitors.append(mon)

    #stay alive until control-C to exit
    while True:
        try:
            time.sleep(300)
            log.debug('Monitor here just saying hi every 5 minutes')
        except:
            handle_error('WTF', traceback.format_exc())
            break
    log.info(f'Exiting {__file__}')



class KtlMonitor():
    '''
    Class to handle monitoring a distinct set of keywords for an instrument to 
    determine when a new image has been written.
    '''

    def __init__(self, instr, keys, queue_mgr=None):
        log.info(f"KtlMonitor: instr: {instr}, service: {keys['service']}")
        self.instr = instr
        self.keys = keys
        self.queue_mgr = queue_mgr

    def start(self):
        '''Start monitoring 'lastfile' keyword for new files.'''

        #These cache calls can throw exceptions (if instr server is down for example)
        #So, we should catch and retry until successful.  Be careful not to multi-register the callback
        try:
            #create service object for easy reads later
            keys = self.keys
            self.service = ktl.cache(keys['service'])

            #monitor keyword that indicates new file
            kw = ktl.cache(keys['service'], keys['lastfile'])
            kw.callback(self.on_new_file)

            # Prime callback to ensure it gets called at least once with current val
            if kw['monitored'] == True:
                print('primed')
                self.on_new_file(kw)
            else:
                kw.monitor()
            
        except Exception as e:
            handle_error('KTL_START_ERROR', "Could not start KTL monitoring.  Retrying in 60 seconds.")
            threading.Timer(60.0, self.start).start()
            return

    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''

        #todo: What is the best way to handle error/crashes in the callback?  Do we want the monitor to continue?
        #todo: Do we need to skip the initial read since that should be old? Can we check keyword time is old?
        try:
            now = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(now, keyword.name, keyword.ascii)

            if keyword['populated'] == False:
                log.warning(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            if len(keyword.history) <= 1:
                log.info(f'Skipping first value read assuming it is old. Val is {keyword.ascii}')
                return

            #get full file path
            #todo: For some instruments, we may need to form full path if lastfile is not defined.
            lastfile = keyword.ascii

            #check for blank lastfile
            if not lastfile or not lastfile.strip():
                log.warning(f"BLANK_FILE\t{self.instr}\t{keyword.service}")
                return

        except Exception as e:
            handle_error('KTL_READ_ERROR', traceback.format_exc())
            return

        #send back to queue manager
        if self.queue_mgr: 
            self.queue_mgr.add_to_queue(lastfile)



def create_logger(name, rootdir, instr):
    '''Creates a logger based on rootdir and instr.'''

    #create directory if it does not exist
    processDir = f'{rootdir}/{instr.upper()}'
    logFile =  f'{processDir}/{name}_{instr.upper()}.log'
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


def handle_error(errcode, text, check_time=True):
    log.error(f'{errcode}: {text}')


#--------------------------------------------------------------------------------
# main command line entry
#--------------------------------------------------------------------------------
if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        handle_error('APP ERROR', traceback.format_exc())
