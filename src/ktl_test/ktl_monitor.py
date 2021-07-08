#!/kroot/rel/default/bin/kpython3
'''
The most basic KOA KTL monitoring test with heartbeat restart and logging.
'''
import os
import sys
import datetime as dt
import ktl
import time
import traceback
import logging as log
import argparse
import threading
import re

import monitor_config



#module globals
KTL_START_RETRY_SEC = 60.0
SERVICE_CHECK_SEC = 60.0
QUEUE_CHECK_SEC = 60.0


def main():

    # parse args 
    parser = argparse.ArgumentParser()
    parser.add_argument("service", type=str, help="Service to monitor.")
    args = parser.parse_args()
    service = args.service

    #log init
    log.basicConfig(filename=f'/usr/local/home/koarti/log/test_ktl_monitor_{service}.log', 
                    level=log.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
    log.debug(f'START monitoring {service}')

    #start monitor
    keys = monitor_config.instr_keymap[service]
    mon = KtlMonitor(service, keys)
    mon.start()

    #stay alive until control-C to exit
    while True:
        try:
            time.sleep(300)
        except:
            break
    log.info(f'Exiting {__file__}')


class KtlMonitor():
    '''
    Class to handle monitoring a distinct keyword for an instrument to 
    determine when a new image has been written.

    Parameters:
        servicename (str): KTL service to monitor.
        keys (dict): Defines service and keyword to monitor 
                     as well as special formatting to construct filepath.
        log (obj): logger object
    '''
    def __init__(self, servicename, keys):
        self.servicename = servicename
        self.keys = keys
        self.service = None
        self.restart_count = 0
        self.resuscitations = None
        self.instr = keys['instr']
        log.info(f"KtlMonitor: instr: {self.instr}, service: {servicename}, trigger: {keys['trigger']}")


    def start(self):
        '''Start monitoring 'trigger' keyword for new files.'''

        #Get service instance.  Keep retrying if it fails.
        try:
            self.service = ktl.Service(self.servicename)
        except Exception as e:
            log.error(traceback.format_exc())
            msg = (f"Could not start KTL monitoring for {self.instr} '{self.service}'. "
                   f"Retry in {KTL_START_RETRY_SEC} seconds.")
            log.error('KTL_START_ERROR: ' + msg)
            threading.Timer(KTL_START_RETRY_SEC, self.start).start()
            return

        #get keyword that indicates new file and define callback
        kw = self.service[self.keys['trigger']]
        kw.callback(self.on_new_file)

        # Start monitoring. Prime callback to ensure it gets called at least once with current val
        if kw['monitored'] == True:
            self.on_new_file(kw)
        else:
            kw.monitor()

        #establish heartbeat restart mechanism and service check interval
        hb = self.keys.get('heartbeat')
        if hb: 
            period = hb[1] + 10 #add 10 seconds of padding
            if period < 30: period = 30 #not too small
            self.service.heartbeat(hb[0], period)

            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start() 
            self.check_failed = False           
            self.resuscitations = self.service.resuscitations


    def check_service(self):
        '''
        Periodically check that service is still working with a read of heartbeat keyword.
        Also keep tabs on resuscitation value and logs when it changes. This should indicate
        service reconnect.
        '''
        try:
            hb = self.keys['heartbeat'][0]
            kw = self.service[hb]
            kw.read(timeout=1)
            if self.service.resuscitations != self.resuscitations:
                log.debug(f"KTL service {self.servicename} resuscitations changed.")
            self.resuscitations = self.service.resuscitations
        except Exception as e:
            self.check_failed = True
            log.debug(f"{self.instr} KTL service '{self.servicename}' heartbeat read failed.")
            log.error('KTL_SERVICE_CHECK_FAIL: ' + self.servicename)
        else:
            if self.check_failed:
                log.debug(f"KTL service {self.servicename} read successful afer prior failure.")
            self.check_failed = False
        finally:
            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()


    def on_new_file(self, kw):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''
        try:
            log.debug(f'on_new_file: '
                f'\tservice={kw.service}'
                f'\tname={kw.name}'
                f'\tascii={kw.ascii}'
                f'\tfull_name={kw.full_name}'
                f'\theartbeats={kw.heartbeats}'
                f'\ttimestamp={kw.timestamp}'
                f'\terror={kw.error}'
                f'\tcourier={kw.courier}'
                f'\tktlc={kw.ktlc}'
                f'\tnotification={kw.notification}'
                f'\tservice_notification={kw.service_notification}'
                f'\tsource={kw.source}'
                f'\tservers={kw.servers}'
                f'\tservice.name={self.service.name}'
                f'\tservice.courier={self.service.courier}'
                f'\tservice.dispatcher={self.service.dispatcher}'
                f'\tservice.ktlc={self.service.ktlc}'
                )
        except Exception as e:
            log.error('KTL_READ_ERROR: ', traceback.format_exc())
            return
        #self.queue_mgr.add_to_queue(keyword.ascii)


#--------------------------------------------------------------------------------
# main command line entry
#--------------------------------------------------------------------------------
if __name__ == '__main__':
    main()
