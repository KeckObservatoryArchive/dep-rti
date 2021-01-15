#!/kroot/rel/default/bin/kpython3
'''
This is a distilled version of our KOA KTL Monitor.  Most of the noise is stripped out 
so we are just left with some simple code dealing with monitoring a keyword and 
restarting service if it detects a problem.

This version does not use the built-in "heartbeat" KTL function but instead regularly 
tries to read a keyword from the service.  If it can't read it for whatever reason, 
it triggers a restart in code that deletes the service object and recreates it.

The reason I went this route was because the heartbeat function needs a keyword that is
reliably periodically updated.  Some instrument services do not have such a keyword.

Also, the automated heartbeat restart is hidden away.  By checking and restarting direct
in code, we can be made aware of the restarts which might be desireable.

However, I don't know if this is at all the correct way to do this.  This is what we've 
been using currently and we've seen some weirdness with the restarts though it is hard to
confirm
'''

import datetime as dt
import ktl
import time
import threading

#module globals
KTL_START_RETRY_SEC = 60.0
SERVICE_CHECK_SEC = 60.0


class QueueMgr():
    '''Dummy placeholder queue mgr for new file callback.'''
    def add_to_queue(self, file):
        print('add_to_queue: ', file)


class KtlMonitor():
    '''
    Class to handle monitoring a distinct keyword for an instrument to 
    determine when a new image has been written.

    Parameters:
        instr (str): Instrument to monitor.
        keys (dict): Defines service and keyword to monitor 
                     as well as special formatting to construct filepath.
        queue_mgr (obj): Class object that contains callback 'add_to_queue' function.
        log (obj): logger object
    '''
    def __init__(self, instr, keys, queue_mgr):
        self.instr = instr
        self.keys = keys
        self.queue_mgr = queue_mgr
        self.service = None
        self.restart = False
        print(f"KtlMonitor: instr: {instr}, service: {keys['service']}, keyword: {keys['keyword']}")


    def start(self):
        '''Start monitoring 'keyword' keyword for new files.'''

        #These cache calls can throw exceptions (if instr server is down for example)
        #So, we should catch and retry until successful.  Be careful not to multi-register the callback
        try:
            #delete service if exists
            if self.service:
                print('deleting service')
                del self.service
                self.service = None

            #create service object for easy reads later
            keys = self.keys
            self.service = ktl.Service(keys['service'])

            #monitor keyword that indicates new file
            kw = self.service[keys['keyword']]
            kw.callback(self.on_new_file)

            # Prime callback to ensure it gets called at least once with current val
            if kw['monitored'] == True:
                self.on_new_file(kw)
            else:
                kw.monitor()

        except Exception as e:
            print(traceback.format_exc())
            msg = f"Could not start KTL monitoring for {self.instr} '{keys['service']}'.  Retry in 60 seconds."
            self.queue_mgr.handle_error('KTL_START_ERROR', msg)
            threading.Timer(KTL_START_RETRY_SEC, self.start).start()
            return

        #Start an interval timer to periodically check that this service is running.
        threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()


    def check_service(self):
        '''
        Try to read heartbeat keyword from service.  If all ok, then check again in 1 minute.
        If we can't get a value, restart service monitoring.  
        '''
        heartbeat = self.keys.get('heartbeat')
        if not heartbeat: return

        try:
            val = self.service[heartbeat].read()
        except Exception as e:
            print(f"KTL read exception: {str(e)}")
            val = None

        if not val:
            msg = f"KTL service {self.instr} '{self.keys['service']}' is NOT running.  Restarting service."
            self.queue_mgr.handle_error('KTL_CHECK_ERROR', msg)
            self.restart = True
            self.start()
        else:
            threading.Timer(SERVICE_CHECK_SEC, self.check_service).start()

    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''

        try:
            print(dt.datetime.now(), f'on_new_file: {keyword.name}={keyword.ascii}')

            if keyword['populated'] == False:
                print(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            #assuming first read is old
            #NOTE: I don't think we could rely on a timestamp check vs now?
            if len(keyword.history) <= 1 or self.restart:
               print(f'Skipping (history <= 1)')
               self.restart = False
               return

            filepath = keyword.ascii

        except Exception as e:
            self.queue_mgr.handle_error('KTL_READ_ERROR', traceback.format_exc())
            return

        #send back to queue manager
        self.queue_mgr.add_to_queue(filepath)

    def do_restart(self):
        print("restart")
        self.restart = True
        self.start()


#start monitor
keys = {
    'service'  : 'kfcs',
    'keyword'  : 'UPTIME',
    # 'keyword'  : 'LASTFILE',
    'heartbeat': 'ITERATION'
}
monitor = KtlMonitor("KCWI", keys, QueueMgr())
monitor.start()


#manual restart cmd
while True:
    cmd = input("cmd: ")  
    if cmd == 'r':
        monitor.do_restart()

