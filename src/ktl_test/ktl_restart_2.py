#!/kroot/rel/default/bin/kpython3
'''
This is a distilled version of our KOA KTL Monitor.  Most of the noise is stripped out 
so we are just left with some simple code dealing with monitoring a keyword and 
restarting service if it detects a problem.

This version uses the the built-in "heartbeat" KTL function.  From what I can tell, 
the heartbeat function needs a keyword that is reliably periodically updated.  
Some instrument services do not have such a keyword.  So, if this is the method we 
need to use, we will need to add such a keyword for some services (UPTIME, ITERATION?)

Also, the automated heartbeat restart is hidden away.  So, to verify it is actually 
restarting, I set the period to some small value and we can see the on_new_file get
triggered again.

When this happens, the 'history' is not cleared so we need to store the last processed 
filepath to know if we need to skip it.

The disadvantage of this method is we are reliant on the heartbeat function working.  
I am under the impression it might have issues.  A direct read is perhaps a fail-proof
way to check the service is running?  I don't know how the heartbeat function works. Maybe
it is doing the same thing?

"
All hearbeats are monitored by a background FirstResponder thread that wakes up according 
to the most imminent expiration of any heartbeat's set period. If the heartbeat does not 
update within period seconds, an external check will be made to see whether the service is 
responding. If it is, and local broadcasts have not resumed, all Service instances 
corresponding to the affected KTL service will be resuscitated.
"

From what I observed, the second check to see if the service is responding may not be happening,
resulting in unnecessary resuscitation??

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
        self.last_filepath = None
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
            kw.monitor()
            kw.callback(self.on_new_file)

            # Prime callback to ensure it gets called at least once with current val
            if kw['monitored'] == True:
                self.on_new_file(kw)
            else:
                kw.monitor()

            #start heartbeat monitoring
            heartbeat = keys['heartbeat']
            if heartbeat:
                self.service.heartbeat(heartbeat, keys['hb_period'])

        except Exception as e:
            print(traceback.format_exc())
            msg = f"Could not start KTL monitoring for {self.instr} '{keys['service']}'.  Retry in 60 seconds."
            self.queue_mgr.handle_error('KTL_START_ERROR', msg)
            threading.Timer(KTL_START_RETRY_SEC, self.start).start()
            return


    def on_new_file(self, keyword):
        '''Callback for KTL monitoring.  Gets full filepath and takes action.'''

        try:
            # Confirm keyword object has a value before attempting to access the ascii value
            if keyword['populated'] == False:
                print(f"KEYWORD_UNPOPULATED\t{self.instr}\t{keyword.service}")
                return

            print(dt.datetime.now(), f'on_new_file: {keyword.name}={keyword.ascii}')
            filepath = keyword.ascii

            #assuming first read is old
            #NOTE: I don't think we could rely on a timestamp check vs now?
            if len(keyword.history) <= 1:
               print(f'Skipping (history <= 1)')
               self.last_filepath = filepath
               return

            #build filepath and make sure it is not the last value (ie restart)
            if filepath == self.last_filepath:
                print(f"Skipping (duplicate of last val)")
                return

        except Exception as e:
            self.queue_mgr.handle_error('KTL_READ_ERROR', traceback.format_exc())
            return

        #send back to queue manager        
        self.queue_mgr.add_to_queue(filepath)
        self.last_filepath = filepath


#start monitor
keys = {
    'service'  : 'kfcs',
    'keyword'  : 'UPTIME',
#    'keyword'  : 'LASTFILE',
    'heartbeat': 'ITERATION',
    'hb_period': 40
}
monitor = KtlMonitor("KCWI", keys, QueueMgr())
monitor.start()


#exit control
while True:
    try: time.sleep (10)
    except: break
print('Exiting...')
