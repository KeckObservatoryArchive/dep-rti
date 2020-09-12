'''
Script to manage single instance of a script (start/stop/restart) 

NOTE: Script is currently designed to assume this file exists 
in same directory as python module it wil start.

Example use:
python manager.py myApp start --interpreter kpython3 --port 55557 --extra "test"
'''
import argparse
import os
import sys
import subprocess
import psutil
import getpass


def is_server_running(server, interpreter=None, port=None, report=False):
    '''
    Returns PID if server is currently running (on same port), else 0
    '''

    matches = []
    current_user = getpass.getuser()
    list1 = []
    if interpreter: list.append(interpreter)
    list1.append(server)
    if port: list1.append(port)
    for proc in psutil.process_iter():
        pinfo = proc.as_dict(attrs=['name', 'username', 'pid', 'cmdline'])

        # if pinfo['username'] != current_user:
        #     continue

        found = 0
        for name in list1:
            for cmd in pinfo['cmdline']:
                if name in cmd: 
                    found += 1
        if found >= len(list1):
            matches.append(pinfo)

    if len(matches) == 0:
        if report: print("WARN: NO MATCHING PROCESSES FOUND")
        return 0
    elif len(matches) > 1:
        if report: print ("WARN: MULTIPLE MATCHES: \n" + str(matches))
        return matches[0]['pid']
    else:
        if report: print ("FOUND PROCESS: " + str(matches[0]))
        return matches[0]['pid']


def process_stop(pid):
    '''
    Use psutil to kill the process ID
    '''

    if pid == 0:
        print(server, 'is not running')
    else:
        print('Killing PID', pid)
        p = psutil.Process(pid)
        p.terminate()
        pid = 0

    return pid


def process_start(pid, server, interpreter=None, port=None, extra=None):
    '''
    Start the requested server
    '''

    if pid > 0:
        print(server, 'already running with PID', pid)
    else:
        cmd = []
        if interpreter:
            cmd.append(interpreter)
        cmd.append(server)
        if extra:
            cmd.append(extra)
        if port:
            cmd.append('--port')
            cmd.append(port)
        print(f'Starting "{server}" with  the cmd:' + str(cmd))
        try:
            p = subprocess.Popen(cmd)
        except Exception as e:
            print ('Error running command: ' + str(e))
        print ('Done')


#===================================== MAiN ===================================

# Define input parameters
parser = argparse.ArgumentParser(description='manager.py input parameters')
parser.add_argument('server', type=str, help='flask server module name')
parser.add_argument('command', type=str, help='start, stop, restart, check')
parser.add_argument("--port", type=str, dest="port", default=None, 
                    help="Port to use for finding existing process and --port option to forward to app.")
parser.add_argument("--extra", type=str, dest="extra", default=None, 
                    help="Extra arguemnts string to pass to app")
parser.add_argument("--interpreter", type=str, default=None, help="Interpreter to call")

# Get input parameters
args = parser.parse_args()
server  = args.server
command = args.command
port    = args.port
extra   = args.extra
interpreter = args.interpreter

# Verify command
assert command in ['start', 'stop', 'restart', 'check'], 'Incorrect command'

# get this script directory (assuming flask module exists here)
dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir)

# Check if server file exists
server = f'{dir}/{server}.py'
assert os.path.isfile(server), print(f'server module {server} does not exist')

# Check if server is running
pid = is_server_running(server, interpreter=interpreter, port=port)

# Do the request
if command == 'stop':
    pid = process_stop(pid)
elif command == 'start':
    process_start(pid, server, interpreter=interpreter, port=port, extra=extra)
elif command == 'restart':
    pid = process_stop(pid)
    process_start(pid, server, interpreter=interpreter, port=port, extra=extra)
elif command == 'check':
    pid = is_server_running(server, interpreter=interpreter, port=port, report=True)

exit()
