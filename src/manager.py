'''
Script to manage single instance of a script (start/stop/restart) 

NOTE: Script is currently designed to assume this file exists 
in same directory as python module it wil start.

Example use:
python manager.py myApp start --interpreter kpython3 --port 55557 --extra "test"
'''
import argparse
import os
import subprocess
import psutil


def is_server_running(server, interpreter=None, port=None, extra=False, report=False):
    '''
    Returns PID if server is currently running (on same port), else 0
    '''
    matches = []
    chk_set = {server}

    if port:        chk_set.add(port)
    if extra:       chk_set.add(extra)
    if interpreter: chk_set.add(interpreter)
    print(chk_set)
    for proc in psutil.process_iter():
        pinfo = proc.as_dict(attrs=['name', 'username', 'pid', 'cmdline'])

        p_info = pinfo['cmdline']
        if not p_info:
            continue

        found = False
        if server in p_info:
            match = set(p_info).intersection(chk_set)
            if match == chk_set:
                found = True
        if found:
            matches.append(pinfo)

    if not matches:
        if report: print("WARN: NO MATCHING PROCESSES FOUND")
        return 0
    elif len(matches) > 1:
        if report: print("WARN: MULTIPLE MATCHES: \n" + str(matches))
        return 0 # matches[0]['pid']
    else:
        if report: print("FOUND PROCESS: " + str(matches[0]))
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
            extra = extra.split()
            cmd = cmd + extra
        if port:
            cmd.append('--port')
            cmd.append(port)
        print(f'Starting "{server}" with  the cmd:' + str(cmd))
        try:
            p = subprocess.Popen(cmd)
        except Exception as err:
            print('Error running command: ' + str(err))
        print('Done')


# ===================================== MAiN ===================================

# Define input parameters
parser = argparse.ArgumentParser(description='manager.py input parameters')
parser.add_argument('server', type=str, help='flask server module name')
parser.add_argument('command', type=str, help='start, stop, restart, check')
parser.add_argument("--port", type=str, dest="port", default=None,
                    help="Port to use for finding existing process and --port option to forward to app.")
parser.add_argument("--extra", type=str, dest="extra", default=None, help="Extra arguemnts string to pass to app")
parser.add_argument("--interpreter", type=str, default=None, help="Interpreter to call")

# Get input parameters
args = parser.parse_args()
server = args.server
command = args.command
port = args.port
extra = args.extra
interpreter = args.interpreter

# Verify command
assert command in ['start', 'stop', 'restart', 'check'], 'Incorrect command'

# get this script directory (assuming flask module exists here)
dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(dir)

# Check if server file exists
server = f'{dir}/{server}.py'
assert os.path.isfile(server), f'server module {server} does not exist'

# Check if server is running
pid = is_server_running(server, interpreter=interpreter, port=port, extra=extra)
# Do the request
if command == 'stop':
    pid = process_stop(pid)
elif command == 'start':
    process_start(pid, server, interpreter=interpreter, port=port, extra=extra)
elif command == 'restart':
    pid = process_stop(pid)
    process_start(pid, server, interpreter=interpreter, port=port, extra=extra)
elif command == 'check':
    pid = is_server_running(server, interpreter=interpreter, port=port, extra=extra, report=True)

exit()
