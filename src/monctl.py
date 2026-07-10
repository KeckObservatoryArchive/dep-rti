#!/usr/bin/env python3
'''
Monitor Controller - Routine Mode

Controls starting, stopping, restarting, and reporting status of RTI monitor processes.

Execute on an RTI ops or test server as the rti user:
  SVRK1 (k1 instruments), SVRK2 (k2 instruments), or SVRBLD (k0, k1 and k2 instruments combined)
  Servers are auto-detected by hostname.

Usage:
  monctl.py [cmd] [svr]

  cmd: restart | start | stop | status  (default: status)
  svr: k0 | k1 | k2  (only applies on test/build server; default: k0 = k1 + k2)

On ops servers (k1 or k2):
  monctl.py                    default cmd=status for the respective server
  monctl.py status             same as above
  monctl.py restart|start|stop run command on all instruments for detected server

On test/build server (k0):
  monctl.py                    default cmd=status, svr=k0 (k1+k2)
  monctl.py status             same as above
  monctl.py restart|start|stop runs for k0 (k1+k2) unless svr is specified
  monctl.py restart|start|stop k1|k2   runs for specified server only
'''

import argparse
import os
import signal
import subprocess
import sys
import time

# ===== Wait time (seconds) between successive start/restart commands =====
WAIT_SECS = 10

# Base install path used for launching monitor shell wrappers on ops systems
INSTALL_DIR = '/usr/local/koa/dep-rti/default/src'

# Hostname-to-server mapping  (replace ADD_SVR_NAME with actual hostnames)
HOSTNAME_MAP = {
    'ADD_SVR_NAME_K1': 'k1',
    'ADD_SVR_NAME_K2': 'k2',
    'ADD_SVR_NAME_K0': 'k0',
}

# ===== K1 Instrument Lists =====
# Base list (all K1 instruments) - do not edit unless adding/removing instruments
K1_INST_BASE = [
    'guiderk1', 'hires', 'kpf', 'lris_blue', 'lris_red', 'mosfire', 'osiris_img', 'osiris_spec',
]

# Run list - customise here if needed (e.g. comment out entries to skip)
# fmt: off
K1_INST_RUN = list(K1_INST_BASE)
#K1_INST_RUN = ['guiderk1', 'hires', 'lris_blue', 'lris_red', 'mosfire', 'osiris_img', 'osiris_spec']  # no KPF
#K1_INST_RUN = ['kpf']  # KPF only
# fmt: on

# K1 DRP lists
K1_DRP_BASE = ['kpf', 'mosfire', 'osiris']
K1_DRP_RUN = list(K1_DRP_BASE)
#K1_DRP_RUN = ['mosfire', 'osiris']  # no KPF DRP
#K1_DRP_RUN = ['kpf']  # KPF DRP only

# ===== K2 Instrument Lists =====
K2_INST_BASE = [
    'deimos_fcs', 'deimos_spec', 'esi', 'guiderk2', 'kcwi_blue', 'kcwi_fcs', 'kcwi_red',
    'nirc2_unp', 'nirc2', 'nires_img', 'nires_spec', 'nirspec_scam', 'nirspec_spec',
]
K2_INST_RUN = list(K2_INST_BASE)
#K2_INST_RUN = ['deimos_fcs', 'deimos_spec', 'guiderk2', 'kcwi_blue', 'kcwi_fcs', 'kcwi_red',
#               'nirc2_unp', 'nirc2', 'nires_img', 'nires_spec', 'nirspec_scam', 'nirspec_spec']  # no ESI
#K2_INST_RUN = ['esi']  # ESI only

K2_DRP_BASE = ['kcwi', 'deimos', 'esi', 'nirc2', 'nires']
K2_DRP_RUN = list(K2_DRP_BASE)
#K2_DRP_RUN = ['kcwi', 'deimos', 'nirc2', 'nires']  # no ESI DRP
#K2_DRP_RUN = ['esi']  # ESI DRP only

# ===== K0 Instrument Lists (build/test server = K1 + K2) =====
K0_INST_BASE = K1_INST_BASE + K2_INST_BASE
K0_INST_RUN  = K1_INST_RUN  + K2_INST_RUN
K0_DRP_BASE  = K1_DRP_BASE  + K2_DRP_BASE
K0_DRP_RUN   = K1_DRP_RUN   + K2_DRP_RUN

# Map each server key to its instrument/drp lists
SVR_MAP = {
    'k1': {'inst_base': K1_INST_BASE, 'inst_run': K1_INST_RUN,
            'drp_base':  K1_DRP_BASE,  'drp_run':  K1_DRP_RUN},
    'k2': {'inst_base': K2_INST_BASE, 'inst_run': K2_INST_RUN,
            'drp_base':  K2_DRP_BASE,  'drp_run':  K2_DRP_RUN},
    'k0': {'inst_base': K0_INST_BASE, 'inst_run': K0_INST_RUN,
            'drp_base':  K0_DRP_BASE,  'drp_run':  K0_DRP_RUN},
}


# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------

def get_hostname():
    '''Return the short hostname of the current machine.'''
    result = subprocess.run(['hostname', '-s'], capture_output=True, text=True)
    return result.stdout.strip()


def detect_server(hostname):
    '''Return server key (k0/k1/k2) for the given hostname, or None if unknown.'''
    return HOSTNAME_MAP.get(hostname)


def _ps_lines():
    '''Return all lines from `ps -ef`.'''
    result = subprocess.run(['ps', '-ef'], capture_output=True, text=True)
    return result.stdout.splitlines()


def find_monitor_pids(instr, drp=False):
    '''
    Find PIDs of running monitor processes for *instr*.

    For raw monitors:  matches lines containing "monitor.py" but NOT "monitor_drp".
    For DRP monitors:  matches lines containing "monitor_drp".
    In both cases the instrument name must appear as a standalone word.

    Returns a list of integer PIDs.
    '''
    pids = []
    for line in _ps_lines():
        if 'grep' in line:
            continue
        if drp:
            if 'monitor_drp' not in line:
                continue
        else:
            if 'monitor.py' not in line:
                continue
            if 'monitor_drp' in line:
                continue
        parts = line.split()
        if instr in parts:
            try:
                pids.append(int(parts[1]))
            except (IndexError, ValueError):
                pass
    return pids


def get_all_monitor_lines(drp=False):
    '''Return sorted list of `ps -ef` lines for all running monitor processes.'''
    lines = []
    for line in sorted(_ps_lines()):
        if 'grep' in line:
            continue
        if drp:
            if 'monitor_drp' not in line:
                continue
        else:
            if 'monitor.py' not in line:
                continue
            if 'monitor_drp' in line:
                continue
        lines.append(line)
    return lines


def print_matching_lines(instr, drp=False):
    '''Print any `ps -ef` lines that match the given instrument monitor.'''
    for line in _ps_lines():
        if 'grep' in line:
            continue
        if drp:
            if 'monitor_drp' not in line:
                continue
        else:
            if 'monitor.py' not in line:
                continue
            if 'monitor_drp' in line:
                continue
        parts = line.split()
        if instr in parts:
            print(f'   {line}')


# ---------------------------------------------------------------------------
# Launch helpers
# ---------------------------------------------------------------------------

def launch_monitor(instr, drp=False):
    '''
    Launch a monitor process for *instr* using the shell wrapper scripts.
    The process is started detached (stdout/stderr discarded).
    '''
    script = os.path.join(INSTALL_DIR, 'monitor_drp.sh' if drp else 'monitor.sh')
    subprocess.Popen([script, instr], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

def cmd_restart(svr, inst_run, drp_run):
    '''Restart all raw and DRP monitors (unconditional stop + start).'''
    print(f'Restarting {svr} Monitors\n')
    for instr in inst_run:
        launch_monitor(instr, drp=False)
        pids = find_monitor_pids(instr, drp=False)
        pid_str = str(pids[0]) if pids else 'N/A'
        print(f'   Re/Started {instr} monitor as PID={pid_str}')
        print_matching_lines(instr, drp=False)
        print(f'   Sleeping {WAIT_SECS}...\n')
        time.sleep(WAIT_SECS)

    print(f'Restarting {svr} DRP Monitors\n')
    for instr in drp_run:
        launch_monitor(instr, drp=True)
        pids = find_monitor_pids(instr, drp=True)
        pid_str = str(pids[0]) if pids else 'N/A'
        print(f'   Started PID={pid_str}')
        print_matching_lines(instr, drp=True)
        print(f'   Sleeping {WAIT_SECS}...\n')
        time.sleep(WAIT_SECS)


def cmd_start(svr, inst_run, drp_run):
    '''Start raw and DRP monitors that are not already running.'''
    print(f'Launching {svr} Monitors\n')
    for instr in inst_run:
        pids = find_monitor_pids(instr, drp=False)
        if not pids:
            launch_monitor(instr, drp=False)
            pids = find_monitor_pids(instr, drp=False)
            pid_str = str(pids[0]) if pids else 'N/A'
            print(f'   Started PID={pid_str} monitor for {instr} since it was not running')
            print_matching_lines(instr, drp=False)
            print(f'   Sleeping {WAIT_SECS}...\n')
            time.sleep(WAIT_SECS)
        else:
            print(f'   {instr} monitor not started since PID={pids[0]} is already running\n')

    print(f'Launching {svr} DRP Monitors\n')
    for instr in drp_run:
        pids = find_monitor_pids(instr, drp=True)
        if not pids:
            launch_monitor(instr, drp=True)
            pids = find_monitor_pids(instr, drp=True)
            pid_str = str(pids[0]) if pids else 'N/A'
            print(f'   Started PID={pid_str} DRP monitor for {instr} since it was not running')
            print_matching_lines(instr, drp=True)
            print(f'   Sleeping {WAIT_SECS}...\n')
            time.sleep(WAIT_SECS)
        else:
            print(f'   {instr} DRP monitor not started since PID={pids[0]} is already running\n')


def cmd_stop(svr, inst_run, drp_run):
    '''Send SIGTERM to all running raw and DRP monitors.'''
    print(f'Terminating {svr} Monitors\n')
    for instr in inst_run:
        print_matching_lines(instr, drp=False)
        pids = find_monitor_pids(instr, drp=False)
        if pids:
            if len(pids) == 1:
                os.kill(pids[0], signal.SIGTERM)
                print(f'   Stopped monitor for PID={pids[0]}\n')
            else:
                print(f'Did not stop since {len(pids)} {instr} processes are still active')
        else:
            print(f'   {instr} Monitor not terminated since it was not running\n')

    print(f'Terminating {svr} DRP Monitors\n')
    for instr in drp_run:
        print_matching_lines(instr, drp=True)
        pids = find_monitor_pids(instr, drp=True)
        if pids:
            if len(pids) == 1:
                os.kill(pids[0], signal.SIGTERM)
                print(f'   Stopped DRP monitor for PID={pids[0]}\n')
            else:
                print(f'Did not stop since {len(pids)} {instr} processes are still active')
        else:
            print(f'   {instr} DRP Monitor not terminated since it was not running\n')


def print_summary(hostname, svr, cmd, inst_base, inst_run, drp_base, drp_run):
    '''Print a summary of running monitors and controller settings.'''
    print('\n=====================================================\n')
    print(f'Monitor Controller Summary for {hostname} ({svr}):')
    print(f'{WAIT_SECS} secs delay between start commands')
    print(f'Command: {cmd}\n')

    mon_lines = get_all_monitor_lines(drp=False)
    print(f'=== Raw (L0) Base Instrument Monitors: {len(inst_base)} Possible ===\n'
          f'[{" ".join(inst_base)}]\n')
    print(f'Requested Instruments: {len(inst_run)}\n[{" ".join(inst_run)}]\n')
    print(f'Running: {len(mon_lines)}')
    for line in mon_lines:
        print(line)

    drp_lines = get_all_monitor_lines(drp=True)
    print(f'\n=== DRP Base Instrument Monitors: {len(drp_base)} Possible ===\n'
          f'[{" ".join(drp_base)}]\n')
    print(f'Requested DRP Instruments: {len(drp_run)}\n[{" ".join(drp_run)}]\n')
    print(f'Running: {len(drp_lines)}')
    for line in drp_lines:
        print(line)

    print('\nNotes:')
    print(' - For any mismatched counts, wait a few seconds, then re-run monctl for status.')
    print(' - Default run list includes all instruments. Terminate undesired monitors manually or'
          ' create custom list(s).')
    print(' - If multiple child processes persist per instrument, run monctl for status then')
    print('      wait until child processes complete and re-run monctl stop for parent.')
    print('      Otherwise manually terminate processes(s), since they are likely stuck.')
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Monitor Controller: manage RTI monitor processes',
        usage='%(prog)s [cmd] [svr]',
    )
    parser.add_argument(
        'cmd', nargs='?', default='status',
        choices=['status', 'restart', 'start', 'stop'],
        help='Command to execute (default: status)',
    )
    parser.add_argument(
        'svr_arg', nargs='?', default=None,
        choices=['k0', 'k1', 'k2'],
        metavar='svr',
        help='Server override - k0|k1|k2 (only effective on the build/test k0 server)',
    )

    args = parser.parse_args()
    cmd = args.cmd

    hostname = get_hostname()
    svr = detect_server(hostname)

    if svr is None:
        print(f'\nInvalid server {hostname}\n')
        sys.exit(1)

    # Allow overriding the target server on the k0 (build/test) machine only
    if svr == 'k0' and args.svr_arg is not None:
        svr = args.svr_arg

    if svr not in SVR_MAP:
        print(f'\nInvalid server arg: {svr}\n')
        sys.exit(1)

    inst_base = SVR_MAP[svr]['inst_base']
    inst_run  = SVR_MAP[svr]['inst_run']
    drp_base  = SVR_MAP[svr]['drp_base']
    drp_run   = SVR_MAP[svr]['drp_run']

    print()

    if cmd == 'restart':
        cmd_restart(svr, inst_run, drp_run)
    elif cmd == 'start':
        cmd_start(svr, inst_run, drp_run)
    elif cmd == 'stop':
        cmd_stop(svr, inst_run, drp_run)
    # 'status' falls through - summary is always printed below

    print_summary(hostname, svr, cmd, inst_base, inst_run, drp_base, drp_run)


if __name__ == '__main__':
    main()
