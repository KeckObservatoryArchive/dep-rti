'''
Unit tests for monctl.py

Tests cover:
- Instrument list definitions (K1, K2, K0)
- Server detection by hostname
- Process-line parsing helpers (find_monitor_pids, get_all_monitor_lines)
- Command logic (start, stop, restart, status) using mocked subprocess/os.kill
'''

import os
import signal
import sys
import importlib
from unittest.mock import MagicMock, patch, call

import pytest

# Allow importing from parent src directory
parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

import monctl


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def make_ps_line(pid, cmd, args=''):
    '''Build a fake `ps -ef` output line.'''
    return f'rti    {pid}  1  0 00:00 ?  00:00:00 {cmd} {args}'


# ---------------------------------------------------------------------------
# Instrument list tests
# ---------------------------------------------------------------------------

class TestInstrumentLists:

    def test_k1_base_list_not_empty(self):
        assert len(monctl.K1_INST_BASE) > 0

    def test_k2_base_list_not_empty(self):
        assert len(monctl.K2_INST_BASE) > 0

    def test_k0_combines_k1_k2_inst(self):
        assert monctl.K0_INST_BASE == monctl.K1_INST_BASE + monctl.K2_INST_BASE

    def test_k0_combines_k1_k2_drp(self):
        assert monctl.K0_DRP_BASE == monctl.K1_DRP_BASE + monctl.K2_DRP_BASE

    def test_svr_map_contains_all_servers(self):
        for svr in ('k0', 'k1', 'k2'):
            assert svr in monctl.SVR_MAP

    def test_svr_map_has_required_keys(self):
        required = {'inst_base', 'inst_run', 'drp_base', 'drp_run'}
        for svr, cfg in monctl.SVR_MAP.items():
            assert required == set(cfg.keys()), f'SVR_MAP[{svr!r}] missing keys'

    def test_run_list_subset_of_base(self):
        '''Run list instruments should be a subset of the base list.'''
        for svr in ('k1', 'k2', 'k0'):
            cfg = monctl.SVR_MAP[svr]
            extra_inst = set(cfg['inst_run']) - set(cfg['inst_base'])
            assert not extra_inst, f'{svr} inst_run has entries not in inst_base: {extra_inst}'
            extra_drp = set(cfg['drp_run']) - set(cfg['drp_base'])
            assert not extra_drp, f'{svr} drp_run has entries not in drp_base: {extra_drp}'

    def test_k1_instruments_expected(self):
        assert 'kpf' in monctl.K1_INST_BASE
        assert 'hires' in monctl.K1_INST_BASE
        assert 'mosfire' in monctl.K1_INST_BASE

    def test_k2_instruments_expected(self):
        assert 'deimos_spec' in monctl.K2_INST_BASE
        assert 'kcwi_blue' in monctl.K2_INST_BASE
        assert 'nirc2' in monctl.K2_INST_BASE

    def test_k1_drp_expected(self):
        assert 'kpf' in monctl.K1_DRP_BASE
        assert 'mosfire' in monctl.K1_DRP_BASE
        assert 'osiris' in monctl.K1_DRP_BASE

    def test_k2_drp_expected(self):
        assert 'kcwi' in monctl.K2_DRP_BASE
        assert 'deimos' in monctl.K2_DRP_BASE
        assert 'esi' in monctl.K2_DRP_BASE


# ---------------------------------------------------------------------------
# Server detection tests
# ---------------------------------------------------------------------------

class TestServerDetection:

    def test_detect_known_k1_hostname(self):
        with patch.dict(monctl.HOSTNAME_MAP, {'svrk1': 'k1'}, clear=True):
            assert monctl.detect_server('svrk1') == 'k1'

    def test_detect_known_k2_hostname(self):
        with patch.dict(monctl.HOSTNAME_MAP, {'svrk2': 'k2'}, clear=True):
            assert monctl.detect_server('svrk2') == 'k2'

    def test_detect_known_k0_hostname(self):
        with patch.dict(monctl.HOSTNAME_MAP, {'svrbld': 'k0'}, clear=True):
            assert monctl.detect_server('svrbld') == 'k0'

    def test_detect_unknown_hostname_returns_none(self):
        assert monctl.detect_server('unknown-host') is None

    def test_get_hostname_returns_string(self):
        with patch('subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(stdout='myhost\n')
            result = monctl.get_hostname()
        assert result == 'myhost'


# ---------------------------------------------------------------------------
# Process discovery tests
# ---------------------------------------------------------------------------

SAMPLE_PS = [
    'rti    100  1  0 00:00 ?  00:00:00 python monitor.py mosfire',
    'rti    101  1  0 00:00 ?  00:00:00 python monitor.py hires',
    'rti    200  1  0 00:00 ?  00:00:00 python monitor_drp.py kcwi',
    'rti    201  1  0 00:00 ?  00:00:00 python monitor_drp.py deimos',
    'root     1  0  0 00:00 ?  00:00:00 /sbin/init',
    'rti    999  1  0 00:00 pts/0 00:00:00 grep monitor.py',  # grep itself
]


class TestFindMonitorPids:

    def test_find_raw_monitor_pid(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            pids = monctl.find_monitor_pids('mosfire', drp=False)
        assert pids == [100]

    def test_find_raw_monitor_pid_hires(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            pids = monctl.find_monitor_pids('hires', drp=False)
        assert pids == [101]

    def test_drp_monitor_not_returned_for_raw(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            pids = monctl.find_monitor_pids('kcwi', drp=False)
        assert pids == []

    def test_find_drp_monitor_pid(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            pids = monctl.find_monitor_pids('kcwi', drp=True)
        assert pids == [200]

    def test_raw_monitor_not_returned_for_drp(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            pids = monctl.find_monitor_pids('mosfire', drp=True)
        assert pids == []

    def test_grep_line_excluded(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            pids = monctl.find_monitor_pids('grep', drp=False)
        assert pids == []

    def test_not_running_returns_empty(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            pids = monctl.find_monitor_pids('nirspec_spec', drp=False)
        assert pids == []


class TestGetAllMonitorLines:

    def test_raw_lines_count(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            lines = monctl.get_all_monitor_lines(drp=False)
        assert len(lines) == 2

    def test_drp_lines_count(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            lines = monctl.get_all_monitor_lines(drp=True)
        assert len(lines) == 2

    def test_grep_excluded(self):
        with patch('monctl._ps_lines', return_value=SAMPLE_PS):
            lines = monctl.get_all_monitor_lines(drp=False)
        assert not any('grep' in l for l in lines)


# ---------------------------------------------------------------------------
# Command: start
# ---------------------------------------------------------------------------

class TestCmdStart:

    def test_start_launches_when_not_running(self):
        with patch('monctl.find_monitor_pids', return_value=[]), \
             patch('monctl.launch_monitor') as mock_launch, \
             patch('monctl.print_matching_lines'), \
             patch('time.sleep'):
            monctl.cmd_start('k1', ['hires'], [])
        mock_launch.assert_called_once_with('hires', drp=False)

    def test_start_skips_when_already_running(self):
        with patch('monctl.find_monitor_pids', return_value=[100]), \
             patch('monctl.launch_monitor') as mock_launch, \
             patch('time.sleep'):
            monctl.cmd_start('k1', ['hires'], [])
        mock_launch.assert_not_called()

    def test_start_drp_launches_when_not_running(self):
        def fake_pids(instr, drp=False):
            return []

        with patch('monctl.find_monitor_pids', side_effect=fake_pids), \
             patch('monctl.launch_monitor') as mock_launch, \
             patch('monctl.print_matching_lines'), \
             patch('time.sleep'):
            monctl.cmd_start('k1', [], ['kpf'])
        mock_launch.assert_called_once_with('kpf', drp=True)

    def test_start_drp_skips_when_already_running(self):
        with patch('monctl.find_monitor_pids', return_value=[200]), \
             patch('monctl.launch_monitor') as mock_launch, \
             patch('time.sleep'):
            monctl.cmd_start('k1', [], ['kpf'])
        mock_launch.assert_not_called()


# ---------------------------------------------------------------------------
# Command: stop
# ---------------------------------------------------------------------------

class TestCmdStop:

    def test_stop_sends_sigterm(self):
        with patch('monctl.find_monitor_pids', return_value=[100]), \
             patch('monctl.print_matching_lines'), \
             patch('os.kill') as mock_kill:
            monctl.cmd_stop('k1', ['hires'], [])
        mock_kill.assert_called_once_with(100, signal.SIGTERM)

    def test_stop_no_action_when_not_running(self):
        with patch('monctl.find_monitor_pids', return_value=[]), \
             patch('monctl.print_matching_lines'), \
             patch('os.kill') as mock_kill:
            monctl.cmd_stop('k1', ['hires'], [])
        mock_kill.assert_not_called()

    def test_stop_no_action_when_multiple_pids(self, capsys):
        with patch('monctl.find_monitor_pids', return_value=[100, 101]), \
             patch('monctl.print_matching_lines'), \
             patch('os.kill') as mock_kill:
            monctl.cmd_stop('k1', ['hires'], [])
        mock_kill.assert_not_called()
        captured = capsys.readouterr()
        assert 'Did not stop' in captured.out

    def test_stop_drp_sends_sigterm(self):
        def fake_pids(instr, drp=False):
            return [200] if drp else []

        with patch('monctl.find_monitor_pids', side_effect=fake_pids), \
             patch('monctl.print_matching_lines'), \
             patch('os.kill') as mock_kill:
            monctl.cmd_stop('k1', [], ['kpf'])
        mock_kill.assert_called_once_with(200, signal.SIGTERM)


# ---------------------------------------------------------------------------
# Command: restart
# ---------------------------------------------------------------------------

class TestCmdRestart:

    def test_restart_always_launches(self):
        with patch('monctl.launch_monitor') as mock_launch, \
             patch('monctl.find_monitor_pids', return_value=[100]), \
             patch('monctl.print_matching_lines'), \
             patch('time.sleep'):
            monctl.cmd_restart('k1', ['hires'], [])
        mock_launch.assert_called_once_with('hires', drp=False)

    def test_restart_drp_launches(self):
        with patch('monctl.launch_monitor') as mock_launch, \
             patch('monctl.find_monitor_pids', return_value=[200]), \
             patch('monctl.print_matching_lines'), \
             patch('time.sleep'):
            monctl.cmd_restart('k1', [], ['kpf'])
        mock_launch.assert_called_once_with('kpf', drp=True)


# ---------------------------------------------------------------------------
# print_summary smoke test
# ---------------------------------------------------------------------------

class TestPrintSummary:

    def test_summary_runs_without_error(self, capsys):
        with patch('monctl.get_all_monitor_lines', return_value=[]):
            monctl.print_summary(
                hostname='testhost',
                svr='k1',
                cmd='status',
                inst_base=monctl.K1_INST_BASE,
                inst_run=monctl.K1_INST_RUN,
                drp_base=monctl.K1_DRP_BASE,
                drp_run=monctl.K1_DRP_RUN,
            )
        captured = capsys.readouterr()
        assert 'Monitor Controller Summary' in captured.out
        assert 'testhost' in captured.out
        assert 'k1' in captured.out
