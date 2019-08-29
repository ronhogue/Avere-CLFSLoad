#
# clfsload/util.py
#
#-------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------

from __future__ import absolute_import
from __future__ import print_function

import collections
import datetime
import logging
import os
import pprint
import signal
import sys
import threading
import time
import traceback

import psutil

NSEC_PER_SEC_INT = 1000000000
NSEC_PER_SEC_FLOAT = float(NSEC_PER_SEC_INT)

# precomputed strings for struct.pack / struct.unpack
STRUCT_LE_U16 = '<H'
STRUCT_LE_U32 = '<I'
STRUCT_LE_U64 = '<Q'

class Size():
    KB = 1024
    MB = KB * 1024
    GB = MB * 1024
    TB = GB * 1024
    PB = TB * 1024
    EB = PB * 1024
    ZB = EB * 1024
    YB = ZB * 1024

LOGGER_NAME = 'clfsload'
UINT32_MAX = 0xffffffff
UINT64_MAX = 0xffffffffffffffff

LOG_FORMAT = "%(asctime)s %(levelname).3s %(message)s"

def logger_create(log_level, name=LOGGER_NAME):
    log_format = LOG_FORMAT
    logging.basicConfig(format=log_format)
    logger = logging.getLogger(name=name)
    logger.setLevel(log_level_get(log_level))
    return logger

def log_level_get(log_level):
    '''
    Given a log_level, attempt to translate it to an integer log level.
    '''
    if isinstance(log_level, int):
        return log_level
    tmpstr = str(log_level)
    try:
        return int(tmpstr)
    except ValueError:
        pass
    for tmp in [tmpstr, tmpstr.upper()]:
        try:
            return int(logging._nameToLevel[tmp]) # pylint: disable=protected-access
        except (AttributeError, KeyError, ValueError):
            pass
        try:
            val = getattr(logging, tmp)
            if isinstance(val, int):
                return val
        except (AttributeError, KeyError, ValueError):
            pass
    raise ValueError("cannot interpret %s '%s' as a log level" % (log_level.__class__.__name__, log_level))

def elapsed(ts0, ts1=None):
    '''
    Return the amount of time elapsed since ts0.
    If ts1 is provided, this is the time elapsed from ts0 to ts1.
    If ts1 is not provided, this is the time elapsed from ts0 to now.
    '''
    if ts1 is None:
        ts1 = time.time()
    if ts1 < ts0:
        # Most likely NTP adjusted the clock
        return 0
    return ts1 - ts0

class SorterBase(): # pylint: disable=eq-without-hash
    '''
    Python 3 does not have __cmp__. Here, we define various
    operators in terms of _cmp.
    '''
    def _cmp(self, other):
        raise NotImplementedError("%s did not implement this method" % self.__class__.__name__)
    def __lt__(self, other):
        return self._cmp(other) < 0
    def __le__(self, other):
        return self._cmp(other) <= 0
    def __eq__(self, other):
        return self._cmp(other) == 0
    def __ne__(self, other):
        return self._cmp(other) != 0
    def __ge__(self, other):
        return self._cmp(other) >= 0
    def __gt__(self, other):
        return self._cmp(other) > 0
    def __cmp__(self, other):
        return self._cmp(other)

class Monitor():
    '''
    Wrapper class that enables:
        with Monitor(lock, blocking=False) as m:
            if m.havelock:
                ...
    '''
    def __init__(self, rlock, blocking=True):
        self._rlock = rlock
        self._holder_thread = None
        self._havelock = rlock.acquire(blocking=blocking)
        if self._havelock:
            self._holder_thread = threading.current_thread()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()

    def __del__(self):
        self.release()

    @property
    def havelock(self):
        'Return whether the associated rlock is held'
        return self._havelock

    def release(self):
        'Release the lock iff it is held'
        if self._havelock:
            assert self._holder_thread == threading.current_thread()
            self._holder_thread = None
            self._havelock = False
            self._rlock.release()

    def acquire(self, blocking=True, timeout=-1):
        '''
        Acquire the lock
        '''
        if self._holder_thread == threading.current_thread():
            assert self._havelock
            return True
        ret = self._rlock.acquire(blocking=blocking, timeout=timeout)
        if ret:
            self._havelock = True
            self._holder_thread = threading.current_thread()
        return ret

def notify_all(cond):
    '''
    Do a notify_all() on the given threading.Condition().
    Assumes that the associated lock is RLock.
    '''
    with Monitor(cond, blocking=False) as m:
        if m.havelock:
            cond.notify_all()

def getframe(idx, include_lineno=True):
    '''
    Return a string of the form caller_name:linenumber.
    idx is the number of frames up the stack, so 1 = immediate caller.
    '''
    f = sys._getframe(idx+1) # pylint: disable=protected-access
    ret = f.f_code.co_name
    if include_lineno:
        ret += ':'
        ret += str(f.f_lineno)
    return ret

def exc_info_name():
    '''
    Invoked from within an exception handler. Returns the
    unqualified class name for the exception. Only use this
    in cases where you are special-casing a particular
    exception.
    '''
    exc_info = sys.exc_info()
    return exc_info[0].__name__.split('.')[-1]

def exc_info_err(*args):
    '''
    Invoked from within an exception handler. Returns a human
    representation of the error.
    '''
    b = ' '.join(args)
    exc_info = sys.exc_info()
    err = getattr(exc_info[0], '__name__', '').split('.')[-1]
    if str(exc_info[1]):
        if err:
            err += " "
        err += str(exc_info[1])
    if b:
        return b + ': ' + err
    return err

def exc_stacklines():
    '''
    Invoked from within an exception handler. Returns traceback.format_exc().splitlines()
    with the exception string removed from the end.
    '''
    exc_info = sys.exc_info()
    exception_class_names = [exc_info[0].__module__+'.'+exc_info[0].__qualname__, exc_info[0].__name__]
    del exc_info
    lines = traceback.format_exc().splitlines()
    lastline = lines[-1]
    for exception_class_name in exception_class_names:
        if lastline.startswith(exception_class_name+': '):
            lines = lines[:-1]
            break
    return lines

def exc_stack():
    '''
    Invoked from within an exception handler. Returns a human
    readable string for the exception stack.
    '''
    return pprint.pformat(exc_stacklines())

def exc_log(logger, log_level, *args):
    '''
    Log a message of the form:
        blah: error
        stack
        blah: error
    where blah is the provided *args
    '''
    exc_info = sys.exc_info()
    stack_is_interesting = getattr(exc_info[1], 'stack_is_interesting', True)
    del exc_info
    err = exc_info_err(*args)
    if stack_is_interesting:
        logger.log(log_level, "%s\n%s\n%s", err, exc_stack(), err)
    else:
        logger.log(log_level, "%s", err)
    return err

def pretty_time(ts):
    'Return a prettified string for (float) timestamp ts'
    ts_str = "%.9f" % ts
    secs_str = ts_str[:-10]
    nsecs_str = ts_str[-10:]
    dt = datetime.datetime.fromtimestamp(ts)
    dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    return secs_str + nsecs_str + ' (' + dt_str + nsecs_str + ')'

def remove_path(path):
    '''
    Remove the target path
    '''
    import shutil
    if os.path.isdir(path) and (not os.path.islink(path)):
        shutil.rmtree(path)
    else:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

class Psutil():
    '''
    Wrap up state for psutil
    '''
    _psutil_lock = threading.Lock()

    SIGKILL = signal.SIGKILL

    @classmethod
    def terminate_iff_child(cls, logger, pid, prefix=''):
        '''
        Terminate pid if and only if its parent is this process
        returns whether or not the signal was sent.
        '''
        with cls._psutil_lock:
            try:
                prefix = prefix.rstrip()
                if prefix:
                    prefix += ' '
                proc = psutil.Process(pid)
                assert proc.pid == pid
                if proc.ppid() == os.getpid():
                    os.kill(pid, cls.SIGKILL)
                    return True
            except psutil.NoSuchProcess:
                pass
            except Exception:
                err = "%sdid not terminate child %s" % (prefix, pid)
                exc_log(logger, logging.DEBUG, err)
                logger.warning("%s", err)
            return False

    @classmethod
    def get_child_pids(cls):
        '''
        Generate a list of process IDs that are children of this process,
        ordered from leaf to parent. This list includes all PIDs that
        have the PID of this process in their ancestry.
        The return value here is a tuple of (pids_to_kill, rows),
        where rows is suitable for passing to tabulate.tabulate()
        to show details for the contents of pids_to_kill.
        '''
        with cls._psutil_lock:
            process_attrs = ['pid', 'ppid', 'cmdline']
            children = dict() # key: pid ; value: list of immediate child pids
            processes = dict() # key: pid ; value: tuple(pid, ppid, command-line)
            for proc in psutil.process_iter():
                try:
                    if proc.status() == psutil.STATUS_ZOMBIE:
                        continue
                    pid = proc.pid
                    ppid = proc.ppid()
                    lst = children.setdefault(ppid, list())
                    lst.append(pid)
                    cmdline = proc.cmdline()
                    processes[pid] = (pid, ppid, cmdline)
                except psutil.NoSuchProcess:
                    pass
            pids_to_kill = sorted(children.get(os.getpid(), list()))
            pids_to_check = collections.deque(pids_to_kill)
            pids_checked = set() # paranoia: avoid circular checks in extreme pid recycling cases
            while pids_to_check:
                pid = pids_to_check.popleft()
                if pid not in pids_checked:
                    pids_checked.add(pid)
                    pids = sorted(children.get(pid, list()))
                    pids_to_kill.extend(pids)
                    pids_to_check.extend(pids)
            rows = [process_attrs]
            for pid in pids_to_kill:
                rows.append(processes[pid])
            return (pids_to_kill, rows)

    @classmethod
    def cleanup_children(cls, logger, log_if_no_children=True):
        '''
        Kill children of this process.
        Checks for respawning, but assumes that once
        there are no children found then no more will
        appear.
        '''
        from tabulate import tabulate
        tup = cls.get_child_pids()
        if not tup[0]:
            if log_if_no_children:
                logger.debug("cleanup_children: no child processes")
            return
        count = 0
        kill_failed = set()
        while tup[0]:
            sleep_secs = 0.0
            count += 1
            logger.debug("cleanup_children: found these processes to kill on pass %d\n%s",
                         count,
                         tabulate(tup[1], headers='firstrow'))
            for pid in tup[0]:
                try:
                    os.kill(pid, int(cls.SIGKILL))
                except Exception:
                    log_level = logging.WARNING if pid in kill_failed else logging.DEBUG
                    logger.log(log_level, "cleanup_children: error killing pid %d: %s",
                               pid, exc_info_err())
                    kill_failed.add(pid)
                    sleep_secs = 0.5
            time.sleep(sleep_secs)
            tup = cls.get_child_pids()
        logger.info("cleanup_children: no more children")
