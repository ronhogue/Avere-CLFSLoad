#
# clfsload/pcom.py
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
'''
Types supporting multiprocessing communication
'''

from __future__ import absolute_import

import collections
import logging
import queue
import threading
import time
import traceback

from clfsload.types import GenericStats, Timer, WRock
from clfsload.util import elapsed, exc_log, getframe

class PcomBase():
    '''
    Base class for everything we shove back and forth
    through the multiprocessing.Queue between a worker
    thread and its associated Process.
    '''
    # nothing here

class PcomInit(PcomBase):
    '''
    Thread -> Process
    Sent once at start-of-day
    '''
    def __init__(self, run_options, writer, reader, readfile_local_class):
        self.run_options_json = run_options.to_json()
        self.writer_import_info = writer.get_import_info()
        self.writer_command_args_json = writer.command_args_to_json()
        self.writer_reconstitute_args = writer.reconstitute_args()
        self.writer_additional = writer.additional_to_json()
        self.reader_import_info = reader.get_import_info()
        self.reader_command_args_json = reader.command_args_to_json()
        self.reader_additional = reader.additional_to_json()
        self.readfile_local_import_info = readfile_local_class.get_import_info()

class PcomReady(PcomBase):
    '''
    Process -> Thread
    The process is ready to accept work
    '''
    def __init__(self, pid):
        self.pid = pid

class PcomTerminate(PcomBase):
    '''
    Thread -> Process or Process -> Thread
    Thread -> Process: Instructs the process to terminate
    Process -> Thread: Heads-up that the process is giving up
    '''
    # nothing here

class PcomTobjDone(PcomBase):
    '''
    Process -> Thread
    Work for the tobj with the given filehandle is done
    '''
    def __init__(self, tobj):
        self.tobj = tobj

class PcomStatsMax(PcomBase):
    '''
    Process -> Thread
    '''
    def __init__(self, name, value):
        self.name = name
        self.value = value

class PcomStatsUpdate(PcomBase):
    '''
    Process -> Thread
    '''
    def __init__(self, updates):
        self.updates = updates

class PcomTimerUpdate(PcomBase):
    '''
    Process -> Thread
    '''
    def __init__(self, updates):
        self.updates = updates

class PcomTransfer(PcomBase):
    '''
    Thread -> Process
    '''
    def __init__(self, tobj):
        self.tobj = tobj

class PcomReconcile(PcomBase):
    '''
    Thread -> Process
    '''
    def __init__(self, tobj):
        self.tobj = tobj

class _PcomExceptionWrapper(PcomBase):
    '''
    Process -> Thread
    Base class - only instantiate subclasses
    '''
    def __init__(self, exception):
        self.exception_json = exception.to_json()
        self.format_exc = traceback.format_exc()

class PcomSourceObjectError(_PcomExceptionWrapper):
    '''
    Process -> Thread
    '''
    # No specialization here

class PcomTargetObjectError(_PcomExceptionWrapper):
    '''
    Process -> Thread
    '''
    # No specialization here

class PcomServerRejectedAuthError(_PcomExceptionWrapper):
    '''
    Process -> Thread
    '''
    # No specialization here

class PcomDirectoryWrite(PcomBase):
    '''
    Thread -> Process
    '''
    def __init__(self, tobj, entry_file_path):
        self.tobj = tobj
        self.entry_file_path = entry_file_path

class ProcessQueues():
    '''
    Used within the Process (child) to wrap the queues
    '''
    def __init__(self, logger, wps, pqueue_r, pqueue_w):
        self.logger = logger
        self._wps = wps
        self.pqueue_r = pqueue_r
        self.pqueue_w = pqueue_w
        self._lock = threading.Lock()
        self._should_run = True
        self._queue_items_r = collections.deque()
        self._queue_items_w = collections.deque()

    @classmethod
    def from_WorkerProcessState(cls, logger, wps):
        '''
        Factory that generates from a WorkerProcessState object
        '''
        pqueue_r = wps.pqueue_wt # read by us
        pqueue_w = wps.pqueue_wp # written by us
        ret = cls(logger, wps, pqueue_r, pqueue_w)
        # Lose wps refs on the pqueues so we clean up okay
        del wps.pqueue_wt
        del wps.pqueue_wp
        return ret

    def __del__(self):
        if getattr(self, 'pqueue_r', None):
            self.pqueue_r.close()
        if getattr(self, 'pqueue_w', None):
            self.pqueue_w.close()

    @property
    def should_run(self):
        'accessor'
        return self._should_run

    @should_run.setter
    def should_run(self, value):
        'accessor - only accepts falsey and latches False'
        assert not value
        if self._should_run:
            self.logger.debug("%s should_run becomes False", self._wps)
        self._should_run = False

    def _read_queue_NL(self):
        '''
        Read all pending items on the queue.
        Process them or append them to the list.
        Caller holds self._lock.
        '''
        while True:
            if not self._should_run:
                return
            try:
                qi = self.pqueue_r.get(block=False, timeout=0.0)
            except queue.Empty:
                return
            if not self._qi_handle_NL(qi):
                self._queue_items_r.append(qi)

    def flush_write(self):
        '''
        Flush pending writes.
        '''
        try:
            with self._lock:
                self._write_queue_NL()
        except:
            exc_log(self.logger, logging.WARNING, "%s: flush_write" % self._wps)

    def _write_queue_NL(self):
        '''
        Push pending items into the queue.
        Caller holds self._lock.
        '''
        while self._queue_items_w:
            qi = self._queue_items_w[0]
            try:
                self.pqueue_w.put(qi, block=False, timeout=0.0)
            except queue.Full:
                return
            self._queue_items_w.popleft()

    def _qi_handle_NL(self, qi):
        '''
        qi is an item read from the queue. Examine it. Return True
        to indicate that the item is handled, False otherwise.
        Caller holds self._lock
        '''
        if isinstance(qi, PcomTerminate):
            self.logger.info("%s got terminate", self._wps)
            self.should_run = False
            # Do not send the terminate. The associated thread knows;
            # it told us to terminate.
            self._wps.send_terminate = False
            return True
        return False

    def get(self, timeout=60.0):
        '''
        Get the next item. Return that item or None on timeout.
        '''
        deadline = time.time() + timeout
        first_try = True
        with self._lock:
            self._write_queue_NL()
            ts_cur = time.time()
            while self._should_run and (first_try or (ts_cur < deadline)):
                first_try = False
                try:
                    self._read_queue_NL()
                except OSError as e:
                    # I wish there were a better way to match this
                    if str(e) == 'handle is closed':
                        self.logger.warning("%s %s.get() saw handle closed; exiting",
                                            self._wps, self.__class__.__name__)
                        raise SystemExit(1) from e
                    raise
                try:
                    return self._queue_items_r.popleft()
                except IndexError:
                    pass
                try:
                    qi = self.pqueue_r.get(block=True, timeout=elapsed(ts_cur, deadline))
                except queue.Empty:
                    return None
                if not self._qi_handle_NL(qi):
                    return qi
        return None

    def put(self, qi, block=True, timeout=10.0):
        '''
        Enqueue one item
        '''
        with self._lock:
            self._read_queue_NL()
            if self._queue_items_w:
                self._queue_items_w.append(qi)
                self._write_queue_NL()
            else:
                try:
                    self.pqueue_w.put(qi, block=block, timeout=timeout)
                    return
                except queue.Full:
                    self._queue_items_w.append(qi)
            if self._queue_items_w:
                self.logger.warning("%s ProcessQueues write backlog length is %d plus what is in the queue",
                                    self._wps, len(self._queue_items_w))

    @staticmethod
    def drain_queue_raw(rawqueue):
        '''
        Get all items from a queue
        '''
        ret = list()
        while True:
            try:
                ret.append(rawqueue.get(block=False, timeout=0.0))
            except queue.Empty:
                break
        return ret

class _StatBuf():
    '''
    Buffer stats updates. Shared between stats and timers
    for a ProcessWRock.
    '''
    def __init__(self, logger, pq):
        self.logger = logger
        self.pq = pq
        self.sb_lock = threading.Lock()
        self.sb_list = list()

    def _put_best_effort(self, item):
        '''
        Make a best-effort to send item, but do not block
        trying to get it done.
        '''
        try:
            self.pq.put(item, block=False, timeout=0.0)
        except Exception:
            exc_log(self.logger, logging.WARNING, getframe(1))

    def sb_append(self, item):
        '''
        Append item to the buffered list
        '''
        with self.sb_lock:
            self.sb_list.append(item)

    def sb_flush(self):
        with self.sb_lock:
            if self.sb_list:
                self._put_best_effort(self.sb_list)
                self.sb_list = list()

class _Stats():
    '''
    Implement ProcessWRock.stats.
    Pushes updates to the parent.
    '''
    def __init__(self, logger, pq, statbuf):
        self.logger = logger
        self.pq = pq
        self.statbuf = statbuf
        self._bufdelta = GenericStats()
        self._bufmax = GenericStats()

    _update_class = PcomStatsUpdate

    def stat_max(self, name, value):
        self._bufmax.stat_max(name, value)

    def stat_add(self, name, value):
        self._bufdelta.stat_add(name, value)

    def stat_inc(self, name):
        self._bufdelta.stat_inc(name)

    def stat_update(self, updates):
        self._bufdelta.stat_update(updates)

    def stats_flush(self):
        '''
        Flush pending updates
        '''
        tmp = self._bufdelta.reset()
        self.statbuf.sb_append(self._update_class(tmp))
        tmp = self._bufmax.reset()
        for name, value in tmp.items():
            self.statbuf.sb_append(PcomStatsMax(name, value))

class _TimerStats(_Stats):
    '''
    Implement ProcessWRock.timers.
    '''
    _update_class = PcomTimerUpdate

    def start(self, name):
        'start the named timer'
        return Timer(name, self)

    def stop_timer(self, timer_name, elapsed_secs):
        'Handle a timer stopping'
        self.stat_add(timer_name, elapsed_secs)

class ProcessWRock(WRock):
    '''
    ProcessWRock is a "worker rock" - it is passed through the writer
    stack in place of _WorkerThreadState when running in a child process.
    '''
    def __init__(self, logger, run_options, name, thread_num, pq):
        super(ProcessWRock, self).__init__(logger, run_options, name, thread_num, None)
        self.statbuf = _StatBuf(self.logger, pq)
        self.stats = _Stats(self.logger, pq, self.statbuf)
        self.timers = _TimerStats(self.logger, pq, self.statbuf)
        self.pq = pq
        self.process_wrock = None

    @property
    def should_run(self):
        'accessor'
        return self.pq.should_run

    @should_run.setter
    def should_run(self, value):
        'accessor'
        self.pq.should_run = value

    def stats_flush(self):
        '''
        Flush buffered stats.
        First flush the two stats types to the underlying buffer,
        then flush that buffer.
        '''
        self.stats.stats_flush()
        self.timers.stats_flush()
        self.statbuf.sb_flush()
