#!/usr/bin/env python3
#
# clfsload/core.py
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

import io
import json
import logging
import math
import multiprocessing
import os
import pprint
import queue
import random
import struct
import sys
import threading
import time

from clfsload.clfsutils import fnv1_hash
from clfsload.db import ClfsLoadDB, PreclaimState
from clfsload.parse import CLFSSegment
from clfsload.pcom import PcomDirectoryWrite, PcomInit, \
                          PcomReady, PcomReconcile, \
                          PcomServerRejectedAuthError, \
                          PcomSourceObjectError, PcomStatsMax, \
                          PcomStatsUpdate, PcomTargetObjectError, PcomTerminate, \
                          PcomTimerUpdate, \
                          PcomTobjDone, PcomTransfer, ProcessQueues, ProcessWRock
from clfsload.reader import ReaderPosix, ReadFilePosix
from clfsload.stypes import BackpointerLimitReached, CLFS_LINK_MAX, \
                            CLFSCompressionType, CLFSEncryptionType, CleanupStats, \
                            CommandArgs, ContainerTerminalError, \
                            DbEntMetaKey, DbInconsistencyError, DbTerminalError, \
                            DryRunResult, ExistingTargetObjKey, Filehandle, \
                            FILEHANDLE_NULL, Ftype, FinalizeStats, GenericStats, \
                            HistoricalPerf, InitStats, NamedObjectError, \
                            Phase, ReconcileStats, RunOptions, ServerRejectedAuthError, \
                            SimpleError, SourceObjectError, \
                            TargetObj, TargetObjState, TargetObjectError, \
                            TerminalError, TimerStats, TransferStats, \
                            WRock, WriteFileError, target_obj_state_name
from clfsload.util import LOG_FORMAT, Monitor, Psutil, \
                          STRUCT_LE_U16, STRUCT_LE_U32, Size, UINT32_MAX, \
                          elapsed, \
                          exc_info_err, exc_info_name, exc_log, exc_stack, \
                          getframe, logger_create, notify_all
from clfsload.version import VERSION_STRING
from clfsload.writerazure import WriterAzure

RERUN_STR = 'One or more errors; run again with --retry_errors=1 for recovery.'
SAS_DOC_URL = 'https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1'
LOCAL_STATE_PATH_UNIQ_STR = 'The local_state_path must be a locally-mounted path that is not part of the transfer.'

class ThreadSharedState():
    '''
    State shared across threads
    should_run: Used to tell all threads to exit
    '''
    def __init__(self):
        self._should_run = True
        self.server_rejected_auth = False

    @property
    def should_run(self):
        return self._should_run

    @should_run.setter
    def should_run(self, value):
        if not isinstance(value, bool):
            raise TypeError('should_run')
        if value:
            raise ValueError('should_run')
        self._should_run = False

class FileCleaner():
    '''
    State for the FileCleaner thread. This thread runs in the background.
    It is fed the paths of files in the local state directory and removes
    them. This is what cleans the directory .entries files. Cleanup is
    best-effort; if a file cannot be removed it is left behind. It does no
    long-term tracking; the pending list is lost across restarts.
    The thread runs through all phases. There is no reason to block transitioning
    out of the transfer phase (where all of this thread's work is generated)
    just because removes are not done; we can continue removes in the background.
    It's okay to complete the run without completing all removes.
    '''
    def __init__(self, thread_shared_state, logger):
        self._thread_shared_state = thread_shared_state
        self._logger = logger
        self._name = "filecleaner"
        self._should_run = True
        self._started = False
        self._running = False
        self._fc_lock = threading.Lock()
        self._fc_cond = threading.Condition(lock=self._fc_lock)
        self._pending_fh = list()
        self._path_proc = None

    def __str__(self):
        return self._name

    def __repr__(self):
        return self._name

    @property
    def should_run(self):
        'accessor'
        return self._should_run and self._thread_shared_state.should_run

    @should_run.setter
    def should_run(self, value):
        'accessor'
        with self._fc_cond:
            value = bool(value)
            assert not value
            self._should_run = value
            self._fc_cond.notify_all()

    def start(self, path_proc):
        'start the thread'
        with self._fc_cond:
            assert not self._started
            self._path_proc = path_proc
            self._started = True
            self._running = True
            # Do not save a pointer to self._thread - circular dependency
            thread = threading.Thread(target=self._run, name=self._name)
            thread.start()

    def stop(self):
        'stop the thread and wait for it to finish'
        with self._fc_cond:
            self._should_run = False
            self._fc_cond.notify_all()
            while self._started and self._running:
                self._fc_cond.wait(timeout=5.0)
            self._path_proc = None # discard reference to transfer worker pool

    def handoff(self, filehandle_list):
        '''
        filehandle_list is a list of Filehandle objects
        whose corresponding .entries files should be removed.
        This operation takes ownership of filehandle_list.
        '''
        with self._fc_cond:
            if self._pending_fh:
                self._pending_fh.extend(filehandle_list)
            else:
                self._pending_fh = filehandle_list
            self._fc_cond.notify_all()

    def _run(self):
        '''
        main loop for file cleaner background thread
        '''
        try:
            while self.should_run:
                self._run_internal()
        finally:
            with self._fc_cond:
                self._running = False
                self._fc_cond.notify_all()

    def _run_internal(self):
        'one iteration of thread main loop'
        fhs = list()
        with self._fc_cond:
            while self.should_run and (not self._pending_fh):
                self._fc_cond.wait(timeout=5.0)
            fhs = self._pending_fh
            self._pending_fh = list()
        for fh in fhs:
            if not self.should_run:
                return
            try:
                path = self._path_proc(fh)
                os.unlink(path)
            except FileNotFoundError:
                pass
            except Exception:
                exc_log(self._logger, logging.WARNING, str(self))

class WorkerThreadState(WRock):
    '''
    State specific to a single worker thread
    See WRock for some specific requirements.
    '''
    def __init__(self, thread_shared_state, logger, run_options, phase, thread_num, stats_class, writer):
        # Do not store a backpointer to the pool here. We do
        # not want circular references between the pool and its threads.
        # Similarly, do not store a backpointer to clfsload.
        name = "thread-%s-%s" % (phase, thread_num)
        super(WorkerThreadState, self).__init__(logger, run_options, name, thread_num, writer)
        self.stats = stats_class()
        self.timers = TimerStats()
        self.thread_shared_state = thread_shared_state
        self.phase = phase
        self.thread = None
        self.process = None
        self.process_ready = None
        self.process_pid = None
        self._process_okay = None
        self.pqueue_wt = multiprocessing.Queue()
        self.pqueue_wt.cancel_join_thread()
        self.pqueue_wp = multiprocessing.Queue()
        self.pqueue_wp.cancel_join_thread()
        self.idle_timer = None
        self.wps = None # WorkerProcessState

    @property
    def should_run(self):
        'accessor'
        return self.thread_shared_state.should_run

    @property
    def process_okay(self):
        'accessor'
        return self._process_okay

    @process_okay.setter
    def process_okay(self, value):
        'accessor'
        value = bool(value)
        self._process_okay = value

    def exc_error_exception(self, pool, what=''):
        'called from any exception handler'
        self.stats.stat_inc('error_count')
        e = sys.exc_info()[1]
        txt = str(self)
        if what:
            txt += " %s" % what
        if isinstance(e, SourceObjectError):
            txt += " source='%s'" % e.name
        elif isinstance(e, TargetObjectError):
            txt += " filehandle=%s" % e.filehandle.hex()
        exc_log(self.logger, logging.ERROR, txt)
        pool.error_poke()

    def process_send_stop(self):
        '''
        Send a stop message to the process.
        Best effort. Does not guarantee that the
        process is terminated.
        '''
        if self.process:
            logger = self.logger
            try:
                closed = getattr(self.pqueue_wt, '_closed', False)
                if not closed:
                    self.pqueue_wt.put(PcomTerminate(), block=True, timeout=30.0)
            except:
                logger.warning("%s cannot instruct process to terminate: %s", self, exc_info_err())
            self.process_okay = False

    def process_join(self):
        '''
        Best-effort wait for the associated process to finish.
        '''
        if self.process:
            logger = self.logger
            try:
                count = 0
                while (self.process.exitcode is None) and (count < 30):
                    count += 1
                    ProcessQueues.drain_queue_raw(self.pqueue_wp)
                    self.process.join(timeout=5.0)
            except:
                logger.warning("%s cannot join process: %s", self, exc_info_err())
            if getattr(self.process, 'exitcode', None) is None:
                logger.warning("%s process did not exit", self)

class WorkerProcessState():
    '''
    State specific to the multiprocessing.Process associated
    with one worker thread. Keep this minimal; it is pushed
    through to the Process.
    '''
    def __init__(self, name, run_options, pqueue_wt, pqueue_wp, local_state_path):
        self.name = name
        self.run_options = run_options
        self.pqueue_wt = pqueue_wt # wt: written by thread
        self.pqueue_wp = pqueue_wp # wp: written by process
        self.local_state_path = local_state_path

        # Used within the Process
        self.send_terminate = False

        # Do not initialize these things here - they are
        # either created in the child or arrive via pqueue.
        self.logger = None
        self.reader = None
        self.writer = None
        self.readfile_local_class = None

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def logfile_path(self):
        'Path to the logfile for this process'
        logfile_name = "output.%s.txt" % self.name
        return os.path.join(self.local_state_path, logfile_name)

class WorkerPool():
    'A pool of worker threads'

    _phase = None
    _stats_class = GenericStats
    _use_process = True

    _MAX_HISTORICAL_STATS_SECONDS = 120

    WAIT_REMOTE_POLL = 30.0

    # How long to wait (seconds) for a child process to be ready
    WAIT_CHILD_PROCESS_READY = 5.0

    def __init__(self, clfsload, num_threads):
        self._clfsload = clfsload
        self._num_threads = num_threads
        self._report_interval = 2.0 if clfsload.azcopy else 30.0
        self._report_time_next = None
        self._report_time_last = None
        self._state_lock = clfsload.db_lock
        self._state_cond = threading.Condition(lock=self._state_lock) # Waited on by pool master
        self._work_cond = threading.Condition(lock=self._state_lock) # Waited on by worker threads
        self._idle_count = 0 # (self._state_lock) Number of threads waiting for work
        self._idle_events = 0 # (self._state_lock) Number of times a thread went idle
        self._done_count = 0 # (self._state_lock) Number of threads that have exited or are exiting
        self._started_count = 0 # (self._state_lock) Number of threads that have started
        self._stopped_count = 0 # (self._state_lock) Number of threads that have stopped
        self._stop = False # (self._state_lock) Threads should stop
        self._aborting = False
        self._thread_states = list()
        self._t0 = None
        self._t1 = None
        self._pcom_init = None
        self._execute_work_items = True # Setting False implies a terminal error
        self._error_limit_reached = False
        if clfsload.subprocess_enable:
            self._pcom_init = PcomInit(self._clfsload.run_options, self.writer, self.reader, self._clfsload.readfile_local_class)
        else:
            self._use_process = False
        self._historical_perf = None
        self.work_count = None
        self._azcopy_pct_last = 0.0
        try:
            self._phase_name = self._clfsload.db.phase.azcopy_name()
        except AttributeError:
            self._phase_name = ''
        self._stats_t0 = None
        self._stats_throughput_last_time = None

    @property
    def db(self):
        return self._clfsload.db

    @property
    def reader(self):
        return self._clfsload.reader

    @property
    def writer(self):
        return self._clfsload.writer

    @property
    def errors_max(self):
        return self._clfsload.errors_max

    @property
    def logger(self):
        return self._clfsload.logger

    @property
    def thread_shared_state(self):
        return self._clfsload.thread_shared_state

    @property
    def error_count(self):
        'Total number of errors encountered'
        ret = 0
        for wts in self._thread_states:
            ret += wts.stats.get('error_count')
        return ret

    def _wps_create(self, wps_name, wts):
        '''
        Create and return WorkerProcessState
        '''
        return WorkerProcessState(wps_name, self._clfsload.run_options, wts.pqueue_wt, wts.pqueue_wp, self._clfsload.local_state_path)

    def run(self):
        'Create and run the worker threads'
        logger = self._clfsload.logger
        terminate = False
        Psutil.cleanup_children(logger)
        self.db.phase_work_poke = self._poke
        logger.debug("launch worker threads (count=%d)", self._num_threads)
        # Create but do not start the threads
        self._stats_t0 = time.time()
        self._stats_throughput_last_time = self._stats_t0
        for thread_num in range(self._num_threads):
            wts = WorkerThreadState(self._clfsload.thread_shared_state,
                                    self._clfsload.logger,
                                    self._clfsload.run_options,
                                    self._phase,
                                    thread_num,
                                    self._stats_class,
                                    self._clfsload.writer)
            wts.stats.t0 = self._stats_t0
            wts.stats.throughput_last_time = self._stats_t0
            self._thread_states.append(wts)
            if self._use_process:
                wps_name = "process-%s-%s" % (self._phase, thread_num)
                wps = self._wps_create(wps_name, wts)
                wts.wps = wps
                wts.process = multiprocessing.Process(target=_one_process_run, name=wps_name, args=(wps,), daemon=False)
            wts.thread = threading.Thread(target=self._one_thread_run, name=str(wts), args=(wts,))
        try:
            self._run_threads()
        except (KeyboardInterrupt, SystemExit) as e:
            logger.warning("%s %s", e.__class__.__name__, getframe(0))
            self._abort()
            terminate = True
        finally:
            self._stop = True
        try:
            self._after_run_threads()
        except (KeyboardInterrupt, SystemExit):
            logger.warning("%s at %s", exc_info_err(), getframe(0))
            if self._report_time_next:
                self._report(report_full=True, log_level=logging.DEBUG)
                logger.warning("%s at %s", exc_info_err(), getframe(0))
            self._abort()
            terminate = True
        except:
            exc_log(logger, logging.ERROR, "after_run_threads")
            terminate = True
        finally:
            self._stop = True
        for cond in [self._state_cond, self._work_cond]:
            # Poke and release locks. This is somewhat hacky.
            # We do the checks here in case we ended up with
            # an exception bubbling up above that left a lock held.
            notify_all(cond)
        Psutil.cleanup_children(logger)
        if (not self._execute_work_items) or terminate:
            # We got a terminal error earlier
            raise SystemExit(1)
        self.work_count = sum([wts.stats.get('count_dir') + wts.stats.get('count_nondir') for wts in self._thread_states])

    def _run_threads(self):
        'Threads are created - do the phase work'
        logger = self._clfsload.logger

        # Force a report now. Doing this gives a valid value to self._report_time_last
        # so _seconds_remaining_before_report() may assume it is not None.
        cur_ts = time.time()
        self._t0 = cur_ts
        self._report_time_next = cur_ts
        self._report(cur_ts=cur_ts, report_is_interval=True)

        # Start the threads
        for wts in self._thread_states:
            wts.thread.start()
        self._wait_for_all_workers_running()

        with self._state_lock:
            while True:
                if self._aborting:
                    self._wake_all_NL()
                log_level = None
                report_is_interval = False
                if (self._idle_count + self._done_count) >= self._num_threads:
                    if (not self._clfsload.azcopy) and logger.isEnabledFor(logging.DEBUG):
                        log_level = logging.DEBUG
                    if (not self.db.all_phase_threads_are_idle) and (not self._aborting):
                        raise AssertionError("(self._idle_count=%d + self._done_count=%d) >= self._num_threads=%d but self.db.all_phase_threads_are_idle=%s" % \
                                             (self._idle_count, self._done_count, self._num_threads, self.db.all_phase_threads_are_idle))
                    if not (self._execute_work_items and self.db.dbc_check_for_more_work()):
                        # All threads are idle and there is nothing more to do in the DB. We are done.
                        break
                if self._error_limit_reached_NL():
                    self._error_limit_reached = True
                    logger.warning("phase %s aborting work because the error limit is reached", self._phase)
                    break
                cur_ts = time.time()
                # Set a min so we do not get stuck waiting for a missing poke
                t = min(self._seconds_remaining_before_report(cur_ts), 10.0)
                if t:
                    if not self._clfsload.should_run:
                        self._report(cur_ts=cur_ts)
                        self._abort()
                        raise SystemExit(1)
                    if log_level is not None:
                        self._report(cur_ts=cur_ts, log_level=log_level)
                        log_level = None
                    # It is frustrating that the timeout here is relative. Just saying.
                    self._state_cond.wait(timeout=t)
                    cur_ts = time.time()
                else:
                    log_level = logging.INFO
                    report_is_interval = True
                if log_level is not None:
                    self._report(cur_ts=cur_ts, log_level=log_level, report_is_interval=report_is_interval)

    def _after_run_threads(self):
        '''
        _run_threads() is done. Stop workers and wait for them to be done.
        '''
        logger = self._clfsload.logger
        self._wait_for_all_workers_done()
        cur_ts = time.time()
        self._t1 = cur_ts
        el = elapsed(self._t0, self._t1)

        logger.info("phase %s complete, elapsed=%f", self._phase, el)
        stats = self._report(cur_ts=cur_ts, report_full=True, log_level=logging.INFO, report_is_final=True)
        stats.report_final(logger, el)
        logger.debug("phase %s idle_events=%d threads=%d", self._phase, self._idle_events, len(self._thread_states))

        # Force a flush
        self.db.dbc_flush()

        with self._state_lock:
            # Use self._error_limit_reached rather than computing from _error_limit_reached_NL()
            # to distinguish between "could have aborted because" and "did abort because".
            verb = ''
            if self._error_limit_reached:
                verb = 'aborted'
            elif self._error_limit_reached_NL():
                verb = 'aborting'
            if verb:
                logger.error("phase %s %s, error count %d >= max %s", self._phase, verb, self.error_count, self.errors_max)
                raise SystemExit(1)

        if self._execute_work_items:
            # All threads have gone idle. If there is any work remaining
            # then something has gone wrong.
            try:
                with self._state_lock:
                    work_item = self._work_item_get(None)
                    if not work_item:
                        work_item = self.db.db_claimable_get() # Real DB access here as a sanity-check
            except DbTerminalError as e:
                logger.error("error: phase %s halted with the database in a terminal state (%s)", self._phase, e)
                raise SystemExit(1)
            except Exception as e:
                logger.error("stack:\n%s\nerror: phase %s cannot determine if there is additional work: %s %s",
                             exc_stack(), self._phase, e.__class__.__name__, e)
                raise SystemExit(1)
            if work_item is not None:
                logger.error("internal error: phase %s completed but at least one additional work_item is found", self._phase)
                raise SystemExit(1)

    def error_poke(self):
        '''
        One thread has hit an error
        '''
        with self._state_cond:
            if self._error_limit_reached_NL():
                self._state_cond.notify_all()

    def _seconds_remaining_before_report(self, cur_ts):
        'Return how many seconds remaining before the next report (0 means "report now")'
        # run() always generates a report before invoking _should_report(),
        # so we may assume that self._report_time_last is not None here.
        if self._report_time_next <= cur_ts:
            return 0.0
        return self._report_time_next - cur_ts

    def _do_report(self, log_level, stats, timers, cur_ts, elapsed_secs, report_is_interval=False, report_is_final=False, report_full=False):
        '''
        Generate and log a report. Log AzCopy info if requested.
        Returns the report results from stats (used by tests)
        '''
        logger = self.logger
        srpt = stats.report_short(cur_ts,
                                  elapsed_secs,
                                  self._clfsload,
                                  self._historical_perf,
                                  report_is_interval=report_is_interval,
                                  report_is_final=report_is_final)
        rs = "%s %d+%d/%d %s" % (self._phase, self._idle_count, self._done_count, self._num_threads, srpt['simple'])
        if report_full:
            db = self._clfsload.db
            rs += '\n'
            rs += '\n'.join(('stats:', stats.pformat(),
                             'timers:', pprint.pformat(timers.elapsed_dict()),
                             'DB stats:', db.stats.pformat(),
                             'DB timers:', pprint.pformat(db.timers.elapsed_dict()),
                            ))
        # We do not drop a report for report_is_final because it
        # is for a fraction of the interval, and the numbers will
        # look wacky. Either the next phase will start soon, or
        # we will finish and generate a final report.
        if self._clfsload.azcopy and report_is_interval:
            azcopy_rpt = self._azcopy_report(srpt)
            if azcopy_rpt:
                rs += '\n'
                rs += self._clfsload.azcopy_report_interval_string(azcopy_rpt)
        logger.log(log_level, "%s", rs)
        return srpt

    def _azcopy_report(self, srpt):
        '''
        Given srpt as a report dict generated by stats.report_short(),
        return the dict for an azcopy report.
        '''
        # These file counts are always valid integers
        files_completed_failed = srpt['files_completed_failed']
        files_completed_total = srpt['files_completed_total']
        files_completed_success = max(files_completed_total-files_completed_failed, 0) # files_skipped always 0

        # This shows all attributes of the generated report for azcopy.
        # Most of these are updated below.
        rpt = {'elapsed' : srpt['elapsed'],
               'files_completed_failed' : files_completed_failed,
               'files_completed_success' : files_completed_success,
               'files_pending' : None,
               'files_skipped' : 0,
               'files_total' : None,
               'pct_complete' : None,
               'phase' : self._phase_name,
               'throughput_Mbps' : srpt['throughput_Mbps'],
               'throughput_delta_secs' : srpt['throughput_delta_secs'],
               'time_eta' : srpt['time_eta'],
               'time_report' : srpt['time_report'],
              }

        if self._clfsload.dry_run_succeeded:
            try:
                rpt['pct_complete'] = self._azcopy_report__pct(min(srpt['pct_bytes'], srpt['pct_eta']))
            except TypeError:
                try:
                    rpt['pct_complete'] = self._azcopy_report__pct(srpt['pct_eta'])
                except TypeError:
                    try:
                        rpt['pct_complete'] = self._azcopy_report__pct(srpt['pct_bytes'])
                    except TypeError:
                        # Do not update self._azcopy_pct_last here.
                        # We have no valid value here, so we would only
                        # be discarding a valid value and risk going backwards.
                        pass
            try:
                files_total = srpt['files_total']
                rpt['files_total'] = files_total
                files_total = max(files_total, files_completed_total)
                rpt['files_pending'] = files_total - files_completed_total
            except KeyError:
                # no srpt['files_total']
                pass

        return rpt

    def _azcopy_report__pct(self, pct):
        '''
        Given a proposed pct, return the value to use.
        '''
        if self._azcopy_pct_last is not None:
            pct = max(self._azcopy_pct_last, pct)
        pct = max(min(pct, 100.0), 0.0)
        self._azcopy_pct_last = pct
        return pct

    def _report(self, cur_ts=None, report_full=False, log_level=logging.INFO, report_is_interval=False, report_is_final=False):
        '''
        Report current progress
        '''
        logger = self.logger
        cur_ts = cur_ts if cur_ts is not None else time.time()
        self._report_time_last = cur_ts
        elapsed_secs = elapsed(self._t0, cur_ts)
        if cur_ts >= self._report_time_next:
            # Figure out our next report time. Don't just add
            # self._report_interval to cur_ts... that gives us
            # a steady drift forward that is aesthetically
            # displeasing.
            el = elapsed(self._report_time_next, cur_ts)
            i = math.ceil(el / self._report_interval)
            self._report_time_next += (i * self._report_interval)
            if self._report_time_next <= cur_ts:
                self._report_time_next += self._report_interval
        # Sum stats into a temporary object. For performance,
        # we avoid locking here and trust that the results are
        # not overly misleading.
        stats = self._stats_class()
        stats.t0 = self._stats_t0
        stats.throughput_last_time = self._stats_throughput_last_time
        timers = TimerStats()
        for wts in self._thread_states:
            stats.add(wts.stats, logger)
            if report_full:
                timers.add(wts.timers, logger)
        self._do_report(log_level,
                        stats,
                        timers,
                        cur_ts,
                        elapsed_secs,
                        report_is_interval=report_is_interval,
                        report_is_final=report_is_final,
                        report_full=report_full)
        # Generating the report updated stats.throughput_last_time.
        # Save it so we can use it the next time around.
        self._stats_throughput_last_time = stats.throughput_last_time
        return stats

    def _poke(self, num):
        '''
        This is invoked by the DB object. If num is greater than zero,
        it is telling us that there are num work items available.
        If num is zero, it is hinting that everything may be done.
        '''
        if num:
            with self._state_lock:
                self._work_cond.notify(num)
        else:
            with self._state_lock:
                self._state_cond.notify_all()
                self._work_cond.notify_all()

    def _wait_for_all_workers_running(self):
        'Wait for all worker threads to start running'
        logger = self.logger
        with self._state_lock:
            while self._started_count < self._num_threads:
                if not self._clfsload.should_run:
                    raise SystemExit(1)
                self._state_cond.wait()
        logger.debug("worker threads are running (count=%d)", self._num_threads)

    def _wait_for_all_workers_done(self):
        'Wait for all worker threads to stop running'
        logger = self.logger
        self._report()
        t0 = time.time()
        with self._state_lock:
            self._stop = True
            self._work_cond.notify_all()
            logger.debug("wait for worker threads to complete (%d/%d)", self._done_count, self._num_threads)
            while self._done_count < self._num_threads:
                self._work_cond.notify_all()
                if (elapsed(t0) > (self.WAIT_REMOTE_POLL + 1)) and not self._clfsload.should_run:
                    raise SystemExit(1)
                self._state_cond.wait(timeout=1.0)
        logger.debug("worker threads are done (count=%d)", self._num_threads)

    def _wake_all(self):
        '''
        Wake all waiters on all condition vars. Assumes the caller
        may or may not hold state_lock. Will not block waiting
        for state_lock.
        '''
        with Monitor(self._state_lock, blocking=False) as m:
            if m.havelock:
                self._wake_all_NL()

    def _wake_all_NL(self):
        '''
        Wake all waiters on all condition vars. Assumes the caller
        holds state_lock.
        '''
        self._state_cond.notify_all()
        self._work_cond.notify_all()

    def _abort(self):
        'abort all threads'
        self._aborting = True
        self._stop = True
        self.thread_shared_state.should_run = False
        self._wake_all()

    def _work_item_get(self, wts):
        'Attempt to get a single pending work item. Return None if none are found.'
        try:
            tobj = self.db.dbc_claim(wts)
        except DbTerminalError as e:
            # Log a brief message to avoid spew from our caller
            self.logger.warning("%s work_item_get DB is terminal: %s", wts, e)
            self._abort()
            return None
        return tobj

    def _work_item_execute(self, wts, tobj):
        'Do the work for a single item previously returned by _work_item_get().'
        raise NotImplementedError("%s did not implement this method" % self.__class__.__name__)

    def _one_thread_run(self, wts):
        '''
        This is invoked by each thread in the pool as its main loop.
        wts is a WorkerThreadState - the state specific to this thread.
        '''
        logger = wts.logger
        wait_for_ready = False
        qi = None
        if wts.process:
            wts.process.start()
            pqueue_wt = wts.pqueue_wt
            pqueue_wp = wts.pqueue_wp
            try:
                pqueue_wt.put(self._pcom_init)
                wait_for_ready = True
            except Exception:
                logger.warning("%s cannot send initial parameters to process: %s", wts, exc_info_err())
        if wait_for_ready:
            t0 = time.time()
            t1 = None
            wait_secs = self.WAIT_CHILD_PROCESS_READY
            t2 = t0 + wait_secs
            while (t1 is None) or (elapsed(t0, t1) < wait_secs):
                try:
                    max_wait_one = 1.0
                    if t1 is None:
                        timeout = min(wait_secs, max_wait_one)
                    else:
                        timeout = min(elapsed(t1, t2), max_wait_one)
                    qi = pqueue_wp.get(block=True, timeout=timeout)
                    break
                except queue.Empty:
                    pass
                t1 = time.time()
            if isinstance(qi, PcomReady):
                wts.process_ready = True
                assert qi.pid == wts.process.pid
                wts.process_pid = qi.pid
            else:
                pqueue_wt.close()
                pqueue_wt = None
                pqueue_wp.close()
                pqueue_wp = None
                wts.process_ready = False
                logger.warning("%s process %s did not become ready (%s); running directly on thread",
                               wts, wts.process.pid, qi.__class__.__name__)
                if wts.process.pid:
                    time.sleep(1) # Give the child a chance to finish logging and exit
                    if Psutil.terminate_iff_child(wts.logger, wts.process.pid, prefix=str(wts)):
                        logger.debug("%s process %s terminated", wts, wts.process.pid)
            del qi
        wts.process_okay = bool(wts.process) and bool(wts.process_ready)
        with self._state_lock:
            self._started_count += 1
            self._state_cond.notify_all()
        try:
            wts.timers.start_working()
            self._one_thread_do_work(wts)
            wts.timers.stop_working()
        except TerminalError as e:
            logger.warning("%s got terminal error %s", wts, exc_info_err())
            if isinstance(e, ServerRejectedAuthError):
                self.thread_shared_state.server_rejected_auth = True
            with self._work_cond:
                self._execute_work_items = False
                self._work_cond.notify_all()
            raise SystemExit(1)
        except (KeyboardInterrupt, SystemExit) as e:
            logger.warning("%s %s %s", wts, e.__class__.__name__, getframe(0))
            self._abort()
        except:
            wts.exc_error_exception(self, what='do work')
        finally:
            with self._state_lock:
                self._one_thread_is_busy_NL(wts)
            if wts.process:
                try:
                    wts.process_send_stop()
                    wts.process_join()
                    wts.pqueue_wt.close()
                    wts.pqueue_wp.close()
                except:
                    err = "%s process cleanup" % self
                    exc_log(logger, logging.ERROR, err)
            with self._state_lock:
                self._done_count += 1
                if (self._idle_count + self._done_count) >= self._num_threads:
                    self.db.any_phase_threads_are_idle = True
                    self.db.all_phase_threads_are_idle = True
                self._state_cond.notify_all()
                self._work_cond.notify_all()

    def _one_thread_is_idle__idle_sleep_secs_NL(self):
        '''
        Compute the return value for _one_thread_is_idle().
        This is how long this idle thread will sleep.
        '''
        if not self._clfsload.should_run:
            return 0.0
        if (self._idle_count + self._done_count + 1) >= self._num_threads:
            # At most one thread is busy
            return 1.0
        return 5.0

    def _one_thread_is_idle_NL(self, wts):
        '''
        Mark that one thread is transitioning from busy to idle.
        Return how long the caller should sleep on the condition
        before checking again.
        Caller holds state_lock.
        '''
        if wts.idle_timer:
            # already marked idle
            ret = self._one_thread_is_idle__idle_sleep_secs_NL()
            return ret
        wts.idle_timer = wts.timers.start('idle')
        wts.stats.stat_inc('idle')
        if self._idle_count == 0:
            self.db.any_phase_threads_are_idle = True
        self._idle_count += 1
        self._idle_events += 1
        if (self._idle_count + self._done_count) >= self._num_threads:
            if self.db.all_phase_threads_are_idle:
                raise AssertionError("(self._idle_count=%d + self._done_count=%d) >= self._num_threads=%d but self.db.all_phase_threads_are_idle=%s" % \
                                     (self._idle_count, self._done_count, self._num_threads, self.db.all_phase_threads_are_idle))
            self.db.any_phase_threads_are_idle = True
            self.db.all_phase_threads_are_idle = True
            self._state_cond.notify_all()
        return self._one_thread_is_idle__idle_sleep_secs_NL()

    def _one_thread_is_busy_NL(self, wts):
        'Mark that one thread is transitioning from idle to busy'
        if not wts.idle_timer:
            # not marked idle
            return
        self.db.all_phase_threads_are_idle = False
        if self._aborting:
            self._wake_all_NL()
        assert self._idle_count > 0
        self._idle_count -= 1
        if self._idle_count == 0:
            self.db.any_phase_threads_are_idle = False
        wts.idle_timer.stop()
        wts.idle_timer = None

    def _error_limit_reached_NL(self):
        '''
        Return whether the number of errors has reached the max.
        Caller holds state_lock
        '''
        return self.errors_max and (self.error_count >= self.errors_max)

    def _one_thread_do_work(self, wts):
        'Core of the worker thread -- find work, do it, repeat'
        while (not self._stop) and self.thread_shared_state.should_run:
            with wts.timers.start('work_item_claim') as timer_claim:
                if not self._execute_work_items:
                    return
                with self._state_lock:
                    try:
                        tobj = self._work_item_get(wts)
                        timer_claim.stop()
                    except Exception as e:
                        tobj = None
                        self.logger.debug("%s work_item_get stack:\n%s", wts, exc_stack())
                        self.logger.error("%s work_item_get error: %s %s", wts, e.__class__.__name__, e)
                        # Fall through - we shall sleep and retry
                    if tobj:
                        self._one_thread_is_busy_NL(wts)
                    else:
                        with wts.timers.start('work_item_wait'):
                            sleep_secs = self._one_thread_is_idle_NL(wts)
                            if self._aborting:
                                self._wake_all_NL()
                            if not self._clfsload.should_run:
                                raise SystemExit(1)
                            wts.stats.stat_inc('wait')
                            self._work_cond.wait(timeout=sleep_secs)
                            if self._aborting:
                                self._wake_all_NL()
                        continue
                with wts.timers.start('work_item_execute'):
                    executed_ok = False
                    try:
                        self._work_item_execute(wts, tobj)
                        executed_ok = True
                    except (KeyboardInterrupt, SystemExit, TerminalError):
                        raise
                    except Exception as e:
                        wts.exc_error_exception(self, what='work_item_execute')
                    if not executed_ok:
                        self.db.dbc_update_state(wts, tobj, TargetObjState.ERROR, surrender_tobj=False)

    def _one_thread_wait_for_remote_done(self, wts, tobj):
        '''
        Wait for the associated Process to complete a work item.
        If the remote told us about an exception, raise it here.
        '''
        logger = wts.logger
        t0 = time.time()
        log_time_last = t0
        while self._clfsload.should_run:
            try:
                qi = wts.pqueue_wp.get(block=True, timeout=self.WAIT_REMOTE_POLL)
            except queue.Empty:
                # We might think about killing and restarting the child here
                # if things are taking too long. We should at least see stats
                # updates regularly. If we do so, we must be careful.
                # If the child died and the pid is recycled, we must not
                # kill the wrong child. We must also be sure the child is
                # dead before proceeding with the restart.
                # One reason we could see no activity is a stuck read.
                # If the source filesystem is remote and having an issue,
                # that's out of our control, and killing and restarting will
                # not help.
                # Check if the child pid is running at all. If not, then we can
                # be sure it is gone. If we see the pid, we cannot be sure if the
                # child is gone or not.
                wts.stats.stat_inc('wait_remote_empty')
                try:
                    with wts.timers.start('get_child_pids'):
                        pids = Psutil.get_child_pids()[0]
                    if wts.process_pid not in pids:
                        logger.error("%s wait_for_remote_done detected child pid=%s not running",
                                     wts, wts.process_pid)
                        raise TargetObjectError(tobj, "child process terminated", stack_is_interesting=False)
                except:
                    exc_log(logger, logging.WARNING, "%s cannot determine the status of child pid=%s" % (wts, wts.process_pid))
                t1 = time.time()
                if elapsed(log_time_last, t1) >= 60.0:
                    logger.info("%s wait_for_remote_done still waiting pid=%s elapsed=%.1f %s",
                                wts, wts.process_pid, elapsed(t0), tobj.describe())
                    log_time_last = t1
                continue
            if isinstance(qi, (list, tuple)) and self._handle_qi_batch(wts, qi):
                continue
            if isinstance(qi, PcomTobjDone):
                if tobj.filehandle != qi.tobj.filehandle:
                    # Um.
                    logger.error("%s wait_for_remote_done got filehandle %s but expected %s",
                                 wts, qi.tobj.filehandle.hex(), tobj.filehandle.hex())
                    wts.process_okay = False
                    raise TargetObjectError(tobj, "filehandle mismatch from child worker", stack_is_interesting=False)
                return qi
            if self._handle_qi(wts, qi):
                continue
            if isinstance(qi, PcomSourceObjectError):
                raise SourceObjectError.from_json(qi.exception_json, stack_is_interesting=False)
            if isinstance(qi, PcomServerRejectedAuthError):
                raise ServerRejectedAuthError.from_json(qi.exception_json, stack_is_interesting=False)
            if isinstance(qi, PcomTargetObjectError):
                e = TargetObjectError.from_json(qi.exception_json, stack_is_interesting=False)
                e.update_from_tobj(tobj)
                raise e
            if isinstance(qi, PcomTerminate):
                logger.warning("%s got PcomTerminate, set process_ready=False", wts)
                wts.process_ready = False
                # Fall through to unexpected-message handling
            logger.error("%s unexpected message from child %s %s",
                         wts, qi.__class__.__name__, str(qi))
            wts.process_okay = False
            # Should we really be failing the operation here?
            # What else can we do? This is a programming error,
            # so doing anything fancy to recover just produces a
            # mass of code unlikely to ever run. Handle it as an
            # error. We can revisit this decision if it proves
            # problematic.
            err = "unexpected message from child (%s)" % qi.__class__.__name__
            raise TargetObjectError(tobj, err, stack_is_interesting=False)
        if Psutil.terminate_iff_child(wts.logger, wts.process_pid, prefix=str(wts)):
            wts.logger.debug("%s sent terminate to %s", wts, wts.process_pid)
        raise SystemExit(1)

    def _handle_qi_batch(self, wts, qis):
        '''
        Child process sent a batched response.
        '''
        for qi in qis:
            if not self._handle_qi(wts, qi):
                wts.logger.warning("%s unknown batched response %s (%s)",
                                   wts, qi.__class__.__name__, qi)
                return False
        return True

    @staticmethod
    def _handle_qi(wts, qi):
        '''
        Common handling for child process responses.
        Returns whether or not it is handled.
        '''
        if isinstance(qi, PcomStatsUpdate):
            wts.stats.stat_update(qi.updates)
            return True
        if isinstance(qi, PcomTimerUpdate):
            wts.timers.stat_update(qi.updates)
            return True
        if isinstance(qi, PcomStatsMax):
            wts.stats.stat_max(qi.name, qi.value)
            return True
        return False

def _one_process_run(wps):
    '''
    This is the target for a multiprocessing.Process. We run one
    of these per worker thread.
    '''
    # Create our own logger. We cannot share with the parent process.
    logger = logger_create(logging.DEBUG)
    wps.logger = logger
    logfile_path = wps.logfile_path()
    logfile_handler = logging.FileHandler(logfile_path)
    logfile_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.handlers = [logfile_handler]
    logger.propagate = False
    logger.info("%s started as pid %s", wps, os.getpid())
    logfile_handler.flush()
    pq = ProcessQueues.from_WorkerProcessState(logger, wps)
    # Create ProcessWRock with thread_num 0. Additional threads
    # in the process are 1-based.
    wrock = ProcessWRock(logger, wps.run_options, str(wps), 0, pq)
    try:
        _one_process_run_internal(wps, wrock, logger)
    except:
        exc_log(logger, logging.ERROR, "%s run_internal" % wps)
    finally:
        if wps.writer:
            wps.writer.deactivate_thread_pool()
        wrock.pq.flush_write()
    logger.info("%s done as pid %s", wps, os.getpid())
    logfile_handler.flush()
    if wps.send_terminate:
        try:
            wrock.pq.put(PcomTerminate(), block=True, timeout=1.0)
        except:
            exc_log(logger, logging.ERROR, "%s send terminate" % wps)
    wrock.pq.flush_write()
    logger.info("%s exiting pid %s send_terminate=%s", wps, os.getpid(), wps.send_terminate)
    logfile_handler.flush()
    raise SystemExit(0)

def _one_process_run_internal(wps, wrock, logger):
    '''
    Guts of _one_process_run().
    This is the heart of a multiprocessing.Process. This executes as
    a child process of the main CLFSLoad process. Communication with
    the parent is via a pair of multiprocessing.Queue objects.
    '''
    wps.send_terminate = True
    try:
        qi_init = wrock.pq.get(timeout=300.0)
    except:
        logger.error("%s cannot get initial parameters: %s", wps, exc_info_err())
        return
    if not isinstance(qi_init, PcomInit):
        logger.error("%s unexpected initialization type %s", wps, qi_init.__class__.__name__)
        return
    try:
        run_options = RunOptions.from_json(qi_init.run_options_json)
        writer_class = WriterAzure.get_reconstituted_class(*qi_init.writer_import_info)
        wps.writer = writer_class(qi_init.writer_command_args_json, logger, run_options, *qi_init.writer_reconstitute_args, child_process_wrock=wrock)
        wps.writer.activate_thread_pool()  # matched up with a call to deactivate in _one_procress_run
        wps.writer.apply_additional_json(qi_init.writer_additional)
        wps.writer.wrock_init(wrock)
        reader_class = ReaderPosix.get_reconstituted_class(*qi_init.reader_import_info)
        wps.reader = reader_class(qi_init.reader_command_args_json, logger, run_options, wps.local_state_path, child_process_wrock=wrock)
        wps.reader.apply_additional_json(qi_init.reader_additional)
        wps.readfile_local_class = ReadFilePosix.get_reconstituted_class(*qi_init.readfile_local_import_info)
    except:
        exc_log(logger, logging.ERROR, "%s reconstitute reader and writer" % wps)
        return

    logger.debug("%s send ready", wps)
    wrock.pq.put(PcomReady(os.getpid()))
    logger.info("%s accepting work", wps)
    while wrock.should_run:
        try:
            qi = wrock.pq.get(timeout=30.0)
        except queue.Empty:
            # For the record, we are alive
            logger.debug("%s nothing to do (empty)", wps)
            continue
        if not qi:
            # For the record, we are alive
            logger.debug("%s nothing to do (none)", wps)
            continue
        try:
            _one_process_run_qi(wps, wrock, qi)
        except Exception:
            exc_log(logger, logging.ERROR, "run_qi")
            break
        wrock.pq.flush_write()

def _one_process_run_qi(wps, wrock, qi):
    '''
    Handle a single item received from the associated thread
    '''
    resp = None
    proc = None
    tobj = None
    if isinstance(qi, PcomTransfer):
        tobj = qi.tobj
        proc = wps.writer.transfer
        proc_args = (wrock, tobj, wps.reader)
        desc = 'wps.writer.transfer'
    elif isinstance(qi, PcomDirectoryWrite):
        tobj = qi.tobj
        proc = wps.directory_write
        proc_args = (wrock, tobj, qi.entry_file_path)
        desc = 'wps.directory_write'
    elif isinstance(qi, PcomReconcile):
        tobj = qi.tobj
        proc = wps.writer.reconcile
        proc_args = (wrock, tobj, wps.reader)
        desc = 'wps.writer.reconcile'
    else:
        raise SimpleError("%s unknown qi type %s" % (wps, qi.__class__.__name__))
    try:
        with wrock.timers.start(desc):
            proc(*proc_args)
        resp = PcomTobjDone(tobj)
    except SourceObjectError as e:
        logger = wrock.logger
        exc_log(logger, logging.WARNING, desc)
        resp = PcomSourceObjectError(e)
    except ServerRejectedAuthError as e:
        logger = wrock.logger
        exc_log(logger, logging.WARNING, desc)
        resp = PcomServerRejectedAuthError(e)
    except TargetObjectError as e:
        logger = wrock.logger
        exc_log(logger, logging.WARNING, desc)
        resp = PcomTargetObjectError(e)
    except BaseException as e:
        logger = wrock.logger
        exc_log(logger, logging.WARNING, desc)
        te = TargetObjectError(tobj.filehandle, "%s error: %s" % (qi.__class__.__name__, e))
        resp = PcomTargetObjectError(te)
        if not isinstance(e, Exception):
            wrock.stats_flush()
            wrock.pq.put(resp)
            raise
    wrock.stats_flush()
    wrock.pq.put(resp)

class TransferWorkerPool(WorkerPool):
    'Worker operations that are specific to the transfer phase'
    _phase = 'transfer'
    _stats_class = TransferStats

    def __init__(self, clfsload, num_threads):
        super(TransferWorkerPool, self).__init__(clfsload, num_threads)
        self._historical_perf = HistoricalPerf(clfsload.db)

    def _wps_create(self, wps_name, wts):
        '''
        Create and return WorkerProcessState for TransferWorkerPool
        '''
        ret = super(TransferWorkerPool, self)._wps_create(wps_name, wts)
        setattr(ret, 'directory_write', self._directory_write)
        return ret

    def run(self):
        'Overridden to activate/deactivate the writer thread pool in the transfer phase.'
        writer = self._clfsload.writer
        writer.activate_thread_pool()
        try:
            super(TransferWorkerPool, self).run()
        finally:
            writer.deactivate_thread_pool()

    def _work_item_execute(self, wts, tobj):
        'Do the work for a single item previously returned by _work_item_get().'
        if (tobj.state == TargetObjState.GENERATING) and (tobj.ftype == Ftype.DIR):
            with wts.timers.start('dir_generate'):
                self._work_item_generate_dir(wts, tobj)
            # The dir is now queued for a state change to WRITING.
            # We will get it back as a subsequent work item.
            return
        if tobj.state == TargetObjState.WRITING:
            tobj_ftype = tobj.ftype
            try:
                self._work_item_writing(wts, tobj)
                del tobj
            finally:
                if tobj_ftype == Ftype.DIR:
                    wts.stats.stat_inc('count_dir')
                else:
                    wts.stats.stat_inc('count_nondir')
            return
        logger = wts.logger
        err = "unexpected target object state=%s/ftype=%s" % (str(tobj.state), str(tobj.ftype))
        logger.error("%s internal error in _work_item_execute %s object is:\n%s",
                     wts, err, tobj.pformat())
        # No need for self._one_thread_error() here; the caller counts it when it sees the exception
        tobj.state = TargetObjState.ERROR
        # Push to the error state here with surrender_tobj=False because
        # we do not know why state is corrupt, and the tobj might be
        # junk.
        self.db.dbc_update_state(wts, tobj, TargetObjState.ERROR, surrender_tobj=False)
        # We've already logged and marked the error; no need for an exception

    _DIR_WRITE_BUFFER_SIZE = max(CLFSSegment.FIRST_SEGMENT_BYTES, CLFSSegment.DIR_OTHER_SEGMENT_BYTES)

    def _work_item_generate_dir(self, wts, tobj):
        '''
        Do the generation work for the directory identified by tobj.
        This holds the dir_entries in core. It may turn out that we
        must store them in the local state dir when scaling to very
        large loads.
        '''
        entry_file_path = self.entry_file_path(tobj.filehandle)
        try:
            what = 'open for writing'
            entry_file = self._clfsload.writer.local_file_writer(entry_file_path, wts.logger, buffersize=self._DIR_WRITE_BUFFER_SIZE)
            what = 'truncate'
            entry_file.truncate()
            what = 'generate'
            child_dir_count = self._work_item_generate_dir_internal(wts, tobj, entry_file)
            what = 'flush and close'
            entry_file.flush_and_close()
            self._done_generate_dir_update_state(wts, tobj, child_dir_count)
        except (NamedObjectError, TerminalError, WriteFileError):
            raise
        except Exception as e:
            msg = "cannot %s '%s': %s %s" % (what, entry_file_path, e.__class__.__name__, e)
            raise TargetObjectError(tobj, msg) from e

    def _done_generate_dir_update_state(self, wts, tobj, child_dir_count):
        '''
        We may now write out the directory. Update it.
        '''
        with wts.timers.start('dir_generate_update_state'):
            self.db.dbc_update_state(wts, tobj, TargetObjState.WRITING, child_dir_count=child_dir_count, surrender_tobj=False)

    @staticmethod
    def _entry_bytes_from_namestr(name, filehandle, stream):
        '''
        Helper for _work_item_generate_dir_internal -- appends
        the bytes for one entry to stream.
        '''
        namebytes = os.fsencode(name)
        fhbytes = filehandle.bytes
        stream.write(struct.pack(STRUCT_LE_U32, len(namebytes)))
        stream.write(namebytes)
        stream.write(struct.pack(STRUCT_LE_U16, len(fhbytes)))
        stream.write(fhbytes)
        return 6 + len(namebytes) + len(fhbytes)

    @staticmethod
    def _entry_bytes_from_namebytes(namebytes, filehandle, stream):
        '''
        Helper for _work_item_generate_dir_internal -- appends
        the bytes for one entry to stream.
        '''
        fhbytes = filehandle.bytes
        stream.write(struct.pack(STRUCT_LE_U32, len(namebytes)))
        stream.write(namebytes)
        stream.write(struct.pack(STRUCT_LE_U16, len(fhbytes)))
        stream.write(fhbytes)
        return 6 + len(namebytes) + len(fhbytes)

    DBC_UPSERT_BATCH_SIZE = 500

    _DUMMY_SEGMENT_LEN = bytes(4) # STRUCT_LE_U32

    # If we must reconcile this directory later, we may need to replace the '..'
    # entry. In that case, the entry might grow because the filehandle grows.
    # Subtract the full size of a filehandle here to provide that wiggle room
    _EFFECTIVE_DIRECTORY_FIRST_SEGMENT_LEN = CLFSSegment.FIRST_SEGMENT_BYTES - Filehandle.fixedlen()

    _NAME_XLAT_ERROR = 'name translation error for child of this directory'

    def _work_item_generate_dir_internal(self, wts, tobj, entry_file):
        '''
        See _work_item_generate_dir().
        Returns child_dir_count.
        '''
        reader = self._clfsload.reader
        directory = reader.opendir(tobj.source_path_str)
        child_dir_count = 0
        segment_len = self._EFFECTIVE_DIRECTORY_FIRST_SEGMENT_LEN
        # Generate . and ..
        pending_len = 0
        segment_offset = 0 # offset in entry_file
        entry_file.write(self._DUMMY_SEGMENT_LEN)
        pending_len += self._entry_bytes_from_namestr('.', tobj.filehandle, entry_file)
        if tobj.first_backpointer == FILEHANDLE_NULL:
            pending_len += self._entry_bytes_from_namestr('..', tobj.filehandle, entry_file)
        else:
            pending_len += self._entry_bytes_from_namestr('..', tobj.first_backpointer, entry_file)
        assert pending_len < segment_len
        new_children = list()
        ent_num = 0
        while True:
            ent_num += 1
            try:
                ri = reader.getnextfromdir(directory)
            except SourceObjectError as e:
                self.logger.error(str(e))
                wts.stats.stat_inc('error_count')
                self.error_poke()
                continue
            if not ri:
                break
            namebytes = ""
            try:
                namebytes = os.fsencode(ri.name)
            except Exception as e:
                err = "%s (#%d): %s" % (self._NAME_XLAT_ERROR, ent_num, exc_info_err())
                raise TargetObjectError(tobj, err, stack_is_interesting=False) from e
            new_entry = self._work_item_generate_target_obj_for_dir_ent(wts, tobj, ri, new_children)
            if not new_entry:
                # Error already logged and counted
                continue
            if new_entry.ftype == Ftype.DIR:
                child_dir_count += 1
            if new_entry.barrier or (len(new_children) >= self.DBC_UPSERT_BATCH_SIZE):
                self.db.dbc_upsert_multi(wts, new_children)
                new_children = list()
            # Entry file format is a bunch of lines. Each line is one entry
            # as a JSON-formatted list of strings. The first string is the filehandle
            # and the second is the name. Note that this is JSON per-line. The whole
            # file is not parsable as a single unit. We do it that way because if the
            # directory is huge, we do not want to force the entire contents in-core at once.
            # The format of the file is a sequence of directory entries.
            # Each directory entry is:
            #   name_length  name  handle_length  handle
            # name_length is a 4-byte little-endian integer
            # handle_length is a 2-byte little-endian integer
            # no padding, so entries can become arbitrarily misaligned
            entlen = 6 + len(namebytes) + len(new_entry.filehandle)
            if pending_len and ((pending_len + entlen) > segment_len):
                # This entry would push us into the next segment.
                # Flush the current segment.
                # Write the number of bytes in the segment followed by the segment contents.
                with wts.timers.start('dir_generate_writeback1'):
                    entry_file.seek(segment_offset, io.SEEK_SET)
                    entry_file.write(struct.pack(STRUCT_LE_U32, pending_len))
                    segment_offset = entry_file.seek(0, io.SEEK_END)
                    entry_file.write(self._DUMMY_SEGMENT_LEN)
                # Reset
                pending_len = 0
                segment_len = CLFSSegment.DIR_OTHER_SEGMENT_BYTES
            # TODO: This avoids concatenating lots of bytes objects by assembling
            # a big list of bytes objects instead. Is that more efficient?
            # Here, I assume that the buffering on entry_file is essentially
            # bytes concat, so I let the work happen there at the cost of
            # making a big list. Need to benchmark to see whether the whole
            # entry should be appended to bytes_list once instead, or whether
            # the concat should just happen here followed by a big unbuffered write.
            # Can also consider having _entry_bytes() return a list/tuple itself.
            pending_len += self._entry_bytes_from_namebytes(namebytes, new_entry.filehandle, entry_file)
        self.db.dbc_upsert_multi(wts, new_children)
        del new_children
        with wts.timers.start('dir_generate_writeback2'):
            entry_file.seek(segment_offset, io.SEEK_SET)
            entry_file.write(struct.pack(STRUCT_LE_U32, pending_len))
            # Our caller will flush_and_close()
        return child_dir_count

    def entry_file_path(self, filehandle):
        'Return the full path name of the entry file for the directory with the given filehandle'
        fn = os.path.join(filehandle.hex()+'.entries')
        return os.path.join(self._clfsload.local_state_path, fn)

    NONUNIQUE_ERR = "inode numbers do not seem to be unique or the source is modified; this transfer must be abandoned"

    def _work_item_generate_target_obj_for_dir_ent(self, wts, tobj, entry, new_children):
        '''
        Generate the target_obj for entry in tobj and return it.
        If this is an additional link to a known object, update
        that target object and return it. It is important that the
        caller not modify the returned object. We do not return a unique
        copy to protect against that to avoid the overhead.
        entry: ReaderInfo
        '''
        logger = self.logger
        nlink = entry.ostat.st_nlink if self._clfsload.preserve_hardlinks else 1
        filehandle = None
        barrier = None
        backpointer = tobj.filehandle
        # tobj is the directory; entry is the directory entry we just found
        if tobj.ic_restored_in_progress or ((entry.ftype != Ftype.DIR) and (entry.ostat.st_nlink != 1) and self._clfsload.preserve_hardlinks):
            existing_key = ExistingTargetObjKey(entry.ostat.st_ino, entry.ftype)
            # We may have already seen this source object.
            # Must query the database to find out.
            wts.stats.stat_inc('check_existing')
            barrier = self.db.dbc_existing_key_barrier_begin(existing_key)
            maybe_matching_tobjs = self.db.dbc_get_existing(wts, existing_key)
            matching_tobjs = list()
            for maybe_tobj in maybe_matching_tobjs:
                if (not self._clfsload.preserve_hardlinks) and (maybe_tobj.source_path_str != entry.path):
                    continue
                if maybe_tobj.ftype == Ftype.DIR:
                    assert tobj.ic_restored_in_progress
                    backpointer_list = maybe_tobj.backpointer_list_generate(include_null_firstbackpointer=False)
                    if tobj.filehandle in backpointer_list:
                        err = "rescanning directory %s unexpectedly found in %s (directory seemingly appears more than once in its parent)" % (tobj.filehandle, maybe_tobj.describe())
                        logger.error("%s %s", wts, err)
                        err = self.NONUNIQUE_ERR
                        logger.error("%s %s", wts, err)
                        self._clfsload.fatal = err
                        raise SystemExit(1)
                    if not backpointer_list:
                        matching_tobjs.append(maybe_tobj)
                    else:
                        # Uh-oh. We are re-doing the generate, and we found a matching inode
                        # for what seems to be a different directory. This could happen
                        # if a directory is renamed in the source during the scan
                        # or if inode numbers are not unique in the source.
                        logger.error("rescanning directory %s found directory %s with mismatched backpointers"
                                     + "\nscanning directory:\n%s"
                                     + "\nfound directory:\n%s",
                                     tobj.filehandle, maybe_tobj.filehandle,
                                     tobj.pformat(),
                                     maybe_tobj.pformat())
                        err = self.NONUNIQUE_ERR
                        logger.error("%s %s", wts, err)
                        self._clfsload.fatal = err
                        raise SystemExit(1)
                else:
                    if (self._clfsload.preserve_hardlinks and (maybe_tobj.nlink > 1)) or (maybe_tobj.nlink_effective() == 0):
                        matching_tobjs.append(maybe_tobj)
            # matching_tobjs (both the list and/or its contents) may be shared with other threads; do not persist
            if matching_tobjs:
                any_at_backpointer_limit = False
                for tmp in matching_tobjs:
                    if (tmp.source_inode_number != entry.ostat.st_ino) or (tmp.ftype != entry.ftype):
                        raise AssertionError("filter mismatch source_inode_number='%s' entry.ostat.st_ino='%s' ftype='%s' entry.ftype='%s'" %
                                             (tmp.source_inode_number, entry.ostat.st_ino, tmp.ftype, entry.ftype))
                    if tmp.ftype != Ftype.DIR:
                        if tmp.nlink_effective() >= CLFS_LINK_MAX:
                            any_at_backpointer_limit = True
                if any_at_backpointer_limit or (len(matching_tobjs) > 1):
                    nlink = 2
                    # Use nlink=2 here so we do not garbage-collect it. That way
                    # this new source_path shows up in future warnings about
                    # multiple matches and we are sure to fix things up in the reconcile phase.
                    if any_at_backpointer_limit:
                        logger.warning("%s source '%s' inode=%s ftype=%s already found at max link count %d, treating as unique with nlink=%d",
                                       wts, entry.path, entry.ostat.st_ino, entry.ftype, CLFS_LINK_MAX, nlink)
                    else:
                        matching_names = [ntobj.source_path_str for ntobj in matching_tobjs]
                        logger.warning("%s source '%s' inode=%s ftype=%s has multiple possible matches, treating as possibly-unique with nlink=%d matching_names:\n%s",
                                       wts, entry.path, entry.ostat.st_ino, entry.ftype, nlink, pprint.pformat(matching_names))
                elif matching_tobjs:
                    existing_tobj = matching_tobjs[0]
                    try:
                        self.db.dbc_add_backpointer(wts, existing_tobj, backpointer)
                        existing_tobj.drop_barrier()
                        return existing_tobj
                    except BackpointerLimitReached:
                        nlink = 2
                        logger.warning("%s cannot add backpointer to %s for %s because the link limit is already reached; treating additional links as unique",
                                       wts, existing_tobj.filehandle, entry.path)
                    except (DbInconsistencyError, NamedObjectError, TerminalError):
                        raise
                    except Exception as e:
                        logger.error("%s cannot add backpointer to %s for %s: %s %s", wts, existing_tobj, entry.path, e.__class__.__name__, e)
                        wts.stats.stat_inc('error_count')
                        self.error_poke()
                        # Fall through - we make a separate copy, with nlink set to 2 to be sure it gets fixed up in the finalize phase
                        nlink = 2
        # In dirmgr terms, 'src' = parent 'tgt' = child
        dmid_src = backpointer.extract_dmid()
        assert dmid_src < self._clfsload.dirmgr_count
        dmid_tgt = self._clfsload.dmid_tgt_get_next() if entry.ftype == Ftype.DIR else dmid_src
        seq = self.db.dbc_fhseq_get()
        # CLFS (RestFillTask::init()) does not like filehandles where Hash::Fnv1Hash() is 0.
        # When that happens, we can get a v2 name parentHash where the high bits are all zero.
        # Appease CLFS by not using such filehandles.
        while True:
            filehandle = Filehandle.generate(wts.run_options.containerid, dmid_src, dmid_tgt, entry.ftype, seq)
            if fnv1_hash(filehandle.bytes) > 0:
                break
            wts.stats.stat_inc('fh_regenerate')
        nt = TargetObj(filehandle=filehandle,
                       ftype=entry.ftype,
                       mode=entry.ostat.st_mode,
                       nlink=nlink, # Updated after we scan
                       mtime=entry.ostat.st_mtime,
                       ctime=entry.ostat.st_ctime,
                       atime=entry.ostat.st_atime,
                       size=entry.ostat.st_size,
                       uid=entry.ostat.st_uid,
                       gid=entry.ostat.st_gid,
                       dev=entry.ostat.st_rdev,
                       first_backpointer=backpointer,
                       state=TargetObjState.PENDING,
                       source_inode_number=entry.ostat.st_ino,
                       source_path=entry.path,
                       pre_error_state=TargetObjState.PENDING,
                       barrier=barrier)
        if nt.ftype == Ftype.DIR:
            wts.stats.stat_inc('found_dir')
        else:
            wts.stats.stat_inc('found_nondir')
        new_children.append(nt)
        return nt

    def _work_item_writing(self, wts, tobj):
        '''
        Do the work to write tobj.
        '''
        will_reconcile = tobj.will_reconcile()
        if will_reconcile:
            self._clfsload.must_reconcile = True
        new_state = TargetObjState.ERROR
        prev_size = tobj.size
        queued = False
        timer_name = 'writing_dir' if tobj.ftype == Ftype.DIR else 'writing_nondir'
        new_size = None
        with wts.timers.start(timer_name):
            try:
                if tobj.ftype == Ftype.DIR:
                    entry_file_path = self.entry_file_path(tobj.filehandle)
                    if wts.process_okay:
                        try:
                            wts.pqueue_wt.put(PcomDirectoryWrite(tobj, entry_file_path), block=True, timeout=30.0)
                            queued = True
                        except:
                            err = "%s queue directory_write" % wts
                            exc_log(wts.logger, logging.WARNING, err)
                    if queued:
                        self._one_thread_wait_for_remote_done(wts, tobj)
                    else:
                        self._directory_write(wts, tobj, entry_file_path)
                else:
                    if wts.process_okay:
                        try:
                            wts.pqueue_wt.put(PcomTransfer(tobj), block=True, timeout=30.0)
                            queued = True
                        except:
                            err = "%s queue transfer" % wts
                            exc_log(wts.logger, logging.WARNING, err)
                    if queued:
                        resp = self._one_thread_wait_for_remote_done(wts, tobj)
                        if resp.tobj.size != prev_size:
                            new_size = resp.tobj.size
                    else:
                        self.writer.transfer(wts, tobj, self.reader)
                        if tobj.size != prev_size:
                            new_size = tobj.size
                will_reconcile = will_reconcile or tobj.will_reconcile()
                if will_reconcile:
                    self._clfsload.must_reconcile = True
                    new_state = TargetObjState.INODE_PENDING
                else:
                    # Within the transaction that updates state, this will
                    # get changed to INODE_PENDING iff necessary
                    new_state = TargetObjState.DONE
            except (NamedObjectError, TerminalError, WriteFileError):
                raise
            except Exception:
                what = "ftype=%s" % tobj.ftype
                wts.exc_error_exception(self, what=what)
            self._done_writing_update_state(wts, tobj, new_state, new_size)

    def _done_writing_update_state(self, wts, tobj, new_state, new_size):
        '''
        Update state after writing a target
        '''
        with wts.timers.start('writing_update_state'):
            self.db.dbc_update_state(wts, tobj, new_state, new_size=new_size, surrender_tobj=True)

    def _directory_write(self, wrock, tobj, entry_file_path):
        '''
        Core operation to do the work of writing directory contents.
        Invoked only from _work_item_writing().
        '''
        try:
            file_reader = self._clfsload.readfile_local_class(entry_file_path)
        except TargetObjectError:
            raise
        except FileNotFoundError as e:
            msg = "directory_write cannot open entry_file_path='%s': %s" % (entry_file_path, exc_info_name())
            raise TargetObjectError(tobj, msg) from e
        except Exception as e:
            msg = "directory_write cannot open entry_file_path='%s': %s" % (entry_file_path, exc_info_err())
            raise TargetObjectError(tobj, msg) from e
        self.writer.directory_write(wrock, tobj, file_reader)

class ReconcileWorkerPool(WorkerPool):
    'Worker operations that are specific to the reconcile phase'
    _phase = 'reconcile'
    _stats_class = ReconcileStats

    def _work_item_execute(self, wts, tobj):
        'Do the work for a single item previously returned by _work_item_get().'
        new_state = TargetObjState.ERROR
        with wts.timers.start('reconcile'):
            tobj_ftype = tobj.ftype
            try:
                if wts.process_okay:
                    wts.pqueue_wt.put(PcomReconcile(tobj), block=True, timeout=30.0)
                    self._one_thread_wait_for_remote_done(wts, tobj)
                else:
                    self.writer.reconcile(wts, tobj, self.reader)
                new_state = TargetObjState.DONE
            except (KeyboardInterrupt, TerminalError, SystemExit):
                raise
            except Exception:
                wts.exc_error_exception(self, what='reconcile')
            finally:
                if tobj_ftype == Ftype.DIR:
                    wts.stats.stat_inc('count_dir')
                else:
                    wts.stats.stat_inc('count_nondir')
            self.db.dbc_update_state(wts, tobj, new_state, surrender_tobj=True)

class CleanupWorkerPool(WorkerPool):
    'Worker operations that are specific to the cleanup phase'
    _phase = 'cleanup'
    _stats_class = CleanupStats
    _use_process = False

    def _work_item_execute(self, wts, tobj):
        'Do the work for a single item previously returned by _work_item_get().'
        new_state = TargetObjState.ERROR
        with wts.timers.start('cleanup'):
            tobj_ftype = tobj.ftype
            try:
                self.writer.cleanup(wts, tobj)
                new_state = TargetObjState.DONE
            except (KeyboardInterrupt, TerminalError, SystemExit):
                raise
            except:
                wts.logger.warning("warning (target is intact): did not cleanup %s: %s", tobj.describe(), exc_info_err())
                exc_log(wts.logger, logging.DEBUG, "did not cleanup %s" % tobj.describe())
                new_state = TargetObjState.DONE
                wts.stats.stat_inc('blob_not_deleted')
            finally:
                if tobj_ftype == Ftype.DIR:
                    wts.stats.stat_inc('count_dir')
                else:
                    wts.stats.stat_inc('count_nondir')
            self.db.dbc_update_state(wts, tobj, new_state, surrender_tobj=True)

class CLFSLoadResult():
    'Result for CLFSLoad.run_wrapped()'
    def __init__(self, clfsloadobj, exit_status, exc_info, time_start, time_done):
        self.clfsloadobj = clfsloadobj
        self.exit_status = exit_status
        self.exc_info = exc_info
        self.time_start = time_start
        self.time_done = time_done
        self.time_total = elapsed(self.time_start, self.time_done)
        self._progress = None
        self.dry_run_succeeded = getattr(self.clfsloadobj, 'dry_run_succeeded', None)
        if hasattr(self.clfsloadobj, 'thread_shared_state'):
            self.server_rejected_auth = self.clfsloadobj.thread_shared_state.server_rejected_auth
        else:
            self.server_rejected_auth = False
        self.fatal = getattr(self.clfsloadobj, 'fatal', None)
        # I wish Python had a truly immutable dict type that
        # we could use here to freeze global_stats.
        self.global_stats = getattr(self.clfsloadobj, 'global_stats', None)

    @property
    def progress(self):
        'Load and cache progress from this run'
        if self._progress is None:
            self._progress = self.clfsloadobj.db.db_get_progress()
        return self._progress

    def log_perf(self, logger=None, log_level=logging.INFO):
        'Log performance for this run'
        logger = logger if logger is not None else self.clfsloadobj.logger
        time_total = self.time_total
        progress = self.progress
        gb = progress.dr_gb
        count = progress.dr_count
        file_rate = count / self.time_total
        gb_rate = gb / time_total
        logger.log(log_level, "elapsed=%.3f GB=%.6f count=%d %.3f files/sec %.6f GB/s",
                   time_total, gb, count, file_rate, gb_rate)

class CLFSLoad():
    def __init__(self, command_args, logger,
                 create_local_state_dir=None,
                 retry_errors=None,
                 logfile_enable=True,
                 subprocess_enable=True,
                 transfer_pool_enable=True,
                 **kwargs):
        '''
        command_args: CommandArgs object encapsulating command-line arguments for this run
        logger: Default logger created by the front-end.
        create_local_state_dir: Whether the local_state_dir should be created or assumed to exist. None=rely on command-line args.
        db_class: Class to use for database actions
        writer_class: Class to use when writing CLFS
        reader_class: Class to use when reading the source
        readfile_local_class: Class to use for reading local files (.entries files)
        dry_run_helper_class: Passed to the dry_run reader
        transfer_worker_pool_class: Class to use for the transfer phase; TransferWorkerPool or a subclass
        reconcile_worker_pool_class: Class to use for the reconcile phase; ReconcileWorkerPool or a subclass
        cleanup_worker_pool_class: Class to use for the cleanup phase; CleanupWorkerPool or a subclass
        db_preclaim_state_class: Class to use for DB preclaim; clfsload.db.PreclaimState or a subclass
        logfile_enable: Output to the log file in the local state dir
        subprocess_enable: Allow the use of child processes to do more work with less GIL competition
        transfer_pool_enable: Enable a pool of threads in each child process.
                              Does nothing if subprocess_enable is not set.
        '''
        self._command_args = command_args
        self._run_started = False
        self._logger = logger
        self.check_python_version()
        self._create_local_state_dir = create_local_state_dir if create_local_state_dir is not None else self._command_args.new

        self._db_class = kwargs.pop('db_class', self._db_class)
        self._writer_class = kwargs.pop('writer_class', self._writer_class)
        self._reader_class = kwargs.pop('reader_class', self._reader_class)
        self._readfile_local_class = kwargs.pop('readfile_local_class', self._readfile_local_class)
        self._dry_run_helper_class = kwargs.pop('dry_run_helper_class', self._dry_run_helper_class)
        self._transfer_worker_pool_class = kwargs.pop('transfer_worker_pool_class', self._transfer_worker_pool_class)
        self._reconcile_worker_pool_class = kwargs.pop('reconcile_worker_pool_class', self._reconcile_worker_pool_class)
        self._cleanup_worker_pool_class = kwargs.pop('cleanup_worker_pool_class', self._cleanup_worker_pool_class)
        self._db_preclaim_state_class = kwargs.pop('db_preclaim_state_class', self._db_preclaim_state_class)

        self._source_root_path = self._command_args.source_root
        self._retry_errors = retry_errors if retry_errors is not None else self._command_args.retry_errors
        self._subprocess_enable = subprocess_enable
        self._azcopy = command_args.azcopy

        self._run_options = RunOptions()
        self._run_options.transfer_pool_enable = self._subprocess_enable and transfer_pool_enable

        self._logfile_handler = None
        self._errors_max = self._command_args.max_errors
        self._thread_shared_state = ThreadSharedState()
        self._db_lock = threading.RLock()
        self._preserve_hardlinks = int(bool(self._command_args.preserve_hardlinks))
        self._dirmgr_count = None
        self._containerid = None
        self._fsid = None
        self._dmid_lock = threading.RLock()
        self._dmid_idx = 0
        self._local_state_generated = False
        self._compression = self._command_args.compression
        self._compression_type = CLFSCompressionType.from_string(self._compression)
        self._encryption = self._command_args.encryption
        self._encryption_type = CLFSEncryptionType.from_string(self._encryption)
        self._reader = None
        self._dry_run_result = None
        self._target_root_filehandle = None
        self._must_reconcile = bool(self._preserve_hardlinks)
        if (not self._create_local_state_dir) or (not self._command_args.new):
            self._must_reconcile = True

        if self._source_root_path and self.local_state_path:
            sp = os.path.abspath(self._source_root_path) + os.path.sep
            tp = os.path.abspath(self.local_state_path) + os.path.sep
            if (sp == tp) or tp.startswith(sp) or sp.startswith(tp):
                logger.error("%s", LOCAL_STATE_PATH_UNIQ_STR)
                raise SystemExit(1)

        # Create the local state dir iff necessary and open logfiles
        self.check_local_state_dir(do_create=self._create_local_state_dir)
        if logfile_enable:
            self._open_logfile()

        self._global_stats = GenericStats()

        self._file_cleaner = FileCleaner(self._thread_shared_state, self._logger)
        self._db = self._db_class(self._thread_shared_state,
                                  self.local_state_path,
                                  logger,
                                  self._file_cleaner,
                                  toc_lock=self._db_lock,
                                  db_preclaim_state_class=self._db_preclaim_state_class)

        if self._source_root_path:
            self._reader = self._reader_class(self._command_args, logger, self._run_options, self._source_root_path)

        self.reconcile_total_count = None
        self.cleanup_total_count = None

        # Sometimes a fresh SAS token for a fresh container is rejected.
        # If the token is rejected, delay and retry before giving up.
        attempt_cur = 0
        attempt_max = 3
        while True:
            if attempt_cur:
                time.sleep(0.5)
            attempt_cur += 1
            try:
                self._writer_create()
                break
            except (ContainerTerminalError, ServerRejectedAuthError):
                if attempt_cur < attempt_max:
                    continue
                raise
        if attempt_cur > 1:
            self._global_stats.stat_inc('sas_token_retry_at_start')

        self._dry_run_succeeded = None

        self._fatal_lock = threading.Lock()
        self._fatal_txt = ''

        self.did_transfer = False
        self.did_reconcile = False
        self.did_cleanup = False
        self.transfer_count = None
        self.reconcile_count = None
        self.cleanup_count = None

        if kwargs:
            raise TypeError("unexpected kwargs: %s" % sorted(kwargs.keys()))

    # Default classes. May be overloaded in testing subclasses
    # or as kwargs to the constructor.
    _db_class = ClfsLoadDB
    _writer_class = WriterAzure
    _reader_class = ReaderPosix
    _readfile_local_class = ReadFilePosix
    _dry_run_helper_class = None
    _transfer_worker_pool_class = TransferWorkerPool
    _reconcile_worker_pool_class = ReconcileWorkerPool
    _cleanup_worker_pool_class = CleanupWorkerPool
    _db_preclaim_state_class = None

    def __del__(self):
        self._close_logfile()

    @property
    def fatal(self):
        'accessor'
        with self._fatal_lock:
            return self._fatal_txt

    @fatal.setter
    def fatal(self, value):
        'accessor'
        assert value
        with self._fatal_lock:
            if not self._fatal_txt:
                self._fatal_txt = str(value)

    @property
    def run_started(self):
        'accessor'
        return self._run_started

    @property
    def thread_shared_state(self):
        'accessor'
        return self._thread_shared_state

    @property
    def should_run(self):
        return self._thread_shared_state.should_run

    @should_run.setter
    def should_run(self, value):
        self._thread_shared_state.should_run = value
        self._db.wake_all()

    @property
    def preserve_hardlinks(self):
        'accessor'
        return self._preserve_hardlinks

    @property
    def subprocess_enable(self):
        'accessor'
        return self._subprocess_enable

    @property
    def must_reconcile(self):
        return self._must_reconcile

    @must_reconcile.setter
    def must_reconcile(self, value):
        assert value
        self._must_reconcile = value

    @property
    def containerid(self):
        'accessor'
        return self._containerid

    @property
    def dirmgr_count(self):
        'accessor'
        return self._dirmgr_count

    @property
    def local_state_path(self):
        'accessor'
        return self._command_args.local_state_path

    @property
    def reader(self):
        'accessor'
        return self._reader

    @property
    def readfile_local_class(self):
        'accessor'
        return self._readfile_local_class

    @property
    def writer(self):
        'accessor'
        return self._writer

    @property
    def db(self):
        'accessor'
        return self._db

    @property
    def logger(self):
        'accessor'
        return self._logger

    @property
    def errors_max(self):
        'accessor'
        return self._errors_max

    @property
    def target_root_filehandle(self):
        'accessor'
        return self._target_root_filehandle

    @property
    def compression(self):
        'accessor'
        return self._compression

    @property
    def compression_type(self):
        'accessor'
        return self._compression_type

    @property
    def encryption(self):
        'accessor'
        return self._encryption

    @property
    def encryption_type(self):
        'accessor'
        return self._encryption_type

    @property
    def db_lock(self):
        'accessor'
        return self._db_lock

    @property
    def run_options(self):
        'accessor'
        return self._run_options

    @property
    def dry_run_succeeded(self):
        'accessor'
        return self._dry_run_succeeded

    @property
    def global_stats(self):
        'accessor'
        return self._global_stats

    @property
    def azcopy(self):
        'accessor'
        return self._azcopy

    def check_python_version(self):
        '''
        Check the version of Python. Log an error and raise SystemExit
        if the version is not supported.
        '''
        ret = self.python_version_supported()
        if ret:
            self._logger.error("%s", ret)
            raise SystemExit(1)

    _REQUIRE_PYTHON_MAJOR = 3
    _REQUIRE_PYTHON_MINOR_MIN = 6

    @classmethod
    def python_version_supported(cls):
        '''
        Return whether or not the version of Python is supported.
        Return a string result. Empty string means the version is
        supported. Non-empty means it is not supported. The string
        contents are a human-readable reason why.
        '''
        ver = sys.version_info
        verstr = "%s.%s.%s" % (ver.major, ver.minor, ver.micro)
        require_major = cls._REQUIRE_PYTHON_MAJOR
        require_minor_min = cls._REQUIRE_PYTHON_MINOR_MIN
        if (ver.major != require_major) or (ver.minor < require_minor_min):
            return "requires Python%s version %s.%s or newer but got %s" % (require_major, require_major, require_minor_min, verstr)
        return ''

    def _writer_create(self):
        '''
        Create the writer to use for this run
        '''
        self._writer = self._writer_class(self._command_args,
                                          self._logger,
                                          self._run_options,
                                          self._command_args.local_state_path,
                                          self._command_args.target_storage_account,
                                          self._command_args.target_container,
                                          self._command_args.sas_token,
                                          self._command_args.worker_thread_count,
                                         )

    def _open_logfile(self):
        'The local state dir exists. Open the output logfile.'
        logfile_path = os.path.join(self.local_state_path, 'output.txt')
        self._logfile_handler = logging.FileHandler(logfile_path)
        self._logfile_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        self._logger.addHandler(self._logfile_handler)

    def _close_logfile(self):
        'Close the output logfile.'
        logger = getattr(self, 'logger', None)
        handler = getattr(self, '_logfile_handler', None)
        self._logfile_handler = None
        if (logger is not None) and (handler is not None):
            handler.flush()
            try:
                logger.removeHandler(handler)
            except Exception:
                pass
            handler.close()

    def _log_file_flush(self):
        'Flush logs'
        self._logfile_handler.flush()

    def dmid_tgt_get_next(self):
        'Return the next tgt_dmid to use for a new directory'
        with self._dmid_lock:
            self._dmid_idx += 1
            self._dmid_idx %= self._dirmgr_count
            return self._dmid_idx

    def _restore(self):
        '''
        Load state for a run. This is called when resuming a run
        as well as after _create() of a new run.
        '''
        logger = self._logger
        mdk = [DbEntMetaKey.COMPRESSION,
               DbEntMetaKey.CONTAINERID,
               DbEntMetaKey.DIRMGR_COUNT,
               DbEntMetaKey.ENCRYPTION,
               DbEntMetaKey.FSID,
               DbEntMetaKey.PRESERVE_HARDLINKS,
               DbEntMetaKey.ROOTFH,
               DbEntMetaKey.TARGET_CONTAINER,
               DbEntMetaKey.TARGET_STORAGE_ACCOUNT,
              ]
        # If a dry_run parameter was set on the command-line,
        # use that and push it into the metastore. Otherwise,
        # extract that parameter from the metastore. We must
        # poke these in here before db_dbc_restore_state().
        pending = list()
        if self._command_args.dry_run_gb is not None:
            pending.append((DbEntMetaKey.DRY_RUN_GB, self._command_args.dry_run_gb))
        if self._command_args.dry_run_count is not None:
            pending.append((DbEntMetaKey.DRY_RUN_COUNT, self._command_args.dry_run_count))
        if pending:
            self._db.db_meta_set(pending)
        mdd = self._db.db_meta_get(mdk)
        compression = mdd[DbEntMetaKey.COMPRESSION]
        if not compression:
            logger.error("compression not found in database")
            raise SystemExit(1)
        encryption = mdd[DbEntMetaKey.ENCRYPTION]
        if not encryption:
            logger.error("encryption not found in database")
            raise SystemExit(1)
        self._containerid = mdd[DbEntMetaKey.CONTAINERID]
        self._fsid = mdd[DbEntMetaKey.FSID]
        self._dirmgr_count = mdd[DbEntMetaKey.DIRMGR_COUNT]
        self._target_root_filehandle = mdd[DbEntMetaKey.ROOTFH]
        if bool(self._preserve_hardlinks) != bool(mdd[DbEntMetaKey.PRESERVE_HARDLINKS]):
            logger.error("preserve_hardlinks must be %s to resume this run", int(bool(self._preserve_hardlinks)))
            raise SystemExit(1)
        logger.debug("containerid %d (%s)", self._containerid, hex(self._containerid))
        logger.debug("fsid %d (%s)", self._fsid, hex(self._fsid))
        logger.info("preserve_hardlinks %s", self._preserve_hardlinks)
        self._propagate_values()
        target_storage_account = mdd[DbEntMetaKey.TARGET_STORAGE_ACCOUNT]
        target_container = mdd[DbEntMetaKey.TARGET_CONTAINER]
        target_db = target_storage_account + os.path.sep + target_container
        target_cmd = self._command_args.target_storage_account + os.path.sep + self._command_args.target_container
        if target_db != target_cmd:
            logger.error("this run was previously launched with target '%s' which does not match '%s'", target_db, target_cmd)
            raise SystemExit(1)
        if self._compression:
            if self._compression != compression:
                logger.error("previous run specified compression '%s' not '%s'", compression, self._compression)
                raise SystemExit(1)
        else:
            self._compression = compression
        self._compression_type = CLFSCompressionType.from_string(self._compression)
        if self._compression_type is None:
            logger.error("unrecognized compression '%s'", self._compression)
            raise SystemExit(1)
        if len(self._command_args.COMPRESSION_TYPES) > 1:
            logger.info("compression %s (%d)", self._compression, self._compression_type)
        if self._encryption:
            if self._encryption != encryption:
                logger.error("previous run specified encryption '%s' not '%s'", encryption, self._encryption)
                raise SystemExit(1)
        else:
            self._encryption = encryption
        self._encryption_type = CLFSEncryptionType.from_string(self._encryption)
        if self._encryption_type is None:
            logger.error("unrecognized encryption '%s'", self._encryption)
            raise SystemExit(1)
        if len(self._command_args.ENCRYPTION_TYPES) > 1:
            logger.info("encryption %s (%d)", self._encryption, self._encryption_type)
        self._propagate_values()

    def _propagate_values(self):
        '''
        Initialization is complete. Propagate values to
        other components before loading work-in-progress
        from the database.
        '''
        self._run_options.compression_type = self.compression_type
        self._run_options.containerid = self._containerid
        self._run_options.encryption_type = self.encryption_type
        self._run_options.fsid = self._fsid
        self._run_options.rootfh = self._target_root_filehandle

    def check_local_state_dir(self, do_create=False):
        '''
        Inspect the local state directory. If do_create is set,
        expect the directory to not exist. Otherwise,
        expect it to exist and be non-empty. Generate user-friendly
        errors.
        '''
        logger = self._logger
        if not self.local_state_path:
            logger.error("invalid local_state_path")
            raise SystemExit(1)
        if os.path.islink(self.local_state_path):
            logger.error("'%s' is a symbolic link", self.local_state_path)
            raise SystemExit(1)
        if os.path.exists(self.local_state_path):
            if not os.path.isdir(self.local_state_path):
                logger.error("'%s' already exists and is not a directory", self.local_state_path)
                raise SystemExit(1)
            if do_create:
                logger.error("'%s' already exists", self.local_state_path)
                raise SystemExit(1)
            try:
                result_list = os.listdir(self.local_state_path)
            except Exception as e:
                logger.error("cannot enumerate contents of '%s': %s %s", self.local_state_path, e.__class__.__name__, e)
                raise SystemExit(1)
            result_set = set(result_list)
            result_set.discard('.')
            result_set.discard('..')
            if not result_set:
                logger.error("'%s' exists but is empty", self.local_state_path)
                raise SystemExit(1)
        else:
            # self.local_state_path does not exist
            if not do_create:
                logger.error("'%s' does not exist", self.local_state_path)
                raise SystemExit(1)
            try:
                os.mkdir(self.local_state_path, mode=0o700)
            except Exception as e:
                logger.error("cannot create '%s': %s %s", self.local_state_path, e.__class__.__name__, e)
                raise SystemExit(1)

    def _create(self):
        'Generate database for a new run'
        # Choose initial values
        self._containerid = random.randint(1, UINT32_MAX)
        self._fsid = random.randint(1, UINT32_MAX)
        self._dirmgr_count = self._command_args.dirmgr_count
        if self._preserve_hardlinks and (not self._writer.supports_hardlinks()):
            self._logger.warning("disabling hardlink support because the writer does not support it")
            self._preserve_hardlinks = 0
        # Create the empty tables
        self._db.db_tables_create()
        # Insert metadata
        mdd = [(DbEntMetaKey.COMPRESSION, self._compression),
               (DbEntMetaKey.CONTAINERID, self._containerid),
               (DbEntMetaKey.DIRMGR_COUNT, self._dirmgr_count),
               (DbEntMetaKey.ENCRYPTION, self._encryption),
               (DbEntMetaKey.FSID, self._fsid),
               (DbEntMetaKey.PHASE, Phase.INIT),
               (DbEntMetaKey.PRESERVE_HARDLINKS, self._preserve_hardlinks),
               (DbEntMetaKey.VERSION_STRING, VERSION_STRING),
              ]
        mdd.extend(self._command_args.target_metastore())
        self._db.db_meta_set(mdd)
        self._db.phase = Phase.INIT
        # Now that we know the metastore contents, generate state from it
        self._propagate_values()
        # We need the toc thread to handle the root object upsert
        self._db_launch_toc_thread()
        # Generate the TargetObj entry for the root dir
        tobj = self.reader.target_obj_for_root_get()
        tobj.state = TargetObjState.GENERATING
        self._db.dbc_upsert_single(None, tobj)
        self._db.dbc_flush()
        self._db.db_meta_set([(DbEntMetaKey.PHASE, Phase.TRANSFER),
                              (DbEntMetaKey.ROOTFH, tobj.filehandle),
                             ])
        # Leave the toc thread running here. We already forced a flush,
        # so there's no work queued for it, so it will not try
        # to do work in the background that might interfere with
        # the forthcoming self._restore().

    def _load(self, wrock):
        'Load the database contents, creating it iff necessary'
        logger = self._logger
        if self._create_local_state_dir:
            self.writer.check_target_for_new_transfer()
            self._create()
            self._restore()
            self.writer.initialize_target_for_new_transfer(wrock)
            # Load newly-initialized state from the DB
            self._db.db_dbc_restore_state(reset_dirs=False)
        else:
            self._db.db_version_format_check()
            self._restore()
            if self._retry_errors:
                self._load__do_retry_errors()
            phase = self._db.db_meta_get(DbEntMetaKey.PHASE)
            if phase == Phase.DONE:
                if self._db.db_any_in_state(TargetObjState.ERROR):
                    logger.info("this transfer has already completed with errors and retry_errors is not specified")
                    raise SystemExit(1)
                logger.info("this transfer has already completed without errors")
                raise SystemExit(0)
            if phase == Phase.INIT:
                logger.error("this transfer previously failed during initialization")
                raise SystemExit(1)
            self.writer.check_target_transfer_in_progress(wrock)
            self._db.toc_preclaim.accepted_all_offers = False
            # Load current state from the DB
            self._db.db_dbc_restore_state(reset_dirs=True)
        self._db_launch_toc_thread()

    def _load__do_retry_errors(self):
        '''
        We are loading a previous run with retry_errors enabled.
        Handle that.
        '''
        ####################
        class RetryErrorsRock():
            def __init__(self, phase):
                self.new_phase = phase
                self.count = 0
                self.count_transfer = 0
        ####################
        logger = self._logger
        db = self._db
        prev_phase = self._db.db_meta_get(DbEntMetaKey.PHASE)
        if not db.db_any_in_state(TargetObjState.ERROR):
            logger.debug("retry_errors phase %s no errors", prev_phase)
            return
        retry_errors_rock = RetryErrorsRock(prev_phase)
        db.db_tobj_iterate_in_state(TargetObjState.ERROR,
                                    self._load__do_retry_errors__one_tobj,
                                    op_args=(retry_errors_rock,))
        db.db_meta_set([(DbEntMetaKey.PHASE, retry_errors_rock.new_phase)])
        prev_progress = db.db_get_progress()
        prev_progress_count = prev_progress.dr_count
        new_progress_count = max(prev_progress_count-retry_errors_rock.count_transfer, 0)
        db.db_meta_set([(DbEntMetaKey.PROGRESS_COUNT, new_progress_count)], session_wrapper=db.dbtobj)
        logger.debug("retry_errors new_phase=%s new_progress_count=%s", retry_errors_rock.new_phase, new_progress_count)
        logger.info("retry_errors reset count %d phase is now %s", retry_errors_rock.count, retry_errors_rock.new_phase)

    @staticmethod
    def _load__do_retry_errors__one_tobj(tobj, retry_errors_rock):
        '''
        Invoked within _load__do_retry_errors() on each TargetObj
        with state TargetObjState.ERROR. Reset the state to the
        pre_error_state and update the new phase to ensure we
        redo the work.
        '''
        retry_errors_rock.count += 1
        if tobj.state != TargetObjState.ERROR:
            raise SimpleError("unexpected state %s" % tobj.state)
        pre_error_state = tobj.pre_error_state
        new_phase = retry_errors_rock.new_phase
        if pre_error_state in (TargetObjState.PENDING, TargetObjState.GENERATING, TargetObjState.WRITING):
            new_phase = Phase.TRANSFER
            retry_errors_rock.count_transfer += 1
            tobj.reconcile_vers += 1
        elif pre_error_state in (TargetObjState.INODE_PENDING, TargetObjState.INODE_WRITING):
            new_phase = Phase.RECONCILE
            tobj.reconcile_vers += 1
        elif pre_error_state in (TargetObjState.INODE_CLEANUP_PENDING, TargetObjState.INODE_CLEANUP_CLEANING):
            new_phase = Phase.CLEANUP
        tobj.state = pre_error_state
        if new_phase < retry_errors_rock.new_phase:
            retry_errors_rock.new_phase = new_phase
        return True

    @classmethod
    def run_wrapped(cls, command_args, logger, proc, proc_args=None, proc_kwargs=None, clfsload_kwargs=None, exit_when_done=True):
        'Invoked from main() to construct a CLFSLoad object and invoke proc() on it'
        proc_args = proc_args if proc_args else tuple()
        proc_kwargs = proc_kwargs if proc_kwargs else dict()
        clfsload_kwargs = clfsload_kwargs if clfsload_kwargs else dict()
        exit_status = 1
        clfsloadobj = None
        exc_info = None
        t0 = time.time()
        constructed = False
        final_job_status = ''
        try:
            clfsloadobj = cls(command_args, logger, **clfsload_kwargs)
            constructed = True
            exit_status = proc(clfsloadobj, *proc_args, **proc_kwargs)
        except KeyboardInterrupt:
            final_job_status = 'Aborted'
            exc_info = sys.exc_info()[:2]
            logger.warning("KeyboardInterrupt %s", getframe(0))
        except SystemExit as e:
            exc_info = sys.exc_info()[:2]
            code = getattr(e, 'code', 'SystemExit without code')
            if isinstance(code, str):
                if not code:
                    logger.error("unexpected SystemExit with empty string, constructed=%s", constructed)
                else:
                    logger.error("SystemExit: %s", code)
                exit_status = 1
            elif isinstance(code, int):
                exit_status = code if constructed else 1
            elif code is None:
                exit_status = 0 if constructed else 1
            else:
                exit_status = int(bool(code)) if constructed else 1
            if exit_status == 0:
                exc_info = None
        except BaseException as e:
            exit_status = 1
            exc_info = sys.exc_info()
            ecn = exc_info[0].__name__.split('.')[-1]
            estr = str(exc_info[1])
            exc_info = exc_info[:2]
            action = 'init' if clfsloadobj is None else 'run'
            if isinstance(e, ContainerTerminalError):
                logger.error("The target container does not exist, or your secure access signature (sas_token) is invalid or expired or lacks the necessary permissions (read,write,delete,list)")
            elif isinstance(e, ServerRejectedAuthError):
                logger.error("%s", exc_info_err())
                logger.error("Your secure access signature is not accepted by Azure.")
                logger.error("See %s for more information.", SAS_DOC_URL)
            else:
                logger.error("%s %s stack:\n%s\n%s error: %s %s", action, ecn, exc_stack(), action, ecn, estr)
        if exit_status == 0:
            final_job_status = 'Completed'
        final_job_status = final_job_status or 'Failed'
        if clfsloadobj is not None:
            clfsloadobj.should_run = False
            if clfsloadobj.db:
                clfsloadobj.db.threads_stop(stop_only=True)
            clfsloadobj.file_cleaner_discard()
            t1 = time.time()
            if clfsloadobj.azcopy:
                clfsloadobj.azcopy_do_report_final(elapsed(t0, t1), final_job_status)
        else:
            t1 = time.time()

        result = CLFSLoadResult(clfsloadobj, exit_status, exc_info, t0, t1)
        if result.server_rejected_auth:
            logger.error("The server rejected authentication. Check your sas_token and try again.")
        if result.fatal:
            logger.error("%s", result.fatal)
        if exit_status:
            logger.debug("exit_status=%d elapsed=%.3f", exit_status, elapsed(t0, t1))
        else:
            result.log_perf()
        if exit_when_done:
            del clfsloadobj # discard the reference before generating the exception
            del result
            raise SystemExit(exit_status)
        return result

    def dump(self):
        'Dump DB contents for debugging. Return exit_status.'
        self._db.dump()
        return 0

    def _db_launch_toc_thread(self):
        'Launch the TOC thread'
        self._db.toc_thread_start()

    def _db_launch_fhseq_thread(self):
        'Launch the fhseq thread -- that thread preallocates ranges of filehandle sequence numbers'
        self._db.fhseq_thread_start()

    def _db_stop_fhseq_thread(self):
        'Stop the fhseq thread'
        self._db.fhseq_thread_stop()

    def file_cleaner_discard(self):
        '''
        Stop and discard the file cleaner
        '''
        if self._file_cleaner:
            self._file_cleaner.stop()
        # discard file_cleaner references
        self._db.file_cleaner = None
        self._file_cleaner = None

    def run(self):
        'Start or resume a transfer - this is the entrypoint from main()'
        logger = self._logger
        if not self._source_root_path:
            logger.error("no source_root provided")
            raise SystemExit(1)
        logger.info("CLFSLoad version %s", VERSION_STRING)
        wrock_init = WRock(self._logger, self._run_options, 'main.INIT', 0, self._writer)
        wrock_init.stats = InitStats()
        wrock_init.timers = TimerStats()
        try:
            self._load(wrock_init)
        except ServerRejectedAuthError:
            self.thread_shared_state.server_rejected_auth = True
            raise
        except TerminalError as e:
            self.fatal = str(e)
            raise SystemExit(1) from e

        if self._command_args.dry_run:
            dry_run_db = DryRunResult.get_from_db(self.db)
            logger.info("perform dry run on %s", self._source_root_path)
            # Use a separate instance of the reader so dry_run
            # state does not intersect other state.
            t0 = time.time()
            try:
                dry_run_reader = self._reader_class(self._command_args, logger, self._run_options, self._source_root_path)
            except SystemExit as e:
                logger.warning("%s %s", e.__class__.__name__, getframe(0))
                logger.error("dry run failed")
                raise

            try:
                self._dry_run_result = dry_run_reader.dry_run(self._command_args.worker_thread_count,
                                                              self._thread_shared_state,
                                                              self._preserve_hardlinks,
                                                              dry_run_helper_class=self._dry_run_helper_class)
                self._dry_run_succeeded = bool(self._thread_shared_state.should_run)
            except:
                self._dry_run_succeeded = False
                raise
            if not self._thread_shared_state.should_run:
                raise SystemExit(1)
            t1 = time.time()
            if self._dry_run_result:
                logger.info("dry run result %s elapsed=%f", self._dry_run_result, elapsed(t0, t1))
            else:
                logger.warning("no result from dry run elapsed=%.9f", elapsed(t0, t1))
            if self._dry_run_result != dry_run_db:
                if dry_run_db:
                    logger.debug("update dry run result from %s to %s", dry_run_db, self._dry_run_result)
                self._dry_run_result.db_put(self.db)
            else:
                logger.debug("dry run result %s unchanged", self._dry_run_result)
            self.db.db_load_dry_run()

        phase = self._db.db_meta_get(DbEntMetaKey.PHASE)
        logger.debug("initialization complete with phase=%s", phase.value)
        self._run_started = True

        self._propagate_values()

        del wrock_init

        if phase == Phase.TRANSFER:
            logger.info("begin phase transfer")
            self.did_transfer = True
            self._db.phase = Phase.TRANSFER
            self._db_launch_fhseq_thread()
            # Do not save a pointer to pool - it contains a backpointer to self
            pool = self._transfer_worker_pool_class(self, self._command_args.worker_thread_count)
            self._file_cleaner.start(pool.entry_file_path)
            pool.run()
            self.transfer_count = pool.work_count
            del pool # discard reference
            self._db_stop_fhseq_thread()
            root_tobj = self.root_tobj_get()
            if root_tobj.state == TargetObjState.ERROR:
                logger.error("root directory did not transfer successfully (state is %s); aborting",
                             target_obj_state_name(root_tobj.state))
                logger.error("%s", RERUN_STR)
                raise SystemExit(1)
            if self.must_reconcile:
                phase = Phase.RECONCILE
            else:
                phase = Phase.FINALIZE

        if phase == Phase.RECONCILE:
            logger.info("begin phase reconcile")
            self.did_reconcile = True
            self._db.phase = Phase.RECONCILE
            self.reconcile_total_count = self._db.db_get_reconcile_pending_count()
            logger.debug("reconcile count %d", self.reconcile_total_count)
            if self.reconcile_total_count:
                # Do not save a pointer to pool - it contains a backpointer to self
                pool = self._reconcile_worker_pool_class(self, self._command_args.worker_thread_count)
                pool.run()
                self.reconcile_count = pool.work_count
                del pool
            self.post_reconcile_sanity()
            phase = Phase.CLEANUP

        if phase == Phase.CLEANUP:
            logger.info("begin phase cleanup")
            self.did_cleanup = True
            self._db.phase = Phase.CLEANUP
            self.cleanup_total_count = self._db.db_get_cleanup_pending_count()
            logger.info("cleanup count %d", self.cleanup_total_count)
            if self.cleanup_total_count:
                # Do not save a pointer to pool - it contains a backpointer to self
                pool = self._cleanup_worker_pool_class(self, self._command_args.worker_thread_count)
                pool.run()
                self.cleanup_count = pool.work_count
                del pool
            phase = Phase.FINALIZE

        any_errors = False

        if phase == Phase.FINALIZE:
            logger.info("begin phase finalize")
            wrock = WRock(self._logger, self._run_options, 'main.finalize', 0, self._writer)
            wrock.stats = FinalizeStats()
            wrock.timers = TimerStats()
            self._db.phase = Phase.FINALIZE
            any_errors = self._phase_finalize(wrock)
            del wrock

        self.file_cleaner_discard()

        logger.info("all phases complete")

        sanity_ps = PreclaimState(logger)
        sanity_ps.load_startup(self._db, reset_dirs=False)
        if sanity_ps:
            logger.error("not all transfers completed (%d aborted)", len(sanity_ps))
            any_errors = True

        logger.debug("mark DB done")
        self._db.phase = Phase.DONE
        self._db.threads_stop()

        if any_errors:
            logger.error("load failed")
            raise SystemExit(1)

        logger.info("CLFSLoad succeeded")
        self._db.phase = Phase.DONE
        return 0

    def post_reconcile_sanity(self):
        '''
        This is invoked by the main thread after the worker threads complete.
        Its job is to verify backpointer state.
        The root object must not have any backpointers.
        Any other object with no backpointers is orphaned and must
        be flagged as an error.
        '''
        logger = self.logger

        # First, check that the target root has no backpointers
        root_tobj = self.root_tobj_get()
        if not root_tobj:
            logger.error("post_reconcile cannot find root %s", self._target_root_filehandle)
            raise SystemExit(1)
        if root_tobj.state in (TargetObjState.INODE_CLEANUP_PENDING, TargetObjState.DONE):
            backpointers = root_tobj.backpointer_list_generate(include_null_firstbackpointer=True)
            backpointers_expected = [FILEHANDLE_NULL]
            if backpointers != backpointers_expected:
                logger.error("post_reconcile found backpointers for root %s:\n%s",
                             self._target_root_filehandle, pprint.pformat(backpointers))
                root_tobj = self.db.db_tobj_set_state(self._target_root_filehandle, TargetObjState.ERROR)
                if not root_tobj:
                    logger.error("post_reconcile update cannot find root %s", self._target_root_filehandle)
                    raise SystemExit(1)
        elif root_tobj.state != TargetObjState.ERROR:
            logger.error("post_reconcile found root %s with unexpected state %s",
                         self._target_root_filehandle, target_obj_state_name(root_tobj.state))
            raise SystemExit(1)

        # Now update all non-root filehandles with no backpointers
        # to ERROR.
        self.db.db_backpointer_check(self._target_root_filehandle)

    @staticmethod
    def _phase_finalize__write_problems__one_error_tobj(tobj, problem_writer):
        '''
        This is invoked on each TargetObj in the ERROR state during _phase_finalize().
        Write the source file path to the problem file using problem_writer.
        '''
        problem_writer.write(tobj.source_path_bytes)
        problem_writer.write(b"\n")
        return False

    def _phase_finalize__write_problems(self):
        '''
        Write a single file in the output directory enumerating
        the source paths of files/directories with transfer errors.
        If there are no errors, do not write a file.
        Returns True iff any problems are found
        '''
        logger = self._logger
        ret = False
        problem_path = os.path.join(self.local_state_path, 'problems.txt')
        if os.path.exists(problem_path):
            logger.debug("remove %s", problem_path)
            os.unlink(problem_path)
        if self._db.db_any_in_state(TargetObjState.ERROR):
            try:
                problem_writer = self.writer.local_file_writer(problem_path, logger)
                problem_writer.truncate()
                self._db.db_tobj_iterate_in_state(TargetObjState.ERROR,
                                                  self._phase_finalize__write_problems__one_error_tobj,
                                                  op_args=(problem_writer,))
                problem_writer.flush_and_close()
            except (KeyboardInterrupt, SystemExit):
                logger.warning("%s %s", exc_info_err(), getframe(0))
                raise
            except:
                err = exc_info_err()
                logger.error("stack:\n%s", exc_stack())
                logger.error("%s", err)
                raise
            self._log_file_flush()
            logger.info("done writing %s", problem_path)
            logger.warning("%s", RERUN_STR)
            ret = True
        else:
            logger.debug("did not write problems file because no target objects with errors remain")
        return ret

    def _phase_finalize(self, wrock):
        '''
        Do the work of the finalize phase.
        Returns True to indicate that one or more errors are detected.
        '''
        ret = self._phase_finalize__write_problems()
        self._log_file_flush()
        root_tobj = self.root_tobj_get()
        logfile_names = sorted([logfile for logfile in os.listdir(self.local_state_path) if logfile.endswith('.txt')])
        try:
            self.writer.finalize(wrock, self._target_root_filehandle, root_tobj, self.local_state_path, logfile_names)
        except (KeyboardInterrupt, SystemExit, TerminalError, WriteFileError):
            raise
        except BaseException as e:
            self._logger.error("%s %s", wrock, exc_info_err())
            if not isinstance(e, NamedObjectError):
                self._logger.debug("%s stack:\n%s", wrock, exc_stack())
            raise SystemExit(1) from e
        return ret

    def root_tobj_get(self):
        '''
        Return the TargetObj for the target root directory.
        Do not call this while the TOC thread is running.
        '''
        return self.db.db_tobj_get(self._target_root_filehandle)

    def azcopy_do_report_final(self, time_elapsed, final_job_status):
        '''
        Generate and log the final azcopy report.
        Called from run_wrapped(). Call this exactly once per azcopy run.
        '''
        time_elapsed = float(time_elapsed)
        final_job_status = str(final_job_status)
        try:
            total = self.db.db_count_all()
        except:
            total = None
        try:
            done = int(self.db.db_count_in_state(TargetObjState.DONE))
        except:
            done = None
        try:
            failed = int(self.db.db_count_in_state(TargetObjState.ERROR))
        except:
            failed = None
        try:
            progress = self.db.db_get_progress()
            total_bytes_transferred = int(progress.dr_gb * Size.GB)
        except:
            total_bytes_transferred = None
        rpt = {'elapsed' : time_elapsed,
               'files_completed_success' : done,
               'files_completed_failed' : failed,
               'files_skipped' : 0,
               'files_total' : total,
               'final_job_status' : str(final_job_status),
               'total_bytes_transferred' : total_bytes_transferred,
              }
        self.logger.info("azcopy:\n%s", self.azcopy_report_final_string(rpt))

    @staticmethod
    def azcopy_report_interval_string(azcopy_rpt):
        '''
        Given a report in dict form (azcopy_rpt), generate the output string for azcopy.
        The caller is expected to log this on a line by itself,
        followed by a newline.
        '''
        return 'AZCOPY:' + json.dumps(azcopy_rpt, separators=(',', ':'), check_circular=False)

    @staticmethod
    def azcopy_report_final_string(azcopy_rpt):
        '''
        Given a report in dict form (azcopy_rpt), generate the output string for azcopy.
        The caller is expected to log this on a line by itself,
        followed by a newline.
        '''
        return 'AZCOPY-FINAL:' + json.dumps(azcopy_rpt, separators=(',', ':'), check_circular=False)

def main(*args):
    logger = logger_create(logging.INFO)
    command_args = CommandArgs(logger, prog='CLFSLoad', add_version=True)
    command_args.parse(*args)
    clfsload_kwargs = dict()
    CLFSLoad.run_wrapped(command_args, logger, CLFSLoad.run, clfsload_kwargs=clfsload_kwargs)

if __name__ == '__main__':
    main()
    sys.exit(1)
