#
# clfsload/db.py
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
A note about hard links: During the transfer phase, any object
upserted with nlink=1 is not eligible to have backpointers added.
Among other things, the update fastpaths assume this. Any code
is free to assume that any object with nlink=1 during the transfer
phase will never legitimately gain extra links.
'''

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import enum
import json
import logging
import os
import pprint
import sqlite3
import threading
import time

import sqlalchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session

from clfsload.types import AbortException, AdditionalBackpointers, BackpointerLimitReached, \
                           CLFS_LINK_MAX, DATA_FTYPES, \
                           DbEntBase, DbEntAdditionalBackpointerMapEnt, DbEntMeta, DbEntTargetObj, \
                           DBStats, DbEntMetaKey, DbInconsistencyError, DbTerminalError,\
                           DryRunResult, ExistingTargetObjKeyBarrier, \
                           FILEHANDLE_NULL_BYTES, Filehandle, Ftype, Phase, \
                           SimpleError, TargetObjState, \
                           TerminalError, TargetObj, TimerStats
from clfsload.util import Monitor, Size, elapsed, exc_info_err, exc_log, exc_stack, getframe, notify_all

_CACHE_SIZE = Size.GB
_JOURNAL_MODE = 'WAL'
_MMAP_SIZE = 4 * Size.GB
_PAGE_SIZE = 16 * Size.KB

@event.listens_for(Engine, "connect")
def _connect__set_sqlite_pragma(dbapi_connection, connection_record): # pylint: disable=unused-argument
    '''
    See:
        https://docs.sqlalchemy.org/en/13/dialects/sqlite.html
        https://sqlite.org/pragma.html
    '''
    cursor = None
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA cache_size=%d;" % _CACHE_SIZE)
        cursor.execute("PRAGMA journal_mode=%s;" % _JOURNAL_MODE)
        cursor.execute("PRAGMA mmap_size=%d;" % _MMAP_SIZE)
        # page_size handled in _DBSessionWrapper.__init__
    finally:
        if cursor:
            cursor.close()

class ClfsLoadDB():
    '''
    This class handles the database-level interactions for CLFSLoad.
    This uses SQLAlchemy with SQLite for the backend.
    To reduce performance impact, this object maintains a partial
    cache of TargetObj rows in various states.

    Method naming:
    db_*: Direct database access. Time-consuming.
    dbc_*: Cached database access. Faster.

    While worker phases are executing, only dbc accesses read and modify
    DbEntTargetObj. This allows aggressive caching and prefetching/preclaiming.

    The metastore and the target object table are not only different
    tables, they are stored as entirely separate databases to eliminate
    locking conflicts between sessions (transactions). Otherwise,
    we get false sharing conflicts between allocating new filehandle
    ranges and flushing target object batches.
    '''
    def __init__(self, thread_shared_state, dirname, logger, file_cleaner, toc_lock=None, db_preclaim_state_class=None):
        self._thread_shared_state = thread_shared_state
        self._dirname = dirname
        self._logger = logger
        self.file_cleaner = file_cleaner

        db_preclaim_state_class = db_preclaim_state_class if db_preclaim_state_class else PreclaimState

        self._phase = None
        self.phase_terminal_states = tuple()

        self.dbmeta = _DBSessionWrapper(logger, os.path.join(self._dirname, 'meta.db'))
        self.dbtobj = _DBSessionWrapper(logger, os.path.join(self._dirname, 'target.db'))
        self._dbws = [self.dbmeta, self.dbtobj]

        self.dr_init = None # progress at start/resume
        self.dr_current = None # current total progress
        self.dr_input = None # dry run parameters
        self.progress_wrote_last_count = None
        self.progress_wrote_last_gb = None

        self._bkg_threadstates = list()

        self._is_consistent = True
        self._terminal_error = ''
        self._terminal_error_lock = threading.RLock()

        # Public names to simplify sharing with BackgroundThreadStateFhseq
        self.fhseq_lock = threading.RLock()
        self.fhseq_run_cond = threading.Condition(lock=self.fhseq_lock)
        self.fhseq_get_cond = threading.Condition(lock=self.fhseq_lock)
        self.fhseq_prev = None # most recent value returned from dbc_fhseq_get()
        self.fhseq_dbval = None # last value in the DB
        self.fhseq_thread_state = self.BackgroundThreadStateFhseq(self._thread_shared_state, 'fhseq-refresh', self.fhseq_run_cond)
        self._bkg_threadstates.append(self.fhseq_thread_state)

        # Public names to simplify sharing with BackgroundThreadStateToc
        # toc: TargetObj Cache
        self.toc_lock = toc_lock if toc_lock else threading.RLock()
        self.toc_run_cond = threading.Condition(lock=self.toc_lock)
        self.toc_flush_cond = threading.Condition(lock=self.toc_lock)
        self.toc_flushing = None # batch that is currently flushing
        self.toc_idx_prev = 1 # Always start with a value > 0 to enable logic that waits for flushes
        self.toc_idx_flushed_last = 0
        self.toc_idx_flushed_and_work_remaining_last = 0
        self.toc_flush_list = collections.deque()
        self.toc_flush_morenondir_list = collections.deque()
        self.toc_flush_moredir_list = collections.deque()
        self.toc_buffering = self.TocBatch(self.toc_idx_prev)
        self.toc_state_claim_from = None
        self.toc_state_claim_to = tuple()
        self.toc_thread_state = self.BackgroundThreadStateToc(self._thread_shared_state, 'toc', self.toc_run_cond)
        self.toc_preclaim = db_preclaim_state_class(self._logger)
        self.toc_flush_next_upsert = False
        self.toc_idx_for_preclaim_last = 0
        self._bkg_threadstates.append(self.toc_thread_state)

        # Set of ExistingTargetObjKey for objects for which dbc_get_existing()
        # has been called and dbc_upsert has not been called.
        # Does not include non-DIR objects with nlink < 2.
        # Note: An alternate implementation sould be to have upsert_existing_pending_dict
        # with ExistingTargetObjKey keys and threading.Condition values. Doing
        # that replaces false wakeups with the expense of constructing
        # and destroying threading.Condition.
        self.upsert_existing_pending_set = set()
        self.upsert_existing_pending_cond = threading.Condition(lock=self.toc_lock)

        # phase_work_poke: invoked as phase_work_poked(num) where num is a number
        # of work items now available.
        self.phase_work_poke = self._noop

        # phase_threads_are_idle: hint from the core that worker threads
        # are sitting around doing nothing
        self._all_phase_threads_are_idle = False
        self._any_phase_threads_are_idle = False

        self.timers = TimerStats()
        self.stats = DBStats(lock=self.toc_lock)

        self.query_DbEntMeta = Query((DbEntMeta,))
        self.query_DbEntTargetObj = Query((DbEntTargetObj,))
        self.query_DbEntAdditionalBackpointerMapEnt = Query((DbEntAdditionalBackpointerMapEnt,))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self._dirname)

    def __repr__(self):
        return "<%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self._dirname)

    @staticmethod
    def _noop(num):
        pass

    def check_usable(self):
        if not self.should_run:
            raise SystemExit(1)
        if not self._is_consistent:
            raise DbTerminalError("%s is inconsistent" % str(self))
        if self._terminal_error:
            raise DbTerminalError(self._terminal_error)

    def raise_terminal(self, err, fromex=None):
        '''
        A terminal error is encountered. Raise and remember it.
        '''
        self.terminal_error = err.rstrip() + ' ' + getframe(1)
        raise DbTerminalError(err) from fromex

    @property
    def logger(self):
        return self._logger

    @property
    def should_run(self):
        return self._thread_shared_state.should_run

    @should_run.setter
    def should_run(self, value):
        self._thread_shared_state.should_run = value

    @property
    def is_consistent(self):
        return self._is_consistent

    @is_consistent.setter
    def is_consistent(self, value):
        with self._terminal_error_lock:
            if value:
                raise SimpleError("%s attempt to set is_consistent to %s %s" % (self, value.__class__.__name__, value))
            if self._is_consistent:
                self._logger.error("%s marking inconsistent", self)
            self._is_consistent = False
            if not self._terminal_error:
                self._terminal_error = "database is inconsistent"
                self._logger.error("%s DB terminal_error is now '%s'" % (self, self._terminal_error))
        self._best_effort_wake_all()

    @property
    def phase(self):
        return self._phase

    @phase.setter
    def phase(self, value):
        if self._set_phase(value):
            self.db_meta_set([(DbEntMetaKey.PHASE, self._phase)])

    def _set_phase(self, value):
        '''
        Set self._phase = value.
        Caller holds toc_lock.
        Returns whether or not the phase changed.
        '''
        ret = False
        with self.toc_lock:
            prev_phase = self._phase
            if prev_phase != value:
                self.toc_idx_flushed_and_work_remaining_last = 0
                ret = True
            self.check_usable()
            if self.toc_flushing is not None:
                self.raise_terminal("attempt to update phase while flushing")
            if self.toc_buffering:
                self.raise_terminal("attempt to update phase with unflushed updates")
            self._phase = value
            if self._phase == Phase.TRANSFER:
                self.toc_state_claim_from = TargetObjState.PENDING
                self.toc_state_claim_to = (TargetObjState.GENERATING, TargetObjState.WRITING)
            elif self._phase == Phase.RECONCILE:
                self.toc_preclaim.accepted_all_offers = False
                self.toc_state_claim_from = TargetObjState.INODE_PENDING
                self.toc_state_claim_to = (TargetObjState.INODE_WRITING,)
            elif self._phase == Phase.CLEANUP:
                self.toc_preclaim.accepted_all_offers = False
                self.toc_state_claim_from = TargetObjState.INODE_CLEANUP_PENDING
                self.toc_state_claim_to = (TargetObjState.INODE_CLEANUP_CLEANING,)
            else:
                self.toc_state_claim_from = None
                self.toc_state_claim_to = tuple()
            self.phase_work_poke = self._noop
            self._all_phase_threads_are_idle = False
            self._any_phase_threads_are_idle = False
            self.toc_preclaim.phase = self._phase
            self._logger.debug("DB internal phase is now %s", self._phase)
            if self._phase == Phase.TRANSFER:
                self.phase_terminal_states = (TargetObjState.INODE_PENDING, TargetObjState.DONE, TargetObjState.ERROR)
            elif self._phase == Phase.RECONCILE:
                self.phase_terminal_states = (TargetObjState.INODE_CLEANUP_PENDING, TargetObjState.ERROR)
            elif self._phase == Phase.CLEANUP:
                self.phase_terminal_states = (TargetObjState.DONE, TargetObjState.ERROR)
            else:
                self.phase_terminal_states = tuple()
        return ret

    @property
    def all_phase_threads_are_idle(self):
        return self._all_phase_threads_are_idle

    @all_phase_threads_are_idle.setter
    def all_phase_threads_are_idle(self, value):
        with self.toc_lock:
            self._all_phase_threads_are_idle = value
            self.check_phase_idle_NL()

    @property
    def any_phase_threads_are_idle(self):
        return self._any_phase_threads_are_idle

    @any_phase_threads_are_idle.setter
    def any_phase_threads_are_idle(self, value):
        with self.toc_lock:
            self._any_phase_threads_are_idle = value
            self.check_phase_idle_NL()

    def _best_effort_wake_all(self):
        '''
        Hacky way to wake up sleeping threads. Used when the DB becomes ill
        to let waiters know to bail out. Not guaranteed to issue wakes.
        Frustrating that Python Condition variables require locks held.
        '''
        with Monitor(self.toc_lock, blocking=False) as m:
            if m.havelock:
                self.toc_run_cond.notify_all()
                self.toc_flush_cond.notify_all()
                self.upsert_existing_pending_cond.notify_all()
        with Monitor(self.fhseq_lock, blocking=False) as m:
            if m.havelock:
                self.fhseq_run_cond.notify_all()
                self.fhseq_get_cond.notify_all()
        self.phase_work_poke(0)

    @property
    def terminal_error(self):
        return self._terminal_error

    @terminal_error.setter
    def terminal_error(self, value):
        with self._terminal_error_lock:
            if not value:
                raise SimpleError("%s attempt to set terminal_error to something falsey (%s %s)" % (self, value.__class__.__name__, value))
            # Latch and log the first terminal error
            if not self._terminal_error:
                self._terminal_error = value
                if self._terminal_error != 'SystemExit':
                    self._logger.error("%s DB terminal_error is now '%s'" % (self, self._terminal_error))
        self._best_effort_wake_all()

    def toc_queue_len(self):
        '''
        Return the number of flushes logically pending.
        This is intended as a user-facing (debugging) number in stats
        reporting.
        '''
        with self.toc_lock:
            ret = 0
            if self.toc_buffering:
                ret += 1
            ret += len(self.toc_flush_list)
            if self.toc_flushing is not None:
                ret += 1
            return ret

    def toc_thread_start(self):
        'launch toc thread only'
        self.toc_thread_state.start(self)
        self.toc_thread_state.wait_for_running(self)

    def fhseq_thread_start(self):
        'start the fhseq thread and wait for it to complete its first fetch'
        self.fhseq_thread_state.start(self)
        self.fhseq_thread_state.wait_for_running(self)
        with self.fhseq_lock:
            while self.fhseq_thread_state.any_work_to_do_NL(self):
                self.check_usable()
                self.fhseq_get_cond.wait(timeout=1.0)

    def fhseq_thread_stop(self):
        'stop the fhseq thread and wait for it to exit'
        self.fhseq_thread_state.stop()
        self.fhseq_thread_state.wait_for_stopped(self)

    def threads_stop(self, stop_only=False):
        'stop background threads'
        for threadstate in self._bkg_threadstates:
            threadstate.stop()
        if stop_only:
            return
        for threadstate in self._bkg_threadstates:
            threadstate.wait_for_stopped(self)
        self._logger.debug("DB timers:\n%s\nstats:\n%s", self.timers.pformat(), self.stats.pformat())

    @staticmethod
    def version_format_get():
        'This is the version number written as VERSION_FORMAT in the metastore'
        return 1

    def db_tables_create(self):
        '''
        Create the database tables. This implies that we are starting a new run.
        '''
        for dbw in self._dbws:
            dbw.create_all()
        # set VERSION_FORMAT in the metastore
        self.db_meta_set([(DbEntMetaKey.VERSION_FORMAT, self.version_format_get())])
        # PROGRESS_* are in the tobj DB so they may be updated when TargetObj states change
        mdd = [(DbEntMetaKey.PROGRESS_COUNT, int(0)),
               (DbEntMetaKey.PROGRESS_GB, float(0.0)),
              ]
        self.db_meta_set(mdd, session_wrapper=self.dbtobj)

    def db_dbc_restore_state(self, reset_dirs=True):
        '''
        We are starting up. Restore state (if any) from previous run.
        This performs real, blocking DB accesses to load dbc state.
        '''
        self.toc_preclaim.load_startup(self, reset_dirs=reset_dirs)
        self.db_load_dry_run()

    def db_get_progress(self):
        'Fetch the current PROGRESS values and return them in DryRunResult form'
        # PROGRESS_* are in the tobj DB so they may be updated when TargetObj states change
        mdk = [DbEntMetaKey.PROGRESS_COUNT,
               DbEntMetaKey.PROGRESS_GB,
              ]
        mdd = self.db_meta_get(mdk, session_wrapper=self.dbtobj)
        return DryRunResult(count=mdd[DbEntMetaKey.PROGRESS_COUNT],
                            gb=mdd[DbEntMetaKey.PROGRESS_GB])

    def db_get_reconcile_pending_count(self):
        'Return the number of items pending reconcile'
        ret = 0
        session = self.dbtobj.session_get()
        try:
            for state in (TargetObjState.INODE_PENDING, TargetObjState.INODE_WRITING):
                query = self.query_DbEntTargetObj.with_session(session)
                query = query.filter(DbEntTargetObj.state == state)
                ret += query.count()
            return ret
        finally:
            self.dbtobj.session_release(session)

    def db_get_cleanup_pending_count(self):
        'Return the number of items pending cleanup'
        ret = 0
        session = self.dbtobj.session_get()
        try:
            for state in (TargetObjState.INODE_CLEANUP_PENDING, TargetObjState.INODE_CLEANUP_CLEANING):
                query = self.query_DbEntTargetObj.with_session(session)
                query = query.filter(DbEntTargetObj.state == state)
                ret += query.count()
            return ret
        finally:
            self.dbtobj.session_release(session)

    def db_any_in_state(self, state):
        'Return whether there are any target objects in the given state'
        return bool(self.db_count_in_state(state, limit=1))

    def db_count_in_state(self, state, limit=0):
        'Return the number of items in the given state'
        session = self.dbtobj.session_get()
        try:
            query = self.query_DbEntTargetObj.with_session(session)
            query = query.filter(DbEntTargetObj.state == state)
            if limit:
                query = query.limit(limit)
            return query.count()
        finally:
            self.dbtobj.session_release(session)

    def db_load_dry_run(self):
        'Load dry_run results'
        self.dr_init = self.db_get_progress()
        self.dr_current = DryRunResult(self.dr_init.to_meta())
        self.progress_wrote_last_count = self.dr_current.dr_count
        self.progress_wrote_last_gb = self.dr_current.dr_gb

        # Pull the dry_run results from the normal metastore.
        # If they were provided on the command-line, then those
        # values are already poked into the DB.
        self.dr_input = DryRunResult.get_from_db(self)

    def db_has_eta_info(self):
        if self.dr_input.dr_gb is not None and self.dr_input.dr_count is not None:
            return True
        return False

    def db_version_format_check(self):
        'Compare the VERSION_FORMAT in the metastore with ours'
        logger = self._logger
        m = self.db_meta_get(DbEntMetaKey.VERSION_FORMAT)
        v = self.version_format_get()
        if m is None:
            raise TerminalError("%s does not appear to contain fully initialized state" % self._dirname)
        if m != v:
            raise TerminalError("%s has version_format=%s which does not match our version %d" % (self._dirname, m, v))
        logger.debug("%s matched version_format=%d", self._dirname, m)

    def db_meta_get(self, keys, session_wrapper=None):
        '''
        Given a single key in keys, return the associated value in the metastore.
        Given a list of keys, return a dict of values from the metastore.
        None in place of an ent means that the row does not exist.
        '''
        session_wrapper = session_wrapper if session_wrapper else self.dbmeta
        session = session_wrapper.session_get()
        try:
            query = self.query_DbEntMeta.with_session(session)
            if isinstance(keys, (list, tuple, set)):
                ret = dict()
                for key in keys:
                    ent = query.get(key)
                    if ent is not None:
                        ret[key] = DbEntMetaKey.value_from_dbe(key, ent.m_value)
                    else:
                        ret[key] = None
                return ret
            key = keys
            ent = query.get(key)
            if ent is None:
                return None
            return DbEntMetaKey.value_from_dbe(key, ent.m_value)
        finally:
            session_wrapper.session_release(session)

    def db_meta_set(self, kvs, session_wrapper=None):
        'Write a list of key, value tuples to the metastore'
        session_wrapper = session_wrapper if session_wrapper else self.dbmeta
        session = session_wrapper.session_get()
        try:
            for k, v in kvs:
                value = v
                if isinstance(value, enum.Enum):
                    value = value.value
                elif isinstance(value, Filehandle):
                    value = value.hex()
                m_value = json.dumps(value)
                md = DbEntMeta(m_key=k, m_value=m_value)
                session.merge(md)
            session.commit()
        finally:
            session_wrapper.session_release(session)

    class _BackgroundThreadState():
        'State for one background thread'
        def __init__(self, thread_shared_state, thread_name, run_cond):
            self._thread_shared_state = thread_shared_state
            self._thread_name = thread_name
            self._thread = None
            self._run_cond = run_cond
            self._run_lock = run_cond._lock # pylint: disable=protected-access
            self._should_run = True
            self._started = False
            self._is_done = False
            self._slept = False
            self._running = False

        def __str__(self):
            return self._thread_name

        def __repr__(self):
            return "<%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self._thread_name)

        def __hash__(self):
            return hash(self._thread_name)

        @property
        def should_run(self):
            if not self._thread_shared_state.should_run:
                return False
            return self._should_run

        @should_run.setter
        def should_run(self, val):
            with self._run_cond:
                self._should_run = val
                self._run_cond.notify_all()

        @property
        def is_done(self):
            return self._is_done

        @staticmethod
        def time_wait_NL(db): # pylint: disable=unused-argument
            '''
            Return the maximum number of seconds to spend waiting for work (relative)
            '''
            return 60.0

        def any_work_to_do_NL(self, db, setup_work=False):
            '''
            Return whether or not there is anything for the thread to do.
            Caller holds the Lock associted with run_cond.
            '''
            raise NotImplementedError("%s(%s) did not implement this method" % (self.__class__.__name__, repr(self)))

        def _do_work(self, db):
            '''
            Perform work designated for this thread.
            '''
            raise NotImplementedError("%s(%s) did not implement this method" % (self.__class__.__name__, repr(self)))

        def _done_work_NL(self, db):
            '''
            self._do_work() has completed. This is invoked after the
            lock associated with _run_cond is re-taken.
            '''
            # noop here in base class

        def start(self, db):
            'Launch the thread'
            with self._run_cond:
                if not self._started:
                    self._thread = threading.Thread(target=self._run, name=self._thread_name, args=(db,))
                    self._thread.start()
                    self._started = True

        def wait_for_running(self, db):
            'wait for the thread to run'
            with self._run_cond:
                while not self._running:
                    db.check_usable()
                    self._run_cond.wait(timeout=30.0)
                    db.check_usable()

        def stop(self):
            'tell the thread to stop running'
            with self._run_cond:
                self._should_run = False
                self._run_cond.notify_all()

        def wait_for_stopped(self, db):
            'wait for it to complete'
            with self._run_cond:
                self._should_run = False
                self._run_cond.notify_all()
                while self._started and (not self._is_done):
                    db.check_usable()
                    self._run_cond.wait(timeout=30.0)
                    db.check_usable()

        def _run(self, db):
            '''
            Wrapper around the main loop for one background thread.
            Catches exceptions and marks the DB failed so other threads
            do not get stuck waiting for this thread. The caller does not
            hold the lock.
            '''
            try:
                with self._run_cond:
                    self._running = True
                    self._run_cond.notify_all()
                self._run__internal(db)
            except (AbortException, DbInconsistencyError, DbTerminalError, SystemExit) as e:
                db.terminal_error = exc_info_err()
                raise SystemExit(1) from e
            except:
                err = "thread %s failed" % self
                db.terminal_error = exc_log(db.logger, logging.ERROR, err)
            finally:
                with self._run_cond:
                    self._is_done = True
                    self._run_cond.notify_all()

        def _run__internal(self, db):
            '''
            Main loop for one background thread
            db: backpointer to ClfsLoadDB. Not embedded in self
            to avoid managing the circular reference.
            '''
            with Monitor(self._run_lock) as monitor:
                while True:
                    if not self.should_run:
                        break
                    do_work = self.any_work_to_do_NL(db, setup_work=True)
                    if do_work:
                        monitor.release()
                        try:
                            self._do_work(db)
                        finally:
                            monitor.acquire()
                        self._done_work_NL(db)
                        self._slept = False
                    else:
                        db.check_usable()
                        self._slept = True
                        self._run_cond.wait(timeout=self.time_wait_NL(db))

    # How much to sdvance FHSEQ each time we fetch it
    fhseq_increment = 10000

    # Prefetch a new range if we do not have at least this many
    # values remaining.
    fhseq_low_water = 1000

    class BackgroundThreadStateFhseq(_BackgroundThreadState):
        'State specific to the fhseq refresh thread'

        def any_work_to_do_NL(self, db, setup_work=False):
            'See base class'
            if not (db.fhseq_prev and db.fhseq_dbval):
                return True
            if (db.fhseq_dbval - db.fhseq_prev) < db.fhseq_low_water:
                return True
            return False

        def _do_work(self, db):
            'See base class'
            key = DbEntMetaKey.FHSEQ
            key_name = str(key)
            session = db.dbmeta.session_get()
            try:
                query = db.query_DbEntMeta.with_session(session)
                ent = query.get(key_name)
                if ent is None:
                    cur = Filehandle.fhseq_first_get()
                    ent = DbEntMeta(m_key=key_name, m_value=cur)
                    session.add(ent)
                else:
                    try:
                        cur = int(ent.m_value)
                    except ValueError as e:
                        db.is_consistent = False
                        raise DbInconsistencyError("cannot interpret %s='%s' as integer" % (key_name, ent.m_value)) from e
                new = cur + db.fhseq_increment
                if new <= cur:
                    db.raise_terminal("db_fhseq_get cur=%s fhseq_increment=%s ent=%s value did not advance" % (cur, db.fhseq_increment, ent))
                ent.m_value = new
                session.commit()
                with db.fhseq_get_cond:
                    db.fhseq_prev = cur
                    db.fhseq_dbval = new
            finally:
                db.dbmeta.session_release(session)

        def _done_work_NL(self, db):
            'See base class'
            db.fhseq_get_cond.notify_all()

    def db_claimable_get(self):
        'Return one claimable entry'
        session = self.dbtobj.session_get()
        try:
            query = self.query_DbEntTargetObj.with_session(session)
            states = (self.toc_state_claim_from,) + self.toc_state_claim_to
            for state in states:
                query2 = query.filter(DbEntTargetObj.state == state).limit(1)
                for dbe in query2:
                    return TargetObj.from_dbe(dbe)
            return None
        finally:
            self.dbtobj.session_release(session)

    @staticmethod
    def db_load_additional_backpointers_for_tobj(bpquery, tobj):
        'Load the additional_backpointers for tobj'
        query_result = bpquery.filter(DbEntAdditionalBackpointerMapEnt.filehandle_from == tobj.filehandle.bytes)
        # Even if the query returns no results, we populate an empty AdditionalBackpointers here.
        # Doing so allows us to differentiate between "nothing found" and "not loaded"
        # when looking at tobj.ic_backpointer_map.
        tobj.ic_backpointer_map = AdditionalBackpointers(tobj.filehandle, query_result)

    @staticmethod
    def db_additional_backpointers_ent_get(bpquery, filehandle_from, filehandle_to):
        'Load the ent corresponding to the given filehandle_from, filehandle_to tuple'
        query = bpquery.filter(DbEntAdditionalBackpointerMapEnt.filehandle_from == filehandle_from.bytes)
        query = query.filter(DbEntAdditionalBackpointerMapEnt.filehandle_to == filehandle_to.bytes)
        query_result = list(query)
        if not query_result:
            return None
        if len(query_result) != 1:
            raise DbInconsistencyError("db_additional_backpointers_ent_get matched %d entries for %s %s"
                                       % (len(query_result), filehandle_from, filehandle_to))
        return query_result[0]

    def dbc_fhseq_get(self):
        'Get a unique fhseq value.'
        with self.fhseq_get_cond:
            self.check_usable()
            while (not self.fhseq_prev) or (not self.fhseq_dbval) or (self.fhseq_prev >= self.fhseq_dbval):
                self.fhseq_run_cond.notify()
                self.check_usable()
                self.fhseq_get_cond.wait(timeout=30.0)
                self.check_usable()
            self.fhseq_prev += 1
            # Trigger prefetch if we hit low-water
            if self.fhseq_thread_state.any_work_to_do_NL(self):
                self.fhseq_run_cond.notify()
            return self.fhseq_prev

    class TocBatch():
        'Describes a logical chunk of changes to the database'

        # Push into the flush queue if flush_dirty_count_min objects are awaiting update
        # If this is too small, flushes are inefficient.
        # If this is too large, then we can suddenly run out of preclaimed
        # work items and need a long time to flush.
        # When in the flush queue, we can allow more upserts, up to
        # flush_dirty_count_nondir_max/flush_dirty_count_max objects in the batch.
        flush_dirty_count_min = 900
        flush_dirty_count_nondir_max = 1400
        flush_dirty_count_max = 1500

        # If the batch has been dirty for at least this many seconds,
        # it wants to flush even if there is no other reason to do so.
        # This is used to ensure that if the worker threads get busy
        # for a long time handling large objects, we do not sit around
        # without processing transitions to DONE. In the presence of
        # objects that take a long time, we could end up repeating a
        # lot of work unnecessarily in the event of a crash if we do
        # not do this.
        flush_dirty_seconds = 60.0

        def __init__(self, idx):
            self.toc_idx = idx
            self._upsert_tobj = dict() # key=Filehandle value=TargetObj
            self._upsert_existing_dict = dict() # key=ExistingTargetObjKey value=list(TargetObj)
            self._upsert_existing_valid = True # whether or not self._upsert_existing_dict is accurate
            self._update_tobj = dict() # key=Filehandle value=_UpdateStateDesc
            self._update_state = dict() # key=Filehandle value=_UpdateStateDesc
            self._add_backpointer = dict() # key=Filehandle(from), value=dict(key=Filehandle(to), value=count)
            self.getexistingkeys = set() # ExistingTargetObjKey
            self.getexistingresults = dict() # key=ExistingTargetObjKey value=[TargetObj,...]
            self.must_flush = False

            # More ideally, we'd set self._dirty_time = None here,
            # and update it to time.time() on the first operation
            # that adds an update. That means checking dirty vs non-dirty
            # during each update operation while holding a very hot
            # lock. The effect is a noticible performance lag. Instead,
            # we use an imperfect time here. The cost of the imperfection
            # is a lesser performance hit than that of being more exact.
            self._dirty_time = time.time()
            self.flush_time = self._dirty_time + self.flush_dirty_seconds

            # flush state
            self._flush_dbobjs = dict() # key=filehandle_bytes val=DbEntTargetObj
            self._flush_reap_dirs = dict() # key=filehandle_bytes, value=DbEntTargetObj
            self._flush_reap_check = dict() # key=filehandle_bytes, value=DbEntTargetObj
            self._flush_dir_writing = list() # Ftype.DIR that changed state to WRITING
            self._dir_fh_done = list() # List of Filehandle for Ftype.DIR that became TargetObjState.DONE

        def __repr__(self):
            return "%s(%s)" % (self.__class__.__name__, self.toc_idx)

        def __len__(self):
            return len(self._upsert_tobj) + len(self._update_tobj) + len(self._update_state) + len(self._add_backpointer)

        def __bool__(self):
            return self.must_flush \
                or bool(self._upsert_tobj) \
                or bool(self._update_tobj) \
                or bool(self._update_state) \
                or bool(self._add_backpointer) \
                or bool(self.getexistingkeys)

        def upsert_count_NL(self):
            'Return the number of pending upserts'
            return len(self._upsert_tobj)

        def flush_for_time(self):
            '''
            Return whether this batch should flush based on
            how long it has been dirty. See flush_dirty_seconds above.
            '''
            return bool(self) and (time.time() >= self.flush_time)

        def flush_for_time_or_idle(self, db):
            '''
            Return whether this batch should flush based on
            how long it has been dirty. See flush_dirty_seconds above.
            If worker threads are idle, go ahead and flush to get things moving.
            '''
            if db.all_phase_threads_are_idle:
                return True
            return self.flush_for_time()

        def time_wait_NL(self):
            '''
            Return the relative number of seconds between now and
            when this batch should flush for the timer. Relative
            because the only caller is doing a condition wait, and
            those timeouts are sadly relative.
            '''
            return elapsed(time.time(), self.flush_time)

        def upsert_ins_NL(self, tobj):
            '''
            Unconditionally insert or update tobj with this new value.
            Caller is surrendering ownership of tobj to this cache.
            Caller must not read or write tobj after this.
            Except when inserting the root object, tobj.state must be TargetObjState.PENDING.
            We do not explicitly check that here to avoid the cost and the special-case.
            '''
            #assert tobj.filehandle not in self._update_tobj # commented out for performance
            self._upsert_tobj[tobj.filehandle] = tobj
            self._upsert_existing_valid = False

        def get_existing_NL(self, db, entkey):
            '''
            Return the list of matching target objects for entkey
            '''
            if not self._upsert_existing_valid:
                self._upsert_existing_compute_NL(db)
            return self._upsert_existing_dict.get(entkey, list())

        def _upsert_existing_compute_NL(self, db):
            '''
            Compute a valid self._upsert_existing_dict
            '''
            with db.timers.start('upsert_existing_compute'):
                for tobj in self._upsert_tobj.values():
                    lst = self._upsert_existing_dict.setdefault(tobj.existing_key(), list())
                    lst.append(tobj)
                self._upsert_existing_valid = True

        def update_state_ins_NL(self, tobj, usd_new):
            'Cache the intent to update the state of tobj as described by usd_new'
            filehandle = tobj.filehandle
            if usd_new.tobj:
                # assert filehandle not in self._upsert_tobj # disabled for performance
                # assert filehandle not in self._update_tobj # disabled for performanc
                self._update_tobj[filehandle] = usd_new
                return
            usd_prev = self._update_state.get(filehandle, None)
            if usd_prev:
                usd_prev.add(usd_new)
            else:
                filehandle = Filehandle(filehandle)
                self._update_state[filehandle] = usd_new

        def add_backpointer_ins_NL(self, tobj, backpointer):
            '''
            Add a backpointer (from=tobj.filehandle to=backpointer)
            '''
            # self._add_backpointer is self._add_backpointer[from][to] = count
            filehandle_from = Filehandle(tobj.filehandle)
            d_from = self._add_backpointer.setdefault(filehandle_from, dict())
            count = d_from.get(backpointer, 0)
            if (count + tobj.backpointer_count()) >= CLFS_LINK_MAX:
                raise BackpointerLimitReached("%s at %s+%s" % (filehandle_from, count, tobj.backpointer_count()))
            d_from[backpointer] = count + 1

        _FLUSH__GET_DBE__COLUMNS = ('filehandle', 'ftype', 'state', 'nlink', 'first_backpointer')

        def _flush__get_dbe(self, db, query, filehandle_bytes):
            'During flush(), fetch one entry from the DB'
            # Doing this with try/except both fastpaths the hit
            # and handles the case where self._flush_dbobjs[filehandle_bytes] = None
            # because we already reaped it.
            try:
                return self._flush_dbobjs[filehandle_bytes]
            except KeyError:
                pass
            # We could limit the columns returned by saying:
            # query = query.options(load_only(*self._FLUSH__GET_DBE__COLUMNS))
            # (load_only is from sqlalchemy.orm import load_only)
            # Doing so ends up being slower rather than faster, however.
            dbe = query.get(filehandle_bytes)
            if not dbe:
                err = "%s flush filehandle=%s cannot find row" % (self, filehandle_bytes.hex())
                db.logger.error("%s", err)
                raise DbInconsistencyError(err)
            dbe.persisted = True
            self._flush_dbobjs[filehandle_bytes] = dbe
            return dbe

        def _flush__update_state(self, db, query, dbe, usd):
            '''
            Used from within flush() to handle a single update_state tuple.
            Returns True to indicate that dbe is reaped.
            May modify the following attributes of dbe:
                state
                size
                child_dir_count
            If that list changes, be sure to update the bulk update or reap fastpath in _flush()
            '''
            prev_state = dbe.state
            if usd.size is not None:
                dbe.size = usd.size
            if (db.phase == Phase.TRANSFER) and (dbe.ftype != Ftype.DIR) and (dbe.nlink != 1) and (prev_state != TargetObjState.DONE) and (usd.state == TargetObjState.DONE):
                # Set this to INODE_PENDING and not DONE so we fix up nlink
                # in the reconcile phase.
                dbe.state = TargetObjState.INODE_PENDING
            elif (db.phase == Phase.RECONCILE) and (prev_state != TargetObjState.DONE) and (usd.state == TargetObjState.DONE):
                # We must clean up the intermediate inode blob
                dbe.state = TargetObjState.INODE_CLEANUP_PENDING
            else:
                dbe.state = usd.state
            if db.phase == Phase.TRANSFER:
                if dbe.ftype == Ftype.DIR:
                    if (dbe.state != TargetObjState.ERROR) and (dbe.state in db.phase_terminal_states):
                        self._dir_fh_done.append(Filehandle(dbe.filehandle))
                    if usd.child_dir_count is not None:
                        dbe.child_dir_count = usd.child_dir_count
                    if db.may_reap_children(dbe):
                        self._flush_reap_dirs[dbe.filehandle] = dbe
                if (prev_state not in db.phase_terminal_states) and (dbe.state in db.phase_terminal_states):
                    db.dr_current.dr_count += 1
                    db.dr_current.dr_gb += dbe.size / Size.GB
                    if (dbe.state == TargetObjState.INODE_PENDING) and (prev_state != dbe.state):
                        dbe.reconcile_vers = dbe.reconcile_vers + 1
                elif (dbe.ftype == Ftype.DIR) and (prev_state == TargetObjState.GENERATING) and (dbe.state == TargetObjState.WRITING):
                    db.toc_preclaim.preclaim_add_pend_writing_data(TargetObj.from_dbe(dbe))
            if (dbe.state == TargetObjState.ERROR) and (prev_state != TargetObjState.ERROR):
                dbe.pre_error_state = prev_state
            return self._flush__update_state_pre_reap(db, query, dbe)

        def _flush__update_state_pre_reap(self, db, query, dbe):
            '''
            Figure out whether the net result of an update_state
            is that the given dbe is reaped. If so, do it.
            Returns whether the reap was done.
            '''
            if db.may_reap(dbe):
                dbe_parent = self._flush__get_dbe(db, query, dbe.first_backpointer)
                if db.may_reap_children(dbe_parent):
                    self._flush__dbe_delete(db, query, dbe)
                    return True
                self._flush_reap_check[dbe.filehandle] = dbe
            return False

        def _flush__dbe_delete(self, db, query, dbe):
            '''
            delete dbe
            '''
            if dbe.persisted is None:
                raise AssertionError("%s _flush__dbe_delete dbe.persisted for %s is not initialized" % (self, dbe.filehandle.hex()))
            try:
                if dbe.persisted:
                    session = query.session
                    session.delete(dbe)
                else:
                    query2 = query.filter(DbEntTargetObj.filehandle == dbe.filehandle)
                    query2.delete(synchronize_session=False)
            except BaseException as e:
                db.logger.error("%s delete_persisted cannot delete %s: %s\ndbe is:\n%s",
                                self, dbe.filehandle.hex(), exc_info_err(), pprint.pformat(vars(dbe)))
                db.raise_terminal('delete_persisted error', fromex=e)
            self._flush_dbobjs[dbe.filehandle] = None

        def flush(self, db, session):
            'Write all pending updates in a single transaction'
            with db.timers.start('flush'):
                self._flush(db, session)

        def _flush(self, db, session):
            '''
            Write all pending updates in a single transaction.
            Do not call this directly; call flush().
            '''
            db.stats.stat_inc('flushes')
            query = db.query_DbEntTargetObj.with_session(session)
            upsert_dbe_list = list()
            upsert_tobj_list = list()
            with db.timers.start('flush_01_compute_upsert'):
                # Note: upsert only happens during TRANSFER as a result
                # of discovering new directory entries.
                for tobj in self._upsert_tobj.values():
                    usd = self._update_state.pop(tobj.filehandle, None)
                    if usd or db.may_reap_children(tobj) or db.may_reap(tobj):
                        dbe = DbEntTargetObj.from_ic(tobj)
                        if usd:
                            self._flush__update_state(db, query, dbe, usd)
                        upsert_dbe_list.append(dbe)
                        self._flush_dbobjs[dbe.filehandle] = dbe
                        assert not (db.may_reap_children(dbe) or db.may_reap(dbe))
                    else:
                        # Fastpath: jam it in
                        db.toc_preclaim.preclaim_offer(tobj)
                        upsert_tobj_list.append(tobj.to_db_dict())
            # info about these bulk APIs:
            #     https://docs.sqlalchemy.org/en/latest/faq/performance.html
            #     https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.bulk_save_objects
            #     https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.bulk_insert_mappings
            #     https://docs.sqlalchemy.org/en/13/orm/session_api.html#sqlalchemy.orm.session.Session.bulk_update_mappings
            with db.timers.start('flush_02_upsert'):
                # These operations have nonzero overhead for the subtransaction
                # even if the list is empty, so check emptiness first.
                if upsert_dbe_list:
                    db.stats.stat_add('flush_upsert_tobj_slowpath', len(upsert_dbe_list))
                    session.bulk_save_objects(upsert_dbe_list)
                if upsert_tobj_list:
                    session.bulk_insert_mappings(DbEntTargetObj, upsert_tobj_list, return_defaults=False, render_nulls=True)
            del upsert_dbe_list
            del upsert_tobj_list
            update_dbe_list = list()
            update_tobj_deferred = dict()
            with db.timers.start('flush_03_compute_update'):
                for filehandle, usd in self._update_tobj.items():
                    tobj = usd.tobj
                    # This could already be in _flush_dbobjs thanks to a reap check
                    if (tobj.filehandle in self._flush_dbobjs) or (tobj.filehandle in self._add_backpointer):
                        update_tobj_deferred[filehandle] = usd
                    else:
                        # Fastpath: bulk update or reap
                        dbe = DbEntTargetObj.from_ic(tobj)
                        if not self._flush__update_state(db, query, dbe, usd):
                            self._flush_dbobjs[dbe.filehandle] = dbe
                            update_dbe_list.append(dbe.to_db_dict())
            with db.timers.start('flush_04_update'):
                # These operations have nonzero overhead for the subtransaction
                # even if the list is empty, so check emptiness first.
                if update_dbe_list:
                    session.bulk_update_mappings(DbEntTargetObj, update_dbe_list)
            del update_dbe_list
            # Must add additional_backpointers before updating state so we
            # can decide about INODE_PENDING. Note that when we did the bulk
            # update above, we deferred anything in self._add_backpointer.
            if self._add_backpointer:
                with db.timers.start('flush_05_add_backpointers'):
                    bpquery = db.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
                    for filehandle_from, to_dict in self._add_backpointer.items():
                        dbe_changed = False
                        dbe_tobj = self._flush__get_dbe(db, query, filehandle_from.bytes)
                        # _flush__get_dbe raises if dbe_tobj is not found
                        for filehandle_to, add_count in to_dict.items():
                            assert add_count > 0
                            if dbe_tobj.first_backpointer == FILEHANDLE_NULL_BYTES:
                                # No first_backpointer on this target yet, so set it there.
                                dbe_tobj.first_backpointer = filehandle_to.bytes
                                add_count -= 1
                                dbe_changed = True
                            if add_count > 0:
                                # Add as an additional backpointer
                                dbe_bp = db.db_additional_backpointers_ent_get(bpquery, filehandle_from, filehandle_to)
                                if dbe_bp:
                                    # Avoid += with SQLAlchemy ORM. Since we know we are
                                    # serialized here, we can read, add, and update safely.
                                    cur = dbe_bp.count
                                    dbe_bp.count = cur + add_count
                                else:
                                    dbe_bp = DbEntAdditionalBackpointerMapEnt(filehandle_from=filehandle_from.bytes,
                                                                              filehandle_to=filehandle_to.bytes,
                                                                              count=add_count)
                                    session.add(dbe_bp)
                        bpquery2 = bpquery.filter(DbEntAdditionalBackpointerMapEnt.filehandle_from == filehandle_from.bytes)
                        new_nlink = sum([x.count for x in bpquery2])
                        if dbe_tobj.first_backpointer != FILEHANDLE_NULL_BYTES:
                            new_nlink += 1
                        # We may not have found all of the links yet, so only decrease
                        # it if we are in the reconcile phase. (At this time, we do not
                        # expect this code to execute in the reconcile phase; this is
                        # for futureproofing/clarity.)
                        if (new_nlink > dbe_tobj.nlink) or ((db.phase == Phase.RECONCILE) and (new_nlink != dbe_tobj.nlink)):
                            dbe_tobj.nlink = new_nlink
                            dbe_changed = True
                        if dbe_changed:
                            db.toc_preclaim.preclaim_update_for_add_backpointer(dbe_tobj)
            with db.timers.start('flush_06_flush'):
                session.flush()
            with db.timers.start('flush_07_update_state'):
                for filehandle, usd in self._update_state.items():
                    dbe = self._flush__get_dbe(db, query, filehandle.bytes)
                    self._flush__update_state(db, query, dbe, usd)
            if update_tobj_deferred:
                with db.timers.start('flush_08_update_state_deferred'):
                    db.stats.stat_add('flush_update_tobj_deferred', len(update_tobj_deferred))
                    for filehandle, usd in update_tobj_deferred.items():
                        dbe = self._flush__get_dbe(db, query, filehandle.bytes)
                        self._flush__update_state(db, query, dbe, usd)
            # Update progress
            if db.dr_current:
                if db.phase == Phase.TRANSFER:
                    if db.dr_current.dr_count != db.progress_wrote_last_count:
                        md = DbEntMeta(m_key=DbEntMetaKey.PROGRESS_COUNT, m_value=json.dumps(db.dr_current.dr_count))
                        session.merge(md)
                        db.progress_wrote_last_count = db.dr_current.dr_count
                    if db.dr_current.dr_gb != db.progress_wrote_last_gb:
                        md = DbEntMeta(m_key=DbEntMetaKey.PROGRESS_GB, m_value=json.dumps(db.dr_current.dr_gb))
                        session.merge(md)
                        db.progress_wrote_last_gb = db.dr_current.dr_gb
            with db.timers.start('flush_09_flush'):
                session.flush()
            with db.timers.start('flush_10_reap'):
                self._flush__reap(db, query)

        def _flush__reap(self, db, query):
            '''
            Called from flush(). flush() has computed updates; now determine what may be reaped.
            Note that this does session.delete() directly and does not update _flush_dbobjs.
            '''
            # There are two loops here. The loop over flush_reap_dirs is iterating
            # over parent directories that have changed state and may have reapable
            # children. The loop over flush_reap_check is a loop over reapable
            # children to see if they may be reaped based on parent state.
            # It is possible for the same child to be deleted in both loops
            # when the batch contains the relevant state transitions for both
            # parent and child.
            # Here we just allow that to happen.
            for dbe_parent in self._flush_reap_dirs.values():
                # We only need to find entries where first_backpointer points to dbe.
                # If the entry has more than one backpointer, it is not eligible to be reaped.
                fquery = query.filter(DbEntTargetObj.first_backpointer == dbe_parent.filehandle)
                fquery = fquery.filter(DbEntTargetObj.state == TargetObjState.DONE)
                fquery = fquery.filter(DbEntTargetObj.nlink == 1)
                for dbe_child in fquery:
                    if self._flush_dbobjs.get(dbe_child.filehandle, True) is None:
                        # already deleted
                        continue
                    dbe_child.persisted = True
                    if db.may_reap(dbe_child):
                        self._flush__dbe_delete(db, query, dbe_child)
                        # No need to pop from self._flush_reap_check
                        # because directories are never added there in
                        # the first place.
            # Directories are never reaped, so it is okay to search for the directory
            # after the pass above.
            for dbe_child in self._flush_reap_check.values():
                dbe_parent = self._flush__get_dbe(db, query, dbe_child.first_backpointer)
                if db.may_reap_children(dbe_parent):
                    self._flush__dbe_delete(db, query, dbe_child)

        def postflush(self, db, session):
            '''
            Flush modifications are done. Do ops that depend on the result.
            This is invoked after flush() within the same transaction before the commit.
            '''
            if self._dir_fh_done:
                db.file_cleaner.handoff(self._dir_fh_done)
                self._dir_fh_done = None
            query = db.query_DbEntTargetObj.with_session(session)
            if self.getexistingkeys:
                bpquery = db.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
                for existingkey in self.getexistingkeys:
                    fquery = query.filter(DbEntTargetObj.source_inode_number == existingkey.inode_number)
                    fquery = fquery.filter(DbEntTargetObj.ftype == existingkey.ftype)
                    fquery = fquery.filter(DbEntTargetObj.nlink > 1)
                    tobjs = [TargetObj.from_dbe(dbe) for dbe in fquery]
                    for tobj in tobjs:
                        # The only caller on this path is directory generation attempting
                        # to match an existing inode for a non-directory target.
                        # That caller needs the accurate current nlink, so we must
                        # fetch the additional_backpointers here.
                        db.db_load_additional_backpointers_for_tobj(bpquery, tobj)
                    self.getexistingresults[existingkey] = tobjs
            # Drop any pending barriers. We must do this even if preserve_hardlinks
            # is not set because when we do crash recovery we may be checking for
            # repeat work.
            for tobj in self._upsert_tobj.values():
                tobj.drop_barrier()
            with db.timers.start('preclaim_more'):
                db.toc_preclaim.preclaim_more(db, session, query)

    def may_reap_children(self, tobj):
        '''
        Return whether it is okay to reap children of this item from the target_obj table.
        This is written in such a way that tobj may be a TargetObj or DbEntTargetObj.
        '''
        return (self.phase == Phase.TRANSFER) and (tobj.ftype == Ftype.DIR) and (tobj.state == TargetObjState.DONE)

    @staticmethod
    def may_reap(tobj):
        '''
        Return whether it is okay to reap this item from the target_obj table.
        This is written in such a way that tobj may be a TargetObj or DbEntTargetObj.
        '''
        return (tobj.state == TargetObjState.DONE) \
          and (tobj.ftype != Ftype.DIR) \
          and (tobj.nlink == 1) \
          and (tobj.first_backpointer != FILEHANDLE_NULL_BYTES)

    def move_current_toc_to_toc_flush_NL(self):
        '''
        Move the current toc batch to the flushing list
        and start another one.
        '''
        toc = self.toc_buffering
        self.toc_flush_list.append(toc)
        count = toc.upsert_count_NL()
        if count < toc.flush_dirty_count_nondir_max:
            self.toc_flush_morenondir_list.append(toc)
        if count < toc.flush_dirty_count_max:
            self.toc_flush_moredir_list.append(toc)
        self.toc_idx_prev += 1
        self.toc_buffering = self.TocBatch(self.toc_idx_prev)

    def toc_flush_list_pop_to_toc_flushing_NL(self):
        '''
        Pop the next item from toc_flush_list.
        Return False iff toc_flush_list is empty.
        '''
        try:
            toc = self.toc_flush_list.popleft()
        except IndexError:
            return False
        try:
            if self.toc_flush_morenondir_list[0] is toc:
                self.toc_flush_morenondir_list.popleft()
        except IndexError:
            pass
        try:
            if self.toc_flush_moredir_list[0] is toc:
                self.toc_flush_moredir_list.popleft()
        except IndexError:
            pass
        self.toc_flushing = toc
        return True

    class BackgroundThreadStateToc(_BackgroundThreadState):
        'State specific to the thread that does toc flushes'

        # Start flushing if this much time has elapsed since the last flush started
        _flush_interval_seconds = 300.0

        @staticmethod
        def time_wait_NL(db):
            '''
            Return the maximum number of seconds to spend waiting for work (relative)
            '''
            return db.toc_buffering.time_wait_NL()

        def _run(self, db):
            'wrapper around base class that tries to ensure flush waiters do not get stuck'
            try:
                super(ClfsLoadDB.BackgroundThreadStateToc, self)._run(db)
            finally:
                notify_all(db.toc_flush_cond)

        def any_work_to_do_NL(self, db, setup_work=False):
            'See base class'
            # A subtlety: we leverage the calls to this operation to detect
            # that the current batch needs to flush, and if so we move it to
            # toc_flush_list. Before we compute the return value, see if the
            # current batch is full enough to warrant flushing. If so, move
            # it to the flush list and start filling the next batch. This
            # prevents batches from becoming oversized and generating stalls
            # while they flush.
            # As a minor performance optimization, we do not check
            # getexistingkeys here. Instead, we rely on the fact that if
            # those are non-empty, must_flush is set.
            buffering_len = len(db.toc_buffering)
            if (buffering_len >= db.toc_buffering.flush_dirty_count_min) \
              or (db.toc_buffering.must_flush and (buffering_len or (db.toc_flushing is None))) \
              or ((not db.toc_flush_list) and buffering_len and db.toc_buffering.flush_for_time_or_idle(db)):
                db.move_current_toc_to_toc_flush_NL()
            else:
                buffering_upsert_count = db.toc_buffering.upsert_count_NL()
                if buffering_upsert_count:
                    if db.toc_flush_next_upsert:
                        db.move_current_toc_to_toc_flush_NL()
                        db.toc_flush_next_upsert = False
                    elif db.toc_flushing is None:
                        if (not db.toc_flush_list) and db.toc_preclaim.want_more():
                            db.move_current_toc_to_toc_flush_NL()
                        elif db.toc_buffering.flush_for_time():
                            db.move_current_toc_to_toc_flush_NL()
            # Now return whether we should start another flush
            ret = False
            if db.toc_flush_list and (db.toc_flushing is None):
                if setup_work:
                    ret = db.toc_flush_list_pop_to_toc_flushing_NL()
                else:
                    ret = True
            return ret

        def _do_work(self, db):
            'See base class'
            flush_count = 0
            session = None
            try:
                session = db.dbtobj.session_get()
                while True:
                    flush_count += 1
                    try:
                        what = 'flush'
                        db.check_usable()
                        db.toc_flushing.flush(db, session)
                        what = 'postflush'
                        db.toc_flushing.postflush(db, session)
                    except (KeyboardInterrupt, SystemExit):
                        db.should_run = False
                        raise
                    except:
                        err = "%s %s cannot %s: %s" % (self, db, what, exc_info_err())
                        db.logger.error("%s\n%s\n%s", err, exc_stack(), err)
                        db.terminal_error = "%s %s cannot %s" % (self, db, what)
                        break
                    if (flush_count >= 5) \
                      or (not db.all_phase_threads_are_idle) \
                      or db.toc_preclaim \
                      or db.toc_flushing.must_flush:
                        break
                    # If we got here, then flushing this toc did not enable
                    # anything and all worker threads are idle. Flush
                    # another toc under the same transaction (session)
                    # to reduce session overhead.
                    with db.toc_lock:
                        did_pop = db.toc_flush_list_pop_to_toc_flushing_NL()
                    if not did_pop:
                        break
                try:
                    what = 'commit'
                    db.check_usable()
                    session.commit()
                except (KeyboardInterrupt, SystemExit):
                    db.should_run = False
                    raise
                except:
                    err = "%s commit %s: %s" % (self, db, exc_info_err())
                    exc_log(db.logger, logging.ERROR, err)
                    db.terminal_error = "%s %s cannot %s" % (self, db, what)
            finally:
                db.dbtobj.session_release(session)

        def _done_work_NL(self, db):
            'See base class'
            if not db.terminal_error:
                db.toc_preclaim.pend_committed()
            db.toc_idx_flushed_last = db.toc_flushing.toc_idx
            db.toc_flushing = None
            if db.dbc_any_work_queued_NL() or (not db.toc_idx_flushed_and_work_remaining_last):
                db.toc_idx_flushed_and_work_remaining_last = db.toc_idx_flushed_last
            db.toc_flush_cond.notify_all()
            count = db.toc_preclaim.ready_count()
            if count:
                db.phase_work_poke(count)
            elif not db.dbc_any_work_queued_NL():
                # We may be done - wake everyone up to check.
                db.phase_work_poke(0)
            else:
                db.check_phase_idle_NL()

    def check_phase_idle_NL(self):
        '''
        If worker (phase) threads are idle, and the only work pending
        is in the current unflushed batch, start flushing it.
        '''
        if self._any_phase_threads_are_idle and (not self.toc_flush_list):
            if self.toc_buffering:
                if self.toc_thread_state.any_work_to_do_NL(self):
                    self.toc_run_cond.notify_all()
            elif not self.toc_flushing:
                # We may be done - wake everyone up to check.
                self.phase_work_poke(0)

    def dbc_check_for_more_work(self):
        '''
        Determine if there is more work to do.
        '''
        return self._dbc_do_simple(self.dbc_check_for_more_work_NL)

    def dbc_any_work_queued_NL(self, include_buffering=True):
        '''
        Return whether any known work is queued. This does not say whether
        or not there is known work in the DB that is not yet read into
        toc_preclaim.
        '''
        if self.toc_preclaim or (self.toc_flushing is not None) or self.toc_flush_list:
            return True
        if include_buffering and self.toc_buffering:
            return True
        return False

    def dbc_check_for_more_work_NL(self, *args): # pylint: disable=unused-argument
        '''
        Determine if there is more work to do for this phase.
        A false return tells the pool of worker threads that
        there is nothing more to do for this phase.
        '''
        if self.dbc_any_work_queued_NL(include_buffering=False):
            return True
        if self.toc_buffering or (self.toc_idx_flushed_and_work_remaining_last == self.toc_idx_flushed_last) or (not self.toc_idx_flushed_and_work_remaining_last):
            # The last time a flush finished, there was more to do. Force
            # another flush to trigger preclaim. If that finishes without
            # preclaiming anything, then self.toc_idx_flushed_last advances
            # past self.toc_idx_flushed_and_work_remaining_last, and a subsequent
            # call to this operation returns false.
            # If self.toc_idx_flushed_and_work_remaining_last is zero, then we have not
            # yet flushed at all since starting this phase, so kick a flush.
            self.move_current_toc_to_toc_flush_NL()
            self.toc_run_cond.notify_all()
            return True
        assert self.toc_idx_flushed_and_work_remaining_last < self.toc_idx_flushed_last
        return False

    def dbc_claim(self, wts):
        '''
        Claim and return one item
        '''
        return self._dbc_do_simple(self._dbc_claim_NL, wts=wts)

    def _dbc_claim_NL(self, sa):
        '''
        Claim and return one item
        Caller holds lock
        '''
        ret = self.toc_preclaim.get_one()
        if (not ret) and sa.wts:
            sa.wts.stats.stat_inc('dbc_claim_miss')
            self.check_phase_idle_NL()
        return ret

    def dbc_flush(self):
        '''
        Flush the current batch (and all preceding batches)
        '''
        self._dbc_do_simple(None, wait_for_flush=True)

    def dbc_get_existing(self, wts, existing):
        'Return a list of entries matching existing (ExistingTargetObjKey)'
        getexisting = {existing : None}
        self._dbc_do_simple(None, getexisting=getexisting, wts=wts)
        self.check_usable()
        return getexisting[existing]

    def dbc_existing_key_barrier_begin(self, existing):
        '''
        The caller is about to perform an operation that must know whether or
        not ExistingTargetObjKey existing is matched. If another such
        operation is in-flight, block until it completes. Return a new
        ExistingTargetObjKeyBarrier object.
        '''
        with self.upsert_existing_pending_cond:
            if existing in self.upsert_existing_pending_set:
                self.stats.stat_inc('get_existing_stall')
                with self.timers.start('get_existing_stall'):
                    while existing in self.upsert_existing_pending_set:
                        if (not self.toc_flushing) and (not self.toc_flush_list):
                            self.toc_buffering.must_flush = True
                            self.toc_run_cond.notify_all()
                        self.upsert_existing_pending_cond.wait(timeout=5.0)
                        self.check_usable()
            self.upsert_existing_pending_set.add(existing)
            return ExistingTargetObjKeyBarrier(existing, self)

    def dbc_existing_key_barrier_end(self, existing):
        '''
        This is used by ExistingTargetObjKeyBarrier to drop
        the barrier. Do not call this directly; always call
        via the drop method of ExistingTargetObjKeyBarrier
        or by destroying ExistingTargetObjKeyBarrier.
        '''
        with self.upsert_existing_pending_cond:
            try:
                self.upsert_existing_pending_set.remove(existing)
                self.upsert_existing_pending_cond.notify_all()
            except KeyError:
                self.raise_terminal("dbc_exiting_key_barrier_end with no corresponding _begin for %s" % str(existing))

    def dbc_upsert_single(self, wts, tobj):
        '''
        Insert or overwrite a targetobj
        '''
        # do not yield here - we yield in other dbc ops to keep
        # this one flowing
        self._dbc_do_simple(self._dbc_upsert_single_NL, tobj, wts=wts)

    def dbc_upsert_multi(self, wts, tobjs):
        '''
        Insert or overwrite multiple targetobjs
        '''
        # do not yield here - we yield in other dbc ops to keep
        # this one flowing
        if tobjs:
            self._dbc_do_simple(self._dbc_upsert_multi_NL, tobjs, wts=wts)

    def _dbc_upsert_multi_NL(self, sa, tobjs):
        for tobj in tobjs:
            self._dbc_upsert_single_NL(sa, tobj)

    def _dbc_upsert_single_NL(self, sa, tobj):
        '''
        Insert or overwrite a targetobj; called from within _dbc_do_simple()
        '''
        if self.toc_preclaim.want_more() and self.toc_flush_list:
            # Preclaim wants more. Find the earliest batch
            # in the flush queue where we can add this upsert.
            wts = sa.wts
            if wts:
                wts.stats.stat_inc('dbc_upsert_toc_check')
            if tobj.ftype == Ftype.DIR:
                deque = self.toc_flush_moredir_list
                maxlen = self.TocBatch.flush_dirty_count_max
            else:
                deque = self.toc_flush_morenondir_list
                maxlen = self.TocBatch.flush_dirty_count_nondir_max
            if deque:
                if wts:
                    wts.stats.stat_inc('dbc_upsert_toc_found')
                toc = deque[0]
                toc.upsert_ins_NL(tobj)
                if len(toc) >= maxlen:
                    deque.popleft()
                return
        sa.toc.upsert_ins_NL(tobj)

    def dbc_update_state(self, wts, tobj, new_state, child_dir_count=None, new_size=None, wait_for_flush=False, surrender_tobj=False):
        '''
        Update state of object to new_state.
        In some cases, we might use this to handle a change to size without changing the state.
        If new_state is terminal, and surrender_tobj
        is set, then we may assume ownership of the tobj and further assume that the
        only changes to tobj that may come after this are via dbc_add_backpointer().
        '''
        if new_state not in self.phase_terminal_states:
            surrender_tobj = False
        assert (child_dir_count is None) or ((tobj.ftype == Ftype.DIR) and (new_state in (TargetObjState.WRITING, tobj.state)))
        assert (new_size is None) or (tobj.ftype != Ftype.DIR)
        usd = _UpdateStateDesc(new_state, child_dir_count, new_size)
        if surrender_tobj:
            usd.tobj = tobj
        self._dbc_do_simple(self._dbc_update_state_NL, tobj, usd, wait_for_flush=wait_for_flush, wts=wts)

    @staticmethod
    def _dbc_update_state_NL(sa, tobj, usd):
        'Update state of object to new_state'
        sa.toc.update_state_ins_NL(tobj, usd)

    def dbc_add_backpointer(self, wts, tobj, backpointer):
        'Add a single backpointer to a target obj'
        self._dbc_do_simple(self._dbc_add_backpointer_NL, tobj, backpointer, wts=wts, wait_for_flush=True)

    @staticmethod
    def _dbc_add_backpointer_NL(sa, tobj, backpointer):
        'Add a single backpointer to a target obj'
        # Important: do not cache tobj here. The add_backpointer case
        # happens when a caller discovers a matching entry via dbc_get_matching_link_targets().
        # Only the filehandle and backpointer matter.
        sa.toc.add_backpointer_ins_NL(tobj, backpointer)

    def _dbc_do_simple(self, proc, *args, wait_for_flush=False, getexisting=None, wts=None):
        '''
        Return proc(SimpleArgs, *args), executing that under the lock
        associated with toc_flush_cond.
        If getexisting is set, it is a dict where keys are ExistingTargetObjKey
        and values are lists of matching TargetObj. Force a flush regardless
        of the value of wait_for_flush, and in the same transaction
        retrieve these matching TargetObjs.
        '''
        ret = None
        getexisting = getexisting or dict()
        self.toc_flush_cond.acquire()
        try:
            self.check_usable()
            toc = self.toc_buffering
            wait_for_flush_this_time = toc.toc_idx if wait_for_flush or getexisting else 0
            if getexisting:
                for k in getexisting.keys():
                    toc.getexistingkeys.add(k)
            if proc:
                class SimpleArgs():
                    def __init__(self, toc, wts):
                        self.toc = toc
                        self.wts = wts
                sa = SimpleArgs(toc, wts)
                ret = proc(sa, *args)
            else:
                ret = None
            if wait_for_flush_this_time:
                toc.must_flush = True
                self.toc_run_cond.notify_all()
                while self.toc_idx_flushed_last < wait_for_flush_this_time:
                    self.check_usable()
                    self.toc_flush_cond.wait(timeout=30.0)
                    self.check_usable()
            for k in getexisting.keys():
                tmp = toc.getexistingresults.get(k, None)
                for toc in self.toc_flush_list:
                    tmp.extend(toc.get_existing_NL(self, k))
                tmp.extend(self.toc_buffering.get_existing_NL(self, k))
                getexisting[k] = tmp
            # If this activity generated work, be sure to kick the thread
            if self.toc_thread_state.any_work_to_do_NL(self):
                self.toc_run_cond.notify_all()
            return ret
        except DbInconsistencyError as e:
            self.is_consistent = False
            err = "%s is inconsistent" % str(self)
            self.raise_terminal(err, fromex=e)
        finally:
            self.toc_flush_cond.release()

    def db_tobj_get(self, filehandle):
        '''
        Return the entry for filehandle, or None if it does not exist.
        Additional backpointers are always included.
        Do not use while dbc is active.
        '''
        session = self.dbtobj.session_get()
        try:
            query = self.query_DbEntTargetObj.with_session(session)
            dbe = query.get(filehandle.bytes)
            if dbe is None:
                return None
            tobj = TargetObj.from_dbe(dbe)
            bpquery = self.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
            self.db_load_additional_backpointers_for_tobj(bpquery, tobj)
            return tobj
        finally:
            self.dbtobj.session_release(session)

    def db_tobj_set_state(self, filehandle, new_state):
        '''
        Update the state of one target object.
        Do not use while dbc is active.
        Returns the new tobj or None if not found.
        '''
        session = self.dbtobj.session_get()
        try:
            query = self.query_DbEntTargetObj.with_session(session)
            dbe = query.get(filehandle.bytes)
            if dbe:
                bpquery = self.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
                dbe.state = new_state
                tobj = TargetObj.from_dbe(dbe)
                self.db_load_additional_backpointers_for_tobj(bpquery, tobj)
                session.commit()
                return tobj
        finally:
            self.dbtobj.session_release(session)
        return None

    DB_BACKPOINTER_CHECK_STATES = (TargetObjState.INODE_CLEANUP_PENDING,
                                   TargetObjState.INODE_CLEANUP_CLEANING,
                                   TargetObjState.ERROR,
                                   TargetObjState.DONE,
                                  )

    def db_backpointer_check(self, rootfh):
        '''
        This is invoked after the reconcile phase. It finds non-root
        entries with no backpointers and updates them to ERROR.
        '''
        logger = self.logger
        session = self.dbtobj.session_get()
        try:
            query = self.query_DbEntTargetObj.with_session(session)
            query = query.filter(DbEntTargetObj.first_backpointer == FILEHANDLE_NULL_BYTES)
            for dbe in query:
                if (dbe.filehandle == rootfh.bytes) or (dbe.state == TargetObjState.ERROR):
                    continue
                if dbe.state not in self.DB_BACKPOINTER_CHECK_STATES:
                    logger.error("post_reconcile db_backpointer_check found %s (%s) with unexpected state %s",
                                 dbe.filehandle.hex(), dbe.source_path, dbe.state)
                    raise SystemExit(1)
                # Warn but do not treat as an error. This can happen
                # if the source is modified during the transfer (including
                # while the transfer is stopped and restarted).
                logger.warning("post_reconcile db_backpointer_check found %s (%s) orphaned",
                               dbe.filehandle.hex(), dbe.source_path)
                dbe.state = TargetObjState.ERROR
                session.commit()
        finally:
            self.dbtobj.session_release(session)

    def db_tobj_list_in_state(self, state, ic_restored_in_progress=False):
        '''
        Return a list of TargetObj in the DB table with the given state.
        If desired, limit may be set to a maximum number of objects to return.
        This is a direct DB query; it does not make use of any cached lists.
        '''
        session = self.dbtobj.session_get()
        try:
            query = self.query_DbEntTargetObj.with_session(session)
            query = query.filter(DbEntTargetObj.state == state)
            ret = [TargetObj.from_dbe(dbe, ic_restored_in_progress=ic_restored_in_progress) for dbe in query]
            if state == TargetObjState.INODE_WRITING:
                bpquery = self.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
                for tobj in ret:
                    self.db_load_additional_backpointers_for_tobj(bpquery, tobj)
            return ret
        finally:
            self.dbtobj.session_release(session)

    def db_tobj_iterate_in_state(self, state, op, op_args=None, op_kwargs=None):
        '''
        For each TargetObj with the given state, invoke op(tobj, *op_args, **op_kwargs).
        op_args and op_kwargs may be None meaning empty.
        If op raises an exception, it is logged and re-raised and iteration aborts.
        If op returns True, the database is updated with the current contents of tobj.
        '''
        op_args = op_args if op_args else tuple()
        op_kwargs = op_kwargs if op_kwargs else dict()
        update_dbe_list = list()
        session = None
        try:
            session = self.dbtobj.session_get()
            query = self.query_DbEntTargetObj.with_session(session)
            query = query.filter(DbEntTargetObj.state == state)
            for dbe in query:
                tobj = TargetObj.from_dbe(dbe)
                if op(tobj, *op_args, **op_kwargs):
                    update_dbe_list.append(tobj.to_db_dict())
            if update_dbe_list:
                session.bulk_update_mappings(DbEntTargetObj, update_dbe_list)
                session.commit()
        except:
            exc_log(self._logger, logging.WARNING, 'db_tobj_iterate_in_state')
            raise
        finally:
            self.dbtobj.session_release(session)

    @staticmethod
    def db_force_reconcile(dbe, force_reconcile_fhs):
        '''
        This is invoked from db_clear_backpointers() to indicate
        that an object has lost a backpointer. Here we update
        the object state to ensure that it is reconciled.
        This can only happen if we are in the TRANSFER phase.
        Caller has a session active and will commit it.
        '''
        if dbe.filehandle not in force_reconcile_fhs:
            dbe.reconcile_vers = dbe.reconcile_vers + 1
            if dbe.nlink == 1:
                # Be sure this is not reaped and is instead reconciled
                dbe.nlink = 2
            if dbe.state == TargetObjState.DONE:
                dbe.state = TargetObjState.INODE_PENDING
            elif dbe.state == TargetObjState.ERROR:
                # Push back pre_error_state so that if we retry, we will reconcile.
                if dbe.pre_error_state > TargetObjState.INODE_WRITING:
                    dbe.pre_error_state = TargetObjState.INODE_PENDING
            force_reconcile_fhs.add(dbe.filehandle)

    def db_clear_backpointers(self, session, filehandle, force_reconcile_fhs):
        '''
        Remove all backpointers to the given filehandle. This includes
        additional_backpointers as well as first_backpointer.
        '''
        # Delete all DbEntAdditionalBackpointerMapEnt that point to filehandle
        query_abp = self.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
        query_fbp = self.query_DbEntTargetObj.with_session(session)
        query_result = list(query_abp.filter(DbEntAdditionalBackpointerMapEnt.filehandle_to == filehandle.bytes))
        for bpe in query_result:
            if bpe.filehandle_from not in force_reconcile_fhs:
                dbe = query_fbp.get(bpe.filehandle_from)
                if dbe:
                    self.db_force_reconcile(dbe, force_reconcile_fhs)
            session.delete(bpe)
        # commit now so the query below does not see the things we deleted
        session.commit()
        # clear first_backpointer for all entries that match
        query = query_fbp.filter(DbEntTargetObj.first_backpointer == filehandle.bytes)
        for dbe in query:
            dbe.first_backpointer = FILEHANDLE_NULL_BYTES
            self.db_force_reconcile(dbe, force_reconcile_fhs)
            # Pull up additional backpointers
            for bpe in query_abp.filter(DbEntAdditionalBackpointerMapEnt.filehandle_from == dbe.filehandle).limit(1):
                dbe.first_backpointer = bpe.filehandle_to
                if bpe.count > 1:
                    bpe.count = bpe.count - 1
                else:
                    session.delete(bpe)
        # commit now so subsequent calls see the pullup results
        session.commit()

    def _dump_meta(self, session):
        'Dump metastore contents from the given session for debugging'
        query = self.query_DbEntMeta.with_session(session).order_by(DbEntMeta.m_key)
        md_dict = {md.m_key : DbEntMetaKey.value_from_dbe(md.m_key, md.m_value) for md in query}
        mt = [(k, md_dict[k]) for k in sorted(md_dict.keys())]
        print(pprint.pformat(mt))

    def dump(self):
        'Dump DB contents for debugging'
        session = self.dbmeta.session_get()
        try:
            self._dump_meta(session)
        finally:
            self.dbmeta.session_release(session)
        session = self.dbtobj.session_get()
        try:
            self._dump_meta(session)
            query = self.query_DbEntTargetObj.with_session(session)
            bpquery = self.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
            for dbe in query.order_by(DbEntTargetObj.filehandle):
                tobj = TargetObj.from_dbe(dbe)
                self.db_load_additional_backpointers_for_tobj(bpquery, tobj)
                print('\n' + tobj.pformat())
            qs = ('PRAGMA cache_size;',
                  'PRAGMA cache_spill;',
                  'PRAGMA journal_mode;',
                  'PRAGMA max_page_count;',
                  'PRAGMA mmap_size;',
                  'PRAGMA page_size;',
                 )
            conn = query._connection_from_session(close_with_result=True) # pylint: disable=protected-access
            for q in qs:
                res = conn.execute(q)
                while True:
                    one = res.fetchone()
                    if not one:
                        break
                    d = {k : one[k] for k in one.keys()}
                    print("%s %s" % (q, pprint.pformat(d)))
        finally:
            self.dbtobj.session_release(session)

    def wake_all(self):
        '''
        Wake all background threads
        '''
        notify_all(self.fhseq_run_cond)
        notify_all(self.fhseq_get_cond)
        notify_all(self.toc_run_cond)
        notify_all(self.toc_flush_cond)

class _UpdateStateDesc():
    'Describe one dbc_update_state() request'
    def __init__(self, state, child_dir_count, size):
        self.state = state
        self.child_dir_count = child_dir_count
        self.size = size
        self.tobj = None

    def __bool__(self):
        'True iff this _UpdateStateDesc modifies anything'
        return (self.state is not None) or (self.child_dir_count is not None) or (self.size is not None)

    def add(self, other):
        'Overwrite settings in self with non-None settings from other'
        if other.state is not None:
            self.state = other.state
        if other.child_dir_count is not None:
            self.child_dir_count = other.child_dir_count
        if other.size is not None:
            self.size = other.size

class _DBSessionWrapper():
    '''
    Wrap session management for one database.
    '''
    def __init__(self, logger, filepath):
        self._logger = logger
        self._filepath = os.path.abspath(filepath)
        self._uri = 'sqlite:///' + self._filepath
        if not os.path.exists(self._filepath):
            # We must set the page_size here before sqlalchemy
            # slips in and starts issuing commands.
            logger.debug("precreate %s", self._filepath)
            conn = None
            cursor = None
            try:
                conn = sqlite3.connect(self._filepath)
                cursor = conn.cursor()
                cursor.execute("PRAGMA page_size=%d;" % _PAGE_SIZE)
                # Create a table so we do something with the page_size.
                # Otherwise, it's gone when a new connection comes along to
                # create a real table.
                cursor.execute("CREATE TABLE bogus(key INTEGER, value INTEGER, PRIMARY KEY (key));")
            finally:
                if cursor:
                    cursor.close()
                    cursor = None
                if conn:
                    conn.commit()
                    conn.close()
            del conn
        self._engine = sqlalchemy.create_engine(self._uri)
        self._count = 0

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self._filepath)

    def __repr__(self):
        return "<%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self._filepath)

    def __del__(self):
        if hasattr(self, '_logger') and hasattr(self, '_count'):
            if self._count:
                self._logger.warning("%s del with outstanding count %d", self, self._count)

    def create_all(self):
        '''
        Create all tables for this session. This implies that we are starting a new run.
        '''
        self._logger.debug("Create new DB %s", self._filepath)
        DbEntBase.metadata.create_all(self._engine)

    def session_get(self):
        '''
        Generate and return a new session with the transaction started.
        The caller is responsible for using session_release() to
        destroy the session.
        '''
        ret = Session(bind=self._engine, autoflush=False, _enable_transaction_accounting=False)
        self._count += 1
        return ret

    def session_release(self, session):
        'Destroy a session previous obtained from session_get()'
        if session is not None:
            self._count -= 1
            session.rollback()

class PreclaimState():
    '''
    Encapsulate state about preclaimed target objects. A target object
    is preclaimed if it is marked claimed in the table but no worker
    thread is currently processing it. We try to keep a minimum level
    of preclaimed objects so that worker threads do not stall waiting
    to mark objects claimed. No locking in this obj; the caller (dbc_*
    methods of ClfsLoadDB) is responsible for that.
    '''
    dirs_generating_min = 2

    def __init__(self, logger):
        self._logger = logger

        # Phase.TRANSFER
        self._writing_data = list()
        self._writing_nodata = list()
        self._generating_dirs = list()
        self._pend_writing_data = list()
        self._pend_writing_nodata = list()
        self._pend_generating_dirs = list()
        self._tobj_index = dict()
        self._tobj_active = dict()

        # Phase.RECONCILE
        self._writing_inodes = list()
        self._pend_writing_inodes = list()

        # Phase.CLEANUP
        self._cleaning_inodes = list()
        self._pend_cleaning_inodes = list()

        self._phase = None
        self._lowat_count = 0 # low-water count; preclaim more if we have fewer than hits
        self._hiwat_count = 0 # when we preclaim, aim for this many

        self.accepted_all_offers = True # fastpath out of preclaim_more()
        self.get_one = None # proc, set in phase.setter

    def __len__(self):
        if self._phase == Phase.TRANSFER:
            return len(self._tobj_index) \
                + len(self._pend_writing_data) \
                + len(self._pend_writing_nodata) \
                + len(self._pend_generating_dirs)
        if self._phase == Phase.RECONCILE:
            return len(self._writing_inodes) + len(self._pend_writing_inodes)
        if self._phase == Phase.CLEANUP:
            return len(self._cleaning_inodes) + len(self._pend_cleaning_inodes)
        return 0

    def __bool__(self):
        '''
        Return whether there is anything preclaimed. Must include pend_*
        so that flushing knows if has pending preclaim.
        '''
        if self._phase == Phase.TRANSFER:
            if self._tobj_index \
              or self._pend_writing_data \
              or self._pend_writing_nodata \
              or self._pend_generating_dirs:
                return True
            return False
        if self._phase == Phase.RECONCILE:
            if self._writing_inodes or self._pend_writing_inodes:
                return True
            return False
        if self._phase == Phase.CLEANUP:
            if self._cleaning_inodes or self._pend_cleaning_inodes:
                return True
            return False
        return False

    @property
    def phase(self):
        return self._phase

    _TRANSFER_LOWAT = 300
    _TRANSFER_HIWAT = 10000
    _RECONCILE_LOWAT = 600
    _RECONCILE_HIWAT = 10000
    _CLEANUP_LOWAT = 5000
    _CLEANUP_HIWAT = 10000

    @phase.setter
    def phase(self, value):
        self._phase = value
        if self._phase == Phase.TRANSFER:
            self._lowat_count = self._TRANSFER_LOWAT
            self._hiwat_count = self._TRANSFER_HIWAT
            self.get_one = self._get_one_transfer
        elif self._phase == Phase.RECONCILE:
            self._lowat_count = self._RECONCILE_LOWAT
            self._hiwat_count = self._RECONCILE_HIWAT
            self.get_one = self._get_one_reconcile
        elif self._phase == Phase.CLEANUP:
            self._lowat_count = self._CLEANUP_LOWAT
            self._hiwat_count = self._CLEANUP_HIWAT
            self.get_one = self._get_one_cleanup
        else:
            self._lowat_count = 0
            self._hiwat_count = 0

    def ready_count(self):
        'Return the number of items ready for claim'
        return len(self._tobj_index) \
            + len(self._writing_inodes) \
            + len(self._cleaning_inodes)

    def want_more(self):
        'Return True if we should fetch more'
        # Skip considering the extra generating dir to avoid repeatedly
        # forcing flushes that do not find it. Just consider low-water.
        return len(self) < self._lowat_count

    def load_startup(self, db, reset_dirs=True):
        '''
        Load all claimed target objects. This is done irrespective
        of target counts. It is called outside any extant transaction.
        If reset_dirs is set, clear backpointers from existing dirs.
        '''
        # Only Ftype.DIR may be GENERATING
        # Clear backpointers before loading.
        self._generating_dirs = db.db_tobj_list_in_state(TargetObjState.GENERATING)
        if reset_dirs:
            session = None
            try:
                session = db.dbtobj.session_get()
                query_f = db.query_DbEntTargetObj.with_session(session)
                force_reconcile_fhs = set()
                for tobj in self._generating_dirs:
                    dbe = query_f.get(tobj.filehandle.bytes)
                    if dbe:
                        db.db_force_reconcile(dbe, force_reconcile_fhs)
                    db.db_clear_backpointers(session, tobj.filehandle, force_reconcile_fhs)
                del force_reconcile_fhs
                session.commit()
            finally:
                db.dbtobj.session_release(session)

        # Reload generating_dirs - we may have changed backpointers above
        self._generating_dirs = db.db_tobj_list_in_state(TargetObjState.GENERATING, ic_restored_in_progress=True)
        self._generating_dirs.sort(key=lambda tobj: tobj.size)
        self._tobj_index = {tobj.filehandle : tobj for tobj in self._generating_dirs}

        # For selection purposes, treat DIR, REG, and LNK together -
        # they are all variable-length types, and we would rather
        # service larger ones first.
        tobjs = db.db_tobj_list_in_state(TargetObjState.WRITING, ic_restored_in_progress=True)
        for tobj in tobjs:
            if tobj.ftype in DATA_FTYPES:
                self._writing_data.append(tobj)
            else:
                self._writing_nodata.append(tobj)
            self._tobj_index[tobj.filehandle] = tobj
        self._writing_data.sort(key=lambda tobj: tobj.size)

        self._writing_inodes = db.db_tobj_list_in_state(TargetObjState.INODE_WRITING, ic_restored_in_progress=True)

        self._cleaning_inodes = db.db_tobj_list_in_state(TargetObjState.INODE_CLEANUP_CLEANING, ic_restored_in_progress=True)

    def preclaim_offer(self, tobj):
        '''
        Offer a tobj to preclaim. This is done while flushing.
        If interested, modify the tobj to reflect the preclaim
        and append to the correct pend list. This works without
        locks because the pend lists are not visible until
        pend_committed().
        BEWARE: This operates without copying the tobj.
        Only upserting objects are offered, and that only
        happens during Phase.TRANSFER.
        '''
        assert (self._phase == Phase.TRANSFER) or (self._phase == Phase.INIT)
        if tobj.state == TargetObjState.PENDING:
            have = len(self._tobj_index)
            have += len(self._pend_writing_data) + len(self._pend_writing_nodata) + len(self._pend_generating_dirs)
            if tobj.ftype == Ftype.DIR:
                have_generating = len(self._generating_dirs) + len(self._pend_generating_dirs)
                if (have_generating < self.dirs_generating_min) or (have < self._hiwat_count):
                    tobj.state = TargetObjState.GENERATING
                    self._pend_generating_dirs.append(tobj)
                else:
                    self.accepted_all_offers = False
            else:
                if have < self._hiwat_count:
                    tobj.state = TargetObjState.WRITING
                    if tobj.ftype <= Ftype.LNK:
                        self._pend_writing_data.append(tobj)
                    else:
                        self._pend_writing_nodata.append(tobj)
                else:
                    self.accepted_all_offers = False

    def preclaim_add_pend_writing_data(self, tobj):
        '''
        Add the given tobj to _pend_writing_data
        '''
        self._pend_writing_data.append(tobj)

    def preclaim_update_for_add_backpointer(self, dbe):
        '''
        dbe is updated within a flush. If we have it in a writing or
        generating list, update it
        '''
        filehandle = Filehandle(dbe.filehandle)
        try:
            tobj = self._tobj_index[filehandle]
        except KeyError:
            try:
                tobj = self._tobj_active[filehandle]
            except KeyError:
                return
            tobj.pend_first_backpointer = Filehandle(dbe.first_backpointer)
            tobj.pend_nlink = dbe.nlink
            return
        tobj.first_backpointer = Filehandle(dbe.first_backpointer)
        tobj.nlink = dbe.nlink

    def preclaim_more(self, db, session, query):
        if self.accepted_all_offers:
            return
        db.check_usable()
        if self._phase == Phase.TRANSFER:
            self._preclaim_more_transfer(query)
        elif self._phase == Phase.RECONCILE:
            self._preclaim_more_reconcile(db, session, query)
        elif self._phase == Phase.CLEANUP:
            self._preclaim_more_cleanup(query)

    def _preclaim_more_transfer(self, query):
        '''
        This is called from within a flush operation during the TRANSFER phase.
        Lay claim to more objects.
        '''
        have = len(self._tobj_index)
        have += len(self._pend_writing_data) + len(self._pend_writing_nodata) + len(self._pend_generating_dirs)
        if (have > self._hiwat_count) and self._generating_dirs:
            return
        remaining = self._hiwat_count - have
        query = query.filter(DbEntTargetObj.state == TargetObjState.PENDING)

        # Something to think about: Should this query order by size, pulling largest first?
        # That would require additional disk accesses to the DB to support the
        # additional indexing required. If we start distributing the work across many
        # threads on many nodes, this could become more important to reduce
        # the likelihood that a small number of large objects appear at the end .

        if remaining > 0:
            query2 = query.filter(DbEntTargetObj.ftype > Ftype.DIR).filter(DbEntTargetObj.ftype <= Ftype.LNK).limit(remaining)
            count = 0
            for dbe in query2:
                dbe.state = TargetObjState.WRITING
                self._pend_writing_data.append(TargetObj.from_dbe(dbe))
                count += 1
            remaining -= count

        if remaining > 0:
            query2 = query.filter(DbEntTargetObj.ftype > Ftype.LNK).limit(remaining)
            count = 0
            for dbe in query2:
                dbe.state = TargetObjState.WRITING
                self._pend_writing_nodata.append(TargetObj.from_dbe(dbe))
                count += 1
            remaining -= count

        # We want to retire leaves over dirs to keep down the size of the table.
        # If we strictly prioritize leaves, we can introduce stalls when we wait
        # to start generating a dir. Our caller (dbc_flush) will hint to get_one_*()
        # when it would rather have a dir, but we want to be sure to keep at least some,
        # even if that puts us one over the high-water mark.
        dga = self.dirs_generating_min - len(self._generating_dirs)
        remaining2 = max(remaining, dga)
        if remaining2 > 0:
            query2 = query.filter(DbEntTargetObj.ftype == Ftype.DIR).limit(remaining2)
            count = 0
            for dbe in query2:
                dbe.state = TargetObjState.GENERATING
                self._pend_generating_dirs.append(TargetObj.from_dbe(dbe))
                count += 1
            remaining -= count

    def _preclaim_more_reconcile(self, db, session, query):
        '''
        This is called from within a flush operation during the RECONCILE phase.
        Lay claim to more objects.
        '''
        remaining = self._hiwat_count - len(self._writing_inodes)
        if remaining > 0:
            query = query.filter(DbEntTargetObj.state == TargetObjState.INODE_PENDING)
            query = query.limit(remaining)
            bpquery = db.query_DbEntAdditionalBackpointerMapEnt.with_session(session)
            for dbe in query:
                dbe.state = TargetObjState.INODE_WRITING
                dbe.reconcile_vers = dbe.reconcile_vers + 1
                tobj = TargetObj.from_dbe(dbe)
                # The writer needs to know the backpointers and the accurate
                # link count, so load them here.
                if tobj.ftype != Ftype.DIR:
                    db.db_load_additional_backpointers_for_tobj(bpquery, tobj)
                self._pend_writing_inodes.append(tobj)

    def _preclaim_more_cleanup(self, query):
        '''
        This is called from within a flush operation during the CLEANUP phase.
        Lay claim to more objects.
        '''
        remaining = self._hiwat_count - len(self._cleaning_inodes)
        if remaining > 0:
            query = query.filter(DbEntTargetObj.state == TargetObjState.INODE_CLEANUP_PENDING)
            query = query.limit(remaining)
            for dbe in query:
                dbe.state = TargetObjState.INODE_CLEANUP_CLEANING
                tobj = TargetObj.from_dbe(dbe)
                self._pend_cleaning_inodes.append(tobj)

    def pend_committed(self):
        '''
        Called from flush after the commit has completed. Caller
        holds the dbc lock. Move pending updates to committed.
        '''
        # These sorts are not global sorts by size of all pending objects -- just the ones we found.
        # This provides bias towards favoring larger objects first without extra DB overhead.
        if self._pend_writing_data:
            for tobj in self._pend_writing_data:
                self._tobj_index[tobj.filehandle] = tobj
                self._tobj_active.pop(tobj.filehandle, None)
            self._writing_data.extend(self._pend_writing_data)
            # Sort smallest to largest. When we pop in _get_one_transfer(),
            # we take the largest first.
            self._writing_data.sort(key=lambda tobj: tobj.size)
        self._pend_writing_data = list()

        for tobj in self._pend_writing_nodata:
            self._tobj_index[tobj.filehandle] = tobj
        self._writing_nodata.extend(self._pend_writing_nodata)
        self._pend_writing_nodata = list()

        if self._pend_generating_dirs:
            for tobj in self._pend_generating_dirs:
                self._tobj_index[tobj.filehandle] = tobj
            self._generating_dirs.extend(self._pend_generating_dirs)
            # Sort smallest to largest. When we pop in _get_one_transfer(),
            # we take the largest first.
            self._generating_dirs.sort(key=lambda tobj: tobj.size)
        self._pend_generating_dirs = list()

        self._writing_inodes.extend(self._pend_writing_inodes)
        self._pend_writing_inodes = list()

        self._cleaning_inodes.extend(self._pend_cleaning_inodes)
        self._pend_cleaning_inodes = list()

    def _get_one_transfer(self):
        '''
        Get one preclaimed item during the transfer phase.
        '''
        if self.want_more():
            try:
                ret = self._generating_dirs.pop()
                self._tobj_index.pop(ret.filehandle, None)
                self._tobj_active[ret.filehandle] = ret
                return ret
            except IndexError:
                pass
        try:
            ret = self._writing_data.pop()
            self._tobj_index.pop(ret.filehandle, None)
            self._tobj_active[ret.filehandle] = ret
            return ret
        except IndexError:
            pass
        try:
            ret = self._writing_nodata.pop()
            self._tobj_index.pop(ret.filehandle, None)
            self._tobj_active[ret.filehandle] = ret
            return ret
        except IndexError:
            pass
        try:
            ret = self._generating_dirs.pop()
            self._tobj_index.pop(ret.filehandle, None)
            self._tobj_active[ret.filehandle] = ret
            return ret
        except IndexError:
            pass
        return None

    def _get_one_reconcile(self):
        '''
        Get one preclaimed item during the reconcile phase.
        '''
        try:
            return self._writing_inodes.pop()
        except IndexError:
            return None

    def _get_one_cleanup(self):
        '''
        Get one preclaimed item during the cleanup phase.
        '''
        try:
            return self._cleaning_inodes.pop()
        except IndexError:
            return None
