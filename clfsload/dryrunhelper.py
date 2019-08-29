#
# clfsload/dryrunhelper.py
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
DryRunHelper is a class for performing a generic dry run using a Reader
'''

from __future__ import absolute_import
from __future__ import division

import collections
import logging
import threading

from clfsload.types import CLFS_LINK_MAX, DryRunResult, Ftype, ReaderInfo, SimpleError
from clfsload.util import Size, exc_info_err, exc_log, getframe

class _ThreadState():
    'state of one DryRunHelper thread'
    def __init__(self, thread_num, target):
        self._thread_num = thread_num
        self._name = "dryrun-thread-%02d" % self._thread_num
        self._thread = threading.Thread(target=target, name=str(self), args=(self,))
        # Accumulate per-thread results to avoid locking shared results for every access
        self.result = DryRunResult()

    def __repr__(self):
        return self._name

    def start(self):
        'start this thread'
        self._thread.start()

class DryRunHelper():
    'class for performing a generic dry run using a Reader'

    # Maximum size of the link set
    MAX_NLINK = 1000000

    def __init__(self, reader, thread_count, thread_shared_state, preserve_hardlinks):
        self._reader = reader
        self._thread_count = thread_count
        self._thread_shared_state = thread_shared_state
        self._should_run = True
        self.result = None
        self._done_count = 0
        self._busy_count = 0
        self._error_count = 0
        self._lock = threading.Lock()
        self._nlink_lock = threading.Lock()
        self._nlink_limit_warned = False
        self._nlink_set = set() # inode_number
        self._dir_cond = threading.Condition(lock=self._lock)
        self._run_cond = threading.Condition(lock=self._lock)
        root_ri = ReaderInfo.from_tobj(reader.target_obj_for_root_get())
        self._root_ri_gb = root_ri.ostat.st_size / Size.GB
        self._dirs_pending = collections.deque()
        self._dirs_pending.append(root_ri)
        self._thread_states = [_ThreadState(thread_num, self._one_thread_run) for thread_num in range(self._thread_count)]
        if preserve_hardlinks:
            self._one_thread_one_directory = self._one_thread_one_directory_withlink
        else:
            self._one_thread_one_directory = self._one_thread_one_directory_nolink
        self._success = None

    @property
    def logger(self):
        return self._reader.logger

    @property
    def should_run(self):
        return self._should_run and self._thread_shared_state.should_run

    def _loc(self):
        '''
        Return a useful location string
        '''
        return "%s.%s" % (self.__class__.__name__, getframe(1))

    def _abort_NL(self):
        '''
        tell all workers to stop and set global shutdown state
        caller holds self._lock
        '''
        self._thread_shared_state.should_run = False
        self._stop_NL()

    def _abort(self):
        '''
        tell all workers to stop and set global shutdown state
        '''
        with self._lock:
            self._abort_NL()

    def _stop_NL(self):
        '''
        tell all workers to stop
        caller holds self._lock
        '''
        self._should_run = False
        self._should_run = False
        self._run_cond.notify_all()
        self._dir_cond.notify_all()

    def run(self):
        '''
        Do the dry run. Return the DryRunResult.
        '''
        logger = self.logger
        with self._lock:
            try:
                for wts in self._thread_states:
                    wts.start()
                while self.should_run:
                    if (not self._dirs_pending) and (not self._busy_count):
                        # Nothing pending and all threads are idle
                        self._stop_NL()
                        self._dir_cond.notify_all()
                        break
                    self._run_cond.wait(timeout=1.0)
                while self._done_count < self._thread_count:
                    self._run_cond.wait(timeout=1.0)

                if self._error_count > 0:
                    logger.error("Dry run failed with error count %u", self._error_count)
                    raise SystemExit(1)

                result = DryRunResult(count=1, gb=self._root_ri_gb)
                for wts in self._thread_states:
                    result.add(wts.result)
                self.result = result
                return result
            except (KeyboardInterrupt, SystemExit) as e:
                logger.warning("%s %s", e.__class__.__name__, self._loc())
                self._abort_NL()
            except:
                exc_log(logger, logging.ERROR, str(wts))
                self._abort_NL()

    def _one_thread_run(self, wts):
        '''
        Do the work associated with one thread.
        Wraps _one_thread_run_inner() and ensures that high-level
        thread mechanics are handled.
        '''
        logger = self.logger
        try:
            self._one_thread_run_inner(wts)
        except KeyboardInterrupt as e:
            logger.warning("KeyboardInterrupt %s", self._loc())
            self._abort()
            raise SystemExit(1) from e
        except SystemExit:
            self._abort()
            raise
        except BaseException as e:
            exc_log(logger, logging.ERROR, str(wts))
            self._abort()
            raise SystemExit(1) from e
        finally:
            with self._lock:
                self._done_count += 1
                self._dir_cond.notify_all()
                self._run_cond.notify_all()

    def _one_thread_run_inner(self, wts):
        'Do the work associated with one thread'
        dir_fail = False
        while self.should_run:
            with self._lock:
                try:
                    dir_ri = self._dirs_pending.popleft()
                    self._busy_count += 1
                except IndexError:
                    self._run_cond.notify_all()
                    self._dir_cond.wait(timeout=1.0)
                    continue
            try:
                self._one_thread_one_directory(wts, dir_ri)
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                dir_fail = True

            with self._lock:
                if dir_fail:
                    self._error_count += 1
                self._busy_count -= 1

    def _one_thread_one_directory_nolink(self, wts, dir_ri):
        '''
        Do the work for a single directory; hardlink support disabled
        This is a fastpath version of _one_thread_one_directory_withlink
        '''
        logger = self.logger
        reader = self._reader
        found_dirs = list()
        try:
            directory = reader.opendir(dir_ri.path)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as e:
            exc_log(logger, logging.DEBUG, str(wts))
            raise SimpleError("%s: unable to open directory '%s': %s" % (wts, dir_ri.path, exc_info_err())) from e

        while True:
            try:
                ri = reader.getnextfromdir(directory)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:
                exc_log(logger, logging.DEBUG, str(wts))
                raise SimpleError("%s: unable to read entry in directory '%s': %s" % (wts, dir_ri.path, exc_info_err())) from e

            if not ri:
                with self._lock:
                    self._dirs_pending.extend(found_dirs)
                return
            if ri.ftype == Ftype.DIR:
                found_dirs.append(ri)
                # Peek at self._busy_count and self._dirs_pending without the lock as an optimization
                if (self._busy_count < self._thread_count) or (not self._dirs_pending) or (len(found_dirs) >= 100):
                    with self._lock:
                        self._dirs_pending.extend(found_dirs)
                    found_dirs = list()
            wts.result.dr_count += 1
            wts.result.dr_gb += ri.ostat.st_size / Size.GB
            if (ri.ftype == Ftype.REG) and (not reader.readable(ri)):
                with self._lock:
                    self.logger.error("cannot read '%s'", ri.path)
                    self._error_count += 1

    def _one_thread_one_directory_withlink(self, wts, dir_ri):
        '''
        Do the work for a single directory; hardlink support enabled.
        '''
        logger = self.logger
        reader = self._reader
        found_dirs = list()
        try:
            directory = reader.opendir(dir_ri.path)
        except (KeyboardInterrupt, SystemExit) as e:
            if isinstance(e, KeyboardInterrupt):
                logger.warning("KeyboardInterrupt %s", self._loc())
            raise
        except BaseException as e:
            exc_log(logger, logging.DEBUG, str(wts))
            raise SimpleError("%s: unable to read entry in directory '%s': %s" % (wts, dir_ri.path, exc_info_err())) from e

        while True:
            try:
                ri = reader.getnextfromdir(directory)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as e:
                exc_log(logger, logging.DEBUG, str(wts))
                raise SimpleError("%s: unable to read entry in directory '%s': %s" % (wts, dir_ri.path, exc_info_err())) from e

            if not ri:
                with self._lock:
                    self._dirs_pending.extend(found_dirs)
                return
            if ri.ftype == Ftype.DIR:
                found_dirs.append(ri)
                # Peek at self._busy_count and self._dirs_pending without the lock as an optimization
                if (self._busy_count < self._thread_count) or (not self._dirs_pending) or (len(found_dirs) >= 100):
                    with self._lock:
                        self._dirs_pending.extend(found_dirs)
                    found_dirs = list()
            elif ri.ostat.st_nlink != 1:
                # Non-directory with nlink != 1
                with self._nlink_lock:
                    if ri.ostat.st_ino in self._nlink_set:
                        # Already counted this one
                        continue
                    if len(self._nlink_set) >= self.MAX_NLINK:
                        if not self._nlink_limit_warned:
                            self.logger.warning("reached nlink tracking limit %d; dry run counts may be inaccurate",
                                                self.MAX_NLINK)
                            self._nlink_limit_warned = True
                    else:
                        self._nlink_set.add(ri.ostat.st_ino)
                if ri.ostat.st_nlink > CLFS_LINK_MAX:
                    count = 1 + ri.ostat.st_nlink - CLFS_LINK_MAX
                    wts.result.dr_count += count
                    wts.result.dr_gb += count * (ri.ostat.st_size / Size.GB)
                    continue
            wts.result.dr_count += 1
            wts.result.dr_gb += ri.ostat.st_size / Size.GB
