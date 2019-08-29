#
# clfsload/writerazure.py
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
from __future__ import division
from __future__ import print_function

import collections
import http.client
import inspect
import logging
import math
import os
import pprint
import random
import socket
import struct
import sys
import threading
import time
import urllib3

import azure.common
from azure.storage.blob import BlockBlobService
from defusedxml import ElementTree

from clfsload.clfsutils import generate_bucket_name
from clfsload.parse import BlockListManager, CLFSBucketObjAttrs, CLFSSegment, Ftype, \
                           ObjectListManager, ObjectListTracker, \
                           add_compression_header, generate_bucket_target_obj, get_unparse_handle_list, \
                           obj_reconcile, parse_obj, strip_compression_header_and_decompress, \
                           unparse_inode, unparse_obj_handles, unparse_segment, unparse_segment_using_obid
from clfsload.reader import ClfsReadAhead
from clfsload.types import AbortException, Btype, CLFSCompressionType, CLFSLoadException, \
                           ContainerTerminalError, \
                           FILEHANDLE_INODE_INPROGRESS, \
                           MKNOD_FTYPES, NamedObjectError, \
                           ObCacheId, ServerRejectedAuthError, \
                           SimpleError, SourceObjectError, TargetObj, \
                           TargetObjectError, TerminalError, WRock
from clfsload.util import STRUCT_LE_U32, Size, elapsed, \
                          exc_info_err, exc_log, exc_stacklines, getframe
from clfsload.writer import Writer

class _MSApiCall():
    '''
    Wrapper object for a single Azure API call: op(*args, **kwargs)
    Initialize the op by creating the object, then invoke it with doit().
    '''
    def __init__(self, writer, target, op, *args, **kwargs):
        '''
        target may be a string (source) or a TargetObj (target)
        '''
        if not isinstance(target, (str, TargetObj)):
            raise TypeError("target (%s)" % target.__class__.__name__)
        self._writer = writer
        self._logger = self._writer.logger
        self._target = target
        self._op_call = op
        self._op_args = args
        self._op_kwargs = kwargs

    class Caught():
        def __init__(self, logger, e, target):
            self._logger = logger
            self._target = target
            exc_info = sys.exc_info()
            self.eclass = exc_info[0]
            self.exception = e
            self.name = self.eclass.__name__.split('.')[-1]
            self.string = str(exc_info[1])
            del exc_info
            self.exception = e
            self.status_code = getattr(self.exception, 'status_code', None)
            try:
                self.status_code_int = int(self.status_code)
            except:
                self.status_code_int = -1
            self.stacklines = exc_stacklines()
            self.error_code = None
            tmp = str(self)
            idx = tmp.find('<?xml')
            if idx >= 0:
                try:
                    xmltxt = ''.join(tmp[idx:].strip().splitlines())
                    et_root = ElementTree.fromstring(xmltxt)
                    et_code_list = et_root.findall('Code')
                    self.error_code = str(et_code_list[0].text)
                except:
                    pass
            self.exception_data_error = None
            try:
                self.exception_data_error = self.exception.error.error
            except:
                pass

        def __str__(self):
            return "%s %s" % (self.name, self.string)

        def __repr__(self):
            return "%s %s" % (self.name, self.string)

        def is_conflict(self):
            return (self.status_code == http.client.CONFLICT) \
              or issubclass(self.eclass, azure.common.AzureConflictHttpError)

        def is_missing(self):
            return (self.status_code == http.client.NOT_FOUND) \
              or issubclass(self.eclass, azure.common.AzureMissingResourceHttpError) \
              or (self.exception_data_error == 'BlobNotFound')

        _no_retry_classes = (azure.common.AzureException,
                             azure.common.AzureHttpError,
                             azure.common.AzureMissingResourceHttpError
                            )

        _no_retry_CloudError_data = ('BlobNotFound',
                                     'ExpiredAuthenticationToken',
                                     'MaxStorageAccountsCountPerSubscriptionExceeded',
                                     'ResourceGroupNotFound',
                                    )

        def describe_target(self):
            '''
            Return a human-readable description of the target
            '''
            try:
                return self._target.describe()
            except:
                return str(self._target)

        def describe(self):
            '''
            Return a human-readable description of this object
            '''
            return "%s %s" % (self.describe_target(), self.eclass.__name__)

        def is_no_retry_class(self):
            '''
            Return whether the class of this error is potentially not retryable.
            This is allowed to return a false positive.
            '''
            return issubclass(self.eclass, self._no_retry_classes)

        def is_server_rejected_auth(self):
            '''
            Return whether this error is server rejected authentication
            '''
            return (self.error_code == 'AuthenticationFailed') \
              or (self.exception_data_error == 'ExpiredAuthenticationToken')

        def retry_time(self):
            '''
            Return None if the operation should not retry
            Return 0.0 if the operations should retry immediately
            Return > 0.0 for an amount of time the operation should sleep before retrying
            '''
            if self.is_server_rejected_auth() \
              or self.is_missing() \
              or issubclass(self.eclass, (KeyboardInterrupt, SystemExit, TypeError)) \
              or (self.is_conflict() and (self.status_code_int >= 0) and (self.status_code_int < 500)):
                return None
            if (self.status_code == http.client.TOO_MANY_REQUESTS) or self.is_conflict():
                # Do a longer sleep to let things cool down.
                # Jitter the sleep to break up convoys.
                return random.uniform(28, 32)
            # AzureException is part of no_retry_classes, so _ERROR_DECRYPTION_FAILURE
            # is never retried.
            if issubclass(self.eclass, self._no_retry_classes) \
              or (self.exception_data_error in self._no_retry_CloudError_data):
                return None
            return random.uniform(1, 3)

        def quiet(self):
            '''
            Return true iff this error is not interesting for verbose logging.
            '''
            return self.is_conflict() or self.is_server_rejected_auth() or self.is_missing()

    class _CaughtWrap(CLFSLoadException):
        'Wrap a Caught object in an exception'
        def __init__(self, caught):
            CLFSLoadException.__init__(self, caught.describe())
            self.caught = caught

    def _op_str(self):
        'Return a human-friendly string version of self._op_call'
        a = inspect.getmembers(self._op_call, inspect.isfunction)
        if len(a) == 1:
            return a[0][1]
        return str(self._op_call)

    def doit(self, not_found_ok=False):
        '''
        Invoke the op, handling retries. Either returns the result
        or raises one of: SourceObjectError, TargetObjectError, ServerRejectedAuthError
        '''
        # Wrap _doit_internal(). This is belt-and-suspenders to ensure
        # that we restrict exception types that come out of here.
        caught = None
        try:
            return self._doit_internal()
        except self._CaughtWrap as e:
            caught = e.caught
        if caught.is_missing() and not_found_ok:
            return None
        if not caught.quiet():
            logger = self._logger
            cannot = "%s cannot %s: %s" % (caught.describe(), self._op_str(), caught.string)
            logstr = "%s\nstack:\n%s" % (cannot, pprint.pformat(caught.stacklines))
            if caught.is_no_retry_class():
                logstr += '\n'
                logstr += pprint.pformat(vars(caught.exception))
            if caught.error_code:
                logstr += "\nerror_code: '%s'" % caught.error_code
            logstr += '\n'
            logstr += cannot
            logger.error("%s", logstr)
        err = "writer error from %s: %s %s" % (self._op_str(), caught.eclass.__name__, caught.string)
        if caught.is_server_rejected_auth():
            # Include detail about the failure in the debug logs
            # to help diagnose why the token might be invalid.
            # Do not log the token itself.
            self._logger.debug("ServerRejectedAuthError %s %s %s %s\n%s",
                               getframe(0), getframe(1),
                               self._op_str(), caught.describe(),
                               pprint.pformat(vars(caught.exception)))
            raise ServerRejectedAuthError(caught.describe())
        if isinstance(self._target, str):
            raise SourceObjectError(self._target, err)
        raise TargetObjectError(self._target, err)

    def _doit_internal(self):
        '''
        Loop that attempts the call.
        '''
        max_attempts = 5
        cur_attempt = 0
        while cur_attempt < max_attempts:
            cur_attempt += 1
            try:
                return self._op_call(*self._op_args, **self._op_kwargs)
            except NamedObjectError:
                raise
            except (KeyboardInterrupt, SystemExit) as e:
                raise
            except BaseException as e: # pylint: disable=broad-except
                caught = self.Caught(self._logger, e, self._target)
            if cur_attempt >= max_attempts:
                break
            sleep_secs = caught.retry_time()
            if sleep_secs is None:
                break
            time.sleep(sleep_secs)
        raise self._CaughtWrap(caught)

class TransferStateOneSource():
    '''
    State for transferring one source.
    This also handles per-file throttling of our requests to the thread pool.
    '''
    # Post a warning if we've waited for this many seconds for writes to complete (for debugging)
    WAIT_TIME_WARN_SECS = 300

    def __init__(self, writerazure, parent_wrock, tobj, tpool, queue_size_max):
        self.writerazure = writerazure
        self.parent_wrock = parent_wrock
        self.tobj = tobj
        self.tpool = tpool
        self.queue_size_max = queue_size_max # max number of threads from our pool that this transfer can consume
        self.tsos_lock = threading.Lock()    # for synchronizing our queue
        self.tsos_cond = threading.Condition(lock=self.tsos_lock)
        self.in_flight_count = 0
        self._should_run = True         # protected by tsos_lock, set to False to stop further transfers for this file
        self.target_object_error = None # protected by tsos_lock; TargetObjectError or None

    def queue_transfer(self, trock):
        'Queue the next item to the thread pool, wait on our throttling limit if necessary.  Inc in_flight_count.'
        with self.tsos_cond:
            timeout = min(2.0, self.WAIT_TIME_WARN_SECS)
            start_time = time.time()
            warned = False
            while self._should_run and (self.in_flight_count >= self.queue_size_max):
                self.tsos_cond.wait(timeout=timeout)
                if self._should_run:
                    wait_time = elapsed(start_time)
                    if (wait_time >= self.WAIT_TIME_WARN_SECS) and (not warned):
                        self.parent_wrock.logger.warning("long wait time in writer queue (%.1f seconds), check for connectivity issues", wait_time)
                        warned = True
            if self._should_run:
                self.in_flight_count += 1
                self.tpool.queue_transfer(trock)

    def transfer_complete(self):
        'A transfer thread must call this when a tranfer is complete.  Dec in_flight_count.'
        with self.tsos_cond:
            assert self.in_flight_count > 0
            self.in_flight_count -= 1
            self.tsos_cond.notify()  # OK to start new work if there is a waiter

    def wait_all_complete(self):
        'Wait until all of our transfers in the thread pool have completed (in_flight_count == 0).'
        start_time = time.time()
        with self.tsos_cond:
            warned = False
            while self._should_run and (self.in_flight_count > 0):
                self.tsos_cond.wait(timeout=1.0)
                wait_time = elapsed(start_time)
                if (wait_time >= self.WAIT_TIME_WARN_SECS) and (not warned):
                    self.parent_wrock.logger.warning("long wait time for writes to complete (%.1f seconds), check for connectivity issues (in_flight_count=%d)",
                                                     wait_time, self.in_flight_count)
                    warned = True

    @property
    def should_run(self):
        'accessor'
        # No point in taking the lock here just to read. If we do
        # decide to take the lock here, switch run_lock to RLock
        # to handle loops where should_run is examined while locked.
        if not self._should_run:
            return False
        return self.parent_wrock.should_run

    def set_should_run_NL(self, value):
        'sets should_run; caller must hold tsos_cond'
        assert not value
        self._should_run = False # asserted above
        self.tsos_cond.notify_all()

    @should_run.setter
    def should_run(self, value):
        with self.tsos_cond:
            self.set_should_run_NL(value)

    def see_exception(self, exception):
        '''
        Called from an exception context - indicates that this transfer
        fails because of this exception
        '''
        with self.tsos_cond:
            if self.target_object_error is None:
                if isinstance(exception, (NamedObjectError, TerminalError)):
                    self.target_object_error = exception
                else:
                    self.target_object_error = TargetObjectError(self.tobj, exc_info_err())
                self.set_should_run_NL(False)
            else:
                assert not self.should_run

class TransferRock():
    'trock filled in by WriterAzure.transfer() that contains the copy parameters for one write_blob request.'
    def __init__(self, blob_ob_id, blob_name, segment_bytes, offset, tsos):
        self.blob_ob_id = blob_ob_id
        self.blob_name = blob_name
        self.segment_bytes = segment_bytes
        self.offset = offset
        self.tsos = tsos    # Reference back to the TransferStateOneSource that maintains the overall transfer state

class TransferThreadWRock(WRock):
    '''
    One of these are created for every TransferRock during a multi-threaded transfer.
    See types.WRock for wrock properties.
    '''
    def __init__(self, thread_num, parent_wrock, storage_account_name, sas_token):
        '''
        Create this new transfer state in terms of an existing wrock.
        offset - offset being transferred (used as a uniquifier)
        parent_wrock - wrock of the child process; used as the template for this wrock
        '''
        name = "%s.%02d" % (parent_wrock.name, thread_num)
        super(TransferThreadWRock, self).__init__(parent_wrock.logger, parent_wrock.run_options, name, thread_num, None)
        self.parent_wrock = parent_wrock
        self.stats = parent_wrock.stats
        self.timers = parent_wrock.timers
        self.blob_client = BlockBlobService(account_name=storage_account_name, sas_token=sas_token)

    @property
    def should_run(self):
        return self.parent_wrock.should_run

class TransferThreadPool():
    '''
    Container for the thread pool that does multi-threaded transfers to azure.
    The thread pool grows to meet demand, and shrinks if threads go idle for a long time.
    The number of simultaneous transfers per file is throttled by the TransferStateOneSource (tsos).
    Call queue_transfer() to submit the next item to the queue.  You must call activate() before first use.
    '''
    GLOBAL_MAX_THREADS = 256        # a safety in case of a coding error
    IDLE_THREAD_TIMEOUT_SECS = 120  # if a thread can't find any work after this long, it will terminate
    MIN_THREAD_COUNT = 2            # min size that our thread pool can shrink to

    def __init__(self, logger, parent_wrock):
        '''
        logger - the logger to use for error messages from the thread
        parent_wrock - child process wrock that we will use as the template for individual thread wrocks
        '''
        self.logger = logger
        self.parent_wrock = parent_wrock
        self.warned_max = False     # Gets flipped to True if we hit the max thread limit
        self.pool_lock = threading.Lock()
        self.pool_cond = threading.Condition(lock=self.pool_lock)
        self.transfer_list = collections.deque() # list of TransferRocks to transfer
        self._should_run = True
        self.thread_count = 0
        self.busy_threads = 0
        self.last_thread_num = 0
        self.storage_account_name = None
        self.sas_token = None
        self._active = False        # Set to True when activate() completes successfully

    def activate(self, storage_account_name, sas_token):
        '''
        Call this to activate the thread pool.
        We require an explicit call to activate to make an obvious pairing with the required call to deactivate.
        The deactivate is required to terminate any remaining idle threads.
        '''
        with self.pool_cond:
            if not self._active:
                self._should_run = True
                self.warned_max = False
                self.storage_account_name = storage_account_name
                self.sas_token = sas_token
                self._active = True

    def deactivate(self):
        'You must call this if activate() was called on the thread pool to terminate any threads.'
        with self.pool_cond:
            if not self._active:
                return
            self._active = False
            self._should_run = False
            self.pool_cond.notify_all()

    @property
    def active(self):
        'accessor'
        with self.pool_cond:
            return self._active

    def queue_transfer(self, trock):
        '''
        Queue a transfer request to the thread pool.  Grow the thread pool if necessary.
        trock - TransferRock for the request.
        '''
        with self.pool_cond:
            assert self._active
            self.transfer_list.append(trock)
            self._ondemand_more_threads()
            self.pool_cond.notify()  # notify a thread to pick up the transfer

    def _ondemand_more_threads(self):
        '''
        Check to see if we need more threads to deal with the amount of work queued up.
        More threads are needed if there is more work on the queue than there are total threads.
        TransferStateOneSource throttles the number of requests, so we generate enough threads accordingly.
        Called under pool_cond.
        '''
        assert self.thread_count >= self.busy_threads
        available_threads = self.thread_count - self.busy_threads
        additional_threads = len(self.transfer_list) - available_threads
        if additional_threads > 0:
            self._grow_pool(additional_threads)

    def _grow_pool(self, num):
        'Grow the thread pool by num threads.  Called under pool_cond.'
        if self.thread_count >= self.GLOBAL_MAX_THREADS:
            # Safety:  should never happen
            if not self.warned_max:
                self.logger.warning("Hit max thread count in transfer thread pool.")
                self.warned_max = True
            return
        num = min(num, self.GLOBAL_MAX_THREADS - self.thread_count)
        assert num > 0  # doc: since we just checked that thread_count is less than max threads above
        threads = [TransferThread(self, i+1) for i in range(self.last_thread_num, self.last_thread_num+num)]
        self.thread_count += num
        self.last_thread_num += num
        for thread in threads:
            thread.start()

    def can_shrink_pool_NL(self, secs_no_work):
        '''
        Called by a thread in the pool to see if it can terminate due to lack of work.
        Threads in the pool occasionally wake up on their own to look for work, so having
        a bunch of them lie around idle may hog a small number of cycles.  Most files are
        likely to be less than OTHER_SEGMENT_BYTES size so might as well keep unneeded
        threads from spinning. May shrink the pool down to a minimum of MIN_THREAD_COUNT threads.
        Called under pool_cond.
        secs_no_work - number of seconds that the thread failed to receive new work
        '''
        if secs_no_work > self.IDLE_THREAD_TIMEOUT_SECS:
            if self.thread_count > self.MIN_THREAD_COUNT:
                return True
        return False

    def thread_exit_NL(self):
        'Call to terminate a thread.  Caller must hold pool_cond.'
        assert self.thread_count > 0
        self.thread_count -= 1

    @property
    def should_run(self):
        'accessor'
        return self._should_run

class TransferThread(threading.Thread):
    '''
    A single thread responsible for writing blobs for regular files to the target.
    Threads wait on a condvar for writes to show up on a deque, and then execute those writes.
    A thread may terminate if it remains idle for a long time.
    '''

    def __init__(self, tpool, thread_id):
        '''
        tpool - TransferThreadPool that manages this thread
        thread_id - an int uniquifer for debugging
        '''
        super(TransferThread, self).__init__()
        self.tpool = tpool
        self.thread_id = thread_id
        assert tpool.storage_account_name is not None
        self.wrock = TransferThreadWRock(thread_id, tpool.parent_wrock, tpool.storage_account_name, tpool.sas_token)

    def run(self):
        '''
        Thread implementation: Wait for a TransferRock to show up on the queue, write the described blob, repeat.
        Thread only terminates if it's been idle for too long.
        '''
        tpool = self.tpool
        try:
            while tpool.should_run:
                trock = None
                try:
                    trock = self._wait_for_work()
                    if not trock:
                        return  # no work to do; terminate thread
                    self._do_work(trock)
                except BaseException as e:
                    err = "%s: transfer thread pool error" % self.wrock
                    exc_log(self.tpool.logger, logging.ERROR, err)
                    if (not isinstance(e, Exception)) or isinstance(e, TerminalError):
                        self.tpool.logger.error("%s: stop running for %s", self.wrock, exc_info_err())
                        if trock:
                            trock.tsos.see_exception(e)
                        raise
                finally:
                    if trock:
                        trock.tsos.transfer_complete()  # done with this transfer, can start next one
                        trock = None
        finally:
            with tpool.pool_cond:
                tpool.thread_exit_NL()

    def _wait_for_work(self):
        '''
        Wait for a TransferRock to show up on the queue.
        Returns a TransferRock if work was found.
        Returns None if we with to terminate this thread due to
        an extended idle period.
        '''
        tpool = self.tpool
        trock = None
        timeout = 10.0
        wrock = self.wrock
        busy = 0
        try:
            with tpool.pool_cond:
                tpool.busy_threads -= busy
                busy = 0
                secs_no_work = 0
                start_time = time.time()
                while tpool.should_run:
                    try:
                        trock = tpool.transfer_list.popleft()
                        # We found some work to do, go do it
                        busy = 1
                        tpool.busy_threads += busy
                        wrock.stats.stat_max('xfer_threads_busy_max', tpool.busy_threads)
                        break
                    except IndexError:
                        if tpool.can_shrink_pool_NL(secs_no_work):
                            break
                    except:
                        err = "transfer thread pool error (wait for work)"
                        exc_log(self.tpool.logger, logging.ERROR, err)
                    # wait for a bit and then retry, unless someone wakes us first
                    tpool.pool_cond.wait(timeout=timeout)
                    secs_no_work = elapsed(start_time)
        finally:
            tpool.busy_threads -= busy
        return trock

    def _do_work(self, trock):
        'Do a transfer using the TransferStateOneSource (tsos) and TransferRock (trock) that decribes this transfer.'
        tsos = trock.tsos
        if not tsos.should_run:
            # another thread for this file got an error, so skip doing this transfer
            return
        wrock = self.wrock
        tobj = tsos.tobj
        try:
            segment_bytes = trock.segment_bytes
            blob_data = unparse_segment_using_obid(wrock.run_options,
                                                   trock.blob_ob_id,
                                                   tobj,
                                                   tobj.ftype,
                                                   tobj.filehandle,
                                                   trock.offset,
                                                   segment_bytes)
            tsos.writerazure.write_blob_from_buffer(trock.blob_name, tobj, blob_data, wrock)
            wrock.stats.stat_add('gb_write', len(segment_bytes) / Size.GB)
        except BaseException as e:
            if isinstance(e, Exception):
                err = "%s transfer error" % wrock.name
                exc_log(wrock.logger, logging.ERROR, err)
            tsos.see_exception(e)
            if not isinstance(e, Exception):
                raise

class WriterAzure(Writer):
    '''
    How to write to Azure
    '''
    def __init__(self, *args, **kwargs):
        'See Writer'
        super(WriterAzure, self).__init__(*args, **kwargs)
        num_connections = 0
        if self._child_process_wrock:
            # We get the sas_token from the parent. Here, we assume that
            # the parent has already validated it.
            self._blob_client_default = None
            num_connections = TransferThreadPool.GLOBAL_MAX_THREADS + 1
        else:
            self._blob_client_default = BlockBlobService(account_name=self._storage_account_name, sas_token=self._sas_token)
            try:
                self._container_is_empty()
            except ServerRejectedAuthError:
                # When the Azure portal generates a secure access signature,
                # it gives it to the user with a question-mark prepended.
                # That is handy for pasting onto the end of a URI, but the
                # problem is the UI does not indicate that the question mark
                # is not part of the token, and nothing in the stack
                # automatically detects and handles this. Here, we attempt
                # to detect this and strip the leading quizzer if that helps.
                if self._sas_token.startswith('?'):
                    self._sas_token = self._sas_token.lstrip('?')
                    self._blob_client_default = BlockBlobService(account_name=self._storage_account_name, sas_token=self._sas_token)
                    self._container_is_empty()
            num_connections = self._wr_worker_thread_count

        self._bucket_obj = CLFSBucketObjAttrs()
        self.urllib3_adjust_connection_pools(num_connections)
        # Single, shared thread pool.  Must call activate() if you want to use it.
        # Only kicks in for larger files.
        self.tpool = TransferThreadPool(self.logger, self._child_process_wrock)

    # Max writer threads to consume per transfer (for a single file).
    # The challenge here is to select a number of threads that helps the write latency meet the read latency.
    # Larger values have diminishing rewards, and use up more memory.
    MAX_TRANSFER_THREADS = 4

    @classmethod
    def calc_num_threads(cls, file_size):
        '''
        Base the number of threads on the file size up to a max value.
        This is allowed to return numbers less than 1; the caller interprets
        such values as 1.
        '''
        if file_size <= CLFSSegment.FIRST_SEGMENT_BYTES:
            return 1
        base_size = file_size - CLFSSegment.FIRST_SEGMENT_BYTES
        seg_count = 1 + int(math.ceil(base_size / CLFSSegment.OTHER_SEGMENT_BYTES))
        return min(seg_count, cls.MAX_TRANSFER_THREADS)

    def activate_thread_pool(self):
        'You must call this to activate the thread pool if you want to enabled multi-threaded transfers for large files.'
        if self._child_process_wrock and self._run_options.transfer_pool_enable:
            self.tpool.activate(self._storage_account_name, self._sas_token)

    def deactivate_thread_pool(self):
        'You must call this to terminate threads in the thread pool if active_thread_pool was called.'
        self.tpool.deactivate()

    def wrock_init(self, wrock):
        'See base class'
        wrock.blob_client = BlockBlobService(account_name=self._storage_account_name, sas_token=self._sas_token)

    def write_blob_from_buffer(self, blob_name, tobj, data_buffer, wrock):
        '''
        Given buffer as string, bytes, or bytearray, write it as a blob.
        '''
        with wrock.timers.start('blob_compress') as timer:
            do_compression = self._run_options.compression_type == CLFSCompressionType.LZ4
            data_buffer = add_compression_header(data_buffer, wrock, do_compression)
        with wrock.timers.start('blob_write') as timer:
            count = 0
            blob_client = wrock.blob_client
            try:
                data_buffer = data_buffer if isinstance(data_buffer, bytes) else bytes(data_buffer)
                count = len(data_buffer)
                # Make a protected-access directly to _put_blob() to avoid
                # extra byte copies converting to and from a stream.
                # create_blob_from_bytes() converts its input from bytes to stream and calls create_blob_from_stream().
                # create_blob_from_stream() converts its input from stream to bytes and calls _put_blob().
                c = _MSApiCall(self, tobj, blob_client._put_blob, self._container_name, blob_name, data_buffer) # pylint: disable=protected-access
                c.doit()
            finally:
                timer.stop()
                wrock.stats.stat_update({'write_blob_secs' : timer.elapsed(),
                                         'write_blob_bytes' : count,
                                         'write_blob_count': 1,
                                        })

    def delete_blob(self, tobj, blob_name, wrock):
        '''
        Delete a blob.
        '''
        with wrock.timers.start('blob_delete'):
            self._delete_blob_internal(wrock.blob_client, tobj, blob_name)

    def _delete_blob_internal(self, blob_client, tobj, blob_name):
        '''
        Delete a blob. Never call this directly; call delete_blob().
        '''
        c = _MSApiCall(self, tobj, blob_client.delete_blob, self._container_name, blob_name)
        c.doit(not_found_ok=True)

    def read_blob(self, blob_name, tobj, wrock):
        '''
        Read an existing blob and return a Blob object.
        The bytes are Blob.content.
        '''
        with wrock.timers.start('blob_read'):
            return self._read_blob_internal(wrock.blob_client, blob_name, tobj)

    def _read_blob_internal(self, blob_client, blob_name, tobj):
        '''
        Read an existing blob and return a Blob object.
        The bytes are Blob.content.
        Never call this directly; call read_blob().
        '''
        c = _MSApiCall(self, tobj, blob_client.get_blob_to_bytes, self._container_name, blob_name)
        return c.doit(not_found_ok=True)

    def _container_is_empty(self):
        '''
        Return whether or not the target container is empty
        '''
        container_src = "container '%s'" % self._container_name
        # It seems like we should set num_results=1 here. That does not work;
        # setting that always returns an iterator with nothing in it, no matter
        # how many blobs are in the container.
        blob_client = self._blob_client_default
        c = _MSApiCall(self, container_src, blob_client.list_blobs, self._container_name)
        blob_iter = c.doit(not_found_ok=True)
        if blob_iter is None:
            self.logger.error("container '%s' is not accessible", container_src)
            raise ContainerTerminalError(container_src)
        try:
            return not bool(next(iter(blob_iter)))
        except StopIteration:
            return True

    def check_target_for_new_transfer(self):
        'See base class'
        logger = self.logger
        if not self._container_is_empty():
            logger.error("storage_account='%s' container='%s' is not empty", self._storage_account_name, self._container_name)
            raise SystemExit(1)

    def initialize_target_for_new_transfer(self, wrock):
        '''
        Create and store the initial contents for a new, empty container
        unparse_obj_handles returns the bytearray without the 4 byte header
        that indicates the obj compression and encryption type. This is
        done in write_blob_from_buffer() which sets the compression header
        after determining if the object needs to be compressed or not.
        Some objects may not be compressed even if the compression is enabled,
        if the size of the compressed data is larger than the size of the
        uncompressed data
        '''
        bucketObjName, tObj = generate_bucket_target_obj()
        extattrs = self._bucket_obj.extattr_dict
        ba = bytearray()
        btypeList = get_unparse_handle_list(tObj.ftype, Btype.BTYPE_SPECIAL)
        objba = unparse_obj_handles(self._run_options,
                                    tObj,
                                    ba,
                                    tObj.filehandle,
                                    objBtypeList=btypeList,
                                    targetObj=tObj,
                                    extattrDict=extattrs,
                                    ownerFh=tObj.filehandle)
        self.write_blob_from_buffer(bucketObjName, tObj, objba, wrock)

    def _flush_indir_blocks_in_list(self, wrock, tobj, ibdeque):
        while ibdeque:
            flush_item = ibdeque.popleft()
            ibid, blob_data = flush_item.get_block_id_and_data()
            blob_name = generate_bucket_name(ibid, tobj.filehandle, Btype.BTYPE_INDIRECT)
            self.write_blob_from_buffer(blob_name, tobj, blob_data, wrock)

    @staticmethod
    def _parse_obj(tobj, blob_data):
        '''
        Parse blob_data and return the parse dict.
        blob_data is already decompressed.
        This wrapper exists to improve testability.
        '''
        return parse_obj(tobj, blob_data)

    def check_target_transfer_in_progress(self, wrock):
        'See Writer'
        bucketObjName, tObj = generate_bucket_target_obj()
        blob = self.read_blob(bucketObjName, tObj, wrock)
        if not blob:
            raise SimpleError("bucketobj blob %s does not exist" % bucketObjName)
        if not blob.content:
            raise SimpleError("bucketobj blob %s has no data" % bucketObjName)
        content = strip_compression_header_and_decompress(blob.content, tObj, bucketObjName)
        # sanity check bucket obj data
        try:
            parseDict = self._parse_obj(tObj, content)
            if len(parseDict) != 1 or 'ExtAttrs' not in parseDict:
                raise SimpleError("bucketobj blob %s has bad data" % bucketObjName)
        except (NamedObjectError, SimpleError, TerminalError):
            self.logger.warning("bucket obj parse error %s", exc_info_err())
            raise
        except Exception:
            self.logger.warning("bucket obj parse error %s:\n%s", exc_info_err(), exc_stacklines())
            raise SimpleError("bucketobj blob %s does not parse: %s" % (bucketObjName, exc_info_err()))

    def directory_write(self, wrock, tobj, file_reader):
        '''
        See base class
        '''
        # read data for first segment -- first segment written out after all other segment
        size = 4
        first_segment_length_bytes = self._do_read(wrock, file_reader, tobj, size, 'read_entries', expect_exact=True, zero_ok=False)
        first_segment_length = struct.unpack(STRUCT_LE_U32, first_segment_length_bytes)[0]
        if not first_segment_length:
            raise TargetObjectError(tobj, "zero-length first segment for directory")
        first_segment_bytes = self._do_read(wrock, file_reader, tobj, first_segment_length, 'read_entries', expect_exact=True, zero_ok=False)
        size += first_segment_length

        # For clarity, we want the gb stat to update as we push out data.
        remaining_gb = (tobj.size / Size.GB)

        # initialize datablock list to contain inode-block afh
        block_manager = BlockListManager(wrock, tobj)
        block_manager.add_block(0, tobj.filehandle)
        # Since each iteration of the loop below unparses segments other than
        # the first segment (out_offset >= FIRST_SEGMENT_BYTES). The out_offset
        # starts at FIRST_SEGMENT_BYTES. out_offset is byte offset of the segment
        # in the object that is being written to container.
        out_offset = CLFSSegment.FIRST_SEGMENT_BYTES
        out_block_index = 1
        while True:
            if not wrock.should_run:
                raise AbortException
            segment_len_bytes = self._do_read(wrock, file_reader, tobj, 4, 'read_entries', expect_exact=True, zero_ok=True)
            if not segment_len_bytes:
                indir_list = block_manager.flush_indir_tree_final()
                self._flush_indir_blocks_in_list(wrock, tobj, indir_list)
                break   #all file data read
            size += 4
            segment_len = struct.unpack(STRUCT_LE_U32, segment_len_bytes[:4])[0]
            segment_bytes = self._do_read(wrock, file_reader, tobj, segment_len, 'read_entries', expect_exact=True, zero_ok=False)
            size += segment_len
            blob_ob_id, blob_data = unparse_segment(wrock.run_options,
                                                    tobj,
                                                    tobj.ftype,
                                                    tobj.filehandle,
                                                    out_offset,
                                                    segment_bytes)
            blob_name = generate_bucket_name(blob_ob_id, tobj.filehandle, Btype.BTYPE_SEGMENT)
            self.write_blob_from_buffer(blob_name, tobj, blob_data, wrock)
            indir_list = block_manager.add_block(out_block_index, blob_ob_id)
            self._flush_indir_blocks_in_list(wrock, tobj, indir_list)
            gb = len(segment_bytes) / Size.GB
            if remaining_gb:
                if gb >= remaining_gb:
                    wrock.stats.stat_update({'gb_write' : remaining_gb,
                                             'gb_read': remaining_gb,
                                            })
                    remaining_gb = 0.0
                else:
                    wrock.stats.stat_update({'gb_write' : gb,
                                             'gb_read' : gb,
                                            })
                    remaining_gb -= gb
            out_offset += CLFSSegment.DIR_OTHER_SEGMENT_BYTES
            out_block_index += 1
            wrock.stats_flush()
        tobj.size = size
        self._transfer_write_inode(wrock, tobj, block_manager, first_segment_bytes, remaining_gb, Btype.BTYPE_DIR, None)

    def transfer(self, wrock, tobj, reader):
        '''
        See base class
        '''
        try:
            block_manager = BlockListManager(wrock, tobj)
            block_manager.add_block(0, tobj.filehandle)
            if tobj.ftype == Ftype.LNK:
                try:
                    link_target = reader.readlink(tobj)
                    wrock.stats.stat_add('gb_read', tobj.size / Size.GB)
                except (NamedObjectError, TerminalError):
                    raise
                except Exception as e:
                    raise SourceObjectError(tobj, exc_info_err()) from e
                first_segment_bytes = bytes(link_target, encoding='utf_8')
                first_segment_len = len(first_segment_bytes)
                tobj.size = len(link_target)
                self._transfer_write_inode(wrock, tobj, block_manager, first_segment_bytes, first_segment_len / Size.GB, Btype.BTYPE_LNK, [tobj.first_backpointer])
            elif tobj.ftype == Ftype.REG:
                # For larger files, we use a multi-threaded transfer that allows us launch writes
                # asynchronously from the reads.
                with reader.open_input_file(tobj) as file_reader:
                    if self.tpool.active:
                        thread_count = self.calc_num_threads(tobj.size)
                        if thread_count > 1:
                            with ClfsReadAhead.get_readahead(wrock.logger, file_reader, tobj.size) as read_ahead_obj:
                                self._transfer_reg_mt(wrock, tobj, block_manager, thread_count, read_ahead_obj)
                            return
                    self._transfer_reg_st(wrock, tobj, block_manager, file_reader)
                # _transfer_reg_* writes the inode
            elif tobj.ftype in MKNOD_FTYPES:
                self._transfer_write_inode(wrock, tobj, block_manager, None, 0.0, Btype.ftype_to_btype(tobj.ftype), [tobj.first_backpointer])
            else:
                raise TargetObjectError(tobj, "unknown file type %s" % tobj.ftype)
        except (NamedObjectError, TerminalError):
            raise
        except Exception as e:
            exc_log(wrock.logger, logging.ERROR, "cannot transfer")
            raise TargetObjectError(tobj, "cannot transfer: %s" % exc_info_err()) from e

    def _transfer_write_inode(self, wrock, tobj, block_manager, first_segment_bytes, gb, btype, backPointerList):
        '''
        Write the inode for a transfer
        '''
        # Write the inode even if nlink > 1. Doing so stores the
        # blockpointers. Later, in reconcile(), we fix them up again.
        inode_blob_data = unparse_inode(wrock.run_options,
                                        tobj,
                                        first_segment_bytes,
                                        block_manager.get_direct_blocks(),
                                        block_manager.get_indirect_root_list(),
                                        backPointerList,
                                        tobj.source_name)
        # Write out the inode. If we will reconcile later, use
        # INODE_INPROGRESS_ID for the owner_id.
        # See INODE_INPROGRESS_ID in types.py for details.
        # for the root directory, add a "tmp" prefix to the name
        # fix the name in finalize phase
        if tobj.filehandle == wrock.run_options.rootfh:
            blob_name = generate_bucket_name(tobj.filehandle, FILEHANDLE_INODE_INPROGRESS, btype, nameprefix="tmp")
        elif tobj.will_reconcile():
            blob_name = generate_bucket_name(tobj.filehandle, FILEHANDLE_INODE_INPROGRESS, btype)
        else:
            blob_name = generate_bucket_name(tobj.filehandle, tobj.first_backpointer, btype)
        self.write_blob_from_buffer(blob_name, tobj, inode_blob_data, wrock)
        wrock.stats.stat_add('gb_write', gb)

    def _transfer_reg_st(self, wrock, tobj, block_manager, file_reader):
        '''
        Perform transfer() on tobj with type REG using a single thread
        '''
        first_segment_bytes = self._do_read(wrock, file_reader, tobj, CLFSSegment.FIRST_SEGMENT_BYTES, 'read_st')
        wrock.stats.stat_add('gb_read', len(first_segment_bytes) / Size.GB)
        first_segment_len = len(first_segment_bytes)
        out_offset = CLFSSegment.FIRST_SEGMENT_BYTES
        # if file length <= FIRST_SEGMENT_BYTES then seek(out_offset, io.SEEK_CUR)
        # takes us beyond EOF in the input file, and the loop below
        # is a no-op
        out_block_index = 1
        size = first_segment_len
        while True:
            if not wrock.should_run:
                break
            segment_bytes = self._do_read(wrock, file_reader, tobj, CLFSSegment.OTHER_SEGMENT_BYTES, 'read_st')
            if not segment_bytes:
                indir_list = block_manager.flush_indir_tree_final()
                self._flush_indir_blocks_in_list(wrock, tobj, indir_list)
                break   #all file data read
            wrock.stats.stat_add('gb_read', len(segment_bytes) / Size.GB)
            size += len(segment_bytes)
            # out_offset in output file is used only to be passed into unparse_segment for creation
            # databack block -- the size header is set from len(segment_bytes).
            blob_ob_id = ObCacheId()
            blob_name = generate_bucket_name(blob_ob_id, tobj.filehandle, Btype.BTYPE_SEGMENT)
            blob_data = unparse_segment_using_obid(wrock.run_options,
                                                   blob_ob_id,
                                                   tobj,
                                                   tobj.ftype,
                                                   tobj.filehandle,
                                                   out_offset,
                                                   segment_bytes)
            self.write_blob_from_buffer(blob_name, tobj, blob_data, wrock)
            wrock.stats.stat_add('gb_write', len(segment_bytes) / Size.GB)
            indir_list = block_manager.add_block(out_block_index, blob_ob_id)
            self._flush_indir_blocks_in_list(wrock, tobj, indir_list)
            # updating this either sets out_offset either at EOF or start of next segment
            out_offset += len(segment_bytes)
            out_block_index += 1
            wrock.stats_flush()
        if not wrock.should_run:
            raise AbortException
        tobj.size = size
        self._transfer_write_inode(wrock, tobj, block_manager, first_segment_bytes, first_segment_len / Size.GB, Btype.BTYPE_REG, [tobj.first_backpointer])

    def _transfer_reg_mt(self, wrock, tobj, block_manager, max_writers, read_ahead_obj):
        '''
        Perform transfer() on tobj with type REG using multiple threads
        '''
        first_segment_bytes = self._do_read(wrock, read_ahead_obj, tobj, CLFSSegment.FIRST_SEGMENT_BYTES, 'read_mt')
        first_segment_len = len(first_segment_bytes)
        wrock.stats.stat_add('gb_read', first_segment_len / Size.GB)
        out_offset = CLFSSegment.FIRST_SEGMENT_BYTES
        # if file length <= FIRST_SEGMENT_BYTES then seek(out_offset, io.SEEK_CUR)
        # takes us beyond EOF in the input file, and the loop below
        # is a no-op
        out_block_index = 1
        size = first_segment_len
        tsos = TransferStateOneSource(self, wrock, tobj, self.tpool, max_writers)  # Manages state for this transfer
        while True:
            if not wrock.should_run:
                break
            segment_bytes = self._do_read(wrock, read_ahead_obj, tobj, CLFSSegment.OTHER_SEGMENT_BYTES, 'read_mt')
            if not segment_bytes:
                indir_list = block_manager.flush_indir_tree_final()
                self._flush_indir_blocks_in_list(wrock, tobj, indir_list)
                break   #all file data read
            wrock.stats.stat_add('gb_read', len(segment_bytes) / Size.GB)
            size += len(segment_bytes)
            # out_offset in output file is used only to be passed into unparse_segment for creation
            # databack block -- the size header is set from len(segment_bytes).
            blob_ob_id = ObCacheId()
            blob_name = generate_bucket_name(blob_ob_id, tobj.filehandle, Btype.BTYPE_SEGMENT)
            trock = TransferRock(blob_ob_id, blob_name, segment_bytes, out_offset, tsos)
            tsos.queue_transfer(trock)  # If the queue is too full, this blocks until we can do the transfer
            if not tsos.should_run:
                break
            indir_list = block_manager.add_block(out_block_index, blob_ob_id)
            self._flush_indir_blocks_in_list(wrock, tobj, indir_list)
            # updating this either sets out_offset either at EOF or start of next segment
            out_offset += len(segment_bytes)
            out_block_index += 1
            wrock.stats_flush()
        if not wrock.should_run:
            tsos.should_run = False
            raise AbortException
        tobj.size = size
        self._transfer_write_inode(wrock, tobj, block_manager, first_segment_bytes, first_segment_len / Size.GB, Btype.BTYPE_REG, [tobj.first_backpointer])
        read_ahead_obj_err = None
        try:
            read_ahead_obj.safe_stop()
        except AttributeError:
            # a non-readahead reader has no safe_stop() method
            pass
        except TerminalError:
            raise
        except NamedObjectError as e:
            read_ahead_obj_err = e
        except Exception as e:
            read_ahead_obj_err = e
        tsos.wait_all_complete()
        # Wait for any remaining writers to complete and terminate the threads
        if tsos.target_object_error is not None:
            raise tsos.target_object_error
        if read_ahead_obj_err is None:
            read_ahead_obj_err = getattr(read_ahead_obj, 'error', None)
        if read_ahead_obj_err is not None:
            if isinstance(read_ahead_obj_err, (NamedObjectError, TerminalError)):
                raise read_ahead_obj_err
            raise SourceObjectError(tobj, str(read_ahead_obj_err))

    def find_inode_blob_name(self, wrock, tobj):
        '''
        Scan the container looking for the newest version of the inode blob for tobj.
        '''
        list_manager = ObjectListManager(tobj.filehandle)
        self._scan_container(wrock, tobj, list_manager)
        return list_manager.best_name

    def _scan_container(self, wrock, tobj, list_manager):
        '''
        Scan the container using a list_manager (ObjectListManager or a subclass)
        '''
        list_blobs_kwargs = {'prefix' : list_manager.prefix}
        while True:
            c = _MSApiCall(self, tobj, wrock.blob_client._list_blobs, self._container_name, **list_blobs_kwargs) # pylint: disable=protected-access
            res = c.doit()
            if not res:
                return
            list_blobs_kwargs['marker'] = res.next_marker
            for blob in res:
                list_manager.see_name(wrock, blob.name)
            if not res.next_marker:
                return

    def reconcile(self, wrock, tobj, reader):
        'See Writer'
        try:
            if tobj.filehandle == wrock.run_options.rootfh:
                # root obj is given an invalid temporary file handle until the finalize phase
                intermediate_blob_name = generate_bucket_name(tobj.filehandle, FILEHANDLE_INODE_INPROGRESS, Btype.BTYPE_DIR, nameprefix="tmp")
            else:
                intermediate_blob_name = generate_bucket_name(tobj.filehandle, FILEHANDLE_INODE_INPROGRESS, Btype.ftype_to_btype(tobj.ftype))

            blob = self.read_blob(intermediate_blob_name, tobj, wrock)
            if blob:
                inode_blob_name = intermediate_blob_name
            else:
                # Most likely we finished the transfer for an object with
                # one or more parent scans unfinished. Later, when we
                # determined we must reconcile the object, we discovered
                # it through a different parent, so the previously
                # reconciled first_backpointer does not match the current one.
                # List the container to find it.
                newest_blob_name = self.find_inode_blob_name(wrock, tobj)
                if not newest_blob_name:
                    raise TargetObjectError(tobj, "inode blob not found for reconcile", blob_name=intermediate_blob_name)
                blob = self.read_blob(newest_blob_name, tobj, wrock)
                if not blob:
                    raise TargetObjectError(tobj, "inode blob not readable", blob_name=newest_blob_name)
                inode_blob_name = newest_blob_name
            if not blob.content:
                raise TargetObjectError(tobj, "inode blob empty during reconcile", blob_name=inode_blob_name)
            inode_blob_data, owner_id = obj_reconcile(wrock, tobj, blob.content, 'blob', inode_blob_name)
            if tobj.filehandle == wrock.run_options.rootfh:
                # The reconcile is idempotent. There is no partial write of
                # a small blob. The database will never return a false positive
                # for whether or not this work is done. So, it's safe to reuse
                # the intermediate name here.
                new_blob_name = intermediate_blob_name
            else:
                # Write the final blob with a newer version number than the
                # intermediate blob. If the delete fails the container is still
                # readable that way.
                new_blob_name = generate_bucket_name(tobj.filehandle, owner_id, Btype.ftype_to_btype(tobj.ftype), version=tobj.reconcile_vers)
            self.write_blob_from_buffer(new_blob_name, tobj, inode_blob_data, wrock)
        except (NamedObjectError, TerminalError):
            raise
        except Exception as e:
            exc_log(wrock.logger, logging.ERROR, "cannot reconcile")
            raise TargetObjectError(tobj, "cannot reconcile: %s" % exc_info_err()) from e

    def cleanup(self, wrock, tobj):
        'See base class'
        if tobj.filehandle != wrock.run_options.rootfh:
            blob_name = generate_bucket_name(tobj.filehandle, FILEHANDLE_INODE_INPROGRESS, Btype.ftype_to_btype(tobj.ftype))
            self.delete_blob(tobj, blob_name, wrock)
        list_manager = ObjectListTracker(tobj.filehandle)
        self._scan_container(wrock, tobj, list_manager)
        for blob_name in list_manager.older_names:
            self.delete_blob(tobj, blob_name, wrock)

    def _update_extattrs_in_bucket_obj(self, wrock, log_file_list):
        '''
        update bucket obj with extattrs with key logX where X is 1 <= X <= number of log files.
        the attribute value is the name of the log file in the bucket.
        '''
        bucketObjName, tObj = generate_bucket_target_obj()
        blob = self.read_blob(bucketObjName, tObj, wrock)
        if not blob:
            raise TargetObjectError(tObj, "bucketobj blob does not exist", blob_name=bucketObjName)
        if not blob.content:
            raise TargetObjectError(tObj, "bucketobj blob has no data", blob_name=bucketObjName)
        content = strip_compression_header_and_decompress(blob.content, tObj, bucketObjName)
        # parse extended attributes
        try:
            parseDict = self._parse_obj(tObj, content)
            if len(parseDict) != 1 or 'ExtAttrs' not in parseDict:
                raise SimpleError("bucketobj blob %s has bad data" % bucketObjName)
        except Exception:
            self.logger.warning("bucket obj parse error %s:\n%s", exc_info_err(), exc_stacklines())
            raise SimpleError("bucketobj blob %s does not parse: %s" % (bucketObjName, exc_info_err()))

        index = 1
        extattr_dict = parseDict['ExtAttrs']
        for logfile_name in log_file_list:
            key = "logfile%03u" % (index)
            extattr_dict[key] = logfile_name
            index += 1
        ba = bytearray()
        btypeList = get_unparse_handle_list(tObj.ftype, Btype.BTYPE_SPECIAL)
        objba = unparse_obj_handles(self._run_options,
                                    tObj,
                                    ba,
                                    tObj.filehandle,
                                    objBtypeList=btypeList,
                                    targetObj=tObj,
                                    extattrDict=extattr_dict,
                                    ownerFh=tObj.filehandle)
        self.write_blob_from_buffer(bucketObjName, tObj, objba, wrock)

    def finalize(self, wrock, root_fh, root_tobj, logfile_dir):
        'See base class'
        log_file_list = sorted([log_file for log_file in os.listdir(logfile_dir) if log_file.endswith('.txt')])
        # write log files to container
        self._save_files_to_container([os.path.join(logfile_dir, x) for x in log_file_list])
        # add extattrs to bucket obj
        self._update_extattrs_in_bucket_obj(wrock, log_file_list)
        # root directory inode is under a temporary name in the container to ensure that we cannot
        # mount root dir until we are done. Copy contents to the correct name for root dir inode
        # and delete object with temporary name.
        tmp_root_blob_name = generate_bucket_name(root_fh, FILEHANDLE_INODE_INPROGRESS, Btype.BTYPE_DIR, nameprefix="tmp")
        root_blob_name = generate_bucket_name(root_fh, root_fh, Btype.BTYPE_DIR, version=root_tobj.reconcile_vers)
        try:
            root_blob_data = self.read_blob(tmp_root_blob_name, root_tobj, wrock)
        except NamedObjectError:
            raise
        except Exception as e:
            raise TargetObjectError(root_tobj, exc_info_err(), blob_name=tmp_root_blob_name) from e

        if root_blob_data:
            # create root dir inode with correct name
            root_blob_data = self.read_blob(tmp_root_blob_name, root_tobj, wrock)
            if not root_blob_data:
                raise TargetObjectError(root_tobj, 'root directory blob not found', blob_name=root_blob_name)
            try:
                content_bytes = strip_compression_header_and_decompress(root_blob_data.content, root_tobj, root_blob_name)
                self.write_blob_from_buffer(root_blob_name, root_tobj, content_bytes, wrock)
            except NamedObjectError:
                raise
            except Exception as e:
                raise TargetObjectError(root_tobj, exc_info_err()) from e
        else:
            # sanity check that root obj was created before temporary name was deleted
            root_blob_data = self.read_blob(root_blob_name, root_tobj, wrock)
            if not root_blob_data:
                raise TargetObjectError(root_tobj, "root inode not found while finalizing", blob_name=root_blob_name)

        # at this point, we have root dir with correct name in bucket. delete temporary root dir inode blob
        self.delete_blob(root_tobj, tmp_root_blob_name, wrock)

    def _save_files_to_container(self, file_paths):
        '''
        file_paths is a list of files readable locally.
        Upload each to the target container, giving the blob the same
        name as the file.
        '''
        blob_client = self._blob_client_default
        filenames = {os.path.split(file_path)[1] for file_path in file_paths}
        if len(filenames) != len(file_paths):
            raise SimpleError("file_paths contains duplicate filenames")
        for file_path in file_paths:
            blob_name = os.path.split(file_path)[1]
            c = _MSApiCall(self, file_path, blob_client.create_blob_from_path, self._container_name, blob_name, file_path)
            c.doit()

    @staticmethod
    def urllib3_adjust_connection_pools(thread_count):
        '''
        Some third-party modules bury requests and urrllib3 initialization
        so it is difficult to tamper with directly. urllib3 itself does
        not provide any mechanism to adjust the default pool size, but
        it does allow us to adjust the pool classes used.
        Here, we tweak the maximum connection pool size to avoid
        repeatedly creating and discarding connections by overloading
        the pool classes. We also overload ConnectionCls to our own HTTPConnection.
        https://urllib3.readthedocs.io/en/latest/advanced-usage.html#customizing-pool-behavior
        '''
        maxsize = thread_count + 2 # + 2: 1 for main, 1 for extra margin

        sndbuf = CLFSSegment.MAX_SEGMENT_BYTES + Size.MB

        socket_options = [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
                          (socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf),
                         ]

        # http
        class HTTPConnection(urllib3.connection.HTTPConnection):
            default_socket_options = socket_options
        class _HTTPConnectionPool(urllib3.connectionpool.HTTPConnectionPool):
            clfsload_pool_maxsize = 10
            ConnectionCls = HTTPConnection
            def __init__(self, *args, **kwargs):
                maxsize = kwargs.get('maxsize', 10)
                kwargs['maxsize'] = max(maxsize, self.clfsload_pool_maxsize)
                super(_HTTPConnectionPool, self).__init__(*args, **kwargs)
        _HTTPConnectionPool.clfsload_pool_maxsize = maxsize
        urllib3.poolmanager.pool_classes_by_scheme['http'] = _HTTPConnectionPool

        # https
        class HTTPSConnection(urllib3.connection.HTTPSConnection):
            default_socket_options = socket_options
        class _HTTPSConnectionPool(urllib3.connectionpool.HTTPSConnectionPool):
            clfsload_pool_maxsize = 10
            ConnectionCls = HTTPSConnection
            def __init__(self, *args, **kwargs):
                maxsize = kwargs.get('maxsize', 10)
                kwargs['maxsize'] = max(maxsize, self.clfsload_pool_maxsize)
                super(_HTTPSConnectionPool, self).__init__(*args, **kwargs)
        _HTTPSConnectionPool.clfsload_pool_maxsize = maxsize
        urllib3.poolmanager.pool_classes_by_scheme['https'] = _HTTPSConnectionPool
