#
# clfsload/writer.py
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

import os

from clfsload.types import NamedObjectError, ReaderWriterBase, TargetObjectError, TerminalError, WriteFileError
from clfsload.util import Size, exc_info_err, exc_stack

class WriteFileBase():
    'Base class for local file writers - things that write to non-DB files in the local_state_dir'
    def __init__(self, path, logger, buffersize=None):
        '''
        buffersize: None for default. Otherwise, a value suitable for the
                    buffering arg to the Python open() builtin.
        '''
        self._logger = logger
        self._path = path
        self._buffersize = buffersize if buffersize is not None else self.buffersize_default

    buffersize_default = Size.MB

    def __repr__(self):
        return "<%s,%s>" % (self.__class__.__name__, self._path)

    def __str__(self):
        return repr(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.flush_and_close()

    def __del__(self):
        self.flush_and_close()

    @property
    def path(self):
        return self._path

    def seek(self, offset, whence):
        pass

    def truncate(self):
        pass

    def write(self, buffer):
        'write a single bytes-like buffer'
        # noop in base class

    def writev(self, buffers, wrock=None): # pylint: disable=unused-argument
        'write a sequence of bytes-like objects'
        for buffer in buffers:
            self.write(buffer)

    def flush(self):
        'Flush anything buffered'
        # noop -- subclasses may define flush() if that makes sense

    def flush_and_close(self):
        pass

class WriteFileAutoSync(WriteFileBase):
    'Wraps a Python file object such as io.BufferedWriter'
    _any_flush_failed = ''

    def __init__(self, *args, **kwargs):
        super(WriteFileAutoSync, self).__init__(*args, **kwargs)
        self._file = open(self._path, mode='wb+', buffering=self._buffersize)
        self._flush_failed = False

    def seek(self, offset, whence):
        return self._file.seek(offset, whence)

    def truncate(self):
        self._file.truncate()

    def write(self, buffer):
        'write a single bytes-like buffer'
        self._file.write(buffer)

    def flush(self):
        'Flush anything buffered'
        self._fsync()

    def _fsync(self):
        'Flush and fsync'
        if self._flush_failed:
            raise WriteFileError("%s flush failed previously" % self)
        if self._file is None:
            return
        try:
            self._flush_real()
        except BaseException as e:
            self._flush_failed = True
            err = "cannot flush %s: %s" % (self, exc_info_err())
            self._logger.error("%s\n%s\n%s", err, exc_stack(), err)
            if isinstance(e, Exception):
                raise WriteFileError(err) from e
            raise

    def _flush_real(self):
        '''
        Core of _fsync() with no error-handling
        '''
        self._file.flush()
        os.fsync(self._file.fileno())

    def flush_and_close(self):
        '''
        Flush and fsync, then close
        '''
        # Even if we have already closed, raise an error if
        # the flush failed previously. The caller does not
        # know that we are already closed and must see the error.
        if not self._flush_failed:
            self._fsync()
        if self._file is not None:
            self._file.close()
            self._file = None
        if self._flush_failed:
            raise WriteFileError("%s flush failed previously" % self)

class Writer(ReaderWriterBase):
    '''
    Base class for writers
    '''
    def __init__(self,
                 command_args,
                 logger,
                 run_options,
                 local_state_path,
                 target_storage_account,
                 target_container,
                 sas_token,
                 worker_thread_count,
                 child_process_wrock=None):
        '''
        Set up the object to be ready to use.
        Do not do any target accesses yet -- the first operation
        to do so is exactly one of initialize_target_for_new_transfer()
        or check_target_transfer_in_progress().
        command_args is provided to support additional arguments
        specific to tests that use various Writer subclasses.
        Do not reference command_args here.
        When reconstituting a subclass through a parent/child relationship,
        command_args is a string that comes from command_args_to_json().
        '''
        super(Writer, self).__init__(command_args, logger, run_options, child_process_wrock=child_process_wrock)
        self._local_state_path = local_state_path
        self._storage_account_name = target_storage_account
        self._container_name = target_container
        self._sas_token = str(sas_token).strip()
        self._wr_worker_thread_count = worker_thread_count

    # Tests that replace writer_class set this to False to
    # indicate that the destination is not really written.
    WRITES_CLFS = True

    def reconstitute_args(self):
        '''
        Return a tuple that may be passed to __init__ capturing
        all of the args past command_args and logger.
        '''
        return (self._local_state_path,
                self._storage_account_name,
                self._container_name,
                self._sas_token,
                self._wr_worker_thread_count,
               )

    @staticmethod
    def supports_hardlinks():
        return True

    def wrock_init(self, wrock):
        '''
        Initialize wrock (types.WRock subclass) for use with this writer.
        '''
        # Nothing to do here in the base class

    def check_target_for_new_transfer(self):
        '''
        Called early to verify that the target is accessible and
        in the correct state to begin a new transfer.
        '''
        # Nothing to do here in the base class

    def initialize_target_for_new_transfer(self, wrock):
        '''
        Verify that the target container is in a state suitable for starting
        a new transfer (eg empty). Write start-of-transfer blobs here.
        '''
        # Nothing to do here in the base class

    def check_target_transfer_in_progress(self, wrock):
        '''
        Verify that the target container has a transfer in progress.
        This operation is performed at start-of-day for restarted
        transfers. It verifies the output of initialize_target_for_new_transfer().
        '''
        # Nothing to do here in the base class

    def directory_write(self, wrock, tobj, file_reader):
        '''
        Write a single directory object.
        wrock: Writer rock. Stringified, it provides a useful prefix for log messages.
             wrock.stats is updated as necessary by this operation.
        tobj: The directory TargetObject
        file_reader: Instance of a subclass of ReadFileBase used to read the entries file
        '''
        # Nothing to do here in the base class

    def transfer(self, wrock, tobj, reader):
        '''
        Transfer a single non-directory object. Update wrock.stats as necessary.
        wrock: Writer rock. Stringified, it provides a useful prefix for log messages.
             wrock.stats is updated as necessary by this operation.
        tobj: The directory TargetObject
        reader: instance of Reader object to use
        When writing the inode, use tobj.nlink_effective() for nlink, not tobj.nlink.
        '''
        # Nothing to do here in the base class

    def reconcile(self, wrock, tobj, reader):
        '''
        We now have enough information to write a non-directory inode.
        Do so. To determine the correct nlink for the inode, use
        tobj.nlink_effective(). To get a list of backpointers for
        this inode, use tobj.backpointer_list_generate().
        '''
        # Nothing to do here in the base class

    def cleanup(self, wrock, tobj):
        '''
        An inode was previously reconciled. Remove the intermediate blob.
        '''
        # Nothing to do here in the base class

    def finalize(self, wrock, root_fh, root_tobj, logfile_dir):
        '''
        Do the work of the finalize phase.
        root_fh, root_tobj : root dir fh/targetobj
        logfile_dir : directory containing log files
        '''
        # Nothing to do here in the base class

    local_file_writer = WriteFileAutoSync

    def activate_thread_pool(self):
        'Give any sub-classes of writer a chance to initialize thread pools.'
        # Nothing to do in the base class

    def deactivate_thread_pool(self):
        'Give any sub-classes of writer a chance to destroy thread pools.'
        # Nothing to do in the base class

    @staticmethod
    def _do_read(wrock, read_obj, tobj, length, timer_name, expect_exact=False, zero_ok=True):
        '''
        Wrap read_obj.read(length) with appropriate exception handling.
        '''
        try:
            with wrock.timers.start(timer_name):
                ret = read_obj.read(length)
        except (NamedObjectError, TerminalError):
            raise
        except Exception as e:
            txt = e.__class__.__name__
            tmp = str(e)
            if tmp:
                txt += ' '
                txt += tmp
            raise TargetObjectError(tobj, txt) from e
        if zero_ok:
            return ret
        if expect_exact:
            if len(ret) != length:
                msg = "%s=%s expected=%d read %d bytes instead" % (read_obj.__class__.__name__, read_obj, length, len(ret))
                raise TargetObjectError(tobj, msg)
        return ret
