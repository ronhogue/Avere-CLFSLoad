#
# clfsload/reader.py
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
Reader is the base class for reading from the source.
At this time, ReaderPosix is the only reader that is implemented.

ReadPosix uses local filesystem operations to scan the source.
'''

import logging
import os
import queue
import stat
import threading

from clfsload.dryrunhelper import DryRunHelper
from clfsload.parse import CLFSSegment
from clfsload.stypes import CLFSLoadThread, ClassExportedBase, FILEHANDLE_NULL, FILEHANDLE_ROOT, Ftype, \
                            NamedObjectError, ReaderInfo, ReaderWriterBase, SourceObjectError, \
                            TargetObj, TargetObjState
from clfsload.util import Size, exc_info_err, exc_info_name, exc_log

class ReadFileBase(ClassExportedBase):
    'Base class for all input readers - things that read files/objects from the input'
    def __init__(self, path):
        self._path = path
        self._file = None

    buffersize_default = Size.MB

    def __repr__(self):
        return "<%s,%s>" % (self.__class__.__name__, self._path)

    def __str__(self):
        return repr(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    @property
    def path(self):
        'accessor'
        return self._path

    def seek(self, offset, whence):
        pass

    def read(self, length): # pylint: disable=no-self-use,unused-argument
        '''
        Read up to length bytes. If the returned buffer contains fewer
        bytes, the caller may assume that EOF is reached.
        '''
        return bytes(0)

    def close(self):
        pass

class ReadFilePosix(ReadFileBase):
    'POSIX file reader'
    _OPEN_MODE = 'rb' # tests may overload this

    def __init__(self, *args, **kwargs):
        super(ReadFilePosix, self).__init__(*args, **kwargs)
        self._file = open(self._path, mode=self._OPEN_MODE, buffering=0)

    def seek(self, offset, whence):
        'see base class'
        return self._file.seek(offset, whence)

    def read(self, length):
        'see base class'
        ret = self._file.read(length)
        r2 = ret
        remain = len(ret) - length
        if remain and r2:
            ret = bytearray(ret)
            while r2 and remain:
                r2 = self._file.read(remain)
                if r2:
                    ret.extend(r2)
                    remain = len(ret) - length
        return ret

    def close(self):
        'see base class'
        if self._file is not None:
            self._file.close()
            self._file = None

class Reader(ReaderWriterBase):
    'Base class for objects that know how to read from the source'
    def __init__(self, command_args, logger, run_options, src, child_process_wrock=None):
        '''
        Do not reference command_args here.
        When reconstituting a subclass through a parent/child relationship,
        command_args is a string that comes from command_args_to_json().
        '''
        super(Reader, self).__init__(command_args, logger, run_options, child_process_wrock=child_process_wrock)
        self._src = src
        self._uid = os.getuid()

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, self._src)

    def __repr__(self):
        return "<%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self._src)

    @property
    def source_path(self):
        return self._src

    def target_obj_for_root_get(self):
        '''
        Generate and return a TargetObj for the root of the source.
        '''
        raise NotImplementedError("%s did not implement this method" % self.__class__.__name__)

    def opendir(self, dirpath):
        '''
        Return an opaque object that may be used to iterate through the
        contents of the directory identified by dirname. If dirname
        is None, use the src root. Otherwise, the dirname is named
        according to whatever is returned from getnextfromdir().
        '''
        raise NotImplementedError("%s did not implement this method" % self.__class__.__name__)

    def getnextfromdir(self, directory):
        '''
        Given a directory returned from opendir(), return a ReaderInfo
        for the next entry, or None if there are no more entries.
        '''
        raise NotImplementedError("%s did not implement this method" % self.__class__.__name__)

    def readerinfo_from_source_path(self, source_path, source_name):
        '''
        Return ReaderInfo for the specified source.
        '''
        raise NotImplementedError("%s did not implement this method" % self.__class__.__name__)

    _dry_run_helper_class = DryRunHelper

    def dry_run(self, thread_count, thread_shared_state, preserve_hardlinks, dry_run_helper_class=None):
        'Perform a dry run and return DryRunResult.'
        dry_run_helper_class = dry_run_helper_class if dry_run_helper_class else self._dry_run_helper_class
        helper = dry_run_helper_class(self, thread_count, thread_shared_state, preserve_hardlinks)
        return helper.run()

    def readable(self, ri):
        '''
        Examine ri (ReaderInfo) and determine if it can be read.
        '''
        # user is root can always read
        # UID trumps everything else, so if owner has read and user is owner, we can read
        if (self._uid == 0) or ((ri.ostat.st_uid == self._uid) and (ri.ostat.st_mode & stat.S_IRUSR)):
            # Assume readable
            return True
        try:
            with open(ri.path, 'rb') as f:
                f.read(1)
                return True
        except PermissionError:
            return False

class ReaderPosix(Reader):
    '''
    Read from local POSIX-compliant filesystem.
    This uses local filesystem operations to scan the source,
    which can be any rooted subtree. For example, this could
    be on the local disk, or it could be a remote filesystem
    such as NFS.
    '''
    def __init__(self, *args, **kwargs):
        super(ReaderPosix, self).__init__(*args, **kwargs)
        try:
            self._source_root_stat = os.lstat(self._src)
        except FileNotFoundError as e:
            raise SystemExit("source_root '%s' does not exist" % self._src) from e
        if not stat.S_ISDIR(self._source_root_stat.st_mode):
            raise SystemExit("source_root '%s' is not a directory" % self._src)
        self._getnextfromdir__dostat = self._getnextfromdir__dostat__both

    @staticmethod
    def _ftype_from_mode(st_mode, name):
        'Given st_mode from a stat result, return the Ftype'
        if stat.S_ISREG(st_mode):
            return Ftype.REG
        if stat.S_ISDIR(st_mode):
            return Ftype.DIR
        if stat.S_ISBLK(st_mode):
            return Ftype.BLK
        if stat.S_ISCHR(st_mode):
            return Ftype.CHR
        if stat.S_ISLNK(st_mode):
            return Ftype.LNK
        if stat.S_ISSOCK(st_mode):
            return Ftype.SOCK
        if stat.S_ISFIFO(st_mode):
            return Ftype.FIFO
        err = "unknown ftype for st_mode=0x%x" % st_mode
        raise SourceObjectError(name, err)

    def _target_obj_from_stat(self, filehandle, st, parent_filehandle, source_path):
        ftype = self._ftype_from_mode(st.st_mode, source_path)
        tobj = TargetObj(filehandle=filehandle,
                         ftype=ftype,
                         mode=st.st_mode,
                         nlink=0, # Updated after we scan
                         mtime=st.st_mtime,
                         ctime=st.st_ctime,
                         atime=st.st_atime,
                         size=st.st_size,
                         uid=st.st_uid,
                         gid=st.st_gid,
                         dev=st.st_rdev,
                         first_backpointer=parent_filehandle,
                         state=TargetObjState.PENDING,
                         source_inode_number=st.st_ino,
                         source_path=source_path,
                         pre_error_state=TargetObjState.PENDING)
        return tobj

    def target_obj_for_root_get(self):
        '''
        See Reader
        '''
        tobj = self._target_obj_from_stat(FILEHANDLE_ROOT, self._source_root_stat, FILEHANDLE_NULL, self._src)
        return tobj

    class _ScandirWrapper():
        'Wrap os.scandir() along with the path'
        def __init__(self, sd, path):
            self.sd = sd
            self.path = path

    def opendir(self, dirpath):
        'See Reader'
        dirpath = dirpath if dirpath is not None else self._src
        try:
            sd = os.scandir(dirpath)
        except FileNotFoundError as e:
            raise SourceObjectError(dirpath, "cannot open directory: %s" % exc_info_name()) from e
        except Exception as e:
            raise SourceObjectError(dirpath, "cannot open directory: %s" % exc_info_err()) from e
        return self._ScandirWrapper(sd, dirpath)

    def getnextfromdir(self, directory):
        '''
        See Reader
        directory is returned from opendir() - a _ScandirWrapper
        '''
        try:
            ent = next(directory.sd)
        except StopIteration:
            return None
        except Exception as e:
            msg = "cannot get next directory entry: %s" % exc_info_err()
            raise SourceObjectError(directory.path, msg) from e
        try:
            st = self._getnextfromdir__dostat(ent)
        except Exception as e:
            msg = "cannot stat: %s %s" % (e.__class__.__name__, e)
            raise SourceObjectError(ent.path, msg) from e
        return self._readerinfo_from_stat(ent.path, ent.name, st)

    def _getnextfromdir__dostat__both(self, ent):
        'Peform the stat operation for getnextfromdir'
        # Hard to say if using ent.stat() is getting us any performance
        # gain or not.
        try:
            return ent.stat(follow_symlinks=False)
        except TypeError:
            # Assume that this implementation does not like follow_symlinks.
            # Fall back to os.lstat().
            pass
        # From now on, do not bother attempting ent.stat().
        self._getnextfromdir__dostat = self._getnextfromdir__dostat__lstat
        return os.lstat(ent.path)

    @staticmethod
    def _getnextfromdir__dostat__lstat(ent):
        'Peform the stat operation for getnextfromdir using only os.lstat'
        return os.lstat(ent.path)

    def _readerinfo_from_stat(self, source_path, source_name, st):
        'Generate ReaderInfo from a stat_result'
        ftype = self._ftype_from_mode(st.st_mode, source_path)
        return ReaderInfo(source_path, source_name, ftype, st)

    def readerinfo_from_source_path(self, source_path, source_name):
        'See Reader'
        try:
            st = os.lstat(source_path)
        except FileNotFoundError as e:
            raise SourceObjectError(source_path, "cannot stat: %s" % exc_info_name()) from e
        except Exception as e:
            msg = "cannot stat: %s %s" % (e.__class__.__name__, e)
            exc_log(self.logger, logging.DEBUG, msg)
            raise SourceObjectError(source_path, msg) from e
        return self._readerinfo_from_stat(source_path, source_name, st)

    @staticmethod
    def readlink(tobj):
        'Fetch the target of a symbolic link'
        if isinstance(tobj, str):
            source_path = tobj
        else:
            source_path = tobj.source_path_str
        try:
            return os.readlink(source_path)
        except Exception as e:
            if isinstance(e, OSError) and os.path.exists(source_path) and (not os.path.islink(source_path)):
                raise SourceObjectError(source_path, "source path is not a symbolic link") from e
            raise SourceObjectError(source_path, "cannot read symbolic link: %s" % exc_info_err()) from e

    input_file_reader = ReadFilePosix

    def open_input_file(self, tobj):
        'Open one input file (ftype REG) for reading'
        if isinstance(tobj, str):
            source_path = tobj
        else:
            source_path = tobj.source_path_str
        try:
            return self.input_file_reader(source_path)
        except SourceObjectError:
            raise
        except Exception as e:
            raise SourceObjectError(source_path, "cannot open file: %s" % exc_info_err()) from e

    def path_exists(self, relpath):
        'Return whether or not relpath exists relative to the root of the source'
        abspath = os.path.join(self._src, relpath)
        return os.path.exists(abspath)

class ClfsReadAhead():
    '''
    Implements read-ahead specific to the clfs format.  This takes advantage of the fact that we will
    always read our input file for regular files in the same pattern before writing in clfs format.
    This does not inherit from ReadFileBase.  Instead, pass your ReadFile* into the constructor.
    '''
    # Global switch to shut off read-ahead
    READ_AHEAD_ON = True
    READ_AHEAD_THRESHOLD = CLFSSegment.FIRST_SEGMENT_BYTES + CLFSSegment.OTHER_SEGMENT_BYTES

    def __init__(self, logger, file_reader):
        self._file_reader = file_reader
        self._segments = queue.Queue(maxsize=1)  # max read-ahead is 1 segment ahead
        self._read_length = CLFSSegment.FIRST_SEGMENT_BYTES  # Initialize to first read size
        self._run = True
        self._active = False
        self._lock = threading.Lock()
        self._cond = threading.Condition(lock=self._lock)
        self._thread = None
        self.error = None
        self._thread = CLFSLoadThread(target=self.readAheadThread, name='readahead', args=(logger,))
        self._thread.start()

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, str(self._file_reader))

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self._file_reader))

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.safe_stop(check_error=False)

    def __del__(self):
        self.safe_stop(check_error=False)

    @property
    def path(self):
        'accessor'
        return self._file_reader.path

    @classmethod
    def get_readahead(cls, logger, file_reader, file_length):
        '''
        If readahead is enabled and should be used, return a wrapped file reader
        that does readahead. Otherwise, return file_reader.
        '''
        if cls.READ_AHEAD_ON and (file_length > cls.READ_AHEAD_THRESHOLD):
            return cls(logger, file_reader)
        return file_reader

    def readAheadThread(self, logger):
        '''
        Thread for performing the read-ahead (producer).
        This requires some work if we want more than one read-ahead thread to ensure that we append to the queue
        in the proper order.
        '''
        try:
            with self._lock:
                if not self._run:
                    return
                self._active = True
            try:
                while self._run:
                    segment_bytes = self._file_reader.read(self._read_length)
                    if not segment_bytes:
                        break   # done
                    self._segments.put(segment_bytes, block=True)
                    self._read_length = CLFSSegment.OTHER_SEGMENT_BYTES
            except BaseException as e:
                exc_log(logger, logging.ERROR, self.__class__.__name__)
                self.error = e
            if self._run:
                try:
                    self._segments.put(bytes())
                except BaseException as e:
                    exc_log(logger, logging.ERROR, self.__class__.__name__)
                    if self.error is None:
                        self.error = e
        finally:
            with self._lock:
                self._active = False
                self._cond.notify_all()

    def read(self, size): # pylint: disable=unused-argument
        '''
        Call this to return the next available file segment (consumer).
        size - ignored, but needed to be compatible with Reader.read().
        '''
        try:
            if self.error:
                raise self.error
            segment_bytes = self._segments.get(block=True)
            self._segments.task_done()
            if self.error:
                raise self.error
            return segment_bytes
        except NamedObjectError:
            raise
        except Exception as e:
            raise SourceObjectError(self._file_reader.path, str(e)) from e

    def _drain(self):
        '''
        Clears out items currently in the queue
        '''
        while True:
            try:
                self._segments.get(block=False)
            except:
                break

    def safe_stop(self, check_error=True):
        'Call this to ensure that the read-ahead thread is properly terminated before continuing.'
        with self._lock:
            self._run = False
            while self._active:
                self._drain()
                self._cond.wait(timeout=0.5)
            self._drain()
        self._thread.join()
        if check_error and self.error:
            raise self.error
