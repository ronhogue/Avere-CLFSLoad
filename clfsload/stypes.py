#
# clfsload/stypes.py
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

import collections # for deque
from collections import OrderedDict
import argparse
import copy
import enum
import importlib
import json
import logging
import math
import os
import pprint
import random
import struct
import sys
import threading
import time
import uuid

from sqlalchemy import BINARY, Column, Float, Index, Integer, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

from clfsload.util import Size, SorterBase, UINT64_MAX, elapsed, log_level_get, pretty_time
from clfsload.version import VERSION_STRING

# Maximum links to a single target supported by CLFS (IOW, max backpointer count)
CLFS_LINK_MAX = 1023

# Maximum length of a symlink target (DirMgrServer::_maxSymlinkNameLength)
CLFS_SYMLINK_LENGTH_MAX = 1023

# Initial version number for objects. This is increased
# each time we decide we need to (re-)reconcile the object.
OBJ_VERSION_INITIAL = 9

class CLFSCompressionType():
    '''
    constants used in the object header indicating whether object is compressed or not.
    from src/cloud/obzip.h
    '''
    # Note: corresponds to CommandArgs.COMPRESSION_TYPES
    DISABLED = 0
    LZ4 = 1

    MAXFRAMESIZE = 262144 # 256K to match src/lz4/lz4wrap.cc in armada source tree

    @classmethod
    def from_string(cls, txt):
        'Convert a string to a compression type constant'
        if txt is None:
            return None
        t = txt.upper()
        return getattr(cls, t, None)

class CLFSEncryptionType():
    '''
    constants used in the object header indicating if object is encrypted in container or not
    from src/cloud/obcrypt.h.
    '''
    # Note: corresponds to CommandArgs.ENCRYPTION_TYPES
    DISABLED = 0
    CBC_AES_256_HMAC_SHA_256 = 1
    CBC_AES_256_HMAC_SHA_512 = 2

    @classmethod
    def from_string(cls, txt):
        'Convert a string to a encryption type constant'
        if txt is None:
            return None
        t = txt.upper()
        if t in ('AES-256', 'AES_256'):
            return cls.CBC_AES_256_HMAC_SHA_256
        return getattr(cls, t, None)

class CommandArgs():
    'Handle command-line arguments'

    # Note: names here correspond to CLFSCompressionType
    COMPRESSION_TYPES = ('DISABLED', 'LZ4')
    COMPRESSION_TYPE_DEFAULT = 'LZ4'

    # Assembling this validates that each value in COMPRESSION_TYPES is good
    COMPRESSION_TYPES_DICT = {compression_type : CLFSCompressionType.from_string(compression_type) for compression_type in COMPRESSION_TYPES}

    # Note: names here correspond to CLFSEncryptionType
    ENCRYPTION_TYPES = ('DISABLED',)
    #ENCRYPTION_TYPES = ('DISABLED', 'AES-256')
    ENCRYPTION_TYPE_DEFAULT = 'DISABLED'

    # Assembling this validates that each value in ENCRYPTION_TYPES is good
    ENCRYPTION_TYPES_DICT = {encryption_type : CLFSEncryptionType.from_string(encryption_type) for encryption_type in ENCRYPTION_TYPES}

    # Initialize mandatory arguments to None
    local_state_path = None
    source_root = None
    target_storage_account = None
    target_container = None
    sas_token = None

    _WORKER_THREAD_COUNT_DEFAULT_MIN = 8
    _WORKER_THREAD_COUNT_DEFAULT_MAX = 32

    # Strictly bound the number of worker threads.
    # If the command-line specifies a number out of this
    # range, it is coerced to be in-range with a warning
    # logged.
    _WORKER_THREAD_COUNT_MIN = 1
    _WORKER_THREAD_COUNT_MAX = 200

    # How many worker threads to use if we cannot compute
    # an auto-scaled count
    _WORKER_THREAD_COUNT_DEFAULT_FALLBACK = 16

    # defaults for optional arguments
    new = False
    azcopy = False
    dry_run = False
    dry_run_gb = None
    dry_run_count = None
    _compression = COMPRESSION_TYPE_DEFAULT
    _encryption = ENCRYPTION_TYPE_DEFAULT
    dirmgr_count = 48
    max_errors = 50
    preserve_hardlinks = 0
    retry_errors = 1

    def __init__(self, logger, prog=None, add_version=False):
        self._logger = logger # leveraged by tests
        self._prog = prog.strip() if prog else 'CLFSLoad'
        self.log_level = self._logger.level
        self._handle_local_state_path = getattr(self, '_handle_local_state_path', True)
        self._handle_source_root_args = getattr(self, '_handle_source_root_args', True)
        self._handle_target_container_args = getattr(self, '_handle_target_container_args', True)
        self._handle_azcopy_args = getattr(self, '_handle_azcopy_args', True)
        self._handle_dry_run_args = getattr(self, '_handle_dry_run_args', True)
        self._handle_compression_args = getattr(self, '_handle_compression_args', len(self.COMPRESSION_TYPE_DEFAULT) > 1)
        self._handle_encryption_args = getattr(self, '_handle_encryption_args', len(self.ENCRYPTION_TYPES) > 1)
        self._handle_preserve_hardlinks_args = getattr(self, '_handle_preserve_hardlinks_args', True)
        self._handle_error_args = getattr(self, '_handle_error_args', True)
        self._handle_worker_thread_count_args = getattr(self, '_handle_worker_thread_count_args', True)
        self._parser = argparse.ArgumentParser(prog=self._prog)
        if add_version:
            self._parser.add_argument("--version", action="version",
                                      version=self.version_string())
        try:
            self.worker_thread_count = self._get_worker_thread_count_default()
        except:
            self.worker_thread_count = self._WORKER_THREAD_COUNT_DEFAULT_FALLBACK
        self.worker_thread_count = min(max(self.worker_thread_count, self._WORKER_THREAD_COUNT_DEFAULT_MIN), self._WORKER_THREAD_COUNT_DEFAULT_MAX)
        self.worker_thread_count = self._worker_thread_count_effective(self.worker_thread_count)
        if self._handle_local_state_path:
            self._parser.add_argument("local_state_path", type=str,
                                      help="directory path for local state")
        if self._handle_source_root_args:
            self._parser.add_argument("source_root", type=str,
                                      help="root directory of source")
        if self._handle_target_container_args:
            self._parser.add_argument("target_storage_account", type=str,
                                      help="name of the target storage account")
            self._parser.add_argument("target_container", type=str,
                                      help="name of the target container")
            self._parser.add_argument("sas_token", type=str,
                                      help="shared access signature for target_container")
        if self._handle_local_state_path:
            self._parser.add_argument("--new", action="store_true",
                                      help="start a new transfer")
        if self._handle_azcopy_args:
            self._parser.add_argument("--azcopy", action="store_true",
                                      help=argparse.SUPPRESS)
        else:
            self.azcopy = False
        if self._handle_dry_run_args:
            self._parser.add_argument("--dry_run", action="store_true",
                                      help="perform a dry-run to approximate the count of entities and the capacity that must be transferred")
            self._parser.add_argument("--dry_run_gb", type=float,
                                      help="assume that a dry_run returned this capacity in GB")
            self._parser.add_argument("--dry_run_count", type=int,
                                      help="assume that a dry_run returned this count")
        else:
            self.dry_run = False
            self.dry_run_gb = None
            self.dry_run_count = None
        if self._handle_compression_args:
            self._parser.add_argument("--compression", type=str, default='', # empty default for resume case
                                      choices=self.COMPRESSION_TYPES,
                                      help="type of compression to enable (default=%s)" % self.COMPRESSION_TYPE_DEFAULT)
        else:
            self.compression = self.COMPRESSION_TYPE_DEFAULT
        if self._handle_encryption_args and (len(self.ENCRYPTION_TYPES) > 1):
            self._parser.add_argument("--encryption", type=str, default='', # empty default for resume case
                                      choices=self.ENCRYPTION_TYPES,
                                      help="type of encryption to enable (default=%s)" % self.ENCRYPTION_TYPE_DEFAULT)
        else:
            self._handle_encryption_args = False
            self.encryption = self.ENCRYPTION_TYPE_DEFAULT
        if self._handle_worker_thread_count_args:
            self._parser.add_argument("--worker_thread_count", type=int, default=self.worker_thread_count,
                                      help=argparse.SUPPRESS)
        if self._handle_error_args:
            self._parser.add_argument("--max_errors", type=int, default=self.max_errors,
                                      help="maximum number of errors to tolerate (0=unlimited)")
        if self._handle_preserve_hardlinks_args:
            self._parser.add_argument("--preserve_hardlinks", type=int, default=self.preserve_hardlinks,
                                      help="preserve hardlinks (default=%s)" % self.preserve_hardlinks)
        if self._handle_error_args:
            self._parser.add_argument("--retry_errors", type=int, default=self.retry_errors,
                                      help="retry errors from previous run (ignored with --new)")
        self._parser.add_argument("--log_level", type=str, default=logging.getLevelName(self._logger.level),
                                  help="logging level (debug,info,warning,error)")
        self._added_arg_names = list()
        self._args = None
        self.local_state_path = None
        self.source_root = None
        self.target_storage_account = None
        self.target_container = None
        self.sas_token = None

    @property
    def args(self):
        'accessor'
        return self._args

    @staticmethod
    def _get_worker_thread_count_default():
        '''
        Return the default number of worker threads
        '''
        return 2 * os.cpu_count()

    @classmethod
    def _worker_thread_count_effective(cls, count):
        '''
        Given a worker thread count, adjust it
        to our global bounds.
        '''
        # _WORKER_THREAD_COUNT_MIN <= count <= _WORKER_THREAD_COUNT_MAX
        return min(max(count, cls._WORKER_THREAD_COUNT_MIN), cls._WORKER_THREAD_COUNT_MAX)

    def version_string(self):
        '''
        Return a string describing the version
        '''
        return "%s %s  (Python %s)" % (self._prog, VERSION_STRING.strip(), sys.version.split('\n')[0].strip())

    def add_argument(self, argname, *args, **kwargs):
        '''
        Add an argument by invoking parser.add_argument(argname, *args, **kwargs).
        At parse time, the parsed value is reflected back into this object.
        '''
        if 'default' in kwargs:
            setattr(self, argname, kwargs['default'])
        self._parser.add_argument(argname, *args, **kwargs)
        self._added_arg_names.append(argname)

    def target_metastore(self):
        'Return a list of metastore key/value tuples representing the target'
        return [(DbEntMetaKey.TARGET_STORAGE_ACCOUNT, self.target_storage_account),
                (DbEntMetaKey.TARGET_CONTAINER, self.target_container),
               ]

    def parse(self, args=None):
        'Invoke the parser and update this object with parsed values'
        logger = self._logger
        self._args = self._parser.parse_args(args=args)
        try:
            self.log_level = log_level_get(self._args.log_level)
        except ValueError as e:
            logger.error("invalid log_level: %s", e)
            raise SystemExit(1)
        logger.setLevel(self.log_level)
        self.local_state_path = getattr(self._args, 'local_state_path', None)
        self.source_root = getattr(self._args, 'source_root', None)
        if self._handle_target_container_args:
            self.target_storage_account = self._args.target_storage_account
            self.target_container = self._args.target_container
            self.sas_token = self._args.sas_token
        if self._handle_source_root_args:
            self.new = self._args.new
        else:
            self.new = False
        if self._handle_azcopy_args:
            self.azcopy = self._args.azcopy
        else:
            self.azcopy = False
        if self._handle_dry_run_args:
            self.dry_run = self._args.dry_run
            self.dry_run_gb = self._args.dry_run_gb
            self.dry_run_count = self._args.dry_run_count
        else:
            self.dry_run = False
            self.dry_run_gb = None
            self.dry_run_count = None
        if self._handle_compression_args:
            self.compression = self._args.compression
        else:
            self.compression = self.COMPRESSION_TYPE_DEFAULT
        if self._handle_encryption_args:
            self.encryption = self._args.encryption
        else:
            self.encryption = self.ENCRYPTION_TYPE_DEFAULT
        if self._handle_source_root_args:
            if self.new and (not self.compression):
                self.compression = self.COMPRESSION_TYPE_DEFAULT
            if self.new and (not self.encryption):
                self.encryption = self.ENCRYPTION_TYPE_DEFAULT
        if self._handle_worker_thread_count_args:
            new_worker_thread_count = self._worker_thread_count_effective(self._args.worker_thread_count)
            if new_worker_thread_count != self._args.worker_thread_count:
                logger.warning("adjusted worker_thread_count from %s to %s",
                               self._args.worker_thread_count, new_worker_thread_count)
            self.worker_thread_count = new_worker_thread_count
        if self._handle_error_args:
            self.max_errors = self._args.max_errors
        if self._handle_preserve_hardlinks_args:
            self.preserve_hardlinks = self._args.preserve_hardlinks
        else:
            # class -> instance
            setattr(self, 'preserve_hardlinks', self.preserve_hardlinks)
        for argname in self._added_arg_names:
            if argname.startswith('--'):
                argname = argname[2:]
            val = getattr(self._args, argname)
            try:
                setattr(self, argname, val)
            except AttributeError:
                # The caller has defined a setter. Ignore.
                pass

class CLFSLoadException(Exception):
    'Base class for all CLFSLoad-specific exceptions'
    def to_json(self):
        '''
        Serialize (lossy) in JSON format
        '''
        tmp = self.to_json_dict()
        return json.dumps(tmp)

    def to_json_dict(self):
        '''
        Helper for to_json() that returns the dict
        '''
        return {'txt' : str(self)}

    @classmethod
    def from_json(cls, txt, stack_is_interesting=None):
        '''
        Reconstitute from the result of to_json()
        '''
        jd = json.loads(txt)
        return cls.from_json_dict(jd, stack_is_interesting=stack_is_interesting)

    @classmethod
    def from_json_dict(cls, jd, stack_is_interesting=None):
        '''
        Given jd as a reconstituted json.loads() of the result from .to_json(),
        return the corresponding object.
        '''
        stack_is_interesting = stack_is_interesting if stack_is_interesting is not None else True
        txt = jd.pop('txt', '')
        ret = cls(txt)
        if stack_is_interesting is not None:
            setattr(ret, 'stack_is_interesting', stack_is_interesting)
        return ret

class InternalError(CLFSLoadException):
    'Generic context-free error'

class SimpleError(CLFSLoadException):
    'Generic context-free error'

class WriteFileError(CLFSLoadException):
    'Cannot write a file in local_state_dir -- failstop'

class BackpointerLimitReached(CLFSLoadException):
    'Object already has the maximum number of backpointers'

class DbInconsistencyError(CLFSLoadException):
    'Database is inconsistent'

class DbAccessError(CLFSLoadException):
    'Caller attempted illegal access'

class BtypeConversionError(CLFSLoadException):
    'clfs btype does not map to existing Ftype'

class TerminalError(CLFSLoadException):
    '''
    Base class for exceptions that should terminate a run.
    Do not instantiate directly; instantiate specific subclasses.
    '''

class AbortException(TerminalError):
    'Operation aborted due to pending shutdown'

class ContainerTerminalError(TerminalError):
    '''
    The container cannot be accessed. Either the container does not
    exist or the SAS token is invalid, expired, or lacks the
    necessary permissions.
    '''

class DbTerminalError(TerminalError):
    '''
    Database is terminally ill'
    '''

class ServerRejectedAuthError(TerminalError):
    'server rejected our authentication'

class NamedObjectError(CLFSLoadException):
    'Base class for errors that are specific to a named object'
    def __init__(self, name, msg, *args, **kwargs):
        if isinstance(name, TargetObj):
            self.name = name.source_path_str
        else:
            self.name = name
        self.name = str(self.name)
        self._msg = msg
        self._args = copy.deepcopy(args)
        self._kwargs = copy.deepcopy(kwargs)
        self._usr_msg = self._usr_msg_compute()
        super(NamedObjectError, self).__init__(self._usr_msg)

    @property
    def args(self):
        return copy.deepcopy(self._args)

    @property
    def kwargs(self):
        return copy.deepcopy(self._kwargs)

    @property
    def stack_is_interesting(self):
        return self._kwargs.get('stack_is_interesting', True)

    @classmethod
    def from_json_dict(cls, jd, stack_is_interesting=None):
        '''
        Given jd as a reconstituted json.loads() of the result from .to_json(),
        return the corresponding object.
        '''
        stack_is_interesting = stack_is_interesting if stack_is_interesting is not None else True
        name = jd.pop('name', 'NO_NAME_from_json')
        _msg = jd.pop('_msg', 'NO_MSG_from_json')
        args = jd.pop('_args', tuple())
        kwargs = jd.pop('_kwargs', dict())
        kwargs['stack_is_interesting'] = stack_is_interesting
        return cls(name, _msg, *args, **kwargs)

    def to_json_dict(self):
        '''
        Helper for to_json() that returns the dict
        '''
        return {'name' : self.name,
                '_msg' : self._msg,
                '_args' : tuple((x if isinstance(x, (bool, int, str)) else repr(x) for x in self._args)), # force tuple to avoid generator
                '_kwargs' : dict({k : v if isinstance(v, (bool, int, str)) else repr(v) for k, v in self._kwargs.items()}), # force dict to avoid generator
               }

    def _usr_msg_compute(self):
        'Return the user-friendly string for the exception'
        msg = self._msg.rstrip()
        if msg.find(self.name) < 0:
            msg = msg + ' (' + self.name + ')'
        ret = msg
        for arg in self._args:
            ret += ', '
            ret += repr(arg)
        for k, v in self._kwargs.items():
            if k == 'stack_is_interesting':
                continue
            ret += ', '
            ret += repr(k)
            ret += '='
            ret += repr(v)
        ret += ')'
        return ret

    def __str__(self):
        return self._usr_msg

    def __repr__(self):
        return self._usr_msg

class SourceObjectError(NamedObjectError):
    'Error specific to a source object (identified by a string name)'

class CLFSCommonObjectId(enum.Enum):
    'Well-known object IDs in CLFS (src/cloud/restdrv.h)'
    BUCKETID = 0x63
    ROOTID = 0x64
    SPACETRACKERID = 0x65
    ORPHANPARENTID = 0x66
    SNAPSHOTDBID = 0x67

    # reserved values used internally by CLFSLoad
    # INODE_INPROGRESS_ID: When we write an inode that will
    #   be reconciled later, we must pick a consistent value
    #   for owner_obj_id. We cannot use first_backpointer
    #   because if we crash and restart we can end up with
    #   a different first_backpointer. CLFS insists that
    #   owner_obj_id match first_backpointer (a mismatch
    #   fails ObHandle::attackCheck()). So, when we write the
    #   inode in the transfer phase, we write it using
    #   INODE_INPROGRESS_ID (FILEHANDLE_INODE_INPROGRESS)
    #   for the owner_id. In the reconcile phase, we write the
    #   final inode with the correct value and remove the
    #   temporary blob. We do not use ORPHANPARENTID here
    #   so we may distinguish between intermediate inode objects
    #   and finalized inode objects.
    INODE_INPROGRESS_ID = 0x62

class CLFSObjHandleType(enum.Enum):
    'CLFS object blob handle types as defined in src/cloud/obtype.h'
    OBTYPE_DATA = 2       # cloud user data
    OBTYPE_DIRENTS = 3       # cloud directory data
    OBTYPE_VATTR = 4       # cloud file attributes
    OBTYPE_BMAP = 5       # cloud file direct/indir block pointers
    OBTYPE_NODEREG = 6       # present in node registration objects only UNUSED
    OBTYPE_INDIR = 7       # indirect block array
    OBTYPE_COR = 8       # COR UNUSED
    OBTYPE_BACK = 9       # back pointer object
    OBTYPE_DATABACK = 10      # back pointer from data and indir blocks (for fs check)
    OBTYPE_SMB = 11      # smb related data (i.e. acl, attributes, etc) UNUSED
    OBTYPE_EXTATTRS = 12      # extended attributes
    OBTYPE_TOMBSTONE = 13      # tombstone records UNUSED
    OBTYPE_SPACEARRAY = 14      # space array UNUSED
    OBTYPE_VERSION = 15      # version blob deprecated UNUSED
    OBTYPE_NAME = 16      # Name blob

class CLFSObjectBlob():
    def __init__(self, obtype, data, blobCount, realCount):
        self.obtype = CLFSObjHandleType(obtype)
        self.data = data
        self.blobCount = blobCount
        self.realCount = realCount

class HandleBase():
    'Base class for handle-like things'
    def __init__(self, *args):
        if args:
            if isinstance(args[0], HandleBase):
                self._ba = bytes(args[0].bytes, *args[1:])
            elif isinstance(args[0], CLFSCommonObjectId):
                tmp = bytearray(16)
                tmp[15] = args[0].value
                self._ba = bytes(tmp, *args[1:])
            else:
                self._ba = bytes(*args)
        else:
            self._ba = bytes()

    def __bytes__(self):
        return self._ba

    def __lt__(self, other):
        if isinstance(other, HandleBase):
            return self.bytes < other.bytes
        return self.bytes < other

    def __le__(self, other):
        if isinstance(other, HandleBase):
            return self.bytes <= other.bytes
        return self.bytes <= other

    def __eq__(self, other):
        if isinstance(other, HandleBase):
            return self.bytes == other.bytes
        return self.bytes == other

    def __ne__(self, other):
        if isinstance(other, HandleBase):
            return self.bytes != other.bytes
        return self.bytes != other

    def __ge__(self, other):
        if isinstance(other, HandleBase):
            return self.bytes >= other.bytes
        return self.bytes >= other

    def __gt__(self, other):
        if isinstance(other, HandleBase):
            return self.bytes > other.bytes
        return self.bytes > other

    def __hash__(self):
        '''
        Compute a hash of the filehandle. This is used for incore purposes
        like managing sets and dicts of HandleBases. Do not rely on this
        hash value for anything persistent in either the target container
        or the local_state_dir.
        '''
        return hash(self._ba)

    def __str__(self):
        return self.hex()

    def __repr__(self):
        return self.hex()

    def __len__(self):
        return len(self._ba)

    def __bool__(self):
        return bool(self._ba)

    def __int__(self):
        ret = 0
        for v in self._ba:
            ret *= 256
            ret += v
        return ret

    def __index__(self):
        return self.__int__()

    @property
    def bytes(self):
        return self._ba

    def hex(self):
        'Return this filehandle represented as a hex string'
        return self._ba.hex()

    def size(self):
        'define method to match string size() method -- used by parse/unparse routines'
        return len(self._ba)

    def isnull(self):
        'Return whether this is the NULL (all zero)'
        # Skeezy hack that is OK because self._ba is small
        ba2 = bytes(len(self._ba))
        return self._ba == ba2

    @classmethod
    def fromhex(cls, txt):
        'Generate assuming that txt is a hex string'
        return cls(bytes.fromhex(txt))

class Filehandle(HandleBase):
    '''
    Represent an NFS filehandle as used by Armada.
    Here, we have the notion of a fixed length (fixedlen()).
    That length applies to all Filehandle objects that are
    written into the target object database. That enables
    more efficient database accesses.
    '''

    def __init__(self, *args):
        if args:
            super(Filehandle, self).__init__(*args)
        else:
            self._ba = bytes(self.fixedlen())

    @staticmethod
    def fixedlen():
        'Return the constant length of all filehandle bytearrays'
        return 24

    @staticmethod
    def fhseq_first_get():
        # Return the first fhseq (filehandle sequence number) to use
        # when starting a new transfer. We skip straight to 1000 to
        # avoid all collisions like the special-case with the root obj ID.
        # This also conveniently avoids 0.
        return 1000

    _GENERATE_PACK1 = '<IBBBBQQ'

    @classmethod
    def generate(cls, containerid, dmid_src, dmid_tgt, ftype, seq):
        '''
        Generate a new filehandle.
        '''
        fh_containerid = containerid & 0x7ffffff # CONTAINERID_MASK
        fh_containerid = fh_containerid << 5 # bitfields version:2 snapshot_root:1 snapshot_passthrough:1 local:1
        fh_containerid &= 0xffffffe0
        fh_containerid |= 0x00000011 # 0x11 for version=1 local=1
        random_seq = random.randint(1, UINT64_MAX)

        # In an ArmFileHandle, the exportid (32 bits, little-endian) would go first
        b = struct.pack(cls._GENERATE_PACK1, fh_containerid, dmid_src, dmid_tgt, ftype, 0, seq, random_seq)

        return cls(b)

    def extract_dmid(self):
        'Given a filehandle, return the embedded dirmgr short id (dmid) for the owning dirmgr'
        return self._ba[5]

FILEHANDLE_NULL = Filehandle()
FILEHANDLE_NULL_BYTES = FILEHANDLE_NULL.bytes
FILEHANDLE_ROOT = Filehandle(CLFSCommonObjectId.ROOTID)
FILEHANDLE_ORPHAN = Filehandle(CLFSCommonObjectId.ORPHANPARENTID)
FILEHANDLE_INODE_INPROGRESS = Filehandle(CLFSCommonObjectId.INODE_INPROGRESS_ID)

class ObCacheId(HandleBase):
    '''
    Represents ObCacheId which is either a valid FileHandle or valid UUID4 bytearray.
    Class implementation identical to filehandle except for generate part which accepts
    FileHandle, UUID4, or CLFSCommonObjectId
    '''
    def __init__(self, *args):
        if args:
            if isinstance(args[0], uuid.UUID):
                self._ba = bytes(args[0].bytes)
            elif args[0] is None:
                self._ba = bytes(uuid.uuid4().bytes)
            else:
                super(ObCacheId, self).__init__(*args)
        else:
            self._ba = bytes(uuid.uuid4().bytes)

    def bucketNameVersionIsV1(self):
        return self._ba in  CLFSCommonObCacheId.SPECIALV1NAMES

    def isRootId(self):
        return self._ba == CLFSCommonObCacheId.ROOTOID

class CLFSCommonObCacheId():
    NULLOID = ObCacheId(bytes(16))
    BUCKETOID = ObCacheId(CLFSCommonObjectId.BUCKETID)
    ROOTOID = ObCacheId(CLFSCommonObjectId.ROOTID)
    SPACETRACKEROID = ObCacheId(CLFSCommonObjectId.SPACETRACKERID)
    ORPHANPARENTID = ObCacheId(CLFSCommonObjectId.ORPHANPARENTID)
    SNAPSHOTDBOID = ObCacheId(CLFSCommonObjectId.SNAPSHOTDBID)
    SPECIALV1NAMES = [BUCKETOID, SPACETRACKEROID, ORPHANPARENTID, SNAPSHOTDBOID]

class TargetObjectError(NamedObjectError):
    'Error specific to a target object (identified by a Filehandle)'
    def __init__(self, filehandle, msg, *args, **kwargs):
        if isinstance(filehandle, TargetObj):
            fhc = Filehandle(filehandle.filehandle)
            name = fhc.hex()
        else:
            try:
                name = filehandle.hex()
                fhc = Filehandle(filehandle)
            except Exception as e:
                name = e.__class__.__name__ + '/' + str(filehandle)
                fhc = FILEHANDLE_NULL
        super(TargetObjectError, self).__init__(name, msg, *args, **kwargs)
        self.filehandle = fhc
        if isinstance(filehandle, TargetObj):
            self.update_from_tobj(filehandle)

    @classmethod
    def from_json_dict(cls, jd, stack_is_interesting=True):
        'See base class'
        try:
            filehandle = Filehandle.fromhex(jd.pop('filehandle'))
        except KeyError:
            filehandle = FILEHANDLE_NULL
        _msg = jd.pop('_msg', 'NO_MSG_from_json')
        args = jd.pop('_args', tuple())
        kwargs = jd.pop('_kwargs', dict())
        kwargs['stack_is_interesting'] = stack_is_interesting
        return cls(filehandle, _msg, *args, **kwargs)

    def to_json_dict(self):
        'See base class'
        ret = super(TargetObjectError, self).to_json_dict()
        ret['filehandle'] = self.filehandle.hex()
        return ret

    def update_from_tobj(self, tobj):
        '''
        Pick up any additional interesting info from tobj
        '''
        updated = False
        if ('source_path' not in self._kwargs) and tobj.has_source_path():
            self._kwargs['source_path'] = tobj.source_path_str
            updated = True
        if updated:
            self._usr_msg = self._usr_msg_compute()

class DryRunResult(): # pylint: disable=eq-without-hash
    'Encapsulate results of a dry run'
    def __init__(self, *args, **kwargs):
        '''
        No args: init to zero
        One arg, a tuple such as that returned from to_meta()
        kwargs: checked for 'count' and 'gb'
        '''
        self.dr_count = 0
        self.dr_gb = 0.0
        if args:
            if kwargs:
                raise TypeError("unexpected args and kwargs")
            if len(args) != 1:
                raise TypeError("too many args")
            if isinstance(args[0], (tuple, list)):
                for tup in args[0]:
                    if len(tup) != 2:
                        raise TypeError("unexpected length for %s.'%s'" % (tup.__class__.__name__, tup))
                    if tup[0] == DbEntMetaKey.DRY_RUN_COUNT:
                        self.dr_count = tup[1]
                    elif tup[0] == DbEntMetaKey.DRY_RUN_GB:
                        self.dr_gb = tup[1]
            elif isinstance(args[0], dict):
                self.dr_count = args[0].get(DbEntMetaKey.DRY_RUN_COUNT, None)
                self.dr_gb = args[0].get(DbEntMetaKey.DRY_RUN_GB, None)
            else:
                raise TypeError("unexpected args[0] %s" % args[0].__class__.__name__)
        if kwargs:
            self.dr_count = kwargs.get('count', self.dr_count)
            self.dr_gb = kwargs.get('gb', self.dr_gb)

    def __str__(self):
        return self.stringify()

    def __repr__(self):
        return self.stringify()

    def __eq__(self, other):
        if self.dr_count != other.dr_count:
            return False
        if self.dr_gb != other.dr_gb:
            return False
        return True

    def __ne__(self, other):
        return not self == other

    def __bool__(self):
        return (self.dr_count is not None) or (self.dr_gb is not None)

    def stringify(self, countstr='count'):
        'stringify this object'
        if self.dr_count is None:
            ret = countstr + "=unavailable"
        else:
            ret = countstr + '=' + str(self.dr_count)
        if self.dr_gb is None:
            ret += " GB=unavailable"
        else:
            ret += " GB=%.6f" % self.dr_gb
        return ret

    def add(self, other):
        'add the counts from other to self'
        if self.dr_count is None:
            if other.dr_count is not None:
                self.dr_count = other.dr_count
        else:
            if other.dr_count is not None:
                self.dr_count += other.dr_count
        if self.dr_gb is None:
            if other.dr_gb is not None:
                self.dr_gb = other.dr_gb
        else:
            if other.dr_gb is not None:
                self.dr_gb += other.dr_gb

    @classmethod
    def get_from_db(cls, db):
        'read current settings in db (ClfsLoadDB)'
        mdk = [DbEntMetaKey.DRY_RUN_COUNT,
               DbEntMetaKey.DRY_RUN_GB,
              ]
        mdd = db.db_meta_get(mdk)
        return cls(mdd)

    def to_meta(self):
        '''
        Return a list suitable for ClfsLoadDB.db_meta_set()
        or the constructor for this class.
        '''
        return [(DbEntMetaKey.DRY_RUN_COUNT, self.dr_count),
                (DbEntMetaKey.DRY_RUN_GB, self.dr_gb),
               ]

    def db_put(self, db):
        'write settings to db (ClfsLoadDB)'
        db.db_meta_set(self.to_meta())

class DbEntMetaKey():
    '''
    This lists unique values for DbEntMeta* keys.
    The names (strings) are used as keys in the tables.
    The values imply the type. Callers should use the is_*()
    methods to infer the types. The values are never written
    into stable store, so it is safe to renumber them.
    We do not use enum here to support extending this for tests.
    The strings are written in the database, so the logical
    names may change without introducing a DB format change,
    but changing the strings requires bumping the
    result of ClfsLoadDB.version_format_get().
    '''
    VERSION_FORMAT = 'VERSION_FORMAT'
    DRY_RUN_COUNT = 'DRY_RUN_COUNT'
    DRY_RUN_GB = 'DRY_RUN_GB'
    CONTAINERID = 'CONTAINERID'
    DIRMGR_COUNT = 'DIRMGR_COUNT'
    FHSEQ = 'FHSEQ'
    VERSION_STRING = 'VERSION_STRING'
    PHASE = 'PHASE'
    ROOTFH = 'ROOTFH'
    PRESERVE_HARDLINKS = 'PRESERVE_HARDLINKS'
    TARGET_STORAGE_ACCOUNT = 'TARGET_STORAGE_ACCOUNT'
    TARGET_CONTAINER = 'TARGET_CONTAINER'
    FSID = 'FSID'
    PROGRESS_COUNT = 'PROGRESS_COUNT'
    PROGRESS_GB = 'PROGRESS_GB'
    COMPRESSION = 'COMPRESSION'
    ENCRYPTION = 'ENCRYPTION'

    @classmethod
    def value_from_dbe(cls, key, value):
        '''
        See table.DbEntMeta. Values stored in that table are strings
        that are JSON-ified. Here we unwind that and handle any
        special cases.
        '''
        v = json.loads(value)
        if key == cls.PHASE:
            v = getattr(Phase, v, v)
        elif key == cls.ROOTFH:
            v = Filehandle.fromhex(v)
        return v

class Ftype():
    '''
    Does not matche ftype3 from RFC 1813. DIR is deliberately
    placed first, so that all non-DIR types may be queried from
    the TargetObj table using ftype>DIR. Similarly, REG and LNK
    come next - these are the two variable-length non-DIR ftypes.
    The ftypes that follow are all fixed length.

    In a better world, this would be enum.Enum. That does not
    work out well, because we want to make these ordered compares.
    We could auto-convert dbe.ftype (Integer) to tobj.ftype (Ftype),
    but then it gets too easy to write code like "dbe.ftype == Ftype.DIR",
    and that will do the wrong thing when dbe.ftype is 1.
    '''
    DIR = 1
    REG = 2
    LNK = 3
    BLK = 4
    CHR = 5
    SOCK = 6
    FIFO = 7

DATA_FTYPES = (Ftype.DIR, Ftype.REG, Ftype.LNK)
MKNOD_FTYPES = (Ftype.BLK, Ftype.CHR, Ftype.SOCK, Ftype.FIFO)
DEVICE_FTYPES = (Ftype.BLK, Ftype.CHR)

class Btype():
    '''
    Btype is the type of blob object in the container. This includes
    some definitions in Ftype for inode types, and additional types
    to denote that an object is a indirect block object or direct block
    or other CLFS-related container object types that are encoded in the
    CLFS V2 object name in the container.
    '''
    BTYPE_INVALID = 0
    BTYPE_REG = 1
    BTYPE_DIR = 2
    BTYPE_BLK = 3
    BTYPE_CHR = 4
    BTYPE_LNK = 5
    BTYPE_SOCK = 6
    BTYPE_FIFO = 7
    BTYPE_SEGMENT = 8
    BTYPE_INDIRECT = 9
    BTYPE_TOMBSTONE = 10
    BTYPE_SPECIAL = 11
    BTYPE_HLINK = 12 # hard linked -- more than one back pointer
    BTYPE_LAST = 12

    bdict = {Ftype.REG : BTYPE_REG,
             Ftype.DIR : BTYPE_DIR,
             Ftype.BLK : BTYPE_BLK,
             Ftype.CHR : BTYPE_CHR,
             Ftype.LNK : BTYPE_LNK,
             Ftype.SOCK : BTYPE_SOCK,
             Ftype.FIFO : BTYPE_FIFO}

    fdict = {BTYPE_REG : Ftype.REG,
             BTYPE_DIR : Ftype.DIR,
             BTYPE_BLK : Ftype.BLK,
             BTYPE_CHR : Ftype.CHR,
             BTYPE_LNK : Ftype.LNK,
             BTYPE_SOCK : Ftype.SOCK,
             BTYPE_FIFO : Ftype.FIFO}

    @classmethod
    def btype_to_ftype(cls, btype):
        try:
            return cls.fdict[btype]
        except KeyError:
            raise BtypeConversionError

    @classmethod
    def ftype_to_btype(cls, ftype):
        try:
            return cls.bdict[ftype]
        except KeyError:
            raise BtypeConversionError

class TargetObjState():
    '''
    State of a row in the TargetObj table.
    Operations such as _db_force_reconcile() rely
    on ordering.
    '''
    PENDING = 1
    GENERATING = 2
    WRITING = 3
    DONE = 4
    ERROR = 5
    INODE_PENDING = 6
    INODE_WRITING = 7
    INODE_CLEANUP_PENDING = 8
    INODE_CLEANUP_CLEANING = 9

TARGET_OBJ_STATE_STRINGS = {v : k for k, v in vars(TargetObjState).items() if isinstance(v, int)}
def target_obj_state_name(state):
    'Return a string for TargetObjState state'
    return "%s(%s)" % (state, TARGET_OBJ_STATE_STRINGS.get(state, '?'))

class Phase(enum.Enum):
    INIT = 'INIT'
    TRANSFER = 'TRANSFER'
    RECONCILE = 'RECONCILE'
    CLEANUP = 'CLEANUP'
    FINALIZE = 'FINALIZE'
    DONE = 'DONE'

    def __lt__(self, other):
        '''
        Return True iff self precedes other
        '''
        if not isinstance(other, Phase):
            raise TypeError("unexpected types %s %s" % (self.__class__.__name__, other.__class__.__name__))
        if self == other:
            return False
        if self == self.INIT:
            return True
        if self == self.TRANSFER:
            return other != self.INIT
        if self == self.RECONCILE:
            return other in (self.CLEANUP, self.FINALIZE, self.DONE)
        if self == self.CLEANUP:
            return other in (self.FINALIZE, self.DONE)
        if self == self.FINALIZE:
            return other == self.DONE
        assert self == self.DONE
        return False

    def azcopy_name(self):
        '''
        Return the azcopy-facing phase name
        '''
        txt = str(self).split('.')[-1]
        return txt[0].upper() + txt[1:].lower()

class GenericStats():
    '''
    Base class for collecting statistics
    '''
    def __init__(self, lock=None):
        self._stats = dict()
        self._lock = lock if lock is not None else threading.Lock()

    def isempty(self):
        '''
        Return True iff no stats are set.
        If any stat is set, even to zero, returns False.
        '''
        return not bool(self._stats)

    def reset(self):
        '''
        Clear pending stats. Return them as a dict.
        '''
        with self._lock:
            ret = self._stats
            self._stats = dict()
            return ret

    def to_dict(self):
        '''
        Return a dict of statistics from this object. Statistics
        are any public attributes -- that is, any attribute whose
        name does not begin with an underscore.
        '''
        with self._lock:
            return dict(self._stats)

    def pformat(self):
        '''
        Return pprint.pformat of dictified self
        '''
        with self._lock:
            return pprint.pformat(self._stats)

    def __str__(self):
        return str(self._stats)

    def __repr__(self):
        return repr(self._stats)

    def get(self, name):
        '''
        Get the current value of the named stat
        '''
        with self._lock:
            return self._stats.get(name, 0)

    def get_NL(self, name):
        '''
        Get the current value of the named stat -- caller holds the lock
        '''
        return self._stats.get(name, 0)

    def stat_min(self, name, val):
        'Update the stat with val only if val is less than the current value.'
        with self._lock:
            cur = self._stats.get(name, None)
            if (cur is None) or cur > val:
                self._stats[name] = val

    def stat_max(self, name, val):
        'Update the stat with val only if val is greater than the current value.'
        with self._lock:
            cur = self._stats.get(name, None)
            if (cur is None) or cur < val:
                self._stats[name] = val

    def stat_set(self, name, val):
        '''
        Perform an absolute set on the named stat
        '''
        with self._lock:
            self._stats[name] = val

    def stat_add(self, name, val):
        'Add val to the named stat'
        with self._lock:
            try:
                self._stats[name] += val
            except KeyError:
                self._stats[name] = val

    def stat_inc(self, name):
        'Increment the named stat'
        with self._lock:
            try:
                self._stats[name] += 1
            except KeyError:
                self._stats[name] = 1

    def stat_update(self, updates):
        'Update multiple values; updates is (name, addend)'
        with self._lock:
            for name, value in updates.items():
                try:
                    self._stats[name] += value
                except KeyError:
                    self._stats[name] = value

    def add(self, other, logger):
        '''
        Add values from other to self.
        Assumes that either self and other are isomorphic or all stats may be treated as numeric.
        Locks other but does not lock self. This is okay because
        we use this to accumulate results; no one else is accessing
        self at this time, but other is being updated by worker threads.
        '''
        with other._lock: # pylint: disable=protected-access
            self._add_NL(other, logger)

    def _add_NL(self, other, logger):
        '''
        Add values from other to self.
        Assumes that either self and other are isomorphic or all stats may be treated as numeric.
        Caller has locked other. self is not locked.
        '''
        for k, v in other._stats.items(): # pylint: disable=protected-access
            try:
                if k.endswith('_min') or k.endswith('_max'):
                    c = self._stats.get(k, None)
                    if v is None:
                        self._stats[k] = c
                    elif c is None:
                        self._stats[k] = v
                    elif v is not None:
                        if k.endswith('_min'):
                            self._stats[k] = min(c, v)
                        else:
                            self._stats[k] = max(c, v)
                    continue
                try:
                    self._stats[k] += v
                except KeyError:
                    self._stats[k] = v
            except Exception as e:
                logger.error("%s.add attribute='%s' v.class=%s other.class=%s cannot add: %s %s" %
                             (self.__class__.__name__, k, v.__class__.__name__, other.__class__.__name__,
                              e.__class__.__name__, e))

    def report_short(self, cur_ts, elapsed_secs, clfsload, *args, **kwargs):
        '''
        Return a dict containing:
            simple: short single-line report suitable for a quick status update.
        '''
        with self._lock:
            ret = {'simple' : self._report_short_NL(cur_ts, elapsed_secs, clfsload, *args, **kwargs)}
            return ret

    def _report_short_NL(self,
                         cur_ts, # pylint: disable=unused-argument
                         elapsed_secs,
                         clfsload, # pylint: disable=unused-argument
                         historical_perf, # pylint: disable=unused-argument
                         report_is_interval=False, # pylint: disable=unused-argument
                         report_is_final=False): # pylint: disable=unused-argument
        '''
        Return a short single-line report suitable for a quick status update
        '''
        return "elapsed=%.1f %s" % (elapsed_secs, str(self))

    def report_final(self, logger, secs_elapsed):
        '''
        Additional logging at end-of-life
        '''
        # noop here in base class

class Timer():
    def __init__(self, name, timer_stats):
        self._t0 = time.time()
        self._t1 = None
        self._name = name
        self._timer_stats = timer_stats

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop()

    def __del__(self):
        self.stop()

    def stop(self):
        'Stop the timer'
        if self._t1 is None:
            self._t1 = time.time()
            elapsed_secs = elapsed(self._t0, self._t1)
            self._timer_stats.stop_timer(self._name, elapsed_secs)

    def time_start(self):
        return self._t0

    @property
    def name(self):
        return self._name

    def elapsed(self):
        if self._t1 is not None:
            return elapsed(self._t0, self._t1)
        return elapsed(self._t0)

class TimerStats(GenericStats):
    def __init__(self, *args, **kwargs):
        self._t0 = kwargs.pop('t0', None)
        self._t1 = kwargs.pop('t1', None)
        super(TimerStats, self).__init__(*args, **kwargs)
        self._count = 0

    @property
    def t0(self):
        return self._t0

    @property
    def t1(self):
        return self._t1

    def start_working(self):
        self._t0 = time.time()

    def stop_working(self):
        with self._lock:
            self._t1 = time.time()

    def start(self, name):
        'start the named timer'
        return Timer(name, self)

    def stop_timer(self, timer_name, elapsed_secs):
        'Handle a timer stopping'
        with self._lock:
            v = self._stats.get(timer_name, 0.0) + elapsed_secs
            self._stats[timer_name] = v
        return v

    def add(self, other, logger):
        'see base class'
        with other._lock: # pylint: disable=protected-access
            super(TimerStats, self)._add_NL(other, logger)
            self._count += 1
            if self._t0 is None:
                if other.t0 is not None:
                    self._t0 = other.t0
            elif other.t0 is not None:
                if other.t0 < self._t0:
                    self._t0 = other.t0
            if self._t1 is None:
                if other.t1 is not None:
                    self._t1 = other.t1
            elif other.t1 is not None:
                if other.t1 > self._t1:
                    self._t1 = other.t1

    def elapsed_dict(self):
        t0 = self._t0 if self._t0 is not None else time.time()
        t1 = self._t1 if self._t1 is not None else time.time()
        el = elapsed(t0, t1)
        ret = {'_elapsed' : el}
        with self._lock:
            ret['_count'] = self._count
            for k, v in self._stats.items():
                pcg = ((v/el) * 100.0) if el else 100.0
                if self._count:
                    pcg /= self._count
                ret[k] = "%.9f (%.02f%%)" % (v, pcg)
        return ret

class DBStats(GenericStats):
    'Database stats'
    _INITIAL_DB_STATS = {'error_count_entity' : 0,
                         'flush_count' : 0,
                         'flush_seconds' : 0.0,
                         'flush_update_tobj_deferred' : 0,
                         'flush_upsert_tobj_slowpath' : 0,
                         'snap0_flush_count' : 0,
                         'snap0_flush_seconds' : 0.0,
                         'snap1_flush_count' : 0,
                         'snap1_flush_seconds' : 0.0,
                         'snap2_flush_count' : 0,
                         'snap2_flush_seconds' : 0.0,
                        }

    def __init__(self, *args, **kwargs):
        super(DBStats, self).__init__(*args, **kwargs)
        self._stats.update(self._INITIAL_DB_STATS)

    def stat_snap_flush_NL(self):
        '''
        Take a snapshot of recent flush times.
        snap0: accumulated flush since last snap
        snap1: the most recent nonzero snap0
        snap2: previous snap1 (whether or not it changed)
        Caller holds the lock.
        '''
        with self._lock:
            self._stats['snap2_flush_count'] = self._stats['snap1_flush_count']
            self._stats['snap2_flush_seconds'] = self._stats['snap1_flush_seconds']
            if self._stats['snap0_flush_count']:
                self._stats['snap1_flush_count'] = self._stats['snap0_flush_count']
                self._stats['snap1_flush_seconds'] = self._stats['snap0_flush_seconds']
            self._stats['snap0_flush_count'] = 0
            self._stats['snap0_flush_seconds'] = 0.0

class WorkerThreadStats(GenericStats):
    'Statistics for any worker thread'
    _INITIAL_WORKER_THREAD_STATS_COMMON = {'compress_blob' : 0,
                                           'count_dir' : 0,
                                           'count_nondir' : 0,
                                           'error_count' : 0,
                                           'idle' : 0,
                                           'wait' : 0,
                                           'write_blob_count' : 0,
                                           'write_blob_secs' : 0.0,
                                           'write_blob_bytes' : 0,
                                           'write_blob_fastpath' : 0,
                                          }

    # Overloaded by subclasses
    _INITIAL_WORKER_THREAD_STATS_SPECIFIC = dict()

    def __init__(self, *args, **kwargs):
        super(WorkerThreadStats, self).__init__(*args, **kwargs)
        self._stats.update(self._INITIAL_WORKER_THREAD_STATS_COMMON)
        self._stats.update(self._INITIAL_WORKER_THREAD_STATS_SPECIFIC)

        self._t0 = None

        # AzCopy does not operate in terms of namespace, so it
        # thinks about files as the set of everything. We include
        # directories here because they represent real loads
        # and stores for CLFS.
        #
        # Many of these values are reported to AzCopy.
        #
        self._files_total = None
        self._files_completed_total = None
        self._files_completed_failed = None
        self._gb_completed = None
        self._bytes_completed = None
        self._gb_total = None
        self._bytes_total = None
        self._throughput_Mbps = None # megabits per second
        self._throughput_delta_secs = None # window size for self._throughput_Mbps
        self.throughput_last_time = None # last time self._throughput_Mbps was sampled
        self._throughput_last_gb_completed = 0.0
        self.__eta = None
        self._pct_files = None
        self._pct_bytes = None
        self._pct_eta = None

    @property
    def eta(self):
        '''
        Readonly external accessor for ETA
        '''
        return self.__eta

    @property
    def _eta(self):
        '''
        Internal accessor for ETA
        '''
        return self.__eta

    @_eta.setter
    def _eta(self, eta):
        '''
        Internal setter for ETA. Caller must not hold self._lock.
        If self._lock is held, use _eta_set_NL().
        '''
        with self._lock:
            self._eta_set_NL(eta, time.time())

    @property
    def pct_files(self):
        '''
        Return the most recent pct_files.
        Save with or without the lock, but only atomic
        wrt other percentages when holding the lock.
        '''
        return self._pct_files

    @property
    def pct_bytes(self):
        '''
        Return the most recent pct_bytes.
        Save with or without the lock, but only atomic
        wrt other percentages when holding the lock.
        '''
        return self._pct_bytes

    @property
    def pct_eta(self):
        '''
        Return the most recent pct_eta.
        Save with or without the lock, but only atomic
        wrt other percentages when holding the lock.
        '''
        return self._pct_eta

    @property
    def t0(self):
        '''
        Read-only accessor. Safe with or without self._lock held.
        '''
        return self._t0

    @t0.setter
    def t0(self, value):
        '''
        Update accessor. Caller must not hold self._lock.
        '''
        with self._lock:
            assert self._t0 is None
            self._t0 = value

    def _eta_set_NL(self, eta, cur_ts):
        '''
        Update self.__eta (self._eta) and self._pct_eta.
        Caller holds self._lock.
        '''
        self.__eta = eta
        if eta is None:
            self._pct_eta = None
            return
        t0 = self._t0
        if (eta <= t0) or (eta <= cur_ts):
            self._pct_eta = 100.0
            return
        if cur_ts < t0:
            self._pct_eta = 0.0
            return
        # t0 <= cur_ts <= eta
        est = elapsed(t0, eta)
        cur = elapsed(t0, cur_ts)
        self._pct_eta = 100.0 * (cur / est)

    def _update_throughput_NL(self, cur_ts):
        '''
        Update self._throughput attributes based
        on current self contents. Caller holds self._lock.
        '''
        if self._gb_completed is not None:
            if self.throughput_last_time is not None:
                self._bytes_completed = self._gb_completed * Size.GB
                te = elapsed(self.throughput_last_time, cur_ts)
                if te > 0.0:
                    self._throughput_delta_secs = te
                    gb = max(self._gb_completed - self._throughput_last_gb_completed, 0.0)
                    megabits = gb * 8192.0 # gigabytes to megabits
                    self._throughput_Mbps = megabits / te
                    self._throughput_last_gb_completed = self._gb_completed
                    self.throughput_last_time = cur_ts
                # If te <= 0.0, the clock did not advance.
                # Do not update throughpuut values.
                # When the clock advances, we will see the
                # accumulated bytecount changes.
            else:
                self._throughput_last_gb_completed = self._gb_completed
                self.throughput_last_time = cur_ts
        else:
            self._bytes_completed = None
            self._throughput_Mbps = None

    def eta_str(self):
        '''
        Return a human-readable string for self._eta.
        This is prefixed with a space if not empty.
        '''
        if not self._eta:
            return ''
        eta = math.ceil(self._eta)
        return ' ' + time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(eta)) + " (%.1f%%)" % self._pct_eta

    def _almost_finished(self):
        '''
        Helper so that we generate ETAs during the final portions
        where we are just writing out directories.
        '''
        return self._pct_files > 99 and self._pct_bytes > 99

    def report_short(self, cur_ts, elapsed_secs, clfsload, *args, **kwargs):
        '''
        Return a dict containing:
            simple: short single-line report suitable for a quick status update.
            Other values are straight stats copies obtained under the same lock.
            (Exporting additional values supports azcopy and testing.)
        Side-effect updates:
            self._eta self._pct_files self._pct_bytes self._pct_eta
        '''
        with self._lock:
            self._files_completed_failed = clfsload.db.stats.get('error_count_entity')
            # Complete _report_short_NL() BEFORE constructing the dict.
            # Some dict contents are set as side effects of _report_short_NL().
            simple = self._report_short_NL(cur_ts, elapsed_secs, clfsload, *args, **kwargs)
            ret = {'elapsed' : elapsed(self._t0, cur_ts),
                   'files_total' : self._files_total,
                   'files_completed_total' : self._files_completed_total,
                   'files_completed_failed' : self._files_completed_failed,
                   'bytes_completed' : self._bytes_completed,
                   'bytes_total' : self._bytes_total,
                   'pct_bytes' : self._pct_bytes,
                   'pct_eta' : self._pct_eta,
                   'pct_files' : self._pct_files,
                   'simple' : simple,
                   'throughput_Mbps' : self._throughput_Mbps,
                   'throughput_delta_secs' : self._throughput_delta_secs,
                   'time_eta' : self.__eta,
                   'time_report' : cur_ts,
                  }
            return ret

    def report_final(self, logger, secs_elapsed):
        'see base class'
        log_level = logging.DEBUG
        with self._lock:
            count = self._stats['count_dir'] + self._stats['count_nondir']
            object_overhead = secs_elapsed / count if count else 0.0
            objects_per_second = count / secs_elapsed if secs_elapsed else 0.0
            logger.log(log_level, "elapsed: %.3f", secs_elapsed)
            logger.log(log_level, "mean time per object: %.6f", object_overhead)
            logger.log(log_level, "objects per second: %.3f", objects_per_second)
            for d in self._dumplines_NL():
                logger.log(log_level, "%s", d)

    def _dumplines_NL(self):
        '''
        Return a list of lines that dump contents.
        Caller holds self._lock.
        '''
        dumpkeys = list(self._stats.keys())
        keylenmax = max([len(k) for k in dumpkeys])
        dumpkeys.sort()
        return ["  %-*s %s" % (keylenmax, k, self._stats[k]) for k in dumpkeys]

class InitStats(WorkerThreadStats):
    '''
    Statistics for the INIT phase.
    These are don't-care, but it shortens the codepaths
    to have stats for every phase.
    '''
    # No specializations for this phase

class _HistoricalPerfData():
    '''
    Convenience structure to hold historical perf data
    ckpt indicates whether or not this is a formal report
    If ckpt is not set, this item is later discarded.
    '''
    def __init__(self, write_rate_mb, rate_count, total_count, count_nondir, gb_write, time_secs, ckpt=False):
        self.write_rate_mb = write_rate_mb
        self.rate_count = rate_count
        self.total_count = total_count
        self.count_nondir = count_nondir
        self.gb_write = gb_write
        self.time_secs = time_secs
        self.ckpt = ckpt

    def __str__(self):
        return "write_rate_mb %f, rate_count %f, total_count %f, count_nondir %f, gb_write %f, time %f" % (self.write_rate_mb, self.rate_count, self.total_count, self.count_nondir, self.gb_write, self.time_secs)

class HistoricalPerf():
    '''
    Preserves historical performance data so we can use it to calculate
    recent transfer rates and an ETA.  Due to per-file overhead, and our
    large (8MB) typical transfer sizes for bigger files, file size tends
    to most dramatically affect perceived performance.  Therefore,
    this class maintains performance buckets based on how fast we can transfer
    certain file sizes and base our ETA based on these historical transfer rates.
    The perf buckets are updated as we progress to handle any slowing down or
    speeding up over the course of the run.
    '''
    _MAX_HISTORY_SECS = 120  # Keep 2min of perf history
    _MIN_HISTORY_SECS = 60   # Min history for which we'll calculate an ETA

    # Create performance buckets for different file sizes.
    _NUM_PERF_BUCKETS = 6

    def __init__(self, db):
        'db - ClfsLoadDB instance.'
        self._reports = collections.deque(maxlen=256)  # maxlen for safety
        self._perf_buckets = list()
        self._max_history_secs = 0
        self._calc_etas = db.db_has_eta_info()
        self._perf_data_ready = False
        if self._calc_etas:
            # Initialize perf buckets if we'll be calculating ETAs
            self._max_history_secs = self._MAX_HISTORY_SECS
            self._perf_buckets = [0 for _ in range(self._NUM_PERF_BUCKETS)]

    def append(self, cur_ts, gb_write, count_dir, count_nondir, ckpt=True):
        'Append to our perf history, updating the proper perf bucket.'
        total_count = count_dir + count_nondir
        # Discard temporary sample(s) at the head
        while self._reports and (not self._reports[0].ckpt):
            self._reports.popleft()
        if not self._reports:
            # empty queue, just append the new item
            self._reports.appendleft(_HistoricalPerfData(0.0, 0.0, total_count, count_nondir, gb_write, cur_ts, ckpt=ckpt))
            return
        # Calculate the most recent rates since our last update, and append the new data
        head = self._reports[0]
        elapsed_secs = elapsed(head.time_secs, cur_ts)
        write_rate_mb = (gb_write - head.gb_write) * 1000.0 / elapsed_secs if elapsed_secs > 0 else 0
        rate_count = (total_count - head.total_count) / elapsed_secs if elapsed_secs > 0 else 0
        self._reports.appendleft(_HistoricalPerfData(write_rate_mb, rate_count, total_count, count_nondir, gb_write, cur_ts, ckpt=ckpt))
        # prune stat data that is older than we currently care about
        oldest_secs = cur_ts - self._max_history_secs
        while len(self._reports) > 1:
            report = self._reports[-1]
            if report.time_secs >= oldest_secs:
                break
            self._reports.pop()
        if not self._calc_etas:
            return
        # Update our performance buckets based upon our historical data
        tail = self._reports[-1]
        hist_elapsed_sec = elapsed(tail.time_secs, cur_ts)
        if hist_elapsed_sec >= self._MIN_HISTORY_SECS:
            hist_gb_write = gb_write - tail.gb_write
            hist_files = max(count_nondir - tail.count_nondir, 1)  # prevent possible div by zero later
            hist_file_size_kb = Size.MB * hist_gb_write / hist_files
            hist_rate_mb = 1000.0 * hist_gb_write / hist_elapsed_sec
            self._update_bucket_rate(hist_rate_mb, hist_file_size_kb)

    def get_latest_write_rates(self):
        if not self._reports:
            return 0.0, 0.0
        t0 = self._reports[0]
        return t0.write_rate_mb, t0.rate_count

    @property
    def perf_data_ready(self):
        return self._perf_data_ready

    def _map_size_to_perf_bucket(self, file_size_kb):
        '''
        Maps a file size to a perf bucket using fancy math.
        These are currently our 6 bucket sizes:  32K 128K 512K 2M 8M 32M
        '''
        bucket = min(math.ceil((int(file_size_kb) >> 5) ** 0.25), self._NUM_PERF_BUCKETS - 1)
        return bucket

    def _update_bucket_rate(self, rate_mb, file_size_kb):
        '''
        Update our historical rate for this average file size.
        Add a weight factor to our historical results to prevent wild/temporary fluctuations.
        '''
        bucket_num = self._map_size_to_perf_bucket(file_size_kb)
        if self._perf_buckets[bucket_num] == 0:
            self._perf_buckets[bucket_num] = rate_mb
        else:
            self._perf_buckets[bucket_num] = (4*self._perf_buckets[bucket_num] + rate_mb) / 5
        self._perf_data_ready = True

    @staticmethod
    def _rate_mb_not_zeroish(rate_mb):
        'If the rate_mb is less than about 100 bytes per second, just call it zero.'
        return rate_mb > 0.00009

    @staticmethod
    def _rate_fps_not_zeroish(rate_fps):
        'If the fps is less than about 0.0001, just call it zero.'
        return rate_fps > 0.0001

    def projected_rate_mb_by_file_size(self, file_size):
        '''
        Get a rate estimate based on our historical data for this file size (bucket).
        If no data is available for a particular file size, return data for a smaller
        (less performant bucket) first so we are more likely to overestimate the ETA.
        '''
        assert self._calc_etas
        bucket_num = self._map_size_to_perf_bucket(file_size / Size.KB)
        for i in range(bucket_num, 0, -1):
            if self._rate_mb_not_zeroish(self._perf_buckets[i]):
                return self._perf_buckets[i]
        for i in range(bucket_num+1, self._NUM_PERF_BUCKETS-1):
            if self._rate_mb_not_zeroish(self._perf_buckets[i]):
                return self._perf_buckets[i]
        return 0

    def projected_rate_fps(self):
        '''
        If the entire transfer is made up of zero-length files, we can also give an
        ETA based on the file count remaining.
        '''
        if len(self._reports) < 2:
            return 0
        t0 = self._reports[0]
        t1 = self._reports[-1]
        elapsed_time = t1.time_secs - t0.time_secs
        if elapsed_time == 0:
            return 0
        rate_fps = (t1.total_count - t0.total_count) / elapsed_time
        if self._rate_fps_not_zeroish(rate_fps):
            return rate_fps
        return 0

class TransferStats(WorkerThreadStats):
    '''
    Statistics for the transfer phase
    '''
    _INITIAL_WORKER_THREAD_STATS_SPECIFIC = {'fh_regenerate' : 0,
                                            }

    def _report_short_NL(self,
                         cur_ts,
                         elapsed_secs,
                         clfsload,
                         historical_perf,
                         report_is_interval=False,
                         report_is_final=False):
        '''
        Type-specific internals for report_short().
        Called with self._lock held.
        '''
        db = clfsload.db
        count_dir = self._stats['count_dir']
        count_nondir = self._stats['count_nondir']
        total_count = count_dir + count_nondir
        gb_write = self._stats.get('gb_write', 0.0)
        write_blob_count = self._stats.get('write_blob_count', 0)
        # calculate cumulative write rates
        if elapsed_secs:
            cumulative_count = total_count / elapsed_secs
            cumulative_mb = (1000.0 * gb_write) / elapsed_secs
        else:
            cumulative_count = total_count
            cumulative_mb = 0.0
        # get rates based on most recent rate calculations
        historical_perf.append(cur_ts, gb_write, count_dir, count_nondir, ckpt=report_is_interval)
        recent_mb, recent_count = historical_perf.get_latest_write_rates()
        count_str = "written=%d (dir/nondir=%d/%d)" % (total_count, count_dir, count_nondir)
        # Include saved values from process restarts in the remaining report data
        total_gb_write = gb_write
        if db.dr_init.dr_gb is not None:
            total_gb_write += db.dr_init.dr_gb
        if db.dr_init.dr_count is not None:
            total_count += db.dr_init.dr_count
        self._files_completed_total = total_count
        self._gb_completed = total_gb_write
        gb_str = "GB=%.6f" % total_gb_write
        eta_str = ''
        if db.dr_input.dr_count is not None:
            self._files_total = db.dr_input.dr_count
            if db.dr_input.dr_count > 0:
                self._pct_files = 100.0 * (total_count / db.dr_input.dr_count) if total_count > 0 else 0.0
                count_str += " (%.1f%%)" % self._pct_files
        if db.dr_input.dr_gb is not None:
            self._gb_total = db.dr_input.dr_gb
            self._bytes_total = self._gb_total * Size.GB
            if db.dr_input.dr_gb > 0:
                self._pct_bytes = 100.0 * (total_gb_write / db.dr_input.dr_gb) if total_gb_write > 0 else 0.0
                gb_str += " (%.1f%%)" % self._pct_bytes
        self._update_throughput_NL(cur_ts)
        db_queue_len, db_queue_est = db.toc_queue_est(snap=report_is_interval)
        assert db_queue_est >= 0.0
        if historical_perf.perf_data_ready:
            eta = None
            if total_gb_write < db.dr_input.dr_gb:
                # We haven't finished transferring everything, make a reasonable guess at the ETA
                remaining_gb = db.dr_input.dr_gb - total_gb_write
                remaining_file_count = db.dr_input.dr_count - total_count
                remaining_mean_file_size = remaining_gb * Size.GB / max(remaining_file_count, 1)
                projected_rate_mb = historical_perf.projected_rate_mb_by_file_size(remaining_mean_file_size)
                projected_rate_fps = historical_perf.projected_rate_fps()
                est_transfer = 0
                if projected_rate_mb > 0:
                    # how long to transfer remaining_gb
                    est_transfer = remaining_gb * 1000.0 / projected_rate_mb
                elif projected_rate_fps > 0:
                    # how long to transfer remaining file count
                    # (only used when rate_mb is zero)
                    est_transfer = remaining_file_count / projected_rate_fps
                if (est_transfer > 0) or self._almost_finished():
                    eta = cur_ts + est_transfer
                    eta += db_queue_est
                else:
                    eta = cur_ts
            elif total_gb_write == db.dr_input.dr_gb:
                eta = cur_ts + db_queue_est
            else:
                # We've already transferred more data than expected, make a guess that we'll be done soon
                eta = cur_ts + 60.0
            if eta is not None:
                eta = max(eta, cur_ts+db_queue_est)
                self._eta_set_NL(eta, cur_ts)
                eta = math.ceil(eta)
                if not report_is_final:
                    eta_str = self.eta_str()
        blob_str = "blobs=%d" % write_blob_count
        return "elapsed=%f recent(fps/mbps)=%.1f/%.2f cumulative=%.1f/%.2f db_queue_len=%d/%.3f %s %s %s%s" \
          % (elapsed_secs, recent_count, recent_mb, cumulative_count, cumulative_mb,
             db_queue_len, db_queue_est, blob_str, count_str, gb_str, eta_str)

class ReconcileStats(WorkerThreadStats):
    '''
    Statistics for the reconcile phase
    '''
    def _report_short_NL(self,
                         cur_ts,
                         elapsed_secs,
                         clfsload,
                         historical_perf,
                         report_is_interval=False,
                         report_is_final=False):
        '''
        Type-specific internals for report_short().
        Called with self._lock held.
        '''
        db = clfsload.db
        total = clfsload.reconcile_total_count
        count_dir = self._stats['count_dir']
        count_nondir = self._stats['count_nondir']
        count = count_dir + count_nondir
        self._files_total = total
        self._files_completed_total = count
        self._pct_files = (100.0 * (self._files_completed_total / self._files_total)) if self._files_total else 0.0
        if elapsed_secs and count:
            mean = elapsed_secs / count
        else:
            mean = 0.0
        eta = cur_ts
        eta_str = ''
        if total:
            self._pct_files = 100.0 * (count / total)
            if count < total:
                remain = total - count
                eta += mean * remain
        else:
            self._pct_files = 100.0
        db_queue_len, db_queue_est = db.toc_queue_est()
        if (not report_is_final) and count and (elapsed_secs >= 30.0):
            # Loose accounting for db_queue_est. Inaccuracies
            # are typically statisticaly insignificant.
            eta = max(eta, cur_ts+db_queue_est)
            self._eta_set_NL(eta, cur_ts)
            eta_str = self.eta_str()
        return "elapsed=%f mean=%.3f db_queue_len=%d reconciled=%d/%d (%.1f%%)%s" \
          % (elapsed_secs, mean,
             db_queue_len,
             count, total, self._pct_files, eta_str)

class CleanupStats(WorkerThreadStats):
    '''
    Statistics for the cleanup phase
    '''
    _INITIAL_WORKER_THREAD_STATS_SPECIFIC = {'delete_blob_count' : 0,
                                             'delete_blob_secs' : 0,
                                            }
    def _report_short_NL(self,
                         cur_ts,
                         elapsed_secs,
                         clfsload,
                         historical_perf,
                         report_is_interval=False,
                         report_is_final=False):
        '''
        Type-specific internals for report_short().
        Called with self._lock held.
        '''
        db = clfsload.db
        total = clfsload.cleanup_total_count
        count_dir = self._stats['count_dir']
        count_nondir = self._stats['count_nondir']
        count = count_dir + count_nondir
        self._files_total = total
        self._files_completed_total = count
        self._pct_files = (100.0 * (self._files_completed_total / self._files_total)) if self._files_total else 0.0
        if elapsed_secs and count:
            rate = count / elapsed_secs
            mean = elapsed_secs / count
        else:
            rate = count
            mean = 0.0
        eta = cur_ts
        eta_str = ''
        if total:
            self._pct_files = 100.0 * (count / total)
            if count < total:
                remain = total - count
                eta += mean * remain
        else:
            self._pct_files = 100.0
        db_queue_len, db_queue_est = db.toc_queue_est()
        if (not report_is_final) and count and (elapsed_secs >= 30.0):
            # Loose accounting for db_queue_est. Inaccuracies
            # are typically statisticaly insignificant.
            eta = max(eta, cur_ts+db_queue_est)
            self._eta_set_NL(eta, cur_ts)
            eta_str = self.eta_str()
        return "elapsed=%f rate=%.1f mean=%.3f db_queue_len=%d cleaned=%d/%d (%.1f%%)%s" \
          % (elapsed_secs, rate, mean,
             db_queue_len,
             count, total, self._pct_files, eta_str)

class FinalizeStats(WorkerThreadStats):
    '''
    Statistics for the FINALIZE phase.
    These are don't-care, but it shortens the codepaths
    to have stats for every phase.
    '''
    # No specializations for this phase

class TargetObj():
    '''
    Database-independent representation of DbEntTargetObj.
    Comparisons are all filehandle-based; two items with the same
    filehandle are identical, whether or not the other attributes match.
    '''
    def __init__(self,
                 filehandle,
                 ftype,
                 mode,
                 nlink,
                 mtime,
                 ctime,
                 atime,
                 size,
                 uid,
                 gid,
                 dev,
                 first_backpointer,
                 state,
                 source_inode_number,
                 source_path,
                 reconcile_vers=OBJ_VERSION_INITIAL,
                 child_dir_count=0,
                 pre_error_state=None,
                 barrier=None,
                 ic_restored_in_progress=False):
        self._filehandle = Filehandle(filehandle)
        self.ftype = ftype
        self.mode = mode
        self.nlink = nlink
        self.mtime = mtime
        self.ctime = ctime
        self.atime = atime
        self.size = size
        self.uid = uid
        self.gid = gid
        self.dev = dev
        self.first_backpointer = Filehandle(first_backpointer)
        self.state = state
        self.source_inode_number = source_inode_number
        # This trusts the caller to not modify source_path
        # os.fsencode() returns identity for bytes, or encodes str -> bytes
        self._source_path = os.fsencode(source_path)
        self.reconcile_vers = reconcile_vers
        self.child_dir_count = child_dir_count
        self.pre_error_state = pre_error_state if pre_error_state is not None else state
        self.barrier = barrier
        # We only read or write these separate pend_ values from the toc thread.
        # Because only one thread ever reads or modifies these values, we
        # do not need any additional locks for them.
        self.pend_first_backpointer = None
        self.pend_nlink = None

        # ic_* attributes are incore-only
        self.ic_restored_in_progress = ic_restored_in_progress # This entry was in-progress at startup time (restored from DB)
        self.ic_backpointer_map = None

    def __repr__(self):
        return "<%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self.filehandle.hex())

    def __str__(self):
        return "%s(%s,%s)" % (self.__class__.__name__, self.filehandle.hex(), self.source_path_str)

    def __lt__(self, other):
        return self.filehandle < other.filehandle

    def __le__(self, other):
        return self.filehandle <= other.filehandle

    def __eq__(self, other):
        return self.filehandle == other.filehandle

    def __ne__(self, other):
        return self.filehandle != other.filehandle

    def __ge__(self, other):
        return self.filehandle >= other.filehandle

    def __gt__(self, other):
        return self.filehandle > other.filehandle

    def __hash__(self):
        return hash(self.filehandle)

    def __index__(self):
        return self.filehandle.__index__()

    def __del__(self):
        self.drop_barrier()

    @property
    def filehandle(self):
        # If we want to be completely paranoid here, copy before returning.
        # That seems unnecessarily expensive.
        return self._filehandle

    @property
    def source_name(self):
        'Name of this TargetObj in its parent directory'
        return os.path.split(self.source_path_str)[1]

    @property
    def source_path_str(self):
        '''
        source_path in string form
        '''
        return os.fsdecode(self._source_path)

    def has_source_path(self):
        '''
        Is source_path set?
        '''
        return bool(self._source_path)

    @property
    def source_path_bytes(self):
        '''
        source_path in bytes form
        '''
        return self._source_path

    def drop_barrier(self):
        '''
        Release the associated barrier
        '''
        try:
            self.barrier.drop()
            self.barrier = None
        except AttributeError:
            pass

    def to_db_dict(self):
        'Return a dict suitable for SQLAlchemy bulk operations'
        return {'filehandle' : self.filehandle.bytes,
                'ftype' : self.ftype,
                'mode' : self.mode,
                'nlink' : self.nlink,
                'mtime' : self.mtime,
                'ctime' : self.ctime,
                'atime' : self.atime,
                'size' : self.size,
                'uid' : self.uid,
                'gid' : self.gid,
                'dev' : self.dev,
                'first_backpointer' : self.first_backpointer.bytes,
                'state' : self.state,
                'source_inode_number' : self.source_inode_number,
                'source_path_bytes' : self.source_path_bytes,
                'reconcile_vers' : self.reconcile_vers,
                'child_dir_count' : self.child_dir_count,
                'pre_error_state' : self.pre_error_state,
               }

    def to_ordered_dict(self):
        'Return an OrderedDict representing the entry contents'
        kv = [('filehandle', self.filehandle.hex()),
              ('ftype', self.ftype),
              ('mode', oct(self.mode)),
              ('nlink', self.nlink),
              ('mtime', self.mtime),
              ('ctime', self.ctime),
              ('atime', self.atime),
              ('size', self.size),
              ('uid', self.uid),
              ('gid', self.gid),
              ('dev', self.dev),
              ('first_backpointer', self.first_backpointer.hex()),
              ('state', target_obj_state_name(self.state)),
              ('source_inode_number', self.source_inode_number),
              ('source_path', self.source_path_str),
              ('reconcile_vers', self.reconcile_vers),
              ('child_dir_count', self.child_dir_count),
              ('pre_error_state', target_obj_state_name(self.pre_error_state)),
             ]
        if self.ic_backpointer_map:
            kv.append(('ic_backpointer_map', self.ic_backpointer_map.to_dict_pretty()))
        return OrderedDict(kv)

    def to_json(self):
        'Return a JSON representation of the entry contents'
        tmp = {'filehandle' : self.filehandle.hex(),
               'ftype' : self.ftype,
               'mode' : self.mode,
               'nlink' : self.nlink,
               'mtime' : self.mtime,
               'ctime' : self.ctime,
               'atime' : self.atime,
               'size' : self.size,
               'uid' : self.uid,
               'gid' : self.gid,
               'dev' : self.dev,
               'first_backpointer' : self.first_backpointer.hex(),
               'state' : self.state,
               'source_inode_number' : self.source_inode_number,
               'source_path' : self.source_path_bytes.hex(),
               'reconcile_vers' : self.reconcile_vers,
               'child_dir_count' : self.child_dir_count,
               'pre_error_state' : self.pre_error_state,
              }
        if self.ic_backpointer_map is not None:
            tmp['backpointer_map'] = self.ic_backpointer_map.to_json()
        return json.dumps(tmp)

    @classmethod
    def from_json(cls, txt):
        'Reverse operation for to_json() -- reconstitute the object'
        tmp = json.loads(txt)
        filehandle = Filehandle.fromhex(tmp['filehandle'])
        ret = cls(filehandle=filehandle,
                  ftype=tmp['ftype'],
                  mode=tmp['mode'],
                  nlink=tmp['nlink'],
                  mtime=tmp['mtime'],
                  ctime=tmp['ctime'],
                  atime=tmp['atime'],
                  size=tmp['size'],
                  uid=tmp['uid'],
                  gid=tmp['gid'],
                  dev=tmp['dev'],
                  first_backpointer=Filehandle.fromhex(tmp['first_backpointer']),
                  state=tmp['state'],
                  source_inode_number=tmp['source_inode_number'],
                  source_path=bytes.fromhex(tmp['source_path']),
                  reconcile_vers=tmp['reconcile_vers'],
                  child_dir_count=tmp['child_dir_count'],
                  pre_error_state=tmp['pre_error_state'])
        backpointer_map_txt = tmp.get('backpointer_map', None)
        if backpointer_map_txt is not None:
            ret.ic_backpointer_map = AdditionalBackpointers.from_json(filehandle, backpointer_map_txt)
        return ret

    def pformat(self):
        return json.dumps(self.to_ordered_dict(), indent=2)

    @classmethod
    def from_dbe(cls, dbe, ic_restored_in_progress=False):
        'Return an instance based on the corresponding database entry (dbe)'
        return cls(filehandle=dbe.filehandle,
                   ftype=dbe.ftype,
                   mode=dbe.mode,
                   nlink=dbe.nlink,
                   mtime=dbe.mtime,
                   ctime=dbe.ctime,
                   atime=dbe.atime,
                   size=dbe.size,
                   uid=dbe.uid,
                   gid=dbe.gid,
                   dev=dbe.dev,
                   first_backpointer=dbe.first_backpointer,
                   state=dbe.state,
                   source_inode_number=dbe.source_inode_number,
                   source_path=dbe.source_path_bytes,
                   reconcile_vers=dbe.reconcile_vers,
                   child_dir_count=dbe.child_dir_count,
                   pre_error_state=dbe.pre_error_state,
                   ic_restored_in_progress=ic_restored_in_progress)

    def backpointer_count(self):
        'Return the number of backpointers this object has'
        # Do not check self.ic_backpointer_map for non-empty here;
        # the caller must not call this without populating it.
        ret = 1 if self.first_backpointer and (self.first_backpointer != FILEHANDLE_NULL) else 0
        if self.ic_backpointer_map:
            ret += len(self.ic_backpointer_map)
        return ret

    def will_reconcile(self):
        '''
        Return whether this object will be reconciled.
        '''
        if self.reconcile_vers > OBJ_VERSION_INITIAL:
            return True
        if (self.first_backpointer == FILEHANDLE_NULL) and (self.filehandle != FILEHANDLE_ROOT):
            return True
        if self.ftype == Ftype.DIR:
            if self.ic_restored_in_progress:
                return True
        else:
            if self.nlink != 1:
                return True
        return False

    def nlink_effective(self):
        '''
        Return nlink based on current knowledge; assumes
        that ic_backpointer_map is loaded.
        '''
        if self.ftype == Ftype.DIR:
            # The +2 is to match the UFS semantic where . and .. count
            return self.child_dir_count + 2
        return self.backpointer_count()

    def backpointer_list_generate(self, include_null_firstbackpointer=True):
        '''
        Return a complete list of backpointers for this target.
        This list will be empty for the root directory. Objects
        that are orphaned (such as by removing the target from
        a dir that is partially scanned at the time this tool
        is restarted) may also have empty backpointer lists.
        '''
        # Do not check self.ic_backpointer_map for non-empty here;
        # the caller must not call this without populating it.
        if self.ic_backpointer_map:
            ret = self.ic_backpointer_map.filehandle_list_generate()
        else:
            ret = list()
        if self.first_backpointer:
            if include_null_firstbackpointer or (self.first_backpointer != FILEHANDLE_NULL):
                ret.insert(0, self.first_backpointer)
        return ret

    def describe(self):
        '''
        Return a single-line description of this object
        '''
        sp = self.source_path_str
        if sp:
            return "%s (%s)" % (self.filehandle.hex(), sp)
        return self.filehandle.hex()

    def existing_key(self):
        '''
        Return an ExistingTargetObjKey that matches this TargetObj
        '''
        return ExistingTargetObjKey(self.source_inode_number, self.ftype)

DbEntBase = declarative_base()

class DbEntCommon():
    '''
    Code common to DbEnt* objects. May not inherit from DbEntBase
    because doing so requires this to be a table.
    '''
    def to_ordered_dict(self):
        raise NotImplementedError("%s did not implement this method" % self.__class__.__name__)

    def pformat(self):
        return pprint.pformat(self.to_ordered_dict())

class DbEntTargetObj(DbEntBase, DbEntCommon):
    '''
    DbEntTargetObj is an entry in the targetobj database table.
    We store timestamps as separate seconds and nseconds
    for efficiency.
    '''
    __tablename__ = 'targetobj'
    filehandle = Column('filehandle', BINARY(length=Filehandle.fixedlen()), primary_key=True)
    state = Column('state', Integer(), index=True)
    ftype = Column('ftype', Integer(), index=False) # Store as Integer so we can do ordered compares
    mode = Column('mode', Integer(), index=False)
    nlink = Column('nlink', Integer(), index=False)
    mtime = Column('mtime', Float(8), index=False)
    ctime = Column('ctime', Float(8), index=False)
    atime = Column('atime', Float(8), index=False)
    size = Column('size', Integer(), index=False)
    uid = Column('uid', Integer(), index=False)
    gid = Column('gid', Integer(), index=False)
    dev = Column('dev', Integer(), index=False)
    source_inode_number = Column('source_inode_number', Integer(), index=True)
    reconcile_vers = Column('reconcile_vers', Integer(), index=False)
    child_dir_count = Column('child_dir_count', Integer(), index=False)
    pre_error_state = Column('pre_error_state', Integer(), index=False)
    first_backpointer = Column('first_backpointer', BINARY(length=Filehandle.fixedlen()), index=True)
    source_path_bytes = Column('source_path', BINARY(), index=False)
    persisted = None # used by the DB flush code

    # * For getexisting, we could also add: Index('source_inode_number__ftype', 'source_inode_number', 'ftype'), # getexisting
    #   and remove the single-column index on source_inode_number.
    #   Doing so slows down upserts to optimze getexisting, so we don't.
    #   If source_inode_number is not sufficiently unique, then preserve_hardlinks
    #   should not be set.
    __table_args__ = (Index('state__ftype', 'state', 'ftype'), # preclaim
                      Index('reap', 'first_backpointer', 'state', 'nlink'),
                     )

    __mapper_args__ = {'confirm_deleted_rows' : False}

    def __repr__(self):
        return "<%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self.filehandle.hex())

    def __str__(self):
        return "%s(%s,%s)" % (self.__class__.__name__, self.filehandle.hex(), self.source_path_str)

    @property
    def source_path_str(self):
        '''
        Return source_path in str form
        '''
        return os.fsdecode(self.source_path_bytes)

    def to_ordered_dict(self):
        'Return an OrderedDict representing the entry contents'
        kv = [('filehandle', self.filehandle.hex()),
              ('ftype', self.ftype),
              ('mode', oct(self.mode)),
              ('nlink', self.nlink),
              ('mtime', self.mtime),
              ('ctime', self.ctime),
              ('atime', self.atime),
              ('size', self.size),
              ('uid', self.uid),
              ('gid', self.gid),
              ('dev', self.dev),
              ('first_backpointer', self.first_backpointer.hex()),
              ('state', target_obj_state_name(self.state)),
              ('source_inode_number', self.source_inode_number),
              ('source_path', self.source_path_str),
              ('reconcile_vers', self.reconcile_vers),
              ('child_dir_count', self.child_dir_count),
              ('pre_error_state', target_obj_state_name(self.pre_error_state)),
             ]
        return OrderedDict(kv)

    def to_db_dict(self):
        'Return a dict suitable for SQLAlchemy bulk operations'
        return {'filehandle' : self.filehandle,
                'ftype' : self.ftype,
                'mode' : self.mode,
                'nlink' : self.nlink,
                'mtime' : self.mtime,
                'ctime' : self.ctime,
                'atime' : self.atime,
                'size' : self.size,
                'uid' : self.uid,
                'gid' : self.gid,
                'dev' : self.dev,
                'first_backpointer' : self.first_backpointer,
                'state' : self.state,
                'source_inode_number' : self.source_inode_number,
                'source_path_bytes' : self.source_path_bytes,
                'reconcile_vers' : self.reconcile_vers,
                'child_dir_count' : self.child_dir_count,
                'pre_error_state' : self.pre_error_state,
               }

    @classmethod
    def from_ic(cls, tobj):
        'Generate an instance based on the corresponding incore entry (tobj)'
        if tobj.pend_first_backpointer is not None:
            first_backpointer = tobj.pend_first_backpointer.bytes
            # pend_nlink should never be less than nlink, but use the max to be safe.
            # Be sure to set this value to at least 2 so we reconcile the
            # target; if pend_first_backpointer is set then we have done
            # add_backpointer.
            nlink = max(tobj.nlink, tobj.pend_nlink, 2)
        else:
            first_backpointer = tobj.first_backpointer.bytes
            nlink = tobj.nlink
        return cls(filehandle=bytes(tobj.filehandle.bytes),
                   ftype=tobj.ftype,
                   mode=tobj.mode,
                   nlink=nlink,
                   mtime=tobj.mtime,
                   ctime=tobj.ctime,
                   atime=tobj.atime,
                   size=tobj.size,
                   uid=tobj.uid,
                   gid=tobj.gid,
                   dev=tobj.dev,
                   first_backpointer=first_backpointer,
                   state=tobj.state,
                   source_inode_number=tobj.source_inode_number,
                   source_path_bytes=tobj.source_path_bytes,
                   reconcile_vers=tobj.reconcile_vers,
                   child_dir_count=tobj.child_dir_count,
                   pre_error_state=tobj.pre_error_state,
                   persisted=False)

class DbEntAdditionalBackpointerMapEnt(DbEntBase, DbEntCommon):
    '''
    Tracks additional backpointers.
    A single entry counts the number of backpointers from one child to one parent.
    This does not include the first_backpointer for the child. So, for example,
    a child that appears three times, all in the same directory, has a single
    entry in this database. In that entry, filehandle_from is the filehandle
    of the child, filehandle_to is the filehandle of the parent directory, and
    count is 2, because the third link is tracked by first_backpointer in
    the DbEntTargetObj for the child.
    '''
    __tablename__ = 'backpointermap'
    id = Column(Integer, primary_key=True)
    filehandle_from = Column('filehandle_from', BINARY(length=Filehandle.fixedlen()), index=True)
    filehandle_to = Column('filehandle_to', BINARY(length=Filehandle.fixedlen()), index=True)
    count = Column('count', Integer())

    __table_args__ = (Index('filehandle_from', 'filehandle_to'),
                     )

    def __repr__(self):
        return "<%s,%s,%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self.filehandle_from.hex(), self.filehandle_to.hex(), self.count)

    def __str__(self):
        return "%s(%s,%s,%s)" % (self.__class__.__name__, self.filehandle_from.hex(), self.filehandle_to.hex(), self.count)

    def to_ordered_dict(self):
        'Return an OrderedDict representing the entry contents'
        kv = [('filehandle_from', self.filehandle_from.hex()),
              ('filehandle_to', self.filehandle_to.hex()),
              ('count', self.count),
             ]
        return OrderedDict(kv)

class DbEntMeta(DbEntBase, DbEntCommon):
    '''
    The metastore contains a bunch of key/value pairs.
    Keys are the DbEntMetaKey enum. Rather than jump
    through hoops to get the ORM to DTRT, the values
    stored in the DB are the JSON-ified values. When
    values are enums, they are stringified to the
    enum name (not value). On restore, the reverse-mapping
    is special-cased by DbEntMetaKey.value_from_dbe().
    '''
    __tablename__ = 'metastore'
    m_key = Column('m_key', VARCHAR(length=128), primary_key=True)
    m_value = Column('m_value', VARCHAR(length=128))

    def to_ordered_dict(self):
        'Return an OrderedDict representing the entry contents'
        return OrderedDict([(self.m_key, self.m_value)])

class AdditionalBackpointers():
    '''
    This represents the set of all additional backpointers for a single object.
    It encompasses all DbEntAdditionalBackpointerMapEnt for a single filehandle_from.
    This does not include first_backpointer for the given object.
    _map format is self._map[filehandle_to] : count
    '''
    def __init__(self, filehandle, dbe_map_ents):
        '''
        dbe_map_ents is an iterable of DbEntAdditionalBackpointerMapEnt
        '''
        self._map = dict() # key=filehandle_to value=count
        for dbe in dbe_map_ents:
            assert dbe.filehandle_from == filehandle
            if dbe.count:
                self._map[dbe.filehandle_to] = dbe.count

    def __len__(self):
        return sum(self._map.values())

    def __bool__(self):
        return bool(len(self))

    def to_json(self):
        'Convert to a JSON-formatted string'
        tmp = {k.hex() : v for k, v in self._map.items()}
        return json.dumps(tmp)

    @classmethod
    def from_json(cls, filehandle, txt):
        'Reconstitute from a JSON-formatted string -- reverses to_json()'
        tmp = json.loads(txt)
        ret = cls(filehandle, tuple())
        ret._map = {Filehandle.fromhex(k) : v for k, v in tmp.items()} # pylint: disable=protected-access
        return ret

    def filehandle_list_generate(self):
        'Return the complete list of additional backpointers for this object'
        ret = list()
        for filehandle_bytes, count in self._map.items():
            filehandle = Filehandle(filehandle_bytes)
            ret.extend([filehandle for _ in range(count)])
        return ret

    def to_dict_pretty(self):
        'Return a dict where the keys are human-readable (hexed) filehandles and the values are counts'
        return {k.hex() : v for k, v in self._map.items()}

class ExistingTargetObjKey(SorterBase):
    '''
    This is used to search for an already-known TargetObj when we
    encounter a directory entry pointing to a non-DIR with
    nlink > 1.
    '''
    def __init__(self, inode_number, ftype):
        self._inode_number = inode_number
        self._ftype = ftype

    def __str__(self):
        return "%s(%s,%s)" % (self.__class__.__name__, self._inode_number, str(self.ftype))

    def __repr__(self):
        return "<%s,%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self.inode_number, str(self.ftype))

    def __hash__(self):
        return self._inode_number + (self._ftype << 32)

    def _cmp(self, other):
        if self._inode_number < other.inode_number:
            return -1
        if self._inode_number > other.inode_number:
            return 1
        if self._ftype < other.ftype:
            return -1
        if self._ftype > other.ftype:
            return 1
        return 0

    @property
    def inode_number(self):
        return self._inode_number

    @property
    def ftype(self):
        return self._ftype

class ExistingTargetObjKeyBarrier():
    '''
    ExistingTargetObjKeyBarrier represents a logical "hold"
    on the creation of a new TargetObj corresponding to an
    ExistingTargetObjKey.
    '''
    def __init__(self, existing, db, held=True):
        self._existing = existing
        self._db = db
        self._held = held

    def __str__(self):
        return "%s(%s,%s)" % (self.__class__.__name__, self._existing.inode_number, self._existing.ftype)

    def __repr__(self):
        return "<%s,%s,%s,%s>" % (self.__class__.__name__, hex(id(self)), self._existing.inode_number, self._existing.ftype)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.drop()

    def __del__(self):
        self.drop()

    def drop(self):
        '''
        Drop the barrier. This acquires the toc_lock in db.
        This works because that lock is recursively safe.
        '''
        if self._held:
            with self._db.upsert_existing_pending_cond:
                self._db.dbc_existing_key_barrier_end(self._existing)
            self._held = False

class ClassExportedBase():
    'Base class for classes that may be exported to children'
    @classmethod
    def get_import_info(cls):
        '''
        Return a tuple of (module_name, class_name)
        for this class suitable for passing to importlib operations.
        These are later passed to get_reconstituted_class().
        This must be pickle-able so we can send it through
        a multiprocessing.Queue.
        '''
        return (cls.__module__, cls.__name__)

    @classmethod
    def get_reconstituted_class(cls, module_name, class_name):
        '''
        Given the info returned by get_import_info(), return the corresponding class.
        Optimized to avoid extra work when called on itself.
        '''
        if (module_name == cls.__module__) and (class_name == cls.__name__):
            return cls
        m = importlib.import_module(module_name)
        return getattr(m, class_name)

class ReaderWriterBase(ClassExportedBase):
    'Base class shared by Reader and Writer'
    def __init__(self, command_args, logger, run_options, child_process_wrock=None): # pylint: disable=unused-argument
        '''
        Do not reference command_args here.
        When reconstituting a subclass through a parent/child relationship,
        command_args is a string that comes from command_args_to_json().
        '''
        self._logger = logger
        self._run_options = run_options
        self._child_process_wrock = child_process_wrock

    @property
    def logger(self):
        return self._logger

    @staticmethod
    def command_args_add_args(command_args):
        '''
        Add command-line arguments specific to this writer
        '''
        # noop in base class

    @staticmethod
    def command_args_to_json():
        '''
        Convert command-line arguments specific to this writer
        to a JSON-formatted string.
        '''
        # No such command args here in the base class, nor should
        # there ever be. This functionality exists to support
        # testing-only subclasses.
        return json.dumps(dict())

    @staticmethod
    def additional_to_json():
        '''
        Convert ancillary data to JSON. This is reversed post-init
        by apply_additional_json().
        '''
        return json.dumps(dict())

    @staticmethod
    def apply_additional_json(txt):
        '''
        Called post-init to apply the contents of additional_to_json().
        '''
        # Nothing to do here in the base class.

class FakeStat():
    '''
    Sufficiently isomorphic to os.stat_result to substitute
    '''
    def __init__(self,
                 st_mode=None,
                 st_nlink=None,
                 st_mtime=None,
                 st_ctime=None,
                 st_atime=None,
                 st_ino=None,
                 st_size=None,
                 st_uid=None,
                 st_gid=None,
                 st_rdev=None):
        self.st_mode = st_mode
        self.st_nlink = st_nlink
        self.st_mtime = st_mtime
        self.st_ctime = st_ctime
        self.st_atime = st_atime
        self.st_ino = st_ino
        self.st_size = st_size
        self.st_uid = st_uid
        self.st_gid = st_gid
        self.st_rdev = st_rdev

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return "%s(st_mode=%s, st_nlink=%s, st_mtime=%s, st_ctime=%s, st_atime=%s, st_ino=%s, st_size=%s, st_uid=%s, st_gid=%s, st_rdev=%s)" % \
               (self.__class__.__name__, self.st_mode,
                self.st_nlink, self.st_mtime, self.st_ctime,
                self.st_atime, self.st_ino, self.st_size,
                self.st_uid, self.st_gid, self.st_rdev)

    @classmethod
    def from_tobj(cls, tobj):
        return cls(st_mode=tobj.mode,
                   st_nlink=tobj.nlink,
                   st_mtime=tobj.mtime,
                   st_ctime=tobj.ctime,
                   st_atime=tobj.atime,
                   st_ino=tobj.source_inode_number,
                   st_size=tobj.size,
                   st_uid=tobj.uid,
                   st_gid=tobj.gid,
                   st_rdev=tobj.dev)

    @classmethod
    def from_stat(cls, st):
        '''
        Create from something isomorphic to FakeStat (such as a FakeStat
        or a real stat).
        '''
        return cls(st_mode=st.st_mode,
                   st_nlink=st.st_nlink,
                   st_mtime=st.st_mtime,
                   st_ctime=st.st_ctime,
                   st_atime=st.st_atime,
                   st_ino=st.st_ino,
                   st_size=st.st_size,
                   st_uid=st.st_uid,
                   st_gid=st.st_gid,
                   st_rdev=st.st_rdev)

class RunOptions():
    '''
    Per-run (per-CLFSLoad) options.
    This is state that is logically defined at the
    start of a run and does not mutate. All attrigbutes
    of this object are latched; once the value is not None
    is may not change. Attributes may not be deleted.
    '''
    def __init__(self, **kwargs):
        self._lock = threading.Lock()
        self._latch_enabled = False
        self.compression_type = None
        self.containerid = None
        self.encryption_type = None
        self.fsid = None
        self.transfer_pool_enable = None
        self.rootfh = None
        self._latch_enabled = True
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __setattr__(self, name, value):
        if name == '_latch_enabled':
            value = bool(value)
            with self._lock:
                if getattr(self, name, False) and (not value):
                    raise ValueError("may not disable latch")
                super(RunOptions, self).__setattr__(name, value)
        if name == '_lock':
            if hasattr(self, name):
                raise ValueError("may not update _lock")
            super(RunOptions, self).__setattr__(name, value)
            return
        with self._lock:
            if getattr(self, '_latch_enabled', False):
                if not hasattr(self, name):
                    raise AttributeError("no such attribute %s" % name)
                cur = getattr(self, name)
                if (cur is not None) and (cur != value):
                    raise ValueError("may not modify %s.%s once set" % (self.__class__.__name__, name))
            super(RunOptions, self).__setattr__(name, value)

    def __delattr__(self, name):
        # We do not permit deleting attributes from this object.
        # TypeError per the Python docs - indicating that the
        # object does not suppor the operation (del).
        raise TypeError("%s does not support attribute delete" % self.__class__.__name__)

    def __str__(self):
        return self.to_json()

    def __repr__(self):
        return "%s(**%s)" % (self.__class__.__name__, self.to_json())

    def to_json(self):
        '''
        JSON-serialize the logical contents of the object
        '''
        d = {k : v for k, v in vars(self).items() if not k.startswith('_')}
        d['rootfh'] = self.rootfh.hex() if self.rootfh else None
        return json.dumps(d)

    @classmethod
    def from_json(cls, txt):
        jd = json.loads(txt)
        rootfh = jd.get('rootfh', None)
        jd['rootfh'] = Filehandle.fromhex(rootfh) if rootfh else None
        return cls(**jd)

class WRock():
    '''
    WRock is a "worker rock" - it may be passed through the writer
    stack in place of WorkerThreadState. There is no common ancestry,
    but there is a minimal isomorphism.
    attributes/properties:
        name: str, ASCII name of this item
        logger: output logger
        thread_num: integer, value is unique to this item within the address space
        should_run: False iff activity should stop
        stats:
            stat_add(name, val): add val to the named stat
            stat_inc(name): add 1 to the named stat
            stat_update(name, updates): updates is a dict of name:value that add value to the named stat
        timers:
            start(name): start a Timer with the given name and return it
            stop_timer(timer_name, elapsed_secs): the named timer has completed in the given time
        run_options: shared RunOptions
    operations:
        str() and repr() always return name
        hash() returns thread_num
        should_run (property) returns a bool of whether or not to continue
        stats_flush() flush buffered stats and timers updates
    Additionally, when the wrock is created, a writer is passed to __init__.
    At that time, writer.wrock_init() is invoked on the new wrock, allowing
    the writer to initialize any interesting per-thread state. If the writer
    is not passed to __init__() (None is passed instead), the caller is
    responsible for invoking writer.wrock_init() before using wrock
    for writing. Never hold a reference on writer here. Doing so can create
    a circular reference.
    '''
    def __init__(self, logger, run_options, name, thread_num, writer):
        self.logger = logger
        self.run_options = run_options
        self.name = name
        self.thread_num = int(thread_num)
        self.stats = None
        self.timers = None
        if writer:
            writer.wrock_init(self)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name

    def __hash__(self):
        return self.thread_num

    def stats_flush(self):
        '''
        Flush any buffered stats/timers updates
        '''
        # Nothing to do for the default

class ReaderInfo():
    'Single entry returned from Reader.getnextfromdir()'
    def __init__(self, path, name, ftype, ostat):
        self.path = path
        self.name = name
        self.ftype = ftype
        self.ostat = ostat

    def __hash__(self):
        '''
        Hash based on the inode_number because two ReaderInfo can
        be logically the same for two ents that point to the same file.
        '''
        return hash(self.ostat.st_ino)

    def __str__(self):
        return str(self.to_ordered_dict())

    def __repr__(self):
        return repr(self.to_ordered_dict())

    def to_ordered_dict(self):
        'Return an OrderedDict representing the contents'
        kv = [('path', self.path),
              ('ftype', self.ftype),
              ('mode', self.ostat.st_mode),
              ('nlink', self.ostat.st_nlink),
              ('mtime', self.ostat.st_mtime),
              ('ctime', self.ostat.st_ctime),
              ('atime', self.ostat.st_atime),
              ('inode_number', self.ostat.st_ino),
              ('size', self.ostat.st_size),
              ('uid', self.ostat.st_uid),
              ('gid', self.ostat.st_gid),
              ('dev', self.ostat.st_rdev),
             ]
        return OrderedDict(kv)

    def ptxt(self, prefix=''):
        'Return "pretty" text'
        rprefix = '\n' + prefix
        return prefix + "path:         " + self.path \
            + rprefix + "ftype:        " + str(self.ftype) \
            + rprefix + "mode:         " + oct(self.ostat.st_mode) \
            + rprefix + "nlink:        " + str(self.ostat.st_nlink) \
            + rprefix + "mtime:        " + pretty_time(self.ostat.st_mtime) \
            + rprefix + "ctime:        " + pretty_time(self.ostat.st_ctime) \
            + rprefix + "atime:        " + pretty_time(self.ostat.st_atime) \
            + rprefix + "inode_number: " + str(self.ostat.st_ino) \
            + rprefix + "size:         " + str(self.ostat.st_size) \
            + rprefix + "uid:          " + str(self.ostat.st_uid) \
            + rprefix + "gid:          " + str(self.ostat.st_gid) \
            + rprefix + "dev:          " + str(self.ostat.st_rdev)

    @classmethod
    def from_tobj(cls, tobj):
        'Generate an instance based on tobj (TargetObj)'
        return cls(path=tobj.source_path_str,
                   name=os.path.split(tobj.source_path_str)[1],
                   ftype=tobj.ftype,
                   ostat=FakeStat.from_tobj(tobj))
