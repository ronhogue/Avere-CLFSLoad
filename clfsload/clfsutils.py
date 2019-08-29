#
# clfsload/clfsutils.py
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

import io
import struct

from clfsload.types import Btype, Filehandle, OBJ_VERSION_INITIAL, ObCacheId
from clfsload.util import NSEC_PER_SEC_FLOAT, STRUCT_LE_U32

def time_secs_nsecs(mtime):
    'Convert float mtime to a tuple of (int(secs), int(nsecs))'
    secs = int(mtime)
    nsecs = int((mtime-secs) * NSEC_PER_SEC_FLOAT)
    return secs, nsecs

def fnv1_hash(somestr):
    '''
    Translated to python from src/util/Hash.h -- note that this
    hash function is broken so we cannot use the fnvHash python library,
    since persistent data used this hash function.
    '''
    # hval = 0x811c9dc5 # initial Magic value for fnv1 hash
    fnvprime = 0x01000193
    fnvsize = 2**32
    hval = 0
    for byte in somestr:
        hval = (fnvprime * hval) % fnvsize
        hval ^= byte & 0xFF
    return hval

def murmur_hash2(data):
    '''translated to python from src/util/Hash.c'''

    seed = 0x8c6749ad
    mval = 0x5bd1e995
    rval = 24
    remain = len(data)
    stream = io.BytesIO(data)
    hval = seed ^ remain
    while remain >= 4:
        (kval,) = struct.unpack(STRUCT_LE_U32, stream.read(4))
        kval = (kval * mval) & 0xFFFFFFFF
        kval = kval ^ ((kval >> rval) & 0xFFFFFFFF)
        kval = (kval * mval) & 0xFFFFFFFF

        hval = (hval * mval) & 0xFFFFFFFF
        hval = hval ^ kval

        remain -= 4

    if 0 < remain < 4:
        hbytes = stream.read(4)
        if remain == 3:
            hval ^= hbytes[2] << 16
            remain -= 1 # execute next if-block

        if remain == 2:
            hval ^= hbytes[1] << 8
            remain -= 1 # execute next if-block

        if remain == 1:
            hval ^= hbytes[0]

        hval = (hval * mval) & 0xFFFFFFFF

    hval ^= (hval >> 13)
    hval = (hval * mval) & 0xFFFFFFFF
    hval ^= (hval >> 15)

    return hval

def hash64(objId):
    '''generate 64-bit hash prefix for cloud bucket names'''
    fhash = fnv1_hash(objId.bytes)
    mhash = murmur_hash2(objId.bytes)
    return (fhash << 32) | mhash

def generate_bucket_name(objId, ownerObjId, objType, version=OBJ_VERSION_INITIAL, nameprefix=None):
    '''
    Generate bucketnames given oid and owner obj id.
    Special objects that are outside the filesystem tree have V1 names.
    All other objects have V2 names
    '''
    if isinstance(objId, Filehandle):
        objId = ObCacheId(objId)

    if objId.bucketNameVersionIsV1():
        nameVersion = 1
    else:
        nameVersion = 2

    if objId.isRootId():
        ownerObjId = objId
        objType = Btype.BTYPE_DIR

    if nameVersion == 1:
        prefix = fnv1_hash(objId.bytes)
    else:
        prefix = hash64(objId)

    parentHash = hash64(ownerObjId)
    snapshotid = 0xFFFFFFFF
    if nameVersion == 1:
        name = "%08X_%s.%011u.%08X.%04u" % (prefix, str(objId).upper(), version, snapshotid, nameVersion)
    else:
        name = "%016X_%s.%04u.%016X.%011u.%08X.%04u" % (prefix, objId.__str__().upper(), objType, parentHash, version, snapshotid, nameVersion)
    if not nameprefix:
        return name
    return nameprefix+name
