# Copyright 2022 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from immudb.constants import *
from immudb.exceptions import ErrNonExpirable, ErrReadOnly
import datetime


DELETED_ATTR_CODE = 0
EXPIRES_AT_ATTR_CODE = 1
NON_INDEXABLE_ATTR_CODE = 2


class KVMetadata():
    def __init__(self):
        self.attributes = dict()
        self.readonly = False

    def AsDeleted(self, deleted: bool):
        if self.readonly:
            raise ErrReadOnly
        if not deleted:
            self.attributes.pop(DELETED_ATTR_CODE, None)
        else:
            self.attributes[DELETED_ATTR_CODE] = None

    def Deleted(self) -> bool:
        return DELETED_ATTR_CODE in self.attributes

    def ExpiresAt(self, expiresAt: datetime.datetime):
        if self.readonly:
            raise ErrReadOnly
        self.attributes[EXPIRES_AT_ATTR_CODE] = expiresAt

    def NonExpirable(self) -> bool:
        self.attributes.pop(EXPIRES_AT_ATTR_CODE, None)

    def IsExpirable(self) -> bool:
        return EXPIRES_AT_ATTR_CODE in self.attributes

    def ExpirationTime(self) -> datetime.datetime:
        if EXPIRES_AT_ATTR_CODE in self.attributes:
            return self.attributes[EXPIRES_AT_ATTR_CODE]
        else:
            raise ErrNonExpirable

    def AsNonIndexable(self, nonIndexable: bool):
        if self.readonly:
            raise ErrReadOnly
        if not nonIndexable:
            self.attributes.pop(NON_INDEXABLE_ATTR_CODE, None)
        else:
            self.attributes[NON_INDEXABLE_ATTR_CODE] = None

    def NonIndexable(self) -> bool:
        return NON_INDEXABLE_ATTR_CODE in self.attributes

    def Bytes(self):
        b = b''
        for attrCode in [DELETED_ATTR_CODE, EXPIRES_AT_ATTR_CODE, NON_INDEXABLE_ATTR_CODE]:
            if attrCode in self.attributes:
                b = b+attrCode.to_bytes(1, 'big')
                if attrCode == EXPIRES_AT_ATTR_CODE:
                    # The attribute is a datetime. Convert it to epoch integer big endian 64 bit
                    b = b + \
                        int(self.attributes[attrCode].replace(tzinfo=datetime.timezone.utc).timestamp()).to_bytes(
                            8, 'big')
        return b
