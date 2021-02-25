# Copyright 2021 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

LEAF_PREFIX = b'\x00'
NODE_PREFIX = b'\x01'
ROOT_CACHE_PATH = ".immudbRoot"

PERMISSION_SYS_ADMIN = 255
PERMISSION_ADMIN = 254
PERMISSION_NONE = 0
PERMISSION_R = 1
PERMISSION_RW = 2

SET_KEY_PREFIX=b'\x00'
SORTED_KEY_PREFIX=b'\x01'

PLAIN_VALUE_PREFIX=b'\x00'
REFERENCE_VALUE_PREFIX=b'\x01'

OLDEST_FIRST=False
NEWEST_FIRST=True

