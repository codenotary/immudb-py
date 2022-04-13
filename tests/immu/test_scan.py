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


import random
import string


def get_random_string(length):
    return ''.join(random.choice(string.printable) for i in range(length))


def test_scan_set(client):
    xset = {}
    for i in range(0, 100):
        xset["scan:{:04X}".format(i).encode(
            'utf8')] = get_random_string(32).encode('utf8')
    ret = client.setAll(xset)
    off = None
    kv = client.scan(None, b"scan:", False, 17, ret.id)
    while len(kv) > 0:
        if len(kv) == 0:
            break
        for k in kv:
            print(k, kv[k], xset[k])
            assert kv[k] == xset[k]
            del xset[k]
            off = k
        kv = client.scan(off, b"scan:", False, 17)
    assert len(xset) == 0


if __name__ == "__main__":
    test_scan_set()
