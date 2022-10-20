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

from random import randint


def test_zfunc(client):
    client.useDatabase(b"defaultdb")
    zsetname = "zset_{:04d}".format(randint(0, 10000)).encode('utf-8')
    keys = []
    for i in range(0, 10):
        key = "zset_key_{:04d}".format(randint(0, 10000))
        value = "zset_value_{:04d}".format(randint(0, 10000))
        keys.append(key)
        resp = client.verifiedSet(key.encode('utf8'), value.encode('utf8'))
        assert resp.verified == True
        if i > 5:
            attx = resp.id
        else:
            attx = 0

        if i > 5:
            resp = client.zAdd(zsetname, float(i), key.encode('utf-8'), attx)
            assert not resp.verified
        else:
            resp = client.verifiedZAdd(zsetname, float(
                i), key.encode('utf-8'), attx)
            assert resp.verified
        lasttx = resp.id
    resp = client.zScan(zsetname, b'zset_key_', 0, 0,
                        True, 10, False, 0.0, 10.0, lasttx)
    assert len(resp.entries) == 10