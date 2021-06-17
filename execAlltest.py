#!/usr/bin/env python
from immudb.client import ImmudbClient
from immudb.datatypes import KeyValue, ZAddRequest, ReferenceRequest

a = ImmudbClient()
a.login("immudb", "immudb")
kv = []
for t in range(0, 100):
    key = "test_sago_key_{:04d}".format(t)
    value = "test_sago_value_{:04d}".format(t)
    kv.append(KeyValue(key=key.encode(), value=value.encode()))

a.execAll(kv)

ref = []
for t in range(0, 100):
    key = "test_sago_ref_{:04d}".format(t)
    referencedKey = "test_sago_key_{:04d}".format(t)
    ref.append(ReferenceRequest(key=key.encode(),
                                referencedKey=referencedKey.encode()))

a.execAll(ref)

zadd = []
for t in range(0, 100):
    key = "test_sago_key_{:04d}".format(t)
    set = "test_sago_set".format(t)
    score = float(100 - t)
    zadd.append(ZAddRequest(key=key.encode(), set=set.encode(), score=score))

a.execAll(zadd)
