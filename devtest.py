#!/usr/bin/env python
from immudb.client import ImmudbClient

a = ImmudbClient()
a.login("immudb","immudb")
kvs={}
for t in range(0,100):
        value="test_sago_value_{:04d}".format(t)
        key="test_sago_key_{:04d}".format(t)
        kvs[key.encode('ascii')]=value.encode('ascii')
a.setAll(kvs)

