#!/usr/bin/env python
from immudb.client import ImmudbClient

a = ImmudbClient()
a.login("immudb","immudb")
print(a.databaseUse(b"defaultdb"))
#print(a.databaseCreate(b"testdb"))
#print(a.databaseList())
#print("Use:")
#print(a.databaseUse(b"defaultdb"))

# print(a.databaseUse(b"testdb"))

b = a.safeSet(b"pythonkey",b"thisismyvalue")
print(b)
print(a.safeGet(bytes("gorilla", encoding="utf-8")))

# arr=[]
# for i in range(0,100):
#     k="key_{}".format(i).encode('utf8')
#     v="value_{}".format(i).encode('utf8')
#     a.safeSet(k,v)
#     arr.append(k)
# resp=a.getAll(arr)
# print(resp)


#xset = {b"key3": b"value3", b"key4": b"value4"}
#print(a.setAll(xset))
#print(a.getAll(xset.keys()))

#print(a.safeSet(b'keykey2', b'valval2'))
#print(a.get(b'keykey2'))
#print(a.history(b'key1',0,0,0))
