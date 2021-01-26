#!/usr/bin/env python
from immudb.client import ImmudbClient

a = ImmudbClient()
a.login("immudb","immudb")
print(a.databaseUse(b"defaultdb"))
print(a.listUsers())
print(a.currentRoot())
#print(a.databaseCreate(b"testdb"))
#print(a.databaseList())
#print("Use:")
#print(a.databaseUse(b"defaultdb"))

# print(a.databaseUse(b"testdb"))
#print(a.set(b"randomkey",b"randomvalue"))
#b = a.safeSet(b"pythonkey",b"thisismyvalue")
#print(b)
#print(a.safeGet(bytes("pitone", encoding="utf-8")))
#print(a.safeGet(bytes("gorilla", encoding="utf-8")))
#for i in range(0,100):
#    print(a.safeGet(bytes("key{}".format(i), encoding="utf-8")))
#print(a.safeSet(b"varano",b"banano"))
#print(a.getAll([b"gorilla",b"key97"]))

#arr=[]
#for i in range(0,100):
    #k="aakey_{}".format(i).encode('utf8')
    #v="value_{}".format(i).encode('utf8')
    #a.safeSet(k,v)
    #arr.append(k)
#resp=a.getAll(arr)
#print(resp)


#xset = {b"key3": b"value3", b"key4": b"value4"}
#print(a.setAll(xset))
#print(a.getAll(xset.keys()))

#print(a.safeSet(b'keykey2', b'valval2'))
#print(a.getValue(b'keykeya2'))
#print(a.history(b'key1',0,0,0))
