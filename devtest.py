#!/usr/bin/env python
from immu.client import ImmuClient
#from immu.schema import schema_pb2
#from immu.service import schema_pb2_grpc

a = ImmuClient("localhost:3322")
a.login("immudb","immudb")
print(a.databaseUse(b"defaultdb"))

print("List:")
print(a.databaseList())
print("Use:")
print(a.databaseUse(b"defaultdb"))
# print(a.databaseCreate(b"testdb"))

b = a.safeSet(b"pythonkey",b"thisismyvalue")
print(b)
print(a.safeGet(bytes("pythonkey", encoding="utf-8")))

#arr=[]
#for i in range(0,100):
    #k="key_{}".format(i).encode('utf8')
    #v="value_{}".format(i).encode('utf8')
    #a.safeSet(k,v)
    #arr.append(k)
#resp=a.getAll(arr)
#print(resp)


#xset=[
#    {'key':b'gorilla', 'value':b'banana'},
#    {'key':b'zebra',   'value':b'grass'},
#    {'key':b'lion',    'value':b'zebra'},
#    {'key':b'crab',    'value':b'coconut'}
#    ]
#print(a.setAll(xset))
##print(a.getAll(""))
#print(a.getAll([x['key'] for x  in xset]))  


