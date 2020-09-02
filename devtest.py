#!/usr/bin/env python
from immu.client import ImmuClient
#from immu.schema import schema_pb2
#from immu.service import schema_pb2_grpc

a = ImmuClient("localhost:3322")
a.login("immudb","immudb")

a.safeSet(b"thisismykey",b"thisismyvalue")
print(a.safeGet(b"thisismykey").value)

#arr=[]
#for i in range(0,100):
    #k="key_{}".format(i).encode('utf8')
    #v="value_{}".format(i).encode('utf8')
    #a.safeSet(k,v)
    #arr.append(k)
#resp=a.getAll(arr)
#print(resp)


xset=[
    {'key':b'gorilla', 'value':b'banana'},
    {'key':b'zebra',   'value':b'grass'},
    {'key':b'crab',    'value':b'coconut'}
    ]
print(a.setAll(xset))
     


