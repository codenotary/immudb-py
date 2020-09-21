import pytest
from immudb.client import ImmudbClient

import random
import string
import hashlib
from multiprocessing import Process

def msgSHA256(msg):
    rawMsgEncoded = msg.encode()
    msgHash = hashlib.sha256(rawMsgEncoded).hexdigest()
    return msgHash

def msgDigest(msg):
    msgHash = msgSHA256(msg)
    key = msgHash[:16]
    return key

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))




class TestMultiProcessing:
    def worker(self,rep):
        a=ImmudbClient()
        a.login("immudb","immudb")
        for i in range(0,rep):
            msg_len=random.randint(50,1000)
            msg_txt= get_random_string(msg_len)
            msg_key = msgDigest(msg_txt)
            a.safeSet(msg_key.encode(),msg_txt.encode())
            res=a.safeGet(msg_key.encode())
            assert res.verified
            
    def test_multiprocess_safeset(self):
        proclist=[]
        for i in range(0,4):
            proclist.append(Process(target=self.worker, args=(2000,)))
        for p in proclist:    
            p.start()
        for p in proclist:
            p.join()
    
