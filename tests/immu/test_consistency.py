import pytest

from immudb import consistency
from immudb.schema import schema_pb2
from immudb.client import ImmudbClient
from immudb.consistency import verify
import grpc._channel

class TestConsistency:
    def test_verify_path(self):
        path = []
        assert True == consistency.verify_path(path, 0, 0, bytes(), bytes())
        assert True == consistency.verify_path(path, 1, 1, bytes(), bytes())

        assert False == consistency.verify_path(path, 0, 0, bytes([1]), bytes([2]))
        assert False == consistency.verify_path(path, 0, 1, bytes(), bytes())
        assert False == consistency.verify_path(path, 1, 0, bytes(), bytes())

        path = [bytes(), bytes(), bytes()]
        assert False == consistency.verify_path(path, 2, 1, bytes(), bytes())

    def test_verify_path2(self):
        path =  [b'\x16\x9f\x05\x81\x86\xcfp\x80\xdf\x89\xc1\x16_\xf2\xd1\xa5i\xbb\xb6\x9b\xfe\x0f\xd6:\x80\xcb\xbf\xb2\xa6\xc8"?',  b'...f9\xe1\x04\xb7\xb9\xe0T',  b'\x8f\xab\xdb\xd3t#L\x9ay\xdey\xb3\xdeZ\x93={Wt\xba\xf5\xda\xd2\xc1\xaf \x15\xf5n\x86\xa6d']
        second = 230269
        first = 230268
        secondHash = b'yP\xf2\xbbh\x02.9\x87\x8e\x1b5\x16k\xe2Zk\xdc3\x82\x96\x0b\xde\x80WJ=\xda\xc9\x8b\x9d\xdc'
        firstHash = b"A\xab\x8e,\xe0/\xbb\x13y\x84\x08\xe7\xff\xf5\xbfg\x98\x8d3\xea\xa9\x0fB\xc6\xaa%'\xa3*\xd2\x8e\x0e"
        assert False == consistency.verify_path(path, second, first, secondHash, firstHash)

    def test_consistency_verify(self):
        a=ImmudbClient()
        try:
            a = ImmudbClient("localhost:3322")
            a.login("immudb","immudb")
        except grpc._channel._InactiveRpcError as e:
            pytest.skip("Cannot reach immudb server")
        a.safeSet(b'dummy',b'dummy')
        root=a.currentRoot()
        a.safeSet(b'dummy1',b'dummy1')
        print(root)
        cns=a.stub.Consistency(schema_pb2.Index(index=root.index))
        print(cns)
        ret=verify(cns,root)
        assert ret == True
