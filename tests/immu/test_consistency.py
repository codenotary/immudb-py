import pytest

from immudb import consistency
from immudb.schema import schema_pb2

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
