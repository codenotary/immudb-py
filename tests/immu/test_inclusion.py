import pytest

from immudb import inclusion
from immudb.schema import schema_pb2

class TestInclusion:
    def test_verify_path(self):
        path = []
        assert True == inclusion.path_verify(path, 0, 0, bytes(), bytes())
        assert False == inclusion.path_verify(path, 0, 1, bytes(), bytes())
        assert False == inclusion.path_verify(path, 1, 0, bytes(), bytes())
        assert False == inclusion.path_verify(path, 1, 1, bytes(), bytes())
