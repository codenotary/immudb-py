import hashlib
import struct

from immu.schema import schema_pb2
from immu import constants


def digest(index: int, key:bytes, value:bytes) -> bytes:
    c = bytearray()
    c.append(constants.LEAF_PREFIX)
    c.extend(struct.pack('>Q', index))
    c.extend(struct.pack('>Q', len(key)))
    c.extend(key)
    c.extend(value)
    return hashlib.sha256(c).digest()
