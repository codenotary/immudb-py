import hashlib

from immudb.grpc import schema_pb2
from immudb import constants


def path_verify(path:list, at:int, i:int, root:bytes, leaf:bytes) -> bool:
    if i > at or (at > 0 and len(path) == 0):
        return False
    h = leaf
    for v in path:
        c = bytearray()
        c.append(constants.NODE_PREFIX)
        v = bytes(v)
        if i%2 == 0 and i != at:
            c.extend(h)
            c.extend(v)
        else:
            c.extend(v)
            c.extend(h)
        h = hashlib.sha256(c).digest()
        i = i // 2
        at = at // 2
    return at == i and h == root

# FIXME: apparently not used
def verify(inclusionProof: schema_pb2.InclusionProof, index: int, leaf: bytes) -> bool:
    if inclusionProof.index != index or bytes(inclusionProof.leaf) != leaf:
        return False
    return path_verify(
        path=inclusionProof.path,
        at=inclusionProof.at,
        i=inclusionProof.index,
        root=bytes(inclusionProof.root),
        leaf=leaf
        )
