import hashlib

from immudb.grpc import schema_pb2
from immudb import constants


def isPowerOfTwo(x: int) -> bool:
    return (x != 0) and ((x & (x-1)) == 0)


def verify_path(path: list, second: int, first: int, secondHash: bytes, firstHash: bytes) -> bool:
    l = len(path)
    if first == second and firstHash == secondHash and l == 0:
        return True
    if not(first < second) or l == 0:
        return False
    pp = path
    if isPowerOfTwo(first + 1):
        pp = [firstHash]
        pp.extend(path)
    fn = first
    sn = second
    while (fn % 2) == 1:
        fn >>= 1
        sn >>= 1
    fr = pp[0]
    sr = pp[0]
    isFirst = True
    for c in pp:
        if isFirst:
            isFirst = False
            continue
        if sn == 0:
            return False
        if (fn % 2) == 1 or fn == sn:
            tmp = bytearray()
            tmp.append(constants.NODE_PREFIX)
            tmp.extend(c)
            tmp.extend(fr)
            fr = hashlib.sha256(tmp).digest()
            tmp = bytearray()
            tmp.append(constants.NODE_PREFIX)
            tmp.extend(c)
            tmp.extend(sr)
            sr = hashlib.sha256(tmp).digest()
            while (fn%2) == 0 and fn != 0:
                fn >>= 1
                sn >>= 1
        else:
            tmp = bytearray()
            tmp.append(constants.NODE_PREFIX)
            tmp.extend(sr)
            tmp.extend(c)
            sr = hashlib.sha256(tmp).digest()
        fn >>= 1
        sn >>= 1
    return fr == firstHash and sr == secondHash and sn == 0

#  FIXME currently not used anywhere except testing
def verify(proof: schema_pb2.ConsistencyProof, prevRoot: schema_pb2.Root) -> bool:
    if proof.first != prevRoot.index:
        return False
    path = proof.path
    verified = verify_path(
        path=proof.path,
        second=proof.second,
        first=proof.first,
        secondHash=proof.secondRoot,
        firstHash=prevRoot.root
    )
    if verified:
        proof.firstRoot = prevRoot.root
        return True
    return False
