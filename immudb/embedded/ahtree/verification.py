from immudb.constants import *
import hashlib


def VerifyInclusion(iproof: list, i: int, j: int, iLeaf: bytes, jRoot: bytes) -> bool:
    if i > j or i == 0 or i < j and len(iproof) == 0:
        return False
    ciRoot = EvalInclusion(iproof, i, j, iLeaf)
    return jRoot == ciRoot


def EvalInclusion(iproof: list, i: int, j: int, iLeaf: bytes) -> bool:
    i1 = i - 1
    j1 = j - 1
    ciRoot = iLeaf
    for h in iproof:
        if i1 % 2 == 0 and i1 != j1:
            b = NODE_PREFIX+ciRoot+h
        else:
            b = NODE_PREFIX+h+ciRoot
        ciRoot = hashlib.sha256(b).digest()
        i1 = i1 >> 1
        j1 = j1 >> 1
    return ciRoot


def VerifyConsistency(cproof: list, i: int, j: int, iRoot: bytes, jRoot: bytes) -> bool:
    if i > j or i == 0 or (i < j and len(cproof) == 0):
        return False
    if i == j and len(cproof) == 0:
        return iRoot == jRoot
    ciRoot, cjRoot = EvalConsistency(cproof, i, j)
    return iRoot == ciRoot and jRoot == cjRoot


def EvalConsistency(cproof: list, i: int, j: int):
    fn = i - 1
    sn = j - 1
    while fn % 2 == 1:
        fn = fn >> 1
        sn = sn >> 1
    ciRoot, cjRoot = cproof[0], cproof[0]
    for h in cproof[1:]:
        if fn % 2 == 1 or fn == sn:
            b = NODE_PREFIX+h+ciRoot
            ciRoot = hashlib.sha256(b).digest()
            b = NODE_PREFIX+h+cjRoot
            cjRoot = hashlib.sha256(b).digest()
            while fn % 2 == 0 and fn != 0:
                fn = fn >> 1
                sn = sn >> 1
        else:
            b = NODE_PREFIX+cjRoot+h
            cjRoot = hashlib.sha256(b).digest()
        fn = fn >> 1
        sn = sn >> 1
    return ciRoot, cjRoot


def VerifyLastInclusion(iproof: list, i: int, leaf: bytes, root: bytes) -> bool:
    if i == 0:
        return False
    return root == EvalLastInclusion(iproof, i, leaf)


def EvalLastInclusion(iproof: list, i: int, leaf: bytes) -> bytes:
    i1 = i - 1
    root = leaf
    for h in iproof:
        b = NODE_PREFIX+h+root
        root = hashlib.sha256(b).digest()
        i1 >>= 1
    return root
