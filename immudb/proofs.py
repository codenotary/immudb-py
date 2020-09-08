from immudb import consistency, inclusion
from copy import deepcopy


def verify(proof, leaf, prevRoot) -> bool:
    if bytes(proof.leaf) != leaf:
        return False

    path = deepcopy(proof.inclusionPath)

    verifiedInclusion = inclusion.path_verify(
        path,
        proof.at,
        proof.index,
        deepcopy(proof.root),
        deepcopy(proof.leaf)
    )

    if not verifiedInclusion:
        return False

    if prevRoot.index == 0 or len(prevRoot.root) == 0:
        return True

    path = deepcopy(proof.consistencyPath)

    return consistency.verify_path(path, proof.at, prevRoot.index, proof.root, prevRoot.root)
