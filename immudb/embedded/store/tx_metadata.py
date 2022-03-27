from immudb.constants import *
from immudb.printable import printable


class TxMetadata(printable):
    def __init__(self):
        pass

    def Bytes(self):
        # TODO: None vs. b''
        return b''
