from enum import Enum

class MsgKind(Enum):
    REQUEST = "REQ"
    UNCHOKE = "UNCHK"
    CHOKE = "CHK"
    HAVE = "HAVE"


class Msg:

    def __init__(self, kind, src, piece, done):
        self.kind = kind
        self.src = src
        self.piece = piece
        self.done = done

