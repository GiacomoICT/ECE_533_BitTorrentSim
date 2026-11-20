from enum import Enum

class MsgKind(Enum):
    REQUEST = "REQ"
    UNCHOKE = "UNCHK"
    CHOKE = "CHK"
    HAVE = "HAVE"
    INTERESTED = "INT"
    NOT_INTERESTED = "NOT_INT"


# value carried by a REQUEST's `done` event
OK, CHOKED, LEFT = "ok", "choked", "left"


class Msg:

    def __init__(self, kind, src, piece, done):
        self.kind = kind
        self.src = src
        self.piece = piece
        self.done = done
