import simpy, random
from msg import Msg, MsgKind



class Peer:

    """
        Defines a Peer object that represents a node in a BitTorrent Swarm. 
        A Peer is always able to receive message, upload and download pieces to/from
        the swarm. Future implementation decisions will decide whether 
        seed/leecher/free-rider are subclasses or tuned versions of this class.

        To instantiate a peer, one need to pass as argument
        
        - env: environment of Simpy
        - properties: dict that contains all the initialization details of the peer
    """

    def __init__(self, env, properties):
        self.inbox = simpy.Store(env)
        #TODO spin processes
        self.unchoked_by = set()
        self.unchoked_peers = set()
        self.neighbor_pieces = {}
        self.env = env



    def dispatcher(self):
        #TODO: reads inbox and filters messages to call apposite handler
        while True:
            msg = yield self.inbox.get()
            if msg.kind == MsgKind.REQUEST:
                self.env.process(self.serve(msg.src, msg.piece, msg.done))
            elif msg.kind == MsgKind.UNCHOKE:
                self.unchoked_by.add(msg.src)
                self.wake();
            elif msg.kind == MsgKind.CHOKE:
                self.unchoked_by.discard(msg.src)
            elif msg.kind == MsgKind.HAVE:
                self.neighbor_pieces[msg.src].add(msg.piece)
                self.wake()

    def choker(self):
        while True:
            #TODO define choking/ unchoking alg
            a = 0;

    def serve(self, src, piece, done):
        #TODO: called by dispatcher upon request, represents the upload 
        a = 0;

    def download(self):
        #TODO: download loop, spinned at the start and woken up by dispatcher
        a = 0;

    def wake(self):
        #TODO: wakes up the downloader, informing that a UNCHOKE message arrived
        a = 0;

    

