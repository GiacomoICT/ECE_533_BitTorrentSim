import math

def norm_entropy(counts):
    """
        Shannon entropy of piece copy counts, scaled to [0, 1]; 1 = perfectly balanced.
        counts[i] is the number of peers holding piece i
    """
    total = sum(counts)
    if total == 0:
        return float("nan")
    h = -sum(c / total * math.log(c / total) for c in counts if c > 0)
    return h / math.log(len(counts))

class Metrics:
    # Sees the whole swarm. Peers REPORT to it; they never READ it to make decisions.
    def __init__(self, env, n_pieces, n_expected, params):
        self.env = env
        self.M = n_pieces                   # number of pieces that composes the file
        self.n_expected = n_expected        # number of non-seed peers will join the run
        self.params = params                # params describing the run

        # Global swarm state
        self.replicas = [0] * n_pieces      # c_i = number of online peers holding piece i, +1 when peer joins with pieces or receives a new piece, -1 when peer leaves
        self.n_online = 0                   # n of peers in the swarm
        self.n_seeds = 0                    # n of seeds
        self.n_done = 0                     # n of leechers that downloaded all the pieces

        # Collect results
        self.rec = {}                       # peer_id -> one row of peers.csv, dict from peer id to peer's record, ie type, arrival time,...
        self.piece_t = {}                   # peer_id -> time each piece arrived
        self.series = []                    # rows of series.csv
        self.all_done = env.event()         # using .succeed() will trigger it, when all pieces arrives

    def on_join(self, peer):
        '''
            Function executed when a new peer joins the swarm
        '''
        self.rec[peer.id] = dict(self.params, peer=peer.id, kind=peer.kind, up_cap=peer.up_cap,
                                 t_arrival=self.env.now, t_complete=None, ct=None,
                                 up=0, down=0, from_seed=0, via_optimistic=0)
        self.piece_t[peer.id] = []      # initialize list with pieces arrival time
        self.n_online += 1
        for p in peer.have:
            self.replicas[p] += 1       # add all the pieces that the peer has to replicas
        if len(peer.have) == self.M:
            self.n_seeds += 1           # if peer already have all the pieces it is a seed

    def on_transfer(self, up, down, piece, optimistic):      # called by the UPLOADER
        '''
            Function called by the uploader.
            up is the uploader peer
            down is downloader peer
            optimistic: True if downloader got this piece because it was in uploader’s optimistic-unchoke slot
        '''
        self.rec[up.id]["up"] += 1                  # uploader have send one more piece
        r = self.rec[down.id]                       # id of downloader
        r["down"] += 1                              # downloader have downloaded one extra piece
        r["from_seed"] += up.kind == "seed"         # +1 if piece downloaded from seed
        r["via_optimistic"] += bool(optimistic)     # +1 if piece downloaded from optimistic unchoke

    def on_piece(self, peer, piece):                         # called by the DOWNLOADER
        '''
            Function called by the downloader. It says that the peer now have that piece
        '''
        self.replicas[piece] += 1                   # one more peer in the swarm has that piece
        self.piece_t[peer.id].append(self.env.now)  # arrival time of piece i
        if len(peer.have) == self.M:
            self._complete(peer)                    # if last piece arrived peer is complete

    def on_leave(self, peer):
        '''
            Function called by a peer leaving the swarm.
            Remove it's count in the metrics
        '''
        self.n_online -= 1          
        for p in peer.have:
            self.replicas[p] -= 1
        if len(peer.have) == self.M:
            self.n_seeds -= 1

    def _complete(self, peer):
        '''
            Function called whenever a peer downloads its last piece needed

        '''
        r, t = self.rec[peer.id], self.piece_t[peer.id]
        r["t_complete"] = self.env.now                          
        r["ct"] = self.env.now - r["t_arrival"]                 # completition time to download the whole file
        r["t_first"] = t[0] - r["t_arrival"]                    # wait to receive the first piece
        gaps = sorted(b - a for a, b in zip(t, t[1:]))          # times between two pieces
        if gaps:
            r["last_gap"] = t[-1] - t[-2]                       # wait for the LAST piece
            r["median_gap"] = gaps[len(gaps) // 2]              # typical time between pieces, the median
        k = math.ceil(0.95 * len(t)) - 1                        # find the moment the peer reached 95%
        r["tail_frac"] = (t[-1] - t[k]) / r["ct"] if r["ct"] else 0.0  # time spent on last 5% of pieces / total completion time
        self.n_seeds += 1                                       # add to count one more seed available
        self.n_done += 1                                        # add to count one more peer has completed the download
        if self.n_done == self.n_expected and not self.all_done.triggered:
            self.all_done.succeed()                             # trigger event if all peers have completed the download

    def sample(self):
        '''
            Take a snapshot of the system
        '''
        lc = [c - self.n_seeds for c in self.replicas]        # copies held by LEECHERS only, with 2 seeds and replicas = [5, 2, 7], you get lc = [3, 0, 5]
        self.series.append(dict(self.params, 
                                t=self.env.now, 
                                entropy=norm_entropy(lc),                   # entropy
                                min_rep=min(lc),                            # pieces with lowest n. of leechers that held them
                                n_absent=sum(c == 0 for c in lc),           # pieces without leechers
                                n_leechers=self.n_online - self.n_seeds,    # number of leechers
                                n_seeds=self.n_seeds))                      # number of seeds

    def sampler(self, dt):
        '''
            Calls sample every dt units. Must define dt, can compute every time a piece arrives, but more computations required.
        '''
        while True:
            self.sample()
            yield self.env.timeout(dt)