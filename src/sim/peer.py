from collections import Counter, defaultdict
from dataclasses import dataclass, field

import simpy

from msg import Msg, MsgKind, OK, CHOKED, LEFT


CHOKE_POLICIES = ("tft", "random")
PIECE_POLICIES = ("rarest", "sequential", "random")


@dataclass(frozen=True)
class Config:
    n_pieces: int
    piece_size: int                     # bytes
    block_size: int = 16 * 1024         # bytes; piece_size need not be a multiple
    rechoke_interval: float = 10.0      # seconds
    optimistic_every: int = 3           # rechoke rounds
    n_regular_unchoke: int = 4
    share_idle_bandwidth: bool = False  # see Peer.current_rate()
    choke_policy: str = "tft"           # regular slots: "tft" = best rate, "random" = uniform baseline
    piece_policy: str = "rarest"        # next piece: "rarest", "sequential" or "random", see Peer.pick()

    def __post_init__(self):
        if self.choke_policy not in CHOKE_POLICIES:
            raise ValueError(f"choke_policy must be one of {CHOKE_POLICIES}, got {self.choke_policy!r}")
        if self.piece_policy not in PIECE_POLICIES:
            raise ValueError(f"piece_policy must be one of {PIECE_POLICIES}, got {self.piece_policy!r}")


@dataclass
class PeerStats:
    arrival_time: float
    completion_time: float | None = None
    departure_time: float | None = None
    bytes_up: int = 0
    bytes_down: int = 0
    piece_log: list = field(default_factory=list)  # (time, piece, src_pid)


def _sorted(peers):
    return sorted(peers, key=lambda p: p.pid)


class Peer:

    """
        Defines a Peer object that represents a node in a BitTorrent Swarm.
        A Peer is always able to receive message, upload and download pieces to/from
        the swarm. Future implementation decisions will decide whether
        seed/leecher/free-rider are subclasses or tuned versions of this class.
    """

    debug = False   # if True, check_invariants() runs after every dispatcher/download step

    def __init__(self, env, pid, cfg, rng, upload_bw, initial_pieces=(), seed_time=None,
                 on_leave=None, metrics=None, kind=None):
        self.env = env
        self.pid = pid
        self.cfg = cfg
        self.rng = rng
        self.upload_bw = upload_bw
        self.seed_time = seed_time
        self.on_leave = on_leave
        self.metrics = metrics

        self.inbox = simpy.Store(env)
        self.have = set(initial_pieces)
        self.requested = set()
        self.inflight = {}                  # done -> (piece, uploader)
        self.neighbors = set()
        self.neighbor_has = {}
        self.availability = Counter()
        self.unchoked_by = set()
        self.interested_in = set()
        self.unchoked = set()
        self.optimistic = None
        self.interested_peers = set()
        self.transfers = {}                 # downloader -> _serve process
        self.bytes_from = defaultdict(int)
        self.bytes_to = defaultdict(int)
        self.wakeup = env.event()
        self.n_slots = cfg.n_regular_unchoke + 1
        self.alive = True
        self.stats = PeerStats(arrival_time=env.now)
        self.kind = kind or ("seed" if self.is_complete() else "leecher")

        if self.metrics is not None:
            self.metrics.on_join(self)                   # tells Metrics the peer has joined the swarm

        self._procs = [env.process(self._dispatcher()), env.process(self._choker())]
        if self.is_complete():
            self._on_complete()
        else:
            self._procs.append(env.process(self._download()))

    # attribute names expected by metrics.py
    @property
    def id(self):
        return self.pid

    @property
    def up_cap(self):
        return self.upload_bw

    def __repr__(self):
        return f"Peer({self.pid})"

    def send(self, dst, kind, piece=None, done=None):
        dst.inbox.put(Msg(kind, self, piece, done))

    def wake(self):
        self.wakeup.succeed()
        self.wakeup = self.env.event()

    def is_complete(self):
        return len(self.have) == self.cfg.n_pieces

    def _dispatcher(self):
        try:
            while True:
                msg = yield self.inbox.get()
                src = msg.src
                if src not in self.neighbors:                       # stale message from a departed peer
                    if msg.done is not None and not msg.done.triggered:
                        msg.done.succeed(LEFT)
                    continue
                match msg.kind:
                    case MsgKind.REQUEST:
                        if src not in self.unchoked:
                            msg.done.succeed(CHOKED)
                        else:
                            assert src not in self.transfers, "one outstanding request per peer"
                            self.transfers[src] = self.env.process(self._serve(src, msg.piece, msg.done))
                            assert len(self.transfers) <= self.n_slots, "slot invariant broken"
                    case MsgKind.UNCHOKE:
                        self.unchoked_by.add(src)
                        self.wake()
                    case MsgKind.CHOKE:
                        self.unchoked_by.discard(src)               # no wake: a choke never creates work
                    case MsgKind.HAVE:
                        if msg.piece not in self.neighbor_has[src]:
                            self.neighbor_has[src].add(msg.piece)
                            self.availability[msg.piece] += 1
                        self._update_interest(src)
                        self.wake()
                    case MsgKind.INTERESTED:
                        self.interested_peers.add(src)
                        # fill a free slot now instead of waiting for the next rechoke
                        if src not in self.unchoked and len(self.unchoked) < self.n_slots:
                            self.unchoked.add(src)
                            self.send(src, MsgKind.UNCHOKE)
                    case MsgKind.NOT_INTERESTED:
                        self.interested_peers.discard(src)
                if self.debug:
                    self.check_invariants()
        except simpy.Interrupt:
            return

    def _download(self):
        try:
            while not self.is_complete():
                busy = {src for _, src in self.inflight.values()}
                while (t := self.pick(self.unchoked_by - busy)):
                    piece, src = t
                    done = self.env.event()
                    self.send(src, MsgKind.REQUEST, piece, done)
                    self.inflight[done] = (piece, src)
                    self.requested.add(piece)
                    busy.add(src)

                fired = yield self.env.any_of([*self.inflight, self.wakeup])

                for done in [d for d in self.inflight if d in fired]:
                    piece, src = self.inflight.pop(done)
                    self.requested.discard(piece)
                    if fired[done] == OK:
                        self._on_piece(piece, src)
                    else:
                        # CHOKED or LEFT: the CHOKE message may not have been processed yet
                        self.unchoked_by.discard(src)
                if self.debug:
                    self.check_invariants()
        except simpy.Interrupt:
            return

    def _on_piece(self, piece, src):
        self.have.add(piece)
        self.stats.piece_log.append((self.env.now, piece, src.pid))
        if self.metrics is not None:
            self.metrics.on_piece(self, piece)  # peer has a new seed
        for n in _sorted(self.neighbors):
            self.send(n, MsgKind.HAVE, piece)
        for n in _sorted(self.neighbors):
            self._update_interest(n)
        if self.is_complete():
            self._on_complete()

    def _on_complete(self):
        self.stats.completion_time = self.env.now
        if self.seed_time is not None:
            self._procs.append(self.env.process(self._seed_then_leave()))

    def pick(self, candidates):
        """
            Next piece still needed that some candidate holds, as (piece, peer), or None.
            rarest: fewest copies among neighbors. sequential: lowest index.
            random: uniform over pieces. Ties between holders are broken at random.
        """
        policy = self.cfg.piece_policy
        draw = {}                       # one draw per piece, so widely held pieces aren't favored
        best = None
        for peer in _sorted(candidates):
            for piece in sorted(self.neighbor_has[peer] - self.have - self.requested):
                if policy == "rarest":
                    rank = self.availability[piece]
                elif policy == "sequential":
                    rank = piece
                else:
                    if piece not in draw:
                        draw[piece] = self.rng.random()
                    rank = draw[piece]
                key = (rank, self.rng.random())
                if best is None or key < best[0]:
                    best = (key, piece, peer)
        return None if best is None else (best[1], best[2])

    def _update_interest(self, p):
        wants = not (self.neighbor_has[p] <= self.have)
        if wants and p not in self.interested_in:
            self.interested_in.add(p)
            self.send(p, MsgKind.INTERESTED)
        elif not wants and p in self.interested_in:
            self.interested_in.discard(p)
            self.send(p, MsgKind.NOT_INTERESTED)

    def _serve(self, requester, piece, done):
        me = self.env.active_process
        remaining = self.cfg.piece_size
        try:
            while remaining > 0:
                b = min(self.cfg.block_size, remaining)
                yield self.env.timeout(b / self.current_rate(requester))
                remaining -= b
                self.bytes_to[requester] += b
                self.stats.bytes_up += b
                requester.bytes_from[self] += b
                requester.stats.bytes_down += b
            if self.metrics is not None:
                self.metrics.on_transfer(self, requester, piece, optimistic=(requester is self.optimistic))  # uploader sent a piece
            done.succeed(OK)
        except simpy.Interrupt as i:                    # i.cause is CHOKED or LEFT
            if not done.triggered:
                done.succeed(i.cause)
        finally:
            if self.transfers.get(requester) is me:     # don't remove a newer transfer to the same peer
                del self.transfers[requester]

    def current_rate(self, dst):
        if self.cfg.share_idle_bandwidth:
            return self.upload_bw / max(1, len(self.transfers))
        return self.upload_bw / self.n_slots

    def _choker(self):
        rnd = 0
        try:
            while True:
                self._rechoke(rnd)
                rnd += 1
                yield self.env.timeout(self.cfg.rechoke_interval)
        except simpy.Interrupt:
            return

    def _rechoke(self, rnd):
        cands = _sorted(self.interested_peers & self.neighbors)
        if self.cfg.choke_policy == "random":
            regular = self.rng.sample(cands, min(self.cfg.n_regular_unchoke, len(cands)))
        else:
            score = self.bytes_to if self.is_complete() else self.bytes_from
            ranked = sorted(cands, key=lambda p: (-score[p], self.rng.random()))
            regular = ranked[: self.cfg.n_regular_unchoke]
        rest = [p for p in cands if p not in regular]
        if rnd % self.cfg.optimistic_every == 0 or self.optimistic not in rest:
            self.optimistic = self.rng.choice(rest) if rest else None
        new = set(regular) | ({self.optimistic} if self.optimistic else set())

        for p in _sorted(new - self.unchoked):
            self.send(p, MsgKind.UNCHOKE)
        for p in _sorted(self.unchoked - new):
            self.send(p, MsgKind.CHOKE)
            proc = self.transfers.pop(p, None)
            if proc is not None and proc.is_alive:
                proc.interrupt(CHOKED)
        self.unchoked = new
        self.bytes_from.clear()
        self.bytes_to.clear()

    def add_neighbor(self, p):
        if p in self.neighbors:
            return
        self.neighbors.add(p)
        self.neighbor_has[p] = set(p.have)              # bitfield exchange as an instant snapshot
        self.availability.update(self.neighbor_has[p])
        self._update_interest(p)

    def remove_neighbor(self, p):
        if p not in self.neighbors:
            return
        self.neighbors.discard(p)
        self.availability.subtract(self.neighbor_has.pop(p))
        for s in (self.unchoked_by, self.unchoked, self.interested_in, self.interested_peers):
            s.discard(p)
        if self.optimistic is p:
            self.optimistic = None
        proc = self.transfers.pop(p, None)
        if proc is not None and proc.is_alive:
            proc.interrupt(LEFT)
        for done, (_, src) in list(self.inflight.items()):
            if src is p and not done.triggered:
                done.succeed(LEFT)

    def _seed_then_leave(self):
        try:
            yield self.env.timeout(self.seed_time)
        except simpy.Interrupt:
            return
        self.leave()

    def leave(self):
        if not self.alive:
            return
        self.alive = False
        self.stats.departure_time = self.env.now
        for n in _sorted(self.neighbors):
            n.remove_neighbor(self)
            self.remove_neighbor(n)
        for proc in self._procs:
            if proc.is_alive and proc is not self.env.active_process:
                proc.interrupt(LEFT)
        if self.on_leave:
            self.on_leave(self)

    def check_invariants(self):
        assert len(self.unchoked) <= self.n_slots and len(self.transfers) <= self.n_slots
        assert set(self.transfers) <= self.unchoked
        assert self.requested == {piece for piece, _ in self.inflight.values()}
        assert self.requested.isdisjoint(self.have)
        uploaders = [src for _, src in self.inflight.values()]
        assert len(uploaders) == len(set(uploaders))
        assert set(self.neighbor_has) == self.neighbors
        for piece in range(self.cfg.n_pieces):
            assert self.availability[piece] == sum(piece in s for s in self.neighbor_has.values())
