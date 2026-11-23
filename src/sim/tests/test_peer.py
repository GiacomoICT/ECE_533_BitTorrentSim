import random
import sys
from collections import Counter
from itertools import combinations
from pathlib import Path

import pytest
import simpy

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from metrics import Metrics
from msg import MsgKind, CHOKED
from peer import Config, Peer

BW = 5 * 16 * 1024        # 16 KiB/s per slot -> one 16 KiB block per second
CFG = Config(n_pieces=10, piece_size=64 * 1024)
HORIZON = 2000


def connect(*peers):
    for a, b in combinations(peers, 2):
        a.add_neighbor(b)
        b.add_neighbor(a)


def make(env, pid, cfg=CFG, bw=BW, seed=False, rng_seed=0, **kw):
    pieces = range(cfg.n_pieces) if seed else ()
    return Peer(env, pid, cfg, random.Random(rng_seed * 1000 + pid), bw, pieces, **kw)


def spy_sends(env, peer, log):
    orig = peer.send
    def send(dst, kind, piece=None, done=None):
        log.append((env.now, peer.pid, dst.pid, kind))
        orig(dst, kind, piece, done)
    peer.send = send


def at(env, t, fn):
    def proc():
        yield env.timeout(t)
        fn()
    env.process(proc())


@pytest.mark.parametrize("share, expected", [
    (False, CFG.n_pieces * CFG.piece_size * (CFG.n_regular_unchoke + 1) / BW),
    (True, CFG.n_pieces * CFG.piece_size / BW),
])
def test_one_seed_one_leecher(share, expected):
    cfg = Config(n_pieces=CFG.n_pieces, piece_size=CFG.piece_size, share_idle_bandwidth=share)
    env = simpy.Environment()
    s, l = make(env, 0, cfg, seed=True), make(env, 1, cfg)
    connect(s, l)
    env.run(until=HORIZON)
    assert l.stats.completion_time == pytest.approx(expected)
    assert l.stats.bytes_down == s.stats.bytes_up == cfg.n_pieces * cfg.piece_size
    assert sorted(p for _, p, _ in l.stats.piece_log) == list(range(cfg.n_pieces))


def test_choke_during_transfer():
    env = simpy.Environment()
    s1, s2, l = make(env, 0, seed=True), make(env, 1, seed=True), make(env, 2)
    connect(s1, s2, l)
    state = {}

    def choke_l():
        state["piece"] = next(p for p, src in l.inflight.values() if src is s1)
        s1.interested_peers.discard(l)      # s1 will not unchoke l again
        s1._rechoke(1)

    at(env, 1.5, choke_l)
    env.run(until=2)
    p = state["piece"]
    assert p not in l.requested and p not in l.have
    assert s1 not in l.unchoked_by
    assert not s1.transfers
    assert [src for _, src in l.inflight.values()] == [s2]
    l.check_invariants(); s1.check_invariants()

    env.run(until=HORIZON)
    assert l.stats.completion_time is not None
    assert all(src == s2.pid for t, _, src in l.stats.piece_log if t > 1.5)
    assert any(piece == p for _, piece, _ in l.stats.piece_log)


def test_unchoke_during_transfer():
    env = simpy.Environment()
    s1, l = make(env, 0, seed=True), make(env, 1)
    connect(s1, l)
    log = []
    spy_sends(env, l, log)

    def join_s2():
        assert any(src is s1 for _, src in l.inflight.values())
        s2 = make(env, 2, seed=True)
        spy_sends(env, s2, log)
        connect(s2, l)

    at(env, 1.5, join_s2)
    env.run(until=HORIZON)
    unchoke = [t for t, src, dst, k in log if (src, dst, k) == (2, 1, MsgKind.UNCHOKE)]
    requests = [t for t, src, dst, k in log if (src, dst, k) == (1, 2, MsgKind.REQUEST)]
    assert unchoke == [1.5]
    assert requests[0] == 1.5
    assert l.stats.completion_time is not None


def test_uploader_leaves_during_transfer():
    env = simpy.Environment()
    left = []
    s1 = make(env, 0, seed=True, on_leave=left.append)
    s2, l = make(env, 1, seed=True), make(env, 2)
    connect(s1, s2, l)

    at(env, 1.5, s1.leave)
    env.run(until=2)
    assert left == [s1] and not s1.alive
    assert s1.stats.departure_time == 1.5
    assert s1 not in l.neighbors and s1 not in l.unchoked_by
    assert all(src is not s1 for _, src in l.inflight.values())
    assert not any(p.is_alive for p in s1._procs)
    l.check_invariants(); s2.check_invariants()

    env.run(until=HORIZON)
    assert l.stats.completion_time is not None


def test_rejected_request():
    cfg = Config(n_pieces=CFG.n_pieces, piece_size=CFG.piece_size, n_regular_unchoke=0)  # 1 slot
    env = simpy.Environment()
    u, a, d = make(env, 0, cfg, seed=True), make(env, 1, cfg), make(env, 2, cfg)
    connect(u, a)
    connect(u, d)       # a already holds u's only slot
    result = {}

    def request():
        yield env.timeout(1)
        assert d not in u.unchoked
        done = env.event()
        d.send(u, MsgKind.REQUEST, 0, done)
        result["value"] = yield done
        result["t"] = env.now

    env.process(request())
    env.run(until=2)
    assert result == {"value": CHOKED, "t": 1}
    assert set(u.transfers) == {a}


def run_swarm(rng_seed, n_leechers=10, seed_time=None, check_every=None):
    cfg = Config(n_pieces=20, piece_size=32 * 1024)
    env = simpy.Environment()
    bw_rng = random.Random(rng_seed)
    seed = make(env, 0, cfg, seed=True, rng_seed=rng_seed)
    leechers = [make(env, i, cfg, bw=bw_rng.choice([BW // 2, BW, 2 * BW]), rng_seed=rng_seed,
                     seed_time=seed_time) for i in range(1, n_leechers + 1)]
    peers = [seed, *leechers]
    connect(*peers)

    if check_every:
        def checker():
            while True:
                for p in peers:
                    if p.alive:
                        p.check_invariants()
                yield env.timeout(check_every)
        env.process(checker())

    env.run(until=HORIZON)
    return seed, leechers


def test_determinism():
    def fingerprint(rng_seed):
        _, leechers = run_swarm(rng_seed)
        return [(l.stats.completion_time, l.stats.piece_log) for l in leechers]

    assert fingerprint(7) == fingerprint(7)


def test_smoke(monkeypatch):
    monkeypatch.setattr(Peer, "debug", True)
    seed, leechers = run_swarm(1, n_leechers=20, check_every=0.5)
    assert all(l.stats.completion_time is not None for l in leechers)
    up = sum(p.stats.bytes_up for p in [seed, *leechers])
    assert up == sum(l.stats.bytes_down for l in leechers)
    assert up >= 20 * seed.cfg.n_pieces * seed.cfg.piece_size   # aborted transfers count too


def test_seed_then_leave(monkeypatch):
    monkeypatch.setattr(Peer, "debug", True)
    seed, leechers = run_swarm(2, n_leechers=15, seed_time=5, check_every=0.5)
    for l in leechers:
        assert l.stats.completion_time is not None
        assert l.stats.departure_time == pytest.approx(l.stats.completion_time + 5)
        assert not l.alive and not l.neighbors
    assert not seed.neighbors


def regular_picks(policy, rounds=200):
    '''Rechoke repeatedly with 4 fast and 4 idle interested peers; count who gets a regular slot.'''
    cfg = Config(n_pieces=CFG.n_pieces, piece_size=CFG.piece_size, choke_policy=policy)
    env = simpy.Environment()
    u, *others = [make(env, i, cfg) for i in range(9)]
    connect(u, *others)
    u.interested_peers = set(others)
    fast = others[:4]
    picks = Counter()
    for rnd in range(rounds):
        for p in fast:
            u.bytes_from[p] = 1000
        u._rechoke(rnd)
        assert len(u.unchoked) == cfg.n_regular_unchoke + 1
        picks.update(p.pid for p in u.unchoked if p is not u.optimistic)
    return {p.pid for p in fast}, picks


def test_tft_policy_keeps_fastest_peers():
    fast, picks = regular_picks("tft")
    assert set(picks) == fast and all(n == 200 for n in picks.values())


def test_random_policy_ignores_rates():
    fast, picks = regular_picks("random")
    assert set(picks) == set(range(1, 9))
    idle_share = sum(n for pid, n in picks.items() if pid not in fast) / sum(picks.values())
    assert idle_share == pytest.approx(0.5, abs=0.1)


def test_unknown_policy_rejected():
    with pytest.raises(ValueError):
        Config(n_pieces=1, piece_size=1, choke_policy="nope")


def picker(policy, holdings):
    '''A leecher whose neighbors hold the given piece sets; returns it and the neighbors.'''
    cfg = Config(n_pieces=CFG.n_pieces, piece_size=CFG.piece_size, piece_policy=policy)
    env = simpy.Environment()
    l = make(env, 0, cfg)
    others = [Peer(env, i, cfg, random.Random(i), BW, pieces) for i, pieces in enumerate(holdings, 1)]
    for p in others:
        l.add_neighbor(p)
    return l, others


def test_rarest_picks_fewest_copies():
    l, others = picker("rarest", [{0, 1}, {0}])
    assert l.pick(set(others)) == (1, others[0])


def test_sequential_picks_lowest_needed_index():
    l, others = picker("sequential", [{0, 1, 2, 3}, {0, 3}])
    assert l.pick(set(others))[0] == 0
    l.have.add(0)
    l.requested.add(1)
    assert l.pick(set(others)) == (2, others[0])


def test_random_is_uniform_over_pieces():
    l, others = picker("random", [{0}] * 5 + [{1}])      # piece 0 has 5 holders, piece 1 has one
    picks = Counter(l.pick(set(others))[0] for _ in range(2000))
    assert picks[1] / 2000 == pytest.approx(0.5, abs=0.05)


@pytest.mark.parametrize("policy", ["rarest", "sequential", "random"])
def test_pick_returns_none_without_useful_pieces(policy):
    l, others = picker(policy, [{0}])
    l.have.add(0)
    assert l.pick(set(others)) is None


def test_unknown_piece_policy_rejected():
    with pytest.raises(ValueError):
        Config(n_pieces=1, piece_size=1, piece_policy="nope")


def test_metrics_hooks():
    env = simpy.Environment()
    m = Metrics(env, CFG.n_pieces, n_expected=3, params={})
    s = make(env, 0, seed=True, metrics=m)
    leechers = [make(env, i, metrics=m) for i in (1, 2, 3)]
    connect(s, *leechers)
    env.run(until=HORIZON)
    assert m.all_done.triggered and m.n_done == 3
    assert m.rec[0]["kind"] == "seed" and m.rec[1]["kind"] == "leecher"
    assert sum(r["up"] for r in m.rec.values()) == 3 * CFG.n_pieces
    for l in leechers:
        r = m.rec[l.pid]
        assert r["down"] == CFG.n_pieces
        assert r["t_complete"] == l.stats.completion_time
        assert r["from_seed"] == sum(src == 0 for _, _, src in l.stats.piece_log)
