import argparse
import csv
import dataclasses
import json
import subprocess
import sys
from collections import deque
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import simulation
from peer import Peer

CFG = dataclasses.replace(simulation.cfg, n_pieces=10)      # small file: tests stay fast
SIM = simulation.sim


def small(**kw):
    return {**SIM, "n_expected": 60, **kw}


def reachable(start):
    seen, todo = {start}, deque([start])
    while todo:
        for n in todo.popleft().neighbors:
            if n not in seen:
                seen.add(n)
                todo.append(n)
    return seen


def run_until_done(env, m, sim):
    env.run(until=env.any_of([m.all_done, env.timeout(sim["until"])]))


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_overlay_limits(seed):
    _, _, swarm, out = simulation.build(CFG, SIM, seed)
    for p in swarm:
        assert p not in p.neighbors
        assert len(p.neighbors) <= SIM["n_neighbors"]
        assert len(out[p]) <= SIM["max_initiate"]
        assert out[p] <= p.neighbors
        for q in p.neighbors:
            assert p in q.neighbors                         # connections are symmetric
            assert (q in out[p]) != (p in out[q])           # opened by exactly one side
    assert reachable(swarm[0]) == set(swarm)                # every leecher can get pieces from the seed


def test_overlay_small_swarm_is_full_mesh():
    sim = {**SIM, "n_expected": 20}                          # 21 peers < n_neighbors
    _, _, swarm, _ = simulation.build(CFG, sim, 0)
    for p in swarm:
        assert p.neighbors == set(swarm) - {p}


def test_overlay_initiate_cap_binds():
    sim = {**SIM, "max_initiate": 3}
    _, _, swarm, out = simulation.build(CFG, sim, 0)
    assert max(len(out[p]) for p in swarm) == 3
    assert all(len(p.neighbors) <= SIM["n_neighbors"] for p in swarm)


def test_full_run():
    rows, series = simulation.run_one(CFG, SIM, 0)
    leechers = [r for r in rows if r["kind"] == "leecher"]
    seeds = [r for r in rows if r["kind"] == "seed"]
    assert len(leechers) == SIM["n_expected"] and len(seeds) == SIM["n_seeds"]
    assert all(r["ct"] is not None and r["ct"] > 0 for r in leechers)
    assert all(r["down"] == CFG.n_pieces for r in leechers)
    assert sum(r["up"] for r in rows) == SIM["n_expected"] * CFG.n_pieces
    assert all(r["seed"] == 0 for r in rows)

    assert series and series[0]["t"] == 0
    assert [s["t"] for s in series] == sorted(s["t"] for s in series)
    assert series[0]["n_leechers"] == SIM["n_expected"]
    assert series[-1]["t"] == max(r["t_complete"] for r in leechers)   # stopped at all_done, final state sampled
    assert series[-1]["n_leechers"] == 0 and series[-1]["n_seeds"] == len(rows)


def test_run_stops_at_horizon():
    rows, series = simulation.run_one(CFG, {**SIM, "until": 3}, 0)
    assert all(r["ct"] is None for r in rows if r["kind"] == "leecher")
    assert series[-1]["t"] == 3


def test_metrics_match_peers(monkeypatch):
    monkeypatch.setattr(Peer, "debug", True)                 # invariants checked on every event
    sim = small()
    env, m, swarm, _ = simulation.build(CFG, sim, 3)
    run_until_done(env, m, sim)
    assert m.all_done.triggered and m.n_done == sim["n_expected"]
    assert m.n_online == len(swarm)
    assert m.n_seeds == len(swarm)
    for piece in range(CFG.n_pieces):
        assert m.replicas[piece] == sum(piece in p.have for p in swarm)
    for p in swarm:
        p.check_invariants()
        r = m.rec[p.pid]
        if r["kind"] == "leecher":
            assert r["t_complete"] == p.stats.completion_time
            assert sorted(piece for _, piece, _ in p.stats.piece_log) == list(range(CFG.n_pieces))
    assert sum(p.stats.bytes_up for p in swarm) == sum(p.stats.bytes_down for p in swarm)


def test_choking_is_exercised(monkeypatch):
    '''Download spans several rechoke rounds: tit-for-tat and optimistic unchoke actually run.'''
    monkeypatch.setattr(Peer, "debug", True)
    cfg = dataclasses.replace(CFG, n_pieces=150)
    sim = small(n_expected=10)
    env, m, swarm, _ = simulation.build(cfg, sim, 4)
    rounds = set()
    orig = Peer._rechoke
    monkeypatch.setattr(Peer, "_rechoke", lambda self, rnd: (rounds.add(env.now), orig(self, rnd))[1])
    run_until_done(env, m, sim)
    assert m.n_done == sim["n_expected"]
    assert len(rounds) > cfg.optimistic_every                    # reaches round optimistic_every: one rotation after t=0
    assert sum(r["via_optimistic"] for r in m.rec.values()) > 0
    for p in swarm:
        p.check_invariants()


def test_determinism():
    run = lambda seed: repr(simulation.run_one(CFG, small(), seed))     # repr: entropy can be nan, and nan != nan
    assert run(5) == run(5)
    assert run(5) != run(6)


def test_random_policy_full_run(monkeypatch):
    monkeypatch.setattr(Peer, "debug", True)
    cfg = dataclasses.replace(CFG, choke_policy="random")
    rows, series = simulation.run_one(cfg, small(), 0)
    assert all(r["ct"] is not None for r in rows if r["kind"] == "leecher")
    assert {r["policy"] for r in rows} == {s["policy"] for s in series} == {"random"}


@pytest.mark.parametrize("policy", ["rarest", "sequential", "random"])
def test_piece_policy_full_run(monkeypatch, policy):
    monkeypatch.setattr(Peer, "debug", True)
    cfg = dataclasses.replace(CFG, piece_policy=policy)
    rows, series = simulation.run_one(cfg, small(), 0)
    assert all(r["ct"] is not None and r["down"] == cfg.n_pieces for r in rows if r["kind"] == "leecher")
    assert {r["piece_policy"] for r in rows} == {s["piece_policy"] for s in series} == {policy}


@pytest.mark.parametrize("change", [{"choke_policy": "random"}, {"piece_policy": "sequential"},
                                    {"piece_policy": "random"}])
def test_policies_share_the_same_overlay(change):
    '''Same seed, same overlay: runs of different policies can be compared pairwise.'''
    edges = lambda c: {p.pid: {q.pid for q in p.neighbors} for p in simulation.build(c, SIM, 0)[2]}
    assert edges(CFG) == edges(dataclasses.replace(CFG, **change))


def test_cli_policy_flag(tmp_path):
    script = Path(simulation.__file__)
    code = ("import sys, dataclasses, simulation as s;"
            "s.cfg = dataclasses.replace(s.cfg, n_pieces=10); s.sim['n_expected'] = 20;"
            f"sys.argv = ['simulation.py', '--seeds', '1', '--jobs', '1', '--policy', 'random', '--piece-policy', 'sequential', '--out', {str(tmp_path)!r}];"
            "s.main()")
    subprocess.run([sys.executable, "-c", code], cwd=script.parent, check=True, capture_output=True)
    conf = json.loads((tmp_path / "config.json").read_text())["cfg"]
    assert conf["choke_policy"] == "random" and conf["piece_policy"] == "sequential"
    peers = list(csv.DictReader(open(tmp_path / "peers.csv")))
    assert {r["policy"] for r in peers} == {"random"} and {r["piece_policy"] for r in peers} == {"sequential"}


def test_cli_capacity_sweep(tmp_path):
    script = Path(simulation.__file__)
    code = ("import sys, dataclasses, simulation as s;"
            "s.cfg = dataclasses.replace(s.cfg, n_pieces=10); s.sim['n_expected'] = 20;"
            "sys.argv = ['simulation.py', '--seeds', '2', '--jobs', '1', '--capacities', '1e6', '500000', '1e6',"
            f" '--out', {str(tmp_path)!r}];"
            "s.main()")
    subprocess.run([sys.executable, "-c", code], cwd=script.parent, check=True, capture_output=True)

    peers = list(csv.DictReader(open(tmp_path / "peers.csv")))
    series = list(csv.DictReader(open(tmp_path / "series.csv")))
    summary = list(csv.DictReader(open(tmp_path / "summary.csv")))
    conf = json.loads((tmp_path / "config.json").read_text())
    assert len(peers) == 2 * 2 * 21                             # the repeated 1e6 runs once
    assert {r["upload_bw"] for r in peers} == {r["upload_bw"] for r in series} == {"500000", "1000000"}
    assert all(r["up_cap"] == r["upload_bw"] for r in peers)    # the capacity reached the peers
    assert conf["sim"]["upload_bw"] == [500000, 1000000]
    assert [int(r["upload_bw"]) for r in summary] == [500000, 1000000]
    assert all(r["n_runs"] == "2" and r["n_done"] == r["n_leechers"] == "40" for r in summary)
    assert float(summary[0]["ct_mean"]) > float(summary[1]["ct_mean"])


def test_summarize_reports_unfinished_leechers():
    row = lambda seed, ct: {"upload_bw": 10, "seed": seed, "kind": "leecher", "ct": ct}
    [s] = simulation.summarize([row(0, 2.0), row(0, 4.0), row(1, 6.0), row(1, None),
                                {"upload_bw": 10, "seed": 0, "kind": "seed", "ct": 0.0}])
    assert (s["n_runs"], s["n_leechers"], s["n_done"]) == (2, 4, 3)
    assert s["ct_mean"] == pytest.approx(4.0) and s["ct_max"] == 6.0
    [empty] = simulation.summarize([row(0, None)])
    assert empty["n_done"] == 0 and empty["ct_mean"] is None and empty["ct_max"] is None


@pytest.mark.parametrize("bad", ["0", "-5", "abc", "inf", "nan"])
def test_capacity_rejects_invalid_values(bad):
    with pytest.raises(argparse.ArgumentTypeError):
        simulation._capacity(bad)


def test_cli_writes_outputs(tmp_path):
    script = Path(simulation.__file__)
    code = ("import runpy, sys, dataclasses, simulation as s;"
            "s.cfg = dataclasses.replace(s.cfg, n_pieces=10); s.sim['n_expected'] = 20;"
            f"sys.argv = ['simulation.py', '--seeds', '2', '--jobs', '1', '--out', {str(tmp_path)!r}];"
            "s.main()")
    subprocess.run([sys.executable, "-c", code], cwd=script.parent, check=True, capture_output=True)

    peers = list(csv.DictReader(open(tmp_path / "peers.csv")))
    series = list(csv.DictReader(open(tmp_path / "series.csv")))
    conf = json.loads((tmp_path / "config.json").read_text())
    assert len(peers) == 2 * 21 and {r["seed"] for r in peers} == {"0", "1"}
    assert {r["seed"] for r in series} == {"0", "1"}
    assert conf["seeds"] == [0, 1] and conf["cfg"]["n_pieces"] == 10
