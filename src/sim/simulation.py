"""
    Script starting the simulation.

    1) Start and init the peers
    2) Handle Peer arrival (TODO: every peer currently joins at t=0)
    3) Collect statistics

    Usage (from anywhere, with the project venv):
        python src/sim/simulation.py                 # 5 seeds, results/<timestamp>/
        python src/sim/simulation.py --seeds 10 --jobs 4 --out results/baseline

    Writes peers.csv (one row per peer per seed), series.csv (one row per sample per seed)
    and config.json (the cfg and sim used) into the output directory.
"""
import argparse, csv, dataclasses, json, os, time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import metrics, simpy, peer, random


# Protocol parameters, read by every Peer (must be a peer.Config, Peer uses attribute access)
cfg = peer.Config(
        n_regular_unchoke=4,
        n_pieces=200,               # 200 x 256 KiB = 50 MiB: downloads last several rechoke rounds
        block_size=16384,
        share_idle_bandwidth=True,
        rechoke_interval=10,        # seconds, as in BitTorrent
        optimistic_every=3,         # rechoke rounds -> optimistic unchoke rotates every 30 s
        piece_size=262144,
)

# Simulation-level parameters (not part of peer.Config)
sim = {
        "n_expected": 200,          # non-seed peers joining the run
        "n_seeds": 1,               # peers starting with every piece
        "upload_bw": 1000000,       # bytes/s per peer
        "until": 3600,              # safety cap, seconds (the run stops as soon as every leecher completes)
        "sample_dt": 1.0,           # seconds between two rows of the series
        "n_neighbors": 50,          # max neighbors per peer (initiated + accepted)
        "max_initiate": 40,         # max connections a peer opens itself; the rest are left for incoming
}


def connect(p, candidates, rng, out, n_neighbors, max_initiate):
    '''
        p contacts the candidates in random order and opens connections until it has
        initiated max_initiate of them or has n_neighbors neighbors.
        A candidate that already has n_neighbors refuses the connection.
        out[p] is the set of peers p initiated a connection to.
    '''
    cands = [q for q in candidates if q is not p and q not in p.neighbors]
    rng.shuffle(cands)
    for q in cands:
        n_out = len(out[p] & p.neighbors)          # still-open connections p initiated
        if n_out >= max_initiate or len(p.neighbors) >= n_neighbors:
            break
        if len(q.neighbors) >= n_neighbors:
            continue                                # q is full
        p.add_neighbor(q)
        q.add_neighbor(p)
        out[p].add(q)


def build(cfg, sim, seed):
    '''
        Create the environment, the metrics and the swarm (peers + overlay), without running it.
    '''
    env = simpy.Environment()
    m = metrics.Metrics(env, cfg.n_pieces, n_expected=sim["n_expected"], params={"seed": seed})
    swarm = []

    for i in range(sim["n_seeds"] + sim["n_expected"]):
        pieces = range(cfg.n_pieces) if i < sim["n_seeds"] else ()     # first n_seeds peers are seeds
        swarm.append(peer.Peer(env, i, cfg, random.Random(f"peer-{seed}-{i}"), sim["upload_bw"],
                               initial_pieces=pieces, on_leave=m.on_leave, metrics=m))

    rng = random.Random(f"overlay-{seed}")         # separate stream from the peers' rngs
    out = {p: set() for p in swarm}
    for p in rng.sample(swarm, len(swarm)):        # peers contact the swarm in random order
        connect(p, swarm, rng, out, sim["n_neighbors"], sim["max_initiate"])

    return env, m, swarm, out


def run_one(cfg, sim, seed):
    env, m, _, _ = build(cfg, sim, seed)
    env.process(m.sampler(sim["sample_dt"]))
    env.run(until=env.any_of([m.all_done, env.timeout(sim["until"])]))      # all leechers done, or horizon
    if not m.series or m.series[-1]["t"] != env.now:
        m.sample()                                  # make the series end on the final state
    return list(m.rec.values()), m.series


def _run_seed(args):
    cfg, sim, seed = args
    t0 = time.perf_counter()
    rows, series = run_one(cfg, sim, seed)
    return seed, rows, series, time.perf_counter() - t0


def write_csv(path, rows):
    fields = list(dict.fromkeys(k for r in rows for k in r))     # union of keys, first-seen order
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, restval="")
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description="Run the BitTorrent swarm simulation for several seeds.")
    ap.add_argument("--seeds", type=int, default=5, help="number of replications, seeds 0..N-1 (default 5)")
    ap.add_argument("--jobs", type=int, default=None, help="parallel processes (default: one per seed, up to the CPU count)")
    ap.add_argument("--out", type=Path, default=None, help="output directory (default: results/<timestamp>/)")
    a = ap.parse_args()

    out = a.out or Path(__file__).resolve().parents[2] / "results" / datetime.now().strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True, exist_ok=True)
    jobs = a.jobs or min(a.seeds, os.cpu_count() or 1)
    print(f"{a.seeds} seeds, {jobs} jobs, {sim['n_expected']} leechers + {sim['n_seeds']} seed(s), "
          f"{cfg.n_pieces} x {cfg.piece_size // 1024} KiB pieces -> {out}")

    all_rows, all_series, incomplete = [], [], 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for seed, rows, series, wall in ex.map(_run_seed, [(cfg, sim, s) for s in range(a.seeds)]):
            cts = sorted(r["ct"] for r in rows if r["kind"] == "leecher" and r["ct"] is not None)
            n_lee = sum(r["kind"] == "leecher" for r in rows)
            incomplete += n_lee - len(cts)
            med = f"{cts[len(cts) // 2]:.1f}" if cts else "-"
            print(f"  seed {seed}: {len(cts)}/{n_lee} leechers done, median ct {med} s, "
                  f"ended t={series[-1]['t']:.1f} s, wall {wall:.1f} s")
            all_rows += rows
            all_series += series

    write_csv(out / "peers.csv", all_rows)
    write_csv(out / "series.csv", all_series)
    (out / "config.json").write_text(json.dumps(
        {"cfg": dataclasses.asdict(cfg), "sim": sim, "seeds": list(range(a.seeds))}, indent=2))
    if incomplete:
        print(f"WARNING: {incomplete} leechers did not complete before until={sim['until']} s")
    print(f"wrote {out / 'peers.csv'}, {out / 'series.csv'}, {out / 'config.json'}")


if __name__ == "__main__":
    main()
