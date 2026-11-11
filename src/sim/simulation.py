"""
    Script starting the simulation. 
    Should 

    1) Start and init the peers
    2) Handle Peer arrival 
    3) Collect statistics
"""
import metrics, simpy, peer

#TODO: script containing actual simulation
def run_one(cfg, seed):
    env = simpy.Environment()
    m = Metrics(env, cfg["n_pieces"], n_expected=cfg["n_peers"], params=params)
    swarm = [Peer(env, dict(metrics=m, ...))]

    return list(m.rec.values()), m.series