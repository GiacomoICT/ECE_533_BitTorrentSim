# Simulation options

```bash
python src/sim/simulation.py [options]
```

| Option | Values | Default | Sets |
|---|---|---|---|
| `--seeds N` | integer | `5` | Replications (seeds `0..N-1`) |
| `--jobs N` | integer | one per run, up to CPU count | Parallel processes |
| `--out DIR` | path | `results/<timestamp>/` | Output directory |
| `--policy P` | `tft`, `random` | `tft` | Peer selection (regular unchoke slots) |
| `--piece-policy P` | `rarest`, `sequential`, `random` | `rarest` | Piece selection |
| `--capacities C [C ...]` | bytes/s, e.g. `5e5 1e6` | `1000000` | Upload capacity of every peer; several values run a sweep |

## Examples

```bash
python src/sim/simulation.py                                  # defaults
python src/sim/simulation.py --seeds 10 --jobs 4 --out results/run1
python src/sim/simulation.py --policy random                  # random peer selection
python src/sim/simulation.py --piece-policy sequential        # sequential pieces
python src/sim/simulation.py --piece-policy random            # random pieces
python src/sim/simulation.py --capacities 250e3 500e3 1e6 2e6 # capacity sweep
python src/sim/simulation.py --policy random --piece-policy random --capacities 5e5 1e6 --seeds 10 --out results/combo
```
