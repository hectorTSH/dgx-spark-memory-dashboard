# DGX Spark Memory Dashboard

**Live GiB-accurate unified-memory map for NVIDIA DGX Spark (GB10).**  
See what’s loaded, what’s on disk, and what still fits — before you OOM the box.

> Not a fleet Grafana clone. Not a model launcher.  
> The question this answers: *“What is eating my ~128 GB right now, and what can I still load?”*

![license](https://img.shields.io/badge/license-MIT-green)
![python](https://img.shields.io/badge/python-3.9%2B-blue)
![platform](https://img.shields.io/badge/platform-DGX%20Spark%20%7C%20GB10-76B900)

## Why this exists

DGX Spark’s CPU and GPU share one **unified memory pool**. Classic `nvidia-smi` VRAM bars lie or return `[N/A]`.  
Most Spark dashboards show host telemetry (CPU/GPU/net). Launchers help you start engines.  
This project is the **occupancy map + fit advisor** in between:

| Panel | What you get |
|-------|----------------|
| **Memory blocks** | ~1 GiB tiles — Dense (green), MoE (purple), system/other (gray), free (outlined) |
| **HOT IN RAM** | Ollama `/api/ps` + live vLLM / llama.cpp / SGLang models |
| **CAN I LOAD NEXT?** | On-disk Ollama + HF candidates ranked against free RAM + headroom |
| **Engines / Docker** | Container status + docker stats memory when available |
| **Multi-Spark tabs** | One browser window, N Sparks (remote SSH and/or local) |
| **Swap log** | Load / unload events with particle bursts |
| **macOS menu bar** | Free GiB + hot count at a glance |

## Quick start

### Demo mode (no hardware)

```bash
git clone https://github.com/hectorTSH/dgx-spark-memory-dashboard.git
cd dgx-spark-memory-dashboard
python3 scripts/run.py --demo
# open http://127.0.0.1:7474/
```

### On the Spark (local collector)

```bash
cp config.example.yaml config.yaml
# set sparks[0].mode: local
python3 scripts/run.py -c config.yaml
```

### From a laptop (SSH remote)

```bash
cp config.example.yaml config.yaml
# edit host / user / ssh_key
python3 scripts/run.py -c config.yaml --host 0.0.0.0
# optional token when binding non-loopback:
#   DGX_SMD_TOKEN=secret python3 scripts/run.py -c config.yaml --host 0.0.0.0 --token secret
```

Open `http://127.0.0.1:7474/` on the laptop, or `http://<laptop-tailscale-ip>:7474/` on your phone (same tailnet).

### Install (optional)

```bash
pip install -e .
dgx-spark-memory-dashboard --demo
```

Zero required dependencies. Optional: `PyYAML` for richer config parsing (a built-in subset parser works without it), `rumps` for the macOS menu bar.

## Architecture

```
Browser  ──HTTP──►  dashboard server (:7474)
                      │
                      ├─ mode: local   → collector on this host
                      ├─ mode: remote  → SSH + python collector on Spark
                      └─ mode: demo    → synthetic GB10 state
```

API (JSON):

| Endpoint | Purpose |
|----------|---------|
| `GET /api/health` | Liveness |
| `GET /api/config` | Spark list + settings (no secrets) |
| `GET /api/sparks` | All spark states |
| `GET /api/spark-state?id=` | Single spark (compat) |
| `GET /api/spark/<id>` | Single spark |

Optional auth: set `server.token` / `DGX_SMD_TOKEN`. Send `Authorization: Bearer …` or `?token=`.

## Configuration

See [`config.example.yaml`](./config.example.yaml) and [`.env.example`](./.env.example).

Multi-spark env one-liner:

```bash
export DGX_SMD_SPARKS="spark-1|lab-a|remote|user@100.1.1.1:22|~/.ssh/id_ed25519;spark-2|lab-b|remote|user@100.2.2.2:22|~/.ssh/id_ed25519"
python3 scripts/run.py --host 127.0.0.1
```

## macOS menu bar

```bash
python3 -m pip install --user rumps
# server must already be running
python3 macos-menubar/spark_memory_menubar.py
```

Env: `DGX_SMD_URL`, `DGX_SMD_TOKEN`, `DGX_SMD_SPARK_ID`, `DGX_SMD_MENUBAR_POLL`.

## Occupancy accounting

1. **Ollama hot models** — prefer live `size` from `/api/ps`
2. **vLLM** — prefer **docker stats** memory when a single vLLM container is up; else name-based estimate; scrape `/metrics` for KV cache % when exposed
3. **llama.cpp / SGLang** — OpenAI `/v1/models` probes on configured ports
4. **System/other** — `used − accounted_model_weights` (OS, page cache, KV, runtime overhead)
5. **Free** — `MemAvailable` from `/proc/meminfo` (correct on GB10 UMA)

Units default to **GiB**; the UI can toggle GiB/GB.

## Non-goals (v0.1)

- No model launch / unload / docker start-stop controls (see Spark Studio, DGX-Model-Manager)
- Not a full Prometheus/Grafana stack (see spark-dashboard, sparkDash)
- Not a substitute for `spark-doctor` diagnostics

## Related projects

| Project | Niche |
|---------|--------|
| [MiaAI-Lab/sparkDash](https://github.com/MiaAI-Lab/sparkDash) | Multi-unit fleet monitor + engine tok/s |
| [niklasfrick/spark-dashboard](https://github.com/niklasfrick/spark-dashboard) | GPU + vLLM Prometheus metrics |
| [TheAwaken1/Spark-Studio](https://github.com/TheAwaken1/Spark-Studio) | Launch / recipe control plane |
| [calico88x/DGX-Model-Manager](https://github.com/calico88x/DGX-Model-Manager) | Model inventory control |
| [joeynyc/spark-doctor](https://github.com/joeynyc/spark-doctor) | CLI diagnostics |
| [engineering87/sparkfit](https://github.com/engineering87/sparkfit) | Static 128 GB planner |

## Security

- Default bind is `127.0.0.1`. Binding `0.0.0.0` without a token prints a warning — anyone on the network can read model inventory and host stats.
- SSH uses `BatchMode` + your key. Keys never leave your machine; only collector stdout JSON is pulled.
- No write/control actions in this project.

## License

MIT — see [LICENSE](./LICENSE).

## Disclaimer

Independent community project. Not affiliated with or endorsed by NVIDIA.
