# Architecture notes

## Components

| Path | Role |
|------|------|
| `dgx_spark_memory_dashboard/config.py` | Defaults, YAML/env merge, multi-spark normalization |
| `dgx_spark_memory_dashboard/collector.py` | Shared remote/local Python collector script + demo fixtures + enrichment |
| `dgx_spark_memory_dashboard/server.py` | Threading HTTP server, cache, auth, static UI |
| `web/index.html` | Multi-tab occupancy UI (no build step) |
| `macos-menubar/spark_memory_menubar.py` | rumps companion |

## Collector payload (stable fields)

```json
{
  "ts": 0,
  "hostname": "spark",
  "ollama_tags": {"models": []},
  "ollama_ps": {"models": []},
  "vllm": [{"url": "...", "models": {"data": []}, "metrics_text": "..."}],
  "llamacpp": [],
  "sglang": [],
  "gpu": {"utilization_gpu": 0, "temperature_gpu": 0, "power_draw_w": 0},
  "docker": [{"name": "", "status": "", "mem_usage": ""}],
  "mem": {"total": 0, "available": 0, "free": 0, "cached": 0},
  "disk": {"total": 0, "used": 0, "free": 0},
  "hf_models": [{"name": "", "size_bytes": 0, "size_gib": 0, "incomplete": 0}]
}
```

Server enrichment adds: `spark_id`, `spark_label`, `mode`, `vllm_models`, `*_size_gib`, `fetched_at`.

## Design choices

- **Stdlib-first server** so a Spark or laptop can run it with stock Python 3.9+.
- **One SSH round-trip** per spark per poll (batched remote script), cached ~`poll_seconds`.
- **UI has no framework** — copy `web/index.html` or open through the server.
- **Read-only** — inventory and fit advice only; no process control surface.
