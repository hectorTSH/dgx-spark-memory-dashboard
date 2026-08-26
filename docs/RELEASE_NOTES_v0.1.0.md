# v0.1.0 — First public release

## Highlights

- **GiB-proportional unified-memory map** for NVIDIA DGX Spark (GB10)
- **Multi-Spark tabs** (remote SSH, local-on-box, or demo)
- **CAN I LOAD NEXT?** fit advisor (Ollama + Hugging Face inventory)
- Hot models from **Ollama / vLLM / llama.cpp / SGLang** + Docker stats
- **macOS menu bar** companion
- **macOS LaunchAgent KeepAlive + 30s watchdog** so the server does not stay dead
- Browser **auto-reconnect** with backoff (keeps last good map during blips)
- Zero required Python deps — `python3 scripts/run.py --demo`

## Quick try

```bash
git clone https://github.com/hectorTSH/dgx-spark-memory-dashboard.git
cd dgx-spark-memory-dashboard
python3 scripts/run.py --demo
# open http://127.0.0.1:7474/
```

## Not in scope

No model launch/unload controls. Complements sparkDash / Spark Studio rather than replacing them.
