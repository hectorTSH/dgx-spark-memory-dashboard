# NVIDIA Developer Forum — draft post

**Category:** DGX Spark / GB10 → Projects  
**Title:** DGX Spark Memory Dashboard — live GiB occupancy map + “can I load next?”

---

Hi all 👋

I kept OOMing my Spark when juggling Ollama + vLLM on the 128 GB unified pool. `nvidia-smi` memory fields are `[N/A]` on GB10, and the existing dashboards are great at host telemetry or launching models — I wanted a simple answer to:

> What’s actually resident right now, and what still fits?

So I open-sourced **DGX Spark Memory Dashboard**:

**Repo:** https://github.com/hectorTSH/dgx-spark-memory-dashboard  
**License:** MIT

### What it shows
- ~1 GiB block map (Dense / MoE / system / free)
- HOT IN RAM from Ollama + vLLM (+ llama.cpp / SGLang probes)
- Full on-disk Ollama + Hugging Face inventory
- **CAN I LOAD NEXT?** ranked against free RAM + headroom
- Multi-Spark tabs, load/unload log, optional macOS menu bar
- macOS LaunchAgent KeepAlive so the local server stays up

### What it is *not*
- Not a Prometheus/Grafana stack (see sparkDash / spark-dashboard)
- Not a model launcher (see Spark Studio / Model Manager)

### Try in 10 seconds (no hardware)

```bash
git clone https://github.com/hectorTSH/dgx-spark-memory-dashboard.git
cd dgx-spark-memory-dashboard
python3 scripts/run.py --demo
# http://127.0.0.1:7474/
```

Happy to take feedback from other GB10 owners — especially multi-Spark and better vLLM KV accounting.

—
