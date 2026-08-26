"""State collectors for local, remote (SSH), and demo modes."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# In-process HF size cache: path -> (ts, list)
_HF_CACHE: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


COLLECTOR_SCRIPT = r'''
import json, os, re, shutil, subprocess, time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

PROBES = json.loads(os.environ.get("DGX_SMD_PROBES_JSON", "{}"))

def get_json(url, timeout=5):
    try:
        req = Request(url, headers={"User-Agent": "dgx-spark-memory-dashboard/0.1"})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception as e:
        return {"error": str(e)}

out = {"ts": time.time(), "collector": "local-script"}

# Ollama
ollama = PROBES.get("ollama_url") or "http://127.0.0.1:11434"
out["ollama_tags"] = get_json(ollama.rstrip("/") + "/api/tags", 8)
out["ollama_ps"] = get_json(ollama.rstrip("/") + "/api/ps", 8)
if "models" not in out.get("ollama_ps", {}):
    out["ollama_ps"] = {"error": out["ollama_ps"].get("error", "bad shape"), "models": []}

# OpenAI-compatible engines
def probe_openai(urls, key):
    items = []
    for url in urls or []:
        base = url.rstrip("/")
        models = get_json(base + "/v1/models", 5)
        metrics = None
        try:
            req = Request(base + "/metrics", headers={"User-Agent": "dgx-spark-memory-dashboard/0.1"})
            with urlopen(req, timeout=4) as r:
                metrics = r.read().decode("utf-8", "replace")
        except Exception:
            metrics = None
        items.append({"url": base, "models": models, "metrics_text": metrics})
    out[key] = items

probe_openai(PROBES.get("vllm_urls") or ["http://127.0.0.1:8000"], "vllm")
probe_openai(PROBES.get("llamacpp_urls") or ["http://127.0.0.1:8080"], "llamacpp")
probe_openai(PROBES.get("sglang_urls") or ["http://127.0.0.1:30000"], "sglang")

# GPU
try:
    gpu = subprocess.run(
        ["nvidia-smi",
         "--query-gpu=utilization.gpu,temperature.gpu,power.draw,name,memory.used,memory.total",
         "--format=csv,noheader,nounits"],
        capture_output=True, text=True, timeout=5,
    )
    line = gpu.stdout.strip().splitlines()[0] if gpu.stdout.strip() else ""
    parts = [p.strip() for p in line.split(",")] if line else []
    def num(i):
        if len(parts) <= i: return None
        v = parts[i]
        if v in ("", "N/A", "[N/A]"): return None
        try: return float(v)
        except: return None
    out["gpu"] = {
        "utilization_gpu": num(0),
        "temperature_gpu": num(1),
        "power_draw_w": num(2),
        "name": parts[3] if len(parts) > 3 else None,
        "memory_used_mib": num(4),
        "memory_total_mib": num(5),
    }
except Exception as e:
    out["gpu"] = {"error": str(e)}

# Docker
try:
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}|{{.Status}}|{{.Image}}|{{.ID}}"],
        capture_output=True, text=True, timeout=8,
    )
    containers = []
    for line in result.stdout.strip().splitlines():
        if not line: continue
        parts = line.split("|", 3)
        containers.append({
            "name": parts[0] if parts else "",
            "status": parts[1] if len(parts) > 1 else "",
            "image": parts[2] if len(parts) > 2 else "",
            "id": parts[3] if len(parts) > 3 else "",
        })
    # docker stats snapshot (no-stream) for memory
    stats = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.MemUsage}}|{{.MemPerc}}|{{.CPUPerc}}"],
        capture_output=True, text=True, timeout=12,
    )
    by_name = {}
    for line in stats.stdout.strip().splitlines():
        p = line.split("|")
        if len(p) >= 2:
            by_name[p[0]] = {"mem_usage": p[1], "mem_perc": p[2] if len(p)>2 else None, "cpu_perc": p[3] if len(p)>3 else None}
    for c in containers:
        c.update(by_name.get(c["name"], {}))
    out["docker"] = containers
except Exception as e:
    out["docker"] = []
    out["docker_error"] = str(e)

# Memory
mem = {}
try:
    with open("/proc/meminfo") as f:
        for line in f:
            k,_,v = line.partition(":")
            mem[k.strip()] = int(v.strip().split()[0]) * 1024
    out["mem"] = {
        "total": mem.get("MemTotal", 0),
        "available": mem.get("MemAvailable", 0),
        "free": mem.get("MemFree", 0),
        "cached": mem.get("Cached", 0) + mem.get("Buffers", 0),
        "swap_total": mem.get("SwapTotal", 0),
        "swap_free": mem.get("SwapFree", 0),
    }
except Exception as e:
    out["mem"] = {"error": str(e)}

# Disk
disk_path = os.path.expanduser(PROBES.get("disk_path") or "~")
try:
    u = shutil.disk_usage(disk_path)
    out["disk"] = {"total": u.total, "used": u.used, "free": u.free, "path": disk_path}
except Exception as e:
    out["disk"] = {"error": str(e)}

# HF cache sizes
hf_root = Path(os.path.expanduser(PROBES.get("hf_cache") or "~/.cache/huggingface/hub"))
hf = []
if hf_root.is_dir():
    for p in sorted(hf_root.glob("models--*")):
        try:
            size = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
        except Exception:
            size = 0
        name = p.name.replace("models--", "").replace("--", "/")
        incomplete = list(p.glob("blobs/*.incomplete"))
        hf.append({
            "name": name,
            "size_bytes": size,
            "size_gb": round(size / 1e9, 2),
            "size_gib": round(size / (1<<30), 2),
            "source": "huggingface",
            "incomplete": len(incomplete),
            "path": str(p),
        })
out["hf_models"] = hf

# hostname
try:
    out["hostname"] = Path("/etc/hostname").read_text().strip()
except Exception:
    out["hostname"] = os.uname().nodename if hasattr(os, "uname") else ""

print(json.dumps(out))
'''


def _http_json(url: str, timeout: float = 5.0) -> Dict[str, Any]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "dgx-spark-memory-dashboard/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "replace"))
    except Exception as e:
        return {"error": str(e)}


def _http_text(url: str, timeout: float = 4.0) -> Optional[str]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "dgx-spark-memory-dashboard/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")
    except Exception:
        return None


def parse_vllm_metrics(text: Optional[str]) -> Dict[str, Any]:
    """Extract useful gauges from Prometheus /metrics text."""
    if not text:
        return {}
    out: Dict[str, Any] = {}
    patterns = {
        "kv_cache_usage_perc": r"^vllm:kv_cache_usage_perc(?:\{[^}]*\})?\s+([0-9.eE+-]+)",
        "gpu_cache_usage_perc": r"^vllm:gpu_cache_usage_perc(?:\{[^}]*\})?\s+([0-9.eE+-]+)",
        "num_requests_running": r"^vllm:num_requests_running(?:\{[^}]*\})?\s+([0-9.eE+-]+)",
        "num_requests_waiting": r"^vllm:num_requests_waiting(?:\{[^}]*\})?\s+([0-9.eE+-]+)",
    }
    import re

    for key, pat in patterns.items():
        m = re.search(pat, text, re.M)
        if m:
            try:
                out[key] = float(m.group(1))
            except ValueError:
                pass
    return out


def parse_docker_mem_to_bytes(usage: str) -> Optional[int]:
    """Parse docker stats MemUsage like '12.3GiB / 128GiB' → used bytes."""
    if not usage:
        return None
    left = usage.split("/")[0].strip()
    m = __import__("re").match(r"([0-9.]+)\s*([KMGTPE]i?B)", left, __import__("re").I)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2).lower()
    mult = {
        "b": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
    }.get(unit, 1)
    return int(val * mult)


def estimate_model_gib_from_name(name: str, arch: str = "Dense") -> float:
    n = name.lower()
    # rough weight+runtime guesses when no better signal
    table = [
        (r"122b|120b", 70),
        (r"80b", 50),
        (r"70b", 45),
        (r"35b", 28),
        (r"32b|31b|30b", 26),
        (r"27b", 24),
        (r"26b", 22),
        (r"14b|13b|12b", 12),
        (r"9b|8b|7b", 8),
        (r"4b|3b|e4b|e2b", 4),
    ]
    import re

    for pat, gib in table:
        if re.search(pat, n):
            return float(gib)
    return 35.0 if arch == "MoE" else 25.0


def collect_local(spark: Dict[str, Any]) -> Dict[str, Any]:
    """Run collector in-process (when dashboard runs on the Spark)."""
    probes = spark.get("probes") or {}
    env = os.environ.copy()
    env["DGX_SMD_PROBES_JSON"] = json.dumps(probes)
    # Execute the shared script body via python -c for parity with remote
    try:
        # Prefer argv form (no shell) so probes JSON never needs quoting.
        result = subprocess.run(
            ["python3", "-c", COLLECTOR_SCRIPT],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )
        if result.returncode != 0:
            return {
                "error": (result.stderr or result.stdout or "collector failed")[:800],
                "spark_id": spark.get("id"),
                "mode": "local",
            }
        data = json.loads(result.stdout.strip().splitlines()[-1])
    except Exception:
        return {"error": traceback.format_exc()[-800:], "spark_id": spark.get("id"), "mode": "local"}
    return enrich_state(data, spark)


def collect_remote(spark: Dict[str, Any]) -> Dict[str, Any]:
    host = spark.get("host") or ""
    user = spark.get("user") or "user"
    key = spark.get("ssh_key") or ""
    port = int(spark.get("ssh_port") or 22)
    probes = spark.get("probes") or {}
    if not host:
        return {"error": "remote spark missing host", "spark_id": spark.get("id"), "mode": "remote"}

    cmd = [
        "ssh",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-p",
        str(port),
    ]
    if key:
        cmd.extend(["-i", key])
    cmd.append(f"{user}@{host}")
    # Pass probes via env on remote — must be shell-quoted for bash over SSH.
    # Avoid subprocess.list2cmdline (Windows-oriented) for remote Unix shells.
    remote = (
        f"DGX_SMD_PROBES_JSON={shlex.quote(json.dumps(probes))} "
        f"python3 -c {shlex.quote(COLLECTOR_SCRIPT)}"
    )
    cmd.append(remote)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=45)
        if result.returncode != 0:
            return {
                "error": (result.stderr or result.stdout or "ssh failed")[:800],
                "spark_id": spark.get("id"),
                "mode": "remote",
                "host": host,
            }
        # last JSON line
        lines = [ln for ln in result.stdout.splitlines() if ln.strip().startswith("{")]
        if not lines:
            return {"error": "no JSON from remote collector", "raw": result.stdout[:400], "spark_id": spark.get("id")}
        data = json.loads(lines[-1])
    except Exception:
        return {"error": traceback.format_exc()[-800:], "spark_id": spark.get("id"), "mode": "remote", "host": host}
    return enrich_state(data, spark)


def collect_demo(spark: Dict[str, Any], variant: int = 0) -> Dict[str, Any]:
    """Synthetic GB10-ish state for screenshots / offline try-out."""
    total = 128 * (1 << 30)
    # alternate two demo sparks
    if (spark.get("id") or "").endswith("b") or variant % 2 == 1:
        used = int(78.4 * (1 << 30))
        ollama_ps = {
            "models": [
                {
                    "name": "qwen3.5:122b-a10b",
                    "size": int(72.0 * (1 << 30)),
                    "size_vram": int(72.0 * (1 << 30)),
                    "details": {"family": "qwen35moe", "parameter_size": "122B"},
                }
            ]
        }
        vllm_data = []
        gpu_util = 41.0
        label_hint = "MoE heavy"
    else:
        used = int(52.2 * (1 << 30))
        ollama_ps = {
            "models": [
                {
                    "name": "gemma4:31b",
                    "size": int(19.0 * (1 << 30)),
                    "size_vram": int(19.0 * (1 << 30)),
                    "details": {"family": "gemma4", "parameter_size": "31B"},
                }
            ]
        }
        vllm_data = [{"id": "mmangkad/Qwen3.6-27B-NVFP4", "object": "model"}]
        gpu_util = 67.0
        label_hint = "Dense + vLLM"

    available = total - used
    data = {
        "ts": time.time(),
        "collector": "demo",
        "hostname": spark.get("label") or "demo-spark",
        "demo": True,
        "demo_hint": label_hint,
        "ollama_tags": {
            "models": [
                {
                    "name": "qwen3.5:122b-a10b",
                    "size": int(81e9),
                    "details": {"family": "qwen35moe", "parameter_size": "122B"},
                },
                {
                    "name": "gpt-oss:120b",
                    "size": int(65e9),
                    "details": {"family": "gptoss", "parameter_size": "120B"},
                },
                {
                    "name": "gemma4:31b",
                    "size": int(19e9),
                    "details": {"family": "gemma4", "parameter_size": "31B"},
                },
                {
                    "name": "gemma4:e2b",
                    "size": int(4.5e9),
                    "details": {"family": "gemma4", "parameter_size": "2B"},
                },
                {
                    "name": "qwen3-coder:30b",
                    "size": int(18e9),
                    "details": {"family": "qwen3moe", "parameter_size": "30B"},
                },
                {
                    "name": "nemotron-3-nano:latest",
                    "size": int(24e9),
                    "details": {"family": "nemotron_h_moe", "parameter_size": "30B"},
                },
            ]
        },
        "ollama_ps": ollama_ps,
        "vllm": [
            {
                "url": "http://127.0.0.1:8000",
                "models": {"data": vllm_data} if vllm_data else {"error": "connection refused", "data": []},
                "metrics_text": (
                    "vllm:kv_cache_usage_perc 0.42\nvllm:num_requests_running 1.0\nvllm:num_requests_waiting 0.0\n"
                    if vllm_data
                    else None
                ),
            }
        ],
        "llamacpp": [{"url": "http://127.0.0.1:8080", "models": {"error": "offline", "data": []}, "metrics_text": None}],
        "sglang": [{"url": "http://127.0.0.1:30000", "models": {"error": "offline", "data": []}, "metrics_text": None}],
        "gpu": {
            "utilization_gpu": gpu_util,
            "temperature_gpu": 58.0,
            "power_draw_w": 92.0,
            "name": "NVIDIA GB10",
            "memory_used_mib": None,
            "memory_total_mib": None,
        },
        "docker": [
            {
                "name": "vllm-qwen36",
                "status": "Up 3 hours" if vllm_data else "Exited (0) 2 hours ago",
                "image": "spark-vllm:latest",
                "mem_usage": "48.2GiB / 128GiB" if vllm_data else "0B / 128GiB",
                "mem_perc": "37.65%" if vllm_data else "0.00%",
                "cpu_perc": "120.00%" if vllm_data else "0.00%",
            }
        ],
        "mem": {
            "total": total,
            "available": available,
            "free": int(available * 0.4),
            "cached": int(8 * (1 << 30)),
            "swap_total": 0,
            "swap_free": 0,
        },
        "disk": {
            "total": int(3.7e12),
            "used": int(0.76e12),
            "free": int(2.94e12),
            "path": "/home/demo",
        },
        "hf_models": [
            {
                "name": "mmangkad/Qwen3.6-27B-NVFP4",
                "size_bytes": int(62e9),
                "size_gb": 62.0,
                "size_gib": 57.7,
                "source": "huggingface",
                "incomplete": 0,
            },
            {
                "name": "Qwen/Qwen3-VL-30B-A3B-Instruct-FP8",
                "size_bytes": int(64.5e9),
                "size_gb": 64.5,
                "size_gib": 60.1,
                "source": "huggingface",
                "incomplete": 0,
            },
        ],
    }
    return enrich_state(data, spark)


def flatten_engine_models(state: Dict[str, Any]) -> None:
    """Normalize multi-URL engine probes into convenience fields used by UI."""
    # Back-compat single vllm_models
    vllm_models = []
    for entry in state.get("vllm") or []:
        models = entry.get("models") or {}
        metrics = parse_vllm_metrics(entry.get("metrics_text"))
        entry["metrics"] = metrics
        for m in models.get("data") or []:
            item = dict(m)
            item["_engine"] = "vllm"
            item["_url"] = entry.get("url")
            item["_metrics"] = metrics
            # container mem guess
            vllm_models.append(item)
    state["vllm_models"] = {"data": vllm_models}

    for eng in ("llamacpp", "sglang"):
        flat = []
        for entry in state.get(eng) or []:
            models = entry.get("models") or {}
            for m in models.get("data") or []:
                item = dict(m)
                item["_engine"] = eng
                item["_url"] = entry.get("url")
                flat.append(item)
        state[f"{eng}_models"] = {"data": flat}


def attach_vllm_sizes(state: Dict[str, Any]) -> None:
    """Prefer docker stats memory for vLLM containers; else name estimate."""
    docker = state.get("docker") or []
    # map container mem
    vllm_containers = [c for c in docker if __import__("re").search(r"vllm", c.get("name", ""), __import__("re").I)]
    container_gib = None
    for c in vllm_containers:
        b = parse_docker_mem_to_bytes(c.get("mem_usage") or "")
        if b and b > 1 << 30:
            container_gib = b / (1 << 30)
            break

    for m in (state.get("vllm_models") or {}).get("data") or []:
        mid = m.get("id") or m.get("name") or ""
        if container_gib and len((state.get("vllm_models") or {}).get("data") or []) == 1:
            m["_size_gib"] = round(container_gib, 2)
            m["_size_source"] = "docker_stats"
        else:
            m["_size_gib"] = estimate_model_gib_from_name(mid)
            m["_size_source"] = "name_estimate"


def enrich_state(data: Dict[str, Any], spark: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(data or {})
    data["spark_id"] = spark.get("id")
    data["spark_label"] = spark.get("label") or spark.get("id")
    data["mode"] = spark.get("mode")
    data["host"] = spark.get("host")
    # legacy single-url shape support if someone posts old collector
    if "vllm" not in data and "vllm_models" in data:
        data["vllm"] = [{"url": "http://127.0.0.1:8000", "models": data.get("vllm_models"), "metrics_text": None}]
    flatten_engine_models(data)
    attach_vllm_sizes(data)
    data["fetched_at"] = time.time()
    return data


def collect_spark(spark: Dict[str, Any], demo_variant: int = 0) -> Dict[str, Any]:
    mode = (spark.get("mode") or "remote").lower()
    if mode == "demo":
        return collect_demo(spark, variant=demo_variant)
    if mode == "local":
        return collect_local(spark)
    return collect_remote(spark)
