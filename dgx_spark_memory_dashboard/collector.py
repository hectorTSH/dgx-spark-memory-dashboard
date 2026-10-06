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

# Read-only policy controller status (e.g. spark-88de :11434/_policy/status).
# Plain Ollama answers 404 here, which is recorded as None. GET only.
_pol = get_json(ollama.rstrip("/") + "/_policy/status", 4)
out["policy_status"] = _pol if isinstance(_pol, dict) and "error" not in _pol else None

# GPU compute processes (read-only attribution so models are not hidden in system/other)
gpu_procs = []
try:
    r = subprocess.run(
        ["nvidia-smi", "--query-compute-apps=pid,process_name,used_memory", "--format=csv,noheader,nounits"],
        capture_output=True, text=True, timeout=5,
    )
    for line in r.stdout.strip().splitlines():
        parts = [x.strip() for x in line.split(",")]
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
        except Exception:
            continue
        try:
            used_mib = float(parts[2])
        except Exception:
            used_mib = None
        cmd, cwd, rss = "", None, None
        try:
            cmd = Path("/proc/%d/cmdline" % pid).read_bytes().replace(b"\0", b" ").decode("utf-8", "replace").strip()
        except Exception:
            pass
        try:
            cwd = os.readlink("/proc/%d/cwd" % pid)
        except Exception:
            pass
        try:
            for ln in Path("/proc/%d/status" % pid).read_text().splitlines():
                if ln.startswith("VmRSS:"):
                    rss = int(ln.split()[1]) * 1024
        except Exception:
            pass
        gpu_procs.append({"pid": pid, "process_name": parts[1], "used_mib": used_mib,
                          "cmdline": cmd[:600], "cwd": cwd, "rss_bytes": rss})
except Exception as e:
    out["gpu_procs_error"] = str(e)
out["gpu_procs"] = gpu_procs

# Local (non-HF-cache) model directories, re-scanned every poll: diffusers /
# transformers checkpoints such as ~/qwen-image21-inference/models/Qwen-Image-2.1
import glob as _glob
WEIGHT_EXT = (".safetensors", ".gguf", ".bin", ".pt", ".pth", ".onnx")
local_models, _seen = [], set()
for pat in (PROBES.get("model_dirs") or ["~/*/models/*", "~/models/*"]):
    for d in sorted(_glob.glob(os.path.expanduser(pat))):
        try:
            rp = os.path.realpath(d)
            if rp in _seen or not os.path.isdir(d):
                continue
            markers = [m for m in ("model_index.json", "config.json") if os.path.isfile(os.path.join(d, m))]
            if not markers:
                continue
            size, nweights, incomplete = 0, 0, 0
            for root, dirs, files in os.walk(d):
                dirs[:] = [x for x in dirs if not x.startswith(".")]
                for f in files:
                    try:
                        size += os.path.getsize(os.path.join(root, f))
                    except Exception:
                        pass
                    if f.endswith(WEIGHT_EXT):
                        nweights += 1
                    if f.endswith(".incomplete") or f.endswith(".part"):
                        incomplete += 1
            if not nweights:
                continue
            _seen.add(rp)
            local_models.append({
                "name": os.path.basename(d.rstrip("/")),
                "size_bytes": size,
                "size_gb": round(size / 1e9, 2),
                "size_gib": round(size / (1 << 30), 2),
                "source": "local",
                "kind": "diffusers" if "model_index.json" in markers else "transformers",
                "incomplete": incomplete,
                "path": d,
            })
        except Exception:
            continue
out["local_models"] = local_models

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


_GENERIC_DIRS = {"policy", "src", "scripts", "bin", "app", "lib", "server", "worker", "share"}


def _proc_label(cmdline: str, process_name: str) -> str:
    """Human label for an unattributed GPU process, e.g. 'cua-s1'."""
    for tok in (cmdline or "").split():
        if tok.endswith(".py") and "/" in tok:
            parts = [x for x in tok.split("/") if x][:-1]
            while parts and (parts[-1] in _GENERIC_DIRS or parts[-1].startswith(".")):
                parts.pop()
            if parts:
                return parts[-1]
    return os.path.basename((process_name or "").strip()) or "gpu process"


def attribute_gpu_processes(data: Dict[str, Any]) -> None:
    """Turn nvidia-smi compute processes + policy status + local model dirs into
    'resident_models' so non-Ollama models (image workers, custom servers) show
    as loaded instead of disappearing into system/other. Read-only."""
    import re

    procs = data.get("gpu_procs") or []
    local = data.get("local_models") or []
    policy = data.get("policy_status") if isinstance(data.get("policy_status"), dict) else {}
    image = policy.get("image") if isinstance(policy.get("image"), dict) else None
    engines_reporting = {
        eng: bool((data.get(eng + "_models") or {}).get("data")) for eng in ("vllm", "llamacpp", "sglang")
    }
    resident: List[Dict[str, Any]] = []
    for p in procs:
        hay = "%s %s" % (p.get("process_name") or "", p.get("cmdline") or "")
        if re.search(r"ollama", hay, re.I):
            continue  # Ollama runners are already counted via /api/ps
        if engines_reporting["vllm"] and re.search(r"vllm", hay, re.I):
            continue
        if engines_reporting["sglang"] and re.search(r"sglang", hay, re.I):
            continue
        if engines_reporting["llamacpp"] and re.search(r"llama-server|llama\.cpp", hay, re.I):
            continue
        used_mib = p.get("used_mib")
        if used_mib:
            gib, src = used_mib / 1024.0, "nvidia-smi"
        elif p.get("rss_bytes"):
            gib, src = p["rss_bytes"] / float(1 << 30), "rss"
        else:
            gib, src = 0.0, "unknown"
        # Match to a local model directory sharing the same project root
        match = None
        locs = [p.get("cmdline") or "", p.get("cwd") or ""]
        cands = []
        for lm in local:
            path = (lm.get("path") or "").rstrip("/")
            root = os.path.dirname(os.path.dirname(path)) if "/models/" in path else os.path.dirname(path)
            if root.count("/") < 3:  # don't match on bare home dir
                continue
            if any(root + "/" in x or x == root for x in locs):
                cands.append(lm)
        if cands:
            named = [c for c in cands if c.get("name") and c["name"].lower() in locs[0].lower()]
            match = (named or cands)[0]
        item: Dict[str, Any] = {
            "name": match["name"] if match else _proc_label(p.get("cmdline") or "", p.get("process_name") or ""),
            "pid": p.get("pid"),
            "gib": round(gib, 2),
            "size_source": src,
            "process": p.get("process_name"),
            "kind": (match or {}).get("kind") or "gpu-process",
            "matched_model": bool(match),
            "local_path": (match or {}).get("path"),
            "on_disk_gib": (match or {}).get("size_gib"),
            "pinned": False,
            "state": "loaded",
        }
        if image and image.get("pid") == p.get("pid"):
            rb = image.get("readback") if isinstance(image.get("readback"), dict) else {}
            item["kind"] = "image-worker"
            item["pinned"] = bool(image.get("resident"))
            item["state"] = (
                "rendering" if image.get("render_active")
                else "resident" if image.get("resident")
                else "warming" if image.get("warming")
                else "loaded"
            )
            item["policy"] = {
                "resident": image.get("resident"),
                "warming": image.get("warming"),
                "render_active": image.get("render_active"),
                "reserved_gib": round((rb.get("reserved_bytes") or 0) / float(1 << 30), 2) or None,
                "load_seconds": rb.get("load_seconds"),
                "error_type": image.get("error_type"),
            }
        if match:
            match["_loaded"] = True
        resident.append(item)
    data["resident_models"] = resident
    # Image worker status even when it is not on the GPU (e.g. evicted for an exclusive task)
    if image is not None:
        on_gpu = any(r.get("kind") == "image-worker" for r in resident)
        data["image_worker"] = {
            "resident": image.get("resident"),
            "warming": image.get("warming"),
            "render_active": image.get("render_active"),
            "pid": image.get("pid"),
            "on_gpu": on_gpu,
            "policy": policy.get("policy"),
        }
    # Local model dirs join the on-SSD catalog (UI 'HF / other' list)
    if local:
        hf = list(data.get("hf_models") or [])
        known = {h.get("path") for h in hf}
        for lm in local:
            if lm.get("path") not in known:
                hf.append(lm)
        data["hf_models"] = hf


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
    attribute_gpu_processes(data)
    data["fetched_at"] = time.time()
    return data


def collect_spark(spark: Dict[str, Any], demo_variant: int = 0) -> Dict[str, Any]:
    mode = (spark.get("mode") or "remote").lower()
    if mode == "demo":
        return collect_demo(spark, variant=demo_variant)
    if mode == "local":
        return collect_local(spark)
    return collect_remote(spark)
