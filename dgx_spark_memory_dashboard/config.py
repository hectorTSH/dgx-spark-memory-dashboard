"""Configuration loading for DGX Spark Memory Dashboard."""

from __future__ import annotations

import os
import copy
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - stdlib fallback
    yaml = None

DEFAULT_CONFIG: Dict[str, Any] = {
    "server": {
        "host": "127.0.0.1",
        "port": 7474,
        "poll_seconds": 8,
        "token": "",  # optional bearer / ?token=
        "cors_origin": "*",
    },
    "headroom_gib": 2.0,
    "units": "GiB",  # GiB | GB
    "probes": {
        "ollama_url": "http://127.0.0.1:11434",
        "vllm_urls": ["http://127.0.0.1:8000"],
        "llamacpp_urls": ["http://127.0.0.1:8080"],
        "sglang_urls": ["http://127.0.0.1:30000"],
        "hf_cache": "~/.cache/huggingface/hub",
        "disk_path": "~",
        "hf_cache_ttl_seconds": 300,
    },
    "sparks": [
        # Example remote host — replace in config.yaml / .env
        # {
        #   "id": "spark-1",
        #   "label": "spark-1",
        #   "mode": "remote",  # remote | local
        #   "host": "100.x.x.x",
        #   "user": "user",
        #   "ssh_key": "~/.ssh/id_ed25519",
        #   "ssh_port": 22,
        # }
    ],
}

ENV_MAP = {
    "DGX_SMD_HOST": ("server", "host"),
    "DGX_SMD_PORT": ("server", "port"),
    "DGX_SMD_POLL_SECONDS": ("server", "poll_seconds"),
    "DGX_SMD_TOKEN": ("server", "token"),
    "DGX_SMD_CORS_ORIGIN": ("server", "cors_origin"),
    "DGX_SMD_HEADROOM_GIB": ("headroom_gib",),
    "DGX_SMD_UNITS": ("units",),
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _parse_simple_yaml(text: str) -> Dict[str, Any]:
    """Tiny YAML subset parser used when PyYAML is unavailable.

    Supports nested maps via indentation, lists of scalars/maps, and
    plain scalars. Good enough for our config.example.yaml shape.
    """
    def parse_scalar(s: str):
        s = s.strip()
        if s in ("", "~", "null", "Null", "NULL"):
            return None
        if s in ("true", "True", "TRUE", "yes", "Yes"):
            return True
        if s in ("false", "False", "FALSE", "no", "No"):
            return False
        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
            return s[1:-1]
        if s.startswith("[") and s.endswith("]"):
            inner = s[1:-1].strip()
            if not inner:
                return []
            return [parse_scalar(p) for p in inner.split(",")]
        try:
            if "." in s:
                return float(s)
            return int(s)
        except ValueError:
            return s

    lines = []
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        lines.append((indent, raw.lstrip(" ")))

    def parse_block(idx: int, indent: int):
        node: Any = None
        while idx < len(lines):
            ind, content = lines[idx]
            if ind < indent:
                break
            if ind > indent:
                # orphan deeper line — attach failure
                break
            if content.startswith("- "):
                if node is None:
                    node = []
                elif not isinstance(node, list):
                    raise ValueError("mixed list/map")
                item_text = content[2:].strip()
                idx += 1
                if item_text and ":" in item_text and not item_text.endswith(":"):
                    # inline key: value on list item start → map
                    k, _, v = item_text.partition(":")
                    item: Dict[str, Any] = {k.strip(): parse_scalar(v)}
                    # following deeper keys belong to this map
                    child, idx = parse_block(idx, indent + 2)
                    if isinstance(child, dict):
                        item.update(child)
                    node.append(item)
                elif item_text.endswith(":") or item_text == "":
                    key = item_text[:-1].strip() if item_text.endswith(":") else None
                    child, idx = parse_block(idx, indent + 2)
                    if key:
                        node.append({key: child})
                    else:
                        node.append(child if child is not None else {})
                else:
                    node.append(parse_scalar(item_text))
            else:
                if node is None:
                    node = {}
                elif not isinstance(node, dict):
                    raise ValueError("mixed list/map")
                if ":" not in content:
                    raise ValueError(f"bad line: {content}")
                k, _, rest = content.partition(":")
                key = k.strip()
                rest = rest.strip()
                idx += 1
                if rest == "" or rest == "|" or rest == ">":
                    child, idx = parse_block(idx, indent + 2)
                    node[key] = child if child is not None else {}
                else:
                    node[key] = parse_scalar(rest)
        return node if node is not None else {}, idx

    result, _ = parse_block(0, 0)
    return result if isinstance(result, dict) else {}


def load_yaml_file(path: Path) -> Dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        data = yaml.safe_load(text) or {}
        if not isinstance(data, dict):
            raise ValueError(f"Config root must be a mapping: {path}")
        return data
    return _parse_simple_yaml(text)


def apply_env(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(cfg)
    for env_key, path in ENV_MAP.items():
        if env_key not in os.environ:
            continue
        raw = os.environ[env_key]
        cur = out
        for p in path[:-1]:
            cur = cur.setdefault(p, {})
        leaf = path[-1]
        if leaf in ("port", "poll_seconds", "ssh_port"):
            cur[leaf] = int(raw)
        elif leaf == "headroom_gib" or path == ("headroom_gib",):
            if path == ("headroom_gib",):
                out["headroom_gib"] = float(raw)
            else:
                cur[leaf] = float(raw)
        else:
            if path == ("headroom_gib",):
                out["headroom_gib"] = float(raw)
            else:
                cur[leaf] = raw

    # Single-spark convenience env (builds/overrides first spark)
    host = os.environ.get("DGX_SMD_SPARK_HOST") or os.environ.get("SPARK_HOST")
    if host:
        spark = {
            "id": os.environ.get("DGX_SMD_SPARK_ID", "spark-1"),
            "label": os.environ.get("DGX_SMD_SPARK_LABEL", host),
            "mode": os.environ.get("DGX_SMD_MODE", "remote"),
            "host": host,
            "user": os.environ.get("DGX_SMD_SPARK_USER") or os.environ.get("SPARK_USER", "user"),
            "ssh_key": os.environ.get("DGX_SMD_SSH_KEY") or os.environ.get("SSH_KEY", ""),
            "ssh_port": int(os.environ.get("DGX_SMD_SSH_PORT", "22")),
        }
        sparks = out.setdefault("sparks", [])
        if sparks:
            sparks[0] = _deep_merge(sparks[0], spark)
        else:
            sparks.append(spark)

    # Multi-spark: DGX_SMD_SPARKS="id|label|mode|user@host:port|key;..."
    multi = os.environ.get("DGX_SMD_SPARKS")
    if multi:
        sparks = []
        for part in multi.split(";"):
            part = part.strip()
            if not part:
                continue
            bits = [b.strip() for b in part.split("|")]
            # id|label|mode|user@host:port|key
            sid = bits[0] if bits else "spark"
            label = bits[1] if len(bits) > 1 else sid
            mode = bits[2] if len(bits) > 2 else "remote"
            target = bits[3] if len(bits) > 3 else ""
            key = bits[4] if len(bits) > 4 else ""
            user, host_port = ("user", target)
            if "@" in target:
                user, host_port = target.split("@", 1)
            host, port = host_port, 22
            if ":" in host_port and not host_port.count(":") > 1:
                # simple host:port (not IPv6)
                h, p = host_port.rsplit(":", 1)
                if p.isdigit():
                    host, port = h, int(p)
            sparks.append(
                {
                    "id": sid,
                    "label": label,
                    "mode": mode,
                    "host": host,
                    "user": user,
                    "ssh_key": key,
                    "ssh_port": port,
                }
            )
        if sparks:
            out["sparks"] = sparks
    return out


def normalize_sparks(cfg: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(cfg)
    sparks = out.get("sparks") or []
    normalized: List[Dict[str, Any]] = []
    for i, s in enumerate(sparks):
        s = dict(s or {})
        sid = str(s.get("id") or f"spark-{i+1}")
        mode = (s.get("mode") or "remote").lower()
        if mode not in ("remote", "local", "demo"):
            mode = "remote"
        n = {
            "id": sid,
            "label": s.get("label") or sid,
            "mode": mode,
            "host": s.get("host") or ("127.0.0.1" if mode == "local" else ""),
            "user": s.get("user") or "user",
            "ssh_key": str(Path(s["ssh_key"]).expanduser()) if s.get("ssh_key") else "",
            "ssh_port": int(s.get("ssh_port") or 22),
            "probes": _deep_merge(out.get("probes") or {}, s.get("probes") or {}),
        }
        normalized.append(n)
    out["sparks"] = normalized
    return out


def find_default_config_path() -> Optional[Path]:
    candidates = [
        Path.cwd() / "config.yaml",
        Path.cwd() / "config.yml",
        Path(__file__).resolve().parent.parent / "config.yaml",
        Path.home() / ".config" / "dgx-spark-memory-dashboard" / "config.yaml",
    ]
    for c in candidates:
        if c.is_file():
            return c
    return None


def load_config(path: Optional[str | Path] = None, demo: bool = False) -> Dict[str, Any]:
    cfg = copy.deepcopy(DEFAULT_CONFIG)
    cfg_path: Optional[Path] = Path(path).expanduser() if path else find_default_config_path()
    if cfg_path and cfg_path.is_file():
        cfg = _deep_merge(cfg, load_yaml_file(cfg_path))
        cfg["_config_path"] = str(cfg_path)
    cfg = apply_env(cfg)
    cfg = normalize_sparks(cfg)
    # --demo always uses synthetic dual-spark fixtures (ignores real hosts)
    if demo:
        probes = cfg.get("probes") or {}
        cfg["sparks"] = [
            {
                "id": "demo-a",
                "label": "demo-spark-a",
                "mode": "demo",
                "host": "demo",
                "user": "demo",
                "ssh_key": "",
                "ssh_port": 22,
                "probes": probes,
            },
            {
                "id": "demo-b",
                "label": "demo-spark-b",
                "mode": "demo",
                "host": "demo",
                "user": "demo",
                "ssh_key": "",
                "ssh_port": 22,
                "probes": probes,
            },
        ]
    # server port/host env already applied; coerce types
    cfg["server"]["port"] = int(cfg["server"].get("port") or 7474)
    cfg["server"]["poll_seconds"] = float(cfg["server"].get("poll_seconds") or 8)
    cfg["headroom_gib"] = float(cfg.get("headroom_gib") or 2.0)
    return cfg
