"""HTTP server for DGX Spark Memory Dashboard."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from . import __version__
from .collector import collect_spark
from .config import load_config


class StateCache:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.by_id: Dict[str, Dict[str, Any]] = {}
        self.ts_by_id: Dict[str, float] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def poll_seconds(self) -> float:
        return float(self.cfg.get("server", {}).get("poll_seconds") or 8)

    def get_one(self, spark_id: str, force: bool = False) -> Dict[str, Any]:
        sparks = {s["id"]: s for s in self.cfg.get("sparks") or []}
        spark = sparks.get(spark_id)
        if not spark:
            return {"error": f"unknown spark_id: {spark_id}"}
        now = time.time()
        with self.lock:
            fresh = (now - self.ts_by_id.get(spark_id, 0)) < self.poll_seconds()
            if not force and fresh and spark_id in self.by_id:
                return self.by_id[spark_id]
        data = collect_spark(spark, demo_variant=0 if not spark_id.endswith("b") else 1)
        with self.lock:
            self.by_id[spark_id] = data
            self.ts_by_id[spark_id] = time.time()
        return data

    def get_all(self, force: bool = False) -> Dict[str, Any]:
        sparks = self.cfg.get("sparks") or []
        results = []
        for i, s in enumerate(sparks):
            sid = s["id"]
            now = time.time()
            with self.lock:
                fresh = (now - self.ts_by_id.get(sid, 0)) < self.poll_seconds()
                cached = self.by_id.get(sid)
            if not force and fresh and cached is not None:
                results.append(cached)
            else:
                data = collect_spark(s, demo_variant=i)
                with self.lock:
                    self.by_id[sid] = data
                    self.ts_by_id[sid] = time.time()
                results.append(data)
        return {
            "version": __version__,
            "fetched_at": time.time(),
            "headroom_gib": self.cfg.get("headroom_gib", 2.0),
            "units": self.cfg.get("units", "GiB"),
            "sparks": [
                {
                    "id": s["id"],
                    "label": s.get("label") or s["id"],
                    "mode": s.get("mode"),
                    "host": s.get("host"),
                }
                for s in sparks
            ],
            "states": results,
        }

    def start_background(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        def loop():
            while not self._stop.is_set():
                try:
                    self.get_all(force=True)
                except Exception:
                    traceback.print_exc()
                self._stop.wait(self.poll_seconds())

        self._thread = threading.Thread(target=loop, name="smd-poller", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()


def web_root() -> Path:
    return Path(__file__).resolve().parent.parent / "web"


def make_handler(cache: StateCache, cfg: Dict[str, Any]):
    token = (cfg.get("server") or {}).get("token") or ""
    cors = (cfg.get("server") or {}).get("cors_origin") or "*"
    root = web_root()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # quiet default
            sys.stderr.write("[%s] %s\n" % (self.log_date_time_string(), fmt % args))

        def _auth_ok(self) -> bool:
            if not token:
                return True
            # Authorization: Bearer <token> or ?token=
            auth = self.headers.get("Authorization") or ""
            if auth.lower().startswith("bearer ") and auth[7:].strip() == token:
                return True
            qs = parse_qs(urlparse(self.path).query)
            if qs.get("token", [None])[0] == token:
                return True
            return False

        def _send(self, code: int, body: bytes, content_type: str, extra_headers: Optional[Dict[str, str]] = None):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", cors)
            self.send_header("Cache-Control", "no-store")
            if extra_headers:
                for k, v in extra_headers.items():
                    self.send_header(k, v)
            self.end_headers()
            self.wfile.write(body)

        def _json(self, code: int, obj: Any):
            body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
            self._send(code, body, "application/json; charset=utf-8")

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", cors)
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
            self.end_headers()

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path in ("/api/health", "/healthz"):
                self._json(200, {"ok": True, "version": __version__})
                return

            if path.startswith("/api/"):
                if not self._auth_ok():
                    self._json(401, {"error": "unauthorized"})
                    return

            if path == "/api/config":
                sparks = cfg.get("sparks") or []
                self._json(
                    200,
                    {
                        "version": __version__,
                        "headroom_gib": cfg.get("headroom_gib", 2.0),
                        "units": cfg.get("units", "GiB"),
                        "poll_seconds": (cfg.get("server") or {}).get("poll_seconds", 8),
                        "auth_required": bool(token),
                        "sparks": [
                            {
                                "id": s["id"],
                                "label": s.get("label") or s["id"],
                                "mode": s.get("mode"),
                                "host": s.get("host"),
                            }
                            for s in sparks
                        ],
                    },
                )
                return

            if path == "/api/sparks":
                force = parse_qs(parsed.query).get("force", ["0"])[0] in ("1", "true", "yes")
                self._json(200, cache.get_all(force=force))
                return

            if path == "/api/spark-state" or path.startswith("/api/spark/"):
                force = parse_qs(parsed.query).get("force", ["0"])[0] in ("1", "true", "yes")
                if path == "/api/spark-state":
                    # single-spark convenience: first spark or ?id=
                    qs = parse_qs(parsed.query)
                    sid = qs.get("id", [None])[0]
                    if not sid:
                        sparks = cfg.get("sparks") or []
                        if not sparks:
                            self._json(404, {"error": "no sparks configured"})
                            return
                        sid = sparks[0]["id"]
                    self._json(200, cache.get_one(sid, force=force))
                    return
                # /api/spark/<id>
                sid = path[len("/api/spark/") :].strip("/")
                if not sid:
                    self._json(400, {"error": "missing spark id"})
                    return
                self._json(200, cache.get_one(sid, force=force))
                return

            # static web
            if path in ("/", "/index.html"):
                fpath = root / "index.html"
            else:
                # prevent path escape
                rel = path.lstrip("/")
                fpath = (root / rel).resolve()
                if not str(fpath).startswith(str(root.resolve())):
                    self._json(403, {"error": "forbidden"})
                    return

            if not fpath.is_file():
                self._json(404, {"error": "not found", "path": path})
                return

            data = fpath.read_bytes()
            ctype = "text/html; charset=utf-8"
            if fpath.suffix == ".js":
                ctype = "application/javascript; charset=utf-8"
            elif fpath.suffix == ".css":
                ctype = "text/css; charset=utf-8"
            elif fpath.suffix == ".svg":
                ctype = "image/svg+xml"
            elif fpath.suffix == ".png":
                ctype = "image/png"
            self._send(200, data, ctype)

    return Handler


def run_server(cfg: Dict[str, Any], background_poll: bool = True) -> None:
    cache = StateCache(cfg)
    if background_poll:
        # warm cache asynchronously
        threading.Thread(target=lambda: cache.get_all(force=True), daemon=True).start()
        cache.start_background()

    host = (cfg.get("server") or {}).get("host") or "127.0.0.1"
    port = int((cfg.get("server") or {}).get("port") or 7474)
    handler = make_handler(cache, cfg)
    httpd = ThreadingHTTPServer((host, port), handler)
    sparks = cfg.get("sparks") or []
    print(f"dgx-spark-memory-dashboard v{__version__}")
    print(f"listening on http://{host}:{port}/")
    if sparks:
        print("sparks:")
        for s in sparks:
            print(f"  - {s.get('label') or s['id']}  [{s.get('mode')}]  {s.get('host')}")
    else:
        print("WARNING: no sparks configured — use --demo or config.yaml")
    if (cfg.get("server") or {}).get("token"):
        print("auth: token required")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        cache.stop()
        httpd.server_close()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dgx-spark-memory-dashboard",
        description="Live unified-memory occupancy map for NVIDIA DGX Spark (GB10)",
    )
    p.add_argument("-c", "--config", help="Path to config.yaml")
    p.add_argument("--demo", action="store_true", help="Serve synthetic demo sparks (no hardware needed)")
    p.add_argument("--host", help="Bind host (default from config / 127.0.0.1)")
    p.add_argument("--port", type=int, help="Bind port (default 7474)")
    p.add_argument("--token", help="Optional access token")
    p.add_argument("--no-background-poll", action="store_true", help="Only collect on request")
    p.add_argument("-v", "--version", action="version", version=f"%(prog)s {__version__}")
    return p


def main(argv: Optional[list] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    cfg = load_config(args.config, demo=args.demo)
    if args.host:
        cfg.setdefault("server", {})["host"] = args.host
    if args.port:
        cfg.setdefault("server", {})["port"] = args.port
    if args.token is not None:
        cfg.setdefault("server", {})["token"] = args.token
    # Safety: if binding non-loopback without token, warn
    host = cfg["server"]["host"]
    if host not in ("127.0.0.1", "localhost", "::1") and not cfg["server"].get("token"):
        print(
            "WARNING: binding to non-loopback without DGX_SMD_TOKEN / server.token — "
            "anyone on the network can read your Spark inventory.",
            file=sys.stderr,
        )
    run_server(cfg, background_poll=not args.no_background_poll)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
