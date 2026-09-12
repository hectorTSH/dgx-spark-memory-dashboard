"""HTTP server for DGX Spark Memory Dashboard."""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import traceback
from concurrent.futures import Future, TimeoutError as FutureTimeout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from . import __version__
from .collector import collect_spark
from .config import load_config


# Match collector.py's subprocess limits; all-node waits share these deadlines.
_COLLECTOR_TIMEOUTS = {"remote": 45.0, "local": 60.0}


class _Collection(Future):
    def __init__(self, spark: Dict[str, Any]):
        super().__init__()
        self.spark = spark
        mode = (spark.get("mode") or "remote").lower()
        self.timeout = _COLLECTOR_TIMEOUTS.get(mode, _COLLECTOR_TIMEOUTS["remote"])
        self.deadline = time.monotonic() + self.timeout


class StateCache:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.by_id: Dict[str, Dict[str, Any]] = {}
        self.ts_by_id: Dict[str, float] = {}
        self._inflight: Dict[str, _Collection] = {}
        self._collectors: Dict[str, threading.Thread] = {}
        self._stop = threading.Event()
        self._pollers: Dict[str, threading.Thread] = {}
        self._background = False

    def poll_seconds(self) -> float:
        return float(self.cfg.get("server", {}).get("poll_seconds") or 8)

    def get_one(self, spark_id: str, force: bool = False) -> Dict[str, Any]:
        return self._wait_one(self._start_one(spark_id, force))

    def _start_one(self, spark_id: str, force: bool) -> Future:
        indexed = {s["id"]: (i, s) for i, s in enumerate(self.cfg.get("sparks") or [])}
        future = Future()
        if spark_id not in indexed:
            future.set_result({"error": f"unknown spark_id: {spark_id}"})
            return future
        index, spark = indexed[spark_id]
        with self.lock:
            if self._stop.is_set():
                future.set_result(self.by_id.get(spark_id) or self._record(spark, {"error": "cache stopped"}))
                return future
            if self._background and not force:
                future.set_result(self.by_id.get(spark_id) or {
                    "spark_id": spark_id,
                    "spark_label": spark.get("label") or spark_id,
                    "mode": spark.get("mode"),
                    "host": spark.get("host"),
                    "pending": True,
                    "stale": False,
                    "last_success_at": None,
                })
                return future
            fresh = (time.monotonic() - self.ts_by_id.get(spark_id, 0)) < self.poll_seconds()
            if not force and fresh and spark_id in self.by_id:
                future.set_result(self.by_id[spark_id])
                return future
            if spark_id in self._inflight:
                return self._inflight[spark_id]
            future = _Collection(spark)
            self._inflight[spark_id] = future
            thread = threading.Thread(
                target=self._collect, args=(spark, index, future),
                name=f"smd-collect-{spark_id}", daemon=True,
            )
            self._collectors[spark_id] = thread
            thread.start()
        return future

    def _wait_one(self, future: Future) -> Dict[str, Any]:
        if not isinstance(future, _Collection):
            return future.result()
        try:
            return future.result(timeout=max(0.0, future.deadline - time.monotonic()))
        except FutureTimeout:
            with self.lock:
                if not future.done():
                    data = self._record(future.spark, {
                        "error": f"collector timed out after {future.timeout:g}s",
                    })
                    future.set_result(data)
                # Keep ownership until the collector exits: a timeout is not cancellation.
                return future.result()

    def _collect(self, spark: Dict[str, Any], index: int, future: Future) -> None:
        sid = spark["id"]
        try:
            data = collect_spark(spark, demo_variant=index)
        except Exception as exc:
            data = {"error": f"{type(exc).__name__}: {exc}"}
        with self.lock:
            data = self._record(spark, data)
            del self._inflight[sid]
            if not future.done():
                future.set_result(data)

    def _record(self, spark: Dict[str, Any], data: Dict[str, Any]) -> Dict[str, Any]:
        """Publish a new snapshot under lock; never re-date retained measurements."""
        sid = spark["id"]
        now = time.time()
        previous = self.by_id.get(sid) or {}
        if data.get("error"):
            state = dict(previous if previous.get("last_success_at") is not None else data)
            state.update(error=data["error"], stale=True, last_success_at=previous.get("last_success_at"))
        else:
            state = dict(data)
            state.pop("error", None)
            state.update(stale=False, last_success_at=now)
        state.update(pending=False, last_attempt_at=now)
        state.update(
            spark_id=sid, spark_label=spark.get("label") or sid,
            mode=spark.get("mode"), host=spark.get("host"),
        )
        self.by_id[sid] = state
        self.ts_by_id[sid] = time.monotonic()
        return state

    def get_all(self, force: bool = False) -> Dict[str, Any]:
        sparks = self.cfg.get("sparks") or []
        futures = [self._start_one(s["id"], force) for s in sparks]
        results = [self._wait_one(future) for future in futures]
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
        with self.lock:
            self._background = True
            for spark in self.cfg.get("sparks") or []:
                sid = spark["id"]
                if sid in self._pollers and self._pollers[sid].is_alive():
                    continue
                thread = threading.Thread(
                    target=self._poll_node, args=(sid,), name=f"smd-poller-{sid}", daemon=True,
                )
                self._pollers[sid] = thread
                thread.start()

    def _poll_node(self, spark_id: str) -> None:
        while not self._stop.is_set():
            try:
                self.get_one(spark_id, force=True)
            except Exception:
                traceback.print_exc()
            self._stop.wait(self.poll_seconds())

    def stop(self) -> None:
        with self.lock:
            self._stop.set()
            workers = list(self._pollers.values()) + list(self._collectors.values())
            # One shared budget, not a full collector timeout for every thread.
            deadline = max([time.monotonic() + 1.0] + [f.deadline for f in self._inflight.values()])
        for thread in workers:
            if thread is not threading.current_thread():
                thread.join(max(0.0, deadline - time.monotonic()))


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
    host = (cfg.get("server") or {}).get("host") or "127.0.0.1"
    port = int((cfg.get("server") or {}).get("port") or 7474)
    handler = make_handler(cache, cfg)
    httpd = ThreadingHTTPServer((host, port), handler)
    if background_poll:
        cache.start_background()
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
