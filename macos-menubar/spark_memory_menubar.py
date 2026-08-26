#!/usr/bin/env python3
"""
macOS menu-bar companion for DGX Spark Memory Dashboard.

Shows free unified memory + hot model count for the active/first spark.
Click the icon to open the dashboard in your browser.

Dependencies (macOS):
  pip install rumps requests
  # or: python3 -m pip install --user rumps requests

Usage:
  python3 macos-menubar/spark_memory_menubar.py
  DGX_SMD_URL=http://127.0.0.1:7474 DGX_SMD_TOKEN=secret python3 macos-menubar/spark_memory_menubar.py
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser

try:
    import rumps
except ImportError:
    print("Missing dependency: rumps\n  python3 -m pip install --user rumps requests", file=sys.stderr)
    raise SystemExit(1)


DEFAULT_URL = os.environ.get("DGX_SMD_URL", "http://127.0.0.1:7474").rstrip("/")
TOKEN = os.environ.get("DGX_SMD_TOKEN", "")
POLL = float(os.environ.get("DGX_SMD_MENUBAR_POLL", "15"))
SPARK_ID = os.environ.get("DGX_SMD_SPARK_ID", "")  # optional pin


def fetch_summary() -> dict:
    url = f"{DEFAULT_URL}/api/sparks"
    headers = {"User-Agent": "dgx-spark-memory-menubar/0.1"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
        url += f"?token={TOKEN}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def pick_state(payload: dict) -> dict | None:
    states = payload.get("states") or []
    if not states:
        return None
    if SPARK_ID:
        for s in states:
            if s.get("spark_id") == SPARK_ID:
                return s
    return states[0]


def format_title(state: dict | None, err: str | None = None) -> str:
    if err:
        return "⚡ Spark ?"
    if not state:
        return "⚡ Spark —"
    if state.get("error"):
        return "⚡ Spark err"
    mem = state.get("mem") or {}
    total = mem.get("total") or 0
    avail = mem.get("available") or mem.get("free") or 0
    free_gib = avail / (1 << 30) if total else 0
    hot = len((state.get("ollama_ps") or {}).get("models") or [])
    hot += len((state.get("vllm_models") or {}).get("data") or [])
    label = (state.get("spark_label") or state.get("spark_id") or "spark")[:10]
    return f"⚡ {free_gib:.0f}G · {hot} hot"


class SparkMemoryApp(rumps.App):
    def __init__(self):
        super().__init__("⚡ Spark", quit_button=None)
        self.menu = [
            rumps.MenuItem("Open Dashboard", callback=self.open_dashboard),
            rumps.MenuItem("Refresh Now", callback=self.refresh_now),
            None,
            rumps.MenuItem("Quit", callback=self.quit_app),
        ]
        self._detail = rumps.MenuItem("Status: starting…")
        self.menu.insert(0, self._detail)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def open_dashboard(self, _):
        webbrowser.open(DEFAULT_URL + "/")

    def refresh_now(self, _):
        self._tick()

    def quit_app(self, _):
        self._stop.set()
        rumps.quit_application()

    def _loop(self):
        while not self._stop.is_set():
            self._tick()
            self._stop.wait(POLL)

    def _tick(self):
        try:
            payload = fetch_summary()
            state = pick_state(payload)
            title = format_title(state)
            rumps.notification  # keep import used under type checkers
            self.title = title
            if state and not state.get("error"):
                mem = state.get("mem") or {}
                free = (mem.get("available") or 0) / (1 << 30)
                used = ((mem.get("total") or 0) - (mem.get("available") or 0)) / (1 << 30)
                hot_names = [m.get("name") for m in (state.get("ollama_ps") or {}).get("models") or []]
                hot_names += [m.get("id") for m in (state.get("vllm_models") or {}).get("data") or []]
                detail = f"{state.get('spark_label')}: {free:.1f} GiB free · used {used:.1f} GiB"
                if hot_names:
                    detail += " · " + ", ".join(hot_names[:2])
                self._detail.title = detail[:80]
            elif state and state.get("error"):
                self._detail.title = f"Error: {str(state.get('error'))[:60]}"
            else:
                self._detail.title = "No spark state"
        except Exception as e:
            self.title = "⚡ Spark ?"
            self._detail.title = f"Offline: {type(e).__name__}"


def main():
    if sys.platform != "darwin":
        print("This menu bar companion is macOS-only.", file=sys.stderr)
        return 1
    SparkMemoryApp().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
