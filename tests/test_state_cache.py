"""State cache behavior with controlled collectors; no Spark hardware required."""

import io
import json
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from urllib.request import urlopen

from dgx_spark_memory_dashboard.server import StateCache, make_handler, run_server


def config(*ids, poll_seconds=60):
    return {
        "server": {"poll_seconds": poll_seconds},
        "sparks": [
            {"id": sid, "label": sid.upper(), "mode": "remote", "host": sid}
            for sid in ids
        ],
    }


def sample(spark, demo_variant=0):
    return {
        "spark_id": spark["id"],
        "mode": spark["mode"],
        "host": spark["host"],
        "ts": 123.0,
        "fetched_at": 124.0,
        "mem": {"total": 128, "available": 80},
        "ollama_ps": {"models": []},
        "variant": demo_variant,
    }


class StateCacheTests(unittest.TestCase):
    def test_get_one_and_get_all_share_one_inflight_collection(self):
        cache = StateCache(config("spark-2bee"))
        entered = threading.Event()
        duplicate = threading.Event()
        release = threading.Event()
        lock = threading.Lock()
        calls = []
        start = threading.Barrier(7)

        def collect(spark, demo_variant=0):
            with lock:
                calls.append(spark["id"])
                if len(calls) > 1:
                    duplicate.set()
            entered.set()
            release.wait(2)
            return sample(spark, demo_variant)

        def request(index):
            start.wait(2)
            if index % 2:
                return cache.get_all(force=True)["states"][0]
            return cache.get_one("spark-2bee", force=True)

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
            with ThreadPoolExecutor(max_workers=6) as pool:
                requests = [pool.submit(request, i) for i in range(6)]
                try:
                    start.wait(2)
                    self.assertTrue(entered.wait(1))
                    self.assertFalse(duplicate.wait(0.1), "overlapping API calls collected the same node")
                finally:
                    release.set()
                results = [future.result(1) for future in requests]
        self.assertEqual(calls, ["spark-2bee"])
        self.assertTrue(all(state == results[0] for state in results))

    def test_force_all_starts_nodes_in_parallel_preserving_config_order(self):
        cache = StateCache(config("slow", "fast"))
        slow_entered = threading.Event()
        fast_entered = threading.Event()
        release = threading.Event()

        def collect(spark, demo_variant=0):
            if spark["id"] == "slow":
                slow_entered.set()
                release.wait(2)
            else:
                fast_entered.set()
            return sample(spark, demo_variant)

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
            with ThreadPoolExecutor(max_workers=1) as pool:
                request = pool.submit(cache.get_all, force=True)
                try:
                    self.assertTrue(slow_entered.wait(1))
                    self.assertTrue(fast_entered.wait(0.2), "fast node waited for slow node")
                    self.assertFalse(request.done())
                finally:
                    release.set()
                result = request.result(1)
        self.assertEqual([s["spark_id"] for s in result["states"]], ["slow", "fast"])
        self.assertEqual([s["id"] for s in result["sparks"]], ["slow", "fast"])

    def test_background_refreshes_fast_node_while_other_node_is_blocked(self):
        cache = StateCache(config("slow", "fast", poll_seconds=0.02))
        slow_entered = threading.Event()
        fast_third = threading.Event()
        release = threading.Event()
        calls = {"slow": 0, "fast": 0}

        def collect(spark, demo_variant=0):
            sid = spark["id"]
            calls[sid] += 1
            if sid == "slow":
                slow_entered.set()
                release.wait(2)
            elif calls[sid] == 3:
                fast_third.set()
            return dict(sample(spark, demo_variant), sequence=calls[sid])

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
            try:
                cache.start_background()
                self.assertTrue(slow_entered.wait(1))
                self.assertTrue(fast_third.wait(0.4), "slow collection stopped fast node's next polls")
                self.assertGreaterEqual(cache.get_one("fast")["sequence"], 3)
                self.assertEqual(calls["slow"], 1)
            finally:
                release.set()
                cache.stop()

    def test_background_initial_reads_return_pending_without_waiting(self):
        cache = StateCache(config("spark-2bee"))
        entered = threading.Event()
        release = threading.Event()
        completed = threading.Event()
        calls = []

        def collect(spark, demo_variant=0):
            calls.append(spark["id"])
            entered.set()
            release.wait(2)
            return sample(spark, demo_variant)

        def request():
            states = [cache.get_one("spark-2bee"), cache.get_all()["states"][0]]
            completed.set()
            return states

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
            with ThreadPoolExecutor(max_workers=1) as pool:
                try:
                    cache.start_background()
                    self.assertTrue(entered.wait(1))
                    future = pool.submit(request)
                    self.assertTrue(completed.wait(0.2), "cached reads waited for the initial collection")
                    for state in future.result(1):
                        self.assertTrue(state.get("pending"))
                        self.assertFalse(state["stale"])
                        self.assertIsNone(state["last_success_at"])
                        self.assertEqual(state["spark_id"], "spark-2bee")
                        self.assertEqual(state["spark_label"], "SPARK-2BEE")
                        self.assertNotIn("mem", state)
                        self.assertNotIn("ts", state)
                        self.assertNotIn("demo", state)
                    self.assertEqual(calls, ["spark-2bee"])
                finally:
                    release.set()
                    cache.stop()

    def test_failure_preserves_last_good_measurement_until_recovery(self):
        cfg = config("spark-2bee")
        cache = StateCache(cfg)
        good = sample(cfg["sparks"][0])
        failed = {"spark_id": "spark-2bee", "mode": "remote", "host": "spark-2bee", "error": "ssh timed out"}
        recovered = dict(good, mem={"total": 128, "available": 90}, ts=350.0, fetched_at=351.0)
        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=[good, failed, failed, recovered]):
            with patch("dgx_spark_memory_dashboard.server.time.time", return_value=200.0) as clock:
                first = cache.get_one("spark-2bee")
                clock.return_value = 300.0
                stale = cache.get_all(force=True)["states"][0]
                self.assertEqual(stale.get("mem"), good["mem"], "lost the last good measurement")
                self.assertEqual(stale["error"], "ssh timed out")
                self.assertTrue(stale["stale"])
                self.assertFalse(stale["pending"])
                self.assertEqual(stale["last_success_at"], 200.0)
                self.assertEqual(stale["last_attempt_at"], 300.0)
                self.assertEqual(stale["ts"], first["ts"])
                self.assertEqual(stale["fetched_at"], first["fetched_at"])
                self.assertNotIn("error", first, "failure mutated an already returned snapshot")
                clock.return_value = 325.0
                again = cache.get_one("spark-2bee", force=True)
                self.assertEqual(again["last_success_at"], 200.0)
                self.assertEqual(again["last_attempt_at"], 325.0)
                clock.return_value = 400.0
                fresh = cache.get_one("spark-2bee", force=True)
                self.assertEqual(fresh["mem"], recovered["mem"])
                self.assertNotIn("error", fresh)
                self.assertFalse(fresh["stale"])
                self.assertFalse(fresh["pending"])
                self.assertEqual(fresh["last_success_at"], 400.0)
                self.assertEqual(fresh["last_attempt_at"], 400.0)
                self.assertTrue(stale["stale"], "recovery mutated a previous snapshot")

    def test_initial_collection_failures_publish_identified_error_states(self):
        for failure in ({"error": "no JSON from remote collector"}, RuntimeError("enrichment failed")):
            with self.subTest(failure=failure):
                cfg = config("broken", "healthy")
                cache = StateCache(cfg)

                def collect(spark, demo_variant=0):
                    if spark["id"] == "broken":
                        if isinstance(failure, Exception):
                            raise failure
                        return failure
                    return sample(spark, demo_variant)

                with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
                    try:
                        states = cache.get_all(force=True)["states"]
                    except RuntimeError as exc:
                        self.fail(f"collector exception escaped the API: {exc}")
                state = states[0]
                self.assertEqual(state.get("spark_id"), "broken")
                self.assertEqual(state["spark_label"], "BROKEN")
                self.assertEqual(state["host"], "broken")
                self.assertEqual(state["mode"], "remote")
                self.assertTrue(state["error"])
                self.assertTrue(state["stale"])
                self.assertFalse(state["pending"])
                self.assertIsNone(state["last_success_at"])
                self.assertNotIn("mem", state)
                self.assertNotIn("ts", state)
                self.assertFalse(states[1]["stale"])

    def test_force_reads_are_bounded_by_one_parallel_collector_deadline(self):
        for single in (True, False):
            with self.subTest(single=single):
                cfg = config("remote-one", "local-two", "remote-three")
                cfg["sparks"][1]["mode"] = "local"
                cache = StateCache(cfg)
                with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=sample):
                    previous = cache.get_all()["states"]
                release = threading.Event()
                completed = threading.Event()
                calls = []

                def collect(spark, demo_variant=0):
                    calls.append(spark["id"])
                    release.wait(2)
                    return sample(spark, demo_variant)

                def request():
                    result = ([cache.get_one("remote-one", force=True)] if single
                              else cache.get_all(force=True)["states"])
                    completed.set()
                    return result

                with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
                    with patch("dgx_spark_memory_dashboard.server._COLLECTOR_TIMEOUTS",
                               {"remote": 0.08, "local": 0.1}, create=True):
                        with ThreadPoolExecutor(max_workers=1) as pool:
                            try:
                                future = pool.submit(request)
                                self.assertTrue(completed.wait(0.25), "force waited beyond the collector deadline")
                                states = future.result(1)
                                self.assertEqual(len(states), 1 if single else 3)
                                for index, state in enumerate(states):
                                    self.assertIn("timed out", state["error"])
                                    self.assertTrue(state["stale"])
                                    self.assertFalse(state["pending"])
                                    self.assertEqual(state["mem"], previous[index]["mem"])
                                    self.assertEqual(state["last_success_at"], previous[index]["last_success_at"])
                                # A timed-out collector still owns the flight until it actually exits.
                                again = cache.get_one("remote-one", force=True)
                                self.assertTrue(again["stale"])
                                self.assertEqual(calls.count("remote-one"), 1)
                            finally:
                                release.set()
                                cache.stop()

    def test_stop_joins_active_workers_without_waiting_for_poll_interval(self):
        cache = StateCache(config("spark-2bee", "spark-88De", poll_seconds=3600))
        both_entered = threading.Barrier(3)
        release = threading.Event()
        stopped = threading.Event()

        def collect(spark, demo_variant=0):
            both_entered.wait(2)
            release.wait(2)
            return sample(spark, demo_variant)

        def stop():
            cache.stop()
            stopped.set()

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
            with ThreadPoolExecutor(max_workers=1) as pool:
                workers = []
                try:
                    cache.start_background()
                    both_entered.wait(1)
                    workers = [t for t in threading.enumerate() if t.name.startswith("smd-")]
                    request = pool.submit(stop)
                    self.assertFalse(stopped.wait(0.05), "stop returned with collectors still active")
                    release.set()
                    self.assertTrue(stopped.wait(0.3), "stop waited for the poll interval")
                    request.result(1)
                    self.assertTrue(all(not thread.is_alive() for thread in workers))
                    cache.stop()  # idempotent
                finally:
                    release.set()
                    cache.stop()
                    for thread in workers:
                        thread.join(1)

    def test_stop_prevents_a_queued_poller_starting_another_collector(self):
        cache = StateCache(config("spark-2bee"))
        queued = threading.Event()
        release = threading.Event()
        start_one = cache._start_one

        def delayed_start(*args, **kwargs):
            queued.set()
            release.wait(2)
            return start_one(*args, **kwargs)

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=sample) as collect:
            with patch.object(cache, "_start_one", side_effect=delayed_start):
                with ThreadPoolExecutor(max_workers=1) as pool:
                    try:
                        cache.start_background()
                        self.assertTrue(queued.wait(1))
                        stopped = pool.submit(cache.stop)
                        self.assertTrue(cache._stop.wait(1))
                        release.set()
                        stopped.result(1)
                        collect.assert_not_called()
                    finally:
                        release.set()
                        cache.stop()

    def test_server_startup_has_no_redundant_all_node_warmup(self):
        cfg = config("spark-2bee", "spark-88De")
        cache = StateCache(cfg)
        collected = threading.Event()
        calls = []

        def collect(spark, demo_variant=0):
            calls.append(spark["id"])
            if len(set(calls)) == 2:
                collected.set()
            return sample(spark, demo_variant)

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect):
            with patch("dgx_spark_memory_dashboard.server.StateCache", return_value=cache):
                with patch("dgx_spark_memory_dashboard.server.ThreadingHTTPServer") as http_server:
                    http_server.return_value.serve_forever.side_effect = lambda: self.assertTrue(collected.wait(1))
                    with patch.object(cache, "get_all", wraps=cache.get_all) as bulk:
                        with patch("sys.stdout", new=io.StringIO()):
                            run_server(cfg)
                        bulk.assert_not_called()
        self.assertCountEqual(calls, ["spark-2bee", "spark-88De"])
        self.assertTrue(all(not thread.is_alive() for thread in cache._pollers.values()))

    def test_expired_background_reads_stay_cached_during_shared_forced_refresh(self):
        cache = StateCache(config("spark-2bee"))
        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=sample):
            previous = cache.get_one("spark-2bee")
        with cache.lock:
            cache.ts_by_id["spark-2bee"] = time.monotonic() - 100
        entered = threading.Event()
        release = threading.Event()
        read_done = threading.Event()

        def collect(spark, demo_variant=0):
            entered.set()
            release.wait(2)
            return dict(sample(spark, demo_variant), sequence=2)

        def read():
            states = [cache.get_one("spark-2bee"), cache.get_all()["states"][0]]
            read_done.set()
            return states

        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=collect) as collect_mock:
            with ThreadPoolExecutor(max_workers=4) as pool:
                try:
                    starts = [pool.submit(cache.start_background) for _ in range(4)]
                    for start in starts:
                        start.result(1)
                    self.assertTrue(entered.wait(1))
                    single = pool.submit(cache.get_one, "spark-2bee", force=True)
                    bulk = pool.submit(cache.get_all, force=True)
                    cached = pool.submit(read)
                    self.assertTrue(read_done.wait(0.2), "expired cache caused a synchronous read")
                    self.assertEqual(cached.result(1), [previous, previous])
                    self.assertFalse(single.done())
                    self.assertFalse(bulk.done())
                    release.set()
                    self.assertEqual(single.result(1)["sequence"], 2)
                    self.assertEqual(bulk.result(1)["states"][0]["sequence"], 2)
                    self.assertEqual(collect_mock.call_count, 1)
                finally:
                    release.set()
                    cache.stop()

    def test_no_background_single_node_cache_expiry_and_force(self):
        cache = StateCache(config("spark-2bee"))
        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=sample) as collect:
            first = cache.get_one("spark-2bee")
            self.assertFalse(first["pending"])
            self.assertEqual(cache.get_all()["states"], [first])
            self.assertEqual(collect.call_count, 1)
            with cache.lock:
                cache.ts_by_id["spark-2bee"] = time.monotonic() - 100
            cache.get_one("spark-2bee")
            self.assertEqual(collect.call_count, 2)
            cache.get_all(force=True)
            self.assertEqual(collect.call_count, 3)
            self.assertEqual(cache.get_one("missing"), {"error": "unknown spark_id: missing"})
            self.assertEqual(collect.call_count, 3)
        cache.stop()
        self.assertEqual(StateCache(config()).get_all()["states"], [])

    def test_single_spark_http_compatibility_without_background(self):
        cfg = config("spark-2bee")
        cache = StateCache(cfg)
        handler = make_handler(cache, cfg)
        with patch.object(handler, "log_message"):
            with ThreadingHTTPServer(("127.0.0.1", 0), handler) as httpd:
                thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01})
                thread.start()
                try:
                    with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=sample) as collect:
                        base = f"http://127.0.0.1:{httpd.server_port}"
                        for path in ("/api/spark-state", "/api/spark-state?id=spark-2bee", "/api/spark/spark-2bee", "/api/sparks"):
                            with urlopen(base + path, timeout=1) as response:
                                self.assertEqual(response.status, 200)
                                payload = json.load(response)
                            state = payload["states"][0] if path == "/api/sparks" else payload
                            self.assertEqual(state["spark_id"], "spark-2bee")
                            self.assertFalse(state["pending"])
                        self.assertEqual(collect.call_count, 1)
                        for path in ("/api/spark-state?force=1", "/api/spark/spark-2bee?force=yes", "/api/sparks?force=true"):
                            with urlopen(base + path, timeout=1) as response:
                                json.load(response)
                        self.assertEqual(collect.call_count, 4)
                finally:
                    httpd.shutdown()
                    thread.join(1)
                    cache.stop()
                self.assertFalse(thread.is_alive())

    def test_server_bind_failure_does_not_leave_pollers_running(self):
        cfg = config("spark-2bee")
        cache = StateCache(cfg)
        with patch("dgx_spark_memory_dashboard.server.StateCache", return_value=cache):
            with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=sample):
                with patch("dgx_spark_memory_dashboard.server.ThreadingHTTPServer", side_effect=OSError("port in use")):
                    try:
                        with self.assertRaisesRegex(OSError, "port in use"):
                            run_server(cfg)
                        self.assertEqual(cache._pollers, {}, "bind failure orphaned background workers")
                    finally:
                        cache.stop()

    def test_single_node_demo_variant_uses_configured_index(self):
        cache = StateCache(config("spark-2bee", "spark-88De"))
        with patch("dgx_spark_memory_dashboard.server.collect_spark", side_effect=sample):
            all_states = cache.get_all(force=True)["states"]
            for index, state in enumerate(all_states):
                one = cache.get_one(state["spark_id"], force=True)
                self.assertEqual(one["variant"], index)
                self.assertEqual(one["variant"], state["variant"])


if __name__ == "__main__":
    unittest.main()
