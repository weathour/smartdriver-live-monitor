"""Single-file format regressions; SQLite, synthetic bytes and no network."""
import concurrent.futures
from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest import mock
import zipfile

import desktop_log as log


class DesktopLogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.session = log.DesktopCaptureSession()
        self.config = {"redis_interval_ms": 1000, "test": True}
        self.path = self.session.start(self.root, self.config)
        self.addCleanup(self.session.stop)

    def observe(self, payload=b"same bytes", kind="udp", **metadata):
        values = {"kind": "udp_message" if kind == "udp" else "redis_observation",
                  "source_ip": "127.0.0.1", "source_port": 5000, "channel": "topic"}
        if kind == "redis":
            values.update(endpoint="127.0.0.1:6379/db0", key="sensor", present=payload is not None)
        values.update(metadata)
        return self.session.observe(kind, payload, values, lambda: {"status": "ok", "decoded": {"value": 3}})

    @contextmanager
    def transaction(self):
        # Connection.__exit__ commits/rolls back but does not close the handle.
        with closing(sqlite3.connect(self.path)) as connection:
            with connection:
                yield connection

    def sql(self, query, parameters=()):
        with self.transaction() as connection:
            return connection.execute(query, parameters).fetchall()

    def test_transaction_commits_and_explicitly_closes_real_connection(self):
        self.session.stop()
        with self.transaction() as connection:
            connection.execute("CREATE TABLE lifecycle_check (value INTEGER)")
            connection.execute("INSERT INTO lifecycle_check VALUES(42)")
        with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
            connection.execute("SELECT 1")
        self.assertEqual(self.sql("SELECT value FROM lifecycle_check"), [(42,)])
        with self.assertRaisesRegex(RuntimeError, "rollback probe"):
            with self.transaction() as failed_connection:
                failed_connection.execute("INSERT INTO lifecycle_check VALUES(99)")
                raise RuntimeError("rollback probe")
        with self.assertRaisesRegex(sqlite3.ProgrammingError, "closed"):
            failed_connection.execute("SELECT 1")
        self.assertEqual(self.sql("SELECT value FROM lifecycle_check"), [(42,)])

    def test_content_dedup_retains_each_observation_and_one_completed_file(self):
        for kind in ("udp", "udp", "redis"):
            parsed, error = self.observe(kind=kind)
            self.assertIsNone(error)
            self.assertEqual(parsed["status"], "ok")
        self.assertTrue(self.session.record_inventory({"keys": [{"key": "sensor"}]}))
        self.session.stop()
        manifest = log.verify_log(self.path)
        self.assertEqual(manifest["format_version"], 4)
        self.assertEqual(manifest["recorder_version"], "1.5")
        self.assertEqual(manifest["counts"], {"redis": 1, "udp": 2, "events": 2, "inventory": 1})
        self.assertEqual(self.sql("SELECT count(*) FROM payloads"), [(1,)])
        self.assertEqual(list(self.root.iterdir()), [self.path])
        self.assertEqual(self.path.suffix, ".sdlog")
        self.assertEqual(manifest["config"], self.config)
        self.assertIn("durability", manifest["semantics"])
        self.assertIsNotNone(manifest["ended_at_utc"])

    def test_raw_bytes_are_in_transaction_before_decoder(self):
        payload = b"original\0\xff"
        digest = hashlib.sha256(payload).hexdigest()

        def decoder():
            row = self.session._connection.execute("SELECT data FROM payloads WHERE sha256=?", (digest,)).fetchone()
            self.assertEqual(row[0], payload)
            return {"status": "parse_error", "error": "synthetic malformed payload"}

        parsed, error = self.session.observe("udp", payload, {}, decoder)
        self.assertIsNone(error)
        self.assertEqual(parsed["status"], "parse_error")
        self.session.stop()
        record = log.read_log_record(self.path, log.read_log_page(self.path, kind="udp")[0]["id"])
        self.assertEqual(record["payload_preview_hex"], payload.hex())
        log.verify_log(self.path)

    def test_pagination_is_bounded_and_never_selects_payload_bytes(self):
        payload = bytes(range(256)) * 16384  # 4 MiB, never selected by a page query.
        for index in range(8):
            self.observe(payload, stream_key="test-stream", sequence=index)
        self.session.stop()
        statements = []
        original_open = log._open_reader

        def traced_reader(path):
            connection = original_open(path)
            connection.set_trace_callback(statements.append)
            return connection

        with mock.patch.object(log, "_open_reader", side_effect=traced_reader):
            rows = log.read_log_page(self.path, offset=2, limit=3, kind="udp")
        self.assertEqual([row["metadata"]["sequence"] for row in rows], [2, 3, 4])
        self.assertTrue(all(row["stream_key"] == "test-stream" for row in rows))
        self.assertTrue(all("parse" in row["metadata"] for row in rows))
        self.assertFalse(any("payloads" in sql.lower() for sql in statements))
        self.assertFalse(any("metadata_json" in sql.lower() for sql in statements))
        self.assertEqual(log.read_log_page(self.path, limit=0), [])
        for kwargs in ({"offset": -1}, {"limit": -1}, {"limit": 1001}):
            with self.assertRaises(ValueError):
                log.read_log_page(self.path, **kwargs)
        log.verify_log(self.path)  # Streams multiple hash chunks.

    def test_page_uses_compact_projection_while_selected_record_keeps_full_metadata(self):
        decoded = [{"index": index, "coordinates": [index, index + 1]} for index in range(30000)]
        metadata = {"stream_key": "stream" * 1000, "topic": "topic" * 1000,
                    "source_ip": "source" * 1000, "received_count": 42,
                    "key": ["a list must not enter summaries"], "raw_hex": "ab" * 200000}
        self.session.observe("udp", b"payload", metadata,
                             lambda: {"status": "ok", "decoded": decoded, "analysis": {"large": decoded}})
        self.session.log("INFO", "event" * 1000)
        self.session.stop()
        page = log.read_log_page(self.path, kind="udp")
        summary = page[0]["metadata"]
        self.assertEqual(summary["parse"], {"status": "ok"})
        self.assertNotIn("key", summary)
        self.assertNotIn("raw_hex", summary)
        self.assertEqual(summary["received_count"], 42)
        self.assertLessEqual(len(summary["topic"]), 1024)
        self.assertLessEqual(len(summary["source_ip"]), 1024)
        self.assertEqual(len(page[0]["stream_key"]), 1024)
        self.assertLess(len(json.dumps(page)), 6000)
        full = log.read_log_record(self.path, page[0]["id"])
        self.assertEqual(full["metadata"]["parse"]["decoded"], decoded)
        self.assertEqual(full["metadata"]["raw_hex"], metadata["raw_hex"])
        self.assertEqual(full["stream_key"], metadata["stream_key"])
        event = next(row for row in log.read_log_page(self.path, kind="events")
                     if row["metadata"]["message"].startswith("event"))
        self.assertEqual(len(event["metadata"]["message"]), 256)
        log.verify_log(self.path)

    def test_record_preview_uses_substr_and_respects_requested_size(self):
        payload = b"abcdef" * 600000
        self.observe(payload)
        self.session.stop()
        record_id = log.read_log_page(self.path, kind="udp")[0]["id"]
        statements = []
        original_open = log._open_reader

        def traced_reader(path):
            connection = original_open(path)
            connection.set_trace_callback(statements.append)
            return connection

        with mock.patch.object(log, "_open_reader", side_effect=traced_reader):
            record = log.read_log_record(self.path, record_id, preview_bytes=19)
        self.assertEqual(record["payload_preview_hex"], payload[:19].hex())
        self.assertEqual(record["payload_length"], len(payload))
        self.assertEqual(record["sha256"], hashlib.sha256(payload).hexdigest())
        self.assertTrue(any("substr(p.data,1,19)" in sql for sql in statements))
        self.assertEqual(log.read_log_record(self.path, record_id, 0)["payload_preview_hex"], "")
        with self.assertRaises(KeyError):
            log.read_log_record(self.path, 99999)
        with self.assertRaises(ValueError):
            log.read_log_record(self.path, record_id, 1024 * 1024 + 1)

    def test_tick_commits_silent_pending_work_and_inspect_exposes_committed_counts(self):
        with mock.patch.object(log.time, "monotonic", return_value=self.session._last_commit):
            self.observe(b"pending")
            self.assertFalse(self.session.tick())
        self.assertEqual(log.inspect_log(self.path)["counts"]["udp"], 0)
        with mock.patch.object(log.time, "monotonic", return_value=self.session._last_commit + 1.01):
            self.assertTrue(self.session.tick())
        self.assertEqual(log.inspect_log(self.path)["counts"]["udp"], 1)
        self.assertEqual(len(log.read_log_page(self.path, kind="udp")), 1)
        self.assertEqual(log.inspect_log(self.path)["status"], "recording")
        with self.assertRaisesRegex(ValueError, "not complete"):
            log.verify_log(self.path)
        self.session.stop()
        log.verify_log(self.path)

    def test_flush_commits_without_waiting_and_inactive_operations_do_not_write(self):
        self.observe(kind="redis", payload=None)
        self.assertTrue(self.session.flush())
        self.assertEqual(log.inspect_log(self.path)["counts"]["redis"], 1)
        self.session.stop()
        decoder = mock.Mock(return_value={"status": "ok"})
        parsed, error = self.session.observe("udp", b"not-recorded", {}, decoder)
        self.assertIsNone(error)
        self.assertEqual(parsed["status"], "ok")
        decoder.assert_called_once()
        self.assertFalse(self.session.flush())
        self.assertFalse(self.session.tick())
        self.assertFalse(self.session.record_inventory({}))
        log.verify_log(self.path)

    def test_repeated_start_stop_creates_distinct_logs_and_advances_generation(self):
        generation = self.session.generation
        self.assertEqual(self.session.start(self.root, {"ignored": True}), self.path)
        self.assertEqual(self.session.generation, generation)
        self.observe()
        self.session.stop()
        self.assertEqual(self.session.stop(), self.path)
        second = self.session.start(self.root, {"second": True})
        self.assertNotEqual(second, self.path)
        self.assertEqual(self.session.generation, generation + 1)
        self.assertEqual(self.session.directory, second)
        self.assertEqual(self.session.counts["udp"], 0)
        self.assertIsNone(self.session.last_error)
        self.session.stop()
        log.verify_log(self.path)
        self.assertEqual(log.verify_log(second)["config"], {"second": True})
        self.assertEqual(len(list(self.root.iterdir())), 2)

    def test_concurrent_observations_keep_exact_counts_and_single_blob(self):
        def produce(worker):
            for number in range(60):
                parsed, error = self.observe(b"shared", producer=worker, sample=number)
                self.assertIsNone(error)
                self.assertEqual(parsed["status"], "ok")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(produce, range(4)))
        self.session.stop()
        self.assertEqual(log.verify_log(self.path)["counts"]["udp"], 240)
        self.assertEqual(self.sql("SELECT count(*) FROM payloads"), [(1,)])
        rows = log.read_log_page(self.path, limit=1000, kind="udp")
        self.assertEqual(len({row["id"] for row in rows}), 240)
        stored = [json.loads(row[0]) for row in self.sql("SELECT metadata_json FROM records WHERE kind='udp'")]
        self.assertEqual(len({(row["producer"], row["sample"]) for row in stored}), 240)

    def test_payload_corruption_is_detected(self):
        self.observe(b"abcd")
        self.session.stop()
        self.sql("UPDATE payloads SET data=?", (b"abce",))
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            log.verify_log(self.path)
        with self.assertRaises(ValueError):
            log.pack_log(self.path)

    def test_length_counts_and_reference_corruption_are_detected(self):
        self.observe(b"payload")
        self.session.stop()
        with self.transaction() as connection:
            original_manifest = connection.execute("SELECT json FROM manifest").fetchone()[0]
            manifest = json.loads(original_manifest)
            manifest["counts"]["udp"] = 0
            connection.execute("UPDATE manifest SET json=?", (json.dumps(manifest),))
        with self.assertRaisesRegex(ValueError, "counts"):
            log.verify_log(self.path)
        self.sql("UPDATE manifest SET json=?", (original_manifest,))
        self.sql("UPDATE payloads SET length=length+1")
        with self.assertRaisesRegex(ValueError, "length mismatch"):
            log.verify_log(self.path)
        self.sql("UPDATE payloads SET length=length-1")
        self.sql("UPDATE records SET payload_sha256=? WHERE kind='udp'", ("f" * 64,))
        with self.assertRaisesRegex(ValueError, "reference"):
            log.verify_log(self.path)

    def test_malformed_record_metadata_is_rejected(self):
        self.observe()
        self.session.stop()
        self.sql("UPDATE records SET metadata_json='[]' WHERE kind='udp'")
        with self.assertRaisesRegex(ValueError, "JSON object"):
            log.verify_log(self.path)

    def test_write_failure_aborts_rolls_back_pending_rows_and_is_not_complete(self):
        connection = self.session._connection
        proxy = mock.Mock(wraps=connection)

        def fail_record(sql, parameters=()):
            if sql.startswith("INSERT INTO records"):
                raise sqlite3.OperationalError("database or disk is full")
            return connection.execute(sql, parameters)

        proxy.execute.side_effect = fail_record
        self.session._connection = proxy
        parsed, error = self.observe(b"cannot store timeline")
        self.assertEqual(parsed["status"], "ok")
        self.assertIn("disk is full", error)
        self.assertFalse(self.session.active)
        self.assertIsNone(self.session._connection)
        self.assertEqual(self.session.counts["udp"], 0)
        self.assertEqual(log.inspect_log(self.path)["status"], "failed")
        with self.assertRaises(ValueError):
            log.verify_log(self.path)
        self.assertEqual(self.sql("SELECT count(*) FROM payloads"), [(0,)])
        next_path = self.session.start(self.root, self.config)
        self.session.stop()
        log.verify_log(next_path)

    def test_tick_commit_failure_is_reported_and_writer_closed(self):
        self.observe(b"pending")
        connection = self.session._connection
        proxy = mock.Mock(wraps=connection)
        commits = []

        def fail_first_commit():
            commits.append(True)
            if len(commits) == 1:
                raise sqlite3.OperationalError("database or disk is full")
            return connection.commit()

        proxy.commit.side_effect = fail_first_commit
        self.session._connection = proxy
        with mock.patch.object(log.time, "monotonic", return_value=self.session._last_commit + 1.01):
            self.assertFalse(self.session.tick())
        self.assertFalse(self.session.active)
        self.assertIsNone(self.session._connection)
        self.assertIn("commit failed", self.session.last_error)
        self.assertEqual(log.inspect_log(self.path)["status"], "failed")
        with self.assertRaises(ValueError):
            log.verify_log(self.path)

    def test_stop_event_write_failure_marks_failed_and_closes(self):
        with mock.patch.object(self.session, "_log_locked", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaisesRegex(RuntimeError, "finalization failed"):
                self.session.stop()
        self.assertFalse(self.session.active)
        self.assertIsNone(self.session._connection)
        self.assertEqual(log.inspect_log(self.path)["status"], "failed")
        with self.assertRaises(ValueError):
            log.verify_log(self.path)

    def test_partial_start_failure_is_closed_and_retryable(self):
        self.session.stop()
        with mock.patch.object(log, "_configure", side_effect=sqlite3.OperationalError("disk full")):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                self.session.start(self.root, self.config)
        failed_path = self.session.directory
        self.assertFalse(self.session.active)
        self.assertIsNone(self.session._connection)
        with self.assertRaises((ValueError, sqlite3.DatabaseError)):
            log.verify_log(failed_path)
        next_path = self.session.start(self.root, self.config)
        self.session.stop()
        log.verify_log(next_path)

    def test_pack_is_optional_and_carries_one_database_plus_external_digest(self):
        self.observe()
        self.session.stop()
        archive = log.pack_log(self.path)
        with zipfile.ZipFile(archive) as bundle:
            self.assertEqual(bundle.namelist(), [self.path.name])
            self.assertEqual(hashlib.sha256(bundle.read(self.path.name)).hexdigest(), log._sha256_file(self.path))
        sidecar = archive.with_name(archive.name + ".sha256")
        self.assertEqual(sidecar.read_text().split("  ")[0], log._sha256_file(archive))
        with self.assertRaises(FileExistsError):
            log.pack_log(self.path)


if __name__ == "__main__":
    unittest.main()
