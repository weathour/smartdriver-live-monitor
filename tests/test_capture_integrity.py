"""Capture lifecycle and GUI provenance regressions; no network or Tk display."""
import importlib.util
import json
from pathlib import Path
import tempfile
from desktop_log import inspect_log, read_log_page
from types import SimpleNamespace
import unittest
from unittest import mock
import zipfile

SOURCE = Path(__file__).resolve().parents[1] / "smartdriver_monitor.py"
spec = importlib.util.spec_from_file_location("capture_integrity_monitor", SOURCE)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


class FakeWorker:
    def __init__(self, *args, **kwargs):
        self.started = False
        self.stopped = False
        self.stop_result = True
        self.kwargs = kwargs

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True
        return self.stop_result


class CaptureIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = monitor.validate_capture_config(
            "127.0.0.1", 6379, 0, "239.255.42.99", 7667, "auto")

    def complete_capture(self):
        capture = monitor.CaptureSession()
        directory = capture.start(self.root, self.config)
        capture.stop()
        return directory

    def rewrite_manifest(self, directory, **updates):
        path = directory / "manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest.update(updates)
        path.write_text(json.dumps(manifest), encoding="utf-8")
        monitor.write_capture_checksums(directory)

    def test_stop_event_disk_error_closes_all_handles_and_marks_failed(self):
        capture = monitor.CaptureSession()
        directory = capture.start(self.root, self.config)
        handles = list(capture.handles.values())
        events = capture.handles["events"]
        wrapper = mock.Mock(wraps=events)
        wrapper.write.side_effect = OSError(28, "No space left on device")
        wrapper.flush.side_effect = OSError(28, "No space left on device")
        capture.handles["events"] = wrapper
        with self.assertRaisesRegex(RuntimeError, "finalization failed"):
            capture.stop()
        self.assertFalse(capture.active)
        self.assertFalse(capture.handles)
        self.assertTrue(all(handle.closed for handle in handles))
        manifest = json.loads((directory / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertIn("No space left", manifest["last_error"])
        with self.assertRaises(ValueError):
            monitor.verify_capture(directory)

    def test_failed_start_manifest_does_not_leave_active_capture(self):
        capture = monitor.CaptureSession()
        original = capture._write_manifest_locked
        calls = []

        def fail_first(*args):
            calls.append(args)
            if len(calls) == 1:
                raise OSError(28, "No space left on device")
            return original(*args)

        with mock.patch.object(capture, "_write_manifest_locked", side_effect=fail_first):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                capture.start(self.root, self.config)
        self.assertFalse(capture.active)
        self.assertFalse(capture.handles)
        self.assertEqual(json.loads((capture.directory / "manifest.json").read_text())["status"], "failed")

    def test_partial_start_open_failure_closes_previously_opened_file(self):
        capture = monitor.CaptureSession()
        original = Path.open
        opened = []

        def fail_udp(path, *args, **kwargs):
            if path.name == "udp-observations.jsonl":
                raise OSError(24, "Too many open files")
            handle = original(path, *args, **kwargs)
            opened.append(handle)
            return handle

        with mock.patch.object(Path, "open", fail_udp):
            with self.assertRaisesRegex(RuntimeError, "start failed"):
                capture.start(self.root, self.config)
        self.assertFalse(capture.active)
        self.assertFalse(capture.handles)
        self.assertTrue(all(handle.closed for handle in opened))

    def test_extra_checksum_named_files_are_rejected(self):
        directory = self.complete_capture()
        for relative in ("payloads/udp/SHA256SUMS", "SHA256SUMS.tmp", "payloads/redis/SHA256SUMS.tmp"):
            with self.subTest(relative=relative):
                extra = directory / relative
                extra.write_bytes(b"unverified extra content")
                with self.assertRaisesRegex(ValueError, "file set mismatch"):
                    monitor.verify_capture(directory)
                with self.assertRaises(ValueError):
                    monitor.pack_capture(directory)
                extra.unlink()

    def test_nested_checksum_named_file_is_checked_when_in_inventory(self):
        directory = self.complete_capture()
        extra = directory / "payloads/udp/SHA256SUMS"
        extra.write_bytes(b"included original")
        monitor.write_capture_checksums(directory)
        monitor.verify_capture(directory)
        extra.write_bytes(b"changed content")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            monitor.verify_capture(directory)

    def test_v3_and_legacy_v2_verify_and_pack_same_file_set(self):
        directory = self.complete_capture()
        self.assertEqual(monitor.verify_capture(directory)["format_version"], 3)
        self.rewrite_manifest(directory, format_version=2)
        self.assertEqual(monitor.verify_capture(directory)["format_version"], 2)
        archive = monitor.pack_capture(directory)
        with zipfile.ZipFile(archive) as bundle:
            actual = {path.relative_to(directory).as_posix() for path in directory.rglob("*") if path.is_file()}
            self.assertEqual({name.split("/", 1)[1] for name in bundle.namelist()}, actual)
        self.assertTrue(archive.with_name(archive.name + ".sha256").is_file())

    def test_complete_capture_with_recorded_error_is_rejected(self):
        directory = self.complete_capture()
        self.rewrite_manifest(directory, last_error="disk write error")
        with self.assertRaisesRegex(ValueError, "error-free"):
            monitor.verify_capture(directory)

    def fake_app(self):
        app = monitor.SmartDriverApp.__new__(monitor.SmartDriverApp)
        app.root = mock.Mock()
        app.messagebox = mock.Mock()
        app.bus = monitor.UiEventBus()
        app.capture = monitor.CaptureSession()
        app.redis_worker = None
        app.udp_worker = None
        app._applied_config = None
        app._validated_config = mock.Mock(return_value=dict(self.config))
        app.output_root = SimpleNamespace(get=lambda: str(self.root))
        app.record_status = mock.Mock()
        app.record_button = mock.Mock()
        app._append_log = mock.Mock()
        app._update_overview = mock.Mock()
        return app

    def test_gui_rejects_recording_before_workers_start_or_settings_applied(self):
        app = self.fake_app()
        app.toggle_recording()
        self.assertFalse(app.capture.active)
        app.messagebox.showerror.assert_called_once()
        with mock.patch.object(monitor, "RedisWorker", FakeWorker), mock.patch.object(monitor, "UdpWorker", FakeWorker):
            app.restart_workers()
        self.assertTrue(app.redis_worker.started)
        self.assertTrue(app.udp_worker.started)
        self.assertEqual(app.redis_worker.kwargs["interval"], self.config["redis_interval_ms"] / 1000.0)
        app._validated_config.return_value = dict(self.config, redis_port=6380)
        app.toggle_recording()
        self.assertFalse(app.capture.active)
        self.assertIsNone(app.capture.directory)
        self.assertIn("尚未应用", app.messagebox.showerror.call_args.args[1])

    def test_gui_recording_uses_applied_config_and_prevents_reconnect(self):
        app = self.fake_app()
        with mock.patch.object(monitor, "RedisWorker", FakeWorker), mock.patch.object(monitor, "UdpWorker", FakeWorker):
            app.restart_workers()
            app.toggle_recording()
            self.assertTrue(app.capture.active)
            self.assertEqual(app.capture.config, self.config)
            app._validated_config.return_value = dict(self.config, redis_port=6380)
            app.restart_workers()
            self.assertFalse(app.redis_worker.stopped)
            self.assertFalse(app.udp_worker.stopped)
            self.assertEqual(app._applied_config, self.config)
        app.toggle_recording()
        monitor.verify_capture(app.capture.directory)

    def test_gui_stop_failure_prevents_replacement_workers(self):
        app = self.fake_app()
        app.redis_worker = FakeWorker()
        app.redis_worker.stop_result = False
        app.udp_worker = FakeWorker()
        with mock.patch.object(monitor, "RedisWorker") as redis_factory, mock.patch.object(monitor, "UdpWorker") as udp_factory:
            app.restart_workers()
        redis_factory.assert_not_called()
        udp_factory.assert_not_called()
        self.assertTrue(app.udp_worker.stopped)
        self.assertIsNone(app._applied_config)

    def test_gui_failed_second_worker_start_is_cleaned_and_retryable(self):
        app = self.fake_app()
        redis = FakeWorker()
        udp = FakeWorker()
        udp.start = mock.Mock(side_effect=RuntimeError("cannot start thread"))
        with mock.patch.object(monitor, "RedisWorker", return_value=redis), mock.patch.object(monitor, "UdpWorker", return_value=udp):
            app.restart_workers()
        self.assertTrue(redis.stopped)
        self.assertIsNone(app._applied_config)
        self.assertIsNone(app.redis_worker)
        self.assertIsNone(app.udp_worker)
        with mock.patch.object(monitor, "RedisWorker", FakeWorker), mock.patch.object(monitor, "UdpWorker", FakeWorker):
            app.restart_workers()
        self.assertEqual(app._applied_config, self.config)

    def test_gui_close_stops_receivers_before_capture_and_reports_disk_error(self):
        app = self.fake_app()
        order = []
        app.redis_worker = mock.Mock(stop=mock.Mock(side_effect=lambda: order.append("redis") or True))
        app.udp_worker = mock.Mock(stop=mock.Mock(side_effect=lambda: order.append("udp") or True))
        app.capture = mock.Mock(stop=mock.Mock(side_effect=lambda: order.append("capture")))
        app.close()
        self.assertEqual(order, ["redis", "udp", "capture"])
        app.root.destroy.assert_called_once()
        app.capture.stop.side_effect = OSError(28, "No space left on device")
        app.close()
        app.messagebox.showerror.assert_called_once()

    def test_headless_drains_workers_before_completion(self):
        captures = []
        original = monitor.DesktopCaptureSession

        def new_capture():
            capture = original()
            captures.append(capture)
            return capture

        class DrainingWorker(FakeWorker):
            def __init__(self, bus, capture, *args, **kwargs):
                super().__init__()
                self.capture = capture

            def stop(self):
                self.capture.log("INFO", "worker drained before finalization")
                return True

        with mock.patch.object(monitor, "DesktopCaptureSession", side_effect=new_capture), mock.patch.object(monitor, "RedisWorker", DrainingWorker), mock.patch.object(monitor, "UdpWorker", DrainingWorker):
            directory = monitor.run_headless(self.config, self.root, duration_seconds=0.001)
        events = [row["metadata"] for row in read_log_page(directory, kind="events")]
        self.assertEqual(sum(event["message"] == "worker drained before finalization" for event in events), 2)
        self.assertEqual(monitor.verify_capture(directory)["status"], "complete")

    def test_headless_shutdown_failure_is_not_a_complete_deliverable(self):
        class FailedStopWorker(FakeWorker):
            def stop(self):
                return False

        with mock.patch.object(monitor, "RedisWorker", FailedStopWorker), mock.patch.object(monitor, "UdpWorker", FakeWorker):
            with self.assertRaisesRegex(RuntimeError, "receiver did not stop"):
                monitor.run_headless(self.config, self.root, duration_seconds=0.001)
        directory = next(self.root.glob("SmartDriverLiveCapture-*"))
        self.assertEqual(inspect_log(directory)["status"], "failed")
        with self.assertRaises(ValueError):
            monitor.verify_capture(directory)

    def test_headless_failed_second_start_cleans_started_worker_and_capture(self):
        redis = FakeWorker()
        udp = FakeWorker()
        udp.start = mock.Mock(side_effect=RuntimeError("cannot start thread"))
        with mock.patch.object(monitor, "RedisWorker", return_value=redis), mock.patch.object(monitor, "UdpWorker", return_value=udp):
            with self.assertRaisesRegex(RuntimeError, "cannot start thread"):
                monitor.run_headless(self.config, self.root, duration_seconds=0.001)
        self.assertTrue(redis.stopped)
        self.assertFalse(udp.stopped)
        directory = next(self.root.glob("SmartDriverLiveCapture-*"))
        self.assertEqual(inspect_log(directory)["status"], "failed")


if __name__ == "__main__":
    unittest.main()
