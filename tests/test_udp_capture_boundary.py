"""Deterministic recording boundaries against in-flight UDP sample flushes."""
import struct
import tempfile
import threading
import unittest
from unittest import mock

from desktop_log import DesktopCaptureSession, read_log_page, verify_log
import smartdriver_monitor as monitor


class ObservedRLock:
    """A real RLock with an event immediately before one thread acquires it."""
    def __init__(self, watched_thread):
        self.lock = threading.RLock()
        self.watched_thread = watched_thread
        self.attempted = threading.Event()

    def __enter__(self):
        if threading.current_thread().name == self.watched_thread:
            self.attempted.set()
        self.lock.acquire()
        return self

    def __exit__(self, *args):
        self.lock.release()


class UdpCaptureBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.config = monitor.validate_capture_config(
            "127.0.0.1", 6379, 0, monitor.DEFAULT_UDP_GROUP, 7621, "127.0.0.1")
        self.capture = DesktopCaptureSession()
        self.addCleanup(self.capture.stop)
        self.worker = monitor.UdpWorker(monitor.UiEventBus(), self.capture,
                                        monitor.DEFAULT_UDP_GROUP, 7621, "127.0.0.1")
        self.app = monitor.SmartDriverApp.__new__(monitor.SmartDriverApp)
        self.app.capture = self.capture
        self.app.udp_worker = self.worker
        self.app.redis_worker = None
        self.app.root = mock.Mock()
        self.app.messagebox = mock.Mock()
        self.app._applied_config = dict(self.config)
        self.app._validated_config = lambda: dict(self.config)
        self.app.output_root = mock.Mock(get=lambda: self.temporary.name)
        self.app.record_status = mock.Mock()
        self.app.record_button = mock.Mock()
        self.app._append_log = mock.Mock()
        self.app._update_overview = mock.Mock()
        self.errors = []

    def spawn(self, name, target):
        def guarded():
            try:
                target()
            except BaseException as exc:
                self.errors.append(exc)
        thread = threading.Thread(name=name, target=guarded, daemon=True)
        thread.start()
        return thread

    def ingest(self, sequence, when, body):
        packet = b"LC02" + struct.pack(">I", sequence) + b"topic\0" + body
        self.worker.sampler.ingest(packet, ("127.0.0.1", 5000), when, "2000-01-01T00:00:00Z")

    def test_start_waits_for_old_flush_and_never_records_pre_start_snapshot(self):
        lock = ObservedRLock("GUI-start")
        self.worker.sample_lock = lock
        self.ingest(1, 1_000_000_000, b"old-before-recording")
        old_observe_entered = threading.Event()
        release_old_observe = threading.Event()
        start_called = threading.Event()
        original_observe = self.capture.observe
        original_start = self.capture.start

        def paused_observe(*args, **kwargs):
            old_observe_entered.set()
            if not release_old_observe.wait(1):
                raise RuntimeError("test did not release old sample")
            return original_observe(*args, **kwargs)

        def tracked_start(*args, **kwargs):
            start_called.set()
            return original_start(*args, **kwargs)

        flush_thread = start_thread = None
        with mock.patch.object(self.capture, "observe", side_effect=paused_observe), \
                mock.patch.object(self.capture, "start", side_effect=tracked_start):
            try:
                flush_thread = self.spawn("old-flush", self.worker.flush_pending)
                self.assertTrue(old_observe_entered.wait(1))
                start_thread = self.spawn("GUI-start", self.app.toggle_recording)
                self.assertTrue(lock.attempted.wait(1), "GUI start must participate in the UDP boundary lock")
                self.assertFalse(start_called.is_set(), "new recording started while an old sample could still write")
                self.assertFalse(self.capture.active)
            finally:
                release_old_observe.set()
                for thread in (flush_thread, start_thread):
                    if thread is not None:
                        thread.join(1)
                        self.assertFalse(thread.is_alive())
        self.assertFalse(self.errors, repr(self.errors))
        self.app.messagebox.showerror.assert_not_called()
        self.assertTrue(start_called.is_set())
        self.assertTrue(self.capture.active)
        self.capture.stop()
        self.assertEqual(read_log_page(self.capture.directory, kind="udp"), [])
        self.assertEqual(verify_log(self.capture.directory)["counts"]["udp"], 0)

    def test_stop_holds_boundary_across_final_flush_and_database_stop(self):
        lock = ObservedRLock("new-ingest")
        self.worker.sample_lock = lock
        self.capture.start(self.temporary.name, self.config)
        self.worker._sync_capture_generation()
        self.ingest(1, 1_000_000_000, b"final-before-stop")
        stop_entered = threading.Event()
        release_stop = threading.Event()
        ingest_acquired = threading.Event()
        original_stop = self.capture.stop

        def paused_stop():
            stop_entered.set()
            if not release_stop.wait(1):
                raise RuntimeError("test did not release database stop")
            return original_stop()

        def next_ingest():
            with self.worker.sample_lock:
                ingest_acquired.set()
                self.worker._sync_capture_generation()
                self.ingest(2, 2_000_000_000, b"after-stop-boundary")

        stop_thread = ingest_thread = None
        with mock.patch.object(self.capture, "stop", side_effect=paused_stop):
            try:
                stop_thread = self.spawn("GUI-stop", self.app.toggle_recording)
                self.assertTrue(stop_entered.wait(1))
                ingest_thread = self.spawn("new-ingest", next_ingest)
                self.assertTrue(lock.attempted.wait(1))
                self.assertFalse(ingest_acquired.is_set(), "a packet entered between final flush and capture.stop")
                self.assertTrue(self.capture.active)
            finally:
                release_stop.set()
                for thread in (stop_thread, ingest_thread):
                    if thread is not None:
                        thread.join(1)
                        self.assertFalse(thread.is_alive())
        self.assertFalse(self.errors, repr(self.errors))
        self.app.messagebox.showerror.assert_not_called()
        self.assertFalse(self.capture.active)
        self.assertTrue(ingest_acquired.is_set())
        rows = read_log_page(self.capture.directory, kind="udp")
        self.assertEqual(len(rows), 1)
        self.assertEqual(verify_log(self.capture.directory)["counts"]["udp"], 1)


if __name__ == "__main__":
    unittest.main()
