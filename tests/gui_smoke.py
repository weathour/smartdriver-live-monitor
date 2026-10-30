"""Exercise native Tk recording and paged viewing without any field network."""
from pathlib import Path
import struct
import sys
import tempfile
import time
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import tkinter as tk
import smartdriver_monitor as monitor
import log_viewer


class FakeWorker:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    def start(self):
        pass

    def stop(self):
        return True


def wait_for(root, predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        root.update()
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("GUI action timed out")


def main():
    root = tk.Tk()
    app = None
    viewers = []
    callback_errors = []
    root.report_callback_exception = lambda *error: callback_errors.append(error)
    real_udp_worker = monitor.UdpWorker
    real_viewer = log_viewer.LogViewer

    def open_viewer(*args):
        viewer = real_viewer(*args)
        viewers.append(viewer)
        return viewer

    try:
        with tempfile.TemporaryDirectory(prefix="smartdriver-gui-smoke-") as temporary:
            with mock.patch.object(monitor, "RedisWorker", FakeWorker), \
                    mock.patch.object(monitor, "UdpWorker", FakeWorker), \
                    mock.patch.object(log_viewer, "LogViewer", side_effect=open_viewer):
                app = monitor.SmartDriverApp(root, output_root=temporary)
                with mock.patch.object(app.messagebox, "showerror") as errors:
                    wait_for(root, lambda: app._applied_config is not None)
                    assert app.redis_worker.kwargs["interval"] == 1.0
                    app.record_button.invoke()
                    assert app.capture.active
                    for index in range(205):
                        payload = struct.pack("<d", float(index))
                        metadata = {
                            "kind": "redis_observation", "key": "Sensor_Location",
                            "endpoint": "127.0.0.1:6379/db0", "observed_at_utc": monitor.utc_now(),
                            "monotonic_ns": time.monotonic_ns(), "sequence": index + 1,
                            "present": True, "length": len(payload),
                        }
                        _, error = app.capture.observe("redis", payload, metadata,
                            lambda: {"status": "ok", "decoded": {"sample": index}})
                        assert error is None
                    udp = real_udp_worker(app.bus, app.capture, monitor.DEFAULT_UDP_GROUP,
                                          monitor.DEFAULT_UDP_PORT, "127.0.0.1")
                    # Feed the real sampler directly; no sockets or thread are started.
                    udp.stop = lambda: True
                    app.udp_worker = udp
                    for sequence in range(50):
                        packet = b"LC02" + struct.pack(">I", sequence) + b"/test/Chassis\0same-content"
                        udp.sampler.ingest(packet, ("127.0.0.1", 12345),
                                           time.monotonic_ns(), monitor.utc_now())
                    udp.sampler.ingest(b"LC02" + struct.pack(">I", 50) + b"/test/Planning\0latest",
                                       ("127.0.0.1", 12345), time.monotonic_ns(), monitor.utc_now())
                    udp.flush_pending()
                    app._process_events()
                    assert len(app.udp_tree.get_children()) == 2
                    assert app.udp_metrics["received"] == 51
                    assert app.udp_metrics["duplicate"] == 49
                    app.record_button.invoke()
                    assert not app.capture.active
                    path = app._last_capture_directory
                    assert monitor.verify_capture(path)["counts"]["udp"] == 2
                    assert len(list(Path(temporary).iterdir())) == 1
                    app.udp_tree.selection_set(app.udp_tree.get_children()[0])
                    wait_for(root, lambda: "RAW HEX" in app.udp_detail.get("1.0", "end"))
                    app.open_capture_directory()
                    assert len(viewers) == 1
                    viewer = viewers[0]
                    wait_for(root, lambda: len(viewer.tree.get_children()) == 200)
                    viewer.next.invoke()
                    wait_for(root, lambda: 0 < len(viewer.tree.get_children()) < 200)
                    viewer.kind.set("UDP")
                    viewer.load_page(0)
                    wait_for(root, lambda: len(viewer.tree.get_children()) == 2)
                    viewer.tree.selection_set(viewer.tree.get_children()[0])
                    wait_for(root, lambda: "payload_length" in viewer.detail.get("1.0", "end"))
                    viewer.close()
                    app.redis_host.set("127.0.0.2")
                    app.record_button.invoke()
                    assert not app.capture.active
                    errors.assert_called_once()
                    assert not callback_errors, callback_errors
    finally:
        for viewer in viewers:
            if not viewer.closed:
                viewer.close()
        if app is not None:
            app.close()
        else:
            root.destroy()
    print("GUI_SMOKE_TEST=OK")


if __name__ == "__main__":
    main()
