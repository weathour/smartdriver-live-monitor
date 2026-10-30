"""Desktop recorder regressions; synthetic data and local sockets only."""
import json
import socket
import struct
import tempfile
import threading
import time
import unittest
from unittest import mock

import smartdriver_monitor as m


def config():
    return dict(m.validate_capture_config("127.0.0.1", 6379, 0,
                                         m.DEFAULT_UDP_GROUP, 7621, "127.0.0.1"),
                self_test=True)


class SamplingTests(unittest.TestCase):
    def test_default_polling_is_one_hz(self):
        worker = m.RedisWorker(m.UiEventBus(), m.CaptureSession(), "unused", 0, 0)
        self.assertEqual(config()["redis_interval_ms"], 1000)
        self.assertEqual(worker.interval, 1.0)
        arrivals = []

        class Redis:
            def scan_keys(self):
                raise m.RedisCommandError("NOPERM SCAN")

            def command(self, name, *keys):
                self_test.assertEqual(name, "MGET")
                arrivals.append(time.monotonic())
                if len(arrivals) == 3:
                    worker.stop_event.set()
                return [None] * len(keys)

        self_test = self
        worker._poll(Redis(), "unused:0/db0")
        self.assertEqual(len(arrivals), 3)
        for first, second in zip(arrivals, arrivals[1:]):
            self.assertGreaterEqual(second - first, 0.95)
            self.assertLess(second - first, 2.0)

    def test_get_disconnect_preserves_every_completed_read(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = m.CaptureSession()
            directory = capture.start(temporary, config())
            worker = m.RedisWorker(m.UiEventBus(), capture, "unused", 0, 0, interval=0)
            completed = []

            class Redis:
                def scan_keys(self):
                    raise m.RedisCommandError("NOPERM SCAN")

                def command(self, name, *keys):
                    if name == "MGET":
                        raise m.RedisCommandError("NOPERM MGET")
                    if len(completed) == 2:
                        raise m.RedisProtocolError("connection closed on third GET")
                    completed.append(keys[0])
                    return b"\x09" + struct.pack("<d", float(len(completed)))

            with mock.patch.object(m.time, "monotonic_ns", side_effect=[100, 200]):
                with self.assertRaises(m.RedisProtocolError):
                    worker._poll(Redis(), "unused:0/db0")
            capture.stop()
            manifest = m.verify_capture(directory)
            rows = [json.loads(line) for line in
                    (directory / "redis-observations.jsonl").read_text().splitlines()]
            self.assertEqual(manifest["counts"]["redis"], 2)
            self.assertEqual([row["key"] for row in rows], completed)
            self.assertEqual([row["monotonic_ns"] for row in rows], [100, 200])
            self.assertEqual([row["sequence"] for row in rows], [1, 1])
            self.assertEqual(len(list((directory / "payloads/redis").glob("*.bin"))), 2)

    def test_stop_interrupts_pending_redis_read(self):
        client, server = socket.socketpair()
        client.settimeout(10)
        server.settimeout(2)
        connection = m.RedisConnection("unused", 0, 0)
        connection.sock = mock.Mock(wraps=client)
        connection.sock.shutdown.side_effect = OSError("shutdown did not wake the read")
        connection.stream = client.makefile("rwb", buffering=0)
        worker = m.RedisWorker(m.UiEventBus(), m.CaptureSession(), "unused", 0, 0)
        try:
            with mock.patch.object(m, "RedisConnection", return_value=connection), \
                    mock.patch.object(connection, "connect"):
                worker.start()
                self.assertTrue(server.recv(4096))  # SCAN is blocked waiting for a reply.
                started = time.monotonic()
                self.assertTrue(worker.stop())
                self.assertLess(time.monotonic() - started, 2)
        finally:
            worker.stop_event.set()
            server.close()
            connection.close()

    def test_stop_interrupts_partial_resp_header_without_waiting_for_newline(self):
        client, server = socket.socketpair()
        client.settimeout(10)
        server.settimeout(2)
        connection = m.RedisConnection("unused", 0, 0, timeout=10)
        connection.sock = mock.Mock(wraps=client)
        connection.sock.shutdown.side_effect = OSError("shutdown did not wake the read")
        connection.stream = client.makefile("rwb", buffering=0)
        worker = m.RedisWorker(m.UiEventBus(), m.CaptureSession(), "unused", 0, 0)
        reading_header = threading.Event()
        original = connection._read_line

        def read_header():
            reading_header.set()
            return original()

        try:
            with mock.patch.object(m, "RedisConnection", return_value=connection), \
                    mock.patch.object(connection, "connect"), \
                    mock.patch.object(connection, "_read_line", side_effect=read_header):
                worker.start()
                self.assertTrue(server.recv(4096))
                server.sendall(b"+incomplete")
                self.assertTrue(reading_header.wait(2))
                started = time.monotonic()
                self.assertTrue(worker.stop())
                self.assertLess(time.monotonic() - started, 2)
        finally:
            worker.stop_event.set()
            server.close()
            worker.thread.join(2)
            connection.close()


class LcmTests(unittest.TestCase):
    @staticmethod
    def short(channel=b"/drivers/canbus/Chassis", body=b"12345678body"):
        return b"LC02" + struct.pack(">I", 23) + channel + b"\0" + body

    def test_lcm_is_never_decoded_as_legacy_vehicle(self):
        parsed = m.parse_udp_packet(self.short(body=m._test_udp_payload()))
        self.assertEqual(parsed["status"], "lcm_unparsed")
        self.assertIsNone(parsed["decoded"])
        self.assertFalse(parsed["schema_verified"])
        self.assertEqual(parsed["transport"]["channel"], "/drivers/canbus/Chassis")
        self.assertEqual(parsed["transport"]["sequence"], 23)

    def test_lcm_fragments_keep_transport_fields_only(self):
        first = b"LC03" + struct.pack(">IIIHH", 24, 10, 0, 0, 2) + b"topic\0abcd"
        second = b"LC03" + struct.pack(">IIIHH", 24, 10, 4, 1, 2) + b"efghij"
        a, b = m.parse_udp_packet(first), m.parse_udp_packet(second)
        for parsed in (a, b):
            self.assertEqual(parsed["status"], "lcm_fragment")
            self.assertIsNone(parsed["decoded"])
            self.assertFalse(parsed["transport"]["reassembled"])
        self.assertEqual(a["transport"]["channel"], "topic")
        self.assertNotIn("channel", b["transport"])
        self.assertEqual(b["transport"]["fragment_offset"], 4)

    def test_malformed_envelopes_are_explicit_errors(self):
        cases = [b"LC02", b"LC02" + bytes(4) + b"unterminated",
                 b"LC03" + bytes(16),
                 b"LC03" + struct.pack(">IIIHH", 1, 1, 1, 0, 2) + b"x\0a",
                 b"LC03" + struct.pack(">IIIHH", 1, 1, 0, 1, 2) + b"ab",
                 self.short(channel=b"\xff")]
        for raw in cases:
            with self.subTest(raw=raw):
                parsed = m.parse_udp_packet(raw)
                self.assertEqual(parsed["status"], "parse_error")
                self.assertIsNone(parsed["decoded"])

    def test_worker_saves_latest_per_source_and_counts_repeats_and_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            capture = m.CaptureSession()
            directory = capture.start(temporary, config())
            worker = m.UdpWorker(m.UiEventBus(), capture, m.DEFAULT_UDP_GROUP, 7621, "127.0.0.1")
            packets = [(self.short(), ("127.0.0.1", 5001)),
                       (self.short(), ("127.0.0.1", 5001)),
                       (self.short(), ("127.0.0.1", 5002)),
                       (b"LC03", ("127.0.0.1", 5001))]

            class Socket:
                def recvfrom(self, size):
                    value = packets.pop(0)
                    if not packets:
                        worker.stop_event.set()
                    return value

            worker._receive(Socket(), "239.255.76.21:7621")
            capture.stop()
            manifest = m.verify_capture(directory)
            rows = [json.loads(line) for line in
                    (directory / "udp-observations.jsonl").read_text().splitlines()]
            self.assertEqual(manifest["counts"]["udp"], 2)
            self.assertEqual([row["duplicate_count"] for row in rows], [1, 0])
            self.assertEqual([row["coalesced_count"] for row in rows], [1, 0])
            self.assertEqual(worker.sampler.stats["received"], 4)
            self.assertEqual(worker.sampler.stats["invalid"], 1)
            self.assertTrue(all(row["parse"]["decoded"] is None for row in rows))
            self.assertTrue(all((directory / row["payload"]).is_file() for row in rows))
            self.assertEqual(rows[-1]["parse"]["status"], "lcm_unparsed")


if __name__ == "__main__":
    unittest.main()
