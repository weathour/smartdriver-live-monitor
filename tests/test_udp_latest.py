"""Deterministic transport and resource-limit tests; no GUI/network dependency."""
import json
import random
import struct
import unittest

from udp_latest import UdpLatestSampler


SOURCE = ("127.0.0.1", 5001)
SECOND = 1_000_000_000


def short(sequence, body=b"body", channel="topic"):
    return b"LC02" + struct.pack(">I", sequence) + channel.encode() + b"\0" + body


def fragment(sequence, body, offset, number, count, size, channel="topic"):
    header = b"LC03" + struct.pack(">IIIHH", sequence, size, offset, number, count)
    return header + (channel.encode() + b"\0" if number == 0 else b"") + body


class LatestTests(unittest.TestCase):
    def send(self, sampler, payload, at=0, source=SOURCE):
        sampler.ingest(payload, source, at, "utc-%d" % at)

    def test_one_thousand_packets_one_latest_with_real_arrival(self):
        sampler = UdpLatestSampler()
        for sequence in range(1501):
            self.send(sampler, short(sequence, str(sequence).encode()), sequence * 1000)
        self.assertEqual(sampler.drain(SECOND - 1), [])
        rows = sampler.drain(SECOND)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["payload"], b"1500")
        self.assertEqual(row["monotonic_ns"], 1_500_000)
        self.assertEqual(row["observed_at_utc"], "utc-1500000")
        self.assertEqual(row["first_monotonic_ns"], 0)
        self.assertEqual(row["received_count"], 1501)
        self.assertEqual(row["complete_count"], 1501)
        self.assertEqual(row["coalesced_count"], 1500)
        self.assertEqual(row["duplicate_count"], 0)
        self.assertEqual(row["topic"], "topic")
        self.assertEqual(json.loads(row["stream_key"]), [*SOURCE, "lcm", "topic"])
        self.assertEqual(sampler.drain(100 * SECOND), [])

    def test_identical_body_ignores_sequence_and_emits_each_active_window(self):
        sampler = UdpLatestSampler()
        for seq, value in enumerate((b"a", b"a", b"b", b"b")):
            self.send(sampler, short(seq, value), seq)
        row = sampler.drain(SECOND)[0]
        self.assertEqual(row["payload"], b"b")
        self.assertEqual(row["duplicate_count"], 2)
        self.assertEqual(row["coalesced_count"], 3)
        self.send(sampler, short(4, b"b"), SECOND + 1)
        row = sampler.drain(2 * SECOND)[0]
        self.assertEqual(row["duplicate_count"], 1)
        self.assertEqual(row["coalesced_count"], 0)
        self.assertEqual(row["received_count"], 1)
        self.assertEqual(sampler.stats["duplicate"], 3)

    def test_source_port_ip_topic_and_direct_are_independent(self):
        sampler = UdpLatestSampler()
        sources = [SOURCE, (SOURCE[0], 5002), ("127.0.0.2", SOURCE[1])]
        for source in sources:
            self.send(sampler, short(1, source[0].encode()), source=source)
        self.send(sampler, short(1, b"different topic", "另一个"))
        self.send(sampler, b"legacy packet")
        self.send(sampler, b"new legacy packet")
        rows = sampler.drain(SECOND)
        self.assertEqual(len(rows), 5)
        self.assertEqual(len({row["stream_key"] for row in rows}), 5)
        direct = next(row for row in rows if row["topic"] is None)
        self.assertEqual(direct["payload"], b"new legacy packet")
        self.assertEqual(direct["wire_hint"], "direct-or-unknown")
        self.assertEqual(direct["complete_count"], 2)

    def test_force_flushes_partial_window_without_idle_rows(self):
        sampler = UdpLatestSampler()
        self.send(sampler, b"a", 70)
        self.assertEqual(sampler.drain(71), [])
        self.assertEqual(sampler.drain(72, force=True)[0]["monotonic_ns"], 70)
        self.assertEqual(sampler.drain(73, force=True), [])
        self.send(sampler, b"a", 100)
        self.assertEqual(sampler.drain(100 + SECOND)[0]["duplicate_count"], 1)

    def test_drain_clock_skips_missed_ticks_without_fresh_timestamps(self):
        sampler = UdpLatestSampler(interval=0.5)
        self.send(sampler, b"a", 100)
        row = sampler.drain(3 * SECOND + 100)[0]
        self.assertEqual(row["monotonic_ns"], 100)
        self.send(sampler, b"b", 3 * SECOND + 200)
        self.assertEqual(sampler.drain(3 * SECOND + 300), [])
        self.assertEqual(sampler.drain(3 * SECOND + SECOND // 2 + 100)[0]["payload"], b"b")

    def test_sequence_order_wrap_and_equal_sequence_conflict(self):
        sampler = UdpLatestSampler()
        for sequence, body in ((0xFFFFFFFE, b"a"), (0xFFFFFFFF, b"b"),
                               (0, b"c"), (0xFFFFFFFF, b"late"),
                               (0, b"conflict"), (0x80000000, b"ambiguous")):
            self.send(sampler, short(sequence, body), 10)
        row = sampler.drain(SECOND + 10)[0]
        self.assertEqual(row["payload"], b"c")
        self.assertEqual(row["out_of_order_count"], 2)
        self.assertEqual(row["complete_count"], 6)
        self.assertEqual(sampler.stats["invalid"], 1)
        self.assertEqual(sampler.stats["sequence_conflict"], 1)
        self.assertFalse(row["sequence_reset"])

    def test_low_sequence_restart_after_silence_is_not_lost_forever(self):
        sampler = UdpLatestSampler()
        self.send(sampler, short(1000, b"old"), 0)
        self.send(sampler, short(1, b"new"), SECOND)
        self.send(sampler, short(2, b"newer"), 3 * SECOND)
        row = sampler.drain(3 * SECOND)[0]
        self.assertEqual(row["payload"], b"newer")
        self.assertTrue(row["sequence_reset"])
        self.assertEqual(sampler.stats["out_of_order"], 1)
        self.assertEqual(sampler.stats["sequence_reset"], 1)

    def test_continuous_late_messages_never_count_as_silence_then_real_gap_resets(self):
        sampler = UdpLatestSampler()
        self.send(sampler, short(1000, b"latest"), 0)
        for tick in range(1, 11):
            self.send(sampler, short(tick, b"late"), tick * SECOND // 2)
        row = sampler.drain(5 * SECOND)[0]
        self.assertEqual(row["payload"], b"latest")
        self.assertEqual(row["monotonic_ns"], 0)
        self.assertEqual(row["out_of_order_count"], 10)
        self.assertFalse(row["sequence_reset"])
        self.assertEqual(sampler.stats["sequence_reset"], 0)
        self.send(sampler, short(11, b"restart"), 7 * SECOND)
        row = sampler.drain(7 * SECOND)[0]
        self.assertEqual(row["payload"], b"restart")
        self.assertTrue(row["sequence_reset"])
        self.assertEqual(sampler.stats["sequence_reset"], 1)

    def test_same_content_sequence_after_idle_is_repeat_without_reset(self):
        sampler = UdpLatestSampler()
        self.send(sampler, short(1, b"same"), 0)
        sampler.drain(SECOND)
        self.send(sampler, short(1, b"same"), 3 * SECOND)
        row = sampler.drain(3 * SECOND)[0]
        self.assertEqual(row["duplicate_count"], 1)
        self.assertFalse(row["sequence_reset"])

    def test_pending_reset_preserves_statistics_not_generation_data(self):
        sampler = UdpLatestSampler()
        self.send(sampler, short(100, b"before"))
        self.send(sampler, fragment(101, b"abc", 0, 0, 2, 6))
        sampler.reset_pending()
        self.assertEqual(sampler.pending_bytes, 0)
        self.assertEqual(sampler.assembly_count, 0)
        self.assertEqual(sampler.stream_count, 0)
        self.assertEqual(sampler.stats["received"], 2)
        self.assertEqual(sampler.stats["incomplete"], 1)
        self.assertEqual(sampler.stats["pending_discarded"], 1)
        self.assertEqual(sampler.drain(10 * SECOND, force=True), [])
        self.send(sampler, short(1, b"after"), 10 * SECOND)
        self.assertEqual(sampler.drain(11 * SECOND)[0]["payload"], b"after")


class FragmentTests(unittest.TestCase):
    def send(self, sampler, payload, at=0, source=SOURCE):
        sampler.ingest(payload, source, at, "utc-%d" % at)

    def parts(self, sequence=1, channel="topic"):
        return [fragment(sequence, b"abc", 0, 0, 3, 9, channel),
                fragment(sequence, b"def", 3, 1, 3, 9, channel),
                fragment(sequence, b"ghi", 6, 2, 3, 9, channel)]

    def test_out_of_order_duplicate_fragments_reassemble_only_when_complete(self):
        sampler = UdpLatestSampler()
        parts = self.parts()
        self.send(sampler, parts[2], 0)
        self.assertEqual(sampler.drain(SECOND, force=True), [])
        self.assertEqual(sampler.stats["incomplete"], 1)
        # A new assembly: last fragment arriving first is never the full message.
        self.send(sampler, parts[2], 2 * SECOND)
        self.send(sampler, parts[2], 2 * SECOND + 1)
        self.send(sampler, parts[0], 2 * SECOND + 2)
        self.assertEqual(sampler.stats["complete"], 0)
        self.send(sampler, parts[1], 2 * SECOND + 3)
        row = sampler.drain(3 * SECOND)[0]
        self.assertEqual(row["payload"], b"abcdefghi")
        self.assertEqual(row["monotonic_ns"], 2 * SECOND + 3)
        self.assertEqual(row["transport"]["first_fragment_monotonic_ns"], 2 * SECOND)
        self.assertTrue(row["transport"]["reassembled"])
        self.assertEqual(row["wire_hint"], "lcm-reassembled")
        self.assertEqual(row["received_count"], 4)
        self.assertEqual(row["complete_count"], 1)
        self.assertEqual(sampler.stats["duplicate_fragments"], 1)
        self.assertEqual(sampler.assembly_count, 0)
        self.assertEqual(sampler.pending_bytes, len(b"abcdefghi") + len(b"topic"))

    def test_message_identity_includes_source(self):
        sampler = UdpLatestSampler()
        first = self.parts(sequence=1)
        second = self.parts(sequence=1, channel="other")
        for a, b in zip(first, second):
            self.send(sampler, a)
            self.send(sampler, b, source=(SOURCE[0], 5002))
        rows = sampler.drain(SECOND)
        self.assertEqual({row["topic"] for row in rows}, {"topic", "other"})
        self.assertEqual(len(rows), 2)

    def test_random_fragment_permutations_round_trip_with_repeat_content(self):
        rng = random.Random(1024)
        sampler = UdpLatestSampler()
        body = bytes(range(251)) * 5
        for sequence in range(40):
            cuts = [0] + sorted(rng.sample(range(1, len(body)), 20)) + [len(body)]
            packets = [fragment(sequence, body[begin:end], begin, number,
                                len(cuts) - 1, len(body))
                       for number, (begin, end) in enumerate(zip(cuts, cuts[1:]))]
            packets += rng.sample(packets, 3)
            rng.shuffle(packets)
            for packet in packets:
                self.send(sampler, packet, sequence * SECOND)
            row = sampler.drain(sequence * SECOND, force=True)[0]
            self.assertEqual(row["payload"], body)
            self.assertEqual(row["duplicate_count"], int(sequence != 0))
            self.assertEqual(sampler.pending_bytes, len(body) + len(b"topic"))

    def test_short_and_fragmented_transports_share_content_repeat_identity(self):
        sampler = UdpLatestSampler()
        self.send(sampler, short(1, b"abcdefghi"))
        for packet in self.parts(sequence=2):
            self.send(sampler, packet)
        row = sampler.drain(SECOND)[0]
        self.assertEqual(row["complete_count"], 2)
        self.assertEqual(row["duplicate_count"], 1)
        self.assertEqual(row["coalesced_count"], 1)
        self.assertEqual(row["payload"], b"abcdefghi")

    def test_conflict_overlap_holes_and_inconsistent_headers_are_rejected(self):
        cases = [
            (self.parts()[0], fragment(1, b"xxx", 0, 0, 3, 9), "conflicting_fragments"),
            (self.parts()[0], fragment(1, b"def", 2, 1, 3, 9), "overlapping_fragments"),
            (self.parts()[0], fragment(1, b"def", 3, 1, 3, 10), "inconsistent_fragments"),
            (self.parts()[0], fragment(1, b"def", 3, 1, 2, 9), "inconsistent_fragments"),
            (fragment(1, b"abc", 0, 0, 2, 8),
             fragment(1, b"fgh", 5, 1, 2, 8), "fragment_holes"),
            (self.parts()[0], fragment(1, b"abc", 0, 0, 3, 9, "different"), "conflicting_fragments"),
            (fragment(1, b"abc", 0, 0, 3, 9),
             fragment(1, b"defghi", 3, 2, 3, 9), "overlapping_fragments"),
        ]
        for a, b, reason in cases:
            with self.subTest(reason=reason):
                sampler = UdpLatestSampler()
                self.send(sampler, a)
                self.send(sampler, b)
                for packet in self.parts():
                    self.send(sampler, packet)
                self.assertEqual(sampler.drain(SECOND, force=True), [])
                self.assertEqual(sampler.stats[reason], 1)
                self.assertEqual(sampler.stats["invalid"], 1)
                self.assertEqual(sampler.stats["incomplete"], 1)
                self.assertEqual(sampler.pending_bytes, 0)

    def test_fragment_numbers_must_follow_payload_offsets(self):
        sampler = UdpLatestSampler()
        for packet in [fragment(1, b"abc", 0, 0, 3, 9),
                       fragment(1, b"ghi", 6, 1, 3, 9),
                       fragment(1, b"def", 3, 2, 3, 9)]:
            self.send(sampler, packet)
        self.assertEqual(sampler.drain(SECOND, force=True), [])
        self.assertEqual(sampler.stats["fragment_order"], 1)

    def test_idle_timeout_and_force_end_count_incomplete_no_fabricated_row(self):
        sampler = UdpLatestSampler()
        self.send(sampler, self.parts()[0], 100)
        self.assertEqual(sampler.drain(2 * SECOND + 99), [])
        self.assertEqual(sampler.assembly_count, 1)
        self.assertEqual(sampler.drain(2 * SECOND + 100), [])
        self.assertEqual(sampler.stats["fragment_timeout"], 1)
        self.assertEqual(sampler.stats["incomplete"], 1)
        self.assertEqual(sampler.pending_bytes, 0)
        self.send(sampler, self.parts(2)[1], 3 * SECOND)
        self.assertEqual(sampler.drain(3 * SECOND + 1, force=True), [])
        self.assertEqual(sampler.stats["incomplete"], 2)
        self.assertEqual(sampler.assembly_count, 0)

    def test_timeout_is_fixed_from_first_fragment_repeats_cannot_extend(self):
        sampler = UdpLatestSampler()
        self.send(sampler, self.parts()[0], 0)
        self.send(sampler, self.parts()[0], SECOND)
        sampler.drain(2 * SECOND)
        self.assertEqual(sampler.stats["incomplete"], 1)
        self.assertEqual(sampler.pending_bytes, 0)

    def test_bad_envelopes_never_become_unknown_complete_datagrams(self):
        invalid = [b"LC02", b"LC03", b"LC02" + bytes(4) + b"no terminator",
                   b"LC02" + bytes(4) + b"\xff\0body",
                   fragment(1, b"x", 0, 0, 0, 1),
                   fragment(1, b"x", 1, 0, 1, 2),
                   fragment(1, b"x", 0, 1, 2, 2),
                   fragment(1, b"toolong", 0, 0, 1, 1),
                   fragment(1, b"", 0, 0, 2, 1)]
        sampler = UdpLatestSampler()
        for packet in invalid:
            self.send(sampler, packet)
        self.assertEqual(sampler.drain(SECOND, force=True), [])
        self.assertEqual(sampler.stats["invalid"], len(invalid))
        self.assertEqual(sampler.stats["complete"], 0)


class LimitTests(unittest.TestCase):
    def send(self, sampler, payload, at=0, source=SOURCE):
        sampler.ingest(payload, source, at, "utc-%d" % at)
        self.assertLessEqual(sampler.pending_bytes, sampler.max_pending_bytes)
        self.assertLessEqual(sampler.assembly_count, sampler.max_assemblies)
        self.assertLessEqual(sampler.stream_count, sampler.max_streams)

    def test_stream_count_evicts_oldest_and_records_lost_pending_row(self):
        sampler = UdpLatestSampler(max_streams=2)
        for topic in ("a", "b", "c"):
            self.send(sampler, short(1, b"x", topic))
        rows = sampler.drain(SECOND)
        self.assertEqual({row["topic"] for row in rows}, {"b", "c"})
        self.assertEqual(sampler.stats["stream_evicted"], 1)
        self.assertEqual(sampler.stats["pending_discarded"], 1)

    def test_stream_eviction_can_reuse_victims_byte_budget(self):
        sampler = UdpLatestSampler(max_streams=1, max_pending_bytes=8)
        self.send(sampler, short(1, b"1234567", "a"))
        self.send(sampler, short(1, b"1234567", "b"))
        self.assertEqual(sampler.drain(SECOND)[0]["topic"], "b")
        self.assertEqual(sampler.stats["stream_evicted"], 1)
        self.assertEqual(sampler.stats["limit_dropped"], 0)

    def test_retained_latest_payloads_are_byte_bounded_after_drain_too(self):
        sampler = UdpLatestSampler(max_pending_bytes=12)
        self.send(sampler, short(1, b"1234567", "t"))
        self.send(sampler, short(1, b"12345", "u"))
        self.assertEqual(sampler.stats["pending_bytes_limit"], 1)
        self.assertEqual(len(sampler.drain(SECOND)), 1)
        self.assertEqual(sampler.pending_bytes, 8)
        self.send(sampler, short(2, b"1234567890123", "t"), SECOND + 1)
        self.assertEqual(sampler.drain(2 * SECOND), [])
        self.assertEqual(sampler.pending_bytes, 8)
        self.send(sampler, short(3, b"12", "t"), 2 * SECOND + 1)
        self.send(sampler, short(2, b"12345", "u"), 2 * SECOND + 2)
        self.assertEqual(len(sampler.drain(3 * SECOND)), 2)

    def test_fragment_and_latest_payloads_share_one_byte_budget(self):
        sampler = UdpLatestSampler(max_pending_bytes=12)
        self.send(sampler, short(1, b"1234", "t"))
        self.send(sampler, fragment(2, b"abc", 0, 0, 2, 6, "u"))
        self.send(sampler, fragment(2, b"def", 3, 1, 2, 6, "u"))
        self.assertEqual(sampler.pending_bytes, 12)
        self.assertEqual(len(sampler.drain(SECOND)), 2)
        self.send(sampler, fragment(3, b"abc", 0, 0, 2, 6, "u"), SECOND + 1)
        self.assertEqual(sampler.stats["limit_dropped"], 1)
        self.assertEqual(sampler.stats["incomplete"], 1)
        self.assertEqual(sampler.pending_bytes, 12)

    def test_message_size_fragment_count_and_assembly_slots_are_bounded(self):
        sampler = UdpLatestSampler(max_message_bytes=4, max_assemblies=2, max_fragments=3)
        self.send(sampler, short(1, b"12345"))
        self.send(sampler, fragment(2, b"a", 0, 0, 2, 5))
        self.send(sampler, fragment(3, b"a", 0, 0, 4, 4))
        for seq in range(10, 20):
            self.send(sampler, fragment(seq, b"a", 0, 0, 2, 4))
        self.assertEqual(sampler.assembly_count, 2)
        self.assertEqual(sampler.stats["limit_dropped"], 11)
        self.assertEqual(sampler.drain(SECOND, force=True), [])
        self.assertEqual(sampler.stats["incomplete"], 2)
        self.assertEqual(sampler.pending_bytes, 0)

    def test_limits_also_cover_channel_bytes_and_oversize_datagrams(self):
        sampler = UdpLatestSampler(max_pending_bytes=8)
        self.send(sampler, short(1, b"x", "longchannel"))
        self.send(sampler, b"x" * 65536)
        self.assertEqual(sampler.stats["limit_dropped"], 2)
        self.assertEqual(sampler.pending_bytes, 0)
        self.assertEqual(sampler.drain(SECOND, force=True), [])

    def test_invalid_constructor_limits(self):
        for kwargs in ({"interval": 0}, {"interval": float("nan")},
                       {"fragment_timeout": float("inf")}, {"max_streams": 0},
                       {"max_fragments": 0}, {"max_pending_bytes": -1}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                UdpLatestSampler(**kwargs)


if __name__ == "__main__":
    unittest.main()
