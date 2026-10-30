"""Bounded transport-only UDP sampling; no application payload decoding.

LCM framing follows https://lcm-proj.github.io/lcm/content/udp-multicast-protocol.html.
Call ``ingest`` for every datagram and ``drain`` on socket timeouts as well as on
traffic. A drain emits at most one newly completed message per source/channel.
The first ingest anchors the sampling clock; idle intervals never fabricate rows.
``force=True`` flushes the partial interval and discards incomplete assemblies.

Received counts on rows count datagrams contributing *complete* messages in that
window (all fragments are attributed when an assembly completes). Cumulative
``stats['received']`` counts every datagram, including rejected/incomplete ones.
Complete counts include late messages; coalesced counts only accepted arrivals
replacing a pending row. Identical bodies count as duplicates independently of
sequence numbers, including across drains. They still produce a new window row.

Sequence ordering uses the unsigned 32-bit half-range rule, per source/channel.
An equal sequence with a different body is rejected. After fragment_timeout
seconds without a complete message, any sequence can establish a new baseline;
its row explicitly records sequence_reset. reset_pending() also clears baselines.
All retained bodies, channel names and fragments share max_pending_bytes; object
counts are separately bounded by max_streams, max_assemblies and max_fragments.
The caller owns returned rows; references retained by the caller are outside this
sampler's memory budget. The class is intended for a single receiving thread.
"""
from bisect import bisect_left
from collections import Counter, OrderedDict
import json
import math
import struct


class UdpLatestSampler:
    def __init__(self, interval=1.0, max_streams=128,
                 max_message_bytes=4 * 1024 * 1024,
                 max_pending_bytes=16 * 1024 * 1024,
                 fragment_timeout=2.0, max_assemblies=64,
                 max_fragments=4096):
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("interval must be positive and finite")
        if not math.isfinite(fragment_timeout) or fragment_timeout <= 0:
            raise ValueError("fragment_timeout must be positive and finite")
        for name, value in (("max_streams", max_streams),
                            ("max_message_bytes", max_message_bytes),
                            ("max_pending_bytes", max_pending_bytes),
                            ("max_assemblies", max_assemblies),
                            ("max_fragments", max_fragments)):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(name + " must be a positive integer")
        self.interval_ns = max(1, int(interval * 1_000_000_000))
        self.timeout_ns = max(1, int(fragment_timeout * 1_000_000_000))
        self.max_streams = max_streams
        self.max_message_bytes = max_message_bytes
        self.max_pending_bytes = max_pending_bytes
        self.max_assemblies = max_assemblies
        self.max_fragments = max_fragments
        self.stats = Counter({key: 0 for key in (
            "received", "complete", "duplicate", "coalesced", "out_of_order",
            "invalid", "incomplete", "limit_dropped")})
        self._streams = OrderedDict()
        self._assemblies = {}
        self._pending_bytes = 0
        self._next_expiry_ns = None
        self._next_drain_ns = None

    @property
    def pending_bytes(self):
        """Retained body/fragment/channel bytes, including duplicate history."""
        return self._pending_bytes

    @property
    def assembly_count(self):
        """Assembly slots, including short-lived rejection tombstones."""
        return len(self._assemblies)

    @property
    def stream_count(self):
        return len(self._streams)

    def _invalid(self, reason):
        self.stats["invalid"] += 1
        self.stats[reason] += 1

    def _limited(self, reason):
        self.stats["limit_dropped"] += 1
        self.stats[reason] += 1

    def _release_assembly(self, key, incomplete=False, tombstone=False):
        assembly = self._assemblies.get(key)
        if assembly is None:
            return
        self._pending_bytes -= assembly["bytes"]
        if incomplete and not assembly["rejected"]:
            self.stats["incomplete"] += 1
        if tombstone:
            # Prevent surviving pieces of a corrupt message from being assembled
            # anew immediately. Slot and lifetime remain bounded as before.
            assembly.update(rejected=True, bytes=0, fragments={}, offsets=[],
                            channel=None)
        else:
            del self._assemblies[key]

    def _expire(self, now_ns):
        if self._next_expiry_ns is None or now_ns < self._next_expiry_ns:
            return
        for key, assembly in list(self._assemblies.items()):
            if now_ns >= assembly["expires_ns"]:
                if not assembly["rejected"]:
                    self.stats["fragment_timeout"] += 1
                self._release_assembly(key, incomplete=True)
        self._next_expiry_ns = min((item["expires_ns"] for item in
                                    self._assemblies.values()), default=None)

    @staticmethod
    def _channel(payload, start):
        end = payload.find(b"\0", start)
        if end < 0:
            raise ValueError("unterminated_channel")
        try:
            channel = payload[start:end].decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("invalid_channel_utf8") from None
        return channel, end + 1, end - start

    def ingest(self, payload, source, arrived_ns, observed_utc):
        """Inspect transport and retain the newest complete body, without decoding."""
        self.stats["received"] += 1
        if self._next_drain_ns is None:
            self._next_drain_ns = arrived_ns + self.interval_ns
        self._expire(arrived_ns)
        if not isinstance(payload, bytes):
            payload = bytes(payload)
        if len(payload) > 65535:
            self._limited("oversize_datagram")
            return
        source = (str(source[0]), int(source[1]))
        magic = payload[:4]
        if magic == b"LC03":
            self._fragment(payload, source, arrived_ns, observed_utc)
            return
        if magic == b"LC02":
            if len(payload) < 8:
                self._invalid("truncated_header")
                return
            try:
                channel, offset, channel_bytes = self._channel(payload, 8)
            except ValueError as exc:
                self._invalid(str(exc))
                return
            sequence = struct.unpack_from(">I", payload, 4)[0]
            body = payload[offset:]
            transport = {"protocol": "lcm", "magic": "LC02",
                         "sequence": sequence, "channel": channel,
                         "reassembled": False, "fragment_count": 1,
                         "payload_size": len(body)}
            self._complete(body, source, channel, channel_bytes, arrived_ns,
                           observed_utc, "lcm-short-message", transport, 1)
            return
        self._complete(payload, source, None, 0, arrived_ns, observed_utc,
                       "short" if len(payload) < 4 else "direct-or-unknown",
                       {"protocol": "direct-or-unknown", "reassembled": False}, 1)

    def _fragment(self, payload, source, arrived_ns, observed_utc):
        if len(payload) < 20:
            self._invalid("truncated_header")
            return
        sequence, size, offset, number, count = struct.unpack_from(">IIIHH", payload, 4)
        key = source + (sequence,)
        if (not count or number >= count or (number == 0 and offset != 0)
                or (number != 0 and offset == 0)):
            self._invalid("invalid_fragment_header")
            self._release_assembly(key, incomplete=True, tombstone=True)
            return
        if size > self.max_message_bytes or count > self.max_fragments:
            self._limited("fragment_message_limit")
            self._release_assembly(key, incomplete=True, tombstone=True)
            return
        channel, channel_bytes, start = None, 0, 20
        if number == 0:
            try:
                channel, start, channel_bytes = self._channel(payload, start)
            except ValueError as exc:
                self._invalid(str(exc))
                self._release_assembly(key, incomplete=True, tombstone=True)
                return
        body = payload[start:]
        if offset + len(body) > size or (not body and size != 0):
            self._invalid("invalid_fragment_range")
            self._release_assembly(key, incomplete=True, tombstone=True)
            return
        assembly = self._assemblies.get(key)
        if assembly is not None and assembly["rejected"]:
            self.stats["rejected_fragment"] += 1
            return
        if assembly is None:
            if len(self._assemblies) >= self.max_assemblies:
                self._limited("assembly_limit")
                return
            assembly = {"size": size, "count": count, "fragments": {},
                        "offsets": [], "channel": None, "channel_bytes": 0,
                        "bytes": 0, "received": 0, "rejected": False,
                        "first_ns": arrived_ns, "first_utc": observed_utc,
                        "last_ns": arrived_ns, "last_utc": observed_utc,
                        "expires_ns": arrived_ns + self.timeout_ns}
            self._assemblies[key] = assembly
            if (self._next_expiry_ns is None or
                    assembly["expires_ns"] < self._next_expiry_ns):
                self._next_expiry_ns = assembly["expires_ns"]
        assembly["received"] += 1
        if arrived_ns >= assembly["last_ns"]:
            assembly["last_ns"], assembly["last_utc"] = arrived_ns, observed_utc
        if size != assembly["size"] or count != assembly["count"]:
            self._invalid("inconsistent_fragments")
            self._release_assembly(key, incomplete=True, tombstone=True)
            return
        previous = assembly["fragments"].get(number)
        if previous is not None:
            if previous == (offset, body) and (number != 0 or channel == assembly["channel"]):
                self.stats["duplicate_fragments"] += 1
            else:
                self._invalid("conflicting_fragments")
                self._release_assembly(key, incomplete=True, tombstone=True)
            return
        offsets = assembly["offsets"]
        position = bisect_left(offsets, (offset,))
        if ((position and offsets[position - 1][1] > offset) or
                (position < len(offsets) and offset + len(body) > offsets[position][0])):
            self._invalid("overlapping_fragments")
            self._release_assembly(key, incomplete=True, tombstone=True)
            return
        weight = len(body) + channel_bytes
        if self._pending_bytes + weight > self.max_pending_bytes:
            self._limited("pending_bytes_limit")
            self._release_assembly(key, incomplete=True, tombstone=True)
            return
        assembly["fragments"][number] = (offset, body)
        offsets.insert(position, (offset, offset + len(body), number))
        assembly["bytes"] += weight
        self._pending_bytes += weight
        if number == 0:
            assembly["channel"], assembly["channel_bytes"] = channel, channel_bytes
        if len(assembly["fragments"]) != count:
            return
        # Every numbered fragment must exist, occupy a disjoint range and cover
        # the entire advertised payload. Receiving the last fragment is not enough.
        cursor = 0
        for expected_number, (begin, end, number) in enumerate(offsets):
            if begin != cursor or number != expected_number:
                self._invalid("fragment_holes" if begin != cursor else "fragment_order")
                self._release_assembly(key, incomplete=True, tombstone=True)
                return
            cursor = end
        if cursor != size or assembly["channel"] is None:
            self._invalid("fragment_holes")
            self._release_assembly(key, incomplete=True, tombstone=True)
            return
        body = b"".join(assembly["fragments"][number][1] for _, _, number in offsets)
        transport = {"protocol": "lcm", "magic": "LC03", "sequence": sequence,
                     "channel": assembly["channel"], "reassembled": True,
                     "fragment_count": count, "payload_size": size,
                     "first_fragment_monotonic_ns": assembly["first_ns"],
                     "first_fragment_observed_at_utc": assembly["first_utc"]}
        self._release_assembly(key)
        self._complete(body, source, assembly["channel"], assembly["channel_bytes"],
                       assembly["last_ns"], assembly["last_utc"], "lcm-reassembled",
                       transport, assembly["received"])

    def _complete(self, body, source, channel, channel_bytes, arrived_ns,
                  observed_utc, wire_hint, transport, received):
        if len(body) > self.max_message_bytes:
            self._limited("message_bytes_limit")
            return
        self.stats["complete"] += 1
        kind = "lcm" if channel is not None else "direct-or-unknown"
        key = source + (kind, channel)
        stream = self._streams.get(key)
        sequence = transport.get("sequence")
        reset = False
        if stream is not None:
            stream["counts"]["received"] += received
            stream["counts"]["complete"] += 1
            # A continuous replay of late messages is traffic, not publisher
            # silence. Rejected old/conflicting sequences must also refresh
            # this timestamp so they cannot eventually masquerade as a restart.
            idle_gap = arrived_ns - stream["last_received_complete_ns"]
            stream["last_received_complete_ns"] = arrived_ns
            previous_sequence = stream["sequence"]
            if sequence is not None and previous_sequence is not None:
                delta = (sequence - previous_sequence) & 0xFFFFFFFF
                reset = idle_gap >= self.timeout_ns
                if not reset and (delta >= 0x80000000):
                    self.stats["out_of_order"] += 1
                    stream["counts"]["out_of_order"] += 1
                    return
                if not reset and delta == 0 and body != stream["body"]:
                    self._invalid("sequence_conflict")
                    return
                # An ordinary forward/wrapped sequence doesn't need a reset flag.
                reset = reset and (delta >= 0x80000000 or
                                   (delta == 0 and body != stream["body"]))
        weight = len(body) + channel_bytes
        old_weight = stream["bytes"] if stream is not None else 0
        eviction = None
        if stream is None and len(self._streams) >= self.max_streams:
            eviction = next(iter(self._streams.values()))
        released_weight = eviction["bytes"] if eviction is not None else 0
        if self._pending_bytes - old_weight - released_weight + weight > self.max_pending_bytes:
            self._limited("pending_bytes_limit")
            return
        if stream is None:
            if eviction is not None:
                _, removed = self._streams.popitem(last=False)
                self._pending_bytes -= removed["bytes"]
                self.stats["stream_evicted"] += 1
                if removed["pending"]:
                    self.stats["pending_discarded"] += 1
            stream = {"body": None, "sequence": None, "bytes": 0,
                      "pending": False, "counts": Counter(received=received, complete=1)}
            self._streams[key] = stream
        if stream["body"] == body:
            self.stats["duplicate"] += 1
            stream["counts"]["duplicate"] += 1
        if stream["pending"]:
            self.stats["coalesced"] += 1
            stream["counts"]["coalesced"] += 1
        else:
            stream["first_ns"], stream["first_utc"] = arrived_ns, observed_utc
        self._pending_bytes += weight - stream["bytes"]
        stream.update(body=body, bytes=weight, sequence=sequence, pending=True,
                      last_ns=arrived_ns, last_utc=observed_utc,
                      last_received_complete_ns=arrived_ns,
                      source=source, channel=channel, kind=kind,
                      wire_hint=wire_hint, transport=transport,
                      sequence_reset=reset or (stream["pending"] and stream.get("sequence_reset", False)))
        if reset:
            self.stats["sequence_reset"] += 1
        self._streams.move_to_end(key)

    def drain(self, now_ns, force=False):
        """Return pending latest messages if due, expiring fragments even if idle."""
        self._expire(now_ns)
        if not force and (self._next_drain_ns is None or now_ns < self._next_drain_ns):
            return []
        rows = []
        for key, stream in self._streams.items():
            if stream["pending"]:
                counts = {name: stream["counts"][name] for name in
                          ("received", "complete", "duplicate", "coalesced", "out_of_order")}
                row = {"payload": stream["body"], "source_ip": stream["source"][0],
                       "source_port": stream["source"][1], "topic": stream["channel"],
                       "stream_key": json.dumps(key, ensure_ascii=False, separators=(",", ":")),
                       "observed_at_utc": stream["last_utc"],
                       "monotonic_ns": stream["last_ns"],
                       "first_monotonic_ns": stream["first_ns"],
                       "first_observed_at_utc": stream["first_utc"],
                       "wire_hint": stream["wire_hint"],
                       "transport": dict(stream["transport"]),
                       "sequence_reset": stream["sequence_reset"], "interval_counts": counts}
                row.update({name + "_count": count for name, count in counts.items()})
                rows.append(row)
                stream["pending"] = False
            stream["counts"].clear()
        if force:
            for key in list(self._assemblies):
                self._release_assembly(key, incomplete=True)
            self._next_expiry_ns = None
            self._next_drain_ns = None
        elif self._next_drain_ns is not None:
            elapsed = max(0, now_ns - self._next_drain_ns) // self.interval_ns + 1
            self._next_drain_ns += elapsed * self.interval_ns
        return rows

    def reset_pending(self):
        """Discard generation-bound data and sequence state, retaining stats."""
        for key in list(self._assemblies):
            self._release_assembly(key, incomplete=True)
        self.stats["pending_discarded"] += sum(s["pending"] for s in self._streams.values())
        self._streams.clear()
        self._pending_bytes = 0
        self._next_expiry_ns = None
        self._next_drain_ns = None
