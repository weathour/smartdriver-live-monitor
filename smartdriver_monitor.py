#!/usr/bin/env python3
"""SmartDriver Redis/UDP latest-state monitor and 1 Hz recorder (stdlib only)."""

import argparse
import collections
import datetime as dt
import hashlib
import ipaddress
import json
import math
import os
import platform
import queue
import socket
import struct
import sys
import tempfile
import threading
import time
import zipfile
from contextlib import nullcontext
from pathlib import Path

from desktop_log import DesktopCaptureSession, verify_log, pack_log
from udp_latest import UdpLatestSampler

APP_VERSION = "1.5"
REDIS_INTERVAL_SECONDS = 1.0
DEFAULT_REDIS_HOST = "192.168.2.112"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DATABASE = 0
DEFAULT_UDP_GROUP = "239.255.76.21"
DEFAULT_UDP_PORT = 7621
DEFAULT_UDP_INTERFACE = "auto"
MAX_COLLABORATIVE_VEHICLES = 64
MAX_TRAJECTORY_POINTS = 256
UNITY_REDIS_KEYS = (
    "Sensor_Location",
    "Sensor_Mmobstacles",
    "Sensor_TrafficLightlist",
    "Sensor_Lanelist",
    "Sensor_SAM",
    "Sensor_CVP",
)
LEGACY_REDIS_KEYS = ("DCU_L2_St", "Sensor_Trafficlightlist", "Sam", "Day2_SPAT_Msg")
REDIS_SCHEMAS = {
    "Sensor_Location": "Location",
    "Sensor_Mmobstacles": "MMobstacles",
    "Sensor_TrafficLightlist": "TrafficLightlist",
    "Sensor_Lanelist": "LaneList",
    "Sensor_SAM": "Sam",
    "Sensor_CVP": "CollaborativePoseBatch",
}


def _schema(*rows):
    fields = {}
    for row in rows:
        number, name, kind = row[:3]
        target = row[3] if len(row) > 3 else None
        repeated = row[4] if len(row) > 4 else False
        fields[number] = (name, kind, target, repeated)
    return fields


# Generated from the descriptors embedded in Assets/1stScripts/proto_models/*.cs.
SCHEMAS = {
    "Location": _schema(
        (1, "timestamp", "double"), (2, "lon", "double"),
        (3, "lat", "double"), (4, "height", "double"),
        (5, "pitch", "double"), (6, "roll", "double"),
        (7, "heading", "double"), (8, "linear_velocity", "double"),
        (9, "velocity_x", "double"), (10, "velocity_y", "double"),
        (11, "velocity_z", "double"), (12, "linear_acceleration", "double"),
        (13, "acceleration_x", "double"), (14, "acceleration_y", "double"),
        (15, "acceleration_z", "double"), (16, "angular_velocity", "double"),
        (17, "angular_velocity_x", "double"), (18, "angular_velocity_y", "double"),
        (19, "angular_velocity_z", "double"), (20, "origin_lon", "double"),
        (21, "origin_lat", "double"), (22, "utm_position_x", "double"),
        (23, "utm_position_y", "double"), (24, "utm_position_z", "double"),
        (25, "rtkflag", "int32"),
    ),
    "Obstacle": _schema(
        (1, "id", "int32"), (2, "center_pos_vehicle_x", "double"),
        (3, "center_pos_vehicle_y", "double"), (4, "center_pos_vehicle_z", "double"),
        (5, "center_pos_abs_x", "double"), (6, "center_pos_abs_y", "double"),
        (7, "center_pos_abs_z", "double"), (8, "velocity_vehicle", "double"),
        (9, "velocity_abs", "double"), (10, "theta_vehicle", "double"),
        (11, "theta_abs", "double"), (12, "length", "double"),
        (13, "width", "double"), (14, "height", "double"),
        (15, "type", "int32"), (16, "confidence", "double"),
        (17, "lane_position", "int32"), (18, "fusion_type", "int32"),
        (19, "cross_id", "int32"), (20, "src_type", "int32"),
        (21, "lane_id", "int32"), (22, "lane_index", "string"),
        (23, "lon", "double"), (24, "lat", "double"),
    ),
    "MMobstacles": _schema(
        (1, "timestamp", "double"), (2, "obstacle_num", "int32"),
        (3, "obstacles", "message", "Obstacle", True),
    ),
    "Traffic_Light": _schema(
        (1, "color", "int32"), (2, "confidence", "double"),
        (3, "light_pt_x", "int32"), (4, "light_pt_y", "int32"),
        (5, "height", "int32"), (6, "width", "int32"),
        (7, "turn_derection", "int32"), (8, "remaining_time", "double"),
        (9, "lon", "double"), (10, "lat", "double"),
    ),
    "TrafficLightlist": _schema(
        (1, "timestamp", "double"), (2, "traffic_light_num", "int32"),
        (3, "trafficlights", "message", "Traffic_Light", True),
    ),
    "Pt": _schema((1, "x", "double"), (2, "y", "double")),
    "Line": _schema(
        (1, "lane_type", "int32"), (2, "pts_vehicle_num", "int32"),
        (3, "pts_vehicle", "message", "Pt", True),
        (4, "pts_abs_num", "int32"), (5, "pts_abs", "message", "Pt", True),
        (6, "confidence", "double"),
    ),
    "LaneList": _schema(
        (1, "timestamp", "double"), (2, "lane_num", "int32"),
        (3, "laneline", "message", "Line", True),
    ),
    "CollaborativeVehicle": _schema(
        (1, "vid", "int32"), (2, "suggested_driving_behavior", "int32"),
    ),
    "Sam": _schema(
        (1, "timestamp", "double"), (2, "scene_id", "int32"),
        (3, "vid", "int32"), (4, "vehicle_num", "string"),
        (5, "auto_level", "string"), (6, "driving_mode_fd", "int32"),
        (7, "gear_location_fd", "int32"), (8, "steering_value_fd", "double"),
        (9, "accelaration_cmd", "double"), (10, "speed_mps", "double"),
        (11, "collaborative_vehicle_count", "int32"),
        (12, "collaborative_vehicle_list", "message", "CollaborativeVehicle", True),
        (13, "driving_intention", "int32"), (14, "intent_reason", "int32"),
        (15, "guide_decision", "int32"), (16, "feedback_result", "int32"),
        (17, "coordinate_behavior", "int32"), (18, "event_type", "enum"),
        (19, "start_time", "double"), (20, "end_time", "double"),
        (21, "check_count", "int32"),
    ),
    "CollaborativeVehiclePose": _schema(
        (1, "vid", "int32"), (2, "v2v_send_timestamp_ms", "uint64"),
        (3, "longitude_deg", "double"), (4, "latitude_deg", "double"),
        (5, "height_m", "double"), (6, "heading_rad", "double"),
    ),
    "CollaborativePoseBatch": _schema(
        (1, "udp_send_timestamp_ms", "uint64"),
        (2, "vehicles", "message", "CollaborativeVehiclePose", True),
    ),
}


class ParseError(ValueError):
    pass


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def _read_varint(data, offset):
    value = 0
    for shift in range(0, 70, 7):
        if offset >= len(data):
            raise ParseError("varint truncated at byte %d" % offset)
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
    raise ParseError("varint exceeds 10 bytes")


def _field_default(kind, repeated):
    if repeated:
        return []
    if kind == "string":
        return ""
    if kind == "message":
        return None
    if kind == "double":
        return 0.0
    return 0


def _read_wire(data, offset, wire_type):
    if wire_type == 0:
        return _read_varint(data, offset)
    if wire_type == 1:
        end = offset + 8
    elif wire_type == 2:
        length, offset = _read_varint(data, offset)
        end = offset + length
    elif wire_type == 5:
        end = offset + 4
    else:
        raise ParseError("unsupported protobuf wire type %d at byte %d" % (wire_type, offset))
    if end > len(data):
        raise ParseError("protobuf field truncated at byte %d" % offset)
    return data[offset:end], end


def decode_protobuf(data, schema_name, depth=0):
    if depth > 16:
        raise ParseError("protobuf nesting exceeds 16 levels")
    schema = SCHEMAS[schema_name]
    result = {
        name: _field_default(kind, repeated)
        for name, kind, _target, repeated in schema.values()
    }
    unknown = []
    offset = 0
    while offset < len(data):
        tag_offset = offset
        tag, offset = _read_varint(data, offset)
        number, wire_type = tag >> 3, tag & 7
        if number == 0:
            raise ParseError("invalid protobuf field 0 at byte %d" % tag_offset)
        raw, offset = _read_wire(data, offset, wire_type)
        definition = schema.get(number)
        if definition is None:
            unknown.append({"field": number, "wire": wire_type, "value": _wire_json(raw, wire_type)})
            continue
        name, kind, target, repeated = definition
        expected_wire = 1 if kind == "double" else 2 if kind in ("string", "message") else 0
        if wire_type != expected_wire:
            unknown.append({
                "field": number,
                "name": name,
                "wire": wire_type,
                "expected_wire": expected_wire,
                "value": _wire_json(raw, wire_type),
            })
            continue
        if kind == "double":
            value = struct.unpack("<d", raw)[0]
        elif kind == "string":
            try:
                value = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ParseError("%s contains invalid UTF-8: %s" % (name, exc))
        elif kind == "message":
            value = decode_protobuf(raw, target, depth + 1)
        elif kind == "int32":
            value = raw & 0xFFFFFFFF
            if value >= 0x80000000:
                value -= 0x100000000
        else:
            value = raw
        if repeated:
            result[name].append(value)
        else:
            result[name] = value
    if unknown:
        result["_unknown_fields"] = unknown
    return result


def _wire_json(raw, wire_type):
    if wire_type == 0:
        return raw
    return {"length": len(raw), "hex": raw.hex()}


def _contains_unknown(value):
    if isinstance(value, dict):
        return "_unknown_fields" in value or any(_contains_unknown(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_unknown(item) for item in value)
    return False


def decode_redis_payload(key, payload):
    if payload is None:
        return {"status": "missing", "schema": REDIS_SCHEMAS.get(key), "decoded": None}
    schema = REDIS_SCHEMAS.get(key)
    if schema is None:
        return {"status": "unknown_schema", "schema": None, "decoded": None}
    try:
        decoded = decode_protobuf(payload, schema)
        status = "ok_with_unknown_fields" if _contains_unknown(decoded) else "ok"
        return {"status": status, "schema": schema, "decoded": decoded}
    except Exception as exc:
        return {"status": "parse_error", "schema": schema, "decoded": None, "error": str(exc)}


class BinaryReader:
    def __init__(self, data):
        self.data = data
        self.offset = 0
        self.issues = []

    @property
    def remaining(self):
        return len(self.data) - self.offset

    def take(self, length):
        end = self.offset + length
        if length < 0 or end > len(self.data):
            raise ParseError("UDP packet truncated at byte %d (need %d)" % (self.offset, length))
        value = self.data[self.offset:end]
        self.offset = end
        return value

    def int32(self):
        return struct.unpack("<i", self.take(4))[0]

    def double(self, field):
        value = struct.unpack("<d", self.take(8))[0]
        if not math.isfinite(value):
            self.issues.append("%s is non-finite" % field)
            return str(value)
        return value

    def boolean(self, field):
        raw = self.take(1)[0]
        if raw not in (0, 1):
            self.issues.append("%s boolean byte is %d" % (field, raw))
        return raw != 0

    def fixed_string(self, length, field):
        raw = self.take(length).split(b"\0", 1)[0]
        try:
            return raw.decode("utf-8").rstrip()
        except UnicodeDecodeError:
            self.issues.append("%s contains invalid UTF-8" % field)
            return raw.decode("utf-8", errors="replace").rstrip()


def _point_llh(reader, prefix):
    return {
        "lon": reader.double(prefix + ".lon"),
        "lat": reader.double(prefix + ".lat"),
        "height": reader.double(prefix + ".height"),
    }


def _point_enu(reader, prefix):
    return {
        "x": reader.double(prefix + ".x"),
        "y": reader.double(prefix + ".y"),
        "z": reader.double(prefix + ".z"),
    }


def _trajectory_point(reader, index):
    prefix = "trajectory[%d]" % index
    path = {}
    for name in ("x", "y", "z", "theta", "kappa", "s", "dkappa", "ddkappa",
                 "lane_id", "x_derivative", "y_derivative"):
        path[name] = reader.double(prefix + ".path_point." + name)
    return {
        "path_point": path,
        "v": reader.double(prefix + ".v"),
        "a": reader.double(prefix + ".a"),
        "relative_time": reader.double(prefix + ".relative_time"),
        "da": reader.double(prefix + ".da"),
        "is_steer_valid": reader.boolean(prefix + ".is_steer_valid"),
        "front_steer": reader.double(prefix + ".front_steer"),
        "rear_steer": reader.double(prefix + ".rear_steer"),
        "gear": reader.int32(),
    }


def parse_lcm_envelope(data):
    """Inspect LCM transport only; never infer a business schema from its bytes.

    Wire layout: https://lcm-proj.github.io/lcm/content/udp-multicast-protocol.html
    Fragment reassembly and generated-type decoding are deliberately separate.
    """
    try:
        fragmented = data.startswith(b"LC03")
        header_size = 20 if fragmented else 8
        if len(data) < header_size:
            raise ParseError("truncated LCM transport header")
        transport = {"protocol": "lcm", "sequence": struct.unpack_from(">I", data, 4)[0]}
        offset = header_size
        if fragmented:
            size, fragment_offset, number, count = struct.unpack_from(">IIHH", data, 8)
            if not count or number >= count:
                raise ParseError("invalid LCM fragment number/count")
            if number == 0 and fragment_offset != 0:
                raise ParseError("first LCM fragment has nonzero offset")
            transport.update(payload_size=size, fragment_offset=fragment_offset,
                             fragment_number=number, fragment_count=count)
        if not fragmented or number == 0:
            end = data.find(b"\0", offset)
            if end < 0:
                raise ParseError("LCM channel has no NUL terminator")
            transport["channel"] = data[offset:end].decode("utf-8")
            offset = end + 1
        payload_size = len(data) - offset
        transport["datagram_payload_bytes"] = payload_size
        if fragmented:
            if fragment_offset + payload_size > size:
                raise ParseError("LCM fragment exceeds declared payload size")
            transport["reassembled"] = False
        elif payload_size >= 8:
            # Only a candidate: LCM also allows application-defined raw payloads.
            transport["type_fingerprint_candidate_hex"] = data[offset:offset + 8].hex()
        return {
            "status": "lcm_fragment" if fragmented else "lcm_unparsed",
            "decoded": None,
            "transport": transport,
            "schema_verified": False,
            "consumed_bytes": len(data),
            "trailing_bytes": 0,
        }
    except (ValueError, struct.error) as exc:
        return {"status": "parse_error", "protocol": "lcm", "decoded": None,
                "error": str(exc), "consumed_bytes": 0, "trailing_bytes": 0}


def parse_udp_packet(data):
    if data.startswith((b"LC02", b"LC03")):
        return parse_lcm_envelope(data)
    reader = BinaryReader(data)
    try:
        message = {
            "header": {
                "seq": reader.int32(),
                "stamp": {"sec": reader.int32(), "nsec": reader.int32()},
                "frame_id": reader.fixed_string(32, "header.frame_id"),
            },
            "vid": reader.int32(),
            "vehicle_num": reader.fixed_string(32, "vehicle_num"),
            "auto_level": reader.fixed_string(32, "auto_level"),
            "driving_mode_fd": reader.int32(),
            "gear_location_fd": reader.int32(),
            "steering_value_fd": reader.double("steering_value_fd"),
            "accelaration_cmd": reader.double("accelaration_cmd"),
            "speed_mps": reader.double("speed_mps"),
            "position": _point_llh(reader, "position"),
            "utm_position": _point_enu(reader, "utm_position"),
            "pitch": reader.double("pitch"),
            "roll": reader.double("roll"),
            "heading": reader.double("heading"),
        }
        nsec = message["header"]["stamp"]["nsec"]
        if nsec < 0 or nsec >= 1_000_000_000:
            reader.issues.append("header.stamp.nsec is outside [0, 1000000000)")
        collaborative_count = reader.int32()
        if (collaborative_count < 0
                or collaborative_count > MAX_COLLABORATIVE_VEHICLES
                or collaborative_count > max(0, (reader.remaining - 24) // 8)):
            raise ParseError("collaborative vehicle count %d is outside protocol bounds" % collaborative_count)
        message["collaborative_vehicle_list_size"] = collaborative_count
        message["collaborative_vehicle_list"] = [
            {"vid": reader.int32(), "suggested_driving_behavior": reader.int32()}
            for _ in range(collaborative_count)
        ]
        trajectory_count = reader.int32()
        if (trajectory_count < 0
                or trajectory_count > MAX_TRAJECTORY_POINTS
                or trajectory_count > max(0, (reader.remaining - 20) // 141)):
            raise ParseError("trajectory point count %d is outside protocol bounds" % trajectory_count)
        message["intend_trajectory_points_size"] = trajectory_count
        message["intend_trajectory_points"] = [
            _trajectory_point(reader, index) for index in range(trajectory_count)
        ]
        for field in ("driving_intention", "intent_reason", "guide_decision",
                      "feedback_result", "coordinate_behavior"):
            message[field] = reader.int32()
        trailing = reader.remaining
        result = {
            "status": "ok_with_trailing" if trailing else "ok",
            "decoded": message,
            "consumed_bytes": reader.offset,
            "trailing_bytes": trailing,
        }
        if trailing:
            result["trailing_hex"] = reader.take(trailing).hex()
        if reader.issues:
            result["issues"] = reader.issues
            if result["status"] == "ok":
                result["status"] = "ok_with_issues"
        return result
    except Exception as exc:
        return {
            "status": "parse_error",
            "error": str(exc),
            "consumed_bytes": reader.offset,
            "trailing_bytes": reader.remaining,
            "decoded": None,
        }


def udp_wire_hint(data):
    if data.startswith(b"LC02"):
        return "lcm-short-message"
    if data.startswith(b"LC03"):
        return "lcm-fragment"
    if len(data) < 4:
        return "short"
    return "direct-or-unknown"


def _json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def json_text(value, compact=False):
    return json.dumps(
        _json_safe(value), ensure_ascii=False, allow_nan=False,
        separators=(",", ":") if compact else None,
        indent=None if compact else 2,
    )


def validate_capture_config(redis_host, redis_port, redis_database, udp_group, udp_port, udp_interface):
    host = str(redis_host).strip()
    if not host:
        raise ValueError("Redis address is required")
    redis_port = int(redis_port)
    database = int(redis_database)
    udp_port = int(udp_port)
    if not 1 <= redis_port <= 65535 or not 1 <= udp_port <= 65535:
        raise ValueError("ports must be between 1 and 65535")
    if database < 0:
        raise ValueError("Redis database must be zero or greater")
    group_address = ipaddress.ip_address(str(udp_group).strip())
    if group_address.version != 4 or not group_address.is_multicast:
        raise ValueError("UDP address must be an IPv4 multicast address")
    interface = str(udp_interface).strip() or "auto"
    if interface.lower() != "auto":
        for item in interface.replace(";", ",").split(","):
            item = item.strip()
            if not item:
                continue
            address = ipaddress.ip_address(item)
            if address.version != 4:
                raise ValueError("multicast interface must be IPv4: " + item)
    return {
        "redis_host": host,
        "redis_port": redis_port,
        "redis_database": database,
        "redis_interval_ms": int(REDIS_INTERVAL_SECONDS * 1000),
        "udp_multicast": str(group_address),
        "udp_port": udp_port,
        "udp_interface": interface,
    }


def _sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _capture_files(directory):
    return sorted(
        (path for path in directory.rglob("*")
         if path.is_file() and path != directory / "SHA256SUMS"),
        key=lambda path: path.relative_to(directory).as_posix(),
    )


def write_capture_checksums(directory):
    directory = Path(directory)
    lines = [
        "%s  %s" % (_sha256_file(path), path.relative_to(directory).as_posix())
        for path in _capture_files(directory)
    ]
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", prefix=".SHA256SUMS-", suffix=".tmp",
                dir=str(directory), delete=False) as handle:
            temporary = Path(handle.name)
            handle.write("\n".join(lines) + "\n")
        os.replace(str(temporary), str(directory / "SHA256SUMS"))
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def verify_capture(directory):
    directory = Path(directory).expanduser().resolve()
    if directory.is_file():
        return verify_log(directory)
    checksum_path = directory / "SHA256SUMS"
    if not checksum_path.is_file():
        raise ValueError("missing SHA256SUMS: " + str(directory))
    expected = {}
    for line_number, line in enumerate(checksum_path.read_text(encoding="utf-8").splitlines(), 1):
        try:
            digest, relative = line.split("  ", 1)
        except ValueError as exc:
            raise ValueError("invalid SHA256SUMS line %d" % line_number) from exc
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError("invalid SHA-256 on line %d" % line_number)
        target = (directory / relative).resolve()
        try:
            target.relative_to(directory)
        except ValueError as exc:
            raise ValueError("checksum path escapes capture directory: " + relative) from exc
        if relative in expected:
            raise ValueError("duplicate checksum path: " + relative)
        expected[relative] = digest
    actual = {path.relative_to(directory).as_posix(): path for path in _capture_files(directory)}
    if set(expected) != set(actual):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise ValueError("capture file set mismatch; missing=%r extra=%r" % (missing, extra))
    for relative, path in actual.items():
        if _sha256_file(path) != expected[relative]:
            raise ValueError("checksum mismatch: " + relative)

    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    if (manifest.get("format_version") not in (2, 3)
            or manifest.get("status") != "complete" or manifest.get("last_error") is not None):
        raise ValueError("capture manifest is not a complete, error-free format-version 2 or 3 recording")
    observed_counts = {}
    for name, count_key, payload_kind in (
        ("redis-observations.jsonl", "redis", "redis"),
        ("redis-inventory.jsonl", "inventory", None),
        ("udp-observations.jsonl", "udp", "udp"),
        ("events.jsonl", "events", None),
    ):
        rows = 0
        with (directory / name).open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    raise ValueError("blank JSONL row: %s:%d" % (name, line_number))
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError("JSONL row is not an object: %s:%d" % (name, line_number))
                rows += 1
                if payload_kind and (payload_kind == "udp" or row.get("present")) and not row.get("payload"):
                    raise ValueError("missing raw payload reference: %s:%d" % (name, line_number))
                if payload_kind and row.get("payload"):
                    expected_payload = "payloads/%s/%s.bin" % (payload_kind, row.get("sha256"))
                    if row["payload"] != expected_payload:
                        raise ValueError("invalid payload reference: %s:%d" % (name, line_number))
                    payload_path = directory / expected_payload
                    if not payload_path.is_file() or payload_path.stat().st_size != row.get("length"):
                        raise ValueError("payload length mismatch: %s:%d" % (name, line_number))
                    if expected.get(expected_payload) != row.get("sha256"):
                        raise ValueError("payload SHA-256 mismatch: %s:%d" % (name, line_number))
        observed_counts[count_key] = rows
    if observed_counts != {key: manifest.get("counts", {}).get(key, 0) for key in observed_counts}:
        raise ValueError("manifest counts do not match JSONL rows")
    return manifest


def pack_capture(directory):
    directory = Path(directory).expanduser().resolve()
    if directory.is_file():
        return pack_log(directory)
    verify_capture(directory)
    archive = directory.parent / (directory.name + ".zip")
    sidecar = archive.with_name(archive.name + ".sha256")
    if archive.exists() or sidecar.exists():
        raise FileExistsError("archive already exists: " + str(archive))
    temporary = tempfile.NamedTemporaryFile(
        prefix=archive.name + ".", suffix=".tmp", dir=str(archive.parent), delete=False)
    temporary_path = Path(temporary.name)
    temporary.close()
    try:
        with zipfile.ZipFile(str(temporary_path), "w", zipfile.ZIP_DEFLATED, allowZip64=True) as bundle:
            for path in _capture_files(directory) + [directory / "SHA256SUMS"]:
                bundle.write(str(path), (Path(directory.name) / path.relative_to(directory)).as_posix())
        verify_capture(directory)
        os.replace(str(temporary_path), str(archive))
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    sidecar.write_text("%s  %s\n" % (_sha256_file(archive), archive.name), encoding="utf-8")
    return archive


class RedisProtocolError(RuntimeError):
    pass


class RedisCommandError(RedisProtocolError):
    pass


class RedisConnection:
    def __init__(self, host, port, database, timeout=2.0):
        self.host = host
        self.port = port
        self.database = database
        self.timeout = timeout
        self.sock = None
        self.stream = None

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port), self.timeout)
        self.sock.settimeout(self.timeout)
        self.stream = self.sock.makefile("rwb", buffering=0)
        if self.database:
            response = self.command("SELECT", str(self.database))
            if response != "OK":
                raise RedisProtocolError("Redis SELECT failed: %r" % (response,))

    def close(self):
        try:
            if self.stream:
                self.stream.close()
        finally:
            self.stream = None
            if self.sock:
                self.sock.close()
            self.sock = None

    def command(self, *parts):
        if self.stream is None:
            raise RedisProtocolError("Redis is not connected")
        encoded = [part if isinstance(part, bytes) else str(part).encode("utf-8") for part in parts]
        request = [b"*%d\r\n" % len(encoded)]
        for part in encoded:
            request.extend((b"$%d\r\n" % len(part), part, b"\r\n"))
        self.sock.sendall(b"".join(request))
        return self._read_resp()

    def _read_line(self):
        line = self.stream.readline()
        if not line:
            raise RedisProtocolError("Redis connection closed")
        if not line.endswith(b"\r\n"):
            raise RedisProtocolError("invalid RESP line ending")
        return line[:-2]

    def _read_exact(self, length):
        chunks = []
        remaining = length
        while remaining:
            chunk = self.stream.read(remaining)
            if not chunk:
                raise RedisProtocolError("Redis bulk value truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _read_resp(self):
        marker = self._read_exact(1)
        if marker == b"+":
            return self._read_line().decode("utf-8", errors="replace")
        if marker == b"-":
            raise RedisCommandError(self._read_line().decode("utf-8", errors="replace"))
        if marker == b":":
            return int(self._read_line())
        if marker == b"$":
            length = int(self._read_line())
            if length < 0:
                return None
            value = self._read_exact(length)
            if self._read_exact(2) != b"\r\n":
                raise RedisProtocolError("invalid RESP bulk ending")
            return value
        if marker == b"*":
            count = int(self._read_line())
            if count < 0:
                return None
            return [self._read_resp() for _ in range(count)]
        raise RedisProtocolError("unknown RESP marker %r" % marker)

    def scan_keys(self):
        keys = set()
        cursor = "0"
        for _ in range(10000):
            result = self.command("SCAN", cursor, "COUNT", "1000")
            if not isinstance(result, list) or len(result) != 2:
                raise RedisProtocolError("unexpected SCAN response")
            cursor = result[0].decode("utf-8", errors="replace")
            keys.update(item.decode("utf-8", errors="replace") for item in result[1])
            if cursor == "0":
                return sorted(keys)
        raise RedisProtocolError("SCAN exceeded 10000 iterations")


class CaptureSession:
    def __init__(self):
        self.lock = threading.RLock()
        self.active = False
        self.generation = 0
        self.directory = None
        self.handles = {}
        self.counts = collections.Counter()
        self.started_at = None
        self.config = None
        self.last_error = None

    def start(self, output_root, config):
        with self.lock:
            if self.active:
                return self.directory
            root = Path(output_root).expanduser()
            root.mkdir(parents=True, exist_ok=True)
            stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f")[:-3]
            directory = root / ("SmartDriverLiveCapture-" + stamp)
            directory.mkdir()
            self.directory = directory
            self.handles = {}
            self.counts.clear()
            self.started_at = utc_now()
            self.config = dict(config)
            self.last_error = None
            try:
                (directory / "payloads" / "redis").mkdir(parents=True)
                (directory / "payloads" / "udp").mkdir(parents=True)
                for kind, filename in (
                        ("redis", "redis-observations.jsonl"), ("udp", "udp-observations.jsonl"),
                        ("inventory", "redis-inventory.jsonl"), ("events", "events.jsonl")):
                    self.handles[kind] = (directory / filename).open("a", encoding="utf-8", buffering=1)
                self.generation += 1
                self.active = True
                self._write_manifest_locked(None, "recording")
                self._log_locked("INFO", "recording started")
            except Exception as exc:
                self.last_error = "recording start failed: %s" % exc
                self._abort_locked()
                raise RuntimeError(self.last_error) from exc
            return directory

    def stop(self):
        with self.lock:
            if not self.active:
                return self.directory
            ended_at = utc_now()
            try:
                self._log_locked("INFO", "recording stopped")
                self.active = False
                self._close_handles_locked()
                self._write_manifest_locked(ended_at, "complete")
                write_capture_checksums(self.directory)
            except Exception as exc:
                self.last_error = "recording finalization failed: %s" % exc
                self.active = False
                self._close_handles_locked(ignore_errors=True)
                try:
                    self._write_manifest_locked(ended_at, "failed")
                    write_capture_checksums(self.directory)
                except Exception:
                    pass
                raise RuntimeError(self.last_error) from exc
            return self.directory

    def observe(self, kind, payload, metadata, decoder):
        parsed = None
        error = None
        with self.lock:
            try:
                payload_ref = self._save_payload_locked(kind, payload) if self.active and payload is not None else None
                # Raw bytes are durable before any parser is called.
                parsed = decoder()
                if self.active:
                    record = dict(metadata)
                    if payload_ref:
                        record.update(payload_ref)
                    record["parse"] = parsed
                    self._write_json_line_locked(kind, record)
                    self.counts[kind] += 1
            except Exception as exc:
                error = "recording failed: %s" % exc
                self.last_error = error
                self._abort_locked()
        if parsed is None:
            parsed = decoder()
        return parsed, error

    def record_inventory(self, value):
        with self.lock:
            if not self.active:
                return False
            try:
                self._write_json_line_locked("inventory", value)
                self.counts["inventory"] += 1
            except Exception as exc:
                self.last_error = "recording failed: %s" % exc
                self._abort_locked()
                return False
        return True

    def log(self, level, message):
        with self.lock:
            if self.active:
                try:
                    self._log_locked(level, message)
                except Exception as exc:
                    self.last_error = "recording failed: %s" % exc
                    self._abort_locked()

    def _save_payload_locked(self, kind, payload):
        digest = hashlib.sha256(payload).hexdigest()
        relative = Path("payloads") / kind / (digest + ".bin")
        target = self.directory / relative
        if not target.exists():
            try:
                with target.open("xb") as handle:
                    handle.write(payload)
                    handle.flush()
            except FileExistsError:
                pass
        return {"sha256": digest, "length": len(payload), "payload": relative.as_posix()}

    def _write_json_line_locked(self, kind, value):
        handle = self.handles[kind]
        handle.write(json_text(value, compact=True) + "\n")
        handle.flush()

    def _log_locked(self, level, message):
        handle = self.handles.get("events")
        if handle:
            self._write_json_line_locked("events", {
                "kind": "runtime_event",
                "observed_at_utc": utc_now(),
                "level": level,
                "message": message,
            })
            self.counts["events"] += 1

    def _write_manifest_locked(self, ended_at, status):
        manifest = {
            "format_version": 3,
            "recorder": "SmartDriverLiveMonitor",
            "recorder_version": APP_VERSION,
            "status": status,
            "started_at_utc": self.started_at,
            "ended_at_utc": ended_at,
            "capture_layer": "application_socket",
            "config": self.config,
            "counts": dict(self.counts),
            "last_error": self.last_error,
            "environment": {
                "operating_system": platform.system(),
                "operating_system_release": platform.release(),
                "machine": platform.machine(),
                "python_version": platform.python_version(),
                "python_implementation": platform.python_implementation(),
            },
            "files": {
                "redis-observations.jsonl": "one row per Redis poll and key",
                "redis-inventory.jsonl": "Redis key/type snapshots",
                "udp-observations.jsonl": "one row per application-socket datagram",
                "events.jsonl": "connection and recorder events",
                "payloads/redis/<sha256>.bin": "content-addressed Redis bytes",
                "payloads/udp/<sha256>.bin": "content-addressed UDP datagram bytes",
                "SHA256SUMS": "integrity list written when recording stops",
            },
            "semantics": {
                "udp": "Every socket datagram is recorded; parse, duplicate, order and trailing-byte states never suppress capture.",
                "redis": "Every polled observation is recorded; identical payloads share one SHA-256 binary but keep separate timeline rows.",
                "boundary": "This records bytes delivered to the Python UDP socket and Redis client, not kernel packets or PCAP frames.",
            },
        }
        target = self.directory / "manifest.json"
        temporary = self.directory / "manifest.json.tmp"
        temporary.write_text(json_text(manifest) + "\n", encoding="utf-8")
        os.replace(str(temporary), str(target))

    def _close_handles_locked(self, ignore_errors=False):
        first_error = None
        for handle in self.handles.values():
            try:
                handle.flush()
            except Exception as exc:
                first_error = first_error or exc
            finally:
                try:
                    handle.close()
                except Exception as exc:
                    first_error = first_error or exc
        self.handles = {}
        if first_error is not None and not ignore_errors:
            raise first_error

    def _abort_locked(self):
        self.active = False
        self._close_handles_locked(ignore_errors=True)
        try:
            self._write_manifest_locked(utc_now(), "failed")
            write_capture_checksums(self.directory)
        except Exception:
            pass


class UiEventBus:
    def __init__(self, maximum=512):
        self.queue = queue.Queue(maxsize=maximum)
        self.dropped = 0

    def publish(self, event):
        try:
            self.queue.put_nowait(event)
            return
        except queue.Full:
            pass
        # ponytail: UI history is bounded; disk capture remains complete at high packet rates.
        try:
            self.queue.get_nowait()
        except queue.Empty:
            pass
        self.dropped += 1
        try:
            self.queue.put_nowait(event)
        except queue.Full:
            self.dropped += 1


class RedisWorker:
    def __init__(self, bus, capture, host, port, database, interval=REDIS_INTERVAL_SECONDS, scan_interval=5.0):
        self.bus = bus
        self.capture = capture
        self.host = host
        self.port = port
        self.database = database
        self.interval = interval
        self.scan_interval = scan_interval
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, name="SmartDriver Redis", daemon=True)
        self.sequence = 0
        self.previous_hash = {}
        self.last_inventory_signature = None
        self.last_inventory_generation = 0
        self.connection = None

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        connection = self.connection
        sock = connection.sock if connection else None
        if sock:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(timeout=3.0)
        return not self.thread.is_alive()

    def _publish_log(self, level, message):
        self.capture.log(level, message)
        self.bus.publish({"type": "log", "level": level, "message": message, "utc": utc_now()})

    def _record_inventory(self, event, signature):
        if (signature == self.last_inventory_signature
                and self.capture.generation == self.last_inventory_generation):
            return False
        recorded = self.capture.record_inventory(event)
        self.last_inventory_signature = signature
        if recorded:
            self.last_inventory_generation = self.capture.generation
        return recorded

    def run(self):
        endpoint = "%s:%d/db%d" % (self.host, self.port, self.database)
        while not self.stop_event.is_set():
            connection = RedisConnection(self.host, self.port, self.database)
            self.connection = connection
            try:
                self.bus.publish({"type": "redis_status", "status": "connecting", "detail": endpoint})
                connection.connect()
                self.bus.publish({"type": "redis_status", "status": "connected", "detail": endpoint})
                self._publish_log("INFO", "Redis connected: " + endpoint)
                self._poll(connection, endpoint)
            except Exception as exc:
                if not self.stop_event.is_set():
                    detail = "%s: %s" % (endpoint, exc)
                    self.bus.publish({"type": "redis_status", "status": "error", "detail": detail})
                    self._publish_log("WARN", "Redis disconnected: " + detail)
                    self.stop_event.wait(2.0)
            finally:
                connection.close()
                self.connection = None
        self.bus.publish({"type": "redis_status", "status": "stopped", "detail": endpoint})

    def _observe_value(self, key, payload, endpoint, key_types, read_mode, request_started,
                       observed_utc=None, observed_ns=None, round_trip_ms=None):
        observed_utc = observed_utc or utc_now()
        observed_ns = observed_ns if observed_ns is not None else time.monotonic_ns()
        if round_trip_ms is None:
            round_trip_ms = (time.perf_counter() - request_started) * 1000.0
        digest = hashlib.sha256(payload).hexdigest() if payload is not None else None
        changed = key in self.previous_hash and self.previous_hash[key] != digest
        self.previous_hash[key] = digest
        metadata = {
            "kind": "redis_observation", "sequence": self.sequence,
            "observed_at_utc": observed_utc, "monotonic_ns": observed_ns,
            "round_trip_ms": round(round_trip_ms, 3), "read_method": read_mode,
            "endpoint": endpoint, "key": key,
            "redis_type": key_types.get(key, "unknown"),
            "present": payload is not None, "changed": changed,
            "length": len(payload) if payload is not None else 0, "sha256": digest,
        }
        parsed, capture_error = self.capture.observe(
            "redis", payload, metadata,
            lambda: decode_redis_payload(key, payload),
        )
        event = dict(metadata)
        event.update(type="redis_observation", parse=parsed,
                     raw_hex=payload[:2048].hex() if payload is not None else "",
                     raw_preview_truncated=payload is not None and len(payload) > 2048)
        self.bus.publish(event)
        if capture_error:
            self._publish_log("ERROR", capture_error)

    def _poll(self, connection, endpoint):
        capture_keys = list(UNITY_REDIS_KEYS)
        key_types = {}
        next_scan = 0.0
        inventory_event = None
        inventory_signature = None
        discovery_enabled = True
        read_mode = "mget"
        reported_get_errors = {}
        while not self.stop_event.is_set():
            started = time.perf_counter()
            now = time.monotonic()
            if discovery_enabled and now >= next_scan:
                try:
                    seen = connection.scan_keys()
                    candidates = sorted(set(UNITY_REDIS_KEYS + LEGACY_REDIS_KEYS + tuple(seen)))
                    new_inventory = []
                    new_capture_keys = []
                    new_key_types = {}
                    for key in candidates:
                        if self.stop_event.is_set():
                            return
                        key_type = connection.command("TYPE", key)
                        new_inventory.append({
                            "key": key,
                            "type": key_type,
                            "seen_by_scan": key in seen,
                            "unity_key": key in UNITY_REDIS_KEYS,
                            "legacy_candidate": key in LEGACY_REDIS_KEYS,
                        })
                        new_key_types[key] = key_type
                        if key_type in ("string", "none"):
                            new_capture_keys.append(key)
                except RedisCommandError as exc:
                    discovery_enabled = False
                    discovery_method = "configured"
                    capture_keys = list(UNITY_REDIS_KEYS)
                    key_types = {}
                    new_inventory = [{
                        "key": key,
                        "type": "unknown",
                        "seen_by_scan": False,
                        "unity_key": True,
                        "legacy_candidate": key in LEGACY_REDIS_KEYS,
                    } for key in capture_keys]
                    self._publish_log(
                        "WARN", "Redis SCAN/TYPE unavailable; using configured Unity keys: %s" % exc)
                else:
                    discovery_method = "scan"
                    capture_keys = new_capture_keys
                    key_types = new_key_types
                    next_scan = now + self.scan_interval
                signature = tuple((item["key"], item["type"]) for item in new_inventory)
                inventory_event = {
                    "type": "redis_inventory",
                    "observed_at_utc": utc_now(),
                    "endpoint": endpoint,
                    "discovery_method": discovery_method,
                    "keys": new_inventory,
                }
                inventory_signature = signature
                self.bus.publish(inventory_event)
            if inventory_event is not None:
                self._record_inventory(inventory_event, inventory_signature)

            self.sequence += 1
            request_started = time.perf_counter()
            if read_mode == "mget" and capture_keys:
                try:
                    values = connection.command("MGET", *capture_keys)
                except RedisCommandError as exc:
                    detail = str(exc).lower()
                    if not any(marker in detail for marker in ("noperm", "unknown command")):
                        raise
                    read_mode = "get"
                    self._publish_log(
                        "WARN", "Redis MGET unavailable; using individual GET commands: %s" % exc)
                else:
                    if not isinstance(values, list) or len(values) != len(capture_keys):
                        raise RedisProtocolError("MGET value count mismatch")
                    observed_utc = utc_now()
                    observed_ns = time.monotonic_ns()
                    round_trip_ms = (time.perf_counter() - request_started) * 1000.0
                    for key, payload in zip(capture_keys, values):
                        self._observe_value(key, payload, endpoint, key_types, read_mode,
                                            request_started, observed_utc, observed_ns, round_trip_ms)
            if read_mode == "get":
                for key in capture_keys:
                    if self.stop_event.is_set():
                        return
                    request_started = time.perf_counter()
                    try:
                        payload = connection.command("GET", key)
                    except RedisCommandError as exc:
                        detail = str(exc)
                        error_state = (detail, self.capture.generation)
                        if reported_get_errors.get(key) != error_state:
                            self._publish_log("WARN", "Redis GET failed for %s: %s" % (key, detail))
                            reported_get_errors[key] = error_state
                        continue
                    reported_get_errors.pop(key, None)
                    # A later GET can disconnect; preserve each completed read now.
                    self._observe_value(key, payload, endpoint, key_types, read_mode, request_started)
            elapsed = time.perf_counter() - started
            self.stop_event.wait(max(0.0, self.interval - elapsed))


def discover_ipv4(route_host=None):
    found = set()
    for name in {socket.gethostname(), socket.getfqdn()}:
        try:
            for item in socket.getaddrinfo(name, None, socket.AF_INET, socket.SOCK_DGRAM):
                found.add(item[4][0])
        except OSError:
            pass
    if route_host:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect((route_host, 9))
            found.add(probe.getsockname()[0])
        except OSError:
            pass
        finally:
            probe.close()
    usable = []
    for value in found:
        try:
            address = ipaddress.ip_address(value)
            if address.version == 4 and not address.is_loopback and not address.is_link_local:
                usable.append(value)
        except ValueError:
            pass
    return sorted(set(usable))


class UdpWorker:
    def __init__(self, bus, capture, group, port, interface_spec, route_host=None):
        self.bus = bus
        self.capture = capture
        self.group = group
        self.port = port
        self.interface_spec = interface_spec.strip()
        self.route_host = route_host
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, name="SmartDriver UDP", daemon=True)
        self.sock = None
        self.sequence = 0
        self.previous_arrival_ns = None
        self.recent_keys = set()
        self.recent_order = collections.deque()
        self.last_timestamp_by_vehicle = {}
        self.sampler = UdpLatestSampler()
        self.sample_lock = threading.RLock()
        self.capture_generation = capture.generation
        self.last_diagnostic_ns = 0
        self.last_diagnostic_stats = {}
        self.next_flush_ns = 0

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        sock = self.sock
        self.sock = None
        if sock:
            try:
                sock.close()
            except OSError:
                pass
        self.thread.join(timeout=2.0)
        return not self.thread.is_alive()

    def _publish_log(self, level, message):
        self.capture.log(level, message)
        self.bus.publish({"type": "log", "level": level, "message": message, "utc": utc_now()})

    def _interfaces(self):
        if not self.interface_spec or self.interface_spec.lower() == "auto":
            return ["0.0.0.0"] + discover_ipv4(self.route_host)
        values = []
        for item in self.interface_spec.replace(";", ",").split(","):
            item = item.strip()
            if item:
                address = ipaddress.ip_address(item)
                if address.version != 4:
                    raise ValueError("multicast interface must be IPv4: " + item)
                values.append(item)
        return list(dict.fromkeys(values)) or ["0.0.0.0"]

    def _open_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        sock.bind(("", self.port))
        joined, errors = [], []
        group_bytes = socket.inet_aton(str(ipaddress.ip_address(self.group)))
        for interface in self._interfaces():
            try:
                membership = group_bytes + socket.inet_aton(interface)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
                joined.append(interface)
            except OSError as exc:
                errors.append("%s: %s" % (interface, exc))
        sock.settimeout(0.5)
        return sock, joined, errors

    def run(self):
        endpoint = "%s:%d" % (self.group, self.port)
        while not self.stop_event.is_set():
            try:
                self.bus.publish({"type": "udp_status", "status": "connecting", "detail": endpoint})
                sock, joined, errors = self._open_socket()
                self.sock = sock
                detail = "%s; joined=%s" % (endpoint, ",".join(joined) if joined else "port-only")
                if errors:
                    detail += "; join-errors=" + " | ".join(errors)
                self.bus.publish({
                    "type": "udp_status", "status": "listening", "detail": detail,
                    "joined": joined, "errors": errors,
                })
                self._publish_log("INFO", "UDP listening: " + detail)
                self._receive(sock, endpoint)
            except Exception as exc:
                if not self.stop_event.is_set():
                    detail = "%s: %s" % (endpoint, exc)
                    self.bus.publish({"type": "udp_status", "status": "error", "detail": detail})
                    self._publish_log("WARN", "UDP receiver stopped: " + detail)
                    self.stop_event.wait(1.0)
            finally:
                sock = self.sock
                self.sock = None
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
        self.bus.publish({"type": "udp_status", "status": "stopped", "detail": endpoint})

    def _sync_capture_generation(self):
        if self.capture_generation != self.capture.generation:
            self.sampler.reset_pending()
            self.capture_generation = self.capture.generation
            self.last_diagnostic_stats = dict(self.sampler.stats)
            self.next_flush_ns = 0

    def flush_pending(self, force=True):
        with self.sample_lock:
            self._sync_capture_generation()
            now_ns = time.monotonic_ns()
            for snapshot in self.sampler.drain(now_ns, force=force):
                payload = snapshot.pop("payload")
                self.sequence += 1
                metadata = dict(snapshot)
                metadata.update(
                    kind="udp_snapshot", sequence=self.sequence,
                    listen_endpoint="%s:%d" % (self.group, self.port),
                    length=len(payload), sha256=hashlib.sha256(payload).hexdigest(),
                    inter_arrival_ms=None, prefix_hex=payload[:24].hex(),
                    sampling_interval_ms=1000)

                def decode_selected():
                    if snapshot.get("transport", {}).get("protocol") == "lcm" or snapshot.get("wire_hint", "").startswith("lcm"):
                        transport = dict(snapshot.get("transport", {}))
                        result = {
                            "status": "lcm_reassembled" if transport.get("reassembled") else "lcm_unparsed",
                            "decoded": None, "schema_verified": False, "transport": transport,
                            "consumed_bytes": len(payload), "trailing_bytes": 0,
                        }
                    else:
                        result = parse_udp_packet(payload)
                    result["analysis"] = {
                        "duplicate_count": snapshot.get("duplicate_count", 0),
                        "coalesced_count": snapshot.get("coalesced_count", 0),
                        "out_of_order_count": snapshot.get("out_of_order_count", 0),
                        "sampling": "latest_complete_per_source_topic_1hz",
                    }
                    return result

                parsed, capture_error = self.capture.observe("udp", payload, metadata, decode_selected)
                event = dict(metadata)
                event.update(type="udp_datagram", parse=parsed, raw_hex=payload[:2048].hex(),
                             raw_preview_truncated=len(payload) > 2048)
                self.bus.publish(event)
                if capture_error:
                    self._publish_log("ERROR", capture_error)
            if force or now_ns - self.last_diagnostic_ns >= 1_000_000_000:
                stats = dict(self.sampler.stats)
                self.bus.publish({"type": "udp_metrics", "stats": stats})
                delta = {key: value - self.last_diagnostic_stats.get(key, 0)
                         for key, value in stats.items() if value != self.last_diagnostic_stats.get(key, 0)}
                if delta:
                    level = "WARN" if any(delta.get(key, 0) for key in ("invalid", "incomplete", "limit_dropped")) else "INFO"
                    self.capture.log(level, "UDP sampling counters: " + json_text({
                        "delta": delta, "total": stats, "total_scope": "receiver_lifetime"}, compact=True))
                self.last_diagnostic_stats = stats
                self.last_diagnostic_ns = now_ns
            if force:
                self.next_flush_ns = 0

    def _receive(self, sock, endpoint):
        try:
            while not self.stop_event.is_set():
                try:
                    payload, source = sock.recvfrom(65535)
                except socket.timeout:
                    self.flush_pending(force=False)
                    continue
                except OSError:
                    if self.stop_event.is_set():
                        return
                    raise
                with self.sample_lock:
                    self._sync_capture_generation()
                    arrived_ns = time.monotonic_ns()
                    self.sampler.ingest(payload, source, arrived_ns, utc_now())
                    if arrived_ns >= self.next_flush_ns:
                        self.flush_pending(force=False)
                        self.next_flush_ns = arrived_ns + 1_000_000_000
        finally:
            self.flush_pending(force=True)


def _short_time(iso_value):
    if iso_value:
        try:
            return dt.datetime.fromisoformat(iso_value.replace("Z", "+00:00")).astimezone().strftime("%H:%M:%S.%f")[:-3]
        except (ValueError, TypeError):
            pass
    return iso_value or ""


def _format_age(seconds):
    if seconds is None:
        return "—"
    if seconds < 1:
        return "%.1f 秒" % seconds
    if seconds < 60:
        return "%d 秒" % seconds
    return "%d分%02d秒" % (seconds // 60, seconds % 60)


def _format_hex(hex_text, bytes_per_line=32):
    width = bytes_per_line * 2
    return "\n".join(hex_text[index:index + width] for index in range(0, len(hex_text), width))


class SmartDriverApp:
    def __init__(self, root, initial_config=None, output_root=None):
        import tkinter as tk
        from tkinter import filedialog, messagebox, ttk

        self.tk = tk
        self.ttk = ttk
        self.filedialog = filedialog
        self.messagebox = messagebox
        self.root = root
        self.root.title("SmartDriver 外部数据实时监看与采集")
        self.root.geometry("1420x900")
        self.root.minsize(1080, 680)
        self.root.configure(background="#ffffff")
        self.root.protocol("WM_DELETE_WINDOW", self.close)

        initial_config = initial_config or {
            "redis_host": DEFAULT_REDIS_HOST,
            "redis_port": DEFAULT_REDIS_PORT,
            "redis_database": DEFAULT_REDIS_DATABASE,
            "udp_multicast": DEFAULT_UDP_GROUP,
            "udp_port": DEFAULT_UDP_PORT,
            "udp_interface": DEFAULT_UDP_INTERFACE,
        }

        self.bus = UiEventBus()
        self.capture = DesktopCaptureSession()
        self.redis_worker = None
        self.udp_worker = None
        self._applied_config = None
        self.redis_states = {}
        self.udp_events = collections.OrderedDict()
        self.udp_metrics = {}
        self.counters = collections.Counter()
        self.inventory_count = 0
        self.inventory_method = "等待"
        self.redis_read_method = "等待"
        self._last_age_refresh = 0.0
        self._record_was_active = False
        self._last_capture_directory = None

        self.redis_host = tk.StringVar(value=str(initial_config["redis_host"]))
        self.redis_port = tk.StringVar(value=str(initial_config["redis_port"]))
        self.redis_db = tk.StringVar(value=str(initial_config["redis_database"]))
        self.udp_group = tk.StringVar(value=str(initial_config["udp_multicast"]))
        self.udp_port = tk.StringVar(value=str(initial_config["udp_port"]))
        self.udp_interface = tk.StringVar(value=str(initial_config["udp_interface"]))
        documents = Path.home() / "Documents"
        self.output_root = tk.StringVar(value=str(output_root or documents / "SmartDriverCaptures"))
        self.redis_status = tk.StringVar(value="未连接")
        self.udp_status = tk.StringVar(value="未监听")
        self.record_status = tk.StringVar(value="未录制")
        self.metric_text = tk.StringVar(value="等待数据…")
        self.traffic_status = tk.StringVar(value="尚未收到 Sensor_TrafficLightlist")

        self._configure_style()
        self._build_ui()
        self.root.after(50, self.restart_workers)
        self.root.after(100, self._process_events)

    def _configure_style(self):
        from tkinter import font as tkfont

        style = self.ttk.Style(self.root)
        try:
            style.theme_use("clam")
        except self.tk.TclError:
            pass
        ui_family = tkfont.nametofont("TkDefaultFont").actual("family")
        self.fixed_font_family = tkfont.nametofont("TkFixedFont").actual("family")
        style.configure("TFrame", background="#ffffff")
        style.configure("Surface.TFrame", background="#f4f5f8")
        style.configure("TLabel", background="#ffffff", foreground="#172033", font=(ui_family, 10))
        style.configure("Muted.TLabel", foreground="#59657a")
        style.configure("Title.TLabel", font=(ui_family, 16, "bold"), foreground="#172033")
        style.configure("Status.TLabel", font=(ui_family, 10, "bold"))
        style.configure("TButton", font=(ui_family, 10), padding=(10, 6))
        style.configure("Accent.TButton", background="#3449a5", foreground="#ffffff")
        style.map("Accent.TButton", background=[("active", "#293b8b"), ("pressed", "#203174")])
        style.configure("Stop.TButton", background="#b42335", foreground="#ffffff")
        style.map("Stop.TButton", background=[("active", "#92202f"), ("pressed", "#781a27")])
        style.configure("Treeview", font=(ui_family, 9), rowheight=25, background="#ffffff",
                        fieldbackground="#ffffff", foreground="#172033")
        style.configure("Treeview.Heading", font=(ui_family, 9, "bold"), padding=(5, 5))
        style.map("Treeview", background=[("selected", "#dce3ff")], foreground=[("selected", "#172033")])
        style.configure("TNotebook", background="#ffffff", borderwidth=0)
        style.configure("TNotebook.Tab", font=(ui_family, 10), padding=(16, 8))
        style.map("TNotebook.Tab", background=[("selected", "#ffffff")], foreground=[("selected", "#293b8b")])

    def _build_ui(self):
        ttk = self.ttk
        outer = ttk.Frame(self.root, padding=(14, 12))
        outer.pack(fill="both", expand=True)

        header = ttk.Frame(outer)
        header.pack(fill="x")
        ttk.Label(header, text="现场监看与录制", style="Title.TLabel").pack(side="left")
        ttk.Label(
            header,
            text="每秒更新最新状态 · 一次录制一个文件",
            style="Muted.TLabel",
        ).pack(side="left", padx=(16, 0), pady=(5, 0))
        ttk.Button(header, text="打开录制文件", command=self.open_recording).pack(side="right")

        settings = ttk.Frame(outer, style="Surface.TFrame", padding=(10, 8))
        settings.pack(fill="x", pady=(10, 8))
        self._entry(settings, "Redis", self.redis_host, 0, 0, 16)
        self._entry(settings, "端口", self.redis_port, 0, 2, 7)
        self._entry(settings, "DB", self.redis_db, 0, 4, 4)
        self._entry(settings, "UDP 组播", self.udp_group, 0, 6, 16)
        self._entry(settings, "端口", self.udp_port, 0, 8, 7)
        self._entry(settings, "网卡 IP", self.udp_interface, 0, 10, 20)
        ttk.Button(settings, text="应用并重连", command=self.restart_workers).grid(
            row=0, column=12, padx=(10, 0), sticky="ew")

        ttk.Label(settings, text="录制目录", background="#f4f5f8").grid(
            row=1, column=0, padx=(0, 5), pady=(8, 0), sticky="w")
        ttk.Entry(settings, textvariable=self.output_root).grid(
            row=1, column=1, columnspan=8, padx=(0, 6), pady=(8, 0), sticky="ew")
        ttk.Button(settings, text="选择目录", command=self._choose_output).grid(
            row=1, column=9, padx=(0, 6), pady=(8, 0))
        self.record_button = ttk.Button(
            settings, text="开始录制", style="Accent.TButton", command=self.toggle_recording)
        self.record_button.grid(row=1, column=10, columnspan=2, padx=(0, 6), pady=(8, 0), sticky="ew")
        ttk.Button(settings, text="查看最近录制", command=self.open_capture_directory).grid(
            row=1, column=12, pady=(8, 0), sticky="ew")
        settings.columnconfigure(1, weight=1)
        settings.columnconfigure(7, weight=1)

        status = ttk.Frame(outer)
        status.pack(fill="x", pady=(0, 8))
        ttk.Label(status, text="Redis:", style="Status.TLabel").pack(side="left")
        ttk.Label(status, textvariable=self.redis_status).pack(side="left", padx=(4, 18))
        ttk.Label(status, text="UDP:", style="Status.TLabel").pack(side="left")
        ttk.Label(status, textvariable=self.udp_status).pack(side="left", padx=(4, 18))
        ttk.Label(status, text="录制:", style="Status.TLabel").pack(side="left")
        ttk.Label(status, textvariable=self.record_status).pack(side="left", padx=(4, 18))
        ttk.Label(status, textvariable=self.metric_text, style="Muted.TLabel").pack(side="right")

        notebook = ttk.Notebook(outer)
        notebook.pack(fill="both", expand=True)
        self._build_overview_tab(notebook)
        self._build_redis_tab(notebook)
        self._build_traffic_tab(notebook)
        self._build_udp_tab(notebook)
        self._build_log_tab(notebook)

    def _entry(self, parent, label, variable, row, column, width):
        self.ttk.Label(parent, text=label, background="#f4f5f8").grid(
            row=row, column=column, padx=(0, 5), sticky="w")
        self.ttk.Entry(parent, textvariable=variable, width=width).grid(
            row=row, column=column + 1, padx=(0, 9), sticky="ew")

    def _build_overview_tab(self, notebook):
        ttk = self.ttk
        frame = ttk.Frame(notebook, padding=14)
        notebook.add(frame, text="总览")
        ttk.Label(frame, text="当前接收状态", style="Title.TLabel").pack(anchor="w")
        ttk.Label(
            frame,
            text="“收到”只表示本工具完成一次观察；“未变化”表示 Redis 返回了相同字节，不等于传感器产生了新帧。",
            style="Muted.TLabel",
        ).pack(anchor="w", pady=(4, 12))
        columns = ("source", "state", "count", "detail")
        self.overview_tree = ttk.Treeview(frame, columns=columns, show="headings", height=5)
        for key, title, width, stretch in (
            ("source", "来源", 150, False), ("state", "状态", 130, False),
            ("count", "观察数", 100, False), ("detail", "明细", 760, True),
        ):
            self.overview_tree.heading(key, text=title)
            self.overview_tree.column(key, width=width, stretch=stretch, anchor="w")
        self.overview_tree.pack(fill="x")
        self.overview_tree.insert("", "end", iid="redis", values=("Redis", "未连接", 0, ""))
        self.overview_tree.insert("", "end", iid="udp", values=("UDP", "未监听", 0, ""))
        self.overview_tree.insert("", "end", iid="record", values=("磁盘录制", "未录制", 0, ""))

        ttk.Label(frame, text="判读提示", style="Title.TLabel").pack(anchor="w", pady=(22, 8))
        tips = (
            "1. Redis 的“最后观察”和“内容未变化”分开计时；后者持续增加时，Redis 可能只是保留旧值。\n"
            "2. UDP 每秒每路保存最近的完整消息，中间消息会合并；没有新消息时不会新增记录。\n"
            "3. 红绿灯页逐条展示当前消息内的全部 trafficlights，不做距离、车道、方向或置信度筛选。\n"
            "4. 一次录制保存为一个 .sdlog 文件。停止后点击“查看最近录制”，按页查看数据和异常。"
        )
        ttk.Label(frame, text=tips, justify="left", wraplength=1050).pack(anchor="w")

    def _build_redis_tab(self, notebook):
        ttk = self.ttk
        frame = ttk.Frame(notebook, padding=8)
        notebook.add(frame, text="Redis 最新值")
        pane = ttk.Panedwindow(frame, orient="vertical")
        pane.pack(fill="both", expand=True)
        table_frame = ttk.Frame(pane)
        detail_frame = ttk.Frame(pane)
        pane.add(table_frame, weight=3)
        pane.add(detail_frame, weight=2)
        columns = ("key", "present", "bytes", "observed", "unchanged", "parse", "sha")
        self.redis_tree = ttk.Treeview(table_frame, columns=columns, show="headings", selectmode="browse")
        definitions = (
            ("key", "键", 220, True), ("present", "存在", 65, False),
            ("bytes", "字节", 80, False), ("observed", "最后观察", 100, False),
            ("unchanged", "内容未变化", 115, False), ("parse", "解析", 160, False),
            ("sha", "SHA-256", 250, True),
        )
        for key, title, width, stretch in definitions:
            self.redis_tree.heading(key, text=title)
            self.redis_tree.column(key, width=width, stretch=stretch, anchor="w")
        scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.redis_tree.yview)
        scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.redis_tree.xview)
        self.redis_tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.redis_tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)
        self._configure_row_tags(self.redis_tree)
        self.redis_tree.bind("<<TreeviewSelect>>", self._show_selected_redis)
        ttk.Label(detail_frame, text="选中键查看明细，原始内容预览前 2048 字节", style="Muted.TLabel").pack(anchor="w")
        self.redis_detail = self._text_with_scroll(detail_frame)

    def _build_traffic_tab(self, notebook):
        ttk = self.ttk
        frame = ttk.Frame(notebook, padding=8)
        notebook.add(frame, text="交通灯")
        ttk.Label(frame, textvariable=self.traffic_status).pack(anchor="w", pady=(0, 8))
        columns = ("index", "color", "confidence", "bbox", "direction", "remaining", "lon", "lat")
        self.traffic_tree = ttk.Treeview(frame, columns=columns, show="headings")
        definitions = (
            ("index", "序号", 55), ("color", "color 原值", 95),
            ("confidence", "confidence", 100), ("bbox", "识别框 x,y,w,h", 170),
            ("direction", "turn_derection", 125), ("remaining", "remaining_time", 125),
            ("lon", "lon", 150), ("lat", "lat", 150),
        )
        for key, title, width in definitions:
            self.traffic_tree.heading(key, text=title)
            self.traffic_tree.column(key, width=width, anchor="w", stretch=key in ("lon", "lat"))
        scroll = ttk.Scrollbar(frame, orient="vertical", command=self.traffic_tree.yview)
        self.traffic_tree.configure(yscrollcommand=scroll.set)
        self.traffic_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

    def _build_udp_tab(self, notebook):
        ttk = self.ttk
        frame = ttk.Frame(notebook, padding=8)
        notebook.add(frame, text="UDP 最新消息")
        ttk.Label(frame, text="每路只显示最新完整消息；重复和中间消息按秒合并，缺失分片计入异常。",
                  style="Muted.TLabel").pack(anchor="w", pady=(0, 6))
        pane = ttk.Panedwindow(frame, orient="vertical")
        pane.pack(fill="both", expand=True)
        table_frame = ttk.Frame(pane)
        detail_frame = ttk.Frame(pane)
        pane.add(table_frame, weight=3)
        pane.add(detail_frame, weight=2)
        columns = ("topic", "source", "time", "age", "bytes", "received", "duplicate", "coalesced", "parse")
        self.udp_tree = ttk.Treeview(table_frame, columns=columns, show="headings", selectmode="browse")
        definitions = (
            ("topic", "话题", 290), ("source", "来源", 160), ("time", "最近接收", 100),
            ("age", "距上次接收", 110), ("bytes", "字节", 75), ("received", "本轮报文", 80),
            ("duplicate", "重复消息", 80), ("coalesced", "已合并", 80), ("parse", "状态", 160),
        )
        for key, title, width in definitions:
            self.udp_tree.heading(key, text=title)
            self.udp_tree.column(key, width=width, anchor="w", stretch=key in ("topic", "parse"))
        scroll_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.udp_tree.yview)
        scroll_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.udp_tree.xview)
        self.udp_tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        self.udp_tree.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)
        self._configure_row_tags(self.udp_tree)
        self.udp_tree.bind("<<TreeviewSelect>>", self._show_selected_udp)
        ttk.Label(detail_frame, text="选中消息查看明细，原始内容预览前 2048 字节", style="Muted.TLabel").pack(anchor="w")
        self.udp_detail = self._text_with_scroll(detail_frame)

    def _build_log_tab(self, notebook):
        frame = self.ttk.Frame(notebook, padding=8)
        notebook.add(frame, text="运行日志")
        self.log_text = self._text_with_scroll(frame)

    def _text_with_scroll(self, parent):
        container = self.ttk.Frame(parent)
        container.pack(fill="both", expand=True, pady=(5, 0))
        text = self.tk.Text(
            container, wrap="none", font=(self.fixed_font_family, 9), background="#f7f8fa",
            foreground="#172033", insertbackground="#172033", relief="flat", padx=8, pady=8)
        scroll_y = self.ttk.Scrollbar(container, orient="vertical", command=text.yview)
        scroll_x = self.ttk.Scrollbar(container, orient="horizontal", command=text.xview)
        text.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        text.grid(row=0, column=0, sticky="nsew")
        scroll_y.grid(row=0, column=1, sticky="ns")
        scroll_x.grid(row=1, column=0, sticky="ew")
        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)
        return text

    @staticmethod
    def _configure_row_tags(tree):
        tree.tag_configure("changed", background="#e7f5ec")
        tree.tag_configure("warning", background="#fff4cf")
        tree.tag_configure("critical", background="#ffe2e5")
        tree.tag_configure("missing", foreground="#6b7280", background="#f3f4f6")
        tree.tag_configure("error", background="#ffe2e5")

    def _choose_output(self):
        selected = self.filedialog.askdirectory(initialdir=self.output_root.get() or str(Path.home()))
        if selected:
            self.output_root.set(selected)

    def _validated_config(self):
        return validate_capture_config(
            self.redis_host.get(), self.redis_port.get(), self.redis_db.get(),
            self.udp_group.get(), self.udp_port.get(), self.udp_interface.get())

    def restart_workers(self):
        if self.capture.active:
            self.messagebox.showerror("请先停止录制", "录制中不能更改连接；请先停止录制，再应用并重连。", parent=self.root)
            return
        try:
            config = self._validated_config()
        except Exception as exc:
            self.messagebox.showerror("连接设置错误", str(exc), parent=self.root)
            return
        self._applied_config = None
        stopping_errors = []
        for worker in (self.redis_worker, self.udp_worker):
            if worker is not None:
                try:
                    if not worker.stop():
                        stopping_errors.append("旧接收线程尚未停止")
                except Exception as exc:
                    stopping_errors.append(str(exc))
        if stopping_errors:
            self.messagebox.showerror("无法重新连接", "；".join(stopping_errors), parent=self.root)
            return
        self.redis_worker = None
        self.udp_worker = None
        started = []
        try:
            redis_worker = RedisWorker(
                self.bus, self.capture, config["redis_host"], config["redis_port"],
                config["redis_database"], interval=config["redis_interval_ms"] / 1000.0, scan_interval=5.0)
            udp_worker = UdpWorker(
                self.bus, self.capture, config["udp_multicast"], config["udp_port"],
                config["udp_interface"], route_host=config["redis_host"])
            redis_worker.start()
            started.append(("redis_worker", redis_worker))
            udp_worker.start()
            started.append(("udp_worker", udp_worker))
        except Exception as exc:
            for attribute, worker in started:
                try:
                    if not worker.stop():
                        setattr(self, attribute, worker)
                except Exception:
                    setattr(self, attribute, worker)
            self.messagebox.showerror("无法启动接收", str(exc), parent=self.root)
            return
        self.redis_worker = redis_worker
        self.udp_worker = udp_worker
        self._applied_config = dict(config)
        self._append_log("INFO", "已应用连接设置；Redis 和 UDP 后台接收已启动。", utc_now())

    def toggle_recording(self):
        if self.capture.active:
            try:
                with getattr(self.udp_worker, "sample_lock", nullcontext()):
                    flush = getattr(self.udp_worker, "flush_pending", None)
                    if flush:
                        flush()
                    directory = self.capture.stop()
            except Exception as exc:
                self.record_status.set("停止录制失败: " + str(exc))
                self.messagebox.showerror("录制文件未完整收尾", str(exc), parent=self.root)
                return
            self._last_capture_directory = directory
            self.record_status.set("已停止，可查看最近录制")
            self.record_button.configure(text="开始录制", style="Accent.TButton")
            self._record_was_active = False
            self._append_log("INFO", "录制已停止: %s" % directory, utc_now())
            self._update_overview()
            return
        try:
            config = self._validated_config()
            if self._applied_config is None:
                raise ValueError("请先应用连接设置，并成功启动 Redis 和 UDP 接收线程。")
            if config != self._applied_config:
                raise ValueError("连接设置尚未应用；请先点击“应用并重连”，再开始录制。")
            with getattr(self.udp_worker, "sample_lock", nullcontext()):
                directory = self.capture.start(self.output_root.get().strip(), self._applied_config)
                sync = getattr(self.udp_worker, "_sync_capture_generation", None)
                if sync:
                    sync()
        except Exception as exc:
            self.messagebox.showerror("无法开始录制", str(exc), parent=self.root)
            return
        self._last_capture_directory = directory
        self.record_status.set("录制中，每秒保存")
        self.record_button.configure(text="停止录制", style="Stop.TButton")
        self._record_was_active = True
        self._append_log("INFO", "录制已开始: %s" % directory, utc_now())
        self._update_overview()

    def open_capture_directory(self):
        if self._last_capture_directory and Path(self._last_capture_directory).is_file():
            self.open_recording(self._last_capture_directory)
        else:
            self.open_recording()

    def open_recording(self, path=None):
        if path is None:
            path = self.filedialog.askopenfilename(
                parent=self.root, title="打开录制文件",
                initialdir=self.output_root.get() or str(Path.home()),
                filetypes=[("SmartDriver 录制", "*.sdlog")])
        if not path:
            return
        try:
            from log_viewer import LogViewer
            LogViewer(self.root, path)
        except Exception as exc:
            self.messagebox.showerror("无法打开录制", str(exc), parent=self.root)

    def _process_events(self):
        self.capture.tick()
        processed = 0
        while processed < 1000:
            try:
                event = self.bus.queue.get_nowait()
            except queue.Empty:
                break
            self._handle_event(event)
            processed += 1
        now = time.monotonic()
        if now - self._last_age_refresh >= 1.0:
            self._refresh_ages(now)
            self._last_age_refresh = now
        if self._record_was_active and not self.capture.active:
            self._record_was_active = False
            self.record_button.configure(text="开始录制", style="Accent.TButton")
            self.record_status.set("录制错误: " + (self.capture.last_error or "未知错误"))
            self._append_log("ERROR", self.capture.last_error or "录制意外停止", utc_now())
        self.metric_text.set(
            "Redis %d · UDP 已采样 %d · 已合并 %d · 重复 %d" % (
                self.counters["redis"], self.counters["udp"],
                self.udp_metrics.get("coalesced", 0), self.udp_metrics.get("duplicate", 0)))
        self._update_overview()
        self.root.after(1000, self._process_events)

    def _handle_event(self, event):
        event_type = event.get("type")
        if event_type == "log":
            self._append_log(event["level"], event["message"], event["utc"])
        elif event_type == "redis_status":
            self.redis_status.set(self._status_text(event["status"], event.get("detail", "")))
            self.redis_status_detail = event.get("detail", "")
        elif event_type == "udp_status":
            self.udp_status.set(self._status_text(event["status"], event.get("detail", "")))
            self.udp_status_detail = event.get("detail", "")
        elif event_type == "redis_inventory":
            self.inventory_count = len(event.get("keys", []))
            self.inventory_method = {
                "scan": "SCAN",
                "configured": "固定 Unity 键",
            }.get(event.get("discovery_method"), "未知")
        elif event_type == "redis_observation":
            self.redis_read_method = event.get("read_method", "未知").upper()
            self._handle_redis(event)
        elif event_type == "udp_datagram":
            self._handle_udp(event)
        elif event_type == "udp_metrics":
            self.udp_metrics = event["stats"]

    @staticmethod
    def _status_text(status, detail):
        labels = {
            "connecting": "连接中",
            "connected": "已连接",
            "listening": "监听中",
            "error": "错误",
            "stopped": "已停止",
        }
        text = labels.get(status, status)
        return text + (" | " + detail if detail else "")

    def _handle_redis(self, event):
        key = event["key"]
        first = key not in self.redis_states
        previous = self.redis_states.get(key)
        changed_monotonic = event["monotonic_ns"] / 1_000_000_000.0
        if previous and not event["changed"]:
            changed_monotonic = previous["changed_monotonic"]
        state = dict(event)
        state["changed_monotonic"] = changed_monotonic
        self.redis_states[key] = state
        self.counters["redis"] += 1
        if event["changed"]:
            self.counters["redis_changed"] += 1
        if event["parse"]["status"] == "parse_error":
            self.counters["redis_parse_error"] += 1
        self._upsert_redis_row(key, state, time.monotonic())
        if key == "Sensor_TrafficLightlist" and (first or event["changed"] or not event["present"]):
            self._refresh_traffic(state)
        selected = self.redis_tree.selection()
        if selected and selected[0] == key:
            self._show_redis_state(state)

    def _upsert_redis_row(self, key, state, now):
        age = max(0.0, now - state["changed_monotonic"])
        status = state["parse"]["status"]
        values = (
            key,
            "有" if state["present"] else "无",
            state["length"],
            _short_time(state["observed_at_utc"]),
            _format_age(age),
            status,
            state["sha256"] or "",
        )
        tag = self._redis_tag(state, age)
        if self.redis_tree.exists(key):
            self.redis_tree.item(key, values=values, tags=(tag,))
        else:
            self.redis_tree.insert("", "end", iid=key, values=values, tags=(tag,))

    @staticmethod
    def _redis_tag(state, age):
        if not state["present"]:
            return "missing"
        if state["parse"]["status"] == "parse_error":
            return "error"
        if age < 1.0:
            return "changed"
        if age >= 10.0:
            return "critical"
        if age >= 2.0:
            return "warning"
        return ""

    def _refresh_ages(self, now):
        for key, state in self.redis_states.items():
            self._upsert_redis_row(key, state, now)
        for iid, event in self.udp_events.items():
            self._upsert_udp_row(iid, event, now)
        traffic = self.redis_states.get("Sensor_TrafficLightlist")
        if traffic:
            self._set_traffic_status(traffic, now)

    def _show_selected_redis(self, _event=None):
        selected = self.redis_tree.selection()
        if selected:
            state = self.redis_states.get(selected[0])
            if state:
                self._show_redis_state(state)

    def _show_redis_state(self, state):
        detail = {key: value for key, value in state.items() if key not in ("raw_hex", "changed_monotonic")}
        text = json_text(detail)[:65536] + "\n\nRAW HEX (预览前 2048 字节，共 %d 字节)\n%s" % (
            state["length"], _format_hex(state["raw_hex"]))
        self._replace_text(self.redis_detail, text)

    def _refresh_traffic(self, state):
        self.traffic_tree.delete(*self.traffic_tree.get_children())
        parsed = state["parse"]
        decoded = parsed.get("decoded") if parsed.get("status", "").startswith("ok") else None
        if decoded:
            for index, light in enumerate(decoded.get("trafficlights", [])):
                bbox = "%s,%s,%s,%s" % (
                    light.get("light_pt_x"), light.get("light_pt_y"),
                    light.get("width"), light.get("height"))
                self.traffic_tree.insert("", "end", values=(
                    index,
                    light.get("color"),
                    light.get("confidence"),
                    bbox,
                    light.get("turn_derection"),
                    light.get("remaining_time"),
                    light.get("lon"),
                    light.get("lat"),
                ))
        self._set_traffic_status(state, time.monotonic())

    def _set_traffic_status(self, state, now):
        age = max(0.0, now - state["changed_monotonic"])
        parsed = state["parse"]
        decoded = parsed.get("decoded") or {}
        lights = decoded.get("trafficlights", [])
        declared = decoded.get("traffic_light_num", "—")
        if not state["present"]:
            text = "Redis 键不存在；当前交通灯列表已清空"
        elif parsed["status"] == "parse_error":
            text = "解析失败；当前交通灯列表已清空 | %s" % parsed.get("error", "")
        else:
            text = "消息内 %d 盏（traffic_light_num=%s） | 最后观察 %s | 内容未变化 %s | %s" % (
                len(lights), declared, _short_time(state["observed_at_utc"]),
                _format_age(age), parsed["status"])
        self.traffic_status.set(text)

    def _handle_udp(self, event):
        self.counters["udp"] += 1
        if event["parse"]["status"] == "parse_error":
            self.counters["udp_parse_error"] += 1
        iid = event.get("stream_key") or "%s:%s" % (event["source_ip"], event["source_port"])
        self.udp_events[iid] = event
        self.udp_events.move_to_end(iid)
        self._upsert_udp_row(iid, event, time.monotonic())
        while len(self.udp_events) > 128:
            old_iid, _ = self.udp_events.popitem(last=False)
            if self.udp_tree.exists(old_iid):
                self.udp_tree.delete(old_iid)
        if self.udp_tree.selection() == (iid,):
            self._show_selected_udp()

    def _upsert_udp_row(self, iid, event, now):
        age = max(0.0, now - event["monotonic_ns"] / 1_000_000_000)
        parsed = event["parse"]
        status = {
            "lcm_unparsed": "已收齐，业务未解码",
            "lcm_reassembled": "已重组，业务未解码",
            "parse_error": "解析失败", "ok": "可解析", "ok_with_trailing": "含尾随字节",
        }.get(parsed["status"], parsed["status"])
        if age >= 3:
            status = "超过 %d 秒未更新" % age
        values = (
            event.get("topic") or event.get("transport", {}).get("channel") or "未识别 UDP",
            "%s:%s" % (event["source_ip"], event["source_port"]),
            _short_time(event["observed_at_utc"]), _format_age(age), event["length"],
            event.get("received_count", 1), event.get("duplicate_count", 0),
            event.get("coalesced_count", 0), status)
        tag = "error" if parsed["status"] == "parse_error" else "critical" if age >= 10 else "warning" if age >= 3 else "changed"
        if self.udp_tree.exists(iid):
            self.udp_tree.item(iid, values=values, tags=(tag,))
        else:
            self.udp_tree.insert("", "end", iid=iid, values=values, tags=(tag,))

    def _show_selected_udp(self, _event=None):
        selected = self.udp_tree.selection()
        if not selected:
            return
        event = self.udp_events.get(selected[0])
        if not event:
            return
        detail = {key: value for key, value in event.items() if key != "raw_hex"}
        text = json_text(detail)[:65536] + "\n\nRAW HEX (预览前 2048 字节，共 %d 字节)\n%s" % (
            event["length"], _format_hex(event["raw_hex"]))
        self._replace_text(self.udp_detail, text)

    def _append_log(self, level, message, utc_value):
        line = "%s [%s] %s\n" % (utc_value, level, message)
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line)
        lines = int(float(self.log_text.index("end-1c").split(".")[0]))
        if lines > 2000:
            self.log_text.delete("1.0", "%d.0" % (lines - 1800))
        self.log_text.see("end")

    @staticmethod
    def _replace_text(widget, value):
        widget.delete("1.0", "end")
        widget.insert("1.0", value)

    def _update_overview(self):
        redis_state = self.redis_status.get().split(" | ", 1)[0]
        udp_state = self.udp_status.get().split(" | ", 1)[0]
        record_state = "录制中" if self.capture.active else "未录制"
        self.overview_tree.item("redis", values=(
            "Redis", redis_state, self.counters["redis"],
            "键清单 %d（%s），读取 %s，已知 Unity 键 %d，解析失败 %d" % (
                self.inventory_count, self.inventory_method, self.redis_read_method,
                len(UNITY_REDIS_KEYS), self.counters["redis_parse_error"])))
        self.overview_tree.item("udp", values=(
            "UDP " + self.udp_port.get(), udp_state, self.counters["udp"],
            "收到 %d 包；重复 %d；迟到 %d；无效 %d；缺失消息 %d；超限 %d；界面略过 %d" % (
                self.udp_metrics.get("received", 0), self.udp_metrics.get("duplicate", 0),
                self.udp_metrics.get("out_of_order", 0), self.udp_metrics.get("invalid", 0),
                self.udp_metrics.get("incomplete", 0), self.udp_metrics.get("limit_dropped", 0), self.bus.dropped)))
        self.overview_tree.item("record", values=(
            "磁盘录制", record_state,
            self.capture.counts.get("redis", 0) + self.capture.counts.get("udp", 0),
            str(self.capture.directory or "尚未创建录制文件")))

    def close(self):
        stopping_errors = []
        for worker in (self.redis_worker, self.udp_worker):
            if worker is not None:
                try:
                    if not worker.stop():
                        stopping_errors.append("接收线程尚未停止，请重试关闭")
                except Exception as exc:
                    stopping_errors.append(str(exc))
        if stopping_errors:
            self._applied_config = None
            self.messagebox.showerror("无法关闭", "；".join(stopping_errors), parent=self.root)
            return
        try:
            self.capture.stop()
        except Exception as exc:
            self.messagebox.showerror("录制文件未完整收尾", str(exc), parent=self.root)
        finally:
            self.root.destroy()


def run_headless(config, output_root, duration_seconds=0, archive=False):
    if duration_seconds < 0:
        raise ValueError("duration must be zero or greater")
    bus = UiEventBus()
    capture = DesktopCaptureSession()
    directory = capture.start(output_root, config)
    redis_worker = RedisWorker(
        bus, capture, config["redis_host"], config["redis_port"],
        config["redis_database"], interval=config["redis_interval_ms"] / 1000.0,
        scan_interval=5.0)
    udp_worker = UdpWorker(
        bus, capture, config["udp_multicast"], config["udp_port"],
        config["udp_interface"], route_host=config["redis_host"])
    started = []
    try:
        redis_worker.start()
        started.append(redis_worker)
        udp_worker.start()
        started.append(udp_worker)
        print("CAPTURE_FILE=" + str(directory), flush=True)
        deadline = time.monotonic() + duration_seconds if duration_seconds else None
        while deadline is None or time.monotonic() < deadline:
            capture.tick()
            if capture.last_error:
                raise RuntimeError(capture.last_error)
            timeout = 0.25 if deadline is None else min(0.25, max(0.0, deadline - time.monotonic()))
            if timeout == 0:
                break
            try:
                event = bus.queue.get(timeout=timeout)
            except queue.Empty:
                continue
            if event.get("type") in ("log", "redis_status", "udp_status"):
                print(json_text(event, compact=True), flush=True)
    except KeyboardInterrupt:
        print("STOP_REQUESTED=keyboard_interrupt", flush=True)
    except Exception as exc:
        with capture.lock:
            capture.last_error = capture.last_error or "capture failed: %s" % exc
        raise
    finally:
        finalization_error = None
        for worker in started:
            try:
                if not worker.stop():
                    raise RuntimeError("capture receiver did not stop")
            except Exception as exc:
                finalization_error = finalization_error or exc
        if finalization_error is not None or capture.last_error:
            with capture.lock:
                if finalization_error is not None:
                    capture.last_error = "capture receiver shutdown failed: %s" % finalization_error
                capture._abort_locked()
        try:
            capture.stop()
        except Exception as exc:
            finalization_error = finalization_error or exc
        if finalization_error:
            raise finalization_error
    if capture.last_error:
        raise RuntimeError(capture.last_error)
    verify_capture(directory)
    print("VERIFY_CAPTURE=OK", flush=True)
    if archive:
        archive_path = pack_capture(directory)
        print("ARCHIVE=" + str(archive_path), flush=True)
        print("ARCHIVE_SHA256=" + _sha256_file(archive_path), flush=True)
    return directory


def _encode_varint(value):
    if value < 0:
        value &= (1 << 64) - 1
    output = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        output.append(byte | (0x80 if value else 0))
        if not value:
            return bytes(output)


def _protobuf_varint(number, value):
    return _encode_varint(number << 3) + _encode_varint(value)


def _protobuf_double(number, value):
    return _encode_varint((number << 3) | 1) + struct.pack("<d", value)


def _protobuf_bytes(number, value):
    return _encode_varint((number << 3) | 2) + _encode_varint(len(value)) + value


def _fixed_test_string(value):
    encoded = value.encode("utf-8")
    return encoded + b"\0" * (32 - len(encoded))


def _test_udp_payload():
    return b"".join((
        struct.pack("<iii", 7, 100, 200), _fixed_test_string("map"),
        struct.pack("<i", 3), _fixed_test_string("渝A12345"), _fixed_test_string("L4"),
        struct.pack("<ii", 1, 2),
        struct.pack("<ddd", 0.1, 0.2, 10.0),
        struct.pack("<ddd", 106.3, 29.5, 250.0),
        struct.pack("<ddd", 1.0, 2.0, 3.0),
        struct.pack("<ddd", 0.01, 0.02, 1.57),
        struct.pack("<i", 1), struct.pack("<ii", 4, 5),
        struct.pack("<i", 0),
        struct.pack("<iiiii", 6, 7, 8, 9, 10),
        b"\xaa\xbb\xcc",
    ))


def run_self_test():
    import io

    assert (
        DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT, DEFAULT_REDIS_DATABASE,
        DEFAULT_UDP_GROUP, DEFAULT_UDP_PORT,
    ) == ("192.168.2.112", 6379, 0, "239.255.76.21", 7621)
    assert REDIS_INTERVAL_SECONDS == 1.0
    assert validate_capture_config(
        "127.0.0.1", 6379, 0, DEFAULT_UDP_GROUP, 7621, "auto")["redis_interval_ms"] == 1000

    lcm = parse_udp_packet(b"LC02" + struct.pack(">I", 23) + b"/test\0" + b"12345678body")
    assert lcm["status"] == "lcm_unparsed" and lcm["decoded"] is None
    assert lcm["transport"]["channel"] == "/test" and not lcm["schema_verified"]
    assert parse_udp_packet(b"LC03")["status"] == "parse_error"

    light = b"".join((
        _protobuf_varint(1, 2),
        _protobuf_double(2, 0.75),
        _protobuf_varint(7, 1),
        _protobuf_double(8, 12.5),
        _protobuf_double(9, 106.3265),
        _protobuf_double(10, 29.5152),
    ))
    traffic_payload = b"".join((
        _protobuf_double(1, 123.0),
        _protobuf_varint(2, 1),
        _protobuf_bytes(3, light),
    ))
    traffic = decode_redis_payload("Sensor_TrafficLightlist", traffic_payload)
    assert traffic["status"] == "ok"
    assert traffic["decoded"]["traffic_light_num"] == 1
    assert traffic["decoded"]["trafficlights"][0]["remaining_time"] == 12.5

    udp_payload = _test_udp_payload()
    udp = parse_udp_packet(udp_payload)
    assert udp["status"] == "ok_with_trailing"
    assert udp["trailing_bytes"] == 3
    assert udp["decoded"]["vid"] == 3
    assert udp["decoded"]["collaborative_vehicle_list"][0]["vid"] == 4
    too_many_vehicles = bytearray(udp_payload[:228] + bytes(64 * 8) + udp_payload[228:])
    struct.pack_into("<i", too_many_vehicles, 216, 65)
    assert parse_udp_packet(too_many_vehicles)["status"] == "parse_error"
    too_many_points = bytearray(udp_payload[:232] + bytes(257 * 141) + udp_payload[232:])
    struct.pack_into("<i", too_many_points, 228, 257)
    assert parse_udp_packet(too_many_points)["status"] == "parse_error"

    connection = RedisConnection("unused", 0, 0)
    connection.stream = io.BytesIO(b"*2\r\n$3\r\nabc\r\n$-1\r\n")
    assert connection._read_resp() == [b"abc", None]
    connection.stream = io.BytesIO(b"-NOPERM command not allowed\r\n")
    try:
        connection._read_resp()
        raise AssertionError("Redis command error was not raised")
    except RedisCommandError as exc:
        assert "NOPERM" in str(exc)

    fallback_bus = UiEventBus()
    fallback_worker = RedisWorker(fallback_bus, CaptureSession(), "unused", 0, 0, interval=0)

    class GetOnlyRedis:
        def __init__(self):
            self.get_keys = []

        @staticmethod
        def scan_keys():
            raise RedisCommandError("NOPERM SCAN is not allowed")

        def command(self, name, *parts):
            if name == "MGET":
                raise RedisCommandError("NOPERM MGET is not allowed")
            assert name == "GET" and len(parts) == 1
            key = parts[0]
            self.get_keys.append(key)
            if len(self.get_keys) == len(UNITY_REDIS_KEYS):
                fallback_worker.stop_event.set()
            if key == "Sensor_Lanelist":
                raise RedisCommandError("NOPERM this key is not allowed")
            return None

    get_only = GetOnlyRedis()
    fallback_worker._poll(get_only, "unused:0/db0")
    assert get_only.get_keys == list(UNITY_REDIS_KEYS)
    fallback_events = []
    while not fallback_bus.queue.empty():
        fallback_events.append(fallback_bus.queue.get_nowait())
    assert len([event for event in fallback_events if event["type"] == "redis_observation"]) == 5
    assert any(event.get("discovery_method") == "configured" for event in fallback_events)
    assert all(
        event["read_method"] == "get"
        for event in fallback_events if event["type"] == "redis_observation")
    assert any("individual GET" in event.get("message", "") for event in fallback_events)

    rejected_worker = RedisWorker(UiEventBus(), CaptureSession(), "unused", 0, 0, interval=0)

    class RejectedMget(GetOnlyRedis):
        def command(self, name, *parts):
            if name == "MGET":
                raise RedisCommandError("CROSSSLOT keys do not hash to the same slot")
            raise AssertionError("ordinary MGET errors must not fall back to GET")

    try:
        rejected_worker._poll(RejectedMget(), "unused:0/db0")
        raise AssertionError("ordinary MGET error was swallowed")
    except RedisCommandError as exc:
        assert "CROSSSLOT" in str(exc)

    with tempfile.TemporaryDirectory(prefix="smartdriver-live-test-") as temporary:
        config = validate_capture_config(
            "127.0.0.1", DEFAULT_REDIS_PORT, DEFAULT_REDIS_DATABASE,
            DEFAULT_UDP_GROUP, DEFAULT_UDP_PORT, DEFAULT_UDP_INTERFACE)
        capture = CaptureSession()
        worker = RedisWorker(UiEventBus(), capture, "unused", 0, 0)
        inventory = {
            "type": "redis_inventory", "observed_at_utc": utc_now(),
            "endpoint": "unused:0/db0", "keys": [{"key": "Sensor_Location", "type": "string"}],
        }
        signature = (("Sensor_Location", "string"),)
        assert worker._record_inventory(inventory, signature) is False
        directory = capture.start(temporary, dict(config, self_test=True))
        assert worker._record_inventory(inventory, signature) is True
        assert worker._record_inventory(inventory, signature) is False
        digest = hashlib.sha256(udp_payload).hexdigest()

        def verify_raw_first():
            assert (directory / "payloads" / "udp" / (digest + ".bin")).read_bytes() == udp_payload
            return parse_udp_packet(udp_payload)

        parsed, error = capture.observe(
            "udp", udp_payload,
            {"kind": "udp_datagram", "sequence": 1, "observed_at_utc": utc_now()},
            verify_raw_first,
        )
        capture.stop()
        assert error is None and parsed["trailing_bytes"] == 3
        lines = (directory / "udp-observations.jsonl").read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1 and json.loads(lines[0])["sha256"] == digest
        manifest = verify_capture(directory)
        assert manifest["counts"] == {"events": 2, "inventory": 1, "udp": 1}
        archive = pack_capture(directory)
        assert _sha256_file(archive) in archive.with_name(archive.name + ".sha256").read_text(encoding="utf-8")
        with zipfile.ZipFile(str(archive)) as bundle:
            assert directory.name + "/manifest.json" in bundle.namelist()
        (directory / "payloads" / "udp" / (digest + ".bin")).write_bytes(udp_payload + b"\0")
        try:
            verify_capture(directory)
            raise AssertionError("corrupt capture unexpectedly passed verification")
        except ValueError as exc:
            assert "mismatch" in str(exc)
    print("SELF_TEST=OK")


def run_network_self_test():
    redis_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    redis_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    redis_listener.bind(("127.0.0.1", 0))
    redis_listener.listen(1)
    redis_listener.settimeout(2.0)
    redis_port = redis_listener.getsockname()[1]
    redis_request = []
    redis_server_errors = []

    def serve_redis_once():
        try:
            with redis_listener:
                client, _ = redis_listener.accept()
                with client, client.makefile("rb") as stream:
                    request_reader = RedisConnection("unused", 0, 0)
                    request_reader.stream = stream
                    redis_request.extend(request_reader._read_resp())
                    reply = [b"*%d\r\n" % len(UNITY_REDIS_KEYS)]
                    for value in (b"abc",) + (None,) * (len(UNITY_REDIS_KEYS) - 1):
                        reply.append(b"$-1\r\n" if value is None else b"$%d\r\n%s\r\n" % (len(value), value))
                    client.sendall(b"".join(reply))
        except BaseException as exc:
            redis_server_errors.append(exc)

    redis_thread = threading.Thread(target=serve_redis_once, daemon=True)
    redis_thread.start()
    redis = RedisConnection("127.0.0.1", redis_port, 0, timeout=2.0)
    try:
        redis.connect()
        redis_values = redis.command("MGET", *UNITY_REDIS_KEYS)
    finally:
        redis.close()
    redis_thread.join(2.0)
    assert not redis_thread.is_alive() and not redis_server_errors
    assert redis_request == [b"MGET"] + [key.encode("utf-8") for key in UNITY_REDIS_KEYS]
    assert redis_values == [b"abc"] + [None] * (len(UNITY_REDIS_KEYS) - 1)

    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(("127.0.0.1", 0))
    udp_socket.settimeout(0.2)
    udp_port = udp_socket.getsockname()[1]
    udp_bus = UiEventBus()
    udp_worker = UdpWorker(
        udp_bus, CaptureSession(), DEFAULT_UDP_GROUP, udp_port, DEFAULT_UDP_INTERFACE)
    udp_errors = []

    def receive_udp():
        try:
            udp_worker._receive(udp_socket, "%s:%d" % (DEFAULT_UDP_GROUP, udp_port))
        except BaseException as exc:
            if not udp_worker.stop_event.is_set():
                udp_errors.append(exc)

    udp_thread = threading.Thread(target=receive_udp, daemon=True)
    udp_thread.start()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender:
            sender.sendto(_test_udp_payload(), ("127.0.0.1", udp_port))
        deadline = time.monotonic() + 3.0
        while True:
            udp_event = udp_bus.queue.get(timeout=max(0.01, deadline - time.monotonic()))
            if udp_event["type"] == "udp_datagram":
                break
            if time.monotonic() >= deadline:
                raise AssertionError("UDP sample did not arrive")
    finally:
        udp_worker.stop_event.set()
        udp_socket.close()
        udp_thread.join(2.0)
    assert not udp_thread.is_alive() and not udp_errors
    assert udp_event["type"] == "udp_datagram" and udp_event["parse"]["decoded"]["vid"] == 3
    print("NETWORK_SELF_TEST=OK")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--self-test", action="store_true", help="run parser/recorder checks without opening a GUI")
    mode.add_argument(
        "--network-self-test", action="store_true",
        help="run loopback Redis/UDP socket checks without contacting field systems")
    mode.add_argument("--headless", action="store_true", help="record without Tk; works on Linux and Windows")
    mode.add_argument("--verify-capture", metavar="PATH", help="verify a completed .sdlog or legacy capture directory")
    mode.add_argument("--pack-capture", metavar="PATH", help="verify a .sdlog or legacy directory and create a ZIP")
    parser.add_argument("--redis-host", default=DEFAULT_REDIS_HOST)
    parser.add_argument("--redis-port", default=DEFAULT_REDIS_PORT, type=int)
    parser.add_argument("--redis-database", default=DEFAULT_REDIS_DATABASE, type=int)
    parser.add_argument("--udp-group", default=DEFAULT_UDP_GROUP)
    parser.add_argument("--udp-port", default=DEFAULT_UDP_PORT, type=int)
    parser.add_argument("--udp-interface", default=DEFAULT_UDP_INTERFACE)
    parser.add_argument(
        "--output-root", default=str(Path.home() / "Documents" / "SmartDriverCaptures"))
    parser.add_argument(
        "--duration-seconds", default=0, type=float,
        help="headless duration; zero records until Ctrl+C")
    parser.add_argument("--archive", action="store_true", help="pack a headless capture after stopping")
    args = parser.parse_args(argv)
    if args.self_test:
        run_self_test()
        return 0
    if args.network_self_test:
        run_network_self_test()
        return 0
    if args.archive and not args.headless:
        parser.error("--archive requires --headless")
    try:
        if args.verify_capture:
            manifest = verify_capture(args.verify_capture)
            print("VERIFY_CAPTURE=OK")
            print("CAPTURE_COUNTS=" + json_text(manifest["counts"], compact=True))
            return 0
        if args.pack_capture:
            archive = pack_capture(args.pack_capture)
            print("ARCHIVE=" + str(archive))
            print("ARCHIVE_SHA256=" + _sha256_file(archive))
            return 0
        if args.headless:
            run_self_test()
            config = validate_capture_config(
                args.redis_host, args.redis_port, args.redis_database,
                args.udp_group, args.udp_port, args.udp_interface)
            run_headless(config, args.output_root, args.duration_seconds, args.archive)
            return 0
    except Exception as exc:
        print("ERROR=" + str(exc), file=sys.stderr)
        return 2
    run_self_test()
    try:
        import tkinter as tk
    except ImportError as exc:
        print(
            "Tkinter is unavailable: %s. Install the system Python Tk package "
            "(Ubuntu/Debian: python3-tk), or use --headless." % exc,
            file=sys.stderr,
        )
        return 2
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        print(
            "GUI is unavailable: %s. Install Tk and start from a graphical desktop; "
            "use --headless on a server." % exc,
            file=sys.stderr,
        )
        return 2
    initial_config = {
        "redis_host": args.redis_host,
        "redis_port": args.redis_port,
        "redis_database": args.redis_database,
        "udp_multicast": args.udp_group,
        "udp_port": args.udp_port,
        "udp_interface": args.udp_interface,
    }
    SmartDriverApp(root, initial_config, args.output_root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
