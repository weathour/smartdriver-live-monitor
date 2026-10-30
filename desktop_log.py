"""Single-file desktop capture format 4; standard-library SQLite only.

A live file exposes its most recently committed snapshot. Writers commit at most
about once a second, including from tick() during quiet periods. Readers never
need a journal sidecar after successful stop().
"""
import collections
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import sqlite3
import tempfile
import threading
import time
import uuid
import zipfile

FORMAT_VERSION = 4
RECORDER_VERSION = "1.5"
COMMIT_INTERVAL_SECONDS = 1.0
COUNT_KINDS = ("redis", "udp", "inventory", "events")


def _utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json_safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    return value


def _json(value):
    return json.dumps(_json_safe(value), ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _stream_key(kind, metadata):
    if metadata.get("stream_key") is not None:
        return str(metadata["stream_key"])
    if kind == "redis":
        return "%s/%s" % (metadata.get("endpoint", ""), metadata.get("key", ""))
    if kind == "udp":
        transport = metadata.get("transport") or (metadata.get("parse") or {}).get("transport") or {}
        return "%s:%s/%s" % (metadata.get("source_ip", ""), metadata.get("source_port", ""),
                              metadata.get("channel", transport.get("channel", metadata.get("listen_endpoint", ""))))
    return str(metadata.get("endpoint", kind))


def _record_summary(metadata):
    """Project a fixed set of bounded scalars; never walk decoded collections."""
    result = {}
    for key in (
            "kind", "sequence", "observed_at_utc", "monotonic_ns", "key", "topic", "channel",
            "source_ip", "source_port", "endpoint", "listen_endpoint", "stream_key",
            "present", "changed", "length", "sha256", "sampling_interval_ms",
            "received_at_utc", "received_count", "complete_count", "coalesced_count", "duplicate_count",
            "first_monotonic_ns", "first_observed_at_utc", "sequence_reset",
            "out_of_order_count", "wire_hint", "read_method", "round_trip_ms",
            "discovery_method", "level", "message"):
        value = metadata.get(key)
        if isinstance(value, str):
            result[key] = value[:256 if key == "message" else 1024]
        elif value is None or isinstance(value, (bool, int, float)):
            if key in metadata:
                result[key] = value
    parsed = metadata.get("parse")
    if isinstance(parsed, dict):
        status = parsed.get("status")
        if isinstance(status, str):
            result["parse"] = {"status": status[:128]}
    transport = metadata.get("transport")
    if not isinstance(transport, dict) and isinstance(parsed, dict):
        transport = parsed.get("transport")
    if isinstance(transport, dict) and "channel" not in result:
        channel = transport.get("channel")
        if isinstance(channel, str):
            result["channel"] = channel[:1024]
    return result


def _configure(connection):
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA cache_size=-2048")
    connection.execute("PRAGMA mmap_size=0")
    connection.execute("PRAGMA temp_store=FILE")
    connection.execute("PRAGMA busy_timeout=3000")


class DesktopCaptureSession:
    def __init__(self):
        self.lock = threading.RLock()
        self.active = False
        self.generation = 0
        self.directory = None  # Compatibility name: this is a file, not a directory.
        self.counts = collections.Counter()
        self.last_error = None
        self.config = None
        self.started_at = None
        self._connection = None
        self._last_commit = 0.0

    def start(self, output_root, config):
        with self.lock:
            if self.active:
                return self.directory
            root = Path(output_root).expanduser()
            root.mkdir(parents=True, exist_ok=True)
            stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            path = root / ("SmartDriverLiveCapture-%s-%s.sdlog" % (stamp, uuid.uuid4().hex[:8]))
            # Reserve the name exclusively; never attach to an existing database.
            with path.open("xb"):
                pass
            self.directory = path
            self.counts = collections.Counter({kind: 0 for kind in COUNT_KINDS})
            self.last_error = None
            self.started_at = _utc_now()
            self.config = dict(config)
            try:
                self._connection = sqlite3.connect(str(path), check_same_thread=False)
                _configure(self._connection)
                self._connection.execute("PRAGMA journal_mode=DELETE")
                self._connection.execute("PRAGMA synchronous=NORMAL")
                self._connection.execute("PRAGMA user_version=4")
                self._connection.executescript('''
                    CREATE TABLE manifest (id INTEGER PRIMARY KEY CHECK(id=1), json TEXT NOT NULL);
                    CREATE TABLE payloads (
                        sha256 TEXT PRIMARY KEY, length INTEGER NOT NULL CHECK(length>=0), data BLOB NOT NULL);
                    CREATE TABLE records (
                        id INTEGER PRIMARY KEY, kind TEXT NOT NULL,
                        observed_at_utc TEXT NOT NULL, stream_key TEXT NOT NULL,
                        metadata_json TEXT NOT NULL, summary_json TEXT NOT NULL,
                        payload_sha256 TEXT REFERENCES payloads(sha256));
                    CREATE INDEX records_kind_id ON records(kind,id);
                    CREATE INDEX records_stream_id ON records(stream_key,id);
                ''')
                self.active = True
                self.generation += 1
                self._log_locked("INFO", "recording started")
                self._commit_locked()
            except Exception as exc:
                self.last_error = "recording start failed: %s" % exc
                self._abort_locked()
                raise RuntimeError(self.last_error) from exc
            return path

    def _manifest_locked(self, status="recording", ended_at=None):
        return {
            "format_version": FORMAT_VERSION, "recorder": "SmartDriverLiveMonitor",
            "recorder_version": RECORDER_VERSION, "status": status,
            "started_at_utc": self.started_at, "ended_at_utc": ended_at,
            "capture_layer": "application_socket", "config": self.config,
            "counts": {kind: self.counts[kind] for kind in COUNT_KINDS},
            "last_error": self.last_error,
            "environment": {
                "operating_system": platform.system(), "operating_system_release": platform.release(),
                "machine": platform.machine(), "python_version": platform.python_version(),
                "python_implementation": platform.python_implementation(), "sqlite_version": sqlite3.sqlite_version,
            },
            "files": {"container": "single SQLite .sdlog file", "payloads": "content-addressed raw BLOBs",
                      "records": "ordered observations, inventories and runtime events"},
            "semantics": {
                "redis": "Every completed sampled Redis read is recorded, including unchanged values.",
                "udp": "Caller-selected latest messages per source/channel at 1 Hz; complete LCM business payloads exclude transport headers; legacy/unknown bytes retain their datagram form.",
                "durability": "Batched transactions commit about once per second or on tick and stop; uncommitted data can be lost on process or storage failure.",
                "boundary": "Application-delivered data, not PCAP or a claim that all network datagrams were retained.",
            },
        }

    def _write_manifest_locked(self, status="recording", ended_at=None):
        self._connection.execute("INSERT OR REPLACE INTO manifest(id,json) VALUES(1,?)",
                                 (_json(self._manifest_locked(status, ended_at)),))

    def _commit_locked(self):
        self._write_manifest_locked()
        self._connection.commit()
        self._last_commit = time.monotonic()

    def tick(self):
        """Commit pending work after one second, even when no new packets arrive."""
        with self.lock:
            if not self.active or time.monotonic() - self._last_commit < COMMIT_INTERVAL_SECONDS:
                return False
            try:
                self._commit_locked()
                return True
            except Exception as exc:
                self.last_error = "recording commit failed: %s" % exc
                self._abort_locked()
                return False

    def flush(self):
        """Commit the current recording snapshot immediately."""
        with self.lock:
            if not self.active:
                return False
            try:
                self._commit_locked()
                return True
            except Exception as exc:
                self.last_error = "recording commit failed: %s" % exc
                self._abort_locked()
                return False

    def _insert_record_locked(self, kind, metadata, payload_sha256=None):
        value = dict(metadata)
        observed_at = value.setdefault("observed_at_utc", _utc_now())
        stream_key = _stream_key(kind, value)
        self._connection.execute(
            "INSERT INTO records(kind,observed_at_utc,stream_key,metadata_json,summary_json,payload_sha256) VALUES(?,?,?,?,?,?)",
            (kind, observed_at, stream_key, _json(value), _json(_record_summary(value)), payload_sha256))
        self.counts[kind] += 1

    def observe(self, kind, payload, metadata, decoder):
        parsed = None
        decoder_called = False
        error = None
        with self.lock:
            try:
                digest = None
                if self.active:
                    if kind not in ("redis", "udp"):
                        raise ValueError("unsupported observation kind: " + str(kind))
                    if payload is not None:
                        digest = hashlib.sha256(payload).hexdigest()
                        self._connection.execute(
                            "INSERT OR IGNORE INTO payloads(sha256,length,data) VALUES(?,?,?)",
                            (digest, len(payload), sqlite3.Binary(payload)))
                # The original bytes exist in the transaction before parsing.
                decoder_called = True
                parsed = decoder()
                if self.active:
                    record = dict(metadata)
                    record["parse"] = parsed
                    record["sha256"] = digest
                    record["length"] = len(payload) if payload is not None else 0
                    self._insert_record_locked(kind, record, digest)
                    self.tick()
                    if not self.active:
                        error = self.last_error
            except Exception as exc:
                error = "recording failed: %s" % exc
                self.last_error = error
                self._abort_locked()
                if decoder_called and parsed is None:
                    raise
        if not decoder_called:
            parsed = decoder()
        return parsed, error

    def record_inventory(self, value):
        with self.lock:
            if not self.active:
                return False
            try:
                self._insert_record_locked("inventory", value)
                self.tick()
                return self.active
            except Exception as exc:
                self.last_error = "recording failed: %s" % exc
                self._abort_locked()
                return False

    def _log_locked(self, level, message):
        self._insert_record_locked("events", {"kind": "runtime_event", "observed_at_utc": _utc_now(),
                                              "level": level, "message": message})

    def log(self, level, message):
        with self.lock:
            if not self.active:
                return
            try:
                self._log_locked(level, message)
                self.tick()
            except Exception as exc:
                self.last_error = "recording failed: %s" % exc
                self._abort_locked()

    def stop(self):
        with self.lock:
            if not self.active:
                return self.directory
            if self.last_error:
                self._abort_locked()
                raise RuntimeError(self.last_error)
            try:
                self._log_locked("INFO", "recording stopped")
                self._write_manifest_locked("complete", _utc_now())
                self._connection.commit()
                self._connection.close()
                self._connection = None
                self.active = False
            except Exception as exc:
                self.last_error = "recording finalization failed: %s" % exc
                self._abort_locked()
                raise RuntimeError(self.last_error) from exc
            return self.directory

    def _abort_locked(self):
        """Best-effort failed marker; never leaves an active or open writer."""
        self.active = False
        connection = self._connection
        if connection is None:
            return
        try:
            connection.rollback()
            self.counts = collections.Counter({kind: 0 for kind in COUNT_KINDS})
            self.counts.update(dict(connection.execute("SELECT kind,count(*) FROM records GROUP BY kind")))
            self._write_manifest_locked("failed", _utc_now())
            connection.commit()
        except Exception:
            # On full/corrupt media even the failed marker may be unwritable.
            # The last committed manifest remains recording, never complete.
            pass
        finally:
            try:
                connection.close()
            except Exception:
                pass
            self._connection = None


def _open_reader(path):
    path = Path(path).expanduser().resolve()
    if not path.is_file():
        raise ValueError("missing desktop log: " + str(path))
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        _configure(connection)
        connection.execute("PRAGMA query_only=ON")
        connection.row_factory = sqlite3.Row
        if connection.execute("PRAGMA user_version").fetchone()[0] != FORMAT_VERSION:
            raise ValueError("unsupported desktop log format")
        return connection
    except Exception:
        connection.close()
        raise


def _read_manifest(connection):
    row = connection.execute("SELECT json FROM manifest WHERE id=1").fetchone()
    if row is None:
        raise ValueError("missing desktop log manifest")
    result = json.loads(row[0])
    if not isinstance(result, dict) or result.get("format_version") != FORMAT_VERSION:
        raise ValueError("invalid desktop log manifest")
    return result


def inspect_log(path):
    connection = _open_reader(path)
    try:
        return _read_manifest(connection)
    finally:
        connection.close()


def _record_value(row, metadata_column="metadata_json"):
    metadata = json.loads(row[metadata_column])
    if not isinstance(metadata, dict):
        raise ValueError("record metadata is not a JSON object")
    return {"id": row["id"], "kind": row["kind"], "observed_at_utc": row["observed_at_utc"],
            "stream_key": row["stream_key"], "metadata": metadata}


def read_log_page(path, offset=0, limit=200, kind=None):
    offset, limit = int(offset), int(limit)
    if offset < 0 or limit < 0 or limit > 1000:
        raise ValueError("offset must be nonnegative and limit between 0 and 1000")
    connection = _open_reader(path)
    try:
        where, parameters = ("", []) if kind is None else (" WHERE kind=?", [kind])
        rows = connection.execute(
            "SELECT id,kind,substr(observed_at_utc,1,1024) AS observed_at_utc,"
            "substr(stream_key,1,1024) AS stream_key,summary_json FROM records" + where + " ORDER BY id LIMIT ? OFFSET ?",
            parameters + [limit, offset])
        return [_record_value(row, "summary_json") for row in rows]
    finally:
        connection.close()


def read_log_record(path, id, preview_bytes=2048):
    preview_bytes = int(preview_bytes)
    if not 0 <= preview_bytes <= 1024 * 1024:
        raise ValueError("preview_bytes must be between 0 and 1048576")
    connection = _open_reader(path)
    try:
        row = connection.execute('''
            SELECT r.id,r.kind,r.observed_at_utc,r.stream_key,r.metadata_json,
                   p.sha256,p.length,substr(p.data,1,?) AS preview
            FROM records r LEFT JOIN payloads p ON p.sha256=r.payload_sha256 WHERE r.id=?
        ''', (preview_bytes, int(id))).fetchone()
        if row is None:
            raise KeyError("record not found: " + str(id))
        result = _record_value(row)
        result.update(payload_preview_hex=bytes(row["preview"] or b"").hex(),
                      payload_length=row["length"] or 0, sha256=row["sha256"])
        return result
    finally:
        connection.close()


def verify_log(path):
    connection = _open_reader(path)
    try:
        connection.execute("BEGIN")
        integrity = [row[0] for row in connection.execute("PRAGMA integrity_check")]
        if integrity != ["ok"]:
            raise ValueError("SQLite integrity check failed: " + "; ".join(integrity))
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ValueError("broken payload reference")
        manifest = _read_manifest(connection)
        if manifest.get("status") != "complete" or manifest.get("last_error") is not None:
            raise ValueError("desktop log is not complete and error-free")
        counts = {kind: 0 for kind in COUNT_KINDS}
        for row in connection.execute("SELECT kind,count(*) AS n FROM records GROUP BY kind"):
            if row["kind"] not in counts:
                raise ValueError("unknown record kind")
            counts[row["kind"]] = row["n"]
        if manifest.get("counts") != counts:
            raise ValueError("manifest counts do not match records")
        for row in connection.execute("SELECT rowid,sha256,length,length(data) AS actual_length FROM payloads"):
            digest = row["sha256"]
            if (not isinstance(digest, str) or len(digest) != 64
                    or any(character not in "0123456789abcdef" for character in digest)):
                raise ValueError("invalid payload SHA-256")
            if row["length"] != row["actual_length"]:
                raise ValueError("payload length mismatch")
            hasher = hashlib.sha256()
            # SQL substr(BLOB,...) bounds each allocation and also supports Python
            # 3.10, which predates sqlite3.Connection.blobopen().
            for offset in range(0, row["length"], 1024 * 1024):
                chunk = connection.execute(
                    "SELECT substr(data,?,?) FROM payloads WHERE rowid=?",
                    (offset + 1, 1024 * 1024, row["rowid"])).fetchone()[0]
                hasher.update(chunk)
            if hasher.hexdigest() != digest:
                raise ValueError("payload SHA-256 mismatch")
        for row in connection.execute('''
            SELECT r.id,r.kind,r.observed_at_utc,r.stream_key,r.metadata_json,r.payload_sha256,p.length
            FROM records r LEFT JOIN payloads p ON p.sha256=r.payload_sha256 ORDER BY r.id
        '''):
            value = _record_value(row)["metadata"]
            digest = row["payload_sha256"]
            if digest is not None:
                if value.get("sha256") != digest or value.get("length") != row["length"]:
                    raise ValueError("record payload reference mismatch: %s" % row["id"])
            elif row["kind"] == "udp" or (row["kind"] == "redis" and value.get("present")):
                raise ValueError("missing raw payload reference: %s" % row["id"])
        return manifest
    except (sqlite3.DatabaseError, json.JSONDecodeError) as exc:
        raise ValueError("invalid desktop log: %s" % exc) from exc
    finally:
        connection.close()


def _sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def pack_log(path):
    path = Path(path).expanduser().resolve()
    verify_log(path)
    archive = path.with_suffix(path.suffix + ".zip")
    sidecar = archive.with_name(archive.name + ".sha256")
    if archive.exists() or sidecar.exists():
        raise FileExistsError("archive already exists: " + str(archive))
    temporary = tempfile.NamedTemporaryFile(prefix=archive.name + ".", suffix=".tmp", dir=str(path.parent), delete=False)
    temporary_path = Path(temporary.name)
    temporary.close()
    try:
        before = _sha256_file(path)
        with zipfile.ZipFile(temporary_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as bundle:
            bundle.write(path, path.name)
        if _sha256_file(path) != before:
            raise ValueError("desktop log changed during packing")
        os.replace(temporary_path, archive)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    sidecar.write_text("%s  %s\n" % (_sha256_file(archive), archive.name), encoding="utf-8")
    return archive
