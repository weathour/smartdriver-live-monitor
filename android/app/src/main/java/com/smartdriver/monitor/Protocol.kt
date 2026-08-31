package com.smartdriver.monitor

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

data class ParseResult(
    val status: String,
    val summary: String,
    val detail: String
)

data class UdpPacketResult(
    val parse: ParseResult,
    val sequence: Int? = null,
    val stampSeconds: Int? = null,
    val stampNanoseconds: Int? = null,
    val frameId: String? = null,
    val vehicleId: Int? = null,
    val vehicleNumber: String? = null,
    val autoLevel: String? = null,
    val speedMps: Double? = null,
    val collaborativeCount: Int? = null,
    val trajectoryCount: Int? = null,
    val drivingIntention: Int? = null,
    val guideDecision: Int? = null,
    val coordinateBehavior: Int? = null,
    val trailingBytes: Int = 0
)

object SmartDriverProtocol {
    private const val MAX_COLLABORATIVE_VEHICLES = 64
    private const val MAX_TRAJECTORY_POINTS = 256

    val redisKeys = listOf(
        "Sensor_Location",
        "Sensor_Mmobstacles",
        "Sensor_TrafficLightlist",
        "Sensor_Lanelist",
        "Sensor_SAM",
        "Sensor_CVP"
    )

    private val knownTopLevelFields = mapOf(
        "Sensor_Location" to (1..25).toSet(),
        "Sensor_Mmobstacles" to setOf(1, 2, 3),
        "Sensor_TrafficLightlist" to setOf(1, 2, 3),
        "Sensor_Lanelist" to setOf(1, 2, 3),
        "Sensor_SAM" to (1..21).toSet(),
        "Sensor_CVP" to setOf(1, 2)
    )

    fun inspectRedis(key: String, payload: ByteArray): ParseResult = try {
        val message = ProtoReader(payload).readMessage()
        val summary = when (key) {
            "Sensor_Location" -> locationSummary(message)
            "Sensor_Mmobstacles" -> "障碍物 ${message.int(2) ?: "—"} 个（实际条目 ${message.count(3)}）"
            "Sensor_TrafficLightlist" -> trafficLightSummary(message)
            "Sensor_Lanelist" -> "车道线 ${message.int(2) ?: "—"} 条（实际条目 ${message.count(3)}）"
            "Sensor_SAM" -> samSummary(message)
            "Sensor_CVP" -> collaborativePoseSummary(message)
            else -> "未知 Redis schema"
        }
        val expected = knownTopLevelFields[key].orEmpty()
        val unknown = message.fieldNumbers - expected
        val status = if (unknown.isEmpty()) "ok" else "ok_with_unknown_fields"
        ParseResult(
            status = status,
            summary = summary,
            detail = buildString {
                append(summary)
                if (unknown.isNotEmpty()) append("\n未知顶层字段：${unknown.sorted().joinToString()}")
                append("\n\n")
                append(message.describe())
            }
        )
    } catch (throwable: Exception) {
        ParseResult(
            status = "parse_error",
            summary = throwable.message ?: throwable::class.java.simpleName,
            detail = throwable.message ?: throwable::class.java.simpleName
        )
    }

    fun parseUdp(data: ByteArray): UdpPacketResult {
        val reader = PacketReader(data)
        return try {
            val sequence = reader.int32()
            val seconds = reader.int32()
            val nanoseconds = reader.int32()
            val frameId = reader.fixedString(32, "header.frame_id")
            val vehicleId = reader.int32()
            val vehicleNumber = reader.fixedString(32, "vehicle_num")
            val autoLevel = reader.fixedString(32, "auto_level")
            val drivingMode = reader.int32()
            val gear = reader.int32()
            val steering = reader.double("steering_value_fd")
            val acceleration = reader.double("accelaration_cmd")
            val speed = reader.double("speed_mps")
            repeat(3) { reader.double("position[$it]") }
            repeat(3) { reader.double("utm_position[$it]") }
            val pitch = reader.double("pitch")
            val roll = reader.double("roll")
            val heading = reader.double("heading")
            if (nanoseconds !in 0 until 1_000_000_000) {
                reader.issues += "header.stamp.nsec 超出 [0, 1000000000)"
            }

            val collaborativeCount = reader.int32()
            if (
                collaborativeCount < 0 ||
                collaborativeCount > MAX_COLLABORATIVE_VEHICLES ||
                collaborativeCount > maxOf(0, (reader.remaining - 24) / 8)
            ) {
                error("协作车辆数量 $collaborativeCount 超出协议边界")
            }
            repeat(collaborativeCount) {
                reader.int32()
                reader.int32()
            }

            val trajectoryCount = reader.int32()
            if (
                trajectoryCount < 0 ||
                trajectoryCount > MAX_TRAJECTORY_POINTS ||
                trajectoryCount > maxOf(0, (reader.remaining - 20) / 141)
            ) {
                error("轨迹点数量 $trajectoryCount 超出协议边界")
            }
            repeat(trajectoryCount) {
                reader.skip(120)
                val steerValid = reader.byte()
                if (steerValid !in 0..1) reader.issues += "trajectory[$it].is_steer_valid=$steerValid"
                reader.skip(20)
            }

            val intention = reader.int32()
            val reason = reader.int32()
            val decision = reader.int32()
            val feedback = reader.int32()
            val behavior = reader.int32()
            val trailing = reader.remaining
            val status = when {
                trailing > 0 -> "ok_with_trailing"
                reader.issues.isNotEmpty() -> "ok_with_issues"
                else -> "ok"
            }
            val summary = "VID $vehicleId · 车速 ${format(speed)} m/s · 意图 $intention · 决策 $decision · 协作车 $collaborativeCount · 轨迹点 $trajectoryCount"
            val detail = buildString {
                appendLine("header: seq=$sequence stamp=$seconds.$nanoseconds frame_id=$frameId")
                appendLine("vehicle: vid=$vehicleId number=$vehicleNumber auto_level=$autoLevel")
                appendLine("state: mode=$drivingMode gear=$gear steering=${format(steering)} acceleration=${format(acceleration)} speed_mps=${format(speed)}")
                appendLine("attitude: pitch=${format(pitch)} roll=${format(roll)} heading=${format(heading)}")
                appendLine("lists: collaborative=$collaborativeCount trajectory=$trajectoryCount")
                appendLine("decision: intention=$intention reason=$reason decision=$decision feedback=$feedback behavior=$behavior")
                append("consumed=${reader.offset} trailing=$trailing")
                if (reader.issues.isNotEmpty()) append("\nissues: ${reader.issues.joinToString("; ")}")
            }
            UdpPacketResult(
                parse = ParseResult(status, summary, detail),
                sequence = sequence,
                stampSeconds = seconds,
                stampNanoseconds = nanoseconds,
                frameId = frameId,
                vehicleId = vehicleId,
                vehicleNumber = vehicleNumber,
                autoLevel = autoLevel,
                speedMps = speed,
                collaborativeCount = collaborativeCount,
                trajectoryCount = trajectoryCount,
                drivingIntention = intention,
                guideDecision = decision,
                coordinateBehavior = behavior,
                trailingBytes = trailing
            )
        } catch (throwable: Exception) {
            UdpPacketResult(
                parse = ParseResult(
                    status = "parse_error",
                    summary = throwable.message ?: throwable::class.java.simpleName,
                    detail = "consumed=${reader.offset} remaining=${reader.remaining}\n${throwable.message ?: throwable::class.java.simpleName}"
                ),
                trailingBytes = reader.remaining
            )
        }
    }

    fun udpWireHint(data: ByteArray): String = when {
        data.size >= 4 && data.copyOfRange(0, 4).contentEquals("LC02".toByteArray()) -> "lcm-short-message"
        data.size >= 4 && data.copyOfRange(0, 4).contentEquals("LC03".toByteArray()) -> "lcm-fragment"
        data.size < 4 -> "short"
        else -> "direct-or-unknown"
    }

    fun sha256(data: ByteArray): String = MessageDigest.getInstance("SHA-256")
        .digest(data)
        .toHex()

    fun hexPreview(data: ByteArray, limit: Int = 4096): String {
        val shown = data.copyOf(minOf(data.size, limit)).toHex(grouped = true)
        return if (data.size <= limit) shown else "$shown\n… 仅显示前 $limit / ${data.size} 字节"
    }

    private fun locationSummary(message: ProtoMessage): String =
        "经度 ${format(message.double(2))} · 纬度 ${format(message.double(3))} · 航向 ${format(message.double(7))} · 速度 ${format(message.double(8))} m/s"

    private fun trafficLightSummary(message: ProtoMessage): String {
        val lights = message.messages(3)
        val descriptions = lights.take(5).map {
            "颜色 ${it.int(1) ?: "—"}/剩余 ${format(it.double(8))}s"
        }
        return "交通灯 ${message.int(2) ?: "—"} 盏（实际条目 ${lights.size}）" +
            if (descriptions.isEmpty()) "" else " · ${descriptions.joinToString()}"
    }

    private fun samSummary(message: ProtoMessage): String =
        "场景 ${message.int(2) ?: "—"} · VID ${message.int(3) ?: "—"} · 车速 ${format(message.double(10))} m/s · 意图 ${message.int(13) ?: "—"} · 决策 ${message.int(15) ?: "—"}"

    private fun collaborativePoseSummary(message: ProtoMessage): String {
        val vehicles = message.messages(2)
        val ids = vehicles.take(8).mapNotNull { it.int(1) }
        return "发送时间 ${message.unsigned(1) ?: "—"} ms · 协作车辆 ${vehicles.size} 辆" +
            if (ids.isEmpty()) "" else " · VID ${ids.joinToString()}"
    }

    private fun format(value: Double?): String = when {
        value == null -> "—"
        !value.isFinite() -> value.toString()
        else -> String.format(Locale.US, "%.6f", value).trimEnd('0').trimEnd('.')
    }
}

private sealed interface ProtoValue {
    data class Varint(val value: Long) : ProtoValue
    data class Fixed64(val value: Long) : ProtoValue
    data class Bytes(val value: ByteArray) : ProtoValue
    data class Fixed32(val value: Int) : ProtoValue
}

private class ProtoMessage(private val fields: Map<Int, List<ProtoValue>>) {
    val fieldNumbers: Set<Int> get() = fields.keys

    fun count(number: Int): Int = fields[number].orEmpty().size

    fun int(number: Int): Int? = (fields[number]?.firstOrNull() as? ProtoValue.Varint)?.value?.toInt()

    fun unsigned(number: Int): String? = (fields[number]?.firstOrNull() as? ProtoValue.Varint)
        ?.value
        ?.let(java.lang.Long::toUnsignedString)

    fun double(number: Int): Double? = (fields[number]?.firstOrNull() as? ProtoValue.Fixed64)
        ?.value
        ?.let(Double::fromBits)

    fun messages(number: Int): List<ProtoMessage> = fields[number]
        .orEmpty()
        .mapNotNull { (it as? ProtoValue.Bytes)?.value }
        .map { ProtoReader(it).readMessage() }

    fun describe(): String = buildString {
        var shown = 0
        fieldLoop@ for ((number, values) in fields.toSortedMap()) {
            for ((index, value) in values.withIndex()) {
                if (shown++ >= 128) {
                    append("… 字段显示已截断")
                    break@fieldLoop
                }
                append("field ").append(number)
                if (values.size > 1) append('[').append(index).append(']')
                append(" = ")
                append(
                    when (value) {
                        is ProtoValue.Varint -> java.lang.Long.toUnsignedString(value.value)
                        is ProtoValue.Fixed64 -> "${Double.fromBits(value.value)} (fixed64=0x${value.value.toULong().toString(16)})"
                        is ProtoValue.Fixed32 -> "${value.value.toUInt()} (fixed32=0x${value.value.toUInt().toString(16)})"
                        is ProtoValue.Bytes -> bytesDescription(value.value)
                    }
                )
                appendLine()
            }
        }
    }.trimEnd()

    private fun bytesDescription(bytes: ByteArray): String {
        val text = bytes.toString(StandardCharsets.UTF_8)
            .takeIf { candidate -> candidate.isNotEmpty() && candidate.all { !it.isISOControl() } }
        return if (text != null && text.length <= 80) {
            "len=${bytes.size} text=$text"
        } else {
            "len=${bytes.size} hex=${bytes.copyOf(minOf(24, bytes.size)).toHex()}${if (bytes.size > 24) "…" else ""}"
        }
    }
}

private class ProtoReader(private val data: ByteArray) {
    private var offset = 0

    fun readMessage(): ProtoMessage {
        val fields = linkedMapOf<Int, MutableList<ProtoValue>>()
        var count = 0
        while (offset < data.size) {
            if (++count > 10_000) error("protobuf 字段数超过 10000")
            val tagOffset = offset
            val tag = readVarint()
            val number = (tag ushr 3).toInt()
            val wireType = (tag and 7).toInt()
            if (number !in 1..536_870_911) error("invalid protobuf field $number at byte $tagOffset")
            val value = when (wireType) {
                0 -> ProtoValue.Varint(readVarint())
                1 -> ProtoValue.Fixed64(readLongLittleEndian())
                2 -> {
                    val length = readVarint()
                    if (length < 0 || length > Int.MAX_VALUE || length > data.size - offset) {
                        error("protobuf length $length exceeds remaining ${data.size - offset}")
                    }
                    ProtoValue.Bytes(readBytes(length.toInt()))
                }
                5 -> ProtoValue.Fixed32(readIntLittleEndian())
                else -> error("unsupported protobuf wire type $wireType at byte $tagOffset")
            }
            fields.getOrPut(number) { mutableListOf() } += value
        }
        return ProtoMessage(fields)
    }

    private fun readVarint(): Long {
        var value = 0L
        for (shift in 0..63 step 7) {
            val byte = readByte()
            if (shift == 63 && byte and 0xFE != 0) error("varint exceeds 64 bits")
            value = value or ((byte and 0x7F).toLong() shl shift)
            if (byte and 0x80 == 0) return value
        }
        error("varint exceeds 10 bytes")
    }

    private fun readLongLittleEndian(): Long {
        val bytes = readBytes(8)
        var value = 0L
        for (index in bytes.indices) value = value or ((bytes[index].toLong() and 0xFF) shl (index * 8))
        return value
    }

    private fun readIntLittleEndian(): Int {
        val bytes = readBytes(4)
        var value = 0
        for (index in bytes.indices) value = value or ((bytes[index].toInt() and 0xFF) shl (index * 8))
        return value
    }

    private fun readBytes(length: Int): ByteArray {
        if (length < 0 || offset + length > data.size) error("protobuf truncated at byte $offset")
        return data.copyOfRange(offset, offset + length).also { offset += length }
    }

    private fun readByte(): Int {
        if (offset >= data.size) error("protobuf truncated at byte $offset")
        return data[offset++].toInt() and 0xFF
    }
}

private class PacketReader(private val data: ByteArray) {
    var offset: Int = 0
        private set
    val remaining: Int get() = data.size - offset
    val issues = mutableListOf<String>()

    fun byte(): Int = take(1)[0].toInt() and 0xFF

    fun int32(): Int {
        val bytes = take(4)
        return (bytes[0].toInt() and 0xFF) or
            ((bytes[1].toInt() and 0xFF) shl 8) or
            ((bytes[2].toInt() and 0xFF) shl 16) or
            ((bytes[3].toInt() and 0xFF) shl 24)
    }

    fun double(field: String): Double {
        val bytes = take(8)
        var bits = 0L
        for (index in bytes.indices) bits = bits or ((bytes[index].toLong() and 0xFF) shl (index * 8))
        return Double.fromBits(bits).also { if (!it.isFinite()) issues += "$field=$it" }
    }

    fun fixedString(length: Int, field: String): String {
        val bytes = take(length)
        val end = bytes.indexOf(0).let { if (it < 0) bytes.size else it }
        val text = bytes.copyOf(end).toString(StandardCharsets.UTF_8).trimEnd()
        if ('\uFFFD' in text) issues += "$field contains invalid UTF-8"
        return text
    }

    fun skip(length: Int) {
        take(length)
    }

    private fun take(length: Int): ByteArray {
        if (length < 0 || offset + length > data.size) {
            error("UDP packet truncated at byte $offset (need $length)")
        }
        return data.copyOfRange(offset, offset + length).also { offset += length }
    }
}

private fun ByteArray.toHex(grouped: Boolean = false): String {
    val alphabet = "0123456789abcdef"
    val separatorCount = if (grouped && isNotEmpty()) (size - 1) / 32 else 0
    val out = StringBuilder(size * 2 + separatorCount)
    forEachIndexed { index, byte ->
        if (grouped && index > 0 && index % 32 == 0) out.append('\n')
        val value = byte.toInt() and 0xFF
        out.append(alphabet[value ushr 4]).append(alphabet[value and 0x0F])
    }
    return out.toString()
}
