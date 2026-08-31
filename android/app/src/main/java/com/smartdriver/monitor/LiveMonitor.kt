package com.smartdriver.monitor

import android.content.Context
import android.net.wifi.WifiManager
import android.os.SystemClock
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.net.DatagramPacket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.MulticastSocket
import java.net.NetworkInterface
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

internal const val DEFAULT_REDIS_HOST = "192.168.2.112"
internal const val DEFAULT_REDIS_PORT = 6379
internal const val DEFAULT_REDIS_DATABASE = 0
internal const val DEFAULT_UDP_GROUP = "239.255.76.21"
internal const val DEFAULT_UDP_PORT = 7621
internal const val DEFAULT_UDP_INTERFACE = "auto"

data class MonitorConfig(
    val redisHost: String = DEFAULT_REDIS_HOST,
    val redisPort: Int = DEFAULT_REDIS_PORT,
    val redisDatabase: Int = DEFAULT_REDIS_DATABASE,
    val udpGroup: String = DEFAULT_UDP_GROUP,
    val udpPort: Int = DEFAULT_UDP_PORT,
    val udpInterface: String = DEFAULT_UDP_INTERFACE
) {
    init {
        require(redisHost.isNotBlank()) { "Redis 地址不能为空" }
        require(redisPort in 1..65535) { "Redis 端口必须在 1..65535" }
        require(redisDatabase >= 0) { "Redis DB 不能为负数" }
        require(udpPort in 1..65535) { "UDP 端口必须在 1..65535" }
        val group = InetAddress.getByName(udpGroup)
        require(group is Inet4Address && group.isMulticastAddress) { "UDP 地址必须是 IPv4 组播地址" }
        if (!udpInterface.equals("auto", ignoreCase = true)) {
            require(InetAddress.getByName(udpInterface) is Inet4Address) { "UDP 网卡地址必须是 IPv4 或 auto" }
        }
    }
}

enum class MonitorSource { REDIS, UDP }

sealed interface MonitorEvent

data class SourceStatus(
    val source: MonitorSource,
    val state: String,
    val detail: String
) : MonitorEvent

data class RedisObservation(
    val key: String,
    val present: Boolean?,
    val error: String?,
    val payload: ByteArray?,
    val sha256: String?,
    val changed: Boolean,
    val observedAtMs: Long,
    val changedAtElapsedMs: Long,
    val parse: ParseResult
)

data class RedisBatch(
    val sequence: Long,
    val observations: List<RedisObservation>,
    val roundTripMs: Double,
    val readMethod: String
) : MonitorEvent

data class UdpObservation(
    val sequence: Long,
    val observedAtMs: Long,
    val sourceAddress: String,
    val sourcePort: Int,
    val interArrivalMs: Double?,
    val payload: ByteArray,
    val sha256: String,
    val wireHint: String,
    val duplicate: Boolean,
    val outOfOrder: Boolean,
    val packet: UdpPacketResult
) : MonitorEvent

data class MonitorLog(val level: String, val message: String) : MonitorEvent

class LiveMonitor(
    context: Context,
    private val config: MonitorConfig,
    private val emit: (MonitorEvent) -> Unit
) {
    private val appContext = context.applicationContext
    private val running = AtomicBoolean(false)
    private var redisThread: Thread? = null
    private var udpThread: Thread? = null

    @Volatile private var redisSession: RedisSession? = null
    @Volatile private var udpSocket: MulticastSocket? = null
    @Volatile private var multicastLock: WifiManager.MulticastLock? = null

    fun start() {
        if (!running.compareAndSet(false, true)) return
        redisThread = thread(name = "SmartDriver Redis", isDaemon = true) { runRedis() }
        udpThread = thread(name = "SmartDriver UDP", isDaemon = true) { runUdp() }
    }

    fun stop() {
        if (!running.compareAndSet(true, false)) return
        runCatching { redisSession?.close() }
        runCatching { udpSocket?.close() }
        runCatching { multicastLock?.release() }
        redisThread?.interrupt()
        udpThread?.interrupt()
        redisThread?.join(750)
        udpThread?.join(750)
        redisThread = null
        udpThread = null
        emit(SourceStatus(MonitorSource.REDIS, "stopped", "前台监视已停止"))
        emit(SourceStatus(MonitorSource.UDP, "stopped", "前台监视已停止"))
    }

    private fun runRedis() {
        val previousHashes = mutableMapOf<String, String?>()
        val changedAt = mutableMapOf<String, Long>()
        var sequence = 0L
        var useMget = true
        while (running.get()) {
            val endpoint = "${config.redisHost}:${config.redisPort}/db${config.redisDatabase}"
            emit(SourceStatus(MonitorSource.REDIS, "connecting", endpoint))
            try {
                RedisSession(config).use { session ->
                    redisSession = session
                    emit(SourceStatus(MonitorSource.REDIS, "connected", endpoint))
                    emit(MonitorLog("INFO", "Redis 已连接：$endpoint"))
                    while (running.get()) {
                        val pollStarted = System.nanoTime()
                        val reads = if (useMget) {
                            try {
                                session.mget(SmartDriverProtocol.redisKeys)
                                    .map { RedisRead(it, null) }
                            } catch (error: RedisCommandException) {
                                val detail = error.message.orEmpty().lowercase()
                                if ("noperm" !in detail && "unknown command" !in detail) throw error
                                useMget = false
                                emit(MonitorLog("WARN", "Redis MGET 不可用，改用逐键 GET：${error.message}"))
                                readIndividually(session)
                            }
                        } else {
                            readIndividually(session)
                        }
                        val nowMs = System.currentTimeMillis()
                        val nowElapsed = SystemClock.elapsedRealtime()
                        val observations = SmartDriverProtocol.redisKeys.zip(reads).map { (key, read) ->
                            if (read.error != null) {
                                RedisObservation(
                                    key = key,
                                    present = null,
                                    error = read.error,
                                    payload = null,
                                    sha256 = null,
                                    changed = false,
                                    observedAtMs = nowMs,
                                    changedAtElapsedMs = changedAt[key] ?: nowElapsed,
                                    parse = ParseResult("read_error", read.error, read.error)
                                )
                            } else {
                                val digest = read.payload?.let(SmartDriverProtocol::sha256)
                                val known = previousHashes.containsKey(key)
                                val changed = known && previousHashes[key] != digest
                                previousHashes[key] = digest
                                if (!known || changed) changedAt[key] = nowElapsed
                                RedisObservation(
                                    key = key,
                                    present = read.payload != null,
                                    error = null,
                                    payload = read.payload,
                                    sha256 = digest,
                                    changed = changed,
                                    observedAtMs = nowMs,
                                    changedAtElapsedMs = changedAt.getValue(key),
                                    parse = read.payload?.let { SmartDriverProtocol.inspectRedis(key, it) }
                                        ?: ParseResult("missing", "键不存在", "Redis 返回 nil")
                                )
                            }
                        }
                        sequence += 1
                        val elapsedMs = (System.nanoTime() - pollStarted) / 1_000_000.0
                        emit(RedisBatch(sequence, observations, elapsedMs, if (useMget) "MGET" else "GET"))
                        sleepRemaining(100L - elapsedMs.toLong())
                    }
                }
            } catch (throwable: Exception) {
                if (running.get() && throwable !is InterruptedException) {
                    val detail = throwable.message ?: throwable::class.java.simpleName
                    emit(SourceStatus(MonitorSource.REDIS, "error", "$endpoint · $detail"))
                    emit(MonitorLog("WARN", "Redis 连接中断：$detail"))
                    sleepRemaining(2_000)
                }
            } finally {
                redisSession = null
            }
        }
    }

    private fun readIndividually(session: RedisSession): List<RedisRead> =
        SmartDriverProtocol.redisKeys.map { key ->
            try {
                RedisRead(session.get(key), null)
            } catch (error: RedisCommandException) {
                RedisRead(null, error.message ?: "Redis GET failed")
            }
        }

    private fun runUdp() {
        var sequence = 0L
        var previousArrivalNs: Long? = null
        val recentKeys = linkedSetOf<String>()
        val lastTimestampByVehicle = mutableMapOf<Int, Long>()
        val endpoint = "${config.udpGroup}:${config.udpPort}"

        while (running.get()) {
            emit(SourceStatus(MonitorSource.UDP, "connecting", endpoint))
            var localLock: WifiManager.MulticastLock? = null
            try {
                localLock = acquireMulticastLock()
                multicastLock = localLock
                val (socket, joined, errors) = openMulticastSocket()
                udpSocket = socket
                val detail = buildString {
                    append(endpoint).append(" · joined=").append(joined.joinToString().ifEmpty { "default" })
                    if (errors.isNotEmpty()) append(" · join-errors=").append(errors.joinToString(" | "))
                }
                emit(SourceStatus(MonitorSource.UDP, "listening", detail))
                emit(MonitorLog("INFO", "UDP 正在监听：$detail"))

                val buffer = ByteArray(65_535)
                val datagram = DatagramPacket(buffer, buffer.size)
                while (running.get()) {
                    datagram.length = buffer.size
                    try {
                        socket.receive(datagram)
                    } catch (timeout: java.net.SocketTimeoutException) {
                        continue
                    }
                    val arrivedNs = System.nanoTime()
                    val payload = datagram.data.copyOfRange(datagram.offset, datagram.offset + datagram.length)
                    val digest = SmartDriverProtocol.sha256(payload)
                    val parsed = SmartDriverProtocol.parseUdp(payload)
                    val messageKey = if (
                        parsed.vehicleId != null && listOf(
                            parsed.vehicleId,
                            parsed.sequence,
                            parsed.stampSeconds,
                            parsed.stampNanoseconds
                        ).any { it != null && it != 0 }
                    ) {
                        "m:${parsed.vehicleId}:${parsed.sequence}:${parsed.stampSeconds}:${parsed.stampNanoseconds}"
                    } else {
                        "p:$digest"
                    }
                    val duplicate = messageKey in recentKeys
                    if (!duplicate) {
                        recentKeys += messageKey
                        // ponytail: UI-only duplicate history is bounded; raise this only for longer diagnostic windows.
                        if (recentKeys.size > 1_024) recentKeys.remove(recentKeys.first())
                    }
                    val timestamp = if (parsed.stampSeconds != null && parsed.stampNanoseconds != null) {
                        parsed.stampSeconds.toLong() * 1_000_000_000L + parsed.stampNanoseconds
                    } else {
                        0L
                    }
                    val outOfOrder = if (timestamp != 0L && parsed.vehicleId != null) {
                        val previous = lastTimestampByVehicle[parsed.vehicleId]
                        if (previous == null || timestamp >= previous) lastTimestampByVehicle[parsed.vehicleId] = timestamp
                        previous != null && timestamp < previous
                    } else {
                        false
                    }
                    sequence += 1
                    val interval = previousArrivalNs?.let { (arrivedNs - it) / 1_000_000.0 }
                    previousArrivalNs = arrivedNs
                    emit(
                        UdpObservation(
                            sequence = sequence,
                            observedAtMs = System.currentTimeMillis(),
                            sourceAddress = datagram.address.hostAddress ?: datagram.address.toString(),
                            sourcePort = datagram.port,
                            interArrivalMs = interval,
                            payload = payload,
                            sha256 = digest,
                            wireHint = SmartDriverProtocol.udpWireHint(payload),
                            duplicate = duplicate,
                            outOfOrder = outOfOrder,
                            packet = parsed
                        )
                    )
                }
            } catch (throwable: Exception) {
                if (running.get() && throwable !is InterruptedException) {
                    val detail = throwable.message ?: throwable::class.java.simpleName
                    emit(SourceStatus(MonitorSource.UDP, "error", "$endpoint · $detail"))
                    emit(MonitorLog("WARN", "UDP 监听中断：$detail"))
                    sleepRemaining(1_000)
                }
            } finally {
                runCatching { udpSocket?.close() }
                udpSocket = null
                runCatching { localLock?.release() }
                multicastLock = null
            }
        }
    }

    private fun acquireMulticastLock(): WifiManager.MulticastLock? {
        val wifi = appContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager ?: return null
        return wifi.createMulticastLock("smartdriver-live-monitor").apply {
            setReferenceCounted(false)
            acquire()
        }
    }

    private fun openMulticastSocket(): MulticastJoin {
        val group = InetAddress.getByName(config.udpGroup)
        val socket = MulticastSocket(null)
        try {
            socket.reuseAddress = true
            socket.receiveBufferSize = 4 * 1024 * 1024
            socket.soTimeout = 500
            socket.bind(InetSocketAddress(config.udpPort))
            val joined = mutableListOf<String>()
            val errors = mutableListOf<String>()
            for (networkInterface in multicastInterfaces()) {
                try {
                    socket.joinGroup(InetSocketAddress(group, config.udpPort), networkInterface)
                    joined += networkInterface.name
                } catch (throwable: Exception) {
                    errors += "${networkInterface.name}: ${throwable.message}"
                }
            }
            if (joined.isEmpty()) {
                try {
                    @Suppress("DEPRECATION")
                    socket.joinGroup(group)
                } catch (throwable: Exception) {
                    throw IOException("无法加入组播；${(errors + throwable.message.orEmpty()).joinToString(" | ")}")
                }
            }
            return MulticastJoin(socket, joined, errors)
        } catch (throwable: Exception) {
            socket.close()
            throw throwable
        }
    }

    private fun multicastInterfaces(): List<NetworkInterface> {
        val requested = config.udpInterface.takeUnless { it.equals("auto", ignoreCase = true) }
        return Collections.list(NetworkInterface.getNetworkInterfaces())
            .filter { networkInterface ->
                runCatching { networkInterface.isUp && !networkInterface.isLoopback }.getOrDefault(false) &&
                    Collections.list(networkInterface.inetAddresses).any { address ->
                        address is Inet4Address && (requested == null || address.hostAddress == requested)
                    }
            }
            .sortedBy { if (it.name.startsWith("wlan")) 0 else 1 }
            .also { if (requested != null && it.isEmpty()) error("未找到 IPv4 网卡 $requested") }
    }

    private fun sleepRemaining(milliseconds: Long) {
        if (milliseconds <= 0 || !running.get()) return
        try {
            Thread.sleep(milliseconds)
        } catch (_: InterruptedException) {
            // stop() interrupts both workers.
        }
    }
}

private data class RedisRead(val payload: ByteArray?, val error: String?)
private data class MulticastJoin(
    val socket: MulticastSocket,
    val joined: List<String>,
    val errors: List<String>
)

internal class RedisCommandException(message: String) : IOException(message)

internal class RedisSession(config: MonitorConfig) : AutoCloseable {
    private val socket = Socket()
    private val input: BufferedInputStream
    private val output: BufferedOutputStream

    init {
        try {
            socket.connect(InetSocketAddress(config.redisHost, config.redisPort), 2_000)
            socket.soTimeout = 2_000
            input = BufferedInputStream(socket.getInputStream())
            output = BufferedOutputStream(socket.getOutputStream())
            if (config.redisDatabase != 0) {
                val response = command("SELECT", config.redisDatabase.toString())
                if (response != "OK") throw IOException("Redis SELECT failed: $response")
            }
        } catch (throwable: Exception) {
            socket.close()
            throw throwable
        }
    }

    fun get(key: String): ByteArray? = when (val response = command("GET", key)) {
        null -> null
        is ByteArray -> response
        is String -> response.toByteArray(StandardCharsets.UTF_8)
        else -> throw IOException("unexpected Redis GET reply: ${response::class.java.simpleName}")
    }

    fun mget(keys: List<String>): List<ByteArray?> {
        val response = command(*(listOf("MGET") + keys).toTypedArray())
        if (response !is List<*> || response.size != keys.size) {
            throw IOException("Redis MGET value count mismatch")
        }
        return response.map { value ->
            when (value) {
                null -> null
                is ByteArray -> value
                is String -> value.toByteArray(StandardCharsets.UTF_8)
                else -> throw IOException("unexpected Redis MGET item: ${value::class.java.simpleName}")
            }
        }
    }

    private fun command(vararg parts: String): Any? {
        output.write("*${parts.size}\r\n".toByteArray(StandardCharsets.UTF_8))
        parts.forEach { part ->
            val bytes = part.toByteArray(StandardCharsets.UTF_8)
            output.write("$${bytes.size}\r\n".toByteArray(StandardCharsets.UTF_8))
            output.write(bytes)
            output.write("\r\n".toByteArray(StandardCharsets.UTF_8))
        }
        output.flush()
        return readReply()
    }

    private fun readReply(): Any? = when (val marker = input.read()) {
        -1 -> throw IOException("Redis connection closed")
        '+'.code -> readLine()
        '-'.code -> throw RedisCommandException(readLine())
        ':'.code -> readLine().toLong()
        '$'.code -> readBulk()
        '*'.code -> {
            val count = readLine().toInt()
            if (count < 0) null else {
                if (count > 10_000) throw IOException("Redis array too large: $count")
                List(count) { readReply() }
            }
        }
        else -> throw IOException("unsupported Redis RESP marker: $marker")
    }

    private fun readBulk(): ByteArray? {
        val length = readLine().toInt()
        if (length < 0) return null
        // ponytail: an 8 MiB value is already too large for a phone monitor; raise only for measured payloads.
        if (length > 8 * 1024 * 1024) throw IOException("Redis payload too large: $length")
        val bytes = input.readExactly(length)
        if (!input.readExactly(2).contentEquals(byteArrayOf('\r'.code.toByte(), '\n'.code.toByte()))) {
            throw IOException("malformed Redis bulk ending")
        }
        return bytes
    }

    private fun readLine(): String {
        val bytes = ByteArrayOutputStream()
        while (true) {
            val value = input.read()
            if (value < 0) throw IOException("Redis connection closed while reading line")
            if (value == '\r'.code) {
                if (input.read() != '\n'.code) throw IOException("malformed Redis line ending")
                return bytes.toString(StandardCharsets.UTF_8.name())
            }
            bytes.write(value)
        }
    }

    override fun close() {
        runCatching { socket.close() }
    }
}

private fun BufferedInputStream.readExactly(length: Int): ByteArray {
    val bytes = ByteArray(length)
    var offset = 0
    while (offset < length) {
        val read = read(bytes, offset, length - offset)
        if (read < 0) throw IOException("Redis payload truncated")
        offset += read
    }
    return bytes
}
