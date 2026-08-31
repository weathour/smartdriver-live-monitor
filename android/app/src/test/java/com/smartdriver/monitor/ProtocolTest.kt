package com.smartdriver.monitor

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.net.ServerSocket
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kotlin.concurrent.thread

class ProtocolTest {
    @Test
    fun defaultsMatchUnityFieldEndpoints() {
        val config = MonitorConfig()
        assertEquals("192.168.2.112", config.redisHost)
        assertEquals(6379, config.redisPort)
        assertEquals(0, config.redisDatabase)
        assertEquals("239.255.76.21", config.udpGroup)
        assertEquals(7621, config.udpPort)
    }

    @Test
    fun parsesRedisLocationAndDirectUdpPacket() {
        val location = ByteArrayOutputStream().apply {
            write(fixed64Field(2, 106.5516))
            write(fixed64Field(3, 29.5630))
            write(fixed64Field(8, 12.5))
        }.toByteArray()
        val redis = SmartDriverProtocol.inspectRedis("Sensor_Location", location)
        assertEquals("ok", redis.status)
        assertTrue(redis.summary.contains("106.5516"))
        assertTrue(redis.summary.contains("12.5"))

        val udp = SmartDriverProtocol.parseUdp(directUdpPacket() + byteArrayOf(1, 2))
        assertEquals("ok_with_trailing", udp.parse.status)
        assertEquals(42, udp.vehicleId)
        assertEquals(2, udp.trailingBytes)
        assertTrue(udp.parse.summary.contains("车速 8.5"))
    }

    @Test
    fun rejectsImpossibleUdpListCount() {
        val packet = directUdpPacket()
        ByteBuffer.wrap(packet).order(ByteOrder.LITTLE_ENDIAN).putInt(216, 10_000)
        assertEquals("parse_error", SmartDriverProtocol.parseUdp(packet).parse.status)

        val tooManyVehicles = packet.copyOfRange(0, 220) + ByteArray(65 * 8) + packet.copyOfRange(220, packet.size)
        ByteBuffer.wrap(tooManyVehicles).order(ByteOrder.LITTLE_ENDIAN).putInt(216, 65)
        assertEquals("parse_error", SmartDriverProtocol.parseUdp(tooManyVehicles).parse.status)

        val tooManyPoints = packet.copyOfRange(0, 224) + ByteArray(257 * 141) + packet.copyOfRange(224, packet.size)
        ByteBuffer.wrap(tooManyPoints).order(ByteOrder.LITTLE_ENDIAN).putInt(220, 257)
        assertEquals("parse_error", SmartDriverProtocol.parseUdp(tooManyPoints).parse.status)
    }

    @Test
    fun readsBinaryMgetReplyWithoutRedisSdk() {
        ServerSocket(0).use { server ->
            var requestLines = emptyList<String>()
            val worker = thread(isDaemon = true) {
                server.accept().use { socket ->
                    requestLines = socket.getInputStream().bufferedReader().let { reader ->
                        List(7) { reader.readLine() }
                    }
                    socket.getOutputStream().apply {
                        write("*2\r\n\$3\r\nabc\r\n\$-1\r\n".toByteArray())
                        flush()
                    }
                }
            }
            RedisSession(MonitorConfig(redisHost = "127.0.0.1", redisPort = server.localPort)).use { redis ->
                val values = redis.mget(listOf("first", "second"))
                assertArrayEquals("abc".toByteArray(), values[0])
                assertNull(values[1])
            }
            worker.join(1_000)
            assertEquals(
                listOf("*3", "\$4", "MGET", "\$5", "first", "\$6", "second"),
                requestLines
            )
        }
    }

    private fun fixed64Field(number: Int, value: Double): ByteArray =
        ByteBuffer.allocate(9)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put(((number shl 3) or 1).toByte())
            .putLong(value.toBits())
            .array()

    private fun directUdpPacket(): ByteArray = ByteBuffer.allocate(244)
        .order(ByteOrder.LITTLE_ENDIAN)
        .apply {
            putInt(7)
            putInt(1_725_000_000)
            putInt(123_000_000)
            putFixedString("map", 32)
            putInt(42)
            putFixedString("渝A00042", 32)
            putFixedString("L4", 32)
            putInt(2)
            putInt(4)
            putDouble(0.1)
            putDouble(0.2)
            putDouble(8.5)
            repeat(3) { putDouble(0.0) }
            repeat(3) { putDouble(0.0) }
            repeat(3) { putDouble(0.0) }
            putInt(0)
            putInt(0)
            putInt(3)
            putInt(4)
            putInt(5)
            putInt(6)
            putInt(7)
            assertEquals(244, position())
        }
        .array()

    private fun ByteBuffer.putFixedString(value: String, length: Int) {
        val bytes = value.toByteArray()
        require(bytes.size <= length)
        put(bytes)
        repeat(length - bytes.size) { put(0) }
    }
}
