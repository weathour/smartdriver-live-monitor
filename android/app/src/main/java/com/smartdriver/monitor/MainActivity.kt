package com.smartdriver.monitor

import android.app.Activity
import android.app.AlertDialog
import android.annotation.SuppressLint
import android.graphics.Color
import android.graphics.Typeface
import android.graphics.drawable.GradientDrawable
import android.os.Bundle
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import android.text.InputType
import android.view.Gravity
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import java.text.SimpleDateFormat
import java.util.ArrayDeque
import java.util.Date
import java.util.Locale
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong

@SuppressLint("SetTextI18n") // Chinese-only field diagnostic UI; most strings include live protocol values.
class MainActivity : Activity() {
    private val mainHandler = Handler(Looper.getMainLooper())
    private val eventQueue = ArrayBlockingQueue<MonitorEvent>(512)
    private val droppedUiEvents = AtomicLong()
    private val clock = SimpleDateFormat("HH:mm:ss.SSS", Locale.CHINA)
    private val redisRows = mutableMapOf<String, TextView>()
    private val redisLatest = mutableMapOf<String, RedisObservation>()
    private val logLines = ArrayDeque<String>()

    private var monitor: LiveMonitor? = null
    private var drainActive = false
    private var redisObservations = 0L
    private var redisChanges = 0L
    private var redisParseErrors = 0L
    private var udpObservations = 0L
    private var udpParseErrors = 0L

    private lateinit var redisHost: EditText
    private lateinit var redisPort: EditText
    private lateinit var redisDatabase: EditText
    private lateinit var udpGroup: EditText
    private lateinit var udpPort: EditText
    private lateinit var udpInterface: EditText
    private lateinit var redisStatus: TextView
    private lateinit var udpStatus: TextView
    private lateinit var metrics: TextView
    private lateinit var udpRows: LinearLayout
    private lateinit var logPanel: TextView

    private val drainEvents = object : Runnable {
        override fun run() {
            var processed = 0
            while (processed < 200) {
                val event = eventQueue.poll() ?: break
                handleEvent(event)
                processed += 1
            }
            updateMetrics()
            if (drainActive) mainHandler.postDelayed(this, if (processed == 200) 10 else 50)
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(buildContentView())
        loadSettings()
        resetDisplay()
    }

    override fun onResume() {
        super.onResume()
        drainActive = true
        mainHandler.post(drainEvents)
        startMonitor()
    }

    override fun onPause() {
        monitor?.stop()
        monitor = null
        drainActive = false
        mainHandler.removeCallbacks(drainEvents)
        eventQueue.clear()
        super.onPause()
    }

    private fun buildContentView(): ScrollView {
        val page = ScrollView(this).apply { isFillViewport = true }
        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(16), dp(18), dp(16), dp(28))
            setBackgroundColor(Color.rgb(248, 250, 252))
        }

        root.addView(TextView(this).apply {
            text = "SmartDriver 数据监看"
            textSize = 24f
            setTypeface(typeface, Typeface.BOLD)
            setTextColor(Color.rgb(15, 23, 42))
        })
        root.addView(TextView(this).apply {
            text = "前台直连 Redis 与 UDP 组播；只监视，不录制、不写入现场数据。"
            textSize = 13f
            setTextColor(Color.rgb(71, 85, 105))
            setPadding(0, dp(4), 0, dp(12))
        })

        root.addView(sectionTitle("连接设置"))
        redisHost = addField(root, "Redis 地址", DEFAULT_REDIS_HOST, InputType.TYPE_CLASS_TEXT)
        redisPort = addField(root, "Redis 端口", DEFAULT_REDIS_PORT.toString(), InputType.TYPE_CLASS_NUMBER)
        redisDatabase = addField(root, "Redis DB", DEFAULT_REDIS_DATABASE.toString(), InputType.TYPE_CLASS_NUMBER)
        udpGroup = addField(root, "UDP 组播", DEFAULT_UDP_GROUP, InputType.TYPE_CLASS_TEXT)
        udpPort = addField(root, "UDP 端口", DEFAULT_UDP_PORT.toString(), InputType.TYPE_CLASS_NUMBER)
        udpInterface = addField(root, "UDP 网卡 IPv4", DEFAULT_UDP_INTERFACE, InputType.TYPE_CLASS_TEXT)

        root.addView(Button(this).apply {
            text = "应用并重连"
            isAllCaps = false
            setOnClickListener { applySettings() }
            layoutParams = margins(top = 8, bottom = 10)
        })

        root.addView(sectionTitle("当前接收状态"))
        redisStatus = statusPanel("Redis：未连接")
        udpStatus = statusPanel("UDP：未监听")
        metrics = TextView(this).apply {
            textSize = 13f
            setTextColor(Color.rgb(51, 65, 85))
            setPadding(dp(2), dp(5), dp(2), dp(8))
        }
        root.addView(redisStatus)
        root.addView(udpStatus)
        root.addView(metrics)

        root.addView(sectionTitle("六个 Unity Redis 键"))
        root.addView(TextView(this).apply {
            text = "“最后观察”与“内容未变化”分开显示。点击任一键查看解析字段和原始十六进制。"
            textSize = 12f
            setTextColor(Color.rgb(71, 85, 105))
            setPadding(0, 0, 0, dp(6))
        })
        SmartDriverProtocol.redisKeys.forEach { key ->
            val row = dataCard("$key\n等待首次观察").apply {
                setOnClickListener { redisLatest[key]?.let { showRedisDetail(it) } }
            }
            redisRows[key] = row
            root.addView(row)
        }

        root.addView(sectionTitle("UDP 最近 50 个 datagram"))
        root.addView(TextView(this).apply {
            text = "重复、乱序、尾随字节和解析失败只加标签。界面历史有上限，观察总数仍累计。"
            textSize = 12f
            setTextColor(Color.rgb(71, 85, 105))
            setPadding(0, 0, 0, dp(6))
        })
        udpRows = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        root.addView(udpRows)

        root.addView(sectionTitle("运行日志"))
        logPanel = TextView(this).apply {
            text = "等待启动…"
            textSize = 12f
            typeface = Typeface.MONOSPACE
            setTextColor(Color.rgb(30, 41, 59))
            setTextIsSelectable(true)
            setPadding(dp(10), dp(10), dp(10), dp(10))
            background = rounded(Color.WHITE)
        }
        root.addView(logPanel)

        page.addView(root)
        return page
    }

    private fun addField(root: LinearLayout, label: String, initial: String, inputType: Int): EditText {
        val row = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            layoutParams = margins(bottom = 5)
        }
        row.addView(TextView(this).apply {
            text = label
            textSize = 13f
            setTextColor(Color.rgb(51, 65, 85))
            layoutParams = LinearLayout.LayoutParams(dp(118), ViewGroup.LayoutParams.WRAP_CONTENT)
        })
        val field = EditText(this).apply {
            setText(initial)
            this.inputType = inputType
            setSingleLine(true)
            textSize = 14f
            layoutParams = LinearLayout.LayoutParams(0, dp(48), 1f)
        }
        row.addView(field)
        root.addView(row)
        return field
    }

    private fun sectionTitle(textValue: String): TextView = TextView(this).apply {
        text = textValue
        textSize = 18f
        setTypeface(typeface, Typeface.BOLD)
        setTextColor(Color.rgb(15, 23, 42))
        setPadding(0, dp(14), 0, dp(8))
    }

    private fun statusPanel(initial: String): TextView = TextView(this).apply {
        text = initial
        textSize = 13f
        setTextColor(Color.rgb(15, 23, 42))
        setTextIsSelectable(true)
        setPadding(dp(11), dp(9), dp(11), dp(9))
        background = rounded(Color.WHITE)
        layoutParams = margins(bottom = 5)
    }

    private fun dataCard(initial: String): TextView = TextView(this).apply {
        text = initial
        textSize = 13f
        setTextColor(Color.rgb(30, 41, 59))
        setTextIsSelectable(true)
        setPadding(dp(11), dp(9), dp(11), dp(9))
        background = rounded(Color.WHITE)
        layoutParams = margins(bottom = 6)
    }

    private fun applySettings() {
        val config = try {
            configFromFields()
        } catch (throwable: Exception) {
            AlertDialog.Builder(this)
                .setTitle("连接设置错误")
                .setMessage(throwable.message ?: "设置无效")
                .setPositiveButton("确定", null)
                .show()
            return
        }
        saveSettings(config)
        monitor?.stop()
        monitor = null
        eventQueue.clear()
        resetDisplay()
        monitor = LiveMonitor(this, config, ::enqueueEvent).also(LiveMonitor::start)
    }

    private fun startMonitor() {
        if (monitor != null) return
        val config = runCatching { configFromFields() }.getOrElse {
            appendLog("ERROR", it.message ?: "连接设置无效")
            return
        }
        monitor = LiveMonitor(this, config, ::enqueueEvent).also(LiveMonitor::start)
    }

    private fun enqueueEvent(event: MonitorEvent) {
        if (eventQueue.offer(event)) return
        eventQueue.poll()
        droppedUiEvents.incrementAndGet()
        if (!eventQueue.offer(event)) droppedUiEvents.incrementAndGet()
    }

    private fun handleEvent(event: MonitorEvent) {
        when (event) {
            is SourceStatus -> updateSourceStatus(event)
            is RedisBatch -> updateRedis(event)
            is UdpObservation -> addUdp(event)
            is MonitorLog -> appendLog(event.level, event.message)
        }
    }

    private fun updateSourceStatus(event: SourceStatus) {
        val label = when (event.state) {
            "connecting" -> "连接中"
            "connected" -> "已连接"
            "listening" -> "监听中"
            "error" -> "错误"
            "stopped" -> "已停止"
            else -> event.state
        }
        val view = if (event.source == MonitorSource.REDIS) redisStatus else udpStatus
        val name = if (event.source == MonitorSource.REDIS) "Redis" else "UDP"
        view.text = "$name：$label\n${event.detail}"
        view.background = rounded(
            when (event.state) {
                "connected", "listening" -> Color.rgb(240, 253, 244)
                "error" -> Color.rgb(254, 242, 242)
                "connecting" -> Color.rgb(255, 251, 235)
                else -> Color.WHITE
            }
        )
    }

    private fun updateRedis(batch: RedisBatch) {
        redisObservations += batch.observations.size
        batch.observations.forEach { observation ->
            redisLatest[observation.key] = observation
            if (observation.changed) redisChanges += 1
            if (observation.parse.status == "parse_error") redisParseErrors += 1
            val state = when {
                observation.error != null -> "读取错误"
                observation.present == true -> "存在"
                else -> "不存在"
            }
            val ageSeconds = (SystemClock.elapsedRealtime() - observation.changedAtElapsedMs).coerceAtLeast(0) / 1_000.0
            val sha = observation.sha256?.take(16) ?: "—"
            redisRows.getValue(observation.key).apply {
                text = buildString {
                    appendLine("${observation.key} · $state · ${observation.payload?.size ?: 0} bytes · ${observation.parse.status}")
                    appendLine("观察 ${clock.format(Date(observation.observedAtMs))} · 内容未变化 ${formatDuration(ageSeconds)} · SHA $sha")
                    append(observation.parse.summary)
                }
                background = rounded(
                    when {
                        observation.error != null || observation.parse.status == "parse_error" -> Color.rgb(254, 242, 242)
                        observation.present != true -> Color.rgb(241, 245, 249)
                        observation.changed -> Color.rgb(240, 253, 244)
                        ageSeconds >= 10 -> Color.rgb(254, 242, 242)
                        ageSeconds >= 2 -> Color.rgb(255, 251, 235)
                        else -> Color.WHITE
                    }
                )
            }
        }
        redisStatus.text = "Redis：已连接\n轮询 #${batch.sequence} · ${batch.readMethod} · ${String.format(Locale.US, "%.2f", batch.roundTripMs)} ms"
    }

    private fun addUdp(event: UdpObservation) {
        udpObservations += 1
        if (event.packet.parse.status == "parse_error") udpParseErrors += 1
        val tags = buildList {
            if (event.duplicate) add("重复")
            if (event.outOfOrder) add("乱序")
            if (event.packet.trailingBytes > 0) add("尾随 ${event.packet.trailingBytes} 字节")
            if (event.wireHint != "direct-or-unknown") add(event.wireHint)
        }
        val row = dataCard(
            buildString {
                appendLine("#${event.sequence} · ${clock.format(Date(event.observedAtMs))} · ${event.sourceAddress}:${event.sourcePort} · ${event.payload.size} bytes")
                appendLine("间隔 ${event.interArrivalMs?.let { String.format(Locale.US, "%.3f ms", it) } ?: "—"} · ${event.packet.parse.status}${if (tags.isEmpty()) "" else " · ${tags.joinToString(" / ")}"}")
                append(event.packet.parse.summary)
            }
        ).apply {
            background = rounded(
                when {
                    event.packet.parse.status == "parse_error" -> Color.rgb(254, 242, 242)
                    tags.isNotEmpty() -> Color.rgb(255, 251, 235)
                    else -> Color.rgb(240, 253, 244)
                }
            )
            setOnClickListener { showUdpDetail(event) }
        }
        udpRows.addView(row, 0)
        while (udpRows.childCount > 50) udpRows.removeViewAt(udpRows.childCount - 1)
    }

    private fun showRedisDetail(observation: RedisObservation) {
        val raw = observation.payload?.let(SmartDriverProtocol::hexPreview) ?: "—"
        showDetail(
            observation.key,
            "状态：${observation.parse.status}\n${observation.parse.detail}\n\nRAW HEX (${observation.payload?.size ?: 0} bytes)\n$raw"
        )
    }

    private fun showUdpDetail(event: UdpObservation) {
        val tags = listOfNotNull(
            "duplicate=${event.duplicate}",
            "out_of_order=${event.outOfOrder}",
            "wire_hint=${event.wireHint}"
        ).joinToString(" · ")
        showDetail(
            "UDP #${event.sequence}",
            "来源：${event.sourceAddress}:${event.sourcePort}\nSHA-256：${event.sha256}\n$tags\n\n${event.packet.parse.detail}\n\nRAW HEX (${event.payload.size} bytes)\n${SmartDriverProtocol.hexPreview(event.payload)}"
        )
    }

    private fun showDetail(title: String, textValue: String) {
        val text = TextView(this).apply {
            text = textValue
            textSize = 12f
            typeface = Typeface.MONOSPACE
            setTextColor(Color.rgb(15, 23, 42))
            setTextIsSelectable(true)
            setPadding(dp(14), dp(12), dp(14), dp(12))
        }
        AlertDialog.Builder(this)
            .setTitle(title)
            .setView(ScrollView(this).apply { addView(text) })
            .setPositiveButton("关闭", null)
            .show()
    }

    private fun appendLog(level: String, message: String) {
        logLines += "${clock.format(Date())} [$level] $message"
        while (logLines.size > 100) logLines.removeFirst()
        logPanel.text = logLines.joinToString("\n")
    }

    private fun updateMetrics() {
        metrics.text = "Redis 观察 $redisObservations · 变化 $redisChanges · 解析失败 $redisParseErrors\n" +
            "UDP $udpObservations · 解析失败 $udpParseErrors · UI 丢弃 ${droppedUiEvents.get()}"
    }

    private fun resetDisplay() {
        redisObservations = 0
        redisChanges = 0
        redisParseErrors = 0
        udpObservations = 0
        udpParseErrors = 0
        droppedUiEvents.set(0)
        redisLatest.clear()
        udpRows.removeAllViews()
        logLines.clear()
        logPanel.text = "等待启动…"
        redisStatus.text = "Redis：未连接"
        udpStatus.text = "UDP：未监听"
        redisRows.forEach { (key, row) ->
            row.text = "$key\n等待首次观察"
            row.background = rounded(Color.WHITE)
        }
        updateMetrics()
    }

    private fun configFromFields(): MonitorConfig = MonitorConfig(
        redisHost = redisHost.text.toString().trim(),
        redisPort = redisPort.text.toString().trim().toIntOrNull() ?: error("Redis 端口不是整数"),
        redisDatabase = redisDatabase.text.toString().trim().toIntOrNull() ?: error("Redis DB 不是整数"),
        udpGroup = udpGroup.text.toString().trim(),
        udpPort = udpPort.text.toString().trim().toIntOrNull() ?: error("UDP 端口不是整数"),
        udpInterface = udpInterface.text.toString().trim().ifEmpty { "auto" }
    )

    private fun loadSettings() {
        val prefs = getSharedPreferences(PREFS, MODE_PRIVATE)
        redisHost.setText(prefs.getString(REDIS_HOST, DEFAULT_REDIS_HOST))
        redisPort.setText(prefs.getInt(REDIS_PORT, DEFAULT_REDIS_PORT).toString())
        redisDatabase.setText(prefs.getInt(REDIS_DB, DEFAULT_REDIS_DATABASE).toString())
        udpGroup.setText(prefs.getString(UDP_GROUP, DEFAULT_UDP_GROUP))
        udpPort.setText(prefs.getInt(UDP_PORT, DEFAULT_UDP_PORT).toString())
        udpInterface.setText(prefs.getString(UDP_INTERFACE, DEFAULT_UDP_INTERFACE))
    }

    private fun saveSettings(config: MonitorConfig) {
        getSharedPreferences(PREFS, MODE_PRIVATE).edit()
            .putString(REDIS_HOST, config.redisHost)
            .putInt(REDIS_PORT, config.redisPort)
            .putInt(REDIS_DB, config.redisDatabase)
            .putString(UDP_GROUP, config.udpGroup)
            .putInt(UDP_PORT, config.udpPort)
            .putString(UDP_INTERFACE, config.udpInterface)
            .apply()
    }

    private fun formatDuration(seconds: Double): String = when {
        seconds < 1 -> String.format(Locale.US, "%.1f 秒", seconds)
        seconds < 60 -> "${seconds.toLong()} 秒"
        else -> "${seconds.toLong() / 60}分${seconds.toLong() % 60}秒"
    }

    private fun rounded(color: Int): GradientDrawable = GradientDrawable().apply {
        setColor(color)
        cornerRadius = dp(8).toFloat()
        setStroke(dp(1), Color.rgb(226, 232, 240))
    }

    private fun margins(top: Int = 0, bottom: Int = 0): LinearLayout.LayoutParams =
        LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT).apply {
            setMargins(0, dp(top), 0, dp(bottom))
        }

    private fun dp(value: Int): Int = (value * resources.displayMetrics.density).toInt()

    companion object {
        private const val PREFS = "smartdriver_monitor"
        private const val REDIS_HOST = "redis_host"
        private const val REDIS_PORT = "redis_port"
        private const val REDIS_DB = "redis_db"
        private const val UDP_GROUP = "udp_group"
        private const val UDP_PORT = "udp_port"
        private const val UDP_INTERFACE = "udp_interface"
    }
}
