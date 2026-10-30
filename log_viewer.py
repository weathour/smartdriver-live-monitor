"""Paged Tk viewer for a single .sdlog; disk reads run outside the UI thread."""
import json
from pathlib import Path
import queue
from concurrent.futures import ThreadPoolExecutor
import tkinter as tk
from tkinter import ttk

from desktop_log import inspect_log, read_log_page, read_log_record


class LogViewer:
    PAGE_SIZE = 200
    KIND_LABELS = {"全部": None, "Redis": "redis", "UDP": "udp", "运行事件": "events", "键清单": "inventory"}

    def __init__(self, master, path):
        self.path = Path(path)
        self.window = tk.Toplevel(master)
        self.window.title("查看录制 · " + self.path.name)
        self.window.geometry("1180x760")
        self.window.minsize(860, 560)
        self.closed = False
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.window.bind("<Destroy>", self._destroyed, add="+")
        self.results = queue.Queue()
        self.request_id = 0
        self.reader = ThreadPoolExecutor(max_workers=1, thread_name_prefix="SmartDriver log reader")
        self.read_task = None
        self.offset = 0
        self.total = 0
        self.kind = tk.StringVar(value="全部")
        self.status = tk.StringVar(value="正在读取录制摘要…")
        self.position = tk.StringVar(value="")

        outer = ttk.Frame(self.window, padding=12)
        outer.pack(fill="both", expand=True)
        ttk.Label(outer, text=self.path.name, style="Title.TLabel").pack(anchor="w")
        ttk.Label(outer, textvariable=self.status).pack(anchor="w", pady=(6, 10))
        toolbar = ttk.Frame(outer)
        toolbar.pack(fill="x", pady=(0, 8))
        ttk.Label(toolbar, text="数据类型").pack(side="left")
        self.filter = ttk.Combobox(toolbar, textvariable=self.kind, values=list(self.KIND_LABELS),
                                   state="readonly", width=12)
        self.filter.pack(side="left", padx=(8, 16))
        self.filter.bind("<<ComboboxSelected>>", lambda _: self.load_page(0))
        self.previous = ttk.Button(toolbar, text="上一页", command=lambda: self.load_page(max(0, self.offset - self.PAGE_SIZE)))
        self.previous.pack(side="left")
        self.next = ttk.Button(toolbar, text="下一页", command=lambda: self.load_page(self.offset + self.PAGE_SIZE))
        self.next.pack(side="left", padx=6)
        self.refresh = ttk.Button(toolbar, text="刷新记录", command=lambda: self.load_page(self.offset))
        self.refresh.pack(side="left")
        ttk.Label(toolbar, textvariable=self.position).pack(side="right")

        panes = ttk.Panedwindow(outer, orient="vertical")
        panes.pack(fill="both", expand=True)
        table = ttk.Frame(panes)
        details = ttk.Frame(panes)
        panes.add(table, weight=3)
        panes.add(details, weight=2)
        columns = ("time", "kind", "stream", "bytes", "state")
        self.tree = ttk.Treeview(table, columns=columns, show="headings", selectmode="browse")
        for key, label, width in (("time", "观察时间（UTC）", 205), ("kind", "类型", 75),
                                  ("stream", "来源 / 话题 / 键", 420), ("bytes", "字节", 80), ("state", "状态", 180)):
            self.tree.heading(key, text=label)
            self.tree.column(key, width=width, stretch=key in ("stream", "state"), anchor="w")
        scroll = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview)
        horizontal = ttk.Scrollbar(table, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=scroll.set, xscrollcommand=horizontal.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        horizontal.grid(row=1, column=0, sticky="ew")
        table.rowconfigure(0, weight=1)
        table.columnconfigure(0, weight=1)
        self.tree.bind("<<TreeviewSelect>>", self.select_record)
        ttk.Label(details, text="选中一条查看明细。原始内容最多预览前 2048 字节；过长的明细会截断。",
                  style="Muted.TLabel").pack(anchor="w", pady=(8, 4))
        self.detail = tk.Text(details, wrap="word", height=14, font="TkFixedFont")
        detail_scroll = ttk.Scrollbar(details, orient="vertical", command=self.detail.yview)
        self.detail.configure(yscrollcommand=detail_scroll.set, state="disabled")
        detail_scroll.pack(side="right", fill="y")
        self.detail.pack(fill="both", expand=True)
        self.window.after(50, self.poll)
        self.load_page(0)

    def _launch(self, function, category):
        self.request_id += 1
        request_id = self.request_id

        def read():
            try:
                result = function()
                self.results.put((request_id, category, result, None))
            except Exception as exc:
                self.results.put((request_id, category, None, str(exc)))

        if self.read_task is not None:
            self.read_task.cancel()
        self.read_task = self.reader.submit(read)

    def load_page(self, offset):
        self.offset = offset
        kind = self.KIND_LABELS[self.kind.get()]
        self.previous.configure(state="disabled")
        self.next.configure(state="disabled")
        self.refresh.configure(state="disabled")
        self.filter.configure(state="disabled")
        self.tree.selection_remove(*self.tree.selection())
        self.tree.delete(*self.tree.get_children())
        self.status.set("正在读取本页…")
        self._launch(lambda: (inspect_log(self.path), read_log_page(
            self.path, offset=offset, limit=self.PAGE_SIZE, kind=kind)), "page")

    def select_record(self, _event=None):
        selection = self.tree.selection()
        if not selection:
            return
        record_id = int(selection[0])
        self._set_detail("正在读取所选记录…")
        self._launch(lambda: self._format_record(read_log_record(
            self.path, record_id, preview_bytes=2048)), "record")

    def _set_detail(self, text):
        self.detail.configure(state="normal")
        self.detail.delete("1.0", "end")
        self.detail.insert("end", text)
        self.detail.configure(state="disabled")

    def poll(self):
        if self.closed:
            return
        while True:
            try:
                request_id, category, result, error = self.results.get_nowait()
            except queue.Empty:
                break
            if request_id != self.request_id:
                continue
            if error:
                self.status.set("读取失败：" + error)
                self.refresh.configure(state="normal")
                self.filter.configure(state="readonly")
                continue
            if category == "page":
                manifest, rows = result
                state = {"complete": "已正常结束", "recording": "录制中或未正常结束", "failed": "录制失败"}.get(
                    manifest.get("status"), "状态未知")
                self.status.set("%s · %s 至 %s · 未校验文件完整性" % (state, manifest.get("started_at_utc", ""),
                                                       manifest.get("ended_at_utc") or "尚未结束"))
                counts = manifest.get("counts", {})
                kind = self.KIND_LABELS[self.kind.get()]
                self.total = counts.get(kind, 0) if kind else sum(counts.values())
                for row in rows:
                    meta = row["metadata"]
                    state = (meta.get("parse") or {}).get("status") or meta.get("level", "")
                    stream = meta.get("key") or meta.get("topic") or row.get("stream_key", "")
                    if row["kind"] == "events":
                        stream = str(meta.get("message", ""))[:180]
                    self.tree.insert("", "end", iid=str(row["id"]), values=(
                        row["observed_at_utc"], row["kind"], stream, meta.get("length", ""), state))
                self.position.set("%d–%d / %d 条 · 每页 %d 条" % (
                    self.offset + 1 if rows else 0, self.offset + len(rows), self.total, self.PAGE_SIZE))
                self.previous.configure(state="normal" if self.offset else "disabled")
                self.next.configure(state="normal" if self.offset + len(rows) < self.total else "disabled")
                self.refresh.configure(state="normal")
                self.filter.configure(state="readonly")
                self._set_detail("选择一条记录查看明细。" if rows else "这一类尚无记录。")
            else:
                self._set_detail(result)
        self.window.after(50, self.poll)

    @staticmethod
    def _format_record(result):
        preview = result.pop("payload_preview_hex", "") or ""
        parts, remaining = [], 65536
        for chunk in json.JSONEncoder(ensure_ascii=False, indent=2).iterencode(result):
            if len(chunk) > remaining:
                parts.append(chunk[:remaining])
                parts.append("\n（明细较长，仅预览前 65536 字符）")
                break
            parts.append(chunk)
            remaining -= len(chunk)
        text = "".join(parts)
        if preview:
            text += "\n\n原始内容预览（最多前 2048 字节，十六进制）\n" + "\n".join(
                preview[index:index + 64] for index in range(0, len(preview), 64))
        return text

    def _destroyed(self, event):
        if event.widget is self.window and not self.closed:
            self.closed = True
            self.reader.shutdown(wait=False, cancel_futures=True)

    def close(self):
        self.closed = True
        self.reader.shutdown(wait=False, cancel_futures=True)
        self.window.destroy()
