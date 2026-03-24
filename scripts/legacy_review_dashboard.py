from __future__ import annotations

"""
Legacy review dashboard (secondary/debug use only).
Prefer the Job Terminal UI via scripts/start_job_terminal.py.
"""

import json
import os
import subprocess
import sys
import threading
import time
import tkinter as tk
from pathlib import Path
from tkinter import messagebox

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.state_server import start_state_server

PY = sys.executable

SOURCE_LABELS = {
    "welcometothejungle": "Welcome to the Jungle",
    "kering": "Kering",
    "chanel": "Chanel",
    "sothebys": "Sotheby's",
    "christies": "Christie's",
    "centre_pompidou": "Centre Pompidou",
    "profilculture": "ProfilCulture",
    "linkedin": "LinkedIn",
    "indeed": "Indeed",
}


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("JobEngine Legacy Review")
        self.geometry("1120x700")
        self.configure(bg="#0b0d14")

        self._current_proc = None
        self._report_path: str | None = None
        self._report_opened = False
        self._run_started_at: float | None = None
        self._source_progress: dict[str, dict] = {}
        self._source_order: list[str] = []

        top = tk.Frame(self, bg="#0b0d14")
        top.pack(fill="x", padx=12, pady=(10, 0))

        tk.Label(
            top,
            text="JobEngine Direction Review",
            font=("Segoe UI", 16, "bold"),
            bg="#0b0d14",
            fg="#e2e4ea",
        ).pack(side="left")

        self.meta_lbl = tk.Label(
            top,
            text="",
            bg="#0b0d14",
            fg="#8f94ab",
            font=("Consolas", 10),
        )
        self.meta_lbl.pack(side="left", padx=14)

        self.status_lbl = tk.Label(
            top,
            text="idle",
            bg="#0b0d14",
            fg="#34d399",
            font=("Consolas", 10),
        )
        self.status_lbl.pack(side="right")

        btns = tk.Frame(self, bg="#0b0d14")
        btns.pack(fill="x", padx=12, pady=8)

        self._make_button(btns, "Run Discovery", self.run_discover, "#1e3a5f").pack(
            side="left", padx=3
        )
        self._make_button(
            btns, "Quick Discovery", self.run_quick_discover, "#0d5240"
        ).pack(side="left", padx=3)
        self._make_button(btns, "Open Report", self.open_report, "#1e3a5f").pack(
            side="left", padx=3
        )

        dev_btn = tk.Menubutton(
            btns,
            text="Dev Tools",
            font=("Segoe UI", 10),
            bg="#2e2e2e",
            fg="#e2e4ea",
            activebackground="#2e2e2e",
            activeforeground="#fff",
            bd=0,
            padx=12,
            pady=6,
            cursor="hand2",
            relief="flat",
        )
        dev_menu = tk.Menu(dev_btn, tearoff=0)
        dev_menu.add_command(label="Open Runs Dir", command=self.open_runs)
        dev_btn.configure(menu=dev_menu)
        dev_btn.pack(side="left", padx=3)

        tk.Button(
            btns,
            text="Quit",
            command=self.destroy,
            font=("Segoe UI", 10),
            bg="#1a1a1a",
            fg="#666",
            bd=0,
            padx=12,
            pady=6,
        ).pack(side="right")

        progress = tk.Frame(self, bg="#0d111a", bd=0, highlightthickness=0)
        progress.pack(fill="x", padx=12, pady=(0, 8))

        self.summary_lbl = tk.Label(
            progress,
            text="elapsed=00:00  jobs=0",
            bg="#0d111a",
            fg="#cbd5e1",
            font=("Consolas", 10),
            anchor="w",
            justify="left",
            padx=10,
            pady=8,
        )
        self.summary_lbl.pack(fill="x")

        self.sources_lbl = tk.Label(
            progress,
            text="No discovery running.",
            bg="#0d111a",
            fg="#8f94ab",
            font=("Consolas", 10),
            anchor="w",
            justify="left",
            padx=10,
            pady=4,
        )
        self.sources_lbl.pack(fill="x")

        self.report_lbl = tk.Label(
            progress,
            text="Report: waiting for discovery",
            bg="#0d111a",
            fg="#5b8def",
            font=("Consolas", 10, "underline"),
            anchor="w",
            justify="left",
            cursor="hand2",
            padx=10,
            pady=8,
        )
        self.report_lbl.pack(fill="x")
        self.report_lbl.bind("<Button-1>", lambda _evt: self.open_report())

        self.text = tk.Text(
            self,
            bg="#08090e",
            fg="#e2e4ea",
            insertbackground="#e2e4ea",
            font=("Consolas", 10),
            bd=0,
            highlightthickness=0,
            padx=10,
            pady=10,
        )
        self.text.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.text.tag_configure("err", foreground="#fca5a5")
        self.text.tag_configure("ok", foreground="#34d399")
        self.text.tag_configure("hdr", foreground="#5b8def")

        self.log("Starting review server on 127.0.0.1:8765...", "hdr")
        self._start_state_server()
        self.refresh_meta()

    def _make_button(self, parent, text, cmd, color):
        return tk.Button(
            parent,
            text=text,
            command=cmd,
            font=("Segoe UI", 10),
            bg=color,
            fg="#e2e4ea",
            activebackground=color,
            activeforeground="#fff",
            bd=0,
            padx=12,
            pady=6,
            cursor="hand2",
        )

    def _ui(self, func, *args, **kwargs):
        self.after(0, lambda: func(*args, **kwargs))

    def _start_state_server(self):
        def go():
            db = ROOT / "data" / "jobengine.sqlite"
            if not db.exists():
                try:
                    from engine.config import load_config

                    cfg = load_config(str(ROOT / "config.json"))
                    db = Path(cfg.db_path)
                except Exception:
                    pass
            start_state_server("127.0.0.1", 8765, str(db))

        threading.Thread(target=go, daemon=True).start()

    def refresh_meta(self):
        p = ROOT / "data" / "last_meta.json"
        if not p.exists():
            self.meta_lbl.config(text="no runs yet")
            return
        try:
            meta = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return

        city_counts = meta.get("city_lane_counts", {})
        lane_counts = meta.get("lane_counts", {})
        self.meta_lbl.config(
            text=(
                f"pulled={meta.get('pulled_total', 0)}"
                f"  new={meta.get('new_count', 0)}"
                f"  Paris={city_counts.get('Paris', 0)}"
                f"  NYC={city_counts.get('NYC', 0)}"
                f"  ParisDir={lane_counts.get('Paris Direction', 0)}"
                f"  Platform={lane_counts.get('Money / Platform Leap', 0)}"
                f"  Apply={meta.get('recommendation_counts', {}).get('apply', 0)}"
                f"  Skip={meta.get('recommendation_counts', {}).get('skip', 0)}"
            )
        )

        report_path = meta.get("report_path")
        if not report_path:
            run_dir = meta.get("run_dir")
            if run_dir:
                report_path = str(Path(run_dir) / "report.html")
        if report_path:
            self._report_path = report_path
            self.report_lbl.config(text=f"Report: {report_path}")

    def log(self, message, tag=None):
        self.text.insert("end", message + "\n", tag or "")
        self.text.see("end")

    def set_status(self, text, color="#34d399"):
        self.status_lbl.config(text=text, fg=color)

    def _render_progress(self):
        pulled_total = 0
        cap = 0
        for payload in self._source_progress.values():
            pulled_total = max(pulled_total, int(payload.get("pulled_total") or 0))
            cap = max(cap, int(payload.get("cap") or 0))
        suffix = f"/{cap}" if cap else ""

        if self._run_started_at is None:
            if self._source_order:
                last_payload = self._source_progress.get(self._source_order[-1], {})
                elapsed_label = last_payload.get("elapsed", "00:00")
                self.summary_lbl.config(
                    text=f"elapsed={elapsed_label}  jobs={pulled_total}{suffix}"
                )
            else:
                self.summary_lbl.config(text="elapsed=00:00  jobs=0")
        else:
            elapsed = max(0, int(time.time() - self._run_started_at))
            mins, secs = divmod(elapsed, 60)
            self.summary_lbl.config(
                text=f"elapsed={mins:02d}:{secs:02d}  jobs={pulled_total}{suffix}"
            )

        lines = []
        ordered_sources = self._source_order or sorted(self._source_progress.keys())
        for name in ordered_sources:
            payload = self._source_progress.get(name)
            if not payload:
                continue
            label = SOURCE_LABELS.get(name, name.title())
            status = payload.get("status")
            if status == "running":
                line = f"{label}: running..."
            elif status == "done":
                returned = payload.get("returned", 0)
                ingested = payload.get("ingested", 0)
                line = f"{label}: {ingested} jobs ok (returned {returned})"
            elif status == "error":
                line = f"{label}: error - {payload.get('error', 'unknown')}"
            else:
                line = f"{label}: {status}"
            lines.append(line)
        self.sources_lbl.config(text="\n".join(lines) if lines else "No discovery running.")

    def _tick_progress(self):
        self._render_progress()
        if self._current_proc and self._current_proc.poll() is None:
            self.after(1000, self._tick_progress)

    def _reset_progress(self):
        self._source_progress = {}
        self._source_order = []
        self._report_opened = False
        self._report_path = None
        self.report_lbl.config(text="Report: waiting for discovery")
        self._render_progress()

    def _handle_progress(self, payload: dict):
        name = payload.get("source")
        if name and name not in self._source_order:
            self._source_order.append(name)
        if name:
            self._source_progress[name] = payload
        self._render_progress()

    def _open_path(self, path: str):
        subprocess.Popen(["cmd", "/c", "start", "", path], shell=False)

    def _announce_report(self, report_path: str):
        self._report_path = report_path
        self.report_lbl.config(text=f"Report: {report_path}")
        if not self._report_opened:
            self._report_opened = True
            self._open_path(report_path)
            self.log(f"Report ready: {report_path}", "ok")

    def _run(self, cmd, label, on_done=None, discovery=False):
        if self._current_proc and self._current_proc.poll() is None:
            messagebox.showinfo("", "A job is already running.")
            return

        if discovery:
            self._run_started_at = time.time()
            self._reset_progress()

        def go():
            self._ui(self.set_status, "running...", "#eab308")
            self._ui(self.log, "\n" + ("-" * 60), "hdr")
            self._ui(self.log, f"  {label}", "hdr")
            self._ui(self.log, "-" * 60, "hdr")
            if discovery:
                self._ui(self._tick_progress)
            try:
                env = {**os.environ, "PYTHONPATH": str(ROOT)}
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=env,
                    cwd=str(ROOT),
                    encoding="utf-8",
                    errors="replace",
                )
                self._current_proc = proc

                for raw_line in proc.stdout:
                    line = raw_line.rstrip()
                    if line.startswith("DISCOVERY_PROGRESS="):
                        try:
                            payload = json.loads(line.split("=", 1)[1])
                            self._ui(self._handle_progress, payload)
                        except Exception:
                            self._ui(self.log, line, "err")
                        continue
                    if line.startswith("REPORT="):
                        report_path = line.split("=", 1)[1].strip()
                        self._ui(self._announce_report, report_path)
                        continue
                    tag = "err" if line.startswith("ERROR") or " ERROR:" in line else None
                    self._ui(self.log, line, tag)

                proc.wait()
                self._current_proc = None
                self._ui(self.refresh_meta)
                if on_done:
                    self._ui(on_done)
            except Exception as exc:
                self._current_proc = None
                self._ui(self.log, f"LAUNCH ERROR: {exc}", "err")
            finally:
                if discovery:
                    self._run_started_at = None
                self._ui(self.set_status, "idle", "#34d399")
                if discovery:
                    self._ui(self._render_progress)

        threading.Thread(target=go, daemon=True).start()

    def run_discover(self):
        self._run(
            [PY, str(ROOT / "scripts" / "run_discovery.py")],
            "Running Discovery...",
            discovery=True,
        )

    def run_quick_discover(self):
        self._run(
            [PY, str(ROOT / "scripts" / "run_discovery.py"), "--quick"],
            "Running Quick Discovery...",
            discovery=True,
        )

    def open_report(self):
        if self._report_path and Path(self._report_path).exists():
            self._open_path(self._report_path)
            return

        meta_path = ROOT / "data" / "last_meta.json"
        if not meta_path.exists():
            messagebox.showinfo("", "Run discovery first.")
            return
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            report_path = meta.get("report_path")
            if not report_path:
                run_dir = meta.get("run_dir")
                if run_dir:
                    report_path = str(Path(run_dir) / "report.html")
            if not report_path:
                messagebox.showinfo("", "No report found yet.")
                return
            self._report_path = report_path
            self.report_lbl.config(text=f"Report: {report_path}")
            self._open_path(report_path)
        except Exception as exc:
            messagebox.showerror("Error", str(exc))

    def open_runs(self):
        runs_dir = ROOT / "data" / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        subprocess.Popen(["explorer", str(runs_dir)], shell=False)


if __name__ == "__main__":
    App().mainloop()
