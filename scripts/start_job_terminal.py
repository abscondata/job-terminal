from __future__ import annotations

import argparse
import sys
import threading
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.config import load_config
from engine.state_server import start_state_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start Job Terminal")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(str(ROOT / "config.json"))
    url = f"http://{args.host}:{args.port}/"
    threading.Thread(target=lambda: webbrowser.open(url), daemon=True).start()
    start_state_server(args.host, args.port, cfg.db_path)


if __name__ == "__main__":
    main()
