from __future__ import annotations

import argparse
from pathlib import Path

from .protocol import DEFAULT_RAW_CHUNK_BYTES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MeshShare TUI for Meshtastic file transfer.")
    parser.add_argument(
        "--download-dir",
        "--temp-dir",
        dest="download_dir",
        default="temp",
        help="Internal temp directory for received files. Default: temp",
    )
    parser.add_argument(
        "--chunk-bytes",
        type=int,
        default=DEFAULT_RAW_CHUNK_BYTES,
        help=f"Raw bytes per data chunk. Default: {DEFAULT_RAW_CHUNK_BYTES}",
    )
    parser.add_argument(
        "--packet-delay",
        type=float,
        default=1.1,
        help="Delay between outgoing packets in seconds. Default: 1.1",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        from .app import MeshShareApp
    except ModuleNotFoundError as exc:
        if exc.name == "textual":
            print("Textual is not installed. Run: python -m pip install -e .")
            return 2
        raise

    app = MeshShareApp(
        download_dir=Path(args.download_dir),
        chunk_bytes=args.chunk_bytes,
        packet_delay=args.packet_delay,
    )
    app.run()
    return 0
