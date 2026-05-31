from __future__ import annotations

from pathlib import Path
from typing import Optional


def open_file_dialog() -> Optional[Path]:
    import tkinter as tk
    from tkinter import filedialog

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        selected = filedialog.askopenfilename(title="Choose file to send")
    finally:
        root.destroy()
    return Path(selected) if selected else None


def save_file_dialog(default_name: str) -> Optional[Path]:
    import tkinter as tk
    from tkinter import filedialog

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        selected = filedialog.asksaveasfilename(
            title="Save received file",
            initialfile=default_name,
        )
    finally:
        root.destroy()
    return Path(selected) if selected else None
