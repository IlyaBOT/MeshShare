from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


SETTINGS_PATH = Path.home() / ".meshshare" / "settings.json"


@dataclass
class SavedBluetoothDevice:
    name: str
    address: str


@dataclass
class SavedSettings:
    last_kind: str = "serial"
    serial_device: str = ""
    serial_baudrate: int = 115200
    tcp_endpoint: str = ""
    tcp_use_https: bool = False
    bluetooth_last_address: str = ""
    bluetooth_devices: list[SavedBluetoothDevice] = field(default_factory=list)
    last_node_name: str = ""

    @classmethod
    def load(cls, path: Path = SETTINGS_PATH) -> "SavedSettings":
        if not path.exists():
            return cls()
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return cls()
        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SavedSettings":
        devices = []
        for item in raw.get("bluetooth_devices", []):
            if not isinstance(item, dict):
                continue
            address = str(item.get("address") or "")
            if not address:
                continue
            devices.append(
                SavedBluetoothDevice(
                    name=str(item.get("name") or address),
                    address=address,
                )
            )
        return cls(
            last_kind=str(raw.get("last_kind") or "serial"),
            serial_device=str(raw.get("serial_device") or ""),
            serial_baudrate=int(raw.get("serial_baudrate") or 115200),
            tcp_endpoint=str(raw.get("tcp_endpoint") or ""),
            tcp_use_https=bool(raw.get("tcp_use_https")),
            bluetooth_last_address=str(raw.get("bluetooth_last_address") or ""),
            bluetooth_devices=devices,
            last_node_name=str(raw.get("last_node_name") or ""),
        )

    def save(self, path: Path = SETTINGS_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=2), encoding="utf-8")

    def remember_bluetooth(self, address: str, name: str) -> None:
        if not address:
            return
        self.bluetooth_last_address = address
        cleaned_name = name or address
        self.bluetooth_devices = [
            device for device in self.bluetooth_devices if device.address != address
        ]
        self.bluetooth_devices.insert(0, SavedBluetoothDevice(name=cleaned_name, address=address))
        self.bluetooth_devices = self.bluetooth_devices[:8]
