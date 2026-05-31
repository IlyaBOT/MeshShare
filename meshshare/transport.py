from __future__ import annotations

import time
import socket
import ssl
import sys
from dataclasses import dataclass
from threading import RLock
from typing import Callable, Optional, Union
from urllib.parse import urlparse

from .protocol import MAX_FRAME_BYTES, ProtocolError, frame_len

Destination = Union[int, str]
SERIAL_SPEEDS = (9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600)
_BLE_DEVICE_CACHE: dict[str, object] = {}


@dataclass(frozen=True)
class ConnectionConfig:
    kind: str
    endpoint: str = ""
    tcp_port: int = 4403
    baudrate: int = 115200
    pin: str = ""
    use_https: bool = False
    name: str = ""


@dataclass(frozen=True)
class BluetoothDevice:
    name: str
    address: str


@dataclass(frozen=True)
class ChannelInfo:
    index: int
    name: str
    encrypted: bool


@dataclass(frozen=True)
class NodeTarget:
    destination: Destination
    node_id: str
    name: str
    snr: Optional[float] = None
    last_heard: Optional[float] = None
    hops_away: Optional[int] = None


@dataclass(frozen=True)
class LocalNodeStatus:
    name: str = ""
    battery_level: Optional[int] = None
    voltage: Optional[float] = None
    powered: bool = False


@dataclass(frozen=True)
class TracerouteResult:
    tx: str
    rx: str = ""


@dataclass(frozen=True)
class MeshMessage:
    text: str
    from_id: Optional[Destination]
    from_node_num: Optional[int]
    rx_snr: Optional[float]
    packet_id: Optional[int]
    channel_index: int = 0
    timestamp: Optional[float] = None
    reply_id: Optional[int] = None
    emoji: str = ""


class MeshtasticTransport:
    def __init__(self, on_message: Optional[Callable[[MeshMessage], None]] = None) -> None:
        self.on_message = on_message
        self.interface = None
        self._pub = None
        self._subscribed = False
        self._lock = RLock()

    def connect(self, config: ConnectionConfig) -> None:
        from pubsub import pub

        with self._lock:
            self.close()
            try:
                self._pub = pub
                pub.subscribe(self._on_receive, "meshtastic.receive.text")
                self._subscribed = True

                if config.kind == "serial":
                    import meshtastic.serial_interface

                    dev_path = config.endpoint or None
                    self.interface = _open_serial_interface(
                        meshtastic.serial_interface,
                        dev_path,
                        config.baudrate,
                    )
                elif config.kind == "ble":
                    self.interface = _open_ble_interface(config.endpoint or None, config.pin)
                elif config.kind == "tcp":
                    import meshtastic.tcp_interface

                    host, port = parse_tcp_endpoint(config.endpoint, config.tcp_port, config.use_https)
                    self.interface = _open_tcp_interface(
                        meshtastic.tcp_interface,
                        host,
                        port,
                        config.use_https,
                    )
                else:
                    raise ValueError(f"unknown connection kind: {config.kind}")
            except Exception:
                self.close()
                raise

    def close(self) -> None:
        with self._lock:
            if self._pub is not None and self._subscribed:
                try:
                    self._pub.unsubscribe(self._on_receive, "meshtastic.receive.text")
                except Exception:
                    pass
                self._subscribed = False
            if self.interface is not None:
                try:
                    self.interface.close()
                except Exception:
                    pass
                self.interface = None

    def send_text(
        self,
        text: str,
        destination_id: Destination,
        channel_index: int = 0,
        want_ack: bool = True,
        reply_id: Optional[int] = None,
    ) -> Optional[int]:
        if frame_len(text) > MAX_FRAME_BYTES:
            raise ProtocolError("attempted to send a frame larger than 200 bytes")
        with self._lock:
            if self.interface is None:
                raise RuntimeError("not connected to a Meshtastic node")
            packet = self.interface.sendText(
                text,
                destinationId=destination_id,
                wantAck=want_ack,
                channelIndex=channel_index,
                replyId=reply_id,
            )
            return _packet_id(packet)

    def send_reaction(
        self,
        emoji: str,
        reply_id: int,
        destination_id: Destination,
        channel_index: int = 0,
    ) -> Optional[int]:
        if not emoji:
            raise ProtocolError("empty emoji reaction")
        with self._lock:
            if self.interface is None:
                raise RuntimeError("not connected to a Meshtastic node")
            return _send_text_reaction(
                self.interface,
                emoji,
                reply_id,
                destination_id,
                channel_index,
            )

    def send_traceroute(
        self,
        destination_id: Destination,
        channel_index: int = 0,
        hop_limit: int = 7,
    ) -> TracerouteResult:
        # Do not hold the transport lock while waiting for a traceroute reply.
        # Some Meshtastic backends can block inside sendTraceRoute() when the
        # route never comes back; keeping the lock would freeze node refresh,
        # chat sends and the connection watchdog.
        with self._lock:
            interface = self.interface
        if interface is None:
            raise RuntimeError("not connected to a Meshtastic node")
        return _send_traceroute_with_result(
            interface,
            destination_id,
            channel_index,
            hop_limit,
        )

    def list_nodes(self) -> list[NodeTarget]:
        with self._lock:
            interface = self.interface
            if interface is None:
                return []

            nodes_by_num = getattr(interface, "nodesByNum", None) or {}
            nodes = []
            for node_num, node in nodes_by_num.items():
                if not isinstance(node, dict):
                    continue
                if _is_self_node(interface, node):
                    continue
                nodes.append(_node_target_from_node(node_num, node))

            if not nodes:
                by_id = getattr(interface, "nodes", None) or {}
                for node_id, node in by_id.items():
                    if not isinstance(node, dict):
                        continue
                    if _is_self_node(interface, node):
                        continue
                    nodes.append(_node_target_from_node(node.get("num"), node, fallback_id=str(node_id)))

            return sorted(
                nodes,
                key=lambda node: (
                    -(node.last_heard or 0),
                    node.name.lower(),
                    node.node_id,
                ),
            )

    def find_node(
        self,
        from_id: Optional[Destination] = None,
        from_node_num: Optional[int] = None,
    ) -> Optional[NodeTarget]:
        """Return a single node from the already-loaded Meshtastic node DB.

        This is intentionally a cache lookup. It does not request a full node
        refresh from the radio; the TUI uses it only to append newly-seen peers
        without rebuilding the visible node list.
        """
        with self._lock:
            interface = self.interface
            if interface is None:
                return None

            if isinstance(from_node_num, int):
                node = (getattr(interface, "nodesByNum", None) or {}).get(from_node_num)
                if isinstance(node, dict) and not _is_self_node(interface, node):
                    return _node_target_from_node(from_node_num, node)

            if from_id is not None:
                node_id = str(from_id)
                by_id = getattr(interface, "nodes", None) or {}
                node = by_id.get(node_id)
                if isinstance(node, dict) and not _is_self_node(interface, node):
                    return _node_target_from_node(node.get("num"), node, fallback_id=node_id)

                for node_num, node in (getattr(interface, "nodesByNum", None) or {}).items():
                    if not isinstance(node, dict) or _is_self_node(interface, node):
                        continue
                    user = node.get("user") or {}
                    if str(user.get("id") or "") == node_id:
                        return _node_target_from_node(node_num, node, fallback_id=node_id)
                    if isinstance(node_num, int) and node_id == f"!{node_num:08x}":
                        return _node_target_from_node(node_num, node, fallback_id=node_id)

            if isinstance(from_node_num, int):
                node_id = f"!{from_node_num:08x}"
                return NodeTarget(
                    destination=from_id if from_id is not None else from_node_num,
                    node_id=node_id,
                    name=node_id,
                )
            if from_id is not None:
                node_id = str(from_id)
                return NodeTarget(destination=from_id, node_id=node_id, name=node_id)
            return None

    def list_channels(self) -> list[ChannelInfo]:
        with self._lock:
            interface = self.interface
            if interface is None:
                return []
            channels = []
            local_node = getattr(interface, "localNode", None)
            raw_channels = getattr(local_node, "channels", None) or []
            for channel in raw_channels:
                info = _channel_info_from_channel(channel)
                if info is not None:
                    channels.append(info)
            return sorted(channels, key=lambda channel: channel.index)

    def get_signal(self, destination: Destination) -> Optional[float]:
        with self._lock:
            interface = self.interface
            if interface is None:
                return None
            for node in self.list_nodes():
                if node.destination == destination or node.node_id == destination:
                    return node.snr
            return None

    def get_local_node_name(self) -> str:
        with self._lock:
            if self.interface is None:
                return ""
            try:
                name = self.interface.getLongName()
            except Exception:
                name = ""
            if name:
                return str(name)
            try:
                user = self.interface.getMyUser()
            except Exception:
                user = None
            if isinstance(user, dict):
                return str(user.get("longName") or user.get("shortName") or "")
            return ""

    def get_local_status(self) -> LocalNodeStatus:
        with self._lock:
            if self.interface is None:
                return LocalNodeStatus()
            name = self.get_local_node_name()
            node = self._get_local_node_dict()
            metrics = _node_device_metrics(node)
            battery = _metric_value(metrics, "batteryLevel", "battery_level", "battery")
            voltage = _metric_value(metrics, "voltage", "batteryVoltage", "battery_voltage")
            powered_value = _metric_value(
                metrics,
                "powered",
                "isPowered",
                "is_powered",
                "powerStatus",
                "power_status",
                "externalPower",
                "external_power",
            )
            battery_level = _as_int_optional(battery)
            powered = _looks_powered(powered_value) or (battery_level is not None and battery_level > 100)
            return LocalNodeStatus(
                name=name,
                battery_level=battery_level,
                voltage=_as_float(voltage),
                powered=powered,
            )

    def _get_local_node_dict(self) -> Optional[dict]:
        interface = self.interface
        if interface is None:
            return None
        local = getattr(interface, "localNode", None)
        local_num = getattr(local, "nodeNum", None)
        if local_num is not None:
            node = (getattr(interface, "nodesByNum", None) or {}).get(local_num)
            if isinstance(node, dict):
                return node
        my_info = getattr(interface, "myInfo", None)
        if isinstance(my_info, dict):
            return my_info
        return None

    def is_connected(self) -> bool:
        with self._lock:
            interface = self.interface
            if interface is None:
                return False
            stream = getattr(interface, "stream", None)
            if stream is not None and hasattr(stream, "is_open"):
                return bool(stream.is_open)
            client = getattr(interface, "client", None)
            bleak_client = getattr(client, "bleak_client", None)
            if bleak_client is not None and hasattr(bleak_client, "is_connected"):
                return bool(bleak_client.is_connected)
            if client is not None and hasattr(client, "is_connected"):
                connected = client.is_connected
                return bool(connected() if callable(connected) else connected)
            sock = getattr(interface, "socket", None)
            if sock is not None:
                return not bool(getattr(sock, "_closed", False))
            return True

    def _on_receive(self, packet, interface=None) -> None:
        if self.interface is not None and interface is not None and interface is not self.interface:
            return

        decoded = packet.get("decoded", {}) if isinstance(packet, dict) else {}
        text = decoded.get("text")
        if text is None:
            payload = decoded.get("payload")
            if isinstance(payload, bytes):
                text = payload.decode("utf-8", "replace")
        emoji = _decoded_str(decoded, "emoji")
        reply_id = _decoded_int(decoded, "replyId", "reply_id")
        if text is None and emoji and reply_id is not None:
            text = ""
        if not isinstance(text, str):
            return

        from_node_num = packet.get("from") if isinstance(packet, dict) else None
        from_id = packet.get("fromId") if isinstance(packet, dict) else None
        if from_id is None:
            from_id = from_node_num
        message = MeshMessage(
            text=text,
            from_id=from_id,
            from_node_num=from_node_num if isinstance(from_node_num, int) else None,
            rx_snr=_as_float(packet.get("rxSnr")) if isinstance(packet, dict) else None,
            packet_id=_as_int_optional(packet.get("id")) if isinstance(packet, dict) else None,
            channel_index=_as_int(packet.get("channel")) if isinstance(packet, dict) else 0,
            timestamp=_packet_timestamp(packet) if isinstance(packet, dict) else None,
            reply_id=reply_id,
            emoji=emoji,
        )
        if self.on_message is not None:
            self.on_message(message)


def parse_tcp_endpoint(endpoint: str, default_port: int, use_https: bool = False) -> tuple[str, int]:
    endpoint = endpoint.strip()
    if not endpoint:
        return "localhost", 443 if use_https else default_port
    if "://" in endpoint:
        parsed = urlparse(endpoint)
        if not parsed.hostname:
            raise ValueError("invalid TCP URL")
        return parsed.hostname, parsed.port or (443 if use_https or parsed.scheme == "https" else default_port)
    if endpoint.count(":") == 1:
        host, raw_port = endpoint.rsplit(":", 1)
        if raw_port.isdigit():
            return host, int(raw_port)
    return endpoint, 443 if use_https else default_port


def serial_port_options() -> list[tuple[str, str]]:
    import serial.tools.list_ports

    ports = []
    for port in serial.tools.list_ports.comports():
        label = port.device
        if port.description and port.description != "n/a":
            label = f"{port.device} - {port.description}"
        ports.append((label, port.device))
    ports.sort(key=lambda item: item[1].lower())
    return ports


def scan_bluetooth_devices() -> list[BluetoothDevice]:
    import meshtastic.ble_interface

    devices = []
    for device in meshtastic.ble_interface.BLEInterface.scan():
        address = getattr(device, "address", "") or ""
        if not address:
            continue
        name = getattr(device, "name", "") or address
        _BLE_DEVICE_CACHE[address] = device
        _BLE_DEVICE_CACHE[name] = device
        devices.append(BluetoothDevice(name=name, address=address))
    return sorted(devices, key=lambda device: (device.name.lower(), device.address.lower()))


def test_tcp_connection(endpoint: str, use_https: bool, timeout: float = 5.0) -> tuple[bool, Optional[float]]:
    host, port = parse_tcp_endpoint(endpoint, 4403, use_https)
    started = time.perf_counter()
    try:
        raw_sock = socket.create_connection((host, port), timeout=timeout)
        with raw_sock:
            if use_https:
                context = ssl.create_default_context()
                with context.wrap_socket(raw_sock, server_hostname=host):
                    pass
        return True, (time.perf_counter() - started) * 1000
    except OSError:
        return False, None


def _send_text_reaction(
    interface,
    emoji: str,
    reply_id: int,
    destination_id: Destination,
    channel_index: int,
) -> Optional[int]:
    from meshtastic import mesh_pb2, portnums_pb2

    packet = mesh_pb2.MeshPacket()
    packet.channel = channel_index
    packet.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
    packet.decoded.payload = b""
    packet.decoded.reply_id = reply_id
    packet.decoded.emoji = ord(emoji[0])
    packet.id = interface._generatePacketId()
    sent = interface._sendPacket(packet, destination_id, wantAck=False)
    return _packet_id(sent)


def _channel_info_from_channel(channel) -> Optional[ChannelInfo]:
    index = _as_int_optional(getattr(channel, "index", None))
    settings = getattr(channel, "settings", None)
    role = str(getattr(channel, "role", "") or "")
    if index is None or settings is None:
        return None
    if role.endswith("DISABLED") or role == "0":
        return None
    name = str(getattr(settings, "name", "") or ("Primary" if index == 0 else f"Channel {index}"))
    psk = bytes(getattr(settings, "psk", b"") or b"")
    encrypted = bool(psk and psk != b"\x01")
    return ChannelInfo(index=index, name=name, encrypted=encrypted)


def _send_traceroute_with_result(
    interface,
    destination_id: Destination,
    channel_index: int,
    hop_limit: int,
) -> TracerouteResult:
    from google.protobuf.json_format import MessageToDict
    from meshtastic import mesh_pb2

    result: dict[str, object] = {}
    had_instance_callback = "onResponseTraceRoute" in getattr(interface, "__dict__", {})
    original_callback = getattr(interface, "onResponseTraceRoute", None)

    def on_response(packet: dict) -> None:
        try:
            route_discovery = mesh_pb2.RouteDiscovery()
            route_discovery.ParseFromString(packet["decoded"]["payload"])
            result["trace"] = _format_traceroute_result(
                interface,
                packet,
                MessageToDict(route_discovery),
                destination_id,
            )
        except Exception as exc:
            result["error"] = exc
        finally:
            _mark_traceroute_received(interface)

    interface.onResponseTraceRoute = on_response
    try:
        interface.sendTraceRoute(destination_id, hop_limit, channel_index)
    finally:
        if had_instance_callback:
            interface.onResponseTraceRoute = original_callback
        else:
            try:
                delattr(interface, "onResponseTraceRoute")
            except AttributeError:
                pass

    if "error" in result:
        raise RuntimeError(f"could not parse traceroute result: {result['error']}")
    trace = result.get("trace")
    if isinstance(trace, TracerouteResult):
        return trace
    return TracerouteResult(
        tx=f"{_trace_node_label(interface, _local_node_num(interface))} -> "
        f"{_trace_node_label(interface, destination_id)} (? dB)",
        rx=f"{_trace_node_label(interface, _local_node_num(interface))} <- "
        f"{_trace_node_label(interface, destination_id)} (? dB)",
    )


def _format_traceroute_result(
    interface,
    packet: dict,
    payload: dict,
    destination_id: Destination,
) -> TracerouteResult:
    local = packet.get("to", _local_node_num(interface))
    remote = packet.get("from", destination_id)
    route = _trace_node_list(payload.get("route"))
    route_back = _trace_node_list(payload.get("routeBack"))
    snr_towards = _trace_snr_list(payload.get("snrTowards"))
    snr_back = _trace_snr_list(payload.get("snrBack"))
    return TracerouteResult(
        tx=_format_trace_tx(interface, local, route, remote, snr_towards),
        rx=_format_trace_rx(interface, local, route_back, remote, snr_back),
    )


def _format_trace_tx(interface, local, route: list[object], remote, snrs: list[float]) -> str:
    parts = [_trace_node_label(interface, local)]
    for index, node in enumerate(route):
        parts.append(f"{_trace_node_label(interface, node)} ({_format_trace_snr(snrs, index)})")
    parts.append(f"{_trace_node_label(interface, remote)} ({_format_trace_snr(snrs, len(route))})")
    return " -> ".join(parts)


def _format_trace_rx(interface, local, route_back: list[object], remote, snrs: list[float]) -> str:
    parts = [_trace_node_label(interface, local)]
    reversed_route = list(reversed(route_back))
    for index, node in enumerate(reversed_route):
        snr_index = len(route_back) - index
        parts.append(f"{_trace_node_label(interface, node)} ({_format_trace_snr(snrs, snr_index)})")
    parts.append(f"{_trace_node_label(interface, remote)} ({_format_trace_snr(snrs, 0)})")
    return " <- ".join(parts)


def _mark_traceroute_received(interface) -> None:
    acknowledgment = getattr(interface, "_acknowledgment", None)
    if acknowledgment is None:
        return
    try:
        acknowledgment.receivedTraceRoute = True
    except Exception:
        pass


def _trace_node_list(value) -> list[object]:
    if not isinstance(value, list):
        return []
    return [_coerce_int(item) if _coerce_int(item) is not None else item for item in value]


def _trace_snr_list(value) -> list[float]:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        number = _as_float(item)
        if number is not None:
            result.append(number)
    return result


def _format_trace_snr(values: list[float], index: int) -> str:
    if 0 <= index < len(values) and int(values[index]) != -128:
        return f"{values[index] / 4:.1f} dB"
    return "? dB"


def _trace_node_label(interface, node_ref) -> str:
    int_ref = _coerce_int(node_ref)
    nodes_by_num = getattr(interface, "nodesByNum", None) or {}
    if int_ref is not None:
        node = nodes_by_num.get(int_ref)
        name = _node_name_from_dict(node)
        if name:
            return name
        if int_ref == _local_node_num(interface):
            try:
                local_name = interface.getLongName()
            except Exception:
                local_name = ""
            if local_name:
                return str(local_name)
        try:
            node_id = interface._nodeNumToId(int_ref, False)
        except Exception:
            node_id = None
        if node_id:
            name = _node_name_by_id(interface, str(node_id))
            return name or str(node_id)
        return f"!{int_ref:08x}"

    node_id = str(node_ref) if node_ref is not None else ""
    if node_id:
        name = _node_name_by_id(interface, node_id)
        return name or node_id
    return "unknown"


def _node_name_by_id(interface, node_id: str) -> str:
    nodes = getattr(interface, "nodes", None) or {}
    node = nodes.get(node_id)
    name = _node_name_from_dict(node)
    if name:
        return name
    for node in (getattr(interface, "nodesByNum", None) or {}).values():
        if not isinstance(node, dict):
            continue
        user = node.get("user") or {}
        if str(user.get("id") or "") == node_id:
            return _node_name_from_dict(node)
    return ""


def _node_name_from_dict(node) -> str:
    if not isinstance(node, dict):
        return ""
    user = node.get("user") or {}
    if not isinstance(user, dict):
        return ""
    return str(user.get("longName") or user.get("shortName") or user.get("id") or "")


def _local_node_num(interface):
    local = getattr(interface, "localNode", None)
    node_num = getattr(local, "nodeNum", None)
    if node_num is not None:
        return node_num
    my_info = getattr(interface, "myInfo", None)
    if isinstance(my_info, dict):
        return my_info.get("myNodeNum") or my_info.get("nodeNum")
    return None


def _coerce_int(value) -> Optional[int]:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            if text.startswith("!"):
                return int(text[1:], 16)
            return int(text, 0)
        except ValueError:
            return None
    return None


def _node_target_from_node(
    node_num,
    node: dict,
    fallback_id: Optional[str] = None,
) -> NodeTarget:
    user = node.get("user") or {}
    node_id = str(user.get("id") or fallback_id or _node_num_to_id(node_num))
    name = str(user.get("longName") or user.get("shortName") or node_id)
    destination: Destination = node_id
    if node_id == "None" and isinstance(node_num, int):
        node_id = _node_num_to_id(node_num)
        destination = node_num
    return NodeTarget(
        destination=destination,
        node_id=node_id,
        name=name,
        snr=_as_float(node.get("snr")),
        last_heard=_as_float(node.get("lastHeard")),
        hops_away=node.get("hopsAway") if isinstance(node.get("hopsAway"), int) else None,
    )


def _node_num_to_id(node_num) -> str:
    if isinstance(node_num, int):
        return f"!{node_num:08x}"
    return "unknown"


def _is_self_node(interface, node: dict) -> bool:
    local = getattr(interface, "localNode", None)
    local_num = getattr(local, "nodeNum", None)
    return local_num is not None and node.get("num") == local_num


def _node_device_metrics(node: Optional[dict]) -> dict:
    if not isinstance(node, dict):
        return {}
    candidates = [
        node.get("deviceMetrics"),
        node.get("device_metrics"),
        node.get("metrics"),
    ]
    telemetry = node.get("telemetry")
    if isinstance(telemetry, dict):
        candidates.extend(
            [
                telemetry.get("deviceMetrics"),
                telemetry.get("device_metrics"),
                telemetry.get("metrics"),
            ]
        )
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _metric_value(metrics: dict, *keys: str):
    for key in keys:
        if key in metrics:
            return metrics.get(key)
    return None


def _looks_powered(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, str):
        normalized = value.strip().lower().replace(" ", "_").replace("-", "_")
        return normalized in {"powered", "external", "external_power", "usb", "mains", "true", "yes", "on"}
    return False


def _as_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_int_optional(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _packet_id(packet) -> Optional[int]:
    if isinstance(packet, dict):
        return _as_int_optional(packet.get("id"))
    return _as_int_optional(getattr(packet, "id", None))


def _packet_timestamp(packet: dict) -> Optional[float]:
    for key in ("rxTime", "rx_time"):
        timestamp = _as_float(packet.get(key))
        if timestamp is not None:
            return timestamp
    return None


def _decoded_int(decoded: dict, *keys: str) -> Optional[int]:
    for key in keys:
        value = _as_int_optional(decoded.get(key))
        if value is not None:
            return value
    return None


def _decoded_str(decoded: dict, key: str) -> str:
    value = decoded.get(key)
    if isinstance(value, str):
        return value
    if isinstance(value, int) and value > 0:
        try:
            return chr(value)
        except ValueError:
            return ""
    return ""


def _open_serial_interface(serial_interface_module, dev_path: Optional[str], baudrate: int):
    if baudrate == 115200:
        return serial_interface_module.SerialInterface(devPath=dev_path)

    original_serial = serial_interface_module.serial.Serial

    def serial_factory(port, _baudrate, *args, **kwargs):
        return original_serial(port, baudrate, *args, **kwargs)

    serial_interface_module.serial.Serial = serial_factory
    try:
        return serial_interface_module.SerialInterface(devPath=dev_path)
    finally:
        serial_interface_module.serial.Serial = original_serial


def _open_ble_interface(address: Optional[str], pin: str):
    import meshtastic.ble_interface

    device = _cached_ble_device(address)

    if not pin and device is None:
        return meshtastic.ble_interface.BLEInterface(address)

    if pin and sys.platform == "darwin":
        raise RuntimeError(
            "Bluetooth pairing with a PIN is not available from MeshShare on macOS. "
            "Connect without a PIN and use the macOS system pairing prompt if it appears."
        )

    class PairingBLEInterface(meshtastic.ble_interface.BLEInterface):
        def connect(self, address: Optional[str] = None):
            target = device or self.find_device(address)
            target = _ble_client_target(address, target)
            client = meshtastic.ble_interface.BLEClient(
                target,
                disconnected_callback=lambda _: self.close(),
                pair=_ble_pair_before_connect(),
            )
            client.connect()
            if pin:
                try:
                    client.pair(passkey=pin)
                except TypeError:
                    client.pair()
            client.discover()
            client = _wrap_ble_client(client, meshtastic.ble_interface)
            _prepare_macos_ble_connection(client, meshtastic.ble_interface, self.from_num_handler)
            return client

    return PairingBLEInterface(address)


def _cached_ble_device(address: Optional[str]) -> Optional[object]:
    if not address:
        return None
    return _BLE_DEVICE_CACHE.get(address)


def _ble_client_target(address: Optional[str], device: object) -> object:
    if sys.platform == "darwin" and address:
        return address
    return device


def _ble_pair_before_connect() -> bool:
    return sys.platform == "darwin"


def _wrap_ble_client(client, ble_interface_module):
    if sys.platform != "darwin":
        return client
    disabled_notify_uuids = {
        str(getattr(ble_interface_module, "LEGACY_LOGRADIO_UUID", "")),
        str(getattr(ble_interface_module, "LOGRADIO_UUID", "")),
    }
    toradio_uuid = str(getattr(ble_interface_module, "TORADIO_UUID", ""))
    return _MacOSBLEClient(client, disabled_notify_uuids, toradio_uuid)


def _prepare_macos_ble_connection(client, ble_interface_module, from_num_handler) -> None:
    if sys.platform != "darwin":
        return
    fromnum_uuid = getattr(ble_interface_module, "FROMNUM_UUID", None)
    if fromnum_uuid is not None and client.has_characteristic(fromnum_uuid):
        client.start_notify(fromnum_uuid, from_num_handler)


class _MacOSBLEClient:
    def __init__(self, client, disabled_notify_uuids: set[str], toradio_uuid: str) -> None:
        self._client = client
        self._disabled_notify_uuids = disabled_notify_uuids
        self._toradio_uuid = toradio_uuid.lower()

    def __getattr__(self, name: str):
        return getattr(self._client, name)

    def has_characteristic(self, specifier) -> bool:
        if str(specifier) in self._disabled_notify_uuids:
            return False
        return self._client.has_characteristic(specifier)

    def start_notify(self, specifier, *args, **kwargs):
        return _retry_ble_pairing_operation(lambda: self._client.start_notify(specifier, *args, **kwargs))

    def write_gatt_char(self, specifier, data, *args, **kwargs):
        kwargs = self._apple_write_kwargs(specifier, kwargs)
        return _retry_ble_pairing_operation(lambda: self._client.write_gatt_char(specifier, data, *args, **kwargs))

    def _apple_write_kwargs(self, specifier, kwargs: dict) -> dict:
        if _characteristic_uuid(specifier) != self._toradio_uuid:
            return kwargs
        characteristic = _resolve_characteristic(self._client, specifier)
        if not _supports_write_without_response(characteristic or specifier):
            return kwargs
        updated = dict(kwargs)
        updated["response"] = False
        return updated


def _retry_ble_pairing_operation(operation, attempts: int = 6, delay_seconds: float = 1.0):
    last_error = None
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as exc:
            if not _looks_like_ble_encryption_error(exc):
                raise
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(delay_seconds)
    raise RuntimeError(
        "Bluetooth pairing/encryption is still not ready. If macOS showed a pairing prompt, "
        "accept it and try connecting again. If no prompt appeared, toggle Bluetooth off/on "
        "or forget/remove the node from known Bluetooth devices and retry."
    ) from last_error


def _looks_like_ble_encryption_error(exc: Exception) -> bool:
    message = _exception_chain_text(exc).lower()
    return (
        "encryption is insufficient" in message
        or "insufficient encryption" in message
        or "cbatterrordomain code=15" in message
    )


def _characteristic_uuid(specifier) -> str:
    return str(getattr(specifier, "uuid", specifier)).lower()


def _supports_write_without_response(specifier) -> bool:
    properties = getattr(specifier, "properties", ()) or ()
    return "write-without-response" in properties or "write_without_response" in properties


def _resolve_characteristic(client, specifier):
    if hasattr(specifier, "properties"):
        return specifier
    bleak_client = getattr(client, "bleak_client", None)
    services = getattr(bleak_client, "services", None)
    get_characteristic = getattr(services, "get_characteristic", None)
    if get_characteristic is None:
        return None
    try:
        return get_characteristic(specifier)
    except Exception:
        return None


def _exception_chain_text(exc: BaseException) -> str:
    parts = []
    current: Optional[BaseException] = exc
    while current is not None:
        parts.append(str(current))
        current = current.__cause__ or current.__context__
    return " ".join(parts)


def _open_tcp_interface(tcp_interface_module, host: str, port: int, use_https: bool):
    if not use_https:
        return tcp_interface_module.TCPInterface(hostname=host, portNumber=port)

    raw_sock = socket.create_connection((host, port))
    context = ssl.create_default_context()
    tls_sock = context.wrap_socket(raw_sock, server_hostname=host)
    interface = tcp_interface_module.TCPInterface(
        hostname=host,
        portNumber=port,
        connectNow=False,
    )
    interface.socket = tls_sock
    interface.connect()
    interface.waitForConfig()
    return interface


def human_last_heard(timestamp: Optional[float]) -> str:
    if not timestamp:
        return "?"
    delta = max(0, int(time.time() - timestamp))
    if delta < 60:
        return f"{delta}s"
    if delta < 3600:
        return f"{delta // 60}m"
    if delta < 86400:
        return f"{delta // 3600}h"
    return f"{delta // 86400}d"
