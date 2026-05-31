from __future__ import annotations

import asyncio
import shutil
import threading
from pathlib import Path
from typing import Callable, Optional

from rich.markup import escape
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    Checkbox,
    Input,
    Label,
    OptionList,
    ProgressBar,
    RichLog,
    Select,
    Static,
)
from textual.widgets.option_list import Option

from .file_dialog import open_file_dialog, save_file_dialog
from .file_transfer import (
    IncomingOffer,
    FileTransferManager,
    STOPPED_ERROR,
    TIMEOUT_ERROR,
    TransferError,
    TransferSnapshot,
)
from .settings import SavedSettings
from .protocol import ProtocolError, frame_len, parse_frame
from .transport import (
    SERIAL_SPEEDS,
    BluetoothDevice,
    ConnectionConfig,
    MeshtasticTransport,
    NodeTarget,
    human_last_heard,
    parse_tcp_endpoint,
    scan_bluetooth_devices,
    serial_port_options,
    test_tcp_connection,
)


class ConfirmSendScreen(ModalScreen[str]):
    def __init__(self, file_path: Path, node_name: str) -> None:
        super().__init__()
        self.file_path = file_path
        self.node_name = node_name

    def compose(self) -> ComposeResult:
        with Container(id="dialog"):
            yield Static(
                f'Are you sure шо вы хотите отправить файл "{self.file_path.name}" '
                f'пользователю "{self.node_name}"?',
                id="dialog-text",
            )
            with Horizontal(classes="dialog-buttons"):
                yield Button("Yes send", id="yes", variant="success")
                yield Button("Change File", id="change")
                yield Button("Cancel", id="cancel", variant="error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        assert event.button.id is not None
        self.dismiss(event.button.id)


class ReceiveOfferScreen(ModalScreen[bool]):
    def __init__(self, offer: IncomingOffer, timeout_seconds: int = 60) -> None:
        super().__init__()
        self.offer = offer
        self.timeout_seconds = timeout_seconds

    def compose(self) -> ComposeResult:
        metadata = self.offer.metadata
        with Container(id="dialog"):
            yield Static(
                f'Получить файл "{metadata.name}" размером {format_bytes(metadata.size)}?\n'
                f'Автоотмена через {self.timeout_seconds} секунд.',
                id="dialog-text",
            )
            with Horizontal(classes="dialog-buttons"):
                yield Button("Yes receive", id="yes", variant="success")
                yield Button("No", id="no", variant="error")

    def on_mount(self) -> None:
        self.set_timer(self.timeout_seconds, lambda: self.dismiss(False))

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "yes")


class ErrorScreen(ModalScreen[None]):
    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message

    def compose(self) -> ComposeResult:
        with Container(id="error-dialog"):
            yield Static("ERROR!", id="error-title")
            yield Static(self.message, id="error-text")
            yield Button("OK", id="ok", variant="error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(None)


class ConnectionScreen(ModalScreen[None]):
    BINDINGS = [
        ("left", "previous_tab", "Previous"),
        ("right", "next_tab", "Next"),
        ("escape", "cancel", "Cancel"),
    ]

    TAB_ORDER = ("serial", "ble", "tcp")

    def __init__(self, settings: SavedSettings) -> None:
        super().__init__()
        self.settings = settings
        self.active_tab = settings.last_kind if settings.last_kind in self.TAB_ORDER else "serial"
        self.ble_devices: dict[str, BluetoothDevice] = {}
        self._ble_scan_task: Optional[asyncio.Task[None]] = None

    def compose(self) -> ComposeResult:
        with Container(id="connect-dialog"):
            with Horizontal(id="connect-tabs"):
                yield Button("SERIAL", id="tab-serial")
                yield Button("bluetooth", id="tab-ble")
                yield Button("tcp", id="tab-tcp")

            yield Static("Disconnected", id="connect-status", classes="status-line")

            with Vertical(id="serial-pane", classes="connect-pane"):
                yield Label("Device:")
                yield Select(self._serial_options(), value=self.settings.serial_device, id="serial-device")
                yield Label("Speed:")
                yield Select(
                    [(str(speed), str(speed)) for speed in SERIAL_SPEEDS],
                    value=str(self.settings.serial_baudrate or 115200),
                    id="serial-speed",
                )
                with Horizontal(classes="center-row"):
                    yield Button("CONNECT", id="serial-connect", variant="primary", classes="center-button")

            with Vertical(id="ble-pane", classes="connect-pane"):
                yield Static("=== Paired ===", classes="section-title")
                yield OptionList(id="paired-list")
                yield Static("=== Devices ===", classes="section-title")
                yield OptionList(id="ble-list")
                yield Input(placeholder="PIN code, if required", password=True, id="ble-pin")
                with Horizontal(classes="center-row"):
                    yield Button("Scan", id="ble-scan", variant="primary", classes="center-button")
                yield Static("Select device with arrows and press Enter.", id="ble-help")

            with Vertical(id="tcp-pane", classes="connect-pane"):
                yield Label("URL or IP:")
                yield Input(value=self.settings.tcp_endpoint, placeholder="192.168.1.25 or host:4403", id="tcp-endpoint")
                yield Checkbox("Use HTTPS", value=self.settings.tcp_use_https, id="tcp-https")
                with Horizontal(id="tcp-buttons"):
                    yield Button("Test connection", id="tcp-test", variant="primary")
                    yield Button("Connect", id="tcp-connect", variant="success")
                yield Static("", id="tcp-result")

    def on_mount(self) -> None:
        self._fill_paired()
        self._update_tabs()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id.startswith("tab-"):
            self.active_tab = button_id.removeprefix("tab-")
            self._update_tabs()
        elif button_id == "serial-connect":
            await self._connect(self._serial_config())
        elif button_id == "ble-scan":
            self._start_ble_scan()
        elif button_id == "tcp-test":
            await self._test_tcp()
        elif button_id == "tcp-connect":
            await self._connect(self._tcp_config())

    async def _connect(self, config: ConnectionConfig) -> None:
        self.query_one("#connect-status", Static).update("Connecting...")
        try:
            await self.app.connect_to_node(config)  # type: ignore[attr-defined]
        except Exception as exc:
            self.query_one("#connect-status", Static).update("Connection error!")
            self.app.show_error(f"ERROR! {exc}")  # type: ignore[attr-defined]
            return
        self.dismiss(None)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        option_id = event.option_id or ""
        if option_id.startswith("paired:"):
            address = option_id.removeprefix("paired:")
            name = _option_prompt_text(event.option)
            asyncio.create_task(self._connect(self._ble_config(address, name)))
        elif option_id.startswith("ble:"):
            address = option_id.removeprefix("ble:")
            device = self.ble_devices.get(address)
            name = device.name if device else address
            asyncio.create_task(self._connect(self._ble_config(address, name)))

    def action_previous_tab(self) -> None:
        index = self.TAB_ORDER.index(self.active_tab)
        self.active_tab = self.TAB_ORDER[(index - 1) % len(self.TAB_ORDER)]
        self._update_tabs()

    def action_next_tab(self) -> None:
        index = self.TAB_ORDER.index(self.active_tab)
        self.active_tab = self.TAB_ORDER[(index + 1) % len(self.TAB_ORDER)]
        self._update_tabs()

    def action_cancel(self) -> None:
        self.dismiss(None)

    def _serial_options(self) -> list[tuple[str, str]]:
        options = [("Auto detect", "")]
        options.extend(serial_port_options())
        if self.settings.serial_device and self.settings.serial_device not in {value for _, value in options}:
            options.append((self.settings.serial_device, self.settings.serial_device))
        return options

    def _fill_paired(self) -> None:
        paired = self.query_one("#paired-list", OptionList)
        paired.clear_options()
        if not self.settings.bluetooth_devices:
            paired.add_option(Option("No remembered devices", id="paired-empty", disabled=True))
            return
        for index, device in enumerate(self.settings.bluetooth_devices, start=1):
            paired.add_option(Option(f"{index}. {device.name}", id=f"paired:{device.address}"))

    def _start_ble_scan(self) -> None:
        if self._ble_scan_task is not None and not self._ble_scan_task.done():
            self.query_one("#ble-help", Static).update("Scan already running...")
            return
        self._ble_scan_task = asyncio.create_task(self._scan_ble_worker())

    async def _scan_ble_worker(self) -> None:
        self.query_one("#ble-help", Static).update("Scanning...")
        scan_button = self.query_one("#ble-scan", Button)
        scan_button.disabled = True
        try:
            devices = await asyncio.to_thread(scan_bluetooth_devices)
        except Exception as exc:
            if self.is_mounted:
                self.query_one("#ble-help", Static).update(f"Scan error: {exc}")
            return
        finally:
            if self.is_mounted:
                scan_button.disabled = False

        if not self.is_mounted:
            return

        self.ble_devices = {device.address: device for device in devices}
        device_list = self.query_one("#ble-list", OptionList)
        device_list.clear_options()
        if not devices:
            device_list.add_option(Option("No devices found", id="ble-empty", disabled=True))
        for index, device in enumerate(devices, start=1):
            device_list.add_option(Option(f"{index}. {device.name}", id=f"ble:{device.address}"))
        self.query_one("#ble-help", Static).update("Select device with arrows and press Enter.")
        if devices:
            device_list.focus()

    async def _test_tcp(self) -> None:
        endpoint = self.query_one("#tcp-endpoint", Input).value.strip()
        use_https = self.query_one("#tcp-https", Checkbox).value
        result = self.query_one("#tcp-result", Static)
        result.update("Testing...")
        ok, ping_ms = await asyncio.to_thread(test_tcp_connection, endpoint, use_https)
        if ok and ping_ms is not None:
            result.update(f"Success! Ping: {ping_ms:.0f}ms.")
        else:
            result.update("Connection error!")

    def _serial_config(self) -> ConnectionConfig:
        device = str(self.query_one("#serial-device", Select).value or "")
        speed_value = str(self.query_one("#serial-speed", Select).value or "115200")
        try:
            baudrate = int(speed_value)
        except ValueError:
            baudrate = 115200
        return ConnectionConfig(kind="serial", endpoint=device, baudrate=baudrate)

    def _ble_config(self, address: str, name: str) -> ConnectionConfig:
        pin = self.query_one("#ble-pin", Input).value.strip()
        return ConnectionConfig(kind="ble", endpoint=address, pin=pin, name=name)

    def _tcp_config(self) -> ConnectionConfig:
        endpoint = self.query_one("#tcp-endpoint", Input).value.strip()
        use_https = self.query_one("#tcp-https", Checkbox).value
        _, port = parse_tcp_endpoint(endpoint, 4403, use_https)
        return ConnectionConfig(kind="tcp", endpoint=endpoint, tcp_port=port, use_https=use_https)

    def _update_tabs(self) -> None:
        for tab in self.TAB_ORDER:
            button = self.query_one(f"#tab-{tab}", Button)
            label = tab.upper() if tab == self.active_tab else ("bluetooth" if tab == "ble" else tab.lower())
            button.label = label
            button.set_class(tab == self.active_tab, "active-tab")
            pane = self.query_one(f"#{tab}-pane", Vertical)
            pane.display = tab == self.active_tab


class MainMenuScreen(Screen):
    BINDINGS = [
        ("tab", "toggle_focus", "Focus"),
        ("escape", "disconnect", "Disconnect"),
    ]

    TOOLBAR = ("Nodes", "Send File", "Traceroute", "Change Device")

    def __init__(self) -> None:
        super().__init__()
        self.toolbar_active = False
        self.toolbar_index = 0
        self.node_ids: dict[str, NodeTarget] = {}

    def compose(self) -> ComposeResult:
        with Vertical(id="dos-root"):
            yield Static("MeshDrop IRC - Meshtastic File Chat Client", id="title-line")
            with Horizontal(id="toolbar"):
                for index, label in enumerate(self.TOOLBAR):
                    yield Button(label, id=f"tool-{index}", classes="tool-button")
                yield Button("Stop transmit", id="stop-transmit", variant="error", classes="stop-button")
                yield Static("DISCONNECTED", id="device-status")
            with Horizontal(id="chat-body"):
                with Vertical(id="chat-panel"):
                    yield Static("#meshdrop  (Meshtastic IRC - file chat over the mesh)", id="chat-topic")
                    yield RichLog(id="chat-log", markup=True, wrap=True, highlight=False)
                    yield Static("", id="transfer-message")
                with Vertical(id="node-panel"):
                    yield Static("NODES", id="nodes-title")
                    yield OptionList(id="nodes")
            with Horizontal(id="input-row"):
                yield Static(">", id="prompt")
                yield Input(placeholder="Type a message", id="message-input")
                yield Static("Recipient: broadcast", id="recipient-status")
            yield Static("RX: waiting for incoming file offer", id="receive-status")
            yield ProgressBar(total=100, id="receive-progress")
            yield Static("", id="receive-stats")

    def on_mount(self) -> None:
        self._set_connected_state()
        self._hide_receive_progress()
        self._hide_transfer_message()
        self._update_toolbar()
        self.set_transmitting(False)
        self.query_one("#message-input", Input).focus()
        if self.app.transport is not None:  # type: ignore[attr-defined]
            asyncio.create_task(self.refresh_nodes())
        self.write_chat("* receive mode enabled")

    def on_screen_resume(self) -> None:
        self._set_connected_state()
        self._update_toolbar()
        if self.app.transport is not None:  # type: ignore[attr-defined]
            asyncio.create_task(self.refresh_nodes())

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id.startswith("tool-"):
            self.toolbar_index = int(button_id.removeprefix("tool-"))
            await self.activate_toolbar_item()
        elif button_id == "stop-transmit":
            self.app.stop_transfer()  # type: ignore[attr-defined]

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "message-input":
            return
        text = event.value.strip()
        event.input.value = ""
        if not text:
            return
        await self.send_chat_message(text)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id != "nodes":
            return
        node = self.node_ids.get(event.option_id or "")
        if node is None:
            return
        self.app.selected_node = node  # type: ignore[attr-defined]
        self.query_one("#recipient-status", Static).update(f"Recipient: {node.name}")
        self.write_chat(f"* recipient selected: {node.name} ({node.node_id})")

    async def on_key(self, event) -> None:
        key = event.key
        if key == "alt":
            self.toolbar_active = not self.toolbar_active
            self._update_toolbar()
            event.stop()
            return
        if self.toolbar_active:
            if key == "left":
                self.toolbar_index = (self.toolbar_index - 1) % len(self.TOOLBAR)
                self._update_toolbar()
                event.stop()
            elif key == "right":
                self.toolbar_index = (self.toolbar_index + 1) % len(self.TOOLBAR)
                self._update_toolbar()
                event.stop()
            elif key == "enter":
                await self.activate_toolbar_item()
                event.stop()
            elif key == "escape":
                self.toolbar_active = False
                self._update_toolbar()
                event.stop()

    async def action_refresh_nodes(self) -> None:
        await self.refresh_nodes()

    def action_toggle_focus(self) -> None:
        nodes = self.query_one("#nodes", OptionList)
        message_input = self.query_one("#message-input", Input)
        if nodes.has_focus:
            message_input.focus()
            nodes.set_class(False, "focused-panel")
        else:
            nodes.focus()
            nodes.set_class(True, "focused-panel")

    def action_disconnect(self) -> None:
        self.app.disconnect_device()  # type: ignore[attr-defined]

    async def action_send_file(self) -> None:
        await self.begin_send()

    async def activate_toolbar_item(self) -> None:
        action = self.TOOLBAR[self.toolbar_index]
        self.toolbar_active = False
        self._update_toolbar()
        if action == "Nodes":
            await self.refresh_nodes()
            self.query_one("#nodes", OptionList).focus()
            self.query_one("#nodes", OptionList).set_class(True, "focused-panel")
        elif action == "Send File":
            await self.begin_send()
        elif action == "Traceroute":
            await self.run_traceroute()
        elif action == "Change Device":
            self.app.show_connection_dialog()  # type: ignore[attr-defined]

    async def refresh_nodes(self) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.write_chat("* not connected")
            return
        self.write_chat("* loading nodes...")
        try:
            nodes = await self.app.load_nodes()  # type: ignore[attr-defined]
        except Exception as exc:
            if self.app.transport is not None and not self.app.transport.is_connected():  # type: ignore[attr-defined]
                self.app._show_local_device_lost()  # type: ignore[attr-defined]
            else:
                self.app.show_error(f"ERROR! {exc}")  # type: ignore[attr-defined]
            return

        option_list = self.query_one("#nodes", OptionList)
        option_list.clear_options()
        self.node_ids.clear()
        self.app.nodes_by_key.clear()  # type: ignore[attr-defined]
        for index, node in enumerate(nodes):
            option_id = f"node-{index}"
            heard = human_last_heard(node.last_heard)
            snr = "?" if node.snr is None else f"{node.snr:.1f}dB"
            option_list.add_option(Option(f"{node.name:<18} {heard:>4} {snr:>8}", id=option_id))
            self.node_ids[option_id] = node
            self.app.nodes_by_key[option_id] = node  # type: ignore[attr-defined]
            self.app.nodes_by_key[node.node_id] = node  # type: ignore[attr-defined]
            self.app.nodes_by_key[node.destination] = node  # type: ignore[attr-defined]
            if isinstance(node.destination, int):
                self.app.nodes_by_key[f"!{node.destination:08x}"] = node  # type: ignore[attr-defined]
        if nodes:
            self.write_chat(f"* nodes loaded: {len(nodes)}")
        else:
            option_list.add_option(Option("No nodes heard", id="empty", disabled=True))
            self.write_chat("* no nodes heard")

    async def begin_send(self) -> None:
        if self.app.manager is None:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Not connected.")  # type: ignore[attr-defined]
            return
        if self.app.selected_node is None:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Choose a recipient node.")  # type: ignore[attr-defined]
            return
        try:
            file_path = await asyncio.to_thread(open_file_dialog)
        except Exception as exc:
            self.app.show_error(f"ERROR! File dialog failed: {exc}")  # type: ignore[attr-defined]
            return
        if file_path is None:
            return
        target = self.app.selected_node  # type: ignore[attr-defined]
        self.app.push_screen(  # type: ignore[attr-defined]
            ConfirmSendScreen(file_path, target.name),
            callback=lambda choice: self._handle_send_confirmation(choice, file_path),
        )

    def _handle_send_confirmation(self, choice: str, file_path: Path) -> None:
        if choice == "yes":
            asyncio.create_task(self.start_send(file_path))
        elif choice == "change":
            asyncio.create_task(self.begin_send())

    async def start_send(self, file_path: Path) -> None:
        if self.app.transfer_task is not None and not self.app.transfer_task.done():  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Transfer already running.")  # type: ignore[attr-defined]
            return
        target = self.app.selected_node  # type: ignore[attr-defined]
        if target is None:
            self.app.show_error("ERROR! Choose a recipient node.")  # type: ignore[attr-defined]
            return
        self.set_transmitting(True)
        self.app.transfer_task = asyncio.create_task(  # type: ignore[attr-defined]
            self.app.send_file(file_path, target, self.app.channel_index)  # type: ignore[attr-defined]
        )

    def apply_connection_state(self) -> None:
        self._set_connected_state()
        self.write_chat(f"* connected to {self.app.connected_node_name}")  # type: ignore[attr-defined]
        asyncio.create_task(self.refresh_nodes())

    def show_receive_waiting(self, offer: IncomingOffer) -> None:
        metadata = offer.metadata
        self.query_one("#receive-progress", ProgressBar).display = True
        self.query_one("#receive-stats", Static).display = True
        self.query_one("#receive-status", Static).update(
            f'Receiving "{metadata.name}" from {offer.sender}'
        )
        self.query_one("#receive-stats", Static).update(
            f"File size: {format_bytes(metadata.size)}\nChunks: 0 / {metadata.total}"
        )

    def clear_receive_waiting(self) -> None:
        self.query_one("#receive-status", Static).update("RX: waiting for incoming file offer")
        self._hide_receive_progress()

    def apply_receive_snapshot(self, snapshot: TransferSnapshot) -> None:
        self.query_one("#receive-progress", ProgressBar).display = True
        self.query_one("#receive-stats", Static).display = True
        self.query_one("#receive-progress", ProgressBar).update(progress=snapshot.progress * 100)
        self.query_one("#receive-status", Static).update(snapshot.message or snapshot.state)
        self.query_one("#receive-stats", Static).update(_format_transfer_stats(snapshot))
        if snapshot.state == "complete":
            self.write_chat(f"* received {snapshot.file_name}")

    def apply_send_snapshot(self, snapshot: TransferSnapshot) -> None:
        self.query_one("#receive-progress", ProgressBar).display = True
        self.query_one("#receive-stats", Static).display = True
        self.query_one("#receive-progress", ProgressBar).update(progress=snapshot.progress * 100)
        self.query_one("#receive-status", Static).update(snapshot.message or snapshot.state)
        self.query_one("#receive-stats", Static).update(_format_transfer_stats(snapshot))
        self._update_transfer_message(snapshot)
        if snapshot.state in {"complete", "error", "stopped"}:
            self.set_transmitting(False)

    def _hide_receive_progress(self) -> None:
        self.query_one("#receive-progress", ProgressBar).display = False
        self.query_one("#receive-stats", Static).display = False

    def _hide_transfer_message(self) -> None:
        self.query_one("#transfer-message", Static).display = False

    def _update_transfer_message(self, snapshot: TransferSnapshot) -> None:
        transfer_message = self.query_one("#transfer-message", Static)
        transfer_message.display = True
        transfer_message.update(_format_chat_transfer_message(snapshot))

    def _set_connected_state(self) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.query_one("#device-status", Static).update("DISCONNECTED")
        else:
            self.query_one("#device-status", Static).update(self.app.device_status_text())  # type: ignore[attr-defined]

    async def send_chat_message(self, text: str) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Not connected.")  # type: ignore[attr-defined]
            return
        if frame_len(text) > 200:
            self.app.show_error("ERROR! Message is longer than 200 bytes.")  # type: ignore[attr-defined]
            return
        target = self.app.selected_node  # type: ignore[attr-defined]
        destination = target.destination if target is not None else "^all"
        try:
            await asyncio.to_thread(
                self.app.transport.send_text,  # type: ignore[attr-defined]
                text,
                destination,
                self.app.channel_index,  # type: ignore[attr-defined]
                False,
            )
        except Exception:
            self.app._show_local_device_lost()  # type: ignore[attr-defined]
            return
        self.write_own_message(self.app.local_display_name(), text)  # type: ignore[attr-defined]

    async def run_traceroute(self) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Not connected.")  # type: ignore[attr-defined]
            return
        target = self.app.selected_node  # type: ignore[attr-defined]
        if target is None:
            self.app.show_error("ERROR! Choose a recipient node.")  # type: ignore[attr-defined]
            return
        self.write_system(f"* Tracerout sended to {target.name}...")
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    self.app.transport.send_traceroute,  # type: ignore[attr-defined]
                    target.destination,
                    self.app.channel_index,  # type: ignore[attr-defined]
                    7,
                ),
                timeout=25.0,
            )
        except asyncio.TimeoutError:
            self.write_system("* Traceroute timeout: no route reply in 25s")
            return
        except Exception as exc:
            self.app.show_error(f"ERROR! Traceroute failed: {exc}")  # type: ignore[attr-defined]
            return
        self.write_system("* Tracerout result:")
        self.write_system(f"* [TX] {result.tx}")
        self.write_system(f"* [RX] {result.rx}")

    def write_chat(self, message: str) -> None:
        self.write_system(message)

    def write_system(self, message: str) -> None:
        self.query_one("#chat-log", RichLog).write(f"[cyan]{escape(message)}[/cyan]")

    def write_own_message(self, node_name: str, text: str) -> None:
        self.query_one("#chat-log", RichLog).write(
            f"[magenta]<{escape(node_name)}>: {escape(text)}[/magenta]"
        )

    def write_peer_message(self, node_name: str, text: str) -> None:
        self.query_one("#chat-log", RichLog).write(
            f"[white]<{escape(node_name)}>: {escape(text)}[/white]"
        )

    def _update_toolbar(self) -> None:
        toolbar = self.query_one("#toolbar", Horizontal)
        toolbar.set_class(self.toolbar_active, "toolbar-active")
        for index, _label in enumerate(self.TOOLBAR):
            button = self.query_one(f"#tool-{index}", Button)
            button.set_class(self.toolbar_active and index == self.toolbar_index, "tool-selected")

    def set_transmitting(self, transmitting: bool) -> None:
        for index, _label in enumerate(self.TOOLBAR):
            self.query_one(f"#tool-{index}", Button).display = not transmitting
        self.query_one("#stop-transmit", Button).display = transmitting
        self._update_toolbar()


class MeshShareApp(App):
    CSS = """
    ConnectionScreen, ErrorScreen, ConfirmSendScreen, ReceiveOfferScreen {
        align: center middle;
    }

    Screen {
        background: black;
        color: #66ff66;
    }

    #dos-root {
        width: 100%;
        height: 100%;
        padding: 0 1;
    }

    #title-line {
        height: 1;
        color: #66ff66;
        text-style: bold;
    }

    #toolbar {
        height: 3;
        border: solid #66ff66;
        padding: 0 1;
    }

    #toolbar.toolbar-active {
        background: white;
        color: black;
    }

    .tool-button {
        width: 17;
        background: black;
        color: #66ff66;
        border: none;
        margin-right: 1;
    }

    .tool-button.tool-selected {
        background: black;
        color: white;
        text-style: bold;
    }

    .stop-button {
        width: 20;
        background: #7a1010;
        color: white;
        border: solid #ff5555;
        text-style: bold;
        margin-right: 1;
    }

    #device-status {
        width: 1fr;
        content-align: right middle;
        color: #66ff66;
    }

    #chat-body {
        height: 1fr;
    }

    #chat-panel {
        width: 1fr;
        height: 100%;
        border: solid #66ff66;
        margin-right: 1;
    }

    #chat-topic {
        height: 1;
        color: #33dddd;
        padding: 0 1;
    }

    #chat-log {
        height: 1fr;
        padding: 0 1;
        background: black;
        color: #d0d0d0;
    }

    #transfer-message {
        height: 4;
        padding: 0 1;
        border-top: solid #66ff66;
        color: #33dddd;
        background: black;
    }

    #node-panel {
        width: 34;
        height: 100%;
        border: solid #66ff66;
    }

    #nodes-title {
        height: 1;
        padding: 0 1;
        color: #66ff66;
        text-style: bold;
    }

    #nodes {
        height: 1fr;
        background: black;
        color: #66ff66;
        border: none;
    }

    #nodes.focused-panel {
        border: solid white;
    }

    #input-row {
        height: 3;
        border: solid #66ff66;
        margin-top: 1;
    }

    #prompt {
        width: 3;
        content-align: center middle;
        color: #66ff66;
    }

    #message-input {
        width: 1fr;
        background: black;
        color: #d0d0d0;
        border: none;
    }

    #recipient-status {
        width: 30;
        content-align: right middle;
        color: #33dddd;
    }

    #receive-status {
        height: 3;
        padding: 1;
        border: solid #66ff66;
        margin-top: 1;
    }

    #receive-stats {
        height: 7;
        padding: 1;
        border: solid #66ff66;
        margin-top: 1;
    }

    #connect-dialog {
        width: 76;
        height: auto;
        padding: 1 2;
        background: black;
        color: #66ff66;
        border: thick #66ff66;
    }

    #connect-tabs {
        height: 3;
        align-horizontal: center;
        margin-bottom: 1;
    }

    #connect-tabs Button {
        width: 18;
        margin: 0 1 1 1;
        background: black;
        color: #66ff66;
        border: none;
    }

    Button.active-tab {
        text-style: bold;
        background: #66ff66;
        color: black;
    }

    .connect-pane {
        min-height: 16;
    }

    #paired-list, #ble-list {
        height: 5;
        border: solid #66ff66;
        margin-bottom: 1;
        background: black;
        color: #66ff66;
    }

    .section-title {
        text-style: bold;
    }

    .center-button {
        width: 24;
    }

    .center-row, #tcp-buttons, #send-actions, #stop-actions {
        height: 3;
        align-horizontal: center;
    }

    #dialog, #error-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        background: black;
        color: #66ff66;
        border: thick #66ff66;
    }

    #error-dialog {
        border: thick #ff5555;
    }

    #error-title {
        text-style: bold;
        color: #ff5555;
        margin-bottom: 1;
    }

    .dialog-buttons {
        height: auto;
    }
    """

    def __init__(
        self,
        download_dir: Path,
        chunk_bytes: int,
        packet_delay: float,
    ) -> None:
        super().__init__()
        self.download_dir = download_dir
        self.chunk_bytes = chunk_bytes
        self.packet_delay = packet_delay
        self.settings = SavedSettings.load()
        self.transport: Optional[MeshtasticTransport] = None
        self.manager: Optional[FileTransferManager] = None
        self.connection_kind = ""
        self.connected_node_name = ""
        self.nodes_by_key: dict[object, NodeTarget] = {}
        self.selected_node: Optional[NodeTarget] = None
        self.selected_file: Optional[Path] = None
        self.channel_index = 0
        self.transfer_task: Optional[asyncio.Task[None]] = None
        self._fatal_device_lost = False

    def on_mount(self) -> None:
        self.push_screen(MainMenuScreen())
        self.set_timer(0.1, self.show_connection_dialog)
        self.set_interval(3.0, self._check_local_device)

    def show_connection_dialog(self) -> None:
        self.push_screen(ConnectionScreen(self.settings))

    async def connect_to_node(self, config: ConnectionConfig) -> None:
        transport = MeshtasticTransport()
        manager = FileTransferManager(
            transport=transport,
            download_dir=self.download_dir,
            on_snapshot=self._snapshot_from_worker,
            on_incoming_offer=self._incoming_offer_from_worker,
            chunk_bytes=self.chunk_bytes,
            packet_delay=self.packet_delay,
            offer_timeout=60.0,
        )
        transport.on_message = self._handle_mesh_message
        try:
            await asyncio.to_thread(transport.connect, config)
            local_name = await asyncio.to_thread(transport.get_local_node_name)
        except Exception:
            manager.close()
            transport.close()
            raise

        if self.manager is not None:
            self.manager.close()
        if self.transport is not None:
            self.transport.close()

        self.transport = transport
        self.manager = manager
        self.connection_kind = _connection_label(config.kind)
        self.connected_node_name = local_name or config.name or config.endpoint or config.kind
        self.selected_node = None
        self.selected_file = None
        self._remember_successful_connection(config, local_name)
        if isinstance(self.screen, MainMenuScreen):
            self.screen.apply_connection_state()

    def _remember_successful_connection(self, config: ConnectionConfig, local_name: str) -> None:
        self.settings.last_kind = config.kind
        self.settings.last_node_name = local_name
        if config.kind == "serial":
            self.settings.serial_device = config.endpoint
            self.settings.serial_baudrate = config.baudrate
        elif config.kind == "ble":
            self.settings.remember_bluetooth(config.endpoint, local_name or config.name or config.endpoint)
        elif config.kind == "tcp":
            self.settings.tcp_endpoint = config.endpoint
            self.settings.tcp_use_https = config.use_https
        self.settings.save()

    async def load_nodes(self) -> list[NodeTarget]:
        if self.transport is None:
            raise RuntimeError("not connected")
        return await asyncio.to_thread(self.transport.list_nodes)

    async def send_file(self, file_path: Path, target: NodeTarget, channel_index: int) -> None:
        assert self.manager is not None
        try:
            await asyncio.to_thread(self.manager.send_file, file_path, target, channel_index)
        except TransferError as exc:
            message = str(exc)
            if message == STOPPED_ERROR:
                if isinstance(self.screen, MainMenuScreen):
                    self.screen.set_transmitting(False)
                    self.screen.write_chat("* transfer stopped")
                else:
                    self.switch_screen(MainMenuScreen())
            elif message == TIMEOUT_ERROR:
                self.show_error("Signal Lost!", callback=lambda: self.switch_screen(MainMenuScreen()))
            else:
                self.show_error(message)
        except Exception:
            self._show_local_device_lost()
        finally:
            if isinstance(self.screen, MainMenuScreen):
                self.screen.set_transmitting(False)
            self.transfer_task = None

    def stop_transfer(self) -> None:
        if self.manager is not None:
            self.manager.stop_active_transfer()

    def disconnect_device(self) -> None:
        if self.manager is not None:
            self.manager.close()
            self.manager = None
        if self.transport is not None:
            self.transport.close()
            self.transport = None
        self.connected_node_name = ""
        self.connection_kind = ""
        self.selected_node = None
        if isinstance(self.screen, MainMenuScreen):
            self.screen.write_chat("* device disconnected")
            self.screen.apply_connection_state()

    def device_status_text(self) -> str:
        if self.transport is None:
            return "DISCONNECTED"
        status = self.transport.get_local_status()
        name = status.name or self.connected_node_name or "node"
        battery = "N/A" if status.battery_level is None else f"{status.battery_level}%"
        voltage = "?" if status.voltage is None else f"{status.voltage:.2f}V"
        return f"{name} | {self.connection_kind or '?'} | {battery} {voltage}"

    def local_display_name(self) -> str:
        if self.transport is not None:
            try:
                status = self.transport.get_local_status()
            except Exception:
                status = None
            if status is not None and status.name:
                return status.name
        return self.connected_node_name or "this-node"

    def peer_display_name(self, message) -> str:
        keys = _message_sender_keys(message)
        seen_nodes: set[int] = set()
        for node in self.nodes_by_key.values():
            node_identity = id(node)
            if node_identity in seen_nodes:
                continue
            seen_nodes.add(node_identity)
            node_keys = {node.node_id, str(node.node_id), node.destination, str(node.destination)}
            node_keys.add(str(node.node_id).lower())
            node_keys.add(str(node.destination).lower())
            if isinstance(node.destination, int):
                node_keys.add(f"!{node.destination:08x}")
            if keys.intersection(node_keys):
                return node.name
        sender = getattr(message, "from_id", None) or getattr(message, "from_node_num", None)
        if isinstance(sender, int):
            return f"!{sender:08x}"
        return str(sender or "unknown")

    def _handle_mesh_message(self, message) -> None:
        try:
            parse_frame(message.text)
        except ProtocolError:
            if isinstance(message.text, str) and message.text.startswith("MS1|"):
                return
            self.call_from_thread(self._append_incoming_chat, message)
            return
        manager = self.manager
        if manager is not None:
            threading.Thread(
                target=manager.handle_message,
                args=(message,),
                name="MeshShareRxFrame",
                daemon=True,
            ).start()

    def _append_incoming_chat(self, message) -> None:
        if isinstance(self.screen, MainMenuScreen):
            sender = self.peer_display_name(message)
            self.screen.write_peer_message(sender, message.text)

    def _incoming_offer_from_worker(self, offer: IncomingOffer) -> bool:
        event = threading.Event()
        result = {"accepted": False}

        def ask_user() -> None:
            if not isinstance(self.screen, MainMenuScreen):
                event.set()
                return

            def done(accepted: bool) -> None:
                result["accepted"] = accepted
                if accepted and isinstance(self.screen, MainMenuScreen):
                    self.screen.show_receive_waiting(offer)
                elif isinstance(self.screen, MainMenuScreen):
                    self.screen.clear_receive_waiting()
                event.set()

            self.push_screen(ReceiveOfferScreen(offer, timeout_seconds=60), callback=done)

        self.call_from_thread(ask_user)
        if not event.wait(60):
            return False
        return result["accepted"]

    def _snapshot_from_worker(self, snapshot: TransferSnapshot) -> None:
        self.call_from_thread(self._apply_snapshot, snapshot)

    def _apply_snapshot(self, snapshot: TransferSnapshot) -> None:
        if snapshot.state == "error" and snapshot.message == TIMEOUT_ERROR:
            self.show_error("Signal Lost!", callback=lambda: self.switch_screen(MainMenuScreen()))
            return
        if snapshot.direction == "send" and isinstance(self.screen, MainMenuScreen):
            self.screen.apply_send_snapshot(snapshot)
        elif snapshot.direction == "receive" and isinstance(self.screen, MainMenuScreen):
            self.screen.apply_receive_snapshot(snapshot)
            if snapshot.state == "complete" and snapshot.output_path:
                asyncio.create_task(self._save_received_file(snapshot))

    async def _save_received_file(self, snapshot: TransferSnapshot) -> None:
        source = Path(snapshot.output_path)
        destination = await asyncio.to_thread(save_file_dialog, snapshot.file_name)
        if destination is None:
            if isinstance(self.screen, MainMenuScreen):
                self.screen.write_chat(f"* received file left in temp: {source}")
            return
        try:
            await asyncio.to_thread(shutil.copy2, source, destination)
        except OSError as exc:
            self.show_error(f"ERROR! Could not save file: {exc}")
            return
        try:
            source.unlink()
        except OSError:
            pass
        if isinstance(self.screen, MainMenuScreen):
            self.screen.write_chat(f"* saved received file: {destination}")

    def _check_local_device(self) -> None:
        if self.transport is None or self._fatal_device_lost:
            return
        if not self.transport.is_connected():
            self._show_local_device_lost()

    def _show_local_device_lost(self) -> None:
        if self._fatal_device_lost:
            return
        self._fatal_device_lost = True
        label = self.connection_kind or "Meshtastic"
        self.show_error(f"{label} Device Lost!", callback=self.exit)

    def show_error(self, message: str, callback: Optional[Callable[[], None]] = None) -> None:
        self.push_screen(ErrorScreen(message), callback=lambda _: callback() if callback else None)

    def on_unmount(self) -> None:
        if self.manager is not None:
            self.manager.close()
        if self.transport is not None:
            self.transport.close()


def _format_transfer_stats(snapshot: TransferSnapshot) -> str:
    signal = "?" if snapshot.signal_db is None else f"{snapshot.signal_db:.1f} dB"
    eta = "?" if snapshot.eta_seconds is None else format_duration(snapshot.eta_seconds)
    transieved = snapshot.sent_chunks if snapshot.direction == "send" else snapshot.received_chunks
    return (
        f"Signal: {signal}\n"
        f"File: {snapshot.file_name or '?'}\n"
        f"File size: {format_bytes(snapshot.file_size)}\n"
        f"Chunks: verified {snapshot.verified_chunks} / transieved {transieved} / total {snapshot.total_chunks}\n"
        f"Elapsed / ETA: {format_duration(snapshot.elapsed_seconds)} / {eta}\n"
        f"Packets sent / received: {snapshot.packets_sent} / {snapshot.packets_received}"
    )


def _message_sender_keys(message) -> set[object]:
    keys: set[object] = set()
    for value in (getattr(message, "from_id", None), getattr(message, "from_node_num", None)):
        if value is None:
            continue
        keys.add(value)
        keys.add(str(value))
        if isinstance(value, str):
            keys.add(value.lower())
        if isinstance(value, int):
            keys.add(f"!{value:08x}")
    return keys


def _format_chat_transfer_message(snapshot: TransferSnapshot) -> str:
    total = max(snapshot.total_chunks, 1)
    verified_bytes = min(snapshot.file_size, round(snapshot.file_size * snapshot.verified_chunks / total))
    percent = round(snapshot.progress * 100)
    bar = _progress_bar(snapshot.progress)
    eta = "?" if snapshot.eta_seconds is None else format_duration(snapshot.eta_seconds)
    return (
        f'/"{snapshot.file_name or "file"}"/ {format_bytes(verified_bytes)} / {format_bytes(snapshot.file_size)}\n'
        f"{bar} {percent}%\n"
        f"{snapshot.verified_chunks}/{snapshot.sent_chunks}/{snapshot.total_chunks} packets | "
        f"{format_duration(snapshot.elapsed_seconds)} / ~{eta}"
    )


def _progress_bar(progress: float, width: int = 28) -> str:
    clamped = max(0.0, min(1.0, progress))
    filled = round(clamped * width)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _option_prompt_text(option: Option) -> str:
    prompt = option.prompt
    return prompt.plain if hasattr(prompt, "plain") else str(prompt)


def _connection_label(kind: str) -> str:
    return {"serial": "Serial", "ble": "Bluetooth", "tcp": "TCP"}.get(kind, kind)


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def format_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024 or unit == "GB":
            return f"{value:.1f} {unit}" if unit != "B" else f"{int(value)} B"
        value /= 1024
    return f"{size} B"
