from __future__ import annotations

import asyncio
import shutil
import threading
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
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
    SYNC_ERROR,
    TIMEOUT_ERROR,
    TransferError,
    TransferSnapshot,
)
from .settings import SavedSettings
from .protocol import ProtocolError, frame_len, is_protocol_like, parse_frame
from .transport import (
    SERIAL_SPEEDS,
    BluetoothDevice,
    ChannelInfo,
    ConnectionConfig,
    MeshtasticTransport,
    NodeTarget,
    human_last_heard,
    parse_tcp_endpoint,
    scan_bluetooth_devices,
    serial_port_options,
    test_tcp_connection,
)


@dataclass(frozen=True)
class ChatRecord:
    sender: str
    text: str
    own: bool
    packet_id: Optional[int] = None
    node: Optional[NodeTarget] = None
    timestamp: Optional[float] = None
    reactions: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ChatLine:
    style: str
    text: str
    timestamp: Optional[float] = None
    record: Optional[ChatRecord] = None


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


class RetryTransferScreen(ModalScreen[bool]):
    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message

    def compose(self) -> ComposeResult:
        with Container(id="error-dialog"):
            yield Static("ERROR!", id="error-title")
            yield Static(self.message, id="error-text")
            with Horizontal(classes="dialog-buttons"):
                yield Button("Retry transfer", id="retry", variant="primary")
                yield Button("Cancel", id="cancel", variant="error")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "retry")


class ChannelSelectScreen(ModalScreen[Optional[ChannelInfo]]):
    def __init__(self, channels: list[ChannelInfo]) -> None:
        super().__init__()
        self.channels = channels
        self.by_id = {f"channel-{channel.index}": channel for channel in channels}

    def compose(self) -> ComposeResult:
        with Container(id="dialog"):
            yield Static("Channels", id="dialog-text")
            options = self._channel_options()
            yield Select(options, value=options[0][1], id="channel-select")
            with Horizontal(classes="dialog-buttons channel-dialog-buttons"):
                yield Button("Connect", id="connect", variant="primary", classes="channel-connect-button")
                yield Button("Cancel", id="cancel", variant="error", classes="channel-cancel-button")

    def on_mount(self) -> None:
        self.query_one("#channel-select", Select).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cancel":
            self.dismiss(None)
            return
        self.dismiss(self._selected_channel())

    def _selected_channel(self) -> Optional[ChannelInfo]:
        value = str(self.query_one("#channel-select", Select).value or "")
        return self.by_id.get(value)

    def _channel_options(self) -> list[tuple[str, str]]:
        if not self.channels:
            return [("No channels found", "")]
        options = []
        for channel in self.channels:
            suffix = "Encrypted" if channel.encrypted else "Unencrypted"
            option_id = f"channel-{channel.index}"
            options.append((f"{channel.index}. {channel.name} ({suffix})", option_id))
        return options


class MessageActionScreen(ModalScreen[str]):
    ACTIONS = ("Reply", "React", "Traceroute", "Back")
    BINDINGS = [("escape", "cancel", "Cancel")]

    def compose(self) -> ComposeResult:
        with Container(id="bbs-dialog"):
            yield OptionList(*(Option(action, id=action.lower()) for action in self.ACTIONS), id="action-list")

    def on_mount(self) -> None:
        self.query_one("#action-list", OptionList).focus()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.dismiss(str(event.option_id or "back"))

    def action_cancel(self) -> None:
        self.dismiss("back")


class EmojiSelectScreen(ModalScreen[str]):
    COLUMNS = 16
    VISIBLE_ROWS = 16
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "select", "Select"),
        ("up", "cursor_up", "Up"),
        ("down", "cursor_down", "Down"),
        ("left", "cursor_left", "Left"),
        ("right", "cursor_right", "Right"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.emojis = _system_emoji_choices()
        self.selected_index = 0
        self.row_offset = 0

    def compose(self) -> ComposeResult:
        with Container(id="emoji-dialog"):
            yield Static("Выбор реакции", id="emoji-title")
            yield Static("", id="emoji-grid", markup=True)
            yield Static("", id="emoji-scroll")

    def on_mount(self) -> None:
        self._render_emojis()

    def action_cancel(self) -> None:
        self.dismiss("")

    def action_select(self) -> None:
        self.dismiss(self.emojis[self.selected_index])

    def action_cursor_up(self) -> None:
        self._move_selection(-self.COLUMNS)

    def action_cursor_down(self) -> None:
        self._move_selection(self.COLUMNS)

    def action_cursor_left(self) -> None:
        self._move_selection(-1)

    def action_cursor_right(self) -> None:
        self._move_selection(1)

    def _move_selection(self, delta: int) -> None:
        self.selected_index = max(0, min(len(self.emojis) - 1, self.selected_index + delta))
        selected_row = self.selected_index // self.COLUMNS
        if selected_row < self.row_offset:
            self.row_offset = selected_row
        elif selected_row >= self.row_offset + self.VISIBLE_ROWS:
            self.row_offset = selected_row - self.VISIBLE_ROWS + 1
        self._render_emojis()

    def _render_emojis(self) -> None:
        grid = []
        start = self.row_offset * self.COLUMNS
        end = min(len(self.emojis), start + self.COLUMNS * self.VISIBLE_ROWS)
        for row_start in range(start, end, self.COLUMNS):
            cells = []
            for index in range(row_start, min(row_start + self.COLUMNS, len(self.emojis))):
                emoji = escape(self.emojis[index])
                if index == self.selected_index:
                    cells.append(f"[black on white] {emoji} [/black on white]")
                else:
                    cells.append(f" {emoji} ")
            grid.append("".join(cells))
        total_rows = max(1, (len(self.emojis) + self.COLUMNS - 1) // self.COLUMNS)
        self.query_one("#emoji-grid", Static).update("\n".join(grid))
        self.query_one("#emoji-scroll", Static).update(
            f"{self.row_offset + 1}-{min(total_rows, self.row_offset + self.VISIBLE_ROWS)} / {total_rows}"
        )


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
                with Horizontal(classes="center-row connect-button-row"):
                    yield Button("CONNECT", id="serial-connect", variant="primary", classes="center-button")

            with Vertical(id="ble-pane", classes="connect-pane"):
                yield Static("=== Paired ===", classes="section-title")
                yield OptionList(id="paired-list")
                yield Static("=== Devices ===", classes="section-title")
                yield OptionList(id="ble-list")
                yield Input(placeholder="PIN code, if required", password=True, id="ble-pin")
                with Horizontal(classes="center-row connect-button-row"):
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
        ("shift+tab", "toggle_focus_back", "Focus back"),
        ("escape", "disconnect", "Disconnect"),
    ]

    TOOLBAR = ("Channels", "Send File", "Traceroute", "Change Device")
    FOCUS_ORDER = ("toolbar", "chat", "nodes", "input")

    def __init__(self) -> None:
        super().__init__()
        self.toolbar_active = False
        self.toolbar_index = 0
        self.node_ids: dict[str, NodeTarget] = {}
        self.transmitting = False
        self.focus_area = "input"
        self.chat_lines: list[ChatLine] = []
        self.selected_chat_index: Optional[int] = None
        self.pending_reply: Optional[ChatRecord] = None

    def compose(self) -> ComposeResult:
        with Vertical(id="dos-root"):
            yield Static("MeshDrop IRC - Meshtastic File Chat Client", id="title-line")
            with Horizontal(id="toolbar"):
                for index, label in enumerate(self.TOOLBAR):
                    yield Button(label, id=f"tool-{index}", classes="tool-button")
                yield Static("DISCONNECTED", id="device-status")
            with Horizontal(id="chat-body"):
                with Vertical(id="chat-panel"):
                    yield Static("", id="chat-topic")
                    yield RichLog(id="chat-log", markup=True, wrap=True, highlight=False)
                    yield Static("", id="transfer-message")
                with Vertical(id="node-panel"):
                    yield Static("NODES", id="nodes-title")
                    yield OptionList(id="nodes")
            with Horizontal(id="input-row"):
                yield Static(">", id="prompt")
                yield Input(placeholder="Type a message", id="message-input")
                yield Static("Recipient: none", id="recipient-status")
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
        self._update_recipient_status()
        self._update_channel_view()
        if self.app.transport is not None:  # type: ignore[attr-defined]
            asyncio.create_task(self.refresh_nodes(quiet=True))

    def on_screen_resume(self) -> None:
        self._set_connected_state()
        self._update_toolbar()
        self._update_recipient_status()
        self._update_channel_view()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id.startswith("tool-"):
            self.toolbar_index = int(button_id.removeprefix("tool-"))
            await self.activate_toolbar_item()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "message-input":
            return
        self.focus_area = "input"
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
        self._update_recipient_status()
        if not self.app.in_channel_mode:  # type: ignore[attr-defined]
            self.write_chat(f"* recipient selected: {node.name} ({node.node_id})")

    async def on_key(self, event) -> None:
        key = event.key
        if key == "tab":
            self._cycle_focus(1)
            event.stop()
            return
        if key in ("shift+tab", "backtab"):
            self._cycle_focus(-1)
            event.stop()
            return
        if key == "alt":
            self._set_focus_area("toolbar")
            event.stop()
            return
        message_input = self.query_one("#message-input", Input)
        if key == "enter" and message_input.has_focus:
            self.focus_area = "input"
            text = message_input.value.strip()
            message_input.value = ""
            if text:
                await self.send_chat_message(text)
            event.stop()
            return
        if self.focus_area == "chat":
            if key == "up":
                self._move_chat_selection(-1)
                event.stop()
                return
            if key == "down":
                self._move_chat_selection(1)
                event.stop()
                return
            if key == "enter":
                self.open_selected_message_actions()
                event.stop()
                return
        if self.toolbar_active:
            if key == "left":
                self.toolbar_index = self._next_toolbar_index(-1)
                self._update_toolbar()
                event.stop()
            elif key == "right":
                self.toolbar_index = self._next_toolbar_index(1)
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
        await self.refresh_nodes(quiet=True)

    def action_toggle_focus(self) -> None:
        self._cycle_focus(1)

    def action_toggle_focus_back(self) -> None:
        self._cycle_focus(-1)

    def action_disconnect(self) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.app.exit()  # type: ignore[attr-defined]
        else:
            self.app.disconnect_device()  # type: ignore[attr-defined]

    async def action_send_file(self) -> None:
        await self.begin_send()

    async def activate_toolbar_item(self) -> None:
        action = self.TOOLBAR[self.toolbar_index]
        self.toolbar_active = False
        self._update_toolbar()
        if action == "Channels":
            await self.open_channels()
        elif action == "Send File" and self.app.in_channel_mode:  # type: ignore[attr-defined]
            self.write_chat("* file transfer is disabled in public channels")
        elif action == "Send File" and self.transmitting:
            self.app.stop_transfer()  # type: ignore[attr-defined]
        elif action == "Send File":
            await self.begin_send()
        elif action == "Traceroute":
            self.start_traceroute()
        elif action == "Change Device":
            self.app.show_connection_dialog()  # type: ignore[attr-defined]

    async def open_channels(self) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Not connected.")  # type: ignore[attr-defined]
            return
        try:
            channels = await self.app.load_channels()  # type: ignore[attr-defined]
        except Exception as exc:
            self.app.show_error(f"ERROR! Could not load channels: {exc}")  # type: ignore[attr-defined]
            return
        self.app.push_screen(ChannelSelectScreen(channels), callback=self._handle_channel_selected)  # type: ignore[attr-defined]

    def _handle_channel_selected(self, channel: Optional[ChannelInfo]) -> None:
        if channel is None:
            return
        self.app.connect_channel(channel)  # type: ignore[attr-defined]
        if self.toolbar_index == 1:
            self.toolbar_index = 2
        self._update_channel_view()
        self._update_toolbar()
        self._update_recipient_status()
        suffix = "Encrypted" if channel.encrypted else "Unencrypted"
        self.write_system(f"#{channel.name} ({suffix})")

    async def refresh_nodes(self, quiet: bool = False) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            if not quiet:
                self.write_chat("* not connected")
            return
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
            signal = _format_node_signal(node)
            option_list.add_option(Option(f"{node.name:<18} {heard:>4} {signal:>18}", id=option_id))
            self.node_ids[option_id] = node
            self.app.nodes_by_key[option_id] = node  # type: ignore[attr-defined]
            self.app.nodes_by_key[node.node_id] = node  # type: ignore[attr-defined]
            self.app.nodes_by_key[node.destination] = node  # type: ignore[attr-defined]
            if isinstance(node.destination, int):
                self.app.nodes_by_key[f"!{node.destination:08x}"] = node  # type: ignore[attr-defined]
        if not nodes:
            option_list.add_option(Option("No nodes heard", id="empty", disabled=True))
            if not quiet:
                self.write_chat("* no nodes heard")
        self._update_recipient_status()

    async def begin_send(self) -> None:
        if self.app.in_channel_mode:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! File transfer is disabled in public channels.")  # type: ignore[attr-defined]
            return
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
        if self.app.transport is not None:  # type: ignore[attr-defined]
            self.write_chat(f"* connected to {self.app.connected_node_name}")  # type: ignore[attr-defined]
            asyncio.create_task(self.refresh_nodes(quiet=True))

    def show_receive_waiting(self, offer: IncomingOffer) -> None:
        metadata = offer.metadata
        self.query_one("#receive-status", Static).display = True
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
        self.query_one("#receive-status", Static).display = True
        self.query_one("#receive-progress", ProgressBar).display = True
        self.query_one("#receive-stats", Static).display = True
        self.query_one("#receive-progress", ProgressBar).update(progress=snapshot.progress * 100)
        self.query_one("#receive-status", Static).update(snapshot.message or snapshot.state)
        self.query_one("#receive-stats", Static).update(_format_transfer_stats(snapshot))
        if snapshot.state == "complete":
            self.write_chat(f"* received {snapshot.file_name}")

    def apply_send_snapshot(self, snapshot: TransferSnapshot) -> None:
        self.query_one("#receive-status", Static).display = True
        self.query_one("#receive-progress", ProgressBar).display = True
        self.query_one("#receive-stats", Static).display = True
        self.query_one("#receive-progress", ProgressBar).update(progress=snapshot.progress * 100)
        self.query_one("#receive-status", Static).update(snapshot.message or snapshot.state)
        self.query_one("#receive-stats", Static).update(_format_transfer_stats(snapshot))
        self._update_transfer_message(snapshot)
        if snapshot.state in {"complete", "error", "stopped"}:
            self.set_transmitting(False)
            self._hide_transfer_message()
            self.clear_receive_waiting()
            if snapshot.state == "complete":
                self.write_chat(f"* sent {snapshot.file_name} to {snapshot.node_name}")

    def _hide_receive_progress(self) -> None:
        self.query_one("#receive-status", Static).display = False
        self.query_one("#receive-progress", ProgressBar).display = False
        self.query_one("#receive-stats", Static).display = False

    def _hide_transfer_message(self) -> None:
        self.query_one("#transfer-message", Static).display = False

    def _update_transfer_message(self, snapshot: TransferSnapshot) -> None:
        transfer_message = self.query_one("#transfer-message", Static)
        transfer_message.display = True
        transfer_message.update(_format_chat_transfer_message(snapshot))

    def _set_connected_state(self) -> None:
        status = self.query_one("#device-status", Static)
        if self.app.transport is None:  # type: ignore[attr-defined]
            status.update("DISCONNECTED")
        else:
            max_width = max(12, status.size.width or 46)
            status.update(self.app.device_status_text(max_width=max_width))  # type: ignore[attr-defined]

    async def send_chat_message(self, text: str) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Not connected.")  # type: ignore[attr-defined]
            return
        if frame_len(text) > 200:
            self.app.show_error("ERROR! Message is longer than 200 bytes.")  # type: ignore[attr-defined]
            return
        target = self.app.selected_node  # type: ignore[attr-defined]
        if self.app.in_channel_mode:  # type: ignore[attr-defined]
            destination = "^all"
        elif target is not None:
            destination = target.destination
        else:
            self.app.show_error("ERROR! Choose a recipient node.")  # type: ignore[attr-defined]
            return
        reply_id = self.pending_reply.packet_id if self.pending_reply is not None else None
        try:
            packet_id = await asyncio.to_thread(
                self.app.transport.send_text,  # type: ignore[attr-defined]
                text,
                destination,
                self.app.channel_index,  # type: ignore[attr-defined]
                False,
                reply_id,
            )
        except Exception:
            self.app._show_local_device_lost()  # type: ignore[attr-defined]
            return
        sender = self.app.local_display_name()  # type: ignore[attr-defined]
        if self.pending_reply is not None:
            self.write_reply_quote(self.pending_reply.sender, self.pending_reply.text)
        record = self.app.remember_chat_message(packet_id, sender, text, own=True, node=target)  # type: ignore[attr-defined]
        self.write_own_message(sender, text, record=record)
        self.pending_reply = None
        self.query_one("#message-input", Input).placeholder = "Type a message"

    def start_traceroute(self, target: Optional[NodeTarget] = None) -> None:
        if target is None:
            target = self.app.selected_node  # type: ignore[attr-defined]
        if target is None:
            self.app.show_error("ERROR! Choose a recipient node.")  # type: ignore[attr-defined]
            return
        asyncio.create_task(self.run_traceroute(target))

    async def run_traceroute(self, target: NodeTarget) -> None:
        if self.app.transport is None:  # type: ignore[attr-defined]
            self.app.show_error("ERROR! Not connected.")  # type: ignore[attr-defined]
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

    def open_selected_message_actions(self) -> None:
        record = self.selected_chat_record()
        if record is None:
            return
        self.app.push_screen(MessageActionScreen(), callback=lambda action: self._handle_message_action(action, record))  # type: ignore[attr-defined]

    def _handle_message_action(self, action: str, record: ChatRecord) -> None:
        if action == "reply":
            if record.packet_id is None:
                self.write_chat("* cannot reply to this message")
                return
            self.pending_reply = record
            message_input = self.query_one("#message-input", Input)
            message_input.placeholder = f"Reply to {record.sender}'s message"
            self._set_focus_area("input")
        elif action == "react":
            if record.packet_id is None:
                self.write_chat("* cannot react to this message")
                return
            self.app.push_screen(EmojiSelectScreen(), callback=lambda emoji: self._send_reaction(record, emoji))  # type: ignore[attr-defined]
        elif action == "traceroute":
            if record.node is None:
                self.write_chat("* traceroute target is unknown")
                return
            self.start_traceroute(record.node)

    def _send_reaction(self, record: ChatRecord, emoji: str) -> None:
        if not emoji:
            return
        asyncio.create_task(self._send_reaction_async(record, emoji))

    async def _send_reaction_async(self, record: ChatRecord, emoji: str) -> None:
        if self.app.transport is None or record.packet_id is None:  # type: ignore[attr-defined]
            return
        destination = "^all" if self.app.in_channel_mode or record.node is None else record.node.destination  # type: ignore[attr-defined]
        try:
            packet_id = await asyncio.to_thread(
                self.app.transport.send_reaction,  # type: ignore[attr-defined]
                emoji,
                record.packet_id,
                destination,
                self.app.channel_index,  # type: ignore[attr-defined]
            )
        except Exception as exc:
            self.app.show_error(f"ERROR! Could not send reaction: {exc}")  # type: ignore[attr-defined]
            return
        if packet_id is not None:
            self.app.sent_reaction_packet_ids.add(packet_id)  # type: ignore[attr-defined]
        self.add_reaction_to_record(record, emoji)

    def write_chat(self, message: str) -> None:
        self.write_system(message)

    def write_system(self, message: str, timestamp: Optional[float] = None) -> None:
        self._append_chat_line(ChatLine("cyan", message, timestamp=timestamp))

    def write_own_message(
        self,
        node_name: str,
        text: str,
        timestamp: Optional[float] = None,
        record: Optional[ChatRecord] = None,
    ) -> None:
        self._append_chat_line(ChatLine("magenta", f"<{node_name}>: {text}", timestamp=timestamp, record=record))

    def write_peer_message(
        self,
        node_name: str,
        text: str,
        timestamp: Optional[float] = None,
        record: Optional[ChatRecord] = None,
    ) -> None:
        self._append_chat_line(ChatLine("white", f"<{node_name}>: {text}", timestamp=timestamp, record=record))

    def write_reply_quote(self, node_name: str, text: str, timestamp: Optional[float] = None) -> None:
        quote = _quote_text(text)
        self._append_chat_line(ChatLine("white", f'@{node_name} "{quote}"', timestamp=timestamp))

    def add_reaction_to_record(self, record: ChatRecord, emoji: str) -> None:
        record.reactions[emoji] = record.reactions.get(emoji, 0) + 1
        self._render_chat_log()

    def _append_chat_line(self, line: ChatLine) -> None:
        self.chat_lines.append(line)
        if line.record is not None:
            self.selected_chat_index = len(self.chat_lines) - 1
        self._render_chat_log()

    def _render_chat_log(self) -> None:
        log = self.query_one("#chat-log", RichLog)
        log.clear()
        for index, line in enumerate(self.chat_lines[-400:]):
            absolute_index = len(self.chat_lines) - min(len(self.chat_lines), 400) + index
            selected = self.focus_area == "chat" and absolute_index == self.selected_chat_index
            text = line.text
            if line.record is not None and line.record.reactions:
                text = _line_with_reactions(text, line.record.reactions)
            log.write(_chat_line(line.style, text, timestamp=line.timestamp, selected=selected))

    def _update_toolbar(self) -> None:
        toolbar = self.query_one("#toolbar", Horizontal)
        toolbar.set_class(self.toolbar_active, "toolbar-active")
        for index, _label in enumerate(self.TOOLBAR):
            button = self.query_one(f"#tool-{index}", Button)
            button.set_class(self.toolbar_active and index == self.toolbar_index, "tool-selected")
            if self.TOOLBAR[index] == "Send File":
                button.disabled = self.app.in_channel_mode and not self.transmitting  # type: ignore[attr-defined]

    def set_transmitting(self, transmitting: bool) -> None:
        self.transmitting = transmitting
        send_button = self.query_one("#tool-1", Button)
        send_button.label = "Stop Sending" if transmitting else "Send File"
        send_button.variant = "error" if transmitting else "default"
        self._update_toolbar()

    def _set_focus_area(self, area: str) -> None:
        self.focus_area = area
        self.toolbar_active = area == "toolbar"
        self.query_one("#node-panel", Vertical).set_class(area == "nodes", "focused-panel")
        self.query_one("#chat-panel", Vertical).set_class(area == "chat", "focused-panel")
        if area == "nodes":
            self.query_one("#nodes", OptionList).focus()
        elif area == "chat":
            self.query_one("#chat-log", RichLog).focus()
            if (
                self.selected_chat_index is None
                or not (0 <= self.selected_chat_index < len(self.chat_lines))
                or self.chat_lines[self.selected_chat_index].record is None
            ):
                self.selected_chat_index = self._last_selectable_chat_index()
            self._render_chat_log()
        elif area == "input":
            self.query_one("#message-input", Input).focus()
        self._update_toolbar()
        if area != "chat":
            self._render_chat_log()

    def _cycle_focus(self, direction: int) -> None:
        index = self.FOCUS_ORDER.index(self.focus_area) if self.focus_area in self.FOCUS_ORDER else 0
        self._set_focus_area(self.FOCUS_ORDER[(index + direction) % len(self.FOCUS_ORDER)])

    def _next_toolbar_index(self, direction: int) -> int:
        index = self.toolbar_index
        for _ in self.TOOLBAR:
            index = (index + direction) % len(self.TOOLBAR)
            if not self.query_one(f"#tool-{index}", Button).disabled:
                return index
        return self.toolbar_index

    def _last_selectable_chat_index(self) -> Optional[int]:
        for index in range(len(self.chat_lines) - 1, -1, -1):
            if self.chat_lines[index].record is not None:
                return index
        return None

    def _move_chat_selection(self, direction: int) -> None:
        if not self.chat_lines:
            return
        index = self.selected_chat_index
        if index is None:
            index = self._last_selectable_chat_index()
        if index is None:
            return
        probe = index
        while 0 <= probe + direction < len(self.chat_lines):
            probe += direction
            if self.chat_lines[probe].record is not None:
                self.selected_chat_index = probe
                self._render_chat_log()
                return

    def selected_chat_record(self) -> Optional[ChatRecord]:
        if self.selected_chat_index is None:
            return None
        if not (0 <= self.selected_chat_index < len(self.chat_lines)):
            return None
        return self.chat_lines[self.selected_chat_index].record

    def clear_nodes(self) -> None:
        self.query_one("#nodes", OptionList).clear_options()
        self.node_ids.clear()
        self.app.nodes_by_key.clear()  # type: ignore[attr-defined]
        self._update_recipient_status()

    def _update_recipient_status(self) -> None:
        status = self.query_one("#recipient-status", Static)
        if self.app.in_channel_mode:  # type: ignore[attr-defined]
            status.display = False
            return
        status.display = True
        node = self.app.selected_node  # type: ignore[attr-defined]
        status.update(f"Recipient: {node.name}" if node is not None else "Recipient: none")

    def _update_channel_view(self) -> None:
        topic = self.query_one("#chat-topic", Static)
        channel = self.app.current_channel  # type: ignore[attr-defined]
        if channel is None:
            topic.update("Direct messages")
            self.query_one("#recipient-status", Static).display = True
            return
        suffix = "Encrypted" if channel.encrypted else "Unencrypted"
        topic.update(f"#{channel.name} ({suffix})")
        self.query_one("#recipient-status", Static).display = False


class MeshShareApp(App):
    CSS = """
    ConnectionScreen, ErrorScreen, RetryTransferScreen, ConfirmSendScreen, ReceiveOfferScreen, ChannelSelectScreen, MessageActionScreen, EmojiSelectScreen {
        align: center middle;
    }

    Screen {
        background: #000000;
        color: #66ff66;
    }

    #dos-root {
        width: 100%;
        height: 100%;
        padding: 0 1;
        background: #000000;
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
        background: #000000;
    }

    #toolbar.toolbar-active {
        border: heavy white;
        background: #000000;
        color: #66ff66;
    }

    .tool-button {
        width: auto;
        min-width: 0;
        background: #000000;
        color: #66ff66;
        border: none;
        margin-right: 2;
    }

    .tool-button:hover, .tool-button:focus, .tool-button.-active,
    .tool-button.-style-default, .tool-button.-style-default:hover,
    .tool-button.-style-default:focus, .tool-button.-style-default.-active {
        background: #000000;
        color: #66ff66;
        background-tint: transparent;
        tint: transparent;
        border: none;
        border-top: none;
        border-bottom: none;
    }

    .tool-button.tool-selected {
        background: white;
        color: black;
        text-style: bold;
    }

    #device-status {
        width: 1fr;
        margin-left: 0;
        content-align: right middle;
        color: #66ff66;
    }

    #chat-body {
        height: 1fr;
        background: #000000;
    }

    #chat-panel {
        width: 1fr;
        height: 100%;
        border: solid #66ff66;
        margin-right: 1;
        background: #000000;
    }

    #chat-panel.focused-panel {
        border: heavy white;
    }

    #chat-topic {
        height: 1;
        color: #33dddd;
        padding: 0 1;
    }

    #chat-log {
        height: 1fr;
        padding: 0 1;
        background: #000000;
        color: #d0d0d0;
        background-tint: transparent;
    }

    #chat-log:focus {
        background: #000000;
        color: #d0d0d0;
        background-tint: transparent;
    }

    #transfer-message {
        height: 4;
        padding: 0 1;
        border-top: solid #66ff66;
        color: #33dddd;
        background: #000000;
    }

    #node-panel {
        width: 34;
        height: 100%;
        border: solid #66ff66;
        background: #000000;
    }

    #node-panel.focused-panel {
        border: heavy white;
    }

    #nodes-title {
        height: 1;
        padding: 0 1;
        color: #66ff66;
        text-style: bold;
    }

    #nodes {
        height: 1fr;
        background: #000000;
        color: #66ff66;
        border: none !important;
        padding: 0;
        background-tint: transparent;
    }

    #nodes:focus {
        background: #000000;
        color: #66ff66;
        background-tint: transparent;
        border: none !important;
    }

    #nodes > .option-list--option-highlighted,
    #nodes:focus > .option-list--option-highlighted,
    #nodes > .option-list--option-hover {
        background: #000000;
        color: white;
        text-style: bold;
    }

    #input-row {
        height: 3;
        border: solid #66ff66;
        margin-top: 1;
        background: #000000;
    }

    #prompt {
        width: 3;
        content-align: center middle;
        color: #66ff66;
    }

    #message-input {
        width: 1fr;
        background: #000000;
        color: #d0d0d0;
        border: none;
        background-tint: transparent;
    }

    #message-input:focus {
        background: #000000;
        color: #d0d0d0;
        border: none;
        background-tint: transparent;
    }

    #message-input > .input--cursor {
        background: #000000;
        color: white;
        text-style: reverse;
    }

    #message-input > .input--selection {
        background: #000000;
        color: white;
        text-style: bold;
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
        max-width: 76;
        height: auto;
        max-height: 32;
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
        height: auto;
        max-height: 22;
    }

    #paired-list, #ble-list, #channel-list {
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

    .connect-button-row {
        margin-top: 1;
    }

    #dialog, #error-dialog {
        width: 72;
        height: auto;
        padding: 1 2;
        background: black;
        color: #66ff66;
        border: thick #66ff66;
    }

    #bbs-dialog {
        width: 24;
        height: auto;
        padding: 1 2;
        background: black;
        color: #66ff66;
        border: thick #66ff66;
    }

    #action-list, #emoji-list {
        height: auto;
        background: black;
        color: #66ff66;
        border: none;
    }

    #emoji-dialog {
        width: 64;
        height: 22;
        padding: 1 2;
        background: black;
        color: #66ff66;
        border: thick #66ff66;
    }

    #emoji-title {
        height: 1;
        color: #33dddd;
        text-style: bold;
        content-align: center middle;
        margin-bottom: 1;
    }

    #emoji-grid {
        height: 16;
        background: black;
        color: #d0d0d0;
    }

    #emoji-scroll {
        height: 1;
        color: #33dddd;
        content-align: right middle;
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

    .channel-dialog-buttons {
        margin-top: 1;
    }

    .channel-connect-button {
        margin-left: 2;
    }

    .channel-cancel-button {
        margin-left: 3;
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
        self.current_channel: Optional[ChannelInfo] = None
        self.in_channel_mode = False
        self.transfer_task: Optional[asyncio.Task[None]] = None
        self._fatal_device_lost = False
        self.chat_messages_by_packet_id: dict[int, ChatRecord] = {}
        self.sent_reaction_packet_ids: set[int] = set()

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
        self.channel_index = 0
        self.current_channel = None
        self.in_channel_mode = False
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

    async def load_channels(self) -> list[ChannelInfo]:
        if self.transport is None:
            raise RuntimeError("not connected")
        return await asyncio.to_thread(self.transport.list_channels)

    def connect_channel(self, channel: ChannelInfo) -> None:
        self.current_channel = channel
        self.channel_index = channel.index
        self.in_channel_mode = True

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
            elif message == SYNC_ERROR:
                self.show_retry_transfer_error(file_path, target, channel_index)
            else:
                self.show_error(message)
        except Exception:
            self._show_local_device_lost()
        finally:
            if isinstance(self.screen, MainMenuScreen):
                self.screen.set_transmitting(False)
            self.transfer_task = None

    def stop_transfer(self) -> None:
        manager = self.manager
        if manager is not None:
            asyncio.create_task(asyncio.to_thread(manager.stop_active_transfer))

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
        self.current_channel = None
        self.in_channel_mode = False
        self.channel_index = 0
        if isinstance(self.screen, MainMenuScreen):
            self.screen.write_chat("* device disconnected")
            self.screen.clear_nodes()
            self.screen.apply_connection_state()

    def device_status_text(self, max_width: int = 46) -> str:
        if self.transport is None:
            return "DISCONNECTED"
        status = self.transport.get_local_status()
        name = getattr(status, "name", "") or self.connected_node_name or "node"
        battery_level = getattr(status, "battery_level", None)
        voltage_value = getattr(status, "voltage", None)
        voltage = _format_voltage(voltage_value)
        if getattr(status, "powered", False) or (battery_level is not None and battery_level > 100):
            power = "[POWERED]" if voltage is None else f"[POWERED] {voltage}"
            return _format_device_status(name, self.connection_kind or "?", power, max_width=max_width)
        battery = "N/A" if battery_level is None else f"{battery_level}%"
        return _format_device_status(name, self.connection_kind or "?", f"{battery} {voltage or '?'}", max_width=max_width)

    def remember_chat_message(
        self,
        packet_id: Optional[int],
        sender: str,
        text: str,
        own: bool,
        node: Optional[NodeTarget] = None,
        timestamp: Optional[float] = None,
    ) -> ChatRecord:
        record = ChatRecord(sender=sender, text=text, own=own, packet_id=packet_id, node=node, timestamp=timestamp)
        if packet_id is not None:
            self.chat_messages_by_packet_id[packet_id] = record
        return record

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
        node = self.peer_node_for_message(message)
        if node is not None:
            return node.name
        sender = getattr(message, "from_id", None) or getattr(message, "from_node_num", None)
        if isinstance(sender, int):
            return f"!{sender:08x}"
        return str(sender or "unknown")

    def peer_node_for_message(self, message) -> Optional[NodeTarget]:
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
                return node
        return None

    def _handle_mesh_message(self, message) -> None:
        try:
            parse_frame(message.text)
        except ProtocolError:
            if isinstance(message.text, str) and is_protocol_like(message.text):
                return
            if self.in_channel_mode and message.channel_index != self.channel_index:
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
            node = self.peer_node_for_message(message)
            if message.emoji and message.reply_id is not None:
                if message.packet_id is not None and message.packet_id in self.sent_reaction_packet_ids:
                    self.sent_reaction_packet_ids.discard(message.packet_id)
                    return
                referenced = self.chat_messages_by_packet_id.get(message.reply_id)
                if referenced is not None:
                    self.screen.add_reaction_to_record(referenced, message.emoji)
                else:
                    self.screen.write_system(
                        f"* reaction {message.emoji} from {sender} to unknown message",
                        timestamp=message.timestamp,
                    )
                return

            referenced = (
                self.chat_messages_by_packet_id.get(message.reply_id)
                if message.reply_id is not None
                else None
            )
            if referenced is not None:
                self.screen.write_reply_quote(
                    referenced.sender,
                    referenced.text,
                    timestamp=message.timestamp,
                )
            record = self.remember_chat_message(
                message.packet_id,
                sender,
                message.text,
                own=False,
                node=node,
                timestamp=message.timestamp,
            )
            self.screen.write_peer_message(sender, message.text, timestamp=message.timestamp, record=record)

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

    def show_retry_transfer_error(self, file_path: Path, target: NodeTarget, channel_index: int) -> None:
        def done(retry: bool) -> None:
            if retry:
                if isinstance(self.screen, MainMenuScreen):
                    self.screen.set_transmitting(True)
                self.transfer_task = asyncio.create_task(self.send_file(file_path, target, channel_index))
            elif isinstance(self.screen, MainMenuScreen):
                self.screen.set_transmitting(False)

        self.push_screen(RetryTransferScreen(SYNC_ERROR), callback=done)

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


def _format_node_signal(node: NodeTarget) -> str:
    snr = "?" if node.snr is None else f"{node.snr:.1f}dB"
    if node.hops_away is None:
        hops = "? Hops"
    elif node.hops_away <= 0:
        hops = "Direct"
    else:
        hops = f"{node.hops_away} Hops"
    return f"{snr} | {hops}"


def _format_device_status(name: str, connection_kind: str, power: str, max_width: int = 46) -> str:
    suffix = f" | {connection_kind} | {power}"
    budget = max(3, max_width - len(suffix))
    return f"{_ellipsize(name, budget)}{suffix}"


def _format_voltage(voltage: Optional[float]) -> Optional[str]:
    if voltage is None or voltage <= 0:
        return None
    return f"{voltage:.2f}V"


def _line_with_reactions(text: str, reactions: dict[str, int], width: int = 92) -> str:
    suffix = _format_reactions(reactions)
    if not suffix:
        return text
    gap = max(1, width - len(text) - len(suffix))
    return f"{text}{' ' * gap}{suffix}"


def _format_reactions(reactions: dict[str, int], max_width: int = 28) -> str:
    parts = [f"{count}x{emoji}" for emoji, count in reactions.items()]
    result: list[str] = []
    used = 0
    for part in parts:
        next_used = used + len(part) + (1 if result else 0)
        if next_used > max_width:
            result.append("...")
            break
        result.append(part)
        used = next_used
    return "|".join(result)


def _ellipsize(text: str, max_length: int) -> str:
    if len(text) <= max_length:
        return text
    if max_length <= 3:
        return "." * max_length
    return text[: max_length - 3].rstrip() + "..."


def _system_emoji_choices() -> tuple[str, ...]:
    emoji_ranges = (
        (0x1F600, 0x1F64F),  # smileys
        (0x1F900, 0x1F9FF),  # faces, hands, people, animals
        (0x1FA70, 0x1FAFF),  # newer symbols
        (0x1F300, 0x1F5FF),  # nature, food, activities, objects
        (0x1F680, 0x1F6FF),  # transport
        (0x2600, 0x27BF),    # symbols and dingbats
        (0x2B00, 0x2BFF),    # arrows and geometric emoji
    )
    choices: list[str] = []
    seen: set[str] = set()
    for start, end in emoji_ranges:
        for codepoint in range(start, end + 1):
            if 0x1F3FB <= codepoint <= 0x1F3FF:
                continue
            char = chr(codepoint)
            name = unicodedata.name(char, "")
            if not name or unicodedata.category(char).startswith("C"):
                continue
            if not _looks_like_emoji_name(name):
                continue
            emoji = f"{char}\ufe0f" if codepoint < 0x2B00 else char
            if emoji not in seen:
                seen.add(emoji)
                choices.append(emoji)
    return tuple(choices)


def _looks_like_emoji_name(name: str) -> bool:
    keywords = (
        "FACE",
        "SMILING",
        "GRINNING",
        "CAT",
        "MONKEY",
        "HEART",
        "HAND",
        "PERSON",
        "MAN",
        "WOMAN",
        "BABY",
        "BODY",
        "ANIMAL",
        "BIRD",
        "FISH",
        "PLANT",
        "FLOWER",
        "TREE",
        "FOOD",
        "DRINK",
        "FRUIT",
        "VEGETABLE",
        "SPORT",
        "BALL",
        "GAME",
        "MUSIC",
        "VEHICLE",
        "CAR",
        "BUS",
        "TRAIN",
        "AIRPLANE",
        "SHIP",
        "BUILDING",
        "HOUSE",
        "OBJECT",
        "SYMBOL",
        "SIGN",
        "MARK",
        "BUTTON",
        "ARROW",
        "STAR",
        "MOON",
        "SUN",
        "CLOUD",
        "FIRE",
        "WARNING",
        "CHECK",
        "CROSS",
        "CIRCLE",
        "SQUARE",
        "DIAMOND",
        "TRIANGLE",
    )
    return any(keyword in name for keyword in keywords)


def _chat_line(style: str, text: str, timestamp: Optional[float] = None, selected: bool = False) -> str:
    if selected:
        return f"[cyan bold]{_chat_time(timestamp)}[/cyan bold] [{style} bold]{escape(text)}[/{style} bold]"
    return f"[cyan]{_chat_time(timestamp)}[/cyan] [{style}]{escape(text)}[/{style}]"


def _chat_time(timestamp: Optional[float] = None) -> str:
    if timestamp is None:
        return datetime.now().strftime("%H:%M")
    return datetime.fromtimestamp(timestamp).strftime("%H:%M")


def _quote_text(text: str, max_length: int = 72) -> str:
    one_line = " ".join(text.split())
    if len(one_line) <= max_length:
        return one_line
    return one_line[: max_length - 3].rstrip() + "..."


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
