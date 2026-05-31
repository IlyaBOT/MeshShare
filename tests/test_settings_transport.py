import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from meshtastic import mesh_pb2
from meshshare.app import ChatRecord, MainMenuScreen, MeshShareApp
from meshshare.app import (
    _format_device_status,
    _format_reactions,
    _connection_error_message,
    _ble_pin_prompt_supported,
    _line_with_reactions,
    _looks_like_pairing_required,
    _format_transfer_left,
    _format_transfer_right,
    _system_emoji_choices,
)
from meshshare.file_transfer import TransferSnapshot
from meshshare.settings import SavedSettings
from meshshare.transport import (
    ChannelInfo,
    LocalNodeStatus,
    MeshtasticTransport,
    NodeTarget,
    _ble_client_target,
    _ble_pair_before_connect,
    _characteristic_uuid,
    _exception_chain_text as _transport_exception_chain_text,
    _looks_like_ble_encryption_error,
    _prepare_macos_ble_connection,
    _retry_ble_pairing_operation,
    _resolve_characteristic,
    _wrap_ble_client,
    _send_traceroute_with_result,
    parse_tcp_endpoint,
)


class TransportSettingsTests(unittest.TestCase):
    def test_parse_tcp_endpoint_defaults(self):
        self.assertEqual(parse_tcp_endpoint("", 4403, False), ("localhost", 4403))
        self.assertEqual(parse_tcp_endpoint("", 4403, True), ("localhost", 443))
        self.assertEqual(parse_tcp_endpoint("192.168.1.20:4403", 4403, False), ("192.168.1.20", 4403))
        self.assertEqual(parse_tcp_endpoint("https://node.local", 4403, True), ("node.local", 443))
        self.assertEqual(parse_tcp_endpoint("http://node.local:8080", 4403, False), ("node.local", 8080))

    def test_settings_roundtrip_and_bluetooth_dedup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "settings.json"
            settings = SavedSettings()
            settings.last_kind = "ble"
            settings.serial_device = "COM7"
            settings.serial_baudrate = 230400
            settings.remember_bluetooth("AA:BB", "Node A")
            settings.remember_bluetooth("AA:BB", "Node A renamed")
            settings.save(path)

            restored = SavedSettings.load(path)

        self.assertEqual(restored.last_kind, "ble")
        self.assertEqual(restored.serial_device, "COM7")
        self.assertEqual(restored.serial_baudrate, 230400)
        self.assertEqual(len(restored.bluetooth_devices), 1)
        self.assertEqual(restored.bluetooth_devices[0].name, "Node A renamed")
        self.assertEqual(restored.bluetooth_devices[0].address, "AA:BB")


class TracerouteFormattingTests(unittest.TestCase):
    def test_send_traceroute_returns_named_tx_and_rx_routes(self):
        route = mesh_pb2.RouteDiscovery()
        route.route.extend([2, 3])
        route.snr_towards.extend([28, 24, 20])
        route.route_back.extend([3, 2])
        route.snr_back.extend([18, 16, 14])

        class FakeInterface:
            def __init__(self):
                self.localNode = SimpleNamespace(nodeNum=1)
                self._acknowledgment = SimpleNamespace(receivedTraceRoute=False)
                self.nodesByNum = {
                    1: {"user": {"longName": "Our Node", "id": "!00000001"}},
                    2: {"user": {"longName": "Hop A", "id": "!00000002"}},
                    3: {"user": {"longName": "Hop B", "id": "!00000003"}},
                    4: {"user": {"longName": "Peer", "id": "!00000004"}},
                }
                self.nodes = {}

            def sendTraceRoute(self, dest, hopLimit, channelIndex):
                self.onResponseTraceRoute(
                    {
                        "decoded": {"payload": route.SerializeToString()},
                        "to": 1,
                        "from": 4,
                        "hopStart": 1,
                    }
                )

            def getLongName(self):
                return "Our Node"

            def _nodeNumToId(self, node_num, no_debug):
                return f"!{node_num:08x}"

        result = _send_traceroute_with_result(FakeInterface(), 4, 0, 7)

        self.assertEqual(
            result.tx,
            "Our Node -> Hop A (7.0 dB) -> Hop B (6.0 dB) -> Peer (5.0 dB)",
        )
        self.assertEqual(
            result.rx,
            "Our Node <- Hop A (3.5 dB) <- Hop B (4.0 dB) <- Peer (4.5 dB)",
        )


class MessageMetadataTests(unittest.TestCase):
    def test_receive_text_extracts_reply_reaction_and_timestamp(self):
        messages = []
        transport = MeshtasticTransport(on_message=messages.append)

        transport._on_receive(
            {
                "decoded": {
                    "text": "reply",
                    "replyId": 101,
                    "emoji": ord("\U0001f44d"),
                },
                "from": 55,
                "fromId": "!00000037",
                "id": 202,
                "rxTime": 1_700_000_000,
                "channel": 3,
            }
        )

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].reply_id, 101)
        self.assertEqual(messages[0].emoji, "\U0001f44d")
        self.assertEqual(messages[0].timestamp, 1_700_000_000)
        self.assertEqual(messages[0].packet_id, 202)

    def test_receive_reaction_without_text_payload(self):
        messages = []
        transport = MeshtasticTransport(on_message=messages.append)

        transport._on_receive(
            {
                "decoded": {
                    "replyId": 101,
                    "emoji": ord("\U0001f44d"),
                },
                "from": 55,
                "fromId": "!00000037",
                "id": 202,
            }
        )

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].text, "")
        self.assertEqual(messages[0].reply_id, 101)
        self.assertEqual(messages[0].emoji, "\U0001f44d")

    def test_send_text_returns_packet_id(self):
        class FakeInterface:
            def __init__(self):
                self.reply_id = None

            def sendText(self, text, destinationId, wantAck, channelIndex, replyId=None):
                self.reply_id = replyId
                return SimpleNamespace(id=303)

        transport = MeshtasticTransport()
        interface = FakeInterface()
        transport.interface = interface

        self.assertEqual(transport.send_text("hello", "^all", reply_id=101), 303)
        self.assertEqual(interface.reply_id, 101)

    def test_send_reaction_builds_text_message_emoji_packet(self):
        class FakeInterface:
            def __init__(self):
                self.sent_packet = None

            def _generatePacketId(self):
                return 404

            def _sendPacket(self, packet, destinationId, wantAck=False):
                self.sent_packet = packet
                return SimpleNamespace(id=packet.id)

        interface = FakeInterface()
        transport = MeshtasticTransport()
        transport.interface = interface

        self.assertEqual(transport.send_reaction("\U0001f44d", 101, "!peer", 2), 404)
        self.assertEqual(interface.sent_packet.decoded.reply_id, 101)
        self.assertEqual(interface.sent_packet.decoded.emoji, ord("\U0001f44d"))
        self.assertEqual(interface.sent_packet.channel, 2)

    def test_list_channels_reads_local_node_channels(self):
        channel0 = SimpleNamespace(
            index=0,
            role=1,
            settings=SimpleNamespace(name="Primary", psk=b"\x01"),
        )
        channel1 = SimpleNamespace(
            index=1,
            role=2,
            settings=SimpleNamespace(name="secret", psk=b"abcd"),
        )
        transport = MeshtasticTransport()
        transport.interface = SimpleNamespace(localNode=SimpleNamespace(channels=[channel1, channel0]))

        self.assertEqual(
            transport.list_channels(),
            [
                ChannelInfo(index=0, name="Primary", encrypted=False),
                ChannelInfo(index=1, name="secret", encrypted=True),
            ],
        )


class DeviceStatusTests(unittest.TestCase):
    def test_powered_status_uses_powered_label(self):
        class FakeTransport:
            def get_local_status(self):
                return LocalNodeStatus(
                    name="Bench Node",
                    battery_level=101,
                    voltage=5.0,
                    powered=True,
                )

        app = MeshShareApp(Path("temp"), 120, 1.1)
        app.transport = FakeTransport()
        app.connection_kind = "Serial"

        self.assertEqual(app.device_status_text(), "Bench Node | Serial | [POWERED] 5.00V")

    def test_device_status_truncates_name_before_connection_and_power(self):
        status = _format_device_status("Very Long Meshtastic Node Name", "Bluetooth", "90% 4.05V", max_width=37)

        self.assertEqual(status, "Very Long... | Bluetooth | 90% 4.05V")


class BluetoothPairingErrorTests(unittest.TestCase):
    def test_pairing_required_detection_ignores_core_bluetooth_unavailable(self):
        self.assertTrue(_looks_like_pairing_required(RuntimeError("Authentication failed: passkey required")))
        self.assertTrue(_looks_like_pairing_required(RuntimeError("Encryption is insufficient.")))
        self.assertFalse(_looks_like_pairing_required(RuntimeError("Pairing is not available in Core Bluetooth")))

    def test_pin_prompt_is_disabled_on_macos(self):
        with patch("meshshare.app.sys.platform", "darwin"):
            self.assertFalse(_ble_pin_prompt_supported())

    def test_encryption_error_explains_macos_pairing(self):
        message = _connection_error_message(RuntimeError("Encryption is insufficient."))
        self.assertIn("system pairing prompt", message)

    def test_ble_client_target_uses_address_on_macos(self):
        device = object()
        with patch("meshshare.transport.sys.platform", "darwin"):
            self.assertEqual(_ble_client_target("AA:BB", device), "AA:BB")

    def test_ble_pair_before_connect_enabled_on_macos(self):
        with patch("meshshare.transport.sys.platform", "darwin"):
            self.assertTrue(_ble_pair_before_connect())

    def test_macos_ble_wrapper_disables_optional_log_notifications(self):
        class FakeClient:
            def __init__(self):
                self.write_kwargs = None

            def has_characteristic(self, specifier):
                return True

            def write_gatt_char(self, *args, **kwargs):
                self.write_kwargs = kwargs
                return "written"

        fake_client = FakeClient()
        fake_module = SimpleNamespace(LEGACY_LOGRADIO_UUID="legacy-log", LOGRADIO_UUID="log-radio", TORADIO_UUID="to-radio")
        with patch("meshshare.transport.sys.platform", "darwin"):
            wrapped = _wrap_ble_client(fake_client, fake_module)

        self.assertFalse(wrapped.has_characteristic("legacy-log"))
        self.assertFalse(wrapped.has_characteristic("log-radio"))
        self.assertTrue(wrapped.has_characteristic("from-num"))
        self.assertEqual(wrapped.write_gatt_char("to-radio", b"x"), "written")

    def test_macos_ble_wrapper_prefers_toradio_write_without_response(self):
        class FakeCharacteristic:
            uuid = "to-radio"
            properties = ["write", "write-without-response"]

        class FakeClient:
            def __init__(self):
                self.write_kwargs = None

            def write_gatt_char(self, *args, **kwargs):
                self.write_kwargs = kwargs

        fake_client = FakeClient()
        fake_module = SimpleNamespace(LEGACY_LOGRADIO_UUID="legacy-log", LOGRADIO_UUID="log-radio", TORADIO_UUID="to-radio")
        with patch("meshshare.transport.sys.platform", "darwin"):
            wrapped = _wrap_ble_client(fake_client, fake_module)
        wrapped.write_gatt_char(FakeCharacteristic(), b"x", response=True)

        self.assertEqual(fake_client.write_kwargs, {"response": False})

    def test_macos_ble_wrapper_resolves_string_toradio_characteristic(self):
        class FakeCharacteristic:
            uuid = "to-radio"
            properties = ["write", "write-without-response"]

        class FakeServices:
            def get_characteristic(self, specifier):
                return FakeCharacteristic() if specifier == "to-radio" else None

        class FakeClient:
            def __init__(self):
                self.write_kwargs = None
                self.bleak_client = SimpleNamespace(services=FakeServices())

            def write_gatt_char(self, *args, **kwargs):
                self.write_kwargs = kwargs

        fake_client = FakeClient()
        fake_module = SimpleNamespace(LEGACY_LOGRADIO_UUID="legacy-log", LOGRADIO_UUID="log-radio", TORADIO_UUID="to-radio")
        with patch("meshshare.transport.sys.platform", "darwin"):
            wrapped = _wrap_ble_client(fake_client, fake_module)
        wrapped.write_gatt_char("to-radio", b"x", response=True)

        self.assertEqual(fake_client.write_kwargs, {"response": False})
        self.assertIsInstance(_resolve_characteristic(fake_client, "to-radio"), FakeCharacteristic)

    def test_characteristic_uuid_accepts_string_or_characteristic(self):
        self.assertEqual(_characteristic_uuid("ABC"), "abc")
        self.assertEqual(_characteristic_uuid(SimpleNamespace(uuid="ABC")), "abc")

    def test_ble_encryption_error_detection_reads_exception_causes(self):
        inner = RuntimeError("CBATTErrorDomain Code=15")
        outer = RuntimeError("Error writing BLE")
        outer.__cause__ = inner

        self.assertIn("CBATTErrorDomain", _transport_exception_chain_text(outer))
        self.assertTrue(_looks_like_ble_encryption_error(outer))

    def test_prepare_macos_ble_connection_starts_fromnum_notification(self):
        class FakeClient:
            def __init__(self):
                self.notifications = []

            def has_characteristic(self, specifier):
                return True

            def start_notify(self, specifier, handler):
                self.notifications.append((specifier, handler))

        client = FakeClient()
        module = SimpleNamespace(FROMRADIO_UUID="from-radio", FROMNUM_UUID="from-num")
        handler = object()
        with patch("meshshare.transport.sys.platform", "darwin"):
            _prepare_macos_ble_connection(client, module, handler)

        self.assertEqual([item[0] for item in client.notifications], ["from-num"])
        self.assertIs(client.notifications[0][1], handler)

    def test_ble_encryption_error_detection(self):
        self.assertTrue(_looks_like_ble_encryption_error(RuntimeError("CBATTErrorDomain Code=15")))
        self.assertTrue(_looks_like_ble_encryption_error(RuntimeError("Encryption is insufficient.")))
        self.assertFalse(_looks_like_ble_encryption_error(RuntimeError("Not connected")))

    def test_ble_pairing_operation_retries_encryption_error(self):
        calls = 0

        def operation():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("Encryption is insufficient.")
            return "ok"

        result = _retry_ble_pairing_operation(operation, attempts=2, delay_seconds=0)

        self.assertEqual(result, "ok")
        self.assertEqual(calls, 2)


class MainMenuNodeLoadingTests(unittest.IsolatedAsyncioTestCase):
    async def test_initial_nodes_load_while_connection_dialog_is_active(self):
        node = NodeTarget(destination="!00000002", node_id="!00000002", name="Peer")

        class FakeTransport:
            def is_connected(self):
                return True

            def get_local_status(self):
                return LocalNodeStatus(name="Local")

            def list_nodes(self):
                return [node]

            def close(self):
                pass

        app = MeshShareApp(Path("temp"), 120, 1.1)
        async with app.run_test() as pilot:
            await pilot.pause(0.2)
            screen = app.main_menu_screen()
            self.assertIsNotNone(screen)
            self.assertNotIsInstance(app.screen, MainMenuScreen)
            assert screen is not None

            app.transport = FakeTransport()
            app.connection_kind = "Serial"
            app.connected_node_name = "Local"
            screen.apply_connection_state()
            await pilot.pause(0.2)

            self.assertEqual(screen.node_ids["node-0"], node)
            self.assertTrue(app.nodes_loaded)

    async def test_initial_node_load_retries_empty_first_result(self):
        node = NodeTarget(destination="!00000002", node_id="!00000002", name="Peer")

        class FakeTransport:
            def __init__(self):
                self.calls = 0

            def is_connected(self):
                return True

            def get_local_status(self):
                return LocalNodeStatus(name="Local")

            def list_nodes(self):
                self.calls += 1
                return [] if self.calls == 1 else [node]

            def close(self):
                pass

        app = MeshShareApp(Path("temp"), 120, 1.1)
        async with app.run_test() as pilot:
            await pilot.pause(0.2)
            await pilot.press("escape")
            screen = app.main_menu_screen()
            assert screen is not None

            app.transport = FakeTransport()
            app.connection_kind = "Serial"
            app.connected_node_name = "Local"
            await screen.load_initial_nodes(attempts=2, delay_seconds=0)

            self.assertEqual(screen.node_ids["node-0"], node)
            self.assertTrue(app.nodes_loaded)


class EmojiPickerDataTests(unittest.TestCase):
    def test_system_emoji_choices_are_grouped_and_large_enough(self):
        emojis = _system_emoji_choices()

        self.assertGreater(len(emojis), 500)
        self.assertIn("\U0001f600", emojis[:32])
        self.assertIn("\U0001f44d", emojis)


class ReactionFormattingTests(unittest.TestCase):
    def test_reactions_render_as_compact_suffix(self):
        record = ChatRecord(sender="Peer", text="Ping", own=False)
        record.reactions["\U0001f44d"] = 2
        record.reactions["\U0001f643"] = 1
        record.reactions["\U0001f601"] = 1

        suffix = _format_reactions(record.reactions)

        self.assertEqual(suffix, "2x\U0001f44d|1x\U0001f643|1x\U0001f601")
        self.assertEqual(_line_with_reactions("<Peer>: Ping", record.reactions, width=20), "<Peer>: Ping 2x\U0001f44d|1x\U0001f643|1x\U0001f601")


class TransferPanelFormattingTests(unittest.TestCase):
    def test_transfer_panel_splits_left_and_right_status(self):
        snapshot = TransferSnapshot(
            direction="send",
            file_name="voicemessage.mp3",
            file_size=65228,
            total_chunks=539,
            verified_chunks=80,
            sent_chunks=166,
            signal_db=7.0,
            ping_ms=123.4,
            elapsed_seconds=295,
            eta_seconds=1078,
        )

        self.assertEqual(
            _format_transfer_left(snapshot),
            '"voicemessage.mp3"\nA: 80 / S: 166 / T: 539 packets',
        )
        self.assertIn("Signal: 7.0 dB", _format_transfer_right(snapshot))
        self.assertIn("Ping: 123ms", _format_transfer_right(snapshot))


if __name__ == "__main__":
    unittest.main()
