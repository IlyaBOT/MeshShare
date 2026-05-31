import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from meshtastic import mesh_pb2
from meshshare.settings import SavedSettings
from meshshare.transport import _send_traceroute_with_result, parse_tcp_endpoint


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


if __name__ == "__main__":
    unittest.main()
