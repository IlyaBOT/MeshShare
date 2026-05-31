import tempfile
import unittest
from pathlib import Path

from meshshare.file_transfer import FileTransferManager
from meshshare.protocol import (
    encode_data_frame,
    encode_accept_frame,
    encode_metadata_frames,
    make_metadata,
    parse_frame,
)
from meshshare.transport import MeshMessage
from meshshare.file_transfer import TransferError, TIMEOUT_ERROR


class FakeTransport:
    def __init__(self):
        self.sent = []

    def send_text(self, text, destination_id, channel_index=0, want_ack=True):
        self.sent.append(text)

    def get_signal(self, destination):
        return None


class FileTransferHandshakeTests(unittest.TestCase):
    def test_receiver_accepts_offer_before_writing_file(self):
        data = b"hello mesh"
        session_id = "abc123"
        metadata = make_metadata(Path("hello.txt"), data, 120)
        transport = FakeTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FileTransferManager(
                transport=transport,
                download_dir=Path(temp_dir),
                on_incoming_offer=lambda offer: True,
            )
            try:
                for text in encode_metadata_frames(session_id, metadata):
                    manager.handle_message(_message(text))
                self.assertEqual(parse_frame(transport.sent[0]).kind, "Y")

                manager.handle_message(_message(encode_data_frame(session_id, 0, data)))

                self.assertEqual((Path(temp_dir) / "hello.txt").read_bytes(), data)
                self.assertIn("C", [parse_frame(text).kind for text in transport.sent])
            finally:
                manager.close()

    def test_receiver_declines_offer_and_ignores_data(self):
        data = b"blocked"
        session_id = "abc123"
        metadata = make_metadata(Path("blocked.txt"), data, 120)
        transport = FakeTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FileTransferManager(
                transport=transport,
                download_dir=Path(temp_dir),
                on_incoming_offer=lambda offer: False,
            )
            try:
                for text in encode_metadata_frames(session_id, metadata):
                    manager.handle_message(_message(text))
                manager.handle_message(_message(encode_data_frame(session_id, 0, data)))

                kinds = [parse_frame(text).kind for text in transport.sent]
                self.assertEqual(kinds[:2], ["N", "S"])
                self.assertFalse((Path(temp_dir) / "blocked.txt").exists())
            finally:
                manager.close()

    def test_data_before_offer_is_ignored(self):
        data = b"manual packet"
        transport = FakeTransport()
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FileTransferManager(
                transport=transport,
                download_dir=Path(temp_dir),
                on_incoming_offer=lambda offer: True,
            )
            try:
                manager.handle_message(_message(encode_data_frame("abc123", 0, data)))
                self.assertEqual(transport.sent, [])
                self.assertFalse(any(Path(temp_dir).iterdir()))
            finally:
                manager.close()

    def test_forged_accept_from_wrong_sender_is_ignored(self):
        payload = Path(__file__).read_bytes()[:16]
        source = None
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "payload.bin"
            source.write_bytes(payload)
            transport = FakeTransport()
            target = type("Target", (), {
                "destination": "!real",
                "node_id": "!real",
                "name": "real",
                "snr": None,
            })()
            manager = FileTransferManager(
                transport=transport,
                download_dir=Path(temp_dir) / "rx",
                complete_timeout=0.2,
                packet_delay=0,
            )
            result = []
            try:
                def run_send():
                    try:
                        manager.send_file(source, target, 0)
                    except TransferError as exc:
                        result.append(str(exc))
                    except Exception as exc:
                        result.append(type(exc).__name__)

                import threading
                thread = threading.Thread(target=run_send)
                thread.start()
                import time
                time.sleep(0.05)
                session_id = parse_frame(transport.sent[0]).session_id
                manager.handle_message(_message(encode_accept_frame(session_id), from_id="!attacker"))
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(result, [TIMEOUT_ERROR])
            finally:
                manager.close()


def _message(text, from_id="!sender"):
    return MeshMessage(
        text=text,
        from_id=from_id,
        from_node_num=None,
        rx_snr=None,
        packet_id=None,
        channel_index=0,
    )


if __name__ == "__main__":
    unittest.main()
