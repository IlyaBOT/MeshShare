import tempfile
import unittest
from pathlib import Path
import time

from meshshare.file_transfer import FileTransferManager, _same_sender
from meshshare.protocol import (
    encode_data_frame,
    encode_accept_frame,
    encode_sync_request_frame,
    encode_metadata_frames,
    encode_complete_frame,
    compress_payload,
    make_metadata,
    parse_frame,
    split_chunks,
)
from meshshare.transport import MeshMessage
from meshshare.file_transfer import TransferError, TIMEOUT_ERROR, STOPPED_ERROR


class FakeTransport:
    def __init__(self):
        self.sent = []

    def send_text(self, text, destination_id, channel_index=0, want_ack=True):
        self.sent.append(text)

    def get_signal(self, destination):
        return None


class FileTransferHandshakeTests(unittest.TestCase):
    def test_sender_identity_matches_node_num_and_node_id(self):
        self.assertTrue(_same_sender("!0000002a", 42))
        self.assertTrue(_same_sender(42, "!0000002a"))
        self.assertFalse(_same_sender("!0000002a", 43))

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

    def test_receiver_decompresses_payload_before_saving_file(self):
        original = b"meshshare text payload " * 200
        payload, compression = compress_payload(original)
        self.assertEqual(compression, "xz")
        session_id = "zip001"
        metadata = make_metadata(
            Path("compressed.txt"),
            payload,
            120,
            original_data=original,
            compression=compression,
        )
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
                for index, chunk in enumerate(split_chunks(payload, 120)):
                    manager.handle_message(_message(encode_data_frame(session_id, index, chunk)))

                self.assertEqual((Path(temp_dir) / "compressed.txt").read_bytes(), original)
            finally:
                manager.close()

    def test_receiver_sync_state_reports_missing_chunks(self):
        data = b"abcdefghijklmnopqrstuvwxyz" * 20
        session_id = "sync01"
        metadata = make_metadata(Path("sync.txt"), data, 120)
        chunks = split_chunks(data, 120)
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
                for index in (0, 1, 3):
                    manager.handle_message(_message(encode_data_frame(session_id, index, chunks[index])))
                manager.handle_message(_message(encode_sync_request_frame(session_id, 4, len(chunks), "nonce1")))

                sync_frames = [parse_frame(text) for text in transport.sent if parse_frame(text).kind == "Z"]
                self.assertEqual(sync_frames[-1].received, 3)
                self.assertEqual(sync_frames[-1].first_missing, 2)
                self.assertEqual(sync_frames[-1].missing, (2,))
            finally:
                manager.close()


    def test_receiver_finishes_zero_byte_file_on_complete_frame(self):
        data = b""
        session_id = "empty1"
        metadata = make_metadata(Path("empty.txt"), data, 120)
        transport = FakeTransport()
        snapshots = []
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = FileTransferManager(
                transport=transport,
                download_dir=Path(temp_dir),
                on_incoming_offer=lambda offer: True,
                on_snapshot=snapshots.append,
            )
            try:
                for text in encode_metadata_frames(session_id, metadata):
                    manager.handle_message(_message(text))
                manager.handle_message(_message(encode_complete_frame(session_id, metadata.sha256)))

                self.assertEqual((Path(temp_dir) / "empty.txt").read_bytes(), data)
                self.assertEqual(snapshots[-1].state, "complete")
                self.assertIn("C", [parse_frame(text).kind for text in transport.sent])
            finally:
                manager.close()

    def test_receiver_requests_resend_when_complete_arrives_before_data(self):
        data = b"not empty"
        session_id = "late01"
        metadata = make_metadata(Path("late.txt"), data, 120)
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
                manager.handle_message(_message(encode_complete_frame(session_id, metadata.sha256)))

                self.assertFalse((Path(temp_dir) / "late.txt").exists())
                self.assertIn("R", [parse_frame(text).kind for text in transport.sent])
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

    def test_stop_interrupts_packet_delay_without_hanging(self):
        payload = b"x" * 400
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
                complete_timeout=5,
                offer_timeout=5,
                packet_delay=5,
            )
            result = []
            try:
                def run_send():
                    try:
                        manager.send_file(source, target, 0)
                    except TransferError as exc:
                        result.append(str(exc))

                import threading
                thread = threading.Thread(target=run_send)
                thread.start()
                deadline = time.monotonic() + 2
                while not transport.sent and time.monotonic() < deadline:
                    time.sleep(0.01)
                session_id = parse_frame(transport.sent[0]).session_id
                manager.handle_message(_message(encode_accept_frame(session_id), from_id="!real"))
                time.sleep(0.05)

                started = time.monotonic()
                manager.stop_active_transfer()
                thread.join(timeout=1)

                self.assertFalse(thread.is_alive())
                self.assertLess(time.monotonic() - started, 1)
                self.assertIn(STOPPED_ERROR, result)
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
