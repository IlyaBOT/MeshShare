import unittest
from pathlib import Path

from meshshare.protocol import (
    MAX_FRAME_BYTES,
    TransferMetadata,
    decode_data_payload,
    decode_metadata,
    encode_accept_frame,
    encode_data_frame,
    encode_decline_frame,
    encode_metadata_frames,
    encode_resend_frames,
    encode_stop_frame,
    frame_len,
    parse_frame,
    parse_range_spec,
    sha256_b64,
    split_chunks,
)


class ProtocolTests(unittest.TestCase):
    def test_data_frames_fit_under_200_bytes_and_roundtrip(self):
        data = bytes(range(256)) * 3
        session_id = "abc123"
        chunks = split_chunks(data, 120)

        restored = bytearray()
        for index, chunk in enumerate(chunks):
            text = encode_data_frame(session_id, index, chunk)
            self.assertLessEqual(frame_len(text), MAX_FRAME_BYTES)
            frame = parse_frame(text)
            self.assertEqual(frame.kind, "D")
            restored.extend(decode_data_payload(frame))

        self.assertEqual(bytes(restored), data)

    def test_metadata_frames_roundtrip(self):
        metadata = TransferMetadata(
            name="very-long-file-name-" * 4 + ".bin",
            size=12345,
            total=103,
            chunk=120,
            sha256=sha256_b64(b"payload"),
        )
        frames = [parse_frame(text) for text in encode_metadata_frames("fff001", metadata)]
        for text_frame in encode_metadata_frames("fff001", metadata):
            self.assertLessEqual(frame_len(text_frame), MAX_FRAME_BYTES)

        restored = decode_metadata(frames)
        self.assertEqual(restored, metadata)

    def test_bad_crc_is_rejected(self):
        text = encode_data_frame("abc123", 0, b"hello")
        corrupted = text[:-1] + ("A" if text[-1] != "A" else "B")
        frame = parse_frame(corrupted)
        with self.assertRaises(Exception):
            decode_data_payload(frame)

    def test_resend_ranges_roundtrip_and_fit(self):
        indices = [0, 1, 2, 5, 6, 9, 40, 41, 42, 1000]
        frames = encode_resend_frames("abc123", indices)
        self.assertTrue(frames)
        restored = []
        for text in frames:
            self.assertLessEqual(frame_len(text), MAX_FRAME_BYTES)
            frame = parse_frame(text)
            self.assertEqual(frame.kind, "R")
            restored.extend(frame.indices)
        self.assertEqual(sorted(set(restored)), indices)

    def test_parse_range_spec(self):
        self.assertEqual(parse_range_spec("0-2,5,a-c"), (0, 1, 2, 5, 10, 11, 12))

    def test_handshake_frames_roundtrip_and_fit(self):
        frames = [
            encode_accept_frame("abc123"),
            encode_decline_frame("abc123", "no thanks"),
            encode_stop_frame("abc123", "user stopped"),
        ]
        kinds = ["Y", "N", "S"]
        for text, kind in zip(frames, kinds):
            self.assertLessEqual(frame_len(text), MAX_FRAME_BYTES)
            self.assertEqual(parse_frame(text).kind, kind)


if __name__ == "__main__":
    unittest.main()
