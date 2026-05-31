import unittest
from pathlib import Path

from meshshare.protocol import (
    MAX_FRAME_BYTES,
    TransferMetadata,
    decode_data_payload,
    decode_metadata,
    compress_payload,
    decompress_payload,
    encode_accept_frame,
    encode_ack_frame,
    encode_data_frame,
    encode_decline_frame,
    encode_metadata_frames,
    encode_resend_frames,
    encode_sync_request_frame,
    encode_sync_state_frame,
    encode_stop_frame,
    frame_header,
    frame_len,
    is_protocol_like,
    make_metadata,
    parse_frame,
    parse_range_spec,
    sha256_b64,
    split_chunks,
)


class ProtocolTests(unittest.TestCase):
    def test_compact_headers_match_protocol_bit_layout(self):
        expected = {
            "M": "44",
            "D": "74",
            "Y": "4c",
            "N": "70",
            "S": "40",
            "A": "48",
            "R": "50",
            "C": "60",
            "E": "7c",
            "Q": "54",
            "Z": "58",
        }

        for kind, header in expected.items():
            self.assertEqual(frame_header(kind), header)
            self.assertTrue(is_protocol_like(f"{header}|abc123"))

    def test_parser_accepts_legacy_and_compact_headers(self):
        self.assertEqual(parse_frame("MS1|D|abc123|0|3610a686|aGVsbG8").kind, "D")
        compact = encode_data_frame("abc123", 0, b"hello")

        self.assertTrue(compact.startswith("74|"))
        self.assertEqual(parse_frame(compact).kind, "D")
        self.assertFalse(parse_frame(compact).ping)

    def test_data_ping_flag_uses_reserved_compact_bit(self):
        text = encode_data_frame("abc123", 2, b"hello", ping=True)
        frame = parse_frame(text)

        self.assertTrue(text.startswith("76|"))
        self.assertEqual(frame.kind, "D")
        self.assertTrue(frame.ping)

    def test_ack_frame_can_report_ping_chunk(self):
        frame = parse_frame(encode_ack_frame("abc123", 3, 10, ping_index=2))

        self.assertEqual(frame.kind, "A")
        self.assertEqual(frame.verified, 3)
        self.assertEqual(frame.total, 10)
        self.assertEqual(frame.ping_index, 2)

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
            original_size=12345,
            original_sha256=sha256_b64(b"payload"),
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

    def test_compressed_payload_roundtrip(self):
        original = (b"mesh text " * 200)
        payload, compression = compress_payload(original)
        metadata = make_metadata(
            Path("text.txt"),
            payload,
            120,
            original_data=original,
            compression=compression,
        )

        self.assertEqual(compression, "xz")
        self.assertLess(len(payload), len(original))
        self.assertEqual(decompress_payload(payload, metadata), original)

    def test_sync_frames_roundtrip_and_fit(self):
        request = parse_frame(encode_sync_request_frame("abc123", 128, 512, "ff00aa"))
        response = parse_frame(
            encode_sync_state_frame("abc123", 125, 3, 127, [3, 5, 6, 12], "ff00aa")
        )

        self.assertLessEqual(frame_len(encode_sync_request_frame("abc123", 128, 512, "ff00aa")), MAX_FRAME_BYTES)
        self.assertEqual(request.kind, "Q")
        self.assertEqual(request.sent, 128)
        self.assertEqual(response.kind, "Z")
        self.assertEqual(response.missing, (3, 5, 6, 12))


if __name__ == "__main__":
    unittest.main()
