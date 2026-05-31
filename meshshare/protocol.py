from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import secrets
import lzma
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Literal, Union

PREFIX = "MS1"
MAX_FRAME_BYTES = 200
DEFAULT_RAW_CHUNK_BYTES = 120
MIN_RAW_CHUNK_BYTES = 24
MAX_METADATA_PARTS = 8
MAX_FILENAME_BYTES = 128
MAX_SHA256_B64_LEN = 43

BASE36_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"
COMPACT_PROTOCOL_BITS = 0b01
COMPACT_RESERVED_BITS = 0b00
COMPACT_PING_FLAG = 0b10
FRAME_KIND_CODES = {
    "S": 0b0000,
    "M": 0b0001,
    "A": 0b0010,
    "Y": 0b0011,
    "R": 0b0100,
    "Q": 0b0101,
    "Z": 0b0110,
    "C": 0b1000,
    "N": 0b1100,
    "D": 0b1101,
    "E": 0b1111,
}
FRAME_CODES_KIND = {code: kind for kind, code in FRAME_KIND_CODES.items()}


class ProtocolError(ValueError):
    """Raised when a MeshShare frame is malformed or invalid."""


@dataclass(frozen=True)
class TransferMetadata:
    name: str
    size: int
    total: int
    chunk: int
    sha256: str
    compression: str = "none"
    original_size: int = 0
    original_sha256: str = ""


@dataclass(frozen=True)
class MetaFrame:
    kind: Literal["M"]
    session_id: str
    part: int
    count: int
    payload: str


@dataclass(frozen=True)
class DataFrame:
    kind: Literal["D"]
    session_id: str
    index: int
    crc32: str
    payload: str
    ping: bool = False


@dataclass(frozen=True)
class AckFrame:
    kind: Literal["A"]
    session_id: str
    verified: int
    total: int
    ping_index: int | None = None


@dataclass(frozen=True)
class ResendFrame:
    kind: Literal["R"]
    session_id: str
    indices: tuple[int, ...]


@dataclass(frozen=True)
class AcceptFrame:
    kind: Literal["Y"]
    session_id: str


@dataclass(frozen=True)
class DeclineFrame:
    kind: Literal["N"]
    session_id: str
    message: str


@dataclass(frozen=True)
class StopFrame:
    kind: Literal["S"]
    session_id: str
    reason: str


@dataclass(frozen=True)
class CompleteFrame:
    kind: Literal["C"]
    session_id: str
    sha256: str


@dataclass(frozen=True)
class ErrorFrame:
    kind: Literal["E"]
    session_id: str
    code: str
    message: str


@dataclass(frozen=True)
class SyncRequestFrame:
    kind: Literal["Q"]
    session_id: str
    sent: int
    total: int
    nonce: str


@dataclass(frozen=True)
class SyncStateFrame:
    kind: Literal["Z"]
    session_id: str
    received: int
    first_missing: int
    last_received: int
    missing: tuple[int, ...]
    nonce: str


Frame = Union[
    MetaFrame,
    DataFrame,
    AckFrame,
    ResendFrame,
    AcceptFrame,
    DeclineFrame,
    StopFrame,
    CompleteFrame,
    ErrorFrame,
    SyncRequestFrame,
    SyncStateFrame,
]


def make_session_id() -> str:
    return secrets.token_hex(3)


def to_base36(value: int) -> str:
    if value < 0:
        raise ProtocolError("negative values cannot be base36 encoded")
    if value == 0:
        return "0"
    chars: list[str] = []
    while value:
        value, remainder = divmod(value, 36)
        chars.append(BASE36_ALPHABET[remainder])
    return "".join(reversed(chars))


def from_base36(value: str) -> int:
    if not value:
        raise ProtocolError("empty base36 value")
    result = 0
    for char in value.lower():
        if char not in BASE36_ALPHABET:
            raise ProtocolError(f"invalid base36 character: {char!r}")
        result = result * 36 + BASE36_ALPHABET.index(char)
    return result


def b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def b64_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode((value + padding).encode("ascii"))
    except (binascii.Error, UnicodeEncodeError) as exc:
        raise ProtocolError("invalid base64 payload") from exc


def sha256_b64(data: bytes) -> str:
    return b64_encode(hashlib.sha256(data).digest())


def crc32_hex(data: bytes) -> str:
    return f"{binascii.crc32(data) & 0xFFFFFFFF:08x}"


def frame_len(frame: str) -> int:
    return len(frame.encode("utf-8"))


def ensure_frame_size(frame: str, max_bytes: int = MAX_FRAME_BYTES) -> str:
    size = frame_len(frame)
    if size > max_bytes:
        raise ProtocolError(f"frame is {size} bytes, max is {max_bytes}")
    return frame


def frame_header(kind: str, flags: int = COMPACT_RESERVED_BITS) -> str:
    try:
        code = FRAME_KIND_CODES[kind]
    except KeyError as exc:
        raise ProtocolError(f"unknown frame kind: {kind}") from exc
    if flags < 0 or flags > 0b11:
        raise ProtocolError("compact frame flags must fit in two bits")
    value = (COMPACT_PROTOCOL_BITS << 6) | (code << 2) | flags
    return f"{value:02x}"


def is_protocol_like(text: str) -> bool:
    if text.startswith(f"{PREFIX}|"):
        return True
    token = text.split("|", 1)[0]
    return _compact_kind(token) is not None


def compress_payload(data: bytes) -> tuple[bytes, str]:
    if not data:
        return data, "none"
    # XZ has a noticeable startup cost and a fixed header/footer overhead.
    # Tiny Meshtastic payloads almost never benefit from it, so skip them to
    # avoid delaying the first metadata packet and making the UI feel frozen.
    if len(data) < 512:
        return data, "none"
    compressed = lzma.compress(data, format=lzma.FORMAT_XZ, preset=9 | lzma.PRESET_EXTREME)
    if len(compressed) >= len(data):
        return data, "none"
    return compressed, "xz"


def decompress_payload(data: bytes, metadata: TransferMetadata, memlimit: int = 256 * 1024 * 1024) -> bytes:
    if metadata.compression == "none":
        restored = data
    elif metadata.compression == "xz":
        try:
            restored = lzma.decompress(data, format=lzma.FORMAT_XZ, memlimit=memlimit)
        except lzma.LZMAError as exc:
            raise ProtocolError("invalid compressed payload") from exc
    else:
        raise ProtocolError(f"unsupported compression: {metadata.compression}")

    expected_size = metadata_original_size(metadata)
    expected_hash = metadata_original_sha256(metadata)
    if len(restored) != expected_size or sha256_b64(restored) != expected_hash:
        raise ProtocolError("decompressed payload checksum mismatch")
    return restored


def metadata_original_size(metadata: TransferMetadata) -> int:
    return metadata.original_size or metadata.size


def metadata_original_sha256(metadata: TransferMetadata) -> str:
    return metadata.original_sha256 or metadata.sha256


def make_metadata(
    file_path: Path,
    data: bytes,
    chunk_size: int,
    *,
    original_data: bytes | None = None,
    compression: str = "none",
) -> TransferMetadata:
    total = math.ceil(len(data) / chunk_size) if data else 1
    original = data if original_data is None else original_data
    return TransferMetadata(
        name=file_path.name,
        size=len(data),
        total=total,
        chunk=chunk_size,
        sha256=sha256_b64(data),
        compression=compression,
        original_size=len(original),
        original_sha256=sha256_b64(original),
    )


def encode_metadata_frames(
    session_id: str,
    metadata: TransferMetadata,
    max_bytes: int = MAX_FRAME_BYTES,
) -> list[str]:
    metadata_dict = asdict(metadata)
    metadata_dict["original_size"] = metadata_original_size(metadata)
    metadata_dict["original_sha256"] = metadata_original_sha256(metadata)
    meta_json = json.dumps(
        metadata_dict,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    payload = b64_encode(meta_json)
    header_budget = len(f"{frame_header('M')}|{session_id}|999|999|".encode("utf-8"))
    payload_budget = max_bytes - header_budget
    if payload_budget < 12:
        raise ProtocolError("metadata frame budget is too small")

    parts = [payload[i : i + payload_budget] for i in range(0, len(payload), payload_budget)]
    if not parts:
        parts = [""]

    frames = [
        ensure_frame_size(
            f"{frame_header('M')}|{session_id}|{to_base36(index)}|{to_base36(len(parts))}|{part}",
            max_bytes,
        )
        for index, part in enumerate(parts)
    ]
    return frames


def decode_metadata(frames: Iterable[MetaFrame]) -> TransferMetadata:
    frame_list = list(frames)
    by_part = {frame.part: frame.payload for frame in frame_list}
    if not by_part:
        raise ProtocolError("no metadata parts")
    count = frame_list[0].count
    if count < 1 or count > MAX_METADATA_PARTS:
        raise ProtocolError("invalid metadata part count")
    if len(by_part) != count:
        raise ProtocolError("metadata is incomplete")
    if set(by_part) != set(range(count)):
        raise ProtocolError("metadata part indexes are incomplete")
    payload = "".join(by_part[index] for index in range(count))
    try:
        data = json.loads(b64_decode(payload).decode("utf-8"))
        metadata = TransferMetadata(
            name=str(data["name"]),
            size=int(data["size"]),
            total=int(data["total"]),
            chunk=int(data["chunk"]),
            sha256=str(data["sha256"]),
            compression=str(data.get("compression", "none")),
            original_size=int(data.get("original_size", data["size"])),
            original_sha256=str(data.get("original_sha256", data["sha256"])),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ProtocolError("invalid metadata payload") from exc
    if metadata.size < 0 or metadata.total < 1 or metadata.chunk < MIN_RAW_CHUNK_BYTES:
        raise ProtocolError("invalid metadata values")
    if metadata.original_size < 0:
        raise ProtocolError("invalid original size")
    if metadata.compression not in {"none", "xz"}:
        raise ProtocolError("unsupported compression")
    if len(metadata.name.encode("utf-8")) > MAX_FILENAME_BYTES:
        raise ProtocolError("filename is too long")
    if len(metadata.sha256) != MAX_SHA256_B64_LEN:
        raise ProtocolError("invalid sha256 length")
    if len(metadata.original_sha256) != MAX_SHA256_B64_LEN:
        raise ProtocolError("invalid original sha256 length")
    expected_total = math.ceil(metadata.size / metadata.chunk) if metadata.size else 1
    if metadata.total != expected_total:
        raise ProtocolError("invalid chunk count")
    return metadata


def encode_data_frame(
    session_id: str,
    index: int,
    chunk: bytes,
    max_bytes: int = MAX_FRAME_BYTES,
    ping: bool = False,
) -> str:
    flags = COMPACT_PING_FLAG if ping else COMPACT_RESERVED_BITS
    frame = (
        f"{frame_header('D', flags=flags)}|{session_id}|{to_base36(index)}|"
        f"{crc32_hex(chunk)}|{b64_encode(chunk)}"
    )
    return ensure_frame_size(frame, max_bytes)


def decode_data_payload(frame: DataFrame) -> bytes:
    data = b64_decode(frame.payload)
    actual_crc = crc32_hex(data)
    if actual_crc != frame.crc32.lower():
        raise ProtocolError(f"crc mismatch: expected {frame.crc32}, got {actual_crc}")
    return data


def encode_ack_frame(session_id: str, verified: int, total: int, ping_index: int | None = None) -> str:
    frame = f"{frame_header('A')}|{session_id}|{to_base36(verified)}|{to_base36(total)}"
    if ping_index is not None:
        frame += f"|{to_base36(ping_index)}"
    return ensure_frame_size(frame)


def encode_accept_frame(session_id: str) -> str:
    return ensure_frame_size(f"{frame_header('Y')}|{session_id}")


def encode_decline_frame(session_id: str, message: str = "declined") -> str:
    payload = b64_encode(message.encode("utf-8"))
    return ensure_frame_size(f"{frame_header('N')}|{session_id}|{payload}")


def encode_stop_frame(session_id: str, reason: str = "stopped") -> str:
    payload = b64_encode(reason.encode("utf-8"))
    return ensure_frame_size(f"{frame_header('S')}|{session_id}|{payload}")


def encode_complete_frame(session_id: str, sha256: str) -> str:
    return ensure_frame_size(f"{frame_header('C')}|{session_id}|{sha256}")


def encode_error_frame(session_id: str, code: str, message: str) -> str:
    payload = b64_encode(message.encode("utf-8"))
    return ensure_frame_size(f"{frame_header('E')}|{session_id}|{code}|{payload}")


def encode_sync_request_frame(session_id: str, sent: int, total: int, nonce: str) -> str:
    return ensure_frame_size(
        f"{frame_header('Q')}|{session_id}|{to_base36(sent)}|{to_base36(total)}|{nonce}"
    )


def encode_sync_state_frame(
    session_id: str,
    received: int,
    first_missing: int,
    last_received: int,
    missing: Iterable[int],
    nonce: str,
    max_bytes: int = MAX_FRAME_BYTES,
) -> str:
    prefix = (
        f"{frame_header('Z')}|{session_id}|{to_base36(received)}|"
        f"{to_base36(first_missing)}|{to_base36(last_received)}|"
    )
    suffix = f"|{nonce}"
    spec = ",".join(range_tokens(missing)) or "-"
    if frame_len(prefix + spec + suffix) <= max_bytes:
        return ensure_frame_size(prefix + spec + suffix, max_bytes)
    fallback = _range_token(first_missing, last_received) if first_missing >= 0 and last_received >= first_missing else "-"
    return ensure_frame_size(prefix + fallback + suffix, max_bytes)


def range_tokens(indices: Iterable[int]) -> list[str]:
    sorted_indices = sorted(set(indices))
    if not sorted_indices:
        return []

    tokens: list[str] = []
    start = previous = sorted_indices[0]
    for index in sorted_indices[1:]:
        if index == previous + 1:
            previous = index
            continue
        tokens.append(_range_token(start, previous))
        start = previous = index
    tokens.append(_range_token(start, previous))
    return tokens


def _range_token(start: int, end: int) -> str:
    if start == end:
        return to_base36(start)
    return f"{to_base36(start)}-{to_base36(end)}"


def parse_range_spec(spec: str) -> tuple[int, ...]:
    if not spec:
        return tuple()

    indices: list[int] = []
    for token in spec.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            left, right = token.split("-", 1)
            start = from_base36(left)
            end = from_base36(right)
            if end < start:
                raise ProtocolError("invalid descending range")
            indices.extend(range(start, end + 1))
        else:
            indices.append(from_base36(token))
    return tuple(sorted(set(indices)))


def encode_resend_frames(
    session_id: str,
    indices: Iterable[int],
    max_bytes: int = MAX_FRAME_BYTES,
) -> list[str]:
    frames: list[str] = []
    current = ""
    prefix = f"{frame_header('R')}|{session_id}|"
    for token in range_tokens(indices):
        candidate = token if not current else f"{current},{token}"
        if frame_len(prefix + candidate) <= max_bytes:
            current = candidate
            continue
        if current:
            frames.append(ensure_frame_size(prefix + current, max_bytes))
        if frame_len(prefix + token) > max_bytes:
            raise ProtocolError("single resend token does not fit into a frame")
        current = token
    if current:
        frames.append(ensure_frame_size(prefix + current, max_bytes))
    return frames


def parse_frame(text: str) -> Frame:
    if frame_len(text) > MAX_FRAME_BYTES:
        raise ProtocolError("frame is too large")
    text, flags = _normalize_frame_text(text)
    if not text.startswith(f"{PREFIX}|"):
        raise ProtocolError("not a MeshShare frame")

    kind = text.split("|", 2)[1]
    if kind == "M":
        parts = text.split("|", 5)
        if len(parts) != 6:
            raise ProtocolError("invalid metadata frame")
        part = from_base36(parts[3])
        count = from_base36(parts[4])
        if count < 1 or count > MAX_METADATA_PARTS or part >= count:
            raise ProtocolError("invalid metadata part index")
        return MetaFrame("M", parts[2], part, count, parts[5])
    if kind == "D":
        parts = text.split("|", 5)
        if len(parts) != 6:
            raise ProtocolError("invalid data frame")
        return DataFrame(
            "D",
            parts[2],
            from_base36(parts[3]),
            parts[4].lower(),
            parts[5],
            bool(flags & COMPACT_PING_FLAG),
        )
    if kind == "A":
        parts = text.split("|", 5)
        if len(parts) not in {5, 6}:
            raise ProtocolError("invalid ack frame")
        ping_index = from_base36(parts[5]) if len(parts) == 6 else None
        return AckFrame("A", parts[2], from_base36(parts[3]), from_base36(parts[4]), ping_index)
    if kind == "R":
        parts = text.split("|", 3)
        if len(parts) != 4:
            raise ProtocolError("invalid resend frame")
        return ResendFrame("R", parts[2], parse_range_spec(parts[3]))
    if kind == "Y":
        parts = text.split("|", 2)
        if len(parts) != 3:
            raise ProtocolError("invalid accept frame")
        return AcceptFrame("Y", parts[2])
    if kind == "N":
        parts = text.split("|", 3)
        if len(parts) != 4:
            raise ProtocolError("invalid decline frame")
        return DeclineFrame("N", parts[2], b64_decode(parts[3]).decode("utf-8", "replace"))
    if kind == "S":
        parts = text.split("|", 3)
        if len(parts) != 4:
            raise ProtocolError("invalid stop frame")
        return StopFrame("S", parts[2], b64_decode(parts[3]).decode("utf-8", "replace"))
    if kind == "C":
        parts = text.split("|", 3)
        if len(parts) != 4:
            raise ProtocolError("invalid complete frame")
        return CompleteFrame("C", parts[2], parts[3])
    if kind == "E":
        parts = text.split("|", 4)
        if len(parts) != 5:
            raise ProtocolError("invalid error frame")
        return ErrorFrame("E", parts[2], parts[3], b64_decode(parts[4]).decode("utf-8", "replace"))
    if kind == "Q":
        parts = text.split("|", 5)
        if len(parts) != 6:
            raise ProtocolError("invalid sync request frame")
        return SyncRequestFrame("Q", parts[2], from_base36(parts[3]), from_base36(parts[4]), parts[5])
    if kind == "Z":
        parts = text.split("|", 7)
        if len(parts) != 8:
            raise ProtocolError("invalid sync state frame")
        missing = tuple() if parts[6] == "-" else parse_range_spec(parts[6])
        return SyncStateFrame(
            "Z",
            parts[2],
            from_base36(parts[3]),
            from_base36(parts[4]),
            from_base36(parts[5]),
            missing,
            parts[7],
        )
    raise ProtocolError(f"unknown frame kind: {kind}")


def _normalize_frame_text(text: str) -> tuple[str, int]:
    if text.startswith(f"{PREFIX}|"):
        return text, COMPACT_RESERVED_BITS
    token, separator, rest = text.partition("|")
    if not separator:
        raise ProtocolError("not a MeshShare frame")
    compact = _compact_info(token)
    if compact is None:
        raise ProtocolError("not a MeshShare frame")
    kind, flags = compact
    return f"{PREFIX}|{kind}|{rest}", flags


def _compact_kind(token: str) -> str | None:
    compact = _compact_info(token)
    return None if compact is None else compact[0]


def _compact_info(token: str) -> tuple[str, int] | None:
    if len(token) != 2:
        return None
    try:
        value = int(token, 16)
    except ValueError:
        return None
    if value >> 6 != COMPACT_PROTOCOL_BITS:
        return None
    code = (value >> 2) & 0b1111
    kind = FRAME_CODES_KIND.get(code)
    if kind is None:
        return None
    flags = value & 0b11
    return kind, flags


def split_chunks(data: bytes, chunk_size: int = DEFAULT_RAW_CHUNK_BYTES) -> list[bytes]:
    if chunk_size < MIN_RAW_CHUNK_BYTES:
        raise ProtocolError(f"chunk size must be at least {MIN_RAW_CHUNK_BYTES} bytes")
    if not data:
        return [b""]
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def validate_chunk_size(
    session_id: str,
    total_chunks: int,
    chunk_size: int,
    max_bytes: int = MAX_FRAME_BYTES,
) -> None:
    worst_index = max(0, total_chunks - 1)
    sample = b"\xff" * chunk_size
    encode_data_frame(session_id, worst_index, sample, max_bytes=max_bytes)


def safe_filename(name: str) -> str:
    candidate = Path(name).name.strip().replace("\x00", "")
    if candidate in {"", ".", ".."}:
        return "meshshare-file"
    return candidate
