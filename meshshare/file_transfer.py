from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

from .protocol import (
    DEFAULT_RAW_CHUNK_BYTES,
    MAX_FRAME_BYTES,
    MAX_METADATA_PARTS,
    AckFrame,
    AcceptFrame,
    CompleteFrame,
    DataFrame,
    DeclineFrame,
    ErrorFrame,
    MetaFrame,
    ProtocolError,
    ResendFrame,
    StopFrame,
    TransferMetadata,
    encode_accept_frame,
    decode_data_payload,
    decode_metadata,
    encode_ack_frame,
    encode_complete_frame,
    encode_data_frame,
    encode_decline_frame,
    encode_error_frame,
    encode_metadata_frames,
    encode_resend_frames,
    encode_stop_frame,
    make_metadata,
    make_session_id,
    parse_frame,
    safe_filename,
    sha256_b64,
    split_chunks,
    validate_chunk_size,
)
from .transport import Destination, MeshMessage, MeshtasticTransport, NodeTarget

LOW_SIGNAL_ERROR = "ERROR! Too low radio signal!"
TIMEOUT_ERROR = "ERROR! Signal Lost!"
STOPPED_ERROR = "ERROR! Transfer stopped."
MAX_RECEIVE_BYTES = 5 * 1024 * 1024


class TransferError(RuntimeError):
    pass


@dataclass(frozen=True)
class TransferSnapshot:
    direction: str = "idle"
    state: str = "idle"
    session_id: str = ""
    file_name: str = ""
    node_name: str = ""
    file_size: int = 0
    total_chunks: int = 0
    verified_chunks: int = 0
    sent_chunks: int = 0
    received_chunks: int = 0
    packets_sent: int = 0
    packets_received: int = 0
    signal_db: Optional[float] = None
    elapsed_seconds: float = 0.0
    eta_seconds: Optional[float] = None
    message: str = ""
    output_path: str = ""

    @property
    def progress(self) -> float:
        if self.total_chunks <= 0:
            return 0.0
        if self.direction == "send":
            return min(1.0, max(self.verified_chunks, self.sent_chunks) / self.total_chunks)
        return min(1.0, self.received_chunks / self.total_chunks)


@dataclass(frozen=True)
class IncomingOffer:
    session_id: str
    sender: Destination
    metadata: TransferMetadata
    signal_db: Optional[float] = None


@dataclass
class OutgoingSession:
    session_id: str
    path: Path
    target: NodeTarget
    channel_index: int
    metadata: TransferMetadata
    chunks: list[bytes]
    started_at: float
    chunk_send_times: list[float] = field(default_factory=list)
    sent_chunks: int = 0
    verified_chunks: int = 0
    packets_sent: int = 0
    packets_received: int = 0
    signal_db: Optional[float] = None
    complete_event: threading.Event = field(default_factory=threading.Event)
    accept_event: threading.Event = field(default_factory=threading.Event)
    stop_event: threading.Event = field(default_factory=threading.Event)
    error: Optional[str] = None
    resend_queue: "queue.Queue[tuple[int, ...]]" = field(default_factory=queue.Queue)


@dataclass
class IncomingSession:
    session_id: str
    sender: Destination
    channel_index: int = 0
    metadata_parts: dict[int, MetaFrame] = field(default_factory=dict)
    metadata: Optional[TransferMetadata] = None
    accepted: bool = False
    declined: bool = False
    offer_prompted: bool = False
    chunks: dict[int, Path] = field(default_factory=dict)
    started_at: float = field(default_factory=time.monotonic)
    last_packet_at: float = field(default_factory=time.monotonic)
    last_resend_at: float = 0.0
    packets_sent: int = 0
    packets_received: int = 0
    signal_db: Optional[float] = None
    completed: bool = False


class FileTransferManager:
    def __init__(
        self,
        transport: MeshtasticTransport,
        download_dir: Path,
        on_snapshot: Optional[Callable[[TransferSnapshot], None]] = None,
        on_incoming_offer: Optional[Callable[[IncomingOffer], bool]] = None,
        chunk_bytes: int = DEFAULT_RAW_CHUNK_BYTES,
        max_receive_bytes: int = MAX_RECEIVE_BYTES,
        min_snr: float = -20.0,
        packet_delay: float = 1.1,
        ack_every: int = 4,
        resend_after: float = 8.0,
        complete_timeout: float = 90.0,
    ) -> None:
        self.transport = transport
        self.download_dir = download_dir
        self.on_snapshot = on_snapshot
        self.on_incoming_offer = on_incoming_offer
        self.chunk_bytes = chunk_bytes
        self.max_receive_bytes = max_receive_bytes
        self.min_snr = min_snr
        self.packet_delay = packet_delay
        self.ack_every = ack_every
        self.resend_after = resend_after
        self.complete_timeout = complete_timeout
        self._lock = threading.RLock()
        self._outgoing: dict[str, OutgoingSession] = {}
        self._incoming: dict[str, IncomingSession] = {}
        self._blocked_sessions: set[str] = set()
        self._stop = threading.Event()
        self._monitor_thread = threading.Thread(
            target=self._monitor_missing_chunks,
            name="MeshShareMissingMonitor",
            daemon=True,
        )
        self._monitor_thread.start()

    def close(self) -> None:
        self._stop.set()
        self._monitor_thread.join(timeout=2)

    def send_file(self, path: Path, target: NodeTarget, channel_index: int = 0) -> None:
        path = Path(path)
        data = path.read_bytes()
        session_id = make_session_id()
        chunks = split_chunks(data, self.chunk_bytes)
        validate_chunk_size(session_id, len(chunks), self.chunk_bytes, max_bytes=MAX_FRAME_BYTES)
        metadata = make_metadata(path, data, self.chunk_bytes)

        signal = self.transport.get_signal(target.destination)
        if signal is None:
            signal = target.snr
        if signal is not None and signal < self.min_snr:
            self._emit(
                TransferSnapshot(
                    direction="send",
                    state="error",
                    session_id=session_id,
                    file_name=path.name,
                    node_name=target.name,
                    file_size=len(data),
                    total_chunks=len(chunks),
                    signal_db=signal,
                    message=LOW_SIGNAL_ERROR,
                )
            )
            raise TransferError(LOW_SIGNAL_ERROR)

        outgoing = OutgoingSession(
            session_id=session_id,
            path=path,
            target=target,
            channel_index=channel_index,
            metadata=metadata,
            chunks=chunks,
            started_at=time.monotonic(),
            signal_db=signal,
        )
        with self._lock:
            self._outgoing[session_id] = outgoing

        try:
            self._emit_outgoing(outgoing, "offering", "Sending transfer request")
            for frame in encode_metadata_frames(session_id, metadata):
                self._raise_if_stopped(outgoing)
                self._send_frame(outgoing, frame)
                time.sleep(self.packet_delay)

            self._wait_for_accept(outgoing)

            for index in range(len(chunks)):
                self._raise_if_stopped(outgoing)
                self._send_chunk(outgoing, index)

            deadline = time.monotonic() + self.complete_timeout
            while not outgoing.complete_event.is_set():
                self._raise_if_stopped(outgoing)
                if outgoing.error:
                    raise TransferError(outgoing.error)
                try:
                    requested = outgoing.resend_queue.get(timeout=0.5)
                except queue.Empty:
                    requested = tuple()

                if requested:
                    for index in requested:
                        if 0 <= index < len(chunks):
                            self._send_chunk(outgoing, index, resend=True)
                    deadline = time.monotonic() + self.complete_timeout

                if time.monotonic() > deadline:
                    raise TransferError(TIMEOUT_ERROR)

            outgoing.verified_chunks = len(chunks)
            self._emit_outgoing(
                outgoing,
                "complete",
                f'File "{path.name}" successfully sended to node "{target.name}"!',
            )
        except Exception as exc:
            message = str(exc)
            if not message.startswith("ERROR!"):
                message = f"ERROR! {message}"
            self._emit_outgoing(outgoing, "error", message)
            raise
        finally:
            with self._lock:
                self._outgoing.pop(session_id, None)

    def handle_message(self, message: MeshMessage) -> None:
        try:
            frame = parse_frame(message.text)
        except ProtocolError:
            return

        with self._lock:
            if frame.kind in {"A", "R", "C", "E", "Y", "N", "S"}:
                self._handle_sender_control(frame, message)
            elif frame.kind == "M":
                self._handle_metadata(frame, message)
            elif frame.kind == "D":
                self._handle_data(frame, message)

    def _handle_sender_control(
        self,
        frame: AckFrame | ResendFrame | AcceptFrame | DeclineFrame | StopFrame | CompleteFrame | ErrorFrame,
        message: MeshMessage,
    ) -> None:
        outgoing = self._outgoing.get(frame.session_id)
        if outgoing is None and frame.kind == "S":
            self._handle_incoming_stop(frame, message)
            return
        if outgoing is None:
            return
        if not _message_matches_target(message, outgoing.target):
            return
        outgoing.packets_received += 1
        outgoing.signal_db = self.transport.get_signal(outgoing.target.destination) or outgoing.signal_db

        if frame.kind == "Y":
            outgoing.accept_event.set()
            self._emit_outgoing(outgoing, "sending", "Receiver accepted transfer")
        elif frame.kind == "N":
            outgoing.error = "ERROR! Receiver declined transfer."
            outgoing.complete_event.set()
        elif frame.kind == "S":
            outgoing.error = "ERROR! Transfer stopped by peer."
            outgoing.stop_event.set()
            outgoing.complete_event.set()
        elif frame.kind == "A":
            outgoing.verified_chunks = max(outgoing.verified_chunks, frame.verified)
            self._emit_outgoing(outgoing, "sending", "Receiver verified chunks")
        elif frame.kind == "R":
            outgoing.resend_queue.put(frame.indices)
            self._emit_outgoing(outgoing, "sending", "Receiver requested retransmit")
        elif frame.kind == "C":
            if frame.sha256 == outgoing.metadata.sha256:
                outgoing.verified_chunks = outgoing.metadata.total
                outgoing.complete_event.set()
            else:
                outgoing.error = "ERROR! Receiver reported wrong SHA-256."
        elif frame.kind == "E":
            outgoing.error = f"ERROR! {frame.message}"

    def _handle_metadata(self, frame: MetaFrame, message: MeshMessage) -> None:
        if message.from_id is None:
            return
        if frame.session_id in self._outgoing:
            return
        if frame.session_id in self._blocked_sessions:
            return
        session = self._incoming.setdefault(
            frame.session_id,
            IncomingSession(
                session_id=frame.session_id,
                sender=message.from_id,
                channel_index=message.channel_index,
            ),
        )
        if not _same_sender(session.sender, message.from_id):
            return
        session.channel_index = message.channel_index
        session.packets_received += 1
        session.signal_db = message.rx_snr or session.signal_db
        session.last_packet_at = time.monotonic()
        session.metadata_parts[frame.part] = frame
        if len(session.metadata_parts) == frame.count and session.metadata is None:
            try:
                session.metadata = decode_metadata(list(session.metadata_parts.values()))
                self._validate_incoming_metadata(session.metadata)
            except ProtocolError as exc:
                self._send_error(session, "META", str(exc))
                self._blocked_sessions.add(session.session_id)
                self._incoming.pop(session.session_id, None)
                return
            self._handle_incoming_offer(session)

    def _handle_data(self, frame: DataFrame, message: MeshMessage) -> None:
        if message.from_id is None:
            return
        if frame.session_id in self._outgoing:
            return
        if frame.session_id in self._blocked_sessions:
            return
        session = self._incoming.get(frame.session_id)
        if session is None:
            return
        if not _same_sender(session.sender, message.from_id):
            return
        session.channel_index = message.channel_index
        session.packets_received += 1
        session.signal_db = message.rx_snr or session.signal_db
        session.last_packet_at = time.monotonic()
        if session.metadata is None or not session.accepted:
            return
        self._store_data_frame(session, frame)

    def _handle_incoming_offer(self, session: IncomingSession) -> None:
        if session.metadata is None or session.offer_prompted:
            return
        session.offer_prompted = True
        offer = IncomingOffer(
            session_id=session.session_id,
            sender=session.sender,
            metadata=session.metadata,
            signal_db=session.signal_db,
        )
        accepted = True
        if self.on_incoming_offer is not None:
            try:
                accepted = bool(self.on_incoming_offer(offer))
            except Exception:
                accepted = False

        if not accepted:
            session.declined = True
            self._blocked_sessions.add(session.session_id)
            self._send_to_session(session, encode_decline_frame(session.session_id, "declined"))
            self._send_to_session(session, encode_stop_frame(session.session_id, "declined"))
            self._incoming.pop(session.session_id, None)
            return

        session.accepted = True
        self._send_to_session(session, encode_accept_frame(session.session_id))
        self._emit_incoming(session, "receiving", "Incoming file accepted")

    def _validate_incoming_metadata(self, metadata: TransferMetadata) -> None:
        if metadata.size > self.max_receive_bytes:
            raise ProtocolError("file is too large")
        validate_chunk_size("ffffff", metadata.total, metadata.chunk, max_bytes=MAX_FRAME_BYTES)

    def _store_data_frame(self, session: IncomingSession, frame: DataFrame) -> None:
        if session.metadata is None or session.completed or not session.accepted:
            return
        try:
            chunk = decode_data_payload(frame)
        except ProtocolError:
            self._request_resend(session, [frame.index])
            return
        if frame.index >= session.metadata.total:
            self._request_resend(session, [frame.index])
            return
        chunk_path = self._chunk_path(session, frame.index)
        chunk_path.write_bytes(chunk)
        session.chunks[frame.index] = chunk_path

        received = len(session.chunks)
        if received % self.ack_every == 0 or received == session.metadata.total:
            self._send_to_session(
                session,
                encode_ack_frame(session.session_id, received, session.metadata.total),
            )

        if received == session.metadata.total:
            self._finish_incoming(session)
        else:
            self._emit_incoming(session, "receiving", "Receiving chunks")

    def _finish_incoming(self, session: IncomingSession) -> None:
        assert session.metadata is not None
        data = b"".join(session.chunks[index].read_bytes() for index in range(session.metadata.total))
        if len(data) != session.metadata.size or sha256_b64(data) != session.metadata.sha256:
            self._request_resend(session, self._missing_indices(session))
            self._emit_incoming(session, "receiving", "Final hash mismatch, requesting retransmit")
            return

        self.download_dir.mkdir(parents=True, exist_ok=True)
        output_path = unique_path(self.download_dir / safe_filename(session.metadata.name))
        output_path.write_bytes(data)
        session.completed = True
        self._send_to_session(session, encode_complete_frame(session.session_id, session.metadata.sha256))
        self._cleanup_session_chunks(session)
        self._emit_incoming(
            session,
            "complete",
            f'Received file saved to temp "{output_path}"',
            output_path=output_path,
        )
        with self._lock:
            self._incoming.pop(session.session_id, None)

    def _monitor_missing_chunks(self) -> None:
        while not self._stop.wait(2.0):
            now = time.monotonic()
            with self._lock:
                sessions = list(self._incoming.values())
            for session in sessions:
                if session.completed or session.metadata is None or not session.accepted:
                    continue
                if now - session.last_packet_at > self.complete_timeout:
                    self._emit_incoming(session, "error", TIMEOUT_ERROR)
                    self._cleanup_session_chunks(session)
                    with self._lock:
                        self._incoming.pop(session.session_id, None)
                    continue
                if now - session.last_packet_at < self.resend_after:
                    continue
                if now - session.last_resend_at < self.resend_after:
                    continue
                missing = self._missing_indices(session)
                if missing:
                    self._request_resend(session, missing)

    def _missing_indices(self, session: IncomingSession) -> list[int]:
        if session.metadata is None:
            return []
        return [index for index in range(session.metadata.total) if index not in session.chunks]

    def _request_resend(self, session: IncomingSession, indices: list[int]) -> None:
        if not indices:
            return
        session.last_resend_at = time.monotonic()
        for frame in encode_resend_frames(session.session_id, indices):
            self._send_to_session(session, frame)
        self._emit_incoming(session, "receiving", "Requested missing chunks")

    def stop_active_transfer(self) -> None:
        with self._lock:
            outgoing_sessions = list(self._outgoing.values())
        for outgoing in outgoing_sessions:
            outgoing.stop_event.set()
            try:
                self._send_stop(outgoing, "user stopped")
            except Exception:
                pass
            self._emit_outgoing(outgoing, "stopped", STOPPED_ERROR)

    def _send_error(self, session: IncomingSession, code: str, message: str) -> None:
        try:
            self._send_to_session(session, encode_error_frame(session.session_id, code, message))
        except Exception:
            pass

    def _send_to_session(self, session: IncomingSession, frame: str) -> None:
        self.transport.send_text(frame, session.sender, channel_index=session.channel_index, want_ack=True)
        session.packets_sent += 1

    def _chunk_path(self, session: IncomingSession, index: int) -> Path:
        session_dir = self.download_dir / ".chunks" / safe_filename(session.session_id)
        session_dir.mkdir(parents=True, exist_ok=True)
        return session_dir / f"{index:08d}.chunk"

    def _cleanup_session_chunks(self, session: IncomingSession) -> None:
        session_dir = self.download_dir / ".chunks" / safe_filename(session.session_id)
        if session_dir.exists():
            for path in session_dir.glob("*.chunk"):
                try:
                    path.unlink()
                except OSError:
                    pass
            try:
                session_dir.rmdir()
            except OSError:
                pass

    def _send_chunk(self, outgoing: OutgoingSession, index: int, resend: bool = False) -> None:
        self._raise_if_stopped(outgoing)
        started = time.monotonic()
        frame = encode_data_frame(outgoing.session_id, index, outgoing.chunks[index])
        self._send_frame(outgoing, frame)
        outgoing.sent_chunks = min(outgoing.metadata.total, outgoing.sent_chunks + (0 if resend else 1))
        outgoing.chunk_send_times.append(time.monotonic() - started)
        self._emit_outgoing(outgoing, "sending", "Retransmitting chunk" if resend else "Sending chunks")
        time.sleep(self.packet_delay * signal_factor(outgoing.signal_db))

    def _wait_for_accept(self, outgoing: OutgoingSession) -> None:
        deadline = time.monotonic() + self.complete_timeout
        self._emit_outgoing(outgoing, "waiting", "Waiting for receiver confirmation")
        while not outgoing.accept_event.is_set():
            self._raise_if_stopped(outgoing)
            if outgoing.error:
                raise TransferError(outgoing.error)
            if time.monotonic() > deadline:
                raise TransferError(TIMEOUT_ERROR)
            time.sleep(0.2)

    def _raise_if_stopped(self, outgoing: OutgoingSession) -> None:
        if outgoing.stop_event.is_set():
            raise TransferError(STOPPED_ERROR)

    def _send_frame(self, outgoing: OutgoingSession, frame: str) -> None:
        self.transport.send_text(
            frame,
            outgoing.target.destination,
            channel_index=outgoing.channel_index,
            want_ack=True,
        )
        outgoing.packets_sent += 1

    def _send_stop(self, outgoing: OutgoingSession, reason: str) -> None:
        self.transport.send_text(
            encode_stop_frame(outgoing.session_id, reason),
            outgoing.target.destination,
            channel_index=outgoing.channel_index,
            want_ack=True,
        )
        outgoing.packets_sent += 1

    def _handle_incoming_stop(self, frame: StopFrame, message: MeshMessage) -> None:
        session = self._incoming.get(frame.session_id)
        if session is not None and not _same_sender(session.sender, message.from_id):
            return
        session = self._incoming.pop(frame.session_id, None)
        self._blocked_sessions.add(frame.session_id)
        if session is not None:
            self._cleanup_session_chunks(session)
            self._emit_incoming(session, "stopped", f"Transfer stopped by peer: {frame.reason}")

    def _emit_outgoing(self, outgoing: OutgoingSession, state: str, message: str) -> None:
        elapsed = time.monotonic() - outgoing.started_at
        eta = estimate_eta(
            outgoing.metadata.total,
            outgoing.sent_chunks,
            elapsed,
            outgoing.signal_db,
        )
        self._emit(
            TransferSnapshot(
                direction="send",
                state=state,
                session_id=outgoing.session_id,
                file_name=outgoing.path.name,
                node_name=outgoing.target.name,
                file_size=outgoing.metadata.size,
                total_chunks=outgoing.metadata.total,
                verified_chunks=outgoing.verified_chunks,
                sent_chunks=outgoing.sent_chunks,
                packets_sent=outgoing.packets_sent,
                packets_received=outgoing.packets_received,
                signal_db=outgoing.signal_db,
                elapsed_seconds=elapsed,
                eta_seconds=eta,
                message=message,
            )
        )

    def _emit_incoming(
        self,
        session: IncomingSession,
        state: str,
        message: str,
        output_path: Optional[Path] = None,
    ) -> None:
        metadata = session.metadata
        elapsed = time.monotonic() - session.started_at
        total = metadata.total if metadata else 0
        received = len(session.chunks)
        eta = estimate_eta(total, received, elapsed, session.signal_db)
        self._emit(
            TransferSnapshot(
                direction="receive",
                state=state,
                session_id=session.session_id,
                file_name=metadata.name if metadata else "",
                node_name=str(session.sender),
                file_size=metadata.size if metadata else 0,
                total_chunks=total,
                verified_chunks=received,
                received_chunks=received,
                packets_sent=session.packets_sent,
                packets_received=session.packets_received,
                signal_db=session.signal_db,
                elapsed_seconds=elapsed,
                eta_seconds=eta,
                message=message,
                output_path=str(output_path or ""),
            )
        )

    def _emit(self, snapshot: TransferSnapshot) -> None:
        if self.on_snapshot is not None:
            self.on_snapshot(snapshot)


def signal_factor(snr: Optional[float]) -> float:
    if snr is None:
        return 1.0
    if snr < -12:
        return 2.5
    if snr < -7:
        return 1.8
    if snr < -2:
        return 1.3
    return 1.0


def estimate_eta(
    total_chunks: int,
    completed_chunks: int,
    elapsed_seconds: float,
    snr: Optional[float],
) -> Optional[float]:
    if total_chunks <= 0 or completed_chunks <= 0:
        return None
    remaining = max(0, total_chunks - completed_chunks)
    if remaining == 0:
        return 0.0
    average = elapsed_seconds / completed_chunks
    return remaining * average * signal_factor(snr)


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}-{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def _same_sender(left: Destination, right: Optional[Destination]) -> bool:
    if right is None:
        return False
    return str(left) == str(right)


def _message_matches_target(message: MeshMessage, target: NodeTarget) -> bool:
    expected = {str(target.destination), str(target.node_id)}
    if message.from_id is not None:
        if str(message.from_id) in expected:
            return True
    if message.from_node_num is not None:
        if str(message.from_node_num) in expected or f"!{message.from_node_num:08x}" in expected:
            return True
    return False
