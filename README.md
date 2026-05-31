# MeshShare

MeshShare is a terminal-style, IRC-inspired chat and file transfer client for Meshtastic networks.

It allows two Meshtastic users to exchange small files through ordinary text packets without modifying Meshtastic firmware. Files are encoded into compact protocol frames, split into chunks, verified with CRC32 per chunk and SHA-256 after completion, and re-requested if corrupted or missing.

MeshShare is intended for experiments with low-bandwidth, off-grid file exchange over mesh radio networks.

## Features

- IRC-style terminal chat interface
- Peer-to-peer file transfer over Meshtastic text packets
- Chunked transfer with CRC32 validation
- Final SHA-256 integrity check
- Missing/corrupt chunk resend requests
- Serial, Bluetooth, and TCP Meshtastic connection modes
- Temporary file assembly before saving
- Compatible with any regular Meshtastic node - no custom firmware required!

## Install

```powershell
python -m pip install -e .
```

## Run

```powershell
meshshare
```

or:

```powershell
python -m meshshare
```

Optional temp directory:

```powershell
python -m meshshare --temp-dir temp
```

## Protocol Notes

Frames use the `MS1` prefix and are sent with Meshtastic `sendText(...)`.

- `M`: metadata, split if needed.
- `D`: data chunk with CRC32.
- `Y`: receiver accepted transfer.
- `N`: receiver declined transfer.
- `S`: peer stopped transfer intentionally.
- `A`: receiver progress ACK.
- `R`: receiver resend request for missing/corrupt chunks.
- `C`: final completion ACK with SHA-256.
- `E`: protocol/application error.

This is intentionally conservative. It is not streaming yet, and it requires
MeshShare to be running on both peers.

## Tests

```powershell
python -m unittest discover -s tests
```
