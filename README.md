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

## Requirements

- Python 3.11 or newer
- `pip` and `venv`
- A Meshtastic node reachable over USB serial, Bluetooth LE, or TCP

On Linux, install Tkinter if you want to use the graphical file picker:

```bash
sudo apt install python3-tk
```

On macOS, the Python installer from python.org already includes Tkinter. If you
use Homebrew Python and file dialogs do not open, install Python with Tk support
or run MeshShare without the file dialog workflow.

## Install

```bash
python3 -m pip install -e .
```

## Run

```bash
meshshare
```

or:

```bash
python3 -m meshshare
```

Optional temp directory:

```bash
python3 -m meshshare --temp-dir temp
```

## Run on Linux

1. Clone the repository and enter it:

```bash
git clone <repo-url>
cd MeshShare
```

2. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Upgrade `pip` and install MeshShare:

```bash
python -m pip install --upgrade pip
python -m pip install -e .
```

4. If you connect to the radio over USB serial, make sure your user can access
the serial device:

```bash
sudo usermod -aG dialout "$USER"
```

Log out and log back in after changing groups. On some distributions the group
can be `uucp` or `tty` instead of `dialout`.

5. Start MeshShare:

```bash
meshshare
```

If the command is not found, use:

```bash
python -m meshshare
```

## Run on macOS

1. Clone the repository and enter it:

```bash
git clone <repo-url>
cd MeshShare
```

2. Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

3. Upgrade `pip` and install MeshShare:

```bash
python -m pip install --upgrade pip
python -m pip install -e .
```

4. Start MeshShare:

```bash
meshshare
```

If the command is not found, use:

```bash
python -m meshshare
```

For USB serial connections, macOS usually exposes the Meshtastic device as
`/dev/tty.usbserial-*`, `/dev/tty.usbmodem*`, or `/dev/cu.*`. No extra group
setup is normally required.

## Connection Notes

- Serial: connect the Meshtastic node over USB and select the serial device in
  the MeshShare UI.
- Bluetooth LE: enable Bluetooth on the computer and the Meshtastic node, then
  scan/select the node in the MeshShare UI.
- TCP: use this when the Meshtastic node is reachable through a network host.
  The default Meshtastic TCP port is `4403`.

## Troubleshooting

### macOS Bluetooth pairing

macOS does not allow Python/Bleak/CoreBluetooth applications to enter a BLE PIN
inside the terminal UI. BLE-only Meshtastic nodes often do not appear in
`System Settings > Bluetooth`; pairing is normally triggered from the app during
connection.

If MeshShare shows `Encryption is insufficient`:

1. Leave the PIN field empty and connect from MeshShare.
2. If macOS shows a system pairing prompt, enter the node PIN there.
3. If no prompt appears, turn Bluetooth off/on and retry.
4. If the node is listed in macOS Bluetooth settings, remove/forget it and retry
   from MeshShare.

If macOS still refuses to pair with the node, use USB serial or TCP instead.

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

```bash
python3 -m unittest discover -s tests
```
