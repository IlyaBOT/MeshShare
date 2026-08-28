# MeshShare

MeshShare is a Meshtastic-compatible Android and Compose Desktop client focused on reliable, direct peer-to-peer file transfer over ordinary Meshtastic networks.

> [!IMPORTANT]
> The MeshShare rewrite is in progress. The imported Meshtastic messaging client builds and runs, but the file-transfer workflow described below is not yet ready for production or safety-critical use.

## What MeshShare is building

MeshShare keeps normal Meshtastic messaging and device management while adding a compact transfer layer designed for slow, lossy, multi-hop LoRa links:

- no custom Meshtastic firmware;
- compatibility with normal Meshtastic nodes;
- one sender and one concrete receiver per transfer;
- no file transfer in channels, group chats, or broadcast conversations;
- small radio-sized data packets instead of whole-file messages;
- integrity checks for chunks and completed files;
- packet-loss recovery, resumable state, and sender/receiver resynchronization;
- optional compression when it reduces airtime;
- conservative pacing for effectively half-duplex, low-bandwidth radios;
- cache-backed temporary files with cleanup after completion or cancellation.

The first implementation of this idea is preserved on the [`legacy/python-tui`](https://github.com/IlyaBOT/MeshShare/tree/legacy/python-tui) branch. It remains useful as a protocol reference, but the current rewrite moves reusable protocol and transfer logic into Kotlin Multiplatform modules.

## Project status

| Area | Status |
|---|---|
| Meshtastic messaging, node management, and device configuration | Inherited and available |
| Android F-Droid and Google debug builds | Build verified |
| Kotlin Multiplatform / Compose Desktop foundation | Inherited and available |
| MeshShare P2P file-transfer protocol and engine | Planned rewrite |
| File offers, progress UI, resume, and resync | Planned rewrite |
| Development/nightly release workflow | Planned |
| Production release | Blocked until a development APK is tested and approved |

Development follows an ordered migration checklist so the protocol, reliability engine, persistence, UI, and release gates are verified separately.

## Compatibility and safety

MeshShare is a client modification, not a firmware fork. Radios continue to use the stock Meshtastic protocol and firmware. File-transfer control and data frames travel through normal direct messages and must never target a channel or broadcast destination.

Radio bandwidth is scarce. Transfer code must tolerate loss, duplication, reordering, delays, and device disconnects without flooding the mesh. A successful build alone does not prove transfer reliability; simulator, emulator, and real-radio verification are tracked separately.

## Build the Android app

Requirements:

- JDK 25;
- Android SDK with API 37;
- `ANDROID_HOME` pointing to the installed SDK;
- an ignored `local.properties` initialized from `secrets.defaults.properties` when needed.

The F-Droid debug flavor requires no Google Maps key:

```bash
unset ANDROID_SDK_ROOT
export ANDROID_HOME=/path/to/Android/Sdk
export JAVA_HOME=/path/to/jdk-25

./gradlew :androidApp:assembleFdroidDebug
```

The universal APK is written to:

```text
androidApp/build/outputs/apk/fdroid/debug/androidApp-fdroid-universal-debug.apk
```

Run the complete local quality gate before publishing changes:

```bash
./gradlew spotlessApply spotlessCheck detekt assembleDebug test allTests
```

The Google flavor can compile with placeholder secrets, but Google-backed map tiles require a valid key until the open-map migration is complete.

## Architecture

MeshShare retains the upstream Kotlin Multiplatform architecture so changes remain reviewable and upstream updates remain practical:

- shared business logic in `core:*` KMP modules;
- Compose Multiplatform and Material 3 UI in `feature:*` modules;
- Navigation 3 routes in `core:navigation`;
- Koin dependency injection;
- Room KMP and DataStore persistence;
- platform adapters in `androidApp` and `desktopApp`;
- Meshtastic protobuf models from the pinned `org.meshtastic:protobufs` dependency.

Internal `org.meshtastic.*` namespaces and stable application identifiers are intentionally retained where renaming would add risk without improving the product.

## Upstream and attribution

MeshShare is based on the GPL-licensed [Meshtastic Android](https://github.com/meshtastic/Meshtastic-Android) project and preserves its copyright notices and license obligations. Meshtastic, its firmware, protocol, and community resources remain upstream projects; MeshShare is an independent modification focused on P2P file transfer.

The imported upstream revision, major fork changes, expected conflict areas, and sync procedure are documented in [`docs/UPSTREAM.md`](docs/UPSTREAM.md).

## License

MeshShare is distributed under the [GNU General Public License v3.0 or later](LICENSE), consistent with Meshtastic Android. Existing source-file copyright notices belong to their respective authors and must not be removed.
