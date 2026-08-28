# Upstream synchronization

MeshShare is an independent modification of [Meshtastic Android](https://github.com/meshtastic/Meshtastic-Android). Upstream copyright and GPL notices must remain intact.

## Recorded import baseline

| Item | Value |
|---|---|
| MeshShare repository | `https://github.com/IlyaBOT/MeshShare.git` |
| Official upstream | `https://github.com/meshtastic/Meshtastic-Android.git` |
| Upstream baseline commit | `5a6d8ab0922e093e2a7589ab37230ec617030a02` |
| Upstream baseline subject | `chore: Scheduled updates (Graphs, Baseline Profile) (#6768)` |
| Local import commit | `e28c3121d73c415cc92a156bd3e9d72da1cbcae4` |
| Import tree | `7968e21ddc8b5b5a5e8594b90deec017b74972ca` |
| Import date | 2026-08-19 |

The upstream baseline and local import have the same Git tree. Their commit histories are unrelated, so a normal merge or rebase against `upstream/main` is not the preferred synchronization mechanism.

## MeshShare-owned changes

The fork intentionally owns these areas:

- MeshShare-first README, visible application identity, and fork metadata;
- an independent compact P2P file-transfer protocol and transfer engine;
- direct-message-only attachment, offer, progress, cancellation, resume, and resync UX;
- open/no-key map-provider integration;
- MeshShare transfer settings and English/Russian copy;
- development/nightly CI and user-gated release workflow.

Stable application IDs, deep-link schemes, protobuf dependencies, and internal `org.meshtastic.*` packages remain upstream-compatible unless a functional migration requires changing them.

## Expected conflict areas

Review these paths first during every sync:

- `README.md` and `docs/UPSTREAM.md`;
- Android display-name resources under `androidApp/src/*/res/values/`;
- `feature/settings/.../AboutScreen.kt` and shared strings in `core/resources/`;
- Desktop window/package metadata in `desktopApp/` and `scripts/build-appimage.sh`;
- map implementation and dependencies;
- messaging composer and direct-conversation models;
- MeshShare protocol, persistence, and settings modules;
- `.github/workflows/` and release metadata.

Do not hand-edit generated protobuf models. Update the pinned upstream `org.meshtastic:protobufs` dependency only when the corresponding upstream change is deliberately adopted.

## Sync procedure

1. Start from a clean, fully verified MeshShare branch.
2. Fetch both remotes and inspect the proposed upstream range:

   ```bash
   git fetch origin
   git fetch upstream
   git log --oneline 5a6d8ab0922e093e2a7589ab37230ec617030a02..upstream/main
   git diff --stat 5a6d8ab0922e093e2a7589ab37230ec617030a02..upstream/main
   ```

3. Create a dedicated sync branch from the current MeshShare `main`:

   ```bash
   git switch main
   git pull --ff-only origin main
   git switch -c sync/upstream-YYYYMMDD
   ```

4. Apply upstream commits after the recorded baseline as patches so the unrelated histories are not merged wholesale:

   ```bash
   git format-patch --stdout 5a6d8ab0922e093e2a7589ab37230ec617030a02..upstream/main > /tmp/meshshare-upstream.patch
   git am -3 /tmp/meshshare-upstream.patch
   ```

5. Resolve conflicts in favor of current upstream behavior first, then reapply the smallest MeshShare-specific delta. Preserve license headers and do not replace platform-independent code with Android/JVM APIs.
6. Update the baseline commit and tree in this document to the newly synchronized `upstream/main` revision.
7. Run string sorting when resources changed, then the full project verification:

   ```bash
   python3 scripts/sort-strings.py
   ./gradlew spotlessApply spotlessCheck detekt assembleDebug test allTests
   ```

8. Record build, test, emulator, and real-hardware results separately. Review the sync branch before merging it into `main`.

If `git am` stops, resolve the listed files, stage only the resolution, and continue with `git am --continue`. Use `git am --abort` to return the sync branch to its pre-application state when the conflict set is not safely reviewable.
