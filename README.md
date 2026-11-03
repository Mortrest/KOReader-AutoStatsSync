# Automatic Statistics Sync for KOReader

This on-device plugin automatically performs an append-only merge of KOReader's `statistics.sqlite3` through the WebDAV location already configured in **Reading statistics → Cloud sync**.

It synchronizes three times:

- three seconds after a book opens;
- after the book closes and KOReader flushes the final reading event;
- whenever **Automatic statistics sync → Sync now** is selected.

## Install

Copy the complete directory:

```text
autostatsync.koplugin/
```

into the device's:

```text
koreader/plugins/
```

The resulting path must be:

```text
koreader/plugins/autostatsync.koplugin/main.lua
```

Restart KOReader completely. Repeat on both Kindle and Boox.

## Configure

1. On both devices, configure the same server and folder under **Reading statistics → Cloud sync**.
   The WebDAV server's start folder may be blank. A blank value means the WebDAV root and is valid. The Source and Destination folders in general CloudSync settings are for syncing book files and are not used by this plugin.
2. Open a book and select **Tools → Automatic statistics sync**.
3. Leave automatic open/close synchronization enabled.
4. Keep **Only sync when Wi-Fi is already online** enabled if you do not want prompts or automatic Wi-Fi activation.

The plugin also replaces the callback used by KOReader's existing **Reading statistics → Sync now** command while a book is open, so manual synchronization uses the same append-only merger.

## Merge guarantees

- Books are matched using KOReader's exact `(title, authors, md5)` identity.
- Events are matched using KOReader's exact `(book, page, start_time)` unique identity.
- Existing events are never deleted because another device lacks them.
- Shared events are counted once.
- Conflicting copies of one event keep the longer duration.
- Cached totals are rebuilt from merged raw events.
- A local backup is created before every merge; the newest ten are retained in `koreader/settings/autostatsync-backups/`.
- Up to five WebDAV ETag conflict retries protect against two devices updating the remote database at the same time when the server supplies ETags.
- A corrupt or schema-incompatible remote database is rejected and is not uploaded over the local database.

## Important limitations

KOReader's Lua network requests are synchronous. The plugin schedules synchronization after the book UI is ready, but a slow WebDAV server can briefly pause the interface while the small database transfers. This is automatic background-triggered work, not a separate operating-system thread.

This is deliberately append-only. If you intentionally delete historical statistics on one device, the next synchronization will restore them from another device or from WebDAV. That is the safety rule that prevents the data-loss problem this plugin is designed to solve.

For the first installation, start both devices from the same known-good `statistics.sqlite3` when possible. Keep a separate computer backup until you have verified the calendar and book totals on both devices.
