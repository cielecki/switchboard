# Persistent supervisor

The supervisor is Switchboard's long-running control loop. The CLI owns every configuration write;
the web UI only reads the resulting state.

Each cycle:

1. selects enabled adapter schedules whose `next_run_at` has arrived;
2. runs each adapter independently and records completion or failure;
3. advances each schedule by its configured interval;
4. dispatches eligible pending chat deliveries when a chats relay is configured;
5. writes a heartbeat, cycle time, and combined error summary.

A failed source or delivery does not terminate the process. Failed chat deliveries stay pending and
retain their stable broker request ID. The retry interval prevents tight failure loops.

## CLI control

```bash
switchboard --json schedule list
switchboard --json schedule disable ingest
switchboard --json schedule enable ingest
switchboard --json schedule delete ingest
switchboard --json supervisor status
```

Schedule configuration and supervisor health live in the external Switchboard database. Private
paths and source data never belong in the plugin checkout.

## Process model

Only one supervisor may hold a database's lock. `supervisor run` also hosts the read-only HTTP UI.
SIGINT and SIGTERM cause a clean shutdown and a final `stopped` heartbeat. Interrupted adapter runs
are marked failed when the next supervisor starts.

`service install` is currently macOS-only. It writes and loads
`~/Library/LaunchAgents/io.github.cielecki.switchboard.plist` with `RunAtLoad` and `KeepAlive`, and
writes stdout/stderr logs under `~/.local/state/switchboard/`. Use `service uninstall` to unload and
remove it. Marketplace installs are versioned, so reinstall the service after a plugin update.
