# Switchboard contributor instructions

Switchboard is a local-first, CLI-managed event coordination service for AI agents.

## Product boundaries

- The CLI is the authoritative mutation interface.
- The web interface is read-only observability.
- Source adapters emit normalized events; they do not write the database directly.
- Agent hosts receive deliveries through adapters; Switchboard does not own agent execution.
- Persistent state and private configuration live outside the plugin directory.
- Never commit captured messages, credentials, personal routing rules, or machine-specific paths.
- The public marketplace catalogs live at `.agents/plugins/marketplace.json` and
  `.claude-plugin/marketplace.json`; the installable plugin lives at `plugins/switchboard/`.

## Development

- Python 3.12+, standard library first.
- Keep the domain and storage APIs independent from CLI and HTTP presentation.
- Every mutation must be auditable and idempotent where an external identifier exists.
- Run `uv run python -m unittest discover -s tests -v` and the plugin validators before committing.
- Commit completed, verified work with conventional commit subjects.
