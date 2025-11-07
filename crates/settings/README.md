# relay-settings: Modular Configuration

This crate defines a modular, file-structurable settings model for the relay, aligned to domains you will find in nostr-rs-relay and extended for Cozo + GraphRank.

## Structure
- network
  - `Ws`: address, port, WS payload limits
  - `Http`: address, port, CORS
- database
  - `Cozo`: path to Cozo store
- limits
  - message/subscription rates, DB concurrency, buffers
- authorization
  - NIP-42 auth toggle, optional pubkey whitelist
- verification (NIP-05)
  - mode and domain allow/deny lists; expiry/update intervals
- payments (pay-to-relay)
  - enable flag, admission and per-event costs, node credentials, messaging

## Example TOML (composed)

settings can be organized modularly across files and merged by your loader (e.g., `config` crate) in this order:
- `base.toml` (defaults)
- `env.toml` (environment overlay)
- `local.toml` (machine overrides)

```toml
[ws]
address = "0.0.0.0"
port = 4848
max_event_bytes = 131072

[http]
address = "0.0.0.0"
port = 8080
cors = true

[database]
path = "./data/cozo"

[limits]
messages_per_sec = 50
subscriptions_per_min = 60
db_conns_per_client = 4
broadcast_buffer = 16384
event_persist_buffer = 4096

[authorization]
nip42_auth = true
pubkey_whitelist = []

[verification]
mode = "disabled"
verify_expiration = "1 week"
verify_update_frequency = "1 day"
max_consecutive_failures = 20

[payments]
enabled = false
admission_cost = 4200
cost_per_event = 0
node_url = ""
api_secret = ""
terms_message = ""
sign_ups = false
direct_message = false
processor = "LNBits"
```

## Loader guidance
- Use the `config` crate to merge multiple files and environment variables.
- Validate cross-field invariants (e.g., buffer sizes, rate limits, port ranges).
- Expose a flattened `Settings` object to the rest of the system.
