# Cozo Social Graph Relay

A experimental Nostr relay focused on social-graph aware reading and Trust/World-of-Trust (WoT) services. It stores Nostr events, derives social relationships (follows, mutes, reports, reactions), and exposes HTTP/WS APIs for clients and scoring engines to build trust-weighted feeds, search, and reputation services.

---

## 1) What this project is

- A Nostr relay with a graph-first design.
- Uses Cozo as the storage and query engine (with JSONL mirroring for durability and export).
- Derives social edges from Nostr events and maintains materialized views for fast queries.
- Exposes simple HTTP endpoints for reading events and for storing/retrieving WoT artifacts (worldviews and scorecards).
- Compatible with external scoring engines to compute trust scores using the cached graph data.

Typical uses:
- Fast, graph-aware reads (e.g., "my follows and their posts since X").
- Trust-powered features (custom feeds, author rankings, abuse mitigation).
- A Nostr-side cache for WoT providers to avoid slow live-network scans.

---

## 2) How it works (high level)

- **Ingestion & Relay**
  - WebSocket endpoint for Nostr `EVENT` and `REQ` messages.
  - HTTP endpoint for read queries that mirror core Nostr filters.
  - Batched persistence with a global broadcast channel for live updates.

- **Storage & Graph Derivation**
  - Events are appended to JSONL and mirrored into Cozo.
  - Social edges are derived from events and tags (e.g., follows from kind 3, mutes, reports, reactions) and stored as relations.
  - Periodic tasks maintain materialized views (e.g., latest windows) for low-latency queries.

- **Trust Artifacts (WoT)**
  - A "worldview" captures an observer’s settings and history of trust computations ("grapevines").
  - "Scorecards" are per-subject trust outputs (score, confidence, interpreter summaries).
  - HTTP endpoints allow external engines to read/write worldviews and scorecards.

- **Interoperability**
  - Scoring engines (e.g., a Graperank/Interpreter/Calculator stack) can target the relay’s HTTP API to persist results.
  - The relay can serve those results to clients or downstream services.

---

## 3) How it can evolve

- **Indexer & Enrichment**
  - Pull from multiple public relays; deduplicate; windowed subscriptions for coverage.
  - Optional heuristics for context-specific graph building (e.g., topic edges).

- **Query & Caching**
  - Push more filter/pagination logic into Cozo; add stable cursors.
  - Expand materialized views (e.g., per-author, per-kind hot sets) with configurable windows.

- **Trust & Explainability**
  - Additional interpreters (e.g., content interactions), context-aware calculations, and snapshot export/import for audits.
  - Server-driven compute hooks or job orchestration for on-demand trust generation.

- **Operations**
  - Authentication/authorization (e.g., NIP-42 gating), per-connection limits, abuse controls.
  - Metrics and tracing for read/write latency, queue depths, and compute throughput.
  - Configurable retention, compaction, and snapshot publishing.

---

## 4) Build & Run

Prerequisites:
- Rust (edition 2021), Cargo.
- Optional: Cozo native runtime/library if you plan to run with a Cozo backend.

Project layout:
- Rust workspace with crates for: models, storage (Cozo), repo layer, WebSocket server, HTTP server, settings, and a launcher binary.

Quick start (development defaults):

1) Build the workspace
```bash
cargo build
```

2) Run the relay launcher (starts WS and HTTP services)
```bash
cargo run -p relay-bin
```

3) Defaults
- HTTP: `http://0.0.0.0:7500`
- WS: `ws://0.0.0.0:7501`

4) Basic HTTP endpoints
- `GET /` – Relay info (NIP-11 style JSON)
- `GET /events?authors=&kinds=&ids=&since=&until=&limit=&cursor=` – Read events (stable order)
- `GET /health` and `GET /ready` – Basic liveness/readiness
- `GET /metrics` and `GET /metrics_prom` – Observability
- `GET /worldview?observer=&context=` – Get worldview
- `POST /worldview` – Upsert worldview
- `POST /grapevine/generate` – Trigger trust generation (if implemented)
- `GET /scorecards?observer=&context[&ts=]` – Get scorecards
- `POST /scorecards` – Upsert scorecards

5) Configuration (example)
- Defaults suitable for local dev are embedded.
- To customize ports, DB path, limits, CORS, etc., add a config loader or set environment variables as your deployment requires.

Notes:
- In development, the relay mirrors events to JSONL and into Cozo (if enabled), then derives edges and refreshes materialized views on intervals.
- For production, enable DB-backed uniqueness, enforce rate limits/auth, and run the indexer to hydrate the graph from multiple sources.

---

## Using with a Scoring Engine (optional)

- Point your scoring engine’s storage adapter to the relay HTTP base URL.
- Read a worldview (or create a default), compute ratings/scorecards, write them back to `/worldview` and `/scorecards`.
- Clients can then fetch trust outputs directly from the relay for feeds/search.
