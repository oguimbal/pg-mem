# Roadmap: pg-mem as a real Postgres for the browser

pg-mem started as an in-memory Postgres emulator for unit tests. This roadmap tracks its
evolution into a production-grade, embeddable Postgres that runs anywhere JavaScript runs —
browsers, edge workers, Deno, Node — with durable persistence and measured SQL parity.

## Positioning

[PGlite](https://pglite.dev) compiles real Postgres to WASM (~3MB). pg-mem takes the other
trade: a pure-TypeScript engine that is

- **tiny** (~300KB core, no WASM),
- **synchronous** (no async ceremony for local reads),
- **instant** to cold-start,
- **hackable** (custom functions, types, languages, query interception, engine events).

We will not claim "100% Postgres". Instead, parity is a **measured conformance score**
(`tools/conformance/`) that must climb every release. Nothing that claims to close a parity
gap merges without a conformance case proving it against real Postgres semantics.

## Phase 0 — Conformance harness *(in progress)*

The measuring stick everything else is prioritized by.

- [x] Corpus + runner: run categorized SQL cases against pg-mem, classify each as
  `pass / wrong-result / parse-error / not-supported / runtime-error` (`tools/conformance/`)
- [x] Report generation (`report.md` / `report.json`), suitable for CI
- [x] Differential mode: execute the corpus against real Postgres (`PG_URL=...`), making
  the real server the source of truth instead of hand-authored `@expect` blocks
- [ ] Import relevant slices of the Postgres regression suite (`src/test/regress/sql/`)
- [ ] CI: conformance score published per PR; bundle-size budget

## Phase 1 — Type-system correctness

The deepest cut; cross-cutting through evaluator, aggregates, comparisons and index keys.

- [ ] `bigint` backed by JS `BigInt` (true 64-bit semantics, overflow errors)
- [ ] `numeric(p,s)` with real arbitrary-precision decimal semantics (today all numerics
  collapse to JS `number` — see `src/datatypes/datatypes.ts` `typeSynonyms`)
- [ ] Integer overflow / division semantics matching Postgres
- [ ] Timezones: real `SET timezone`, `timestamptz` arithmetic, `AT TIME ZONE`
  (base on the `Temporal` API with fallback)
- [ ] Domains, range types (operators, not just the faked `pg_range` catalog), proper
  `interval` arithmetic

Breaking-change note: numeric results changing JS type ships behind a compat flag first.

## Phase 2 — Query-engine parity

- [ ] Window functions (`OVER`, partitions, frames) — most-hit gap in real app code
- [ ] `WITH RECURSIVE`
- [ ] `FULL OUTER JOIN`, `LATERAL`
- [ ] Builtin function library — the best contribution surface: hundreds of small,
  independently testable functions (string, math, date/time, regex, json/jsonb, array,
  `generate_series`, ...). Tracked by conformance categories; see `src/functions/`
- [ ] Join planning improvements (kill `catastrophic-join-optimization` paths)

## Phase 3 — Procedural layer

- [ ] Triggers (BEFORE/AFTER, row/statement) — currently absent entirely
- [ ] plpgsql interpreter via the existing `registerLanguage` hook (target the 80% subset
  real migrations use)
- [x] `SAVEPOINT` / `ROLLBACK TO` (cheap on the copy-on-write store)
- [ ] Deferrable constraints, isolation-level basics

## Phase 3b — Roles & Row-Level Security ✅

Table stakes for multi-tenant apps (the core browser-DB use case). Enforcement defers
role/bypass/policy-applicability to `enumerate(t)` (an `RlsSelection` transform), so a
cached plan stays correct across `SET ROLE`. Verified against live Postgres 16.

- [x] Roles & session identity: `CREATE`/`DROP ROLE`, `SET`/`RESET ROLE`, role
  attributes (`SUPERUSER`, `BYPASSRLS`, `LOGIN`); `current_user`/`current_role`/
  `session_user` are dynamic
- [x] Policy DDL: `CREATE`/`DROP POLICY`, `ALTER TABLE … ENABLE/DISABLE/FORCE ROW
  LEVEL SECURITY`
- [x] Enforcement: `USING` (read) / `WITH CHECK` (write) per command, `PERMISSIVE`
  OR-combined and `RESTRICTIVE` AND-combined; default-deny when RLS is on with no
  matching policy; `BYPASSRLS`/superuser exemption
- [x] `pg_policies` introspection; `GRANT`/`REVOKE` parsed as no-ops
- v1 scope note: GRANT/table privileges are no-ops (no privilege system), role-membership
  hierarchy and owner-`FORCE` nuance out of scope — only superuser/`BYPASSRLS` bypass

## Phase 4 — Durable persistence (browser & beyond)

The storage layer is already immutable structural-sharing maps — snapshots are cheap.

- [ ] Storage adapter interface (journal + snapshot model: append committed transactions
  to a WAL, periodic compaction)
- [ ] Browser backend: OPFS `SyncAccessHandle` in a worker (keeps the sync API), IndexedDB
  fallback
- [ ] Node backend: file-based persistence (makes pg-mem a viable embedded DB)
- [ ] Multi-tab sharing via SharedWorker (post-v1)

## Phase 5 — Ecosystem & launch

- [ ] Prisma and Drizzle support, likely via wire-protocol-over-MessagePort (generalizing
  `src/adapters/pg-socket-adapter.ts`) — covers every pg client at once
- [ ] Playground backed by the live conformance dashboard
- [ ] Docs + honest pg-mem vs PGlite positioning page

## Contributing

The fastest way to help: pick a failing conformance case from `tools/conformance/report.md`
and make it pass. Builtin functions (`src/functions/`) and aggregates
(`src/transforms/aggregation.ts`) are small, self-contained, and always welcome.
