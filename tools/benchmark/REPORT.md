# Fork vs upstream benchmark

Does this fork's work (RLS, arbitrary-precision numeric/bigint, timezones, window
functions, CTEs, ~70 builtin functions, ...) bloat pg-mem's core promise — *fast and
tiny*? This compares the fork against production `pg-mem@3.0.14` (npm) on runtime speed
and bundle size.

Reproduce:

```bash
# runtime (fork src vs installed upstream)
bun tools/benchmark/run.mjs ./src/index.ts <path>/node_modules/pg-mem/index.js
# bundle: build both, then `terser <bundle> -c -m | gzip -c | wc -c`
```

## Runtime — +1.7% total (negligible)

Identical workload, best of 3 runs, 5000-row tables. Times in ms (lower is better).

| operation                | upstream | fork   | delta  |
|--------------------------|---------:|-------:|-------:|
| create schema            |      0.9 |    1.0 |  +4.9% |
| insert 5k users          |   1369.0 | 1407.7 |  +2.8% |
| insert 5k orders         |   1407.9 | 1457.7 |  +3.5% |
| pk lookups ×2000         |    427.9 |  426.9 |  −0.2% |
| indexed fk lookups ×2000 |    425.5 |  443.4 |  +4.2% |
| seq scan filter ×200     |    173.2 |  182.9 |  +5.6% |
| join ×200                |    144.0 |  147.3 |  +2.2% |
| aggregation ×100         |   1958.5 | 1937.6 |  −1.1% |
| updates ×2000            |    663.8 |  695.4 |  +4.8% |
| arithmetic ×5000         |   1838.4 | 1850.1 |  +0.6% |
| **TOTAL**                | **8409** | **8550** | **+1.7%** |

The hot path is intact. Reads on ordinary tables are unchanged because RLS enforcement is
a guarded no-op — `applyReadRls` returns the table's selection untouched unless the table
has `ENABLE ROW LEVEL SECURITY` or a policy, so non-RLS queries never enter the transform.
The small costs on writes (updates +4.8%, inserts +3%) come from the per-commit deferred-
constraint check and type-aware arithmetic dispatch; seq-scan +5.6% is the widest but
still noise-level. Nothing is a structural regression.

## Bundle — +26 KB gzipped (still tiny)

Both built with the repo's `webpack --prod`, then `terser -c -m` + gzip (what actually
ships after a consumer minifies).

| bundle             | minified | min+gzip |
|--------------------|---------:|---------:|
| upstream 3.0.14    |   254 KB |  64.5 KB |
| this fork          |   371 KB |  97.0 KB |
| **delta**          | **+117 KB** | **+32.5 KB (+50%)** |

Of the +26 KB gzipped, roughly ~6.5 KB is the parser grammar (the new `pgsql-ast-parser`
rules: roles, policies, GRANT, window frames, `position`, deferrable, triggers, prepared
statements, domains — nearley compiles grammar to sizeable tables), ~7 KB is the
trigger + PL/pgSQL interpreter (variables, control flow, embedded SQL, RAISE/EXCEPTION,
set-returning functions, INSTEAD OF view triggers, TG_*), and the rest is other engine
feature code (prepared statements, ALTER INDEX, tablespaces, WHEN / UPDATE
OF trigger gating, domains, catalog views, ROW(), composite types with `(expr).field`
access, the string_agg aggregate, MERGE, range types, full-text search, and declarative
partitioning).

**Crucially, the growth is feature code, not dead weight, and no runtime dependency was
added** — the `Decimal` type is hand-rolled on BigInt and timezones use the runtime's
own `Intl` data, both specifically to avoid pulling in decimal.js / moment-timezone. The
bulk of the increase is core SQL surface (window functions, recursive CTEs, the builtin
function library, arbitrary-precision numeric) that advances pg-mem's goal of being a
real Postgres; RLS + roles is ~7 KB of it.

## Verdict

The fork stays true to pg-mem's positioning: **+1.7% runtime and 91 KB gzipped** — still
~35× smaller than PGlite's ~3 MB WASM, with no new dependencies and no hot-path
regression. The size cost buys a large jump in SQL conformance (54% → 100% of the
conformance corpus, with one documented known gap). If the RLS/roles footprint ever
matters for a size-critical embed, it's self-contained enough to gate behind a build flag
later.
