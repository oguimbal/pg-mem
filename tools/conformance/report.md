# pg-mem conformance report

**Score: 255/255 (100.0%) · 1 known gap** — verified against differential (postgres://***@localhost:5433/postgres)

| Category | Pass | Known gap | Wrong result | Missing function | Not supported | Parse error | Error |
|---|---|---|---|---|---|---|---|
| basics | 12/12 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-string | 22/22 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-math | 10/10 | 1 | 0 | 0 | 0 | 0 | 0 |
| functions-datetime | 9/9 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-json | 8/8 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-array | 12/12 | 0 | 0 | 0 | 0 | 0 | 0 |
| window-functions | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| recursive-cte | 3/3 | 0 | 0 | 0 | 0 | 0 | 0 |
| joins | 8/8 | 0 | 0 | 0 | 0 | 0 | 0 |
| numeric-types | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| datetime-tz | 3/3 | 0 | 0 | 0 | 0 | 0 | 0 |
| triggers | 10/10 | 0 | 0 | 0 | 0 | 0 | 0 |
| transactions | 2/2 | 0 | 0 | 0 | 0 | 0 | 0 |
| prepared | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| domains | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| catalog | 3/3 | 0 | 0 | 0 | 0 | 0 | 0 |
| plpgsql | 15/15 | 0 | 0 | 0 | 0 | 0 | 0 |
| supabase | 4/4 | 0 | 0 | 0 | 0 | 0 | 0 |
| composite-types | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| merge | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| ranges | 13/13 | 0 | 0 | 0 | 0 | 0 | 0 |
| rls | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| text-search | 10/10 | 0 | 0 | 0 | 0 | 0 | 0 |
| partitioning | 7/7 | 0 | 0 | 0 | 0 | 0 | 0 |
| set-operations | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| aggregate-filter | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| all-and-series | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| regexp-functions | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| ordered-set-aggregates | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| ordinality-jsonb | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| datetime-timestamptz | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| array-json-extras | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| is-distinct-from | 4/4 | 0 | 0 | 0 | 0 | 0 | 0 |
| interval-units-window | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| grouping-sets | 4/4 | 0 | 0 | 0 | 0 | 0 | 0 |
| numeric-funcs-operators | 4/4 | 0 | 0 | 0 | 0 | 0 | 0 |
| quote-subscripts-make | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |

## Failures

_None._

## Known gaps (accepted divergences)

- `functions-math/exp ln` — numeric ln/exp computed in float64, not arbitrary precision (postgres says [{"a":2.718281828459045,"b":"0.9999999999999999"}], pg-mem says [{"a":2.718281828459045,"b":1}])

