# pg-mem conformance report

**Score: 129/129 (100.0%) · 1 known gap** — verified against differential (postgres://***@localhost:5433/postgres)

| Category | Pass | Known gap | Wrong result | Missing function | Not supported | Parse error | Error |
|---|---|---|---|---|---|---|---|
| basics | 10/10 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-string | 22/22 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-math | 10/10 | 1 | 0 | 0 | 0 | 0 | 0 |
| functions-datetime | 9/9 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-json | 8/8 | 0 | 0 | 0 | 0 | 0 | 0 |
| functions-array | 9/9 | 0 | 0 | 0 | 0 | 0 | 0 |
| window-functions | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| recursive-cte | 3/3 | 0 | 0 | 0 | 0 | 0 | 0 |
| joins | 4/4 | 0 | 0 | 0 | 0 | 0 | 0 |
| numeric-types | 6/6 | 0 | 0 | 0 | 0 | 0 | 0 |
| datetime-tz | 3/3 | 0 | 0 | 0 | 0 | 0 | 0 |
| triggers | 8/8 | 0 | 0 | 0 | 0 | 0 | 0 |
| transactions | 2/2 | 0 | 0 | 0 | 0 | 0 | 0 |
| prepared | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| domains | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |
| catalog | 3/3 | 0 | 0 | 0 | 0 | 0 | 0 |
| plpgsql | 11/11 | 0 | 0 | 0 | 0 | 0 | 0 |
| rls | 5/5 | 0 | 0 | 0 | 0 | 0 | 0 |

## Failures

_None._

## Known gaps (accepted divergences)

- `functions-math/exp ln` — numeric ln/exp computed in float64, not arbitrary precision (postgres says [{"a":2.718281828459045,"b":"0.9999999999999999"}], pg-mem says [{"a":2.718281828459045,"b":1}])

