# pg-mem conformance report

**Score: 97/99 (98.0%)** — verified against differential (postgres://***@localhost:5433/postgres)

| Category | Pass | Wrong result | Missing function | Not supported | Parse error | Error |
|---|---|---|---|---|---|---|
| basics | 10/10 | 0 | 0 | 0 | 0 | 0 |
| functions-string | 22/22 | 0 | 0 | 0 | 0 | 0 |
| functions-math | 10/11 | 1 | 0 | 0 | 0 | 0 |
| functions-datetime | 9/9 | 0 | 0 | 0 | 0 | 0 |
| functions-json | 8/8 | 0 | 0 | 0 | 0 | 0 |
| functions-array | 9/9 | 0 | 0 | 0 | 0 | 0 |
| window-functions | 6/6 | 0 | 0 | 0 | 0 | 0 |
| recursive-cte | 3/3 | 0 | 0 | 0 | 0 | 0 |
| joins | 4/4 | 0 | 0 | 0 | 0 | 0 |
| numeric-types | 6/6 | 0 | 0 | 0 | 0 | 0 |
| datetime-tz | 3/3 | 0 | 0 | 0 | 0 | 0 |
| triggers | 0/1 | 0 | 0 | 0 | 1 | 0 |
| transactions | 2/2 | 0 | 0 | 0 | 0 | 0 |
| rls | 5/5 | 0 | 0 | 0 | 0 | 0 |

## Failures

### functions-math

- `exp ln` — **wrong-result**: postgres says [{"a":2.718281828459045,"b":"0.9999999999999999"}], pg-mem says [{"a":2.718281828459045,"b":1}]

### triggers

- `create trigger` — **parse-error**: 💔 Your query failed to parse.

