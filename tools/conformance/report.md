# pg-mem conformance report

**Score: 93/99 (93.9%)** — verified against offline (@expect annotations)

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
| numeric-types | 2/6 | 4 | 0 | 0 | 0 | 0 |
| datetime-tz | 3/3 | 0 | 0 | 0 | 0 | 0 |
| triggers | 0/1 | 0 | 0 | 0 | 1 | 0 |
| transactions | 2/2 | 0 | 0 | 0 | 0 | 0 |
| rls | 5/5 | 0 | 0 | 0 | 0 | 0 |

## Failures

### functions-math

- `exp ln` — **wrong-result**: @expect says [{"a":2.718281828459045,"b":"0.9999999999999999"}], pg-mem says [{"a":2.718281828459045,"b":1}]

### numeric-types

- `bigint 64-bit precision` — **wrong-result**: @expect says [{"r":"9007199254740993"}], pg-mem says [{"r":"9007199254740992"}]
- `numeric scale rounding` — **wrong-result**: @expect says [{"r":"1.01"}], pg-mem says [{"r":"1.005"}]
- `integer overflow errors` — **wrong-result**: should error (@error integer out of range), but pg-mem succeeded
- `numeric division keeps precision` — **wrong-result**: @expect says [{"r":"0.33333333333333333333"}], pg-mem says [{"r":"0.3333333333333333"}]

### triggers

- `create trigger` — **parse-error**: 💔 Your query failed to parse.

