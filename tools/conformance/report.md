# pg-mem conformance report

**Score: 84/94 (89.4%)** — verified against offline (@expect annotations)

| Category | Pass | Wrong result | Missing function | Not supported | Parse error | Error |
|---|---|---|---|---|---|---|
| basics | 10/10 | 0 | 0 | 0 | 0 | 0 |
| functions-string | 21/22 | 0 | 1 | 0 | 0 | 0 |
| functions-math | 10/11 | 1 | 0 | 0 | 0 | 0 |
| functions-datetime | 9/9 | 0 | 0 | 0 | 0 | 0 |
| functions-json | 8/8 | 0 | 0 | 0 | 0 | 0 |
| functions-array | 9/9 | 0 | 0 | 0 | 0 | 0 |
| window-functions | 6/6 | 0 | 0 | 0 | 0 | 0 |
| recursive-cte | 3/3 | 0 | 0 | 0 | 0 | 0 |
| joins | 4/4 | 0 | 0 | 0 | 0 | 0 |
| numeric-types | 2/6 | 4 | 0 | 0 | 0 | 0 |
| datetime-tz | 1/3 | 0 | 0 | 0 | 0 | 2 |
| triggers | 0/1 | 0 | 0 | 0 | 1 | 0 |
| transactions | 1/2 | 0 | 0 | 0 | 1 | 0 |

## Failures

### functions-string

- `position in` — **missing-function**: ERROR: function position(bool) does not exist

### functions-math

- `exp ln` — **wrong-result**: @expect says [{"a":2.718281828459045,"b":"0.9999999999999999"}], pg-mem says [{"a":2.718281828459045,"b":1}]

### numeric-types

- `bigint 64-bit precision` — **wrong-result**: @expect says [{"r":"9007199254740993"}], pg-mem says [{"r":"9007199254740992"}]
- `numeric scale rounding` — **wrong-result**: @expect says [{"r":"1.01"}], pg-mem says [{"r":"1.005"}]
- `integer overflow errors` — **wrong-result**: should error (@error integer out of range), but pg-mem succeeded
- `numeric division keeps precision` — **wrong-result**: @expect says [{"r":"0.33333333333333333333"}], pg-mem says [{"r":"0.3333333333333333"}]

### datetime-tz

- `at time zone` — **error**: operator does not exist: timestamp without time zone AT TIME ZONE text
- `timestamptz cast conversion` — **error**: cannot cast type timestamp with time zone to text

### triggers

- `create trigger` — **parse-error**: 💔 Your query failed to parse.

### transactions

- `deferrable constraint` — **parse-error**: 💔 Your query failed to parse.

