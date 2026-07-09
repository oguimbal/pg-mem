import { _ISchema, QueryError } from '../../interfaces-private';
import { numbers, dateTypes, Types, numberPriorities } from '../../datatypes';
import { Decimal } from '../../datatypes/numeric';
import { DataType } from '../../interfaces';
import { dateAddInterval, queryJson } from '../../utils';
import { jsonPathGet, jsonAsText, jsonRemovePath } from '../../functions/json';
import { timestampAtZone, instantToZoneWall } from '../../datatypes/timezone';
import moment from 'moment';

export function registerCommonOperators(schema: _ISchema) {
    registerNumericOperators(schema);
    registerBitwiseOperators(schema);
    registerDatetimeOperators(schema);
    registerJsonOperators(schema);
    registerTextOperators(schema);
}

function registerBitwiseOperators(schema: _ISchema) {
    // integer bitwise operators
    const bit: [string, (a: number, b: number) => number][] = [
        ['&', (a, b) => a & b],
        ['|', (a, b) => a | b],
        ['#', (a, b) => a ^ b], // # is XOR in Postgres
        ['<<', (a, b) => a << b],
        ['>>', (a, b) => a >> b],
    ];
    for (const [op, impl] of bit) {
        schema.registerOperator({
            operator: op as any,
            commutative: op === '&' || op === '|' || op === '#',
            left: Types.integer,
            right: Types.integer,
            returns: Types.integer,
            implementation: impl,
        });
    }
    // exponentiation (^) — Postgres returns double precision
    for (const [left, right] of [[Types.float, Types.float], [Types.integer, Types.integer]] as const) {
        schema.registerOperator({
            operator: '^' as any,
            commutative: false,
            left,
            right,
            returns: Types.float,
            implementation: (a: number, b: number) => Math.pow(a, b),
        });
    }
}

function* numberPairs() {
    for (const a of numbers) {
        for (const b of numbers) {
            yield [a, b, numberPriorities[a] < numberPriorities[b] ? b : a] as const;
        }
    }
}

const INT4_MIN = -2147483648;
const INT4_MAX = 2147483647;

function toNum(v: any): number {
    return typeof v === 'string' ? Number(v) : v;
}
function toBig(v: any): bigint {
    return typeof v === 'string' ? BigInt(v.includes('.') ? v.split('.')[0] : v) : BigInt(Math.trunc(v));
}
function toDec(v: any): Decimal {
    return typeof v === 'string' ? Decimal.fromText(v) : Decimal.fromNumber(v);
}

type ArithSym = '+' | '-' | '*' | '/' | '%';

// Type-aware arithmetic: the result type decides the representation. bigint/decimal
// operate in exact BigInt/Decimal and yield strings; integer/float stay JS numbers.
function arith(sym: ArithSym, returns: DataType): (a: any, b: any) => any {
    if (returns === DataType.decimal) {
        return (a, b) => {
            const x = toDec(a), y = toDec(b);
            switch (sym) {
                case '+': return x.add(y).toString();
                case '-': return x.sub(y).toString();
                case '*': return x.mul(y).toString();
                case '/': return x.div(y).toString();
                case '%': return x.mod(y).toString();
            }
        };
    }
    if (returns === DataType.bigint) {
        return (a, b) => {
            const x = toBig(a), y = toBig(b);
            switch (sym) {
                case '+': return (x + y).toString();
                case '-': return (x - y).toString();
                case '*': return (x * y).toString();
                case '/':
                    if (y === BigInt(0)) { throw new QueryError('division by zero', '22012'); }
                    return (x / y).toString();
                case '%':
                    if (y === BigInt(0)) { throw new QueryError('division by zero', '22012'); }
                    return (x % y).toString();
            }
        };
    }
    const isInt = returns === DataType.integer;
    return (a, b) => {
        const x = toNum(a), y = toNum(b);
        let r: number;
        switch (sym) {
            case '+': r = x + y; break;
            case '-': r = x - y; break;
            case '*': r = x * y; break;
            case '/':
                if (y === 0) { throw new QueryError('division by zero', '22012'); }
                r = isInt ? Math.trunc(x / y) : x / y;
                break;
            case '%':
                if (y === 0) { throw new QueryError('division by zero', '22012'); }
                r = x % y;
                break;
        }
        if (isInt && (r < INT4_MIN || r > INT4_MAX)) {
            throw new QueryError('integer out of range', '22003');
        }
        return r;
    };
}

function registerNumericOperators(schema: _ISchema) {
    // ======= "+ - * /" on numeric types (integer/bigint/decimal/float) =======
    for (const sym of ['+', '-', '*', '/'] as const) {
        for (const [left, right, returns] of numberPairs()) {
            schema.registerOperator({
                // + and * are commutative (lets indexes match a+b with b+a)
                operator: sym,
                commutative: sym === '+' || sym === '*',
                left,
                right,
                returns,
                implementation: arith(sym, returns),
            });
        }
    }

    // ======= "%" (modulo) — postgres defines it for integer/bigint/numeric, not float =======
    for (const [left, right, returns] of numberPairs()) {
        if (returns === DataType.float) {
            continue;
        }
        schema.registerOperator({
            operator: '%',
            commutative: false,
            left,
            right,
            returns,
            implementation: arith('%', returns),
        });
    }
}


function registerDatetimeOperators(schema: _ISchema) {
    // ======= date "-" date =======
    schema.registerOperator({
        operator: '-',
        commutative: false,
        left: Types.date,
        right: Types.date,
        returns: Types.interval,
        implementation: (a, b) => moment(a).diff(moment(b), 'days'),
    })

    // ======= date/time "+ -" timestamp =======
    for (const dt of dateTypes) {
        for (const [operator, f] of [['+', 1], ['-', -1]] as const) {
            schema.registerOperator({
                operator,
                commutative: operator === '+',
                left: dt,
                right: Types.interval,
                returns: dt,
                implementation: (a, b) => dateAddInterval(a, b, f),
            });
        }
    }

    // ======= timestamp/timestamptz "AT TIME ZONE" zone =======
    // timestamp AT TIME ZONE zone -> timestamptz (wall-clock interpreted as local in zone)
    schema.registerOperator({
        operator: 'AT TIME ZONE',
        commutative: false,
        left: Types.timestamp(),
        right: Types.text(),
        returns: Types.timestamptz(),
        implementation: (ts, zone) => timestampAtZone(ts, zone),
    });
    // timestamptz AT TIME ZONE zone -> timestamp (instant rendered as wall-clock in zone)
    schema.registerOperator({
        operator: 'AT TIME ZONE',
        commutative: false,
        left: Types.timestamptz(),
        right: Types.text(),
        returns: Types.timestamp(),
        implementation: (instant, zone) => instantToZoneWall(instant, zone),
    });

    // ==== date "+ -" integer  (add days.. only works on dates, not timestamps)
    for (const [operator, f] of [['+', 1], ['-', -1]] as const) {
        schema.registerOperator({
            operator,
            commutative: operator === '+',
            left: Types.date,
            right: Types.integer,
            returns: Types.date,
            implementation: (a, b) => moment(a).add(f * b, 'days').toDate(),
        });
    }
}



function registerJsonOperators(schema: _ISchema) {
    // ======= "json @> json" query operator
    schema.registerOperator({
        operator: '@>',
        left: Types.jsonb,
        right: Types.jsonb,
        returns: Types.bool,
        implementation: (a, b) => queryJson(b, a),
    });

    // ======= "json #> path" (extract json at path)
    schema.registerOperator({
        operator: '#>',
        left: Types.jsonb,
        right: Types.text().asArray(),
        returns: Types.jsonb,
        implementation: (a, b: string[]) => jsonPathGet(a, b),
    });

    // ======= "json #>> path" (extract text at path)
    schema.registerOperator({
        operator: '#>>',
        left: Types.jsonb,
        right: Types.text().asArray(),
        returns: Types.text(),
        implementation: (a, b: string[]) => jsonAsText(jsonPathGet(a, b)),
    });

    // ======= "json #- path" (remove element at path)
    schema.registerOperator({
        operator: '#-',
        left: Types.jsonb,
        right: Types.text().asArray(),
        returns: Types.jsonb,
        implementation: (a, b: string[]) => jsonRemovePath(a, b),
    });

    // ======= "json - text" (remove key)
    schema.registerOperator({
        operator: '-',
        left: Types.jsonb,
        right: Types.text(),
        returns: Types.jsonb,
        implementation: (a, b) => {
            if (Array.isArray(a)) {
                return a.filter(x => x !== b);
            }
            if (typeof a === 'object') {
                const ret = { ...a };
                delete ret[b];
                return ret;
            }
            throw new QueryError('cannot delete from scalar', '22023');
        },
    });

    // ======= "json - int" (remove index)
    schema.registerOperator({
        operator: '-',
        left: Types.jsonb,
        right: Types.integer,
        returns: Types.jsonb,
        implementation: (a, b) => {
            if (Array.isArray(a)) {
                const ret = [...a];
                ret.splice(b, 1);
                return ret;
            }
            if (typeof a === 'object') {
                throw new QueryError('cannot delete from object using integer index', '22023');
            }
            throw new QueryError('cannot delete from scalar', '22023');
        },
    })
}


function registerTextOperators(schema: _ISchema) {
    // ======== "text || text" (concat text operator)
    schema.registerOperator({
        operator: '||',
        left: Types.text(),
        right: Types.text(),
        returns: Types.text(),
        implementation: (a, b) => a + b,
    })
}
