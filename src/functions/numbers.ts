import { DataType, FunctionDefinition, QueryError } from "../interfaces-private";

// pg rounds numerics half away from zero; js Math.round rounds half toward +Infinity
function roundHalfAwayFromZero(x: number): number {
    return Math.sign(x) * Math.round(Math.abs(x));
}

export const numberFunctions: FunctionDefinition[] = [
    {
        name: 'greatest',
        args: [DataType.integer],
        argsVariadic: DataType.integer,
        returns: DataType.integer,
        implementation: (...args: number[]) => Math.max(...args),
    },
    {
        name: 'least',
        args: [DataType.integer],
        argsVariadic: DataType.integer,
        returns: DataType.integer,
        implementation: (...args: number[]) => Math.min(...args),
    },
    {
        name: 'abs',
        args: [DataType.float],
        returns: DataType.float,
        implementation: Math.abs,
    },
    ...['ceil', 'ceiling'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.float],
        returns: DataType.float,
        implementation: Math.ceil,
    })),
    {
        name: 'floor',
        args: [DataType.float],
        returns: DataType.float,
        implementation: Math.floor,
    },
    {
        name: 'round',
        args: [DataType.float],
        returns: DataType.float,
        implementation: roundHalfAwayFromZero,
    },
    {
        name: 'round',
        args: [DataType.float, DataType.integer],
        returns: DataType.float,
        implementation: (x: number, scale: number) => {
            const factor = 10 ** scale;
            return roundHalfAwayFromZero(x * factor) / factor;
        },
    },
    {
        name: 'trunc',
        args: [DataType.float],
        returns: DataType.float,
        implementation: Math.trunc,
    },
    {
        name: 'trunc',
        args: [DataType.float, DataType.integer],
        returns: DataType.float,
        implementation: (x: number, scale: number) => {
            const factor = 10 ** scale;
            return Math.trunc(x * factor) / factor;
        },
    },
    {
        name: 'power',
        args: [DataType.float, DataType.float],
        returns: DataType.float,
        implementation: Math.pow,
    },
    {
        name: 'sqrt',
        args: [DataType.float],
        returns: DataType.float,
        implementation: (x: number) => {
            if (x < 0) {
                throw new QueryError('cannot take square root of a negative number');
            }
            return Math.sqrt(x);
        },
    },
    {
        name: 'cbrt',
        args: [DataType.float],
        returns: DataType.float,
        implementation: Math.cbrt,
    },
    {
        name: 'exp',
        args: [DataType.float],
        returns: DataType.float,
        implementation: Math.exp,
    },
    {
        name: 'ln',
        args: [DataType.float],
        returns: DataType.float,
        implementation: (x: number) => {
            if (x <= 0) {
                throw new QueryError('cannot take logarithm of a nonpositive number');
            }
            return Math.log(x);
        },
    },
    {
        name: 'log',
        args: [DataType.float],
        returns: DataType.float,
        implementation: (x: number) => {
            if (x <= 0) {
                throw new QueryError('cannot take logarithm of a nonpositive number');
            }
            return Math.log10(x);
        },
    },
    {
        name: 'mod',
        args: [DataType.integer, DataType.integer],
        returns: DataType.integer,
        implementation: (a: number, b: number) => {
            if (b === 0) {
                throw new QueryError('division by zero');
            }
            return a % b;
        },
    },
    {
        name: 'sign',
        args: [DataType.float],
        returns: DataType.integer,
        implementation: Math.sign,
    },
    {
        name: 'pi',
        args: [],
        returns: DataType.float,
        implementation: () => Math.PI,
    },
    {
        name: 'degrees',
        args: [DataType.float],
        returns: DataType.float,
        implementation: (x: number) => x * 180 / Math.PI,
    },
    {
        name: 'radians',
        args: [DataType.float],
        returns: DataType.float,
        implementation: (x: number) => x * Math.PI / 180,
    },
    {
        name: 'div',
        args: [DataType.integer, DataType.integer],
        returns: DataType.integer,
        implementation: (a: number, b: number) => {
            if (b === 0) { throw new QueryError('division by zero', '22012'); }
            return Math.trunc(a / b);
        },
    },
    {
        name: 'gcd',
        args: [DataType.integer, DataType.integer],
        returns: DataType.integer,
        implementation: (a: number, b: number) => {
            a = Math.abs(a); b = Math.abs(b);
            while (b) { [a, b] = [b, a % b]; }
            return a;
        },
    },
    {
        name: 'lcm',
        args: [DataType.integer, DataType.integer],
        returns: DataType.integer,
        implementation: (a: number, b: number) => {
            if (a === 0 || b === 0) { return 0; }
            let x = Math.abs(a), y = Math.abs(b);
            let g = x, h = y;
            while (h) { [g, h] = [h, g % h]; }
            return Math.abs(a / g * b);
        },
    },
    {
        name: 'factorial',
        args: [DataType.integer],
        returns: DataType.integer,
        implementation: (n: number) => {
            if (n < 0) { throw new QueryError('factorial of a negative number is undefined', '2201F'); }
            let r = 1;
            for (let i = 2; i <= n; i++) { r *= i; }
            return r;
        },
    },
    {
        name: 'width_bucket',
        args: [DataType.float, DataType.float, DataType.float, DataType.integer],
        returns: DataType.integer,
        implementation: (op: number, lo: number, hi: number, count: number) => {
            if (count <= 0) { throw new QueryError('count must be greater than zero', '22004'); }
            if (lo === hi) { throw new QueryError('lower bound cannot equal upper bound'); }
            const asc = lo < hi;
            if (asc ? op < lo : op > lo) { return 0; }
            if (asc ? op >= hi : op <= hi) { return count + 1; }
            return Math.floor(((op - lo) / (hi - lo)) * count) + 1;
        },
    },
    {
        name: 'bit_length',
        args: [DataType.text],
        returns: DataType.integer,
        implementation: (s: string) => Buffer.byteLength(s, 'utf8') * 8,
    },
    {
        name: 'random',
        args: [],
        returns: DataType.float,
        impure: true,
        implementation: () => Math.random(),
    },
]
