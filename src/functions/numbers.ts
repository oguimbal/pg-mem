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
]
