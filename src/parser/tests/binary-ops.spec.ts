import 'mocha';
import 'chai';
import { checkTreeExpr } from './spec-utils';

describe('PG syntax: Binary operations & precedence', () => {


    checkTreeExpr(['42', '(42)'], {
        type: 'integer',
        value: 42,
    });


    checkTreeExpr('*', {
        type: 'star',
    });

    checkTreeExpr('a.*', {
        type: 'member',
        operand: {
            type: 'ref',
            name: 'a',
        },
        member: '*',
    });

    checkTreeExpr('a.b', {
        type: 'member',
        operand: {
            type: 'ref',
            name: 'a',
        },
        member: 'b',
    });

    checkTreeExpr(['42.', '42.0'], {
        type: 'numeric',
        value: 42,
    });

    checkTreeExpr(['.42', '0.42'], {
        type: 'numeric',
        value: .42,
    });

    checkTreeExpr(['42+51', '42 + 51'], {
        type: 'binary',
        op: '+',
        left: {
            type: 'integer',
            value: 42,
        },
        right: {
            type: 'integer',
            value: 51,
        }
    });

    checkTreeExpr(['42*51', '42 * 51'], {
        type: 'binary',
        op: '*',
        left: {
            type: 'integer',
            value: 42,
        },
        right: {
            type: 'integer',
            value: 51,
        }
    });


    checkTreeExpr('42 + 51 - 30', {
        type: 'binary',
        op: '-',
        left: {
            type: 'binary',
            op: '+',
            left: {
                type: 'integer',
                value: 42,
            },
            right: {
                type: 'integer',
                value: 51,
            }
        },
        right: {
            type: 'integer',
            value: 30,
        },
    });

    checkTreeExpr(['(a + b)::jsonb', '(a + b)::"JSONB"'], {
        type: 'cast',
        to: 'JSONB',
        operand: {
            type: 'binary',
            op: '+',
            left: {
                type: 'ref',
                name: 'a',
            },
            right: {
                type: 'ref',
                name: 'b',
            }
        },
    });


    checkTreeExpr(['a + b::jsonb', '"a"+"b"::"JSONB"'], {
        type: 'binary',
        op: '+',
        left: {
            type: 'ref',
            name: 'a',
        },
        right: {
            type: 'cast',
            to: 'JSONB',
            operand: {
                type: 'ref',
                name: 'b',
            },
        },
    });

    checkTreeExpr('2 + 3 * 4', {
        type: 'binary',
        op: '+',
        left: {
            type: 'integer',
            value: 2,
        },
        right: {
            type: 'binary',
            op: '*',
            left: {
                type: 'integer',
                value: 3,
            },
            right: {
                type: 'integer',
                value: 4,
            }
        }
    });

    checkTreeExpr('2 * 3 + 4', {
        type: 'binary',
        op: '+',
        left: {
            type: 'binary',
            op: '*',
            left: {
                type: 'integer',
                value: 2,
            },
            right: {
                type: 'integer',
                value: 3,
            }
        },
        right: {
            type: 'integer',
            value: 4,
        },
    });


    checkTreeExpr('2. * .3 + 4.5', {
        type: 'binary',
        op: '+',
        left: {
            type: 'binary',
            op: '*',
            left: {
                type: 'numeric',
                value: 2,
            },
            right: {
                type: 'numeric',
                value: 0.3,
            }
        },
        right: {
            type: 'numeric',
            value: 4.5,
        },
    });


    checkTreeExpr(['2 * (3 + 4)', '2*(3+4)'], {
        type: 'binary',
        op: '*',
        left: {
            type: 'integer',
            value: 2,
        },
        right: {
            type: 'binary',
            op: '+',
            left: {
                type: 'integer',
                value: 3,
            },
            right: {
                type: 'integer',
                value: 4,
            }
        },
    });
});