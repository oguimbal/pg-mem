import 'mocha';
import 'chai';
import { checkTreeExpr, checkInvalid } from './spec-utils';

describe('PG syntax: Expressions', () => {

    // ====================================
    // =============== VALUES =============
    // ====================================

    describe('Comments', () => {
        checkTreeExpr(['2 /* yo */ + 2', '2 +/* yo */ 2', '2+-- yo \n2', '2-- yo \n+2', '2+-- yo \n  \n2'], {
            type: 'binary',
            op: '+',
            left: { type: 'integer', value: 2},
            right: { type: 'integer', value: 2},
        });

    });

    // ====================================
    // =============== VALUES =============
    // ====================================

    describe('Simple values & arithmetic precedence', () => {

        checkTreeExpr(['42', '(42)'], {
            type: 'integer',
            value: 42,
        });

        checkTreeExpr(['true', '(true)'], {
            type: 'boolean',
            value: true,
        });

        checkTreeExpr(['false', '(false)'], {
            type: 'boolean',
            value: false,
        });


        checkTreeExpr([`'test'`], {
            type: 'string',
            value: 'test',
        });

        checkTreeExpr([`'te''st'`], {
            type: 'string',
            value: `te'st`,
        });

        checkTreeExpr([`'te"s\\\\t'`], {
            type: 'string',
            value: `te"s\\t`,
        });

        checkTreeExpr([`'te\\n\\tst'`], {
            type: 'string',
            value: `te\n\tst`,
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
    })


    // ====================================
    // =============== LOGIC ==============
    // ====================================

    describe('Logic', () => {
        checkTreeExpr(['a and b OR c', '"a"AND"b"or"c"', '"a"and "b"or "c"'], {
            type: 'binary',
            op: 'OR',
            left: {
                type: 'binary',
                op: 'AND',
                left: { type: 'ref', name: 'a' },
                right: { type: 'ref', name: 'b' },
            },
            right: { type: 'ref', name: 'c' }
        });

        checkTreeExpr(['a or b AND c', '"a"OR"b"and"c"', '"a"or "b"and "c"'], {
            type: 'binary',
            op: 'OR',
            left: { type: 'ref', name: 'a' },
            right: {
                type: 'binary',
                op: 'AND',
                left: { type: 'ref', name: 'b' },
                right: { type: 'ref', name: 'c' },
            },
        });

        checkTreeExpr('a.b or c', {
            type: 'binary',
            op: 'OR',
            left: {
                type: 'member',
                operand: { type: 'ref', name: 'a' },
                member: 'b',
            },
            right: { type: 'ref', name: 'c' },
        });
    });


    // ====================================
    // =============== CASTS ==============
    // ====================================

    describe('Cast', () => {
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
    });


    // ====================================
    // =============== UNARIES ============
    // ====================================
    describe('Unaries', () => {
        checkTreeExpr(['not a and b', 'NOT"a"and"b"'], {
            type: 'binary',
            op: 'AND',
            left: {
                type: 'unary',
                op: 'NOT',
                operand: { type: 'ref', name: 'a' },
            },
            right: { type: 'ref', name: 'b' },
        });

        checkInvalid('not not a');

        checkTreeExpr(['not a is null', 'not"a"is null', 'not a isnull', 'not"a"isnull'], {
            type: 'unary',
            op: 'NOT',
            operand: {
                type: 'unary',
                op: 'IS NULL',
                operand: { type: 'ref', name: 'a' }
            }
        });

        checkTreeExpr(['a is not null', 'a notnull', '"a"notnull'], {
            type: 'unary',
            op: 'IS NOT NULL',
            operand: { type: 'ref', name: 'a' }
        });


        checkTreeExpr(['a is null is null', 'a isnull isnull', 'a is null isnull', 'a isnull is null'], {
            type: 'unary',
            op: 'IS NULL',
            operand: {
                type: 'unary',
                op: 'IS NULL',
                operand: { type: 'ref', name: 'a' },
            }
        });

        checkTreeExpr(['a is false is true'], {
            type: 'unary',
            op: 'IS TRUE',
            operand: {
                type: 'unary',
                op: 'IS FALSE',
                operand: { type: 'ref', name: 'a' },
            }
        });

        checkTreeExpr(['a is not false is not true'], {
            type: 'unary',
            op: 'IS NOT TRUE',
            operand: {
                type: 'unary',
                op: 'IS NOT FALSE',
                operand: { type: 'ref', name: 'a' },
            }
        });


        checkTreeExpr(['+a', '+ a', '+"a"'], {
            type: 'unary',
            op: '+',
            operand: { type: 'ref', name: 'a' }
        });

        checkTreeExpr(['-a', '- a', '-"a"'], {
            type: 'unary',
            op: '-',
            operand: { type: 'ref', name: 'a' }
        });
    });


    // ====================================
    // ============== BINARIES ============
    // ====================================
    describe('Binaries', () => {
        checkTreeExpr(['a > b', 'a>b', '"a">"b"'], {
            type: 'binary',
            op: '>',
            left: { type: 'ref', name: 'a' },
            right: { type: 'ref', name: 'b' },
        });


        checkTreeExpr(['a like b', '"a"LIKE"b"'], {
            type: 'binary',
            op: 'LIKE',
            left: { type: 'ref', name: 'a' },
            right: { type: 'ref', name: 'b' },
        });

        checkTreeExpr(['a not like b', '"a"not LIKE"b"'], {
            type: 'binary',
            op: 'NOT LIKE',
            left: { type: 'ref', name: 'a' },
            right: { type: 'ref', name: 'b' },
        });

        checkTreeExpr(['a^b', '"a"^"b"', 'a ^ b'], {
            type: 'binary',
            op: '^',
            left: { type: 'ref', name: 'a' },
            right: { type: 'ref', name: 'b' },
        });
    });


    // ====================================
    // =============== TERNARIES ==========
    // ====================================
    describe('Ternaries', () => {
        checkTreeExpr(['a between b and 42', '"a"between"b"and 42'], {
            type: 'ternary',
            op: 'BETWEEN',
            value: { type: 'ref', name: 'a' },
            lo: { type: 'ref', name: 'b' },
            hi: { type: 'integer', value: 42 },
        });

        checkTreeExpr(['a not between b and 42', 'a not     between b and 42', '"a"not between"b"and 42'], {
            type: 'ternary',
            op: 'NOT BETWEEN',
            value: { type: 'ref', name: 'a' },
            lo: { type: 'ref', name: 'b' },
            hi: { type: 'integer', value: 42 },
        });
    });


    // ====================================
    // =============== MEMBERS ============
    // ====================================

    describe('Member access', () => {
        checkTreeExpr(['a.b[c]', 'a."b"["c"]'], {
            type: 'arrayIndex',
            array: {
                type: 'member',
                operand: { type: 'ref', name: 'a' },
                member: 'b'
            },
            index: { type: 'ref', name: 'c' }
        })
    });
});