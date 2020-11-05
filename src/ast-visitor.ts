import { Expr, ExprBinary, ExprUnary, ExprRef, ExprList, ExprCall, ExprCast, ExprCase, ExprMember, ExprArrayIndex, ExprTernary, SelectStatement } from './parser/syntax/ast';
import { nil, NotSupported } from './interfaces-private';

export class AstVisitor {
    visit(val: Expr | nil) {
        if (!val) {
            return;
        }
        switch (val.type) {
            case 'binary':
                return this.visitBinary(val);
            case 'unary':
                return this.visitUnary(val);
            case 'ref':
                return this.visitRef(val);
            case 'string':
            case 'numeric':
            case 'integer':
            case 'boolean':
            case 'constant':
                return this.visitConstant(val.value);
            case 'null':
                return this.visitConstant(null);
            case 'list':
                return this.visitArray(val);
            case 'call':
                return this.visitCall(val);
            case 'cast':
                return this.visitCast(val)
            case 'case':
                return this.visitCase(val);
            case 'member':
                return this.visitMember(val);
            case 'arrayIndex':
                return this.visitArrayIndex(val);
            case 'ternary':
                return this.visitTernary(val);
            case 'select':
                return this.visitSelection(val);
            default:
                throw NotSupported.never(val);
        }
    }
    visitSelection(val: SelectStatement) {
        for (const c of val.columns!) {
            this.visit(c.expr);
        }
        this.visit(val.where);
    }
    visitTernary(val: ExprTernary) {
        this.visit(val.value);
        this.visit(val.lo);
        this.visit(val.hi);
    }
    visitArrayIndex(val: ExprArrayIndex) {
        this.visit(val.array);
        this.visit(val.index);
    }
    visitMember(val: ExprMember) {
        this.visit(val.operand);
    }
    visitCase(val: ExprCase) {
        this.visit(val.value);
        for (const w of val.whens) {
            this.visit(w.when);
            this.visit(w.value);
        }
        this.visit(val.else);
    }
    visitCast(val: ExprCast) {
        this.visit(val.operand);
    }
    visitCall(val: ExprCall) {
        for (const a of val.args) {
            this.visit(a);
        }
    }

    visitArray(val: ExprList) {
        for (const e of val.expressions) {
            this.visit(e);
        }
    }

    visitConstant(value: string | number | boolean | null) {
    }

    visitRef(val: ExprRef) {

    }

    visitUnary(val: ExprUnary) {
        this.visit(val.operand);
    }

    visitBinary(val: ExprBinary) {
        this.visit(val.left);
        this.visit(val.right);
    }
}
