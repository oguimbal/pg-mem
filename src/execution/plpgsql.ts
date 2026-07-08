import { _IDb, _ISchema, _ITable, _Transaction, IValue, _ISelection, QueryError, NotSupported, getId, setId } from '../interfaces-private';
import { Expr, parse } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { withSelection } from '../parser/context';
import { JoinSelection } from '../transforms/join';
import { Types } from '../datatypes';

// A minimal PL/pgSQL interpreter, scoped to the common trigger-function subset:
//   BEGIN <stmts> END
//   <NEW|OLD>.<col> := <expr>   (and `=`)
//   RETURN NEW | OLD | NULL | <expr>
//   IF <cond> THEN <stmts> [ELSIF <cond> THEN <stmts>]* [ELSE <stmts>] END IF
//   RAISE ... (parsed & ignored)
// Expressions are real SQL, evaluated with NEW/OLD in scope.

export interface TriggerContext {
    table: _ITable;
    new: any | null;
    old: any | null;
    op: 'INSERT' | 'UPDATE' | 'DELETE';
}

type Stmt =
    | { kind: 'assign'; target: 'new' | 'old'; column: string; expr: string }
    | { kind: 'return'; expr: string }
    | { kind: 'if'; branches: { cond: string; body: Stmt[] }[]; else?: Stmt[] }
    | { kind: 'raise' };

// ---- parsing -------------------------------------------------------------------------

function stripComments(code: string): string {
    return code
        .replace(/--[^\n]*/g, '')
        .replace(/\/\*[\s\S]*?\*\//g, '');
}

/** Split a plpgsql fragment into top-level statements at ';', respecting parens and the
 * IF…END IF nesting. Returns statement source strings (without the trailing ';'). */
class PlpgsqlParser {
    private toks: string[];
    private i = 0;

    constructor(code: string) {
        // tokenize into words, punctuation and strings
        this.toks = stripComments(code).match(/'(?:[^']|'')*'|[a-zA-Z_][\w$]*|:=|[(),.;]|[^\s]/g) ?? [];
    }

    private peek(n = 0) { return this.toks[this.i + n]?.toLowerCase(); }
    private next() { return this.toks[this.i++]; }
    private eatKw(kw: string) {
        if (this.peek() !== kw) { throw new QueryError(`plpgsql: expected "${kw}", got "${this.toks[this.i] ?? '<eof>'}"`); }
        this.i++;
    }

    parseBlock(): Stmt[] {
        this.eatKw('begin');
        const stmts = this.parseStmtsUntil(['end']);
        this.eatKw('end');
        return stmts;
    }

    private parseStmtsUntil(terminators: string[]): Stmt[] {
        const out: Stmt[] = [];
        while (this.i < this.toks.length && !terminators.includes(this.peek()!)) {
            const s = this.parseStmt();
            if (s) { out.push(s); }
        }
        return out;
    }

    private parseStmt(): Stmt | null {
        const kw = this.peek();
        if (kw === 'return') {
            this.i++;
            const expr = this.readUntilSemicolon();
            return { kind: 'return', expr };
        }
        if (kw === 'if') {
            return this.parseIf();
        }
        if (kw === 'raise') {
            this.readUntilSemicolon();
            return { kind: 'raise' };
        }
        // assignment:  new.col := expr   |   new.col = expr
        const target = this.peek();
        if ((target === 'new' || target === 'old') && this.peek(1) === '.') {
            this.i += 2; // target .
            const column = this.next();
            const op = this.peek();
            if (op !== ':=' && op !== '=') {
                throw new QueryError(`plpgsql: expected assignment after ${target}.${column}`);
            }
            this.i++;
            const expr = this.readUntilSemicolon();
            return { kind: 'assign', target: target as 'new' | 'old', column, expr };
        }
        throw new NotSupported(`plpgsql statement starting with "${this.toks[this.i]}"`);
    }

    private parseIf(): Stmt {
        this.eatKw('if');
        const branches: { cond: string; body: Stmt[] }[] = [];
        let cond = this.readUntil(['then']);
        this.eatKw('then');
        branches.push({ cond, body: this.parseStmtsUntil(['elsif', 'else', 'end']) });
        while (this.peek() === 'elsif') {
            this.i++;
            cond = this.readUntil(['then']);
            this.eatKw('then');
            branches.push({ cond, body: this.parseStmtsUntil(['elsif', 'else', 'end']) });
        }
        let elseBody: Stmt[] | undefined;
        if (this.peek() === 'else') {
            this.i++;
            elseBody = this.parseStmtsUntil(['end']);
        }
        this.eatKw('end');
        this.eatKw('if');
        if (this.peek() === ';') { this.i++; }
        return { kind: 'if', branches, else: elseBody };
    }

    private readUntilSemicolon(): string {
        return this.readUntil([';'], true);
    }

    private readUntil(stops: string[], consumeStop = false): string {
        const parts: string[] = [];
        let depth = 0;
        while (this.i < this.toks.length) {
            const t = this.peek()!;
            if (depth === 0 && stops.includes(t)) {
                if (consumeStop) { this.i++; }
                break;
            }
            if (this.toks[this.i] === '(') { depth++; }
            if (this.toks[this.i] === ')') { depth--; }
            parts.push(this.next());
        }
        return joinTokens(parts);
    }
}

function joinTokens(toks: string[]): string {
    let out = '';
    for (let i = 0; i < toks.length; i++) {
        const t = toks[i];
        const prev = toks[i - 1];
        const noSpace = t === '.' || t === '(' || t === ')' || t === ',' || prev === '.' || prev === '(';
        out += (i > 0 && !noSpace ? ' ' : '') + t;
    }
    return out;
}

// ---- compilation & execution ---------------------------------------------------------

interface CompiledStmt {
    run(ctx: TriggerContext, t: _Transaction): { returned?: any } | void;
}

/** Builds a NEW⋈OLD context selection so `new.col` / `old.col` resolve, then compiles a
 * scalar SQL expression string against it. Cached per table. */
class TriggerCompiler {
    private ctxSel: _ISelection;
    private buildRow: (ctx: TriggerContext) => any;

    constructor(private table: _ITable) {
        const newSel = table.selection.setAlias('new');
        const oldSel = table.selection.setAlias('old');
        const join = new JoinSelection(newSel, oldSel, { type: 'INNER JOIN', on: { type: 'boolean', value: true } }, true);
        this.ctxSel = join;
        // buildItem only reads columns off its operands and derives a joined-row id from
        // their ids; a freshly-built NEW row (e.g. from INSERT) has none yet. Feed it
        // shallow copies so a throwaway id never lands on the real row that will be
        // inserted (which must still receive the table's own id later).
        this.buildRow = (ctx) => join.buildItem(
            ensureId({ ...(ctx.new ?? {}) }, 'trg_new'),
            ensureId({ ...(ctx.old ?? {}) }, 'trg_old'),
        );
    }

    compileExpr(exprSrc: string): (ctx: TriggerContext, t: _Transaction) => any {
        const ast = parseExpr(exprSrc);
        const value: IValue = withSelection(this.ctxSel, () => buildValue(ast));
        return (ctx, t) => value.get(this.buildRow(ctx), t);
    }

    columnId(name: string): string {
        return this.table.getColumnRef(name).expression.id!;
    }

    row(ctx: TriggerContext) { return this.buildRow(ctx); }
    get selection() { return this.ctxSel; }
}

let idCounter = 0;
function ensureId(row: any, prefix: string): any {
    try {
        getId(row);
    } catch {
        setId(row, `${prefix}_${idCounter++}`);
    }
    return row;
}

function parseExpr(src: string): Expr {
    const stmt = parse(`select ${src} as _v`);
    const one = Array.isArray(stmt) ? stmt[0] : stmt;
    if (one.type !== 'select' || !one.columns) {
        throw new QueryError(`plpgsql: invalid expression "${src}"`);
    }
    return one.columns[0].expr;
}

function compile(stmts: Stmt[], c: TriggerCompiler): CompiledStmt[] {
    return stmts.map<CompiledStmt>(s => {
        switch (s.kind) {
            case 'assign': {
                const val = c.compileExpr(s.expr);
                // row objects are keyed by column id (== name for ordinary tables)
                const colId = c.columnId(s.column);
                return {
                    run(ctx, t) {
                        const target = ctx[s.target];
                        if (!target) {
                            throw new QueryError(`record "${s.target}" is not assigned yet`);
                        }
                        target[colId] = val(ctx, t);
                    },
                };
            }
            case 'return': {
                const src = s.expr.trim().toLowerCase();
                if (src === 'new' || src === 'old') {
                    return { run: (ctx) => ({ returned: ctx[src as 'new' | 'old'] }) };
                }
                if (src === 'null') {
                    return { run: () => ({ returned: null }) };
                }
                const val = c.compileExpr(s.expr);
                return { run: (ctx, t) => ({ returned: val(ctx, t) }) };
            }
            case 'if': {
                const branches = s.branches.map(b => ({ cond: c.compileExpr(b.cond), body: compile(b.body, c) }));
                const elseBody = s.else ? compile(s.else, c) : undefined;
                return {
                    run(ctx, t) {
                        for (const b of branches) {
                            if (b.cond(ctx, t)) {
                                return runBody(b.body, ctx, t);
                            }
                        }
                        if (elseBody) { return runBody(elseBody, ctx, t); }
                    },
                };
            }
            case 'raise':
                return { run() { /* messages not surfaced yet */ } };
        }
    });
}

function runBody(body: CompiledStmt[], ctx: TriggerContext, t: _Transaction): { returned?: any } | void {
    for (const st of body) {
        const r = st.run(ctx, t);
        if (r && 'returned' in r) { return r; }
    }
}

/** A compiled trigger function: given a trigger context, runs the body and returns the
 * resulting row (or null). */
export type TriggerRunner = (ctx: TriggerContext, t: _Transaction) => any;

export function registerPlpgsqlLanguage(db: _IDb) {
    db.registerLanguage('plpgsql', ({ code }) => {
        const body = new PlpgsqlParser(code).parseBlock();
        // expressions are compiled lazily per table (a function may be attached to many)
        const perTable = new Map<_ITable, CompiledStmt[]>();
        const runner: TriggerRunner = (ctx, t) => {
            let compiled = perTable.get(ctx.table);
            if (!compiled) {
                // establish a build context (compilation builds selections & expressions,
                // which the executor phase otherwise lacks)
                compiled = withSelection(ctx.table.selection, () => {
                    const c = new TriggerCompiler(ctx.table);
                    return compile(body, c);
                });
                perTable.set(ctx.table, compiled);
            }
            const r = runBody(compiled, ctx, t);
            return r && 'returned' in r ? r.returned : ctx.new;
        };
        // trigger functions are never called as scalar functions; carry the runner so the
        // trigger executor can invoke it
        const impl: any = () => { throw new QueryError('trigger function cannot be called directly'); };
        impl.__triggerRunner = runner;
        return impl;
    });
}

export function getTriggerRunner(impl: any): TriggerRunner | null {
    return impl?.__triggerRunner ?? null;
}
