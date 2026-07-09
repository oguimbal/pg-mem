import { _IDb, _ISchema, _ITable, _Transaction, IValue, _ISelection, QueryError, NotSupported, getId, setId, _IType, Parameter, nil, StatementResult, _IStatementExecutor } from '../interfaces-private';
import { Expr, parse, SelectStatement } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { buildSelect } from './select';
import { withSelection, withParameters } from '../parser/context';
import { JoinSelection } from '../transforms/join';
import { Types } from '../datatypes';
import { Evaluator } from '../evaluator';
import { executionCtx } from '../utils';
import { StatementExec } from './statement-exec';

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

/** Tokenize a plpgsql fragment: strings, numbers, `..` range, `:=`, words, punctuation. */
function tokenize(code: string): string[] {
    return stripComments(code)
        .match(/'(?:[^']|'')*'|\d+\.\d+|\d+|\.\.|:=|::|>=|<=|<>|!=|\|\||[a-zA-Z_][\w$]*|[(),.;]|[^\s]/g) ?? [];
}

/** Split a plpgsql fragment into top-level statements at ';', respecting parens and the
 * IF…END IF nesting. Returns statement source strings (without the trailing ';'). */
class PlpgsqlParser {
    private toks: string[];
    private i = 0;

    constructor(code: string) {
        this.toks = tokenize(code);
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

/** strip surrounding single quotes and unescape '' -> ' */
function unquote(s: string): string {
    if (s.length >= 2 && s.startsWith(`'`) && s.endsWith(`'`)) {
        return s.slice(1, -1).replace(/''/g, `'`);
    }
    return s;
}

function joinTokens(toks: string[]): string {
    let out = '';
    for (let i = 0; i < toks.length; i++) {
        const t = toks[i];
        const prev = toks[i - 1];
        // nb: we do NOT glue a word to a following "(" - `values(a)` would parse
        // ambiguously; `values (a)` and `f (a)` are both accepted
        const noSpace = t === '.' || t === ')' || t === ',' || prev === '.' || prev === '(';
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
        return this.compileAst(parseExpr(exprSrc));
    }

    compileAst(ast: Expr): (ctx: TriggerContext, t: _Transaction) => any {
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

// ============================================================================
// General PL/pgSQL: regular (callable) functions and DO blocks
// ============================================================================

interface VarDef {
    name: string;
    type: _IType;
    default?: string | null;
}

type GStmt =
    | { k: 'assign'; name: string; expr: string }
    | { k: 'return'; expr: string | null }
    | { k: 'if'; branches: { cond: string; body: GStmt[] }[]; else?: GStmt[] }
    | { k: 'while'; cond: string; body: GStmt[] }
    | { k: 'loop'; body: GStmt[] }
    | { k: 'forrange'; varName: string; reverse: boolean; from: string; to: string; by: string | null; body: GStmt[] }
    | { k: 'exit'; when: string | null }
    | { k: 'continue'; when: string | null }
    | { k: 'block'; body: GStmt[]; handlers: ExceptionHandler[] }
    // embedded SQL
    | { k: 'selectinto'; into: string[]; sql: string }
    | { k: 'perform'; sql: string }
    | { k: 'sql'; sql: string }
    | { k: 'execute'; exprSrc: string }
    | { k: 'forquery'; varName: string; sql: string; body: GStmt[] }
    | { k: 'returnnext'; expr: string }
    | { k: 'returnquery'; sql: string }
    | { k: 'raise'; level: string; format: string | null; args: string[] }
    | { k: 'null' };

interface ExceptionHandler {
    conditions: string[]; // lowercased condition names, or 'others'
    body: GStmt[];
}

/** parses a regular plpgsql function body: optional DECLARE section + BEGIN/END block */
class GParser {
    private toks: string[];
    private i = 0;

    constructor(code: string) {
        this.toks = tokenize(code);
    }

    private peek(n = 0) { return this.toks[this.i + n]?.toLowerCase(); }
    private next() { return this.toks[this.i++]; }
    private eat(kw: string) {
        if (this.peek() !== kw) {
            throw new QueryError(`plpgsql: expected "${kw}", got "${this.toks[this.i] ?? '<eof>'}"`);
        }
        this.i++;
    }

    parse(): { decls: VarDef[]; block: GStmt } {
        const decls: VarDef[] = [];
        if (this.peek() === 'declare') {
            this.i++;
            while (this.i < this.toks.length && this.peek() !== 'begin') {
                decls.push(this.parseDecl());
            }
        }
        return { decls, block: this.parseBlockBody() };
    }

    private parseDecl(): VarDef {
        const name = this.next();
        // read the type tokens up to ':=' / 'default' / ';'
        const typeToks: string[] = [];
        while (this.i < this.toks.length && this.peek() !== ';' && this.peek() !== ':=' && this.peek() !== 'default') {
            typeToks.push(this.next());
        }
        let def: string | null = null;
        if (this.peek() === ':=' || this.peek() === 'default') {
            this.i++;
            def = this.readUntil([';'], true);
        } else if (this.peek() === ';') {
            this.i++;
        }
        return { name, type: null as any, default: def, ...{ typeSrc: joinTokens(typeToks) } } as any;
    }

    /** BEGIN <stmts> [EXCEPTION WHEN ... THEN ...]* END */
    private parseBlockBody(): GStmt {
        this.eat('begin');
        const body = this.parseStmtsUntil(['end', 'exception']);
        const handlers: ExceptionHandler[] = [];
        if (this.peek() === 'exception') {
            this.i++;
            while (this.peek() === 'when') {
                this.i++;
                const conditions: string[] = [];
                const readCond = () => {
                    if (this.peek() === 'sqlstate') { this.i++; conditions.push(unquote(this.next())); }
                    else { conditions.push(this.next().toLowerCase()); }
                };
                readCond();
                while (this.peek() === 'or') { this.i++; readCond(); }
                this.eat('then');
                handlers.push({ conditions, body: this.parseStmtsUntil(['when', 'end']) });
            }
        }
        this.eat('end');
        if (this.peek() === ';') { this.i++; }
        return { k: 'block', body, handlers };
    }

    private parseStmtsUntil(terms: string[]): GStmt[] {
        const out: GStmt[] = [];
        while (this.i < this.toks.length && !terms.includes(this.peek()!)) {
            const s = this.parseStmt();
            if (s) { out.push(s); }
        }
        return out;
    }

    private parseStmt(): GStmt | null {
        const kw = this.peek();
        switch (kw) {
            case 'return': {
                this.i++;
                if (this.peek() === 'next') { this.i++; return { k: 'returnnext', expr: this.readUntil([';'], true) }; }
                if (this.peek() === 'query') { this.i++; return { k: 'returnquery', sql: this.readUntil([';'], true) }; }
                if (this.peek() === ';') { this.i++; return { k: 'return', expr: null }; }
                return { k: 'return', expr: this.readUntil([';'], true) };
            }
            case 'if':
                return this.parseIf();
            case 'while': {
                this.i++;
                const cond = this.readUntil(['loop']);
                this.eat('loop');
                const body = this.parseStmtsUntil(['end']);
                this.eat('end'); this.eat('loop'); if (this.peek() === ';') { this.i++; }
                return { k: 'while', cond, body };
            }
            case 'loop': {
                this.i++;
                const body = this.parseStmtsUntil(['end']);
                this.eat('end'); this.eat('loop'); if (this.peek() === ';') { this.i++; }
                return { k: 'loop', body };
            }
            case 'for':
                return this.parseFor();
            case 'exit':
            case 'continue': {
                this.i++;
                let when: string | null = null;
                if (this.peek() === 'when') { this.i++; when = this.readUntil([';'], true); }
                else if (this.peek() === ';') { this.i++; }
                return { k: kw as 'exit' | 'continue', when };
            }
            case 'raise':
                return this.parseRaise();
            case 'select':
                return this.parseSelect();
            case 'perform': {
                this.i++;
                return { k: 'perform', sql: 'select ' + this.readUntil([';'], true) };
            }
            case 'insert':
            case 'update':
            case 'delete':
                return { k: 'sql', sql: joinTokens(this.captureStmt()) };
            case 'execute': {
                this.i++;
                return { k: 'execute', exprSrc: this.readUntil([';'], true) };
            }
            case 'null':
                this.i++; if (this.peek() === ';') { this.i++; }
                return { k: 'null' };
            case 'begin':
                return this.parseBlockBody();
            default: {
                // assignment:  name := expr   |   name = expr
                const name = this.next();
                const op = this.peek();
                if (op !== ':=' && op !== '=') {
                    throw new NotSupported(`plpgsql statement starting with "${name}"`);
                }
                this.i++;
                const expr = this.readUntil([';'], true);
                return { k: 'assign', name, expr };
            }
        }
    }

    private parseIf(): GStmt {
        this.eat('if');
        const branches: { cond: string; body: GStmt[] }[] = [];
        let cond = this.readUntil(['then']);
        this.eat('then');
        branches.push({ cond, body: this.parseStmtsUntil(['elsif', 'else', 'end']) });
        while (this.peek() === 'elsif') {
            this.i++;
            cond = this.readUntil(['then']);
            this.eat('then');
            branches.push({ cond, body: this.parseStmtsUntil(['elsif', 'else', 'end']) });
        }
        let elseBody: GStmt[] | undefined;
        if (this.peek() === 'else') {
            this.i++;
            elseBody = this.parseStmtsUntil(['end']);
        }
        this.eat('end'); this.eat('if'); if (this.peek() === ';') { this.i++; }
        return { k: 'if', branches, else: elseBody };
    }

    private parseFor(): GStmt {
        this.eat('for');
        const varName = this.next();
        this.eat('in');
        // FOR rec IN SELECT ... LOOP  (query loop)
        if (this.peek() === 'select') {
            const sql = this.readUntil(['loop']);
            this.eat('loop');
            const body = this.parseStmtsUntil(['end']);
            this.eat('end'); this.eat('loop'); if (this.peek() === ';') { this.i++; }
            return { k: 'forquery', varName, sql, body };
        }
        // FOR i IN [REVERSE] a..b [BY s] LOOP  (integer range)
        let reverse = false;
        if (this.peek() === 'reverse') { this.i++; reverse = true; }
        const from = this.readUntil(['..']);
        this.eat('..');
        const to = this.readUntil(['by', 'loop']);
        let by: string | null = null;
        if (this.peek() === 'by') { this.i++; by = this.readUntil(['loop']); }
        this.eat('loop');
        const body = this.parseStmtsUntil(['end']);
        this.eat('end'); this.eat('loop'); if (this.peek() === ';') { this.i++; }
        return { k: 'forrange', varName, reverse, from, to, by, body };
    }

    /** RAISE [level] ['format' [, expr]*] [USING ...] */
    private parseRaise(): GStmt {
        this.eat('raise');
        const raw = this.readRaw([';'], true);
        const levels = ['debug', 'log', 'info', 'notice', 'warning', 'exception'];
        let idx = 0;
        let level = 'exception';
        if (raw[0] && levels.includes(raw[0].toLowerCase())) { level = raw[0].toLowerCase(); idx = 1; }
        let format: string | null = null;
        const args: string[] = [];
        if (raw[idx]?.startsWith(`'`)) {
            format = raw[idx];
            idx++;
            // remaining: , expr , expr ... (stop at USING)
            while (idx < raw.length && raw[idx] !== ',') {
                if (raw[idx].toLowerCase() === 'using') { idx = raw.length; break; }
                idx++;
            }
            while (idx < raw.length && raw[idx] === ',') {
                idx++;
                const parts: string[] = [];
                let depth = 0;
                while (idx < raw.length) {
                    const t = raw[idx];
                    if (depth === 0 && (t === ',' || t.toLowerCase() === 'using')) { break; }
                    if (t === '(') { depth++; }
                    if (t === ')') { depth--; }
                    parts.push(raw[idx]);
                    idx++;
                }
                args.push(joinTokens(parts));
                if (raw[idx]?.toLowerCase() === 'using') { break; }
            }
        }
        return { k: 'raise', level, format, args };
    }

    private readUntil(stops: string[], consumeStop = false): string {
        return joinTokens(this.readRaw(stops, consumeStop));
    }

    private readRaw(stops: string[], consumeStop = false): string[] {
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
        return parts;
    }

    /** capture a whole statement's tokens up to (and consuming) the terminating ';' */
    private captureStmt(): string[] {
        return this.readRaw([';'], true);
    }

    /** SELECT ... [INTO [STRICT] v1, v2] ... : split off the INTO targets */
    private parseSelect(): GStmt {
        const toks = this.captureStmt();
        let depth = 0, intoIdx = -1;
        for (let j = 0; j < toks.length; j++) {
            const tk = toks[j];
            if (tk === '(') { depth++; }
            else if (tk === ')') { depth--; }
            else if (depth === 0 && tk.toLowerCase() === 'into') { intoIdx = j; break; }
        }
        if (intoIdx < 0) {
            return { k: 'perform', sql: joinTokens(toks) };
        }
        let j = intoIdx + 1;
        if (toks[j]?.toLowerCase() === 'strict') { j++; }
        const into: string[] = [];
        while (j < toks.length) {
            into.push(toks[j]);
            j++;
            if (toks[j] === ',') { j++; } else { break; }
        }
        const rest = [...toks.slice(0, intoIdx), ...toks.slice(j)];
        return { k: 'selectinto', into, sql: joinTokens(rest) };
    }
}

// ---- runtime: a stack of call frames (supports recursion) ----
interface Frame { vars: Map<string, any>; }
const frameStack: Frame[] = [];
function frame(): Frame { return frameStack[frameStack.length - 1]; }

const DUMMY_ROW = {};

type Signal =
    | { type: 'return'; value: any }
    | { type: 'exit' }
    | { type: 'continue' }
    | null;

interface RunCtx { t: _Transaction; emit?: (row: any) => void; }

interface GCompiled {
    run(ctx: RunCtx): Signal;
}

type ExprCompiler = (src: string, castTo?: _IType) => (t: _Transaction) => any;

/** the tools a compiled statement needs: expression + embedded-SQL compilation */
interface Helpers {
    returns: _IType | nil;
    setof: boolean;
    compileExpr: ExprCompiler;
    /** an expression compiler with an extra selection in scope (e.g. a FOR-loop record) */
    mkCompiler(sel: _ISelection, rowFn: () => any): ExprCompiler;
    /** build a SELECT's selection with variables in scope (for FOR-over-query) */
    querySelection(sql: string): _ISelection;
    /** output columns for a set-returning (RETURNS TABLE) function, else null */
    outColumns: { name: string }[] | null;
    /** compile an embedded SQL statement (variables in scope); returns its QueryResult */
    prepareSql(sql: string): (ctx: RunCtx) => any;
    /** run a dynamic (runtime-built) SQL string */
    runDynamic(ctx: RunCtx, sqlText: string): any;
}

/** does this block (recursively) use RETURN NEXT / RETURN QUERY? => set-returning */
function usesSetof(stmts: GStmt[]): boolean {
    for (const s of stmts) {
        if (s.k === 'returnnext' || s.k === 'returnquery') { return true; }
        const nested: GStmt[][] = [];
        if (s.k === 'if') { s.branches.forEach(b => nested.push(b.body)); if (s.else) { nested.push(s.else); } }
        if (s.k === 'while' || s.k === 'loop' || s.k === 'forrange' || s.k === 'forquery' || s.k === 'block') { nested.push((s as any).body); }
        if (s.k === 'block') { s.handlers.forEach(h => nested.push(h.body)); }
        if (nested.some(usesSetof)) { return true; }
    }
    return false;
}

interface GProgram {
    locals: (VarDef & { defaultVal: ((t: _Transaction) => any) | null })[];
    compiled: GCompiled[];
    setof: boolean;
}

/** compiles a regular plpgsql function into a callable implementation */
function buildPlpgsqlFunction(code: string, args: VarDef[], returns: _IType | nil, schema: _ISchema) {
    const { decls, block } = new GParser(code).parse();
    const setof = usesSetof([block]);
    // compilation is deferred to first call: the function's own name (recursion) and any
    // later-defined helpers must already be registered, which they are by call time
    let program: GProgram | null = null;

    const compile = (): GProgram => {
        const locals = decls.map(d => ({
            name: d.name,
            type: schema.getType(parseTypeDef((d as any).typeSrc)) as _IType,
            default: d.default ?? null,
        }));
        // `found` is an implicit boolean variable set by embedded SQL
        const foundVar: VarDef = { name: 'found', type: Types.bool };
        const allVars = [foundVar, ...args, ...locals];
        // a placeholder IValue per variable, resolving against the current call frame
        const params: Parameter[] = allVars.map((v, index) => ({
            index,
            value: new Evaluator(v.type, v.name, `plpgsql_var_${v.name}`, [],
                () => frame().vars.get(v.name), { forceNotConstant: true }),
        }));
        const mkCompiler = (sel: _ISelection, rowFn: () => any): ExprCompiler =>
            (src, castTo) => {
                const ast = parseExpr(src);
                const val = withParameters(params, () => withSelection(sel, () => {
                    const v = buildValue(ast);
                    return castTo ? v.cast(castTo) : v;
                }));
                return (t) => val.get(rowFn(), t);
            };
        const compileExpr = mkCompiler(schema.dualTable.selection, () => DUMMY_ROW);
        // nb: a StatementExec is compiled directly (not via schema.prepare) so that our
        // variable parameters stay on top of the stack during compile - schema.prepare
        // pushes the statement's own (empty) param set, which would shadow them
        const compileStmt = (sql: string): any => {
            const parsed = parse(sql);
            const stmt = Array.isArray(parsed) ? parsed[0] : parsed;
            const se = new StatementExec(schema, stmt, null) as any;
            withParameters(params, () => se.compile());
            return se;
        };
        const runStmt = (ctx: RunCtx, se: any): any => {
            const r: StatementResult = se.executeStatement(ctx.t, []);
            ctx.t = r.state;
            frame().vars.set('found', (r.result.rows?.length ?? 0) > 0);
            return r.result;
        };
        const outColumns = ((returns as any)?.of?.columns as { name: string }[] | undefined) ?? null;
        const helpers: Helpers = {
            returns,
            setof,
            compileExpr,
            mkCompiler,
            outColumns,
            querySelection: (sql) => {
                const parsed = parse(sql);
                const stmt = (Array.isArray(parsed) ? parsed[0] : parsed) as SelectStatement;
                return withParameters(params, () => withSelection(schema.dualTable.selection, () => buildSelect(stmt)));
            },
            prepareSql: (sql) => {
                const se = compileStmt(sql);
                return (ctx) => runStmt(ctx, se);
            },
            runDynamic: (ctx, sqlText) => runStmt(ctx, compileStmt(sqlText)),
        };
        return {
            locals: locals.map(l => ({ ...l, defaultVal: l.default != null ? compileExpr(l.default, l.type) : null })),
            compiled: compileBody([block], helpers),
            setof,
        };
    };

    return (...inArgs: any[]) => {
        const ctx: RunCtx = { t: executionCtx().transaction };
        if (!program) { program = compile(); }
        const vars = new Map<string, any>();
        vars.set('found', false);
        args.forEach((a, i) => vars.set(a.name, inArgs[i] ?? null));
        const out: any[] = [];
        if (program.setof) {
            ctx.emit = (row) => {
                // downstream (explicit-column selection) needs an internal row id
                if (row && typeof row === 'object') { ensureId(row, 'plpgsql_out'); }
                out.push(row);
            };
        }
        frameStack.push({ vars });
        try {
            // initialise locals (defaults evaluated in declaration order)
            for (const l of program.locals) {
                vars.set(l.name, l.defaultVal ? l.defaultVal(ctx.t) : null);
            }
            const sig = runGBody(program.compiled, ctx);
            if (program.setof) { return out; }
            return sig?.type === 'return' ? sig.value : null;
        } finally {
            frameStack.pop();
        }
    };
}

function parseTypeDef(typeSrc: string): any {
    const e = parseExpr(`null::${typeSrc}`);
    if (e.type !== 'cast') {
        throw new QueryError(`plpgsql: invalid variable type "${typeSrc}"`);
    }
    return e.to;
}

function compileBody(stmts: GStmt[], h: Helpers): GCompiled[] {
    const compileExpr = h.compileExpr;
    return stmts.map<GCompiled>(s => {
        switch (s.k) {
            case 'assign': {
                const val = compileExpr(s.expr);
                return { run(ctx) { frame().vars.set(s.name, val(ctx.t)); return null; } };
            }
            case 'return': {
                if (s.expr == null) { return { run: () => ({ type: 'return', value: null }) }; }
                const val = compileExpr(s.expr, h.returns ?? undefined);
                return { run: (ctx) => ({ type: 'return', value: val(ctx.t) }) };
            }
            case 'if': {
                const branches = s.branches.map(b => ({ cond: compileExpr(b.cond, Types.bool), body: compileBody(b.body, h) }));
                const elseBody = s.else ? compileBody(s.else, h) : null;
                return {
                    run(ctx) {
                        for (const b of branches) {
                            if (b.cond(ctx.t) === true) { return runGBody(b.body, ctx); }
                        }
                        return elseBody ? runGBody(elseBody, ctx) : null;
                    },
                };
            }
            case 'while': {
                const cond = compileExpr(s.cond, Types.bool);
                const body = compileBody(s.body, h);
                return {
                    run(ctx) {
                        while (cond(ctx.t) === true) {
                            const sig = runGBody(body, ctx);
                            if (sig?.type === 'return') { return sig; }
                            if (sig?.type === 'exit') { break; }
                        }
                        return null;
                    },
                };
            }
            case 'loop': {
                const body = compileBody(s.body, h);
                return {
                    run(ctx) {
                        while (true) {
                            const sig = runGBody(body, ctx);
                            if (sig?.type === 'return') { return sig; }
                            if (sig?.type === 'exit') { break; }
                        }
                        return null;
                    },
                };
            }
            case 'forrange': {
                const from = compileExpr(s.from, Types.integer);
                const to = compileExpr(s.to, Types.integer);
                const by = s.by ? compileExpr(s.by, Types.integer) : null;
                const body = compileBody(s.body, h);
                const varName = s.varName;
                const reverse = s.reverse;
                return {
                    run(ctx) {
                        // FOR i IN [REVERSE] a..b : iterate from a to b (down when REVERSE)
                        const a = from(ctx.t), b = to(ctx.t);
                        const step = Math.abs(by ? by(ctx.t) : 1) * (reverse ? -1 : 1);
                        for (let i = a; reverse ? i >= b : i <= b; i += step) {
                            frame().vars.set(varName, i);
                            const sig = runGBody(body, ctx);
                            if (sig?.type === 'return') { return sig; }
                            if (sig?.type === 'exit') { break; }
                        }
                        return null;
                    },
                };
            }
            case 'exit': {
                const when = s.when ? compileExpr(s.when, Types.bool) : null;
                return { run: (ctx) => (!when || when(ctx.t) === true) ? { type: 'exit' } : null };
            }
            case 'continue': {
                const when = s.when ? compileExpr(s.when, Types.bool) : null;
                return { run: (ctx) => (!when || when(ctx.t) === true) ? { type: 'continue' } : null };
            }
            case 'block': {
                const body = compileBody(s.body, h);
                if (!s.handlers.length) {
                    return { run: (ctx) => runGBody(body, ctx) };
                }
                const handlers = s.handlers.map(hd => ({ conditions: hd.conditions, body: compileBody(hd.body, h) }));
                return {
                    run(ctx) {
                        // run the block in a sub-transaction: on a handled exception it is
                        // rolled back (its partial changes discarded) before the handler runs
                        const child = ctx.t.fork();
                        ctx.t = child;
                        try {
                            const sig = runGBody(body, ctx);
                            ctx.t = ctx.t.commit();
                            return sig;
                        } catch (e) {
                            if (!(e instanceof QueryError)) { ctx.t = child.rollback(); throw e; }
                            const hd = handlers.find(x => x.conditions.some(c => matchCondition(c, e)));
                            ctx.t = child.rollback();
                            if (!hd) { throw e; }
                            return runGBody(hd.body, ctx);
                        }
                    },
                };
            }
            case 'forquery': {
                // FOR rec IN SELECT ... LOOP : `rec.col` resolves against the query's
                // (aliased) selection; the current row is fed to the body's expressions
                const sel = h.querySelection(s.sql).setAlias(s.varName);
                const holder: { row: any } = { row: DUMMY_ROW };
                const body = compileBody(s.body, { ...h, compileExpr: h.mkCompiler(sel, () => holder.row) });
                return {
                    run(ctx) {
                        for (const row of sel.enumerate(ctx.t)) {
                            holder.row = row;
                            const sig = runGBody(body, ctx);
                            if (sig?.type === 'return') { return sig; }
                            if (sig?.type === 'exit') { break; }
                        }
                        return null;
                    },
                };
            }
            case 'returnnext': {
                const val = compileExpr(s.expr);
                const cols = h.outColumns;
                return {
                    run(ctx) {
                        const v = val(ctx.t);
                        // a single-column TABLE wants row objects, not bare scalars
                        const row = (cols && cols.length === 1 && (v === null || typeof v !== 'object'))
                            ? { [cols[0].name]: v }
                            : v;
                        ctx.emit?.(row);
                        return null;
                    },
                };
            }
            case 'returnquery': {
                const run = h.prepareSql(s.sql);
                const cols = h.outColumns;
                return {
                    run(ctx) {
                        const res = run(ctx);
                        for (const row of res.rows ?? []) {
                            // map the query's columns positionally onto the output columns
                            if (cols) {
                                const keys = Object.keys(row);
                                const mapped: any = {};
                                cols.forEach((c, i) => mapped[c.name] = row[keys[i]] ?? null);
                                ctx.emit?.(mapped);
                            } else {
                                ctx.emit?.(row);
                            }
                        }
                        return null;
                    },
                };
            }
            case 'raise': {
                if (s.level !== 'exception') {
                    return { run: () => null }; // notice/warning/info/log/debug: not surfaced
                }
                const argFns = s.args.map(a => compileExpr(a));
                const fmt = s.format != null ? unquote(s.format) : null;
                return {
                    run(ctx) {
                        const msg = fmt != null
                            ? formatRaise(fmt, argFns.map(f => f(ctx.t)))
                            : 'raised exception';
                        throw new QueryError(msg, 'P0001');
                    },
                };
            }
            case 'selectinto': {
                const run = h.prepareSql(s.sql);
                const into = s.into;
                return {
                    run(ctx) {
                        const res = run(ctx);
                        const row = res.rows?.[0];
                        const keys = row ? Object.keys(row) : [];
                        into.forEach((v, i) => frame().vars.set(v, row ? row[keys[i]] ?? null : null));
                        return null;
                    },
                };
            }
            case 'perform':
            case 'sql': {
                const run = h.prepareSql(s.sql);
                return { run(ctx) { run(ctx); return null; } };
            }
            case 'execute': {
                const exprFn = compileExpr(s.exprSrc, Types.text());
                return {
                    run(ctx) {
                        const sqlText = exprFn(ctx.t);
                        if (sqlText != null) { h.runDynamic(ctx, String(sqlText)); }
                        return null;
                    },
                };
            }
            case 'null':
                return { run: () => null };
        }
    });
}

/** substitute `%` placeholders in a RAISE format string (%% -> literal %) */
function formatRaise(fmt: string, args: any[]): string {
    let i = 0;
    return fmt.replace(/%%|%/g, m => m === '%%' ? '%' : String(args[i++] ?? ''));
}

// common condition-name -> SQLSTATE, for EXCEPTION WHEN matching
const CONDITION_CODES: Record<string, string> = {
    unique_violation: '23505',
    not_null_violation: '23502',
    foreign_key_violation: '23503',
    check_violation: '23514',
    division_by_zero: '22012',
    no_data_found: 'P0002',
    raise_exception: 'P0001',
    string_data_right_truncation: '22001',
    invalid_text_representation: '22P02',
};

function matchCondition(cond: string, e: QueryError): boolean {
    if (cond === 'others') { return true; }
    const code = (e as any).code as string | undefined;
    if (!code) { return false; }
    // match by exact SQLSTATE, or by mapped condition name
    return cond.toUpperCase() === code || CONDITION_CODES[cond] === code;
}

function runGBody(body: GCompiled[], ctx: RunCtx): Signal {
    for (const st of body) {
        const sig = st.run(ctx);
        if (sig) { return sig; }
    }
    return null;
}

/** A compiled trigger function: given a trigger context, runs the body and returns the
 * resulting row (or null). */
export type TriggerRunner = (ctx: TriggerContext, t: _Transaction) => any;

export function registerPlpgsqlLanguage(db: _IDb) {
    db.registerLanguage('plpgsql', ({ code, args, returns, schema }) => {
        // a `RETURNS trigger` function (record pseudo-type) uses the trigger interpreter;
        // everything else is a regular callable function
        const isTrigger = !!returns && (returns as _IType).primary === 'record';
        if (!isTrigger) {
            return buildPlpgsqlFunction(
                code,
                (args ?? []).map(a => ({ name: a.name!, type: a.type as _IType })),
                returns as _IType,
                schema as _ISchema);
        }
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

/** Compile a trigger WHEN condition (with NEW/OLD in scope) against a table. */
export function compileTriggerWhen(table: _ITable, when: Expr): (ctx: TriggerContext, t: _Transaction) => any {
    return withSelection(table.selection, () => {
        const c = new TriggerCompiler(table);
        return c.compileAst(when);
    });
}
