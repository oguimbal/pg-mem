import { _IDb, _ISchema, _ITable, _Transaction, IValue, _ISelection, QueryError, NotSupported, getId, setId, _IType, Parameter, nil } from '../interfaces-private';
import { Expr, parse } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { withSelection, withParameters } from '../parser/context';
import { JoinSelection } from '../transforms/join';
import { Types } from '../datatypes';
import { Evaluator } from '../evaluator';
import { executionCtx } from '../utils';

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
    | { k: 'block'; body: GStmt[] }
    | { k: 'raise' }
    | { k: 'null' };

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

    parse(): { decls: VarDef[]; body: GStmt[] } {
        const decls: VarDef[] = [];
        if (this.peek() === 'declare') {
            this.i++;
            while (this.i < this.toks.length && this.peek() !== 'begin') {
                decls.push(this.parseDecl());
            }
        }
        const body = this.parseBlockBody();
        return { decls, body };
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

    /** BEGIN <stmts> END */
    private parseBlockBody(): GStmt[] {
        this.eat('begin');
        const body = this.parseStmtsUntil(['end']);
        this.eat('end');
        if (this.peek() === ';') { this.i++; }
        return body;
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
                this.readUntil([';'], true);
                return { k: 'raise' };
            case 'perform':
                throw new NotSupported('PERFORM in plpgsql (coming in a later slice)');
            case 'null':
                this.i++; if (this.peek() === ';') { this.i++; }
                return { k: 'null' };
            case 'begin': {
                const body = this.parseBlockBody();
                return { k: 'block', body };
            }
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

interface GCompiled {
    run(t: _Transaction): Signal;
}

interface GProgram {
    locals: (VarDef & { defaultVal: ((t: _Transaction) => any) | null })[];
    compiled: GCompiled[];
}

/** compiles a regular plpgsql function into a callable implementation */
function buildPlpgsqlFunction(code: string, args: VarDef[], returns: _IType | nil, schema: _ISchema) {
    const { decls, body } = new GParser(code).parse();
    // compilation is deferred to first call: the function's own name (recursion) and any
    // later-defined helpers must already be registered, which they are by call time
    let program: GProgram | null = null;

    const compile = (): GProgram => {
        const locals = decls.map(d => ({
            name: d.name,
            type: schema.getType(parseTypeDef((d as any).typeSrc)) as _IType,
            default: d.default ?? null,
        }));
        const allVars = [...args, ...locals];
        // a placeholder IValue per variable, resolving against the current call frame
        const params: Parameter[] = allVars.map((v, index) => ({
            index,
            value: new Evaluator(v.type, v.name, `plpgsql_var_${v.name}`, [],
                () => frame().vars.get(v.name), { forceNotConstant: true }),
        }));
        const compileExpr = (src: string, castTo?: _IType): (t: _Transaction) => any => {
            const ast = parseExpr(src);
            // build (and cast) within the parameter + selection context
            const val = withParameters(params, () => withSelection(schema.dualTable.selection, () => {
                const v = buildValue(ast);
                return castTo ? v.cast(castTo) : v;
            }));
            return (t) => val.get(DUMMY_ROW, t);
        };
        return {
            locals: locals.map(l => ({ ...l, defaultVal: l.default != null ? compileExpr(l.default, l.type) : null })),
            compiled: compileBody(body, compileExpr, returns),
        };
    };

    return (...inArgs: any[]) => {
        const t = executionCtx().transaction;
        if (!program) { program = compile(); }
        const vars = new Map<string, any>();
        args.forEach((a, i) => vars.set(a.name, inArgs[i] ?? null));
        frameStack.push({ vars });
        try {
            // initialise locals (defaults evaluated in declaration order)
            for (const l of program.locals) {
                vars.set(l.name, l.defaultVal ? l.defaultVal(t) : null);
            }
            const sig = runGBody(program.compiled, t);
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

function compileBody(stmts: GStmt[], compileExpr: (src: string, castTo?: _IType) => (t: _Transaction) => any, returns: _IType | nil): GCompiled[] {
    return stmts.map<GCompiled>(s => {
        switch (s.k) {
            case 'assign': {
                const val = compileExpr(s.expr);
                return { run(t) { frame().vars.set(s.name, val(t)); return null; } };
            }
            case 'return': {
                if (s.expr == null) { return { run: () => ({ type: 'return', value: null }) }; }
                const val = compileExpr(s.expr, returns ?? undefined);
                return { run: (t) => ({ type: 'return', value: val(t) }) };
            }
            case 'if': {
                const branches = s.branches.map(b => ({ cond: compileExpr(b.cond, Types.bool), body: compileBody(b.body, compileExpr, returns) }));
                const elseBody = s.else ? compileBody(s.else, compileExpr, returns) : null;
                return {
                    run(t) {
                        for (const b of branches) {
                            if (b.cond(t) === true) { return runGBody(b.body, t); }
                        }
                        return elseBody ? runGBody(elseBody, t) : null;
                    },
                };
            }
            case 'while': {
                const cond = compileExpr(s.cond, Types.bool);
                const body = compileBody(s.body, compileExpr, returns);
                return {
                    run(t) {
                        while (cond(t) === true) {
                            const sig = runGBody(body, t);
                            if (sig?.type === 'return') { return sig; }
                            if (sig?.type === 'exit') { break; }
                        }
                        return null;
                    },
                };
            }
            case 'loop': {
                const body = compileBody(s.body, compileExpr, returns);
                return {
                    run(t) {
                        while (true) {
                            const sig = runGBody(body, t);
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
                const body = compileBody(s.body, compileExpr, returns);
                const varName = s.varName;
                const reverse = s.reverse;
                return {
                    run(t) {
                        // FOR i IN [REVERSE] a..b : iterate from a to b (down when REVERSE)
                        const a = from(t), b = to(t);
                        const step = Math.abs(by ? by(t) : 1) * (reverse ? -1 : 1);
                        for (let i = a; reverse ? i >= b : i <= b; i += step) {
                            frame().vars.set(varName, i);
                            const sig = runGBody(body, t);
                            if (sig?.type === 'return') { return sig; }
                            if (sig?.type === 'exit') { break; }
                        }
                        return null;
                    },
                };
            }
            case 'exit': {
                const when = s.when ? compileExpr(s.when, Types.bool) : null;
                return { run: (t) => (!when || when(t) === true) ? { type: 'exit' } : null };
            }
            case 'continue': {
                const when = s.when ? compileExpr(s.when, Types.bool) : null;
                return { run: (t) => (!when || when(t) === true) ? { type: 'continue' } : null };
            }
            case 'block': {
                const body = compileBody(s.body, compileExpr, returns);
                return { run: (t) => runGBody(body, t) };
            }
            case 'raise':
            case 'null':
                return { run: () => null };
        }
    });
}

function runGBody(body: GCompiled[], t: _Transaction): Signal {
    for (const st of body) {
        const sig = st.run(t);
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
