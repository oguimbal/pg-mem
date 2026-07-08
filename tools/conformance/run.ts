// Conformance harness: runs the SQL corpus against pg-mem and classifies each case.
//
//   bun tools/conformance/run.ts                 offline mode: verified against @expect
//   PG_URL=... bun tools/conformance/run.ts      differential mode: verified against real postgres
//
// e.g. PG_URL=postgres://postgres:pgmem@localhost:5433/postgres
// (docker run -d --name pgmem-conformance -e POSTGRES_PASSWORD=pgmem -p 5433:5432 postgres:16-alpine)
//
// Corpus format (tools/conformance/corpus/*.sql):
//   -- @case: <name>          starts a new case (fresh database)
//   -- @expect: <json array>  expected rows of the case's last statement (offline mode only)
//   -- @error: <regex>        the case is expected to fail (offline mode only)
//
// In differential mode, @expect/@error are ignored: each case runs on the real server
// (in a throwaway schema) and its rows — or its failure — are the truth pg-mem must match.
//
// Outcomes:
//   pass             matches real postgres (or @expect/@error offline)
//   wrong-result     executed but rows differ (or errored when postgres succeeds, or vice-versa)
//   missing-function a builtin function/operator is not implemented (easiest to contribute!)
//   not-supported    engine raised NotSupported (parsed, but unimplemented)
//   parse-error      pgsql-ast-parser rejected the syntax
//   error            any other failure
//
// Emits tools/conformance/report.md and report.json.

// make node-pg parse dates/timestamps in UTC, like pg-mem stores them
process.env.TZ = 'UTC';

import { readdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';
import { newDb } from '../../src/db';
import { NotSupported } from '../../src/interfaces';

interface Case {
    category: string;
    name: string;
    sql: string;
    expected?: any[];
    expectedError?: string;
}

type Outcome = 'pass' | 'wrong-result' | 'missing-function' | 'not-supported' | 'parse-error' | 'error';

interface CaseResult extends Case {
    outcome: Outcome;
    detail?: string;
}

const corpusDir = join(import.meta.dir, 'corpus');

function parseCorpus(file: string): Case[] {
    const category = file.replace(/^\d+-/, '').replace(/\.sql$/, '');
    const text = readFileSync(join(corpusDir, file), 'utf-8');
    const cases: Case[] = [];
    let current: Case | null = null;
    for (const line of text.split('\n')) {
        const caseMatch = line.match(/^--\s*@case:\s*(.+)$/);
        const expectMatch = line.match(/^--\s*@expect:\s*(.+)$/);
        const errorMatch = line.match(/^--\s*@error:\s*(.+)$/);
        if (caseMatch) {
            if (current) cases.push(current);
            current = { category, name: caseMatch[1].trim(), sql: '' };
        } else if (expectMatch) {
            if (!current) throw new Error(`${file}: @expect before any @case`);
            current.expected = JSON.parse(expectMatch[1]);
        } else if (errorMatch) {
            if (!current) throw new Error(`${file}: @error before any @case`);
            current.expectedError = errorMatch[1].trim();
        } else if (current) {
            current.sql += line + '\n';
        }
    }
    if (current) cases.push(current);
    return cases;
}

// === result comparison ============================================================

// Normalizes a cell so pg-mem and node-pg representations compare equal:
// dates → ISO strings, and numbers may compare against numeric strings
// (node-pg returns numeric/bigint columns as strings).
function normalizeVal(v: any): any {
    if (v instanceof Date) {
        return v.toISOString();
    }
    if (Array.isArray(v)) {
        return v.map(normalizeVal);
    }
    if (v && typeof v === 'object') {
        const ret: any = {};
        for (const k of Object.keys(v)) {
            ret[k] = normalizeVal(v[k]);
        }
        return ret;
    }
    return v;
}

function valEqual(a: any, b: any): boolean {
    if (a === null || a === undefined) {
        return b === null || b === undefined;
    }
    if (typeof a === 'number' && typeof b === 'string' || typeof a === 'string' && typeof b === 'number') {
        const [num, str] = typeof a === 'number' ? [a, b as string] : [b as number, a as string];
        return str.trim() !== '' && !isNaN(+str) && +str === num;
    }
    if (Array.isArray(a) && Array.isArray(b)) {
        return a.length === b.length && a.every((x, i) => valEqual(x, b[i]));
    }
    if (typeof a === 'object' && typeof b === 'object' && a && b) {
        const ka = Object.keys(a);
        const kb = Object.keys(b);
        return ka.length === kb.length && ka.every(k => k in b && valEqual(a[k], b[k]));
    }
    return a === b;
}

function rowsEqual(a: any[], b: any[]): boolean {
    return valEqual(normalizeVal(a), normalizeVal(b));
}

// === execution ====================================================================

interface ExecResult {
    rows?: any[];
    error?: string;
}

function runOnPgMem(c: Case): ExecResult {
    try {
        const db = newDb();
        return { rows: db.public.query(c.sql).rows };
    } catch (e: any) {
        return { error: e?.message ?? String(e) };
    }
}

function classifyMemError(c: Case, msg: string): CaseResult {
    const detail = msg.split('\n')[0].slice(0, 200);
    if (/(function|operator) .* does not exist/i.test(msg)) {
        return { ...c, outcome: 'missing-function', detail };
    }
    if (/not (yet )?(supported|implemented)/i.test(msg)) {
        return { ...c, outcome: 'not-supported', detail };
    }
    if (/syntax error|failed to parse/i.test(msg)) {
        return { ...c, outcome: 'parse-error', detail };
    }
    return { ...c, outcome: 'error', detail };
}

function verdict(c: Case, mem: ExecResult, truth: { rows?: any[]; error?: boolean; label: string }): CaseResult {
    if (truth.error) {
        // the reference errors: pg-mem must error too (messages are not compared)
        return mem.error
            ? { ...c, outcome: 'pass' }
            : { ...c, outcome: 'wrong-result', detail: `should error (${truth.label}), but pg-mem succeeded` };
    }
    if (mem.error) {
        return classifyMemError(c, mem.error);
    }
    if (truth.rows && !rowsEqual(mem.rows!, truth.rows)) {
        return {
            ...c,
            outcome: 'wrong-result',
            detail: `${truth.label} says ${JSON.stringify(normalizeVal(truth.rows))}, pg-mem says ${JSON.stringify(normalizeVal(mem.rows))}`,
        };
    }
    return { ...c, outcome: 'pass' };
}

// === real-postgres reference (differential mode) ==================================

async function connectRealPg(url: string) {
    const pg = await import('pg');
    const client = new pg.default.Client({ connectionString: url });
    await client.connect();
    return client;
}

async function runOnRealPg(client: any, c: Case, schemaId: number): Promise<ExecResult> {
    const schema = `conformance_${schemaId}`;
    await client.query('rollback').catch(() => { });
    await client.query(`drop schema if exists ${schema} cascade`);
    await client.query(`create schema ${schema}`);
    await client.query(`set search_path to ${schema}; set timezone to 'UTC'`);
    try {
        const res = await client.query(c.sql);
        const last = Array.isArray(res) ? res[res.length - 1] : res;
        return { rows: last?.rows ?? [] };
    } catch (e: any) {
        return { error: e?.message ?? String(e) };
    } finally {
        await client.query('rollback').catch(() => { });
        await client.query(`drop schema if exists ${schema} cascade`).catch(() => { });
    }
}

// === main =========================================================================

const files = readdirSync(corpusDir).filter(f => f.endsWith('.sql')).sort();
const cases = files.flatMap(parseCorpus);
const pgUrl = process.env.PG_URL;
const client = pgUrl ? await connectRealPg(pgUrl) : null;

const results: CaseResult[] = [];
for (let i = 0; i < cases.length; i++) {
    const c = cases[i];
    const mem = runOnPgMem(c);
    if (client) {
        const real = await runOnRealPg(client, c, i);
        results.push(verdict(c, mem, { rows: real.rows, error: !!real.error, label: 'postgres' }));
    } else if (c.expectedError) {
        results.push(verdict(c, mem, { error: true, label: `@error ${c.expectedError}` }));
    } else {
        results.push(verdict(c, mem, { rows: c.expected, label: '@expect' }));
    }
}
await client?.end();

const mode = client ? `differential (${pgUrl!.replace(/\/\/[^@]*@/, '//***@')})` : 'offline (@expect annotations)';
const byCategory = new Map<string, CaseResult[]>();
for (const r of results) {
    if (!byCategory.has(r.category)) byCategory.set(r.category, []);
    byCategory.get(r.category)!.push(r);
}

const count = (rs: CaseResult[], o: Outcome) => rs.filter(r => r.outcome === o).length;
const passed = count(results, 'pass');
const score = ((passed / results.length) * 100).toFixed(1);

let md = `# pg-mem conformance report\n\n`;
md += `**Score: ${passed}/${results.length} (${score}%)** — verified against ${mode}\n\n`;
md += `| Category | Pass | Wrong result | Missing function | Not supported | Parse error | Error |\n`;
md += `|---|---|---|---|---|---|---|\n`;
for (const [cat, rs] of byCategory) {
    md += `| ${cat} | ${count(rs, 'pass')}/${rs.length} | ${count(rs, 'wrong-result')} | ${count(rs, 'missing-function')} | ${count(rs, 'not-supported')} | ${count(rs, 'parse-error')} | ${count(rs, 'error')} |\n`;
}
md += `\n## Failures\n\n`;
for (const [cat, rs] of byCategory) {
    const failures = rs.filter(r => r.outcome !== 'pass');
    if (!failures.length) continue;
    md += `### ${cat}\n\n`;
    for (const f of failures) {
        md += `- \`${f.name}\` — **${f.outcome}**${f.detail ? `: ${f.detail}` : ''}\n`;
    }
    md += `\n`;
}

writeFileSync(join(import.meta.dir, 'report.md'), md);
writeFileSync(join(import.meta.dir, 'report.json'), JSON.stringify({
    mode: client ? 'differential' : 'offline',
    score: +score,
    passed,
    total: results.length,
    results: results.map(({ sql, ...r }) => r),
}, null, 2));

console.log(`Conformance: ${passed}/${results.length} (${score}%) — ${mode}`);
for (const [cat, rs] of byCategory) {
    console.log(`  ${cat}: ${count(rs, 'pass')}/${rs.length}`);
}
console.log(`Report: tools/conformance/report.md`);
