import { Statement, toSql } from 'pgsql-ast-parser';
import { _IDb, _ITable } from './interfaces-private';
import { DbSnapshot } from './interfaces';
import { isBuf } from './misc/buffer-node';

const SYSTEM_SCHEMAS = new Set(['pg_catalog', 'information_schema']);

/** true for schema-defining statements (create/alter/drop/comment/grant/revoke) */
export function isSchemaStatement(st: Statement): boolean {
    const t = st.type;
    return t.startsWith('create ')
        || t.startsWith('alter ')
        || t.startsWith('drop ')
        || t === 'comment'
        || t === 'grant'
        || t === 'revoke';
}

// ---- value encoding (keep the snapshot pure-JSON and round-trippable) --------

function encode(v: any): any {
    if (v === null || v === undefined) { return v ?? null; }
    if (v instanceof Date) { return { $date: v.toISOString() }; }
    if (isBuf(v)) { return { $bytea: Buffer.from(v).toString('base64') }; }
    if (Array.isArray(v)) { return v.map(encode); }
    if (typeof v === 'object') {
        const ret: any = {};
        for (const [k, val] of Object.entries(v)) { ret[k] = encode(val); }
        return ret;
    }
    return v;
}

function decode(v: any): any {
    if (v === null || v === undefined) { return v; }
    if (Array.isArray(v)) { return v.map(decode); }
    if (typeof v === 'object') {
        if (typeof v.$date === 'string' && Object.keys(v).length === 1) { return new Date(v.$date); }
        if (typeof v.$bytea === 'string' && Object.keys(v).length === 1) { return Buffer.from(v.$bytea, 'base64'); }
        const ret: any = {};
        for (const [k, val] of Object.entries(v)) { ret[k] = decode(val); }
        return ret;
    }
    return v;
}

function quoteId(id: string): string {
    return '"' + id.replace(/"/g, '""') + '"';
}

// -----------------------------------------------------------------------------

export function serializeDb(db: _IDb): DbSnapshot {
    const ddl = db.ddl.map(st => toSql.statement(st));

    const data: DbSnapshot['data'] = [];
    for (const schema of db.listSchemas()) {
        if (SYSTEM_SCHEMAS.has(schema.name)) { continue; }
        for (const table of schema.listTables()) {
            if (table.hidden) { continue; }
            const rows = db.public.many(`select * from ${quoteId(schema.name)}.${quoteId(table.name)}`);
            data.push({ schema: schema.name, table: table.name, rows: rows.map(encode) });
        }
    }
    return { pgMemPersistence: 1, ddl, data };
}

export function deserializeDb(db: _IDb, snapshot: DbSnapshot): void {
    if (!snapshot || snapshot.pgMemPersistence !== 1) {
        throw new Error('Invalid pg-mem snapshot');
    }
    // 1. recreate the schema by replaying its DDL
    for (const sql of snapshot.ddl) {
        db.public.none(sql);
    }
    // 2. bulk-load table data (bypasses triggers; preserves stored values)
    for (const { schema, table, rows } of snapshot.data) {
        const tbl = db.getSchema(schema).getTable(table, true) as _ITable | null;
        if (!tbl) {
            throw new Error(`Cannot load data: table "${schema}.${table}" is missing from the restored schema`);
        }
        const decoded = rows.map(decode);
        for (const row of decoded) {
            tbl.insert(row);
        }
        // advance serial/identity counters past the loaded values
        tbl.restoreSerials?.(db.data, decoded);
    }
}
