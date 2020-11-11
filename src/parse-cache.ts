
import LRUCache from 'lru-cache';
import hash from 'object-hash';
import { Expr, parse, Statement } from 'pgsql-ast-parser';


const astCache: LRUCache<any, any> = new LRUCache({
    max: 1000,
});


/** Parse an AST from SQL */
export function parseSql(sql: string): Statement[];
export function parseSql(sql: string, entry: 'expr'): Expr;
export function parseSql(sql: string, entry?: string): any {
    // when 'entry' is not specified, lets cache parsings
    // => better perf on repetitive requests
    const key = !entry && hash(sql);
    if (!entry) {
        const cached = astCache.get(key);
        if (cached) {
            return cached;
        }
    }
    let ret = parse(sql, entry as any);

    // cache result
    if (!entry) {
        astCache.set(key, ret);
    }
    return ret;
}
