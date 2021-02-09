export const JSON_NIL = Symbol('null');
export const IS_PARTIAL_INDEXING = Symbol('partial_indexing');


export function cleanResults(results: any[]): any {
    // ugly hack to turn jsonb nulls & partial indexed results into actual nulls
    // This will bite me someday ... but please dont judge me, I too try to have a life outside here 🤔
    // The sane thing to do would be to refactor things & introduce a DBNULL value in pgmem
    //   since the need of such DBNULL value could arise somehow on another type
    //   see:
    //   - `can select jsonb null` test in nulls.spec.ts
    //   - `executes array multiple index incomplete indexing` test in operators.queries.spec.ts


    for (const obj of results) {
        for (const [k, v] of Object.entries(obj)) {
            if (v === JSON_NIL) {
                obj[k] = null;
            }
            if (Array.isArray(v) && (v as any)[IS_PARTIAL_INDEXING]) {
                obj[k] = null;
            }
        }
    }
    return results;
}