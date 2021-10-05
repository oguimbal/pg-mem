import { nullIsh } from './utils.ts';

export const JSON_NIL = Symbol('null');
export const IS_PARTIAL_INDEXING = Symbol('partial_indexing');
export const SELECT_ALL = Symbol('select *');


export function cleanResults(results: any[]): any {
    // ugly hack to turn jsonb nulls & partial indexed results into actual nulls
    // This will bite me someday ... but please dont judge me, I too try to have a life outside here 🤔
    // The sane thing to do would be to refactor things & introduce a DBNULL value in pgmem
    //   since the need of such DBNULL value could arise somehow on another type
    //   see:
    //   - `can select jsonb null` test in nulls.spec.ts
    //   - `executes array multiple index incomplete indexing` test in operators.queries.spec.ts

    function cleanObj(obj: any) {
        if (!obj || typeof obj !== 'object') {
            return;
        }
        for (const [k, v] of Object.entries(obj)) {
            if (v === JSON_NIL) {
                obj[k] = null;
            } else if (Array.isArray(v)) {
                if ((v as any)[IS_PARTIAL_INDEXING]) {
                    obj[k] = null;
                } else {
                    for (let i = 0; i < v.length; i++) {
                        if (obj[i] === JSON_NIL) {
                            obj[i] = null;
                        } else {
                            cleanObj(v);
                        }
                    }
                }
            } else {
                cleanObj(v);
            }
        }
    }

    for (let i = 0; i < results.length; i++) {
        const sel = results[i][SELECT_ALL];
        if (sel) {
            results[i] = sel();
        }
        cleanObj(results[i]);
    }
    return results;
}