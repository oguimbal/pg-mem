import { isBuf } from '../misc/buffer-deno.ts';
import { deepCloneSimple, hasExecutionCtx, isTopLevelExecutionContext } from '../utils.ts';

export const JSON_NIL = Symbol('null');
export const IS_PARTIAL_INDEXING = Symbol('partial_indexing');
export const SELECT_ALL = Symbol('select *');


export function cleanResults<T>(results: T): T {

    // ugly hack to turn jsonb nulls & partial indexed results into actual nulls
    // This will bite me someday ... but please dont judge me, I too try to have a life outside here 🤔
    // The sane thing to do would be to refactor things & introduce a DBNULL value in pgmem
    //   since the need of such DBNULL value could arise somehow on another type
    //   see:
    //   - `can select jsonb null` test in nulls.spec.ts
    //   - `executes array multiple index incomplete indexing` test in operators.queries.spec.ts

    function cleanObj(obj: any) {
        if (!obj || typeof obj !== 'object' || obj instanceof Date || isBuf(obj)) {
            return obj;
        }
        if (Array.isArray(obj)) {
            if ((obj as any)[IS_PARTIAL_INDEXING]) {
                return null;
            } else {
                const newArr = Array(obj.length);
                for (let i = 0; i < obj.length; i++) {
                    if (obj[i] === JSON_NIL) {
                        newArr[i] = null;
                    } else {
                        newArr[i] = cleanObj(obj[i]);
                    }
                }
                return newArr;
            }
        }
        const ret: any = {};
        for (const [k, v] of Object.entries(obj)) {
            if (v === JSON_NIL) {
                ret[k] = null;
            } else {
                ret[k] = cleanObj(v);
            }
        }
        return ret;
    }

    if (!Array.isArray(results)) {
        return cleanObj(results);
    }

    const ret = Array(results.length);
    for (let i = 0; i < results.length; i++) {
        const sel = results[i][SELECT_ALL];
        if (sel) {
            ret[i] = cleanObj(sel());
        } else {
            ret[i] = cleanObj(results[i]);
        }
    }
    return ret as any;
}
