import { DataType, getId, nil, QueryError, _IType, _ISelection, _Transaction, setId } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { RecordCol } from './datatypes';
import { Evaluator } from '../evaluator';

export class RecordType extends TypeBase<any> {


    public static matches(type: _IType): type is RecordType {
        return type.primary === DataType.record;
    }

    constructor(readonly columns: readonly RecordCol[]) {
        super();
    }

    get primary(): DataType {
        return DataType.record;
    }

    doEquals(a: any, b: any): boolean {
        return getId(a) === getId(b);
    }

    public static from(selection: _ISelection): RecordType {
        const recordCols = selection.columns
            .filter(c => !!c.id)
            .map<RecordCol>(x => ({ name: x.id!, type: x.type }));
        return new RecordType(recordCols);
    }

    /** Build a function that will transform a record of this type to a record of the target type  */
    transformItemFrom(source: _ISelection): ((raw: any, t: _Transaction, execId: string) => any) | null {
        if (source.columns.length !== this.columns.length) {
            return null;
        }
        const setters: ((oldItem: any, newItem: any, t: _Transaction) => any)[] = [];
        for (let i = 0; i < this.columns.length; i++) {
            const to = this.columns[i];
            const from = source.columns[i];
            if (!from.type.canConvertImplicit(to.type)) {
                return null;
            }
            const casted = from.cast(to.type);
            setters.push((old, neu, t) => neu[to.name] = casted.get(old, t));
        }

        return (raw: any, t, execId) => {
            const ret = {};
            // alter the items id, so each execution will have a different id
            setId(ret, execId + getId(raw));
            for (const s of setters) {
                s(raw, ret, t);
            }
            return ret;
        };
    }

    doCanCast(to: _IType): boolean | nil {
        // lets say that any type can cast to a record with no columns
        // this is a hack ... see row_to_json() UT
        return to instanceof RecordType && !to.columns.length;
    }

    doCast(value: Evaluator<any>, to: _IType): Evaluator<any> | nil {
        return value;
    }
}
