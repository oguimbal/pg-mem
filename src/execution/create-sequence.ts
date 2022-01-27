import { _ISchema, _Transaction, NotSupported, _ISequence } from '../interfaces-private';
import { QName, CreateSequenceStatement } from 'pgsql-ast-parser';
import { Sequence } from '../schema/sequence';
import { checkExistence } from './exec-utils';

export class ExecuteCreateSequence {
    schema: _ISchema;
    constructor(inSchema: _ISchema, private statement: CreateSequenceStatement, private acceptTempSequences: boolean) {
        const name: QName = statement.name;
        this.schema = inSchema.getThisOrSiblingFor(name);
    }

    execute(t: _Transaction) {
        const p = this.statement;
        const name: QName = p.name;
        // const ret = this.simple('CREATE', p);

        let ret: _ISequence | null = null;
        checkExistence(this.schema, name, p.ifNotExists, () => {
            if (p.temp && !this.acceptTempSequences) {
                throw new NotSupported('temp sequences');
            }
            ret = new Sequence(name.name, this.schema)
                .alter(t, p.options);
            this.schema.db.onSchemaChange();
        });
        return ret;
    }
}
