import { _ISchema, _Transaction, NotSupported, _ISequence, _IStatementExecutor } from '../../interfaces-private';
import { QName, CreateSequenceStatement } from 'pgsql-ast-parser';
import { Sequence } from '../../schema/sequence';
import { checkExistence, ExecHelper } from '../exec-utils';

export class ExecuteCreateSequence extends ExecHelper implements _IStatementExecutor {
    schema: _ISchema;
    constructor(inSchema: _ISchema, private p: CreateSequenceStatement, private acceptTempSequences: boolean) {
        super(p);
        const name: QName = p.name;
        this.schema = inSchema.getThisOrSiblingFor(name);
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();

        // create the sequence
        this.createSeq(t);

        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }

    createSeq(t: _Transaction) {
        const p = this.p;
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
