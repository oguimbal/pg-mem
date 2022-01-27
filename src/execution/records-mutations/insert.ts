import { _ITable, _Transaction, IValue, _Explainer, nil, _ISchema, asTable, _ISelection, _IIndex, QueryError, OnConflictHandler, ChangeOpts, _IStatement } from '../../interfaces-private';
import { InsertStatement } from 'pgsql-ast-parser';
import { buildValue } from '../../expression-builder';
import { Types } from '../../datatypes';
import { JoinSelection } from '../../transforms/join';
import { MutationDataSourceBase, createSetter } from './mutation-base';
import { ArrayFilter } from '../../transforms/array-filter';

export class Insert extends MutationDataSourceBase<any> {

    private valueRawSource: _ISelection;
    private insertColumns: string[];
    private valueConvertedSource: IValue<any>[];
    private opts: ChangeOpts;


    constructor(statement: _IStatement, ast: InsertStatement) {
        // get table to insert into
        const table = asTable(statement.schema.getObject(ast.into));
        const selection = table
            .selection
            .setAlias(ast.into.alias);

        // init super
        super(table, selection, ast);

        // get data to insert
        this.valueRawSource = ast.insert.type === 'values'
            ? statement.buildValues(ast.insert, true)
            : statement.buildSelect(ast.insert);

        // check not inserting too many values
        this.insertColumns = ast.columns?.map(x => x.name)
            ?? this.table.selection.columns.map(x => x.id!)
                .slice(0, this.valueRawSource.columns.length);
        if (this.valueRawSource.columns.length > this.insertColumns.length) {
            throw new QueryError(`INSERT has more expressions than target columns`);
        }

        // check insert types
        this.valueConvertedSource = this.insertColumns.map((col, i) => {
            const value = this.valueRawSource.columns[i];
            const insertInto = this.table.selection.getColumn(col);
            // It seems that the explicit conversion is only performed when inserting values.
            const canConvert = ast.insert.type === 'values'
                ? value.type.canCast(insertInto.type)
                : value.type.canConvertImplicit(insertInto.type);
            if (!canConvert) {
                throw new QueryError(`column "${col}" is of type ${insertInto.type.name} but expression is of type ${value.type.name}`);
            }
            return value.type === Types.default
                ? value  // handle "DEFAULT" values
                : value.cast(insertInto.type);
        });


        // build 'on conflict' strategy
        let ignoreConflicts: OnConflictHandler | nil = undefined;
        if (ast.onConflict) {
            // find the targeted index
            const on = ast.onConflict.on?.map(x => buildValue(this.table.selection, x));
            let onIndex: _IIndex | nil = null;
            if (on) {
                onIndex = this.table.getIndex(...on);
                if (!onIndex?.unique) {
                    throw new QueryError(`There is no unique or exclusion constraint matching the ON CONFLICT specification`);
                }
            }

            // check if 'do nothing'
            if (ast.onConflict.do === 'do nothing') {
                ignoreConflicts = { ignore: onIndex ?? 'all' };
            } else {
                if (!onIndex) {
                    throw new QueryError(`ON CONFLICT DO UPDATE requires inference specification or constraint name`);
                }
                const subject = new JoinSelection(statement
                    , this.mutatedSel
                    // fake data... we're only using this to get the multi table column resolution:
                    , new ArrayFilter(this.table.selection, []).setAlias('excluded')
                    , {
                        type: 'LEFT JOIN',
                        on: { type: 'boolean', value: false }
                    }
                    , false
                );
                const setter = createSetter(this.table, subject, ast.onConflict.do.sets,);
                const where = ast.onConflict.where && buildValue(subject, ast.onConflict.where);
                ignoreConflicts = {
                    onIndex,
                    update: (item, excluded, t) => {
                        // build setter context
                        const jitem = subject.buildItem(item, excluded);

                        // check "WHERE" clause on conflict
                        if (where) {
                            const whereClause = where.get(jitem, t);
                            if (whereClause !== true) {
                                return;
                            }
                        }

                        // execute set
                        setter(t, item, jitem);
                    },
                }
            }
        }

        this.opts = {
            onConflict: ignoreConflicts,
            overriding: ast.overriding
        };

    }

    protected performMutation(t: _Transaction): any[] {

        // enumerate & get
        const values: any[][] = [];
        for (const o of this.valueRawSource.enumerate(t)) {
            const nv = [];
            for (let i = 0; i < this.insertColumns.length; i++) {
                const _custom = this.valueConvertedSource[i].get(o, t);
                nv.push(_custom);
            }
            values.push(nv);
        }


        // insert values
        const ret: any[] = [];

        for (const val of values) {
            if (val.length !== this.insertColumns.length) {
                throw new QueryError('Insert columns / values count mismatch');
            }
            const toInsert: any = {};
            for (let i = 0; i < val.length; i++) {
                const v = val[i];
                const col = this.valueConvertedSource[i];
                if (col.type === Types.default) {
                    continue; // insert a 'DEFAULT' value
                }
                toInsert[this.insertColumns[i]] = v;
                // if ('_custom' in v) {
                //      toInsert[columns[i]] = v._custom;
                // } else {
                //     const notConv = buildValue(table.selection, v);
                //     const converted = notConv.cast(col.type);
                //     if (!converted.isConstant) {
                //         throw new QueryError('Cannot insert non constant expression');
                //     }
                //     toInsert[columns[i]] = converted.get();
                // }
            }
            ret.push(this.table.doInsert(t, toInsert, this.opts));
        }

        return ret;
    }
}
