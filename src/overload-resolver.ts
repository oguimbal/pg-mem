import { _IType, _ArgDefDetails, nil, DataType } from './interfaces-private';
import { Types } from './datatypes';
import { it } from './utils';
import { QueryError } from './interfaces';

export interface HasSig {
    name: string;
    args: _ArgDefDetails[];
    argsVariadic?: _IType | nil;
}

export class OverloadResolver<T extends HasSig> {

    private byName = new Map<string, OverloadNode<T>>();

    constructor(private implicitCastOnly: boolean) { }

    add(value: T, replaceIfExists: boolean) {
        let ret = this.byName.get(value.name);
        if (!ret) {
            this.byName.set(value.name, ret = new OverloadNode<T>(Types.null, this.implicitCastOnly));
        }
        ret.index(value, 0, replaceIfExists);
    }

    getOverloads(name: string) {
        const ovr = this.byName.get(name);
        if (!ovr) {
            return [];
        }
        return [...ovr.all()];
    }

    remove(value: T) {
        this.byName.get(value.name)?.unindex(value);
    }

    resolve(name: string, args: _IType[]) {
        return this.byName.get(name)?.resolve(args, 0);
    }
}


class OverloadNode<T extends HasSig> {

    private nexts = new Map<DataType, OverloadNode<T>[]>();
    private leaf: T | nil;

    constructor(readonly type: _IType, private implicitCastOnly: boolean) {
    }

    *all(): IterableIterator<T> {
        if (this.leaf) {
            yield this.leaf;
        }
        for (const children of this.nexts.values()) {
            for (const child of children) {
                yield* child.all();
            }
        }
    }

    index(value: T, at: number, replaceIfExists: boolean) {
        if (at >= value.args.length) {
            if (this.leaf && !replaceIfExists) {
                throw new QueryError('Function already exists: ' + value.name);
            }
            this.leaf = value;
            return;
        }
        const arg = value.args[at];
        const primary = arg.type.primary;
        let lst = this.nexts.get(primary);
        if (!lst) {
            this.nexts.set(primary, lst = []);
        }
        // get or add corresponding node
        let node = lst.find(x => x.type === arg.type);
        if (!node) {
            lst.push(node = new OverloadNode(arg.type, this.implicitCastOnly));
        }

        // process arg list
        node.index(value, at + 1, replaceIfExists);
    }

    unindex(value: T) {
        if (this.leaf === value) {
            this.leaf = null;
            return;
        }
        for (const children of this.nexts.values()) {
            for (const child of children) {
                child.unindex(value);
            }
        }
    }

    resolve(args: _IType[], at: number): T | nil {
        if (at >= args.length) {
            return this.leaf;
        }

        // gets the child which type matches the current arg better
        const arg = args[at];
        const sigsToCheck = it(
            this.nexts // perf tweak: search by primary type
                .get(arg.primary)
            ?? it(this.nexts.values()).flatten() // else, search all registered overloads
        );

        const match = sigsToCheck.reduce<OverloadNode<T> | nil>((acc, x) => {
            // check that arg can be converted to the target type
            if (!this.compatible(arg, x.type)) {
                return acc;
            }
            // first match
            if (!acc) {
                return x;
            }
            // returns the prefered type
            return acc.type.prefer(x.type) === x.type ? x : acc;
        }, null);

        if (match) {
            return match.resolve(args, at + 1);
        }

        // handle variadic args
        if (this.leaf && this.leaf.argsVariadic && this.compatible(arg, this.leaf.argsVariadic)) {
            return this.leaf;
        }

        // not found
        return null;
    }

    private compatible(arg: _IType<any>, type: _IType<any>) {
        return this.implicitCastOnly ? arg.canConvertImplicit(type) : arg.canCast(type)
    }
}