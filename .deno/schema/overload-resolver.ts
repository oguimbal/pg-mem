import { _IType, _ArgDefDetails, nil, DataType, IValue } from '../interfaces-private.ts';
import { Types } from '../datatypes/index.ts';
import { it } from '../utils.ts';
import { QueryError } from '../interfaces.ts';

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
            this.byName.set(value.name, ret = new OverloadNode<T>(Types.null, this.implicitCastOnly, 0));
        }
        ret.index(value, replaceIfExists);
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

    resolve(name: string, args: IValue[]) {
        return this.byName.get(name)?.resolve(args);
    }

    getExact(name: string, types: _IType[]): T | nil {
        return this.byName.get(name)?.getExact(types);
    }
}


class OverloadNode<T extends HasSig> {

    private nexts = new Map<DataType, OverloadNode<T>[]>();
    private leaf: T | nil;

    constructor(readonly type: _IType, private implicitCastOnly: boolean, private at: number) {
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

    index(value: T, replaceIfExists: boolean) {
        if (this.at >= value.args.length) {
            if (this.leaf && !replaceIfExists) {
                throw new QueryError('Function already exists: ' + value.name);
            }
            this.leaf = value;
            return;
        }
        const arg = value.args[this.at];
        const primary = arg.type.primary;
        let lst = this.nexts.get(primary);
        if (!lst) {
            this.nexts.set(primary, lst = []);
        }
        // get or add corresponding node
        let node = lst.find(x => x.type === arg.type);
        if (!node) {
            lst.push(node = new OverloadNode(arg.type, this.implicitCastOnly, this.at + 1));
        }

        // process arg list
        node.index(value, replaceIfExists);
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

    getExact(types: _IType[]): T | nil {
        if (this.at >= types.length) {
            return this.leaf;
        }
        const target = types[this.at];
        const found = this.nexts.get(target.primary)
            ?.find(x => x.type == target);
        return found?.getExact(types);
    }

    resolve(args: IValue[]): T | nil {
        if (this.at >= args.length) {
            return this.leaf;
        }

        // gets the child which type matches the current arg better
        const arg = args[this.at];
        const sigsToCheck = it(
            this.nexts // perf tweak: search by primary type
                .get(arg.type.primary)
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
            return match.resolve(args);
        }

        // handle variadic args
        if (this.leaf && this.leaf.argsVariadic && this.compatible(arg, this.leaf.argsVariadic)) {
            return this.leaf;
        }

        // not found
        return null;
    }

    private compatible(givenArg: IValue, expectedArg: _IType) {
        if (givenArg.type === expectedArg) {
            return true;
        }
        return givenArg.isConstantLiteral
            ? givenArg.type.canCast(expectedArg)
            : givenArg.type.canConvertImplicit(expectedArg) ?? givenArg.type.canCast(expectedArg);
    }
}