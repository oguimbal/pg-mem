import { SelectFromStatement, SelectStatement, Expr, ExprRef, astMapper } from 'pgsql-ast-parser';

// GROUP BY ROLLUP(...) / CUBE(...) are expanded into a UNION ALL of one ordinary
// GROUP BY per grouping set, with grouping columns absent from a given set projected
// as NULL. (GROUPING SETS shares the same machinery once the parser produces it.)

function isGroupingCall(e: Expr): e is Expr & { function: { name: string }, args: Expr[] } {
    return e.type === 'call'
        && ['rollup', 'cube', 'grouping sets', 'groupingsets'].includes(e.function.name.toLowerCase());
}

export function hasGroupingSets(p: SelectFromStatement): boolean {
    return !!p.groupBy?.some(isGroupingCall);
}

/** the list of grouping sets contributed by a single GROUP BY element */
function elementSets(e: Expr): Expr[][] {
    if (isGroupingCall(e)) {
        const name = e.function.name.toLowerCase();
        const args = e.args;
        if (name === 'rollup') {
            // prefixes, longest first, including the empty set
            const sets: Expr[][] = [];
            for (let i = args.length; i >= 0; i--) {
                sets.push(args.slice(0, i));
            }
            return sets;
        }
        if (name === 'cube') {
            // all subsets, full first
            const sets: Expr[][] = [];
            const n = args.length;
            for (let mask = (1 << n) - 1; mask >= 0; mask--) {
                const s: Expr[] = [];
                for (let b = 0; b < n; b++) {
                    if (mask & (1 << b)) { s.push(args[b]); }
                }
                sets.push(s);
            }
            return sets;
        }
        // grouping sets (...): each arg is itself a set (a list, a single expr, or empty)
        return args.map(a => a.type === 'list' ? a.expressions : [a]);
    }
    // a plain grouping element is always present
    return [[e]];
}

function cartesian(perElement: Expr[][][]): Expr[][] {
    let acc: Expr[][] = [[]];
    for (const sets of perElement) {
        const next: Expr[][] = [];
        for (const chosen of acc) {
            for (const s of sets) {
                next.push([...chosen, ...s]);
            }
        }
        acc = next;
    }
    return acc;
}

function refKey(e: Expr): string | null {
    if (e.type === 'ref') {
        return (e.table ? e.table.name + '.' : '') + e.name;
    }
    return null;
}

/** replace refs to dropped grouping columns with NULL */
function nullOutRefs(drop: Set<string>) {
    return astMapper(() => ({
        ref: (r: ExprRef) => {
            const k = (r.table ? r.table.name + '.' : '') + r.name;
            return drop.has(k) ? { type: 'null' } as Expr : r;
        },
    }));
}

/**
 * If `p` groups by ROLLUP/CUBE/GROUPING SETS, return an equivalent statement (a UNION ALL
 * over the grouping sets, wrapped so ORDER BY/LIMIT apply globally); otherwise null.
 */
export function expandGroupingSets(p: SelectFromStatement): SelectStatement | null {
    if (!hasGroupingSets(p)) {
        return null;
    }
    const perElement = (p.groupBy ?? []).map(elementSets);
    const sets = cartesian(perElement);

    // every column that participates in grouping (so it can be NULLed where absent)
    const allGroupKeys = new Map<string, Expr>();
    for (const el of p.groupBy ?? []) {
        for (const s of elementSets(el)) {
            for (const e of s) {
                const k = refKey(e);
                if (k) { allGroupKeys.set(k, e); }
            }
        }
    }

    const branches: SelectFromStatement[] = sets.map(set => {
        const present = new Set<string>();
        for (const e of set) {
            const k = refKey(e);
            if (k) { present.add(k); }
        }
        const drop = new Set<string>();
        for (const k of allGroupKeys.keys()) {
            if (!present.has(k)) { drop.add(k); }
        }
        const mapper = nullOutRefs(drop);
        const columns = (p.columns ?? []).map(c => ({ ...c, expr: mapper.expr(c.expr)! }));
        const branch: SelectFromStatement = {
            ...p,
            columns,
            groupBy: set.length ? set : undefined,
            having: p.having ? mapper.expr(p.having) ?? undefined : undefined,
            orderBy: undefined,
            limit: undefined,
            distinct: undefined,
        };
        return branch;
    });

    // fold into a UNION ALL
    let union: SelectStatement = branches[0];
    for (let i = 1; i < branches.length; i++) {
        union = { type: 'union all', left: union, right: branches[i] };
    }
    if (branches.length === 1) {
        return union;
    }

    // wrap so a global ORDER BY / LIMIT sees the whole set
    return {
        type: 'select',
        columns: [{ expr: { type: 'ref', name: '*' } }],
        from: [{ type: 'statement', statement: union, alias: '__grouping_sets' }],
        ...(p.orderBy ? { orderBy: p.orderBy } : {}),
        ...(p.limit ? { limit: p.limit } : {}),
    };
}
