// Zero-dependency drop-in for `json-stable-stringify` (default options): deterministic
// JSON with object keys sorted lexicographically. Matches its output — `JSON.stringify`
// formatting (no whitespace), `toJSON()` honored, `undefined`/function object values
// dropped, `undefined`/function array elements and non-finite numbers rendered as `null`.
export default function stableStringify(node: any): string | undefined {
    if (node === undefined) {
        return undefined;
    }
    if (node !== null && typeof node === 'object' && typeof node.toJSON === 'function') {
        node = node.toJSON();
    }
    if (node === null) {
        return 'null';
    }
    const t = typeof node;
    if (t === 'number') {
        return isFinite(node) ? String(node) : 'null';
    }
    if (t === 'boolean') {
        return String(node);
    }
    if (t === 'string') {
        return JSON.stringify(node);
    }
    if (t !== 'object') {
        return undefined; // function / symbol
    }
    if (Array.isArray(node)) {
        let out = '[';
        for (let i = 0; i < node.length; i++) {
            out += (i ? ',' : '') + (stableStringify(node[i]) ?? 'null');
        }
        return out + ']';
    }
    let out = '';
    for (const k of Object.keys(node).sort()) {
        const v = stableStringify(node[k]);
        if (v === undefined) {
            continue;
        }
        out += (out ? ',' : '') + JSON.stringify(k) + ':' + v;
    }
    return '{' + out + '}';
}
