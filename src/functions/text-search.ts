// Minimal PostgreSQL full-text search: tsvector / tsquery.
//
// The 'simple' configuration is reproduced exactly (lowercase, positions, sorted
// lexemes). The default 'english' configuration additionally applies stop-word removal
// and Porter stemming; the stemmer matches Postgres's snowball-english on common words
// (edge cases may differ - documented).

// snowball english stop words (the set Postgres ships for the english config)
const ENGLISH_STOPWORDS = new Set<string>([
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours',
    'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers',
    'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
    'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are',
    'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does',
    'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until',
    'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into',
    'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down',
    'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here',
    'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more',
    'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
    'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now',
]);

// ---- classic Porter stemmer ------------------------------------------------

const isVowel = (w: string, i: number): boolean => {
    const c = w[i];
    if (c === 'a' || c === 'e' || c === 'i' || c === 'o' || c === 'u') { return true; }
    if (c === 'y') { return i === 0 ? false : !isVowel(w, i - 1); }
    return false;
};

// measure m: number of vowel-consonant sequences
function measure(w: string): number {
    let n = 0;
    let prevVowel = false;
    let seenVowel = false;
    for (let i = 0; i < w.length; i++) {
        const v = isVowel(w, i);
        if (!v && seenVowel && prevVowel) { n++; }
        if (v) { seenVowel = true; }
        prevVowel = v;
    }
    return n;
}

const containsVowel = (w: string): boolean => {
    for (let i = 0; i < w.length; i++) { if (isVowel(w, i)) { return true; } }
    return false;
};

const endsDoubleConsonant = (w: string): boolean => {
    if (w.length < 2) { return false; }
    const a = w[w.length - 1], b = w[w.length - 2];
    return a === b && !isVowel(w, w.length - 1);
};

// cvc: word ends consonant-vowel-consonant where the final consonant is not w, x, y
const cvc = (w: string): boolean => {
    const n = w.length;
    if (n < 3) { return false; }
    if (isVowel(w, n - 1) || !isVowel(w, n - 2) || isVowel(w, n - 3)) { return false; }
    const c = w[n - 1];
    return c !== 'w' && c !== 'x' && c !== 'y';
};

function stem(word: string): string {
    let w = word;
    if (w.length <= 2) { return w; }

    // step 1a
    if (w.endsWith('sses')) { w = w.slice(0, -2); }
    else if (w.endsWith('ies')) { w = w.slice(0, -2); }
    else if (w.endsWith('ss')) { /* keep */ }
    else if (w.endsWith('s')) { w = w.slice(0, -1); }

    // step 1b
    let step1bDone = false;
    if (w.endsWith('eed')) {
        if (measure(w.slice(0, -3)) > 0) { w = w.slice(0, -1); }
    } else if (w.endsWith('ed') && containsVowel(w.slice(0, -2))) {
        w = w.slice(0, -2);
        step1bDone = true;
    } else if (w.endsWith('ing') && containsVowel(w.slice(0, -3))) {
        w = w.slice(0, -3);
        step1bDone = true;
    }
    if (step1bDone) {
        if (w.endsWith('at') || w.endsWith('bl') || w.endsWith('iz')) {
            w += 'e';
        } else if (endsDoubleConsonant(w)) {
            const c = w[w.length - 1];
            if (c !== 'l' && c !== 's' && c !== 'z') { w = w.slice(0, -1); }
        } else if (measure(w) === 1 && cvc(w)) {
            w += 'e';
        }
    }

    // step 1c
    if (w.endsWith('y') && containsVowel(w.slice(0, -1))) {
        w = w.slice(0, -1) + 'i';
    }

    // step 2
    const step2: [string, string][] = [
        ['ational', 'ate'], ['tional', 'tion'], ['enci', 'ence'], ['anci', 'ance'],
        ['izer', 'ize'], ['bli', 'ble'], ['alli', 'al'], ['entli', 'ent'], ['eli', 'e'],
        ['ousli', 'ous'], ['ization', 'ize'], ['ation', 'ate'], ['ator', 'ate'],
        ['alism', 'al'], ['iveness', 'ive'], ['fulness', 'ful'], ['ousness', 'ous'],
        ['aliti', 'al'], ['iviti', 'ive'], ['biliti', 'ble'], ['logi', 'log'],
    ];
    for (const [suf, rep] of step2) {
        if (w.endsWith(suf)) {
            if (measure(w.slice(0, -suf.length)) > 0) { w = w.slice(0, -suf.length) + rep; }
            break;
        }
    }

    // step 3
    const step3: [string, string][] = [
        ['icate', 'ic'], ['ative', ''], ['alize', 'al'], ['iciti', 'ic'], ['ical', 'ic'],
        ['ful', ''], ['ness', ''],
    ];
    for (const [suf, rep] of step3) {
        if (w.endsWith(suf)) {
            if (measure(w.slice(0, -suf.length)) > 0) { w = w.slice(0, -suf.length) + rep; }
            break;
        }
    }

    // step 4
    const step4 = [
        'al', 'ance', 'ence', 'er', 'ic', 'able', 'ible', 'ant', 'ement', 'ment', 'ent',
        'ou', 'ism', 'ate', 'iti', 'ous', 'ive', 'ize',
    ];
    for (const suf of step4) {
        if (w.endsWith(suf)) {
            const stem2 = w.slice(0, -suf.length);
            if (suf === 'ion') { continue; }
            if (measure(stem2) > 1) { w = stem2; }
            break;
        }
    }
    if (w.endsWith('ion')) {
        const stem2 = w.slice(0, -3);
        const last = stem2[stem2.length - 1];
        if (measure(stem2) > 1 && (last === 's' || last === 't')) { w = stem2; }
    }

    // step 5a
    if (w.endsWith('e')) {
        const stem2 = w.slice(0, -1);
        const m = measure(stem2);
        if (m > 1 || (m === 1 && !cvc(stem2))) { w = stem2; }
    }
    // step 5b
    if (measure(w) > 1 && endsDoubleConsonant(w) && w.endsWith('l')) {
        w = w.slice(0, -1);
    }

    return w;
}

// ---- tokenization & configs -------------------------------------------------

export type TsConfig = 'simple' | 'english';

export function normalizeConfig(cfg: string | nil): TsConfig {
    const c = (cfg ?? 'english').toLowerCase();
    return c === 'simple' ? 'simple' : 'english';
}

type Nil = null | undefined;
type nil = Nil;

/** split into lowercased word tokens, in order */
function tokenize(text: string): string[] {
    const m = text.toLowerCase().match(/[\p{L}\p{N}_]+/gu);
    return m ?? [];
}

/** apply the config's lexeme processing to a single token; returns null to drop it */
function lexeme(cfg: TsConfig, token: string): string | null {
    if (cfg === 'simple') { return token; }
    if (ENGLISH_STOPWORDS.has(token)) { return null; }
    return stem(token);
}

interface Lexemes {
    // lexeme -> sorted list of 1-based positions
    map: Map<string, number[]>;
}

export function toTsvector(cfg: TsConfig, text: string | nil): string {
    if (text == null) { return null as any; }
    const tokens = tokenize(text);
    const map = new Map<string, number[]>();
    let pos = 0;
    for (const tok of tokens) {
        pos++;
        const lex = lexeme(cfg, tok);
        if (lex === null) { continue; }
        const arr = map.get(lex);
        if (arr) { arr.push(pos); } else { map.set(lex, [pos]); }
    }
    return renderTsvector({ map });
}

function renderTsvector(v: Lexemes): string {
    const keys = [...v.map.keys()].sort();
    return keys.map(k => `'${k.replace(/'/g, "''")}':${v.map.get(k)!.join(',')}`).join(' ');
}

/** parse a canonical tsvector text back into the set of its lexemes */
function tsvectorLexemes(vec: string): Set<string> {
    const set = new Set<string>();
    const re = /'((?:[^']|'')*)'(?::[\d,ABCD]+)?/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(vec))) {
        set.add(m[1].replace(/''/g, "'"));
    }
    return set;
}

/** normalize a tsvector literal (from `'...'::tsvector`) to canonical form */
export function parseTsvectorLiteral(text: string | nil): string {
    if (text == null) { return null as any; }
    const map = new Map<string, number[]>();
    // tokens: 'quoted lexeme' or bare word, each optionally followed by :pos,pos
    const re = /(?:'((?:[^']|'')*)'|([\p{L}\p{N}_]+))(?::([\d,]+))?/gu;
    let m: RegExpExecArray | null;
    while ((m = re.exec(text))) {
        const lex = (m[1] !== undefined ? m[1].replace(/''/g, "'") : m[2]);
        if (lex === undefined) { continue; }
        const positions = m[3] ? m[3].split(',').map(x => parseInt(x, 10)) : [];
        const arr = map.get(lex);
        if (arr) { arr.push(...positions); } else { map.set(lex, positions); }
    }
    const keys = [...map.keys()].sort();
    return keys.map(k => {
        const p = map.get(k)!.sort((a, b) => a - b);
        return p.length ? `'${k.replace(/'/g, "''")}':${p.join(',')}` : `'${k.replace(/'/g, "''")}'`;
    }).join(' ');
}

/** normalize a tsquery literal (from `'...'::tsquery`) - no stemming */
export function parseTsqueryLiteral(text: string | nil): string {
    if (text == null) { return null as any; }
    const raw = text.match(/&|\||!|\(|\)|'(?:[^']|'')*'|[\p{L}\p{N}_]+/gu) ?? [];
    const out: string[] = [];
    for (const tk of raw) {
        if (tk === '&' || tk === '|' || tk === '!' || tk === '(' || tk === ')') {
            out.push(tk === '&' ? ' & ' : tk === '|' ? ' | ' : tk);
            continue;
        }
        let word = tk;
        if (word.startsWith("'")) { word = word.slice(1, -1).replace(/''/g, "'"); }
        out.push(`'${word.replace(/'/g, "''")}'`);
    }
    return out.join('').replace(/\s+/g, ' ').trim();
}

// ---- tsquery ----------------------------------------------------------------

// tsquery is stored canonically like: 'a' & 'b' | !'c'
export function plainToTsquery(cfg: TsConfig, text: string | nil): string {
    if (text == null) { return null as any; }
    const lexes: string[] = [];
    for (const tok of tokenize(text)) {
        const lex = lexeme(cfg, tok);
        if (lex !== null) { lexes.push(`'${lex.replace(/'/g, "''")}'`); }
    }
    return lexes.join(' & ');
}

// to_tsquery: parse operators & | ! ( ) and lexemes (each lexeme processed by config)
export function toTsquery(cfg: TsConfig, text: string | nil): string {
    if (text == null) { return null as any; }
    // tokenize into query tokens: operators, parens, and quoted/unquoted words
    const raw = text.match(/&|\||!|\(|\)|'(?:[^']|'')*'|[\p{L}\p{N}_]+/gu) ?? [];
    const out: string[] = [];
    for (const tk of raw) {
        if (tk === '&' || tk === '|' || tk === '!' || tk === '(' || tk === ')') {
            out.push(tk === '&' ? ' & ' : tk === '|' ? ' | ' : tk);
            continue;
        }
        let word = tk;
        if (word.startsWith("'")) { word = word.slice(1, -1).replace(/''/g, "'"); }
        const lex = lexeme(cfg, word.toLowerCase());
        if (lex === null) { continue; }
        out.push(`'${lex.replace(/'/g, "''")}'`);
    }
    return out.join('').replace(/\s+/g, ' ').trim();
}

// ---- @@ match ----------------------------------------------------------------

// evaluate a canonical tsquery against the lexeme set of a tsvector
export function tsMatch(vec: string | nil, query: string | nil): boolean | null {
    if (vec == null || query == null) { return null; }
    const lexemes = tsvectorLexemes(vec);
    const tokens = query.match(/&|\||!|\(|\)|'(?:[^']|'')*'/g) ?? [];
    let i = 0;
    const peek = () => tokens[i];
    const next = () => tokens[i++];

    // recursive-descent: or -> and -> not -> atom
    function parseOr(): boolean {
        let v = parseAnd();
        while (peek() === '|') { next(); v = parseAnd() || v; }
        return v;
    }
    function parseAnd(): boolean {
        let v = parseNot();
        while (peek() === '&') { next(); v = parseNot() && v; }
        return v;
    }
    function parseNot(): boolean {
        if (peek() === '!') { next(); return !parseNot(); }
        return parseAtom();
    }
    function parseAtom(): boolean {
        const t = next();
        if (t === '(') {
            const v = parseOr();
            if (peek() === ')') { next(); }
            return v;
        }
        const lex = t.slice(1, -1).replace(/''/g, "'");
        return lexemes.has(lex);
    }
    if (!tokens.length) { return false; }
    return parseOr();
}

// ---- ts_rank ----------------------------------------------------------------

// A simplified rank: proportion of query lexemes present in the document, scaled.
// (Postgres's ts_rank is more elaborate; this is offline-only.)
export function tsRank(vec: string | nil, query: string | nil): number | null {
    if (vec == null || query == null) { return null; }
    const lexemes = tsvectorLexemes(vec);
    const qlex = [...(query.matchAll(/'((?:[^']|'')*)'/g))].map(m => m[1].replace(/''/g, "'"));
    if (!qlex.length) { return 0; }
    const hits = qlex.filter(l => lexemes.has(l)).length;
    return hits === 0 ? 0 : 0.0607927 * hits; // roughly PG's single-hit weight scale
}
