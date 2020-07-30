@lexer lexer
@{%
    function unwrap(e) {
        if (Array.isArray(e) && e.length === 1) {
            e = unwrap(e[0]);
        }
        if (Array.isArray(e) && !e.length) {
            return null;
        }
        return e;
    }
    const get = i => x => x[i];
    const last = x => Array.isArray(x) ? x[x.length - 1] : x;
    const trim = x => x && x.trim();
    const value = x => x && x.value;
    function flatten(e) {
        if (Array.isArray(e)) {
            const ret = [];
            for (const i of e) {
                ret.push(...flatten(i));
            }
            return ret;
        }
        if (!e) {
            return [];
        }
        return [e];
    }
    function flattenStr(e) {
        const fl = flatten(e);
        return fl.filter(x => !!x)
                    .map(x => typeof x === 'string' ? x
                            : 'value' in x ? x.value
                            : x)
                    .filter(x => typeof x === 'string')
                    .map(x => x.trim())
                    .filter(x => !!x);
    }
%}
# @preprocessor typescript


# === Basic constructs
lparen -> %lparen
rparen -> %rparen
number -> float | int
dot -> %dot {% id %}
float
    -> %int dot %int:? {% args => parseFloat(args.join('')) %}
    | dot %int {% args => parseFloat(args.join('')) %}
int -> %int {% arg => parseInt(arg, 10) %}
comma -> %comma {% id %}
star -> %star {% x => x[0].value %}
string -> %string {% x => x[0].value %}

ident -> word {% unwrap %}
word -> %word  {% x => {
    const val = x[0].value;
    return val[0] === '"' ? val.substr(1, val.length - 2) : val;
} %}

collist_paren -> lparen collist rparen {% get(1) %}
collist -> ident (comma ident {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

# === Non reserved keywords
# ... which are not in keywords.ts (thus parsed as words)
@{%
 const notReservedKw = (kw) => (x, _, rej) => {
     const val = typeof x[0] === 'string' ? x[0] : x[0].value;
     const low = val.toLowerCase();
     return low === kw ? low : rej;
 }
 const kw = notReservedKw;
 const anyKw = (...kw) => (x, _, rej) => {
     const val = typeof x[0] === 'string' ? x[0] : x[0].value;
     const low = val.toLowerCase();
     return kw.includes(low) ? low : rej;
 }
%}
kw_between -> %word {% notReservedKw('between')  %}
kw_if -> %word {% notReservedKw('if')  %}
kw_exists -> %word {% notReservedKw('exists')  %}
kw_key -> %word {% notReservedKw('key')  %}
kw_index -> %word {% notReservedKw('index')  %}
kw_nulls -> %word {% notReservedKw('nulls')  %}
kw_first -> %word {% notReservedKw('first')  %}
kw_last -> %word {% notReservedKw('last')  %}
kw_start -> %word {% notReservedKw('start')  %}
kw_commit -> %word {% notReservedKw('commit')  %}
kw_transaction -> %word {% notReservedKw('transaction')  %}
kw_rollback -> %word {% notReservedKw('rollback')  %}
kw_insert -> %word {% notReservedKw('insert')  %}
kw_values -> %word {% notReservedKw('values')  %}
kw_update -> %word {% notReservedKw('update')  %}
kw_set -> %word {% notReservedKw('set')  %}
kw_alter -> %word {% notReservedKw('alter')  %}
kw_rename -> %word {% notReservedKw('rename')  %}
kw_add -> %word {% notReservedKw('add')  %}
kw_drop -> %word {% notReservedKw('drop')  %}
kw_data -> %word {% notReservedKw('data')  %}
kw_type -> %word {% notReservedKw('type')  %}
kw_delete -> %word {% notReservedKw('delete')  %}
kw_cascade -> %word {% notReservedKw('cascade')  %}
kw_no -> %word {% notReservedKw('no')  %}
kw_action -> %word {% notReservedKw('action')  %}
kw_restrict -> %word {% notReservedKw('restrict')  %}
kw_truncate -> %word {% notReservedKw('truncate')  %}
kw_by -> %word {% notReservedKw('by')  %}
kw_row -> %word {% notReservedKw('row')  %}
kw_rows -> %word {% notReservedKw('rows')  %}
kw_next -> %word {% notReservedKw('next')  %}


# === Composite keywords
kw_ifnotexists -> kw_if %kw_not kw_exists
kw_ifexists -> kw_if kw_exists
kw_not_null -> %kw_not %kw_null
kw_primary_key -> %kw_primary kw_key


# === Datatype

# https://www.postgresql.org/docs/9.5/datatype.html
data_type -> data_type_simple (lparen int rparen {% get(1) %}):? (%kw_array | (%lbracket %rbracket):+):? {% x => {
    let asArray = x[2];
    const type = flattenStr(x[0]).join(' ').toLowerCase();
    let ret = {
        type,
        ... (typeof x[1] === 'number' && x[1] >= 0 ) ? { length: x[1] } : {},
    };
    if (asArray) {
        if (asArray[0].type === 'kw_array') {
            asArray = [['array']]
        }
        for (const _ of asArray[0]) {
            ret = {
                type: 'array',
                arrayOf: ret,
            };
        }
    }
    return ret;
} %}

data_type_simple
    -> data_type_text
    | data_type_numeric
    | data_type_date
    | word {% anyKw('json', 'jsonb', 'boolean', 'money', 'bytea', 'regtype') %}


# https://www.postgresql.org/docs/9.5/datatype-numeric.html
data_type_numeric -> word {% anyKw('smallint', 'int', 'float', 'integer', 'bigint', 'bigint', 'decimal', 'numeric', 'real', 'smallserial', 'serial', 'bigserial') %}
                    | (%word {% kw('double') %}) (%word {% kw('precision') %})

# https://www.postgresql.org/docs/9.5/datatype-character.html
data_type_text
            -> word  {% anyKw('character', 'varchar', 'char', 'text') %}
            | word {% kw('character') %}
            | (%word {% kw('character') %}) (%word {% kw('varying') %})

#https://www.postgresql.org/docs/9.5/datatype-datetime.html
data_type_date
    -> word {% kw('date') %}
    | word {% kw('interval') %}
    | word {% kw('timestamp') %}
    | (%word {% anyKw('timestamp', 'time') %}) (%kw_with | %word {% kw('without') %}) (%word {% kw('time') %}) (%word {% kw('zone') %})



# === Table ref  (ex:  [db.]mytable [as X] )

# [AS x] or just [x]
ident_aliased -> (%kw_as ident {% last %}) | ident {% unwrap %}

table_ref
    -> (ident dot {% id %}):? (ident | current_schema) {% x => ({
        table: unwrap(x[1]),
        ...x[0] ? { db: unwrap(x[0]) } : {},
    })%}

current_schema -> %kw_current_schema (lparen rparen):? {% () => 'current_schema' %}

# Select on tables MAY have an alias
table_ref_aliased -> table_ref ident_aliased:? {% x => {
    const alias = unwrap(x[1]);
    return {
        ...unwrap(x[0]),
        ...alias ? { alias } : {},
    }
} %}
