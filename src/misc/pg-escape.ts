// stolen from https://github.com/segmentio/pg-escape/blob/master/index.js

export function literal(val: any) {
    if (null == val) return 'NULL';
    if (Array.isArray(val)) {
        var vals: any[] = val.map(literal)
        return "(" + vals.join(", ") + ")"
    }
    var backslash = ~val.indexOf('\\');
    var prefix = backslash ? 'E' : '';
    val = val.replace(/'/g, "''");
    val = val.replace(/\\/g, '\\\\');
    return prefix + "'" + val + "'";
};
