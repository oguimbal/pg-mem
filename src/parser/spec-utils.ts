import { createParser } from './parser';
import { expect } from 'chai';

export function checkTree(value: string | string[], expected: any) {
    if (typeof value === 'string') {
        value = [value];
    }
    for (const sql of value) {
        it('parses ' + sql, () => {
            const got = /^[a-zA-Z\s]+^s+\:\s+/.exec(sql);
            let toTest = got
                ? sql.substr(got.length)
                : sql;
            const parser = createParser();
            parser.feed(toTest);
            expect(parser.finish())
                .to.deep.equal([expected]);
        });
    }
}