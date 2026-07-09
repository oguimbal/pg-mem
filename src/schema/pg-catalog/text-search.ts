import { _ISchema } from '../../interfaces-private';
import { Types } from '../../datatypes';
import { registerTextSearchTypes } from '../../datatypes/t-textsearch';
import {
    normalizeConfig, toTsvector, toTsquery, plainToTsquery, tsMatch, tsRank,
} from '../../functions/text-search';

/**
 * Registers the tsvector / tsquery types, the to_tsvector / to_tsquery /
 * plainto_tsquery builders, the `@@` match operator and ts_rank.
 *
 * The 'simple' text-search configuration is reproduced exactly; the default 'english'
 * config adds stop-words + Porter stemming (matches Postgres on common words).
 */
export function registerTextSearch(schema: _ISchema) {
    const { tsvector, tsquery } = registerTextSearchTypes(schema);

    // ---- to_tsvector ----
    schema.registerFunction({
        name: 'to_tsvector',
        args: [Types.text()],
        returns: tsvector,
        implementation: (text: string) => toTsvector('english', text),
    });
    schema.registerFunction({
        name: 'to_tsvector',
        args: [Types.text(), Types.text()],
        returns: tsvector,
        implementation: (cfg: string, text: string) => toTsvector(normalizeConfig(cfg), text),
    });

    // ---- to_tsquery ----
    schema.registerFunction({
        name: 'to_tsquery',
        args: [Types.text()],
        returns: tsquery,
        implementation: (text: string) => toTsquery('english', text),
    });
    schema.registerFunction({
        name: 'to_tsquery',
        args: [Types.text(), Types.text()],
        returns: tsquery,
        implementation: (cfg: string, text: string) => toTsquery(normalizeConfig(cfg), text),
    });

    // ---- plainto_tsquery ----
    schema.registerFunction({
        name: 'plainto_tsquery',
        args: [Types.text()],
        returns: tsquery,
        implementation: (text: string) => plainToTsquery('english', text),
    });
    schema.registerFunction({
        name: 'plainto_tsquery',
        args: [Types.text(), Types.text()],
        returns: tsquery,
        implementation: (cfg: string, text: string) => plainToTsquery(normalizeConfig(cfg), text),
    });

    // ---- @@ match operator ----
    schema.registerOperator({
        operator: '@@', left: tsvector, right: tsquery, returns: Types.bool,
        implementation: (v, q) => tsMatch(v, q),
    });
    schema.registerOperator({
        operator: '@@', left: tsquery, right: tsvector, returns: Types.bool,
        implementation: (q, v) => tsMatch(v, q),
    });

    // ---- ts_rank (simplified) ----
    schema.registerFunction({
        name: 'ts_rank',
        args: [tsvector, tsquery],
        returns: Types.float,
        implementation: (v, q) => tsRank(v, q),
    });
}
