import { DataType, QueryError } from '../interfaces.ts';
import { _IType } from '../interfaces-private.ts';
import { Box, Circle, Line, Path, Point, Polygon, Segment } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { Evaluator } from '../evaluator.ts';
import { TypeBase } from './datatype-base.ts';

export function pointToStr(p: Point) {
    return `(${p.x},${p.y})`;
}

export function pointEq(a: Point, b: Point) {
    return a.x === b.x && a.y === b.y;
}

export class PointType extends TypeBase<Point> {
    get primary(): DataType {
        return DataType.point;
    }
    get name() {
        return 'point';
    }
    doCanCast(t: _IType) {
        return t.primary === DataType.text;
    }

    doCast(value: Evaluator<Point>, to: _IType) {
        if (to.primary !== DataType.text) {
            throw new QueryError(`Invalid cast to: ` + to.primary);
        }
        return value
            .setConversion((p: Point) => {
                return pointToStr(p);
            }
                , pointToTxt => ({ pointToTxt }));
    }

    doEquals(a: Point, b: Point) {
        return pointEq(a, b);
    }

    doGt(a: Point, b: Point) {
        if (a.x !== b.x) {
            return a.x > b.x;
        }
        return a.y > b.y;
    }

    doLt(a: Point, b: Point) {
        if (a.x !== b.x) {
            return a.x < b.x;
        }
        return a.y < b.y;
    }
}



export class LineType extends TypeBase<Line> {

    get primary(): DataType {
        return DataType.line;
    }
    get name() {
        return 'line';
    }
    doCanCast(t: _IType) {
        return t.primary === DataType.text;
    }

    doCast(value: Evaluator<Line>, to: _IType) {
        if (to.primary !== DataType.text) {
            throw new QueryError(`Invalid cast to: ` + to.primary);
        }
        return value
            .setConversion((l: Line) => {
                return `{${l.a},${l.b},${l.c}}`;
            }
                , lineToTxt => ({ lineToTxt }));
    }

    doEquals(a: Line, b: Line) {
        return a.a === b.a && a.b === b.b && a.c === b.c;
    }
}

export class LsegType extends TypeBase<Segment> {

    get primary(): DataType {
        return DataType.lseg;
    }
    get name() {
        return 'lseg';
    }
    doCanCast(t: _IType) {
        return t.primary === DataType.text;
    }

    doCast(value: Evaluator<Segment>, to: _IType) {
        if (to.primary !== DataType.text) {
            throw new QueryError(`Invalid cast to: ` + to.primary);
        }
        return value
            .setConversion(([a, b]: Segment) => {
                return `[${pointToStr(a)},${pointToStr(b)}]`;
            }
                , SegmentToTxt => ({ SegmentToTxt }));
    }

    doEquals([as, ae]: Segment, [bs, be]: Segment) {
        return pointEq(as, bs) && pointEq(ae, be);
    }
}

export class BoxType extends TypeBase<Box> {

    get primary(): DataType {
        return DataType.box;
    }
    get name() {
        return 'box';
    }
    doCanCast(t: _IType) {
        return t.primary === DataType.text;
    }

    doCast(value: Evaluator<Box>, to: _IType) {
        if (to.primary !== DataType.text) {
            throw new QueryError(`Invalid cast to: ` + to.primary);
        }
        return value
            .setConversion(([a, b]: Box) => {
                return `${pointToStr(a)},${pointToStr(b)}`;
            }
                , BoxToTxt => ({ BoxToTxt }));
    }

    doEquals([as, ae]: Box, [bs, be]: Box) {
        return pointEq(as, bs) && pointEq(ae, be);
    }
}

export class PathType extends TypeBase<Path> {

    get primary(): DataType {
        return DataType.path;
    }
    get name() {
        return 'path';
    }
    doCanCast(t: _IType) {
        return t.primary === DataType.text;
    }

    doCast(value: Evaluator<Path>, to: _IType) {
        if (to.primary !== DataType.text) {
            throw new QueryError(`Invalid cast to: ` + to.primary);
        }
        return value
            .setConversion((p: Path) => {
                const vals = p.path.map(pointToStr).join(',');
                return p.closed
                    ? '(' + vals + ')'
                    : '[' + vals + ']';
            }
                , PathToTxt => ({ PathToTxt }));
    }

    doEquals(a: Path, b: Path) {
        // Yup, you read that right ...
        //  Try it... path equality always returns true (???)
        return true;
        // return !!a.closed === !!b.closed
        //     && a.path.length === b.path.length
        //     && a.path.every((x, i) => pointEq(x, b.path[i]));
    }
}

export class PolygonType extends TypeBase<Polygon> {

    get primary(): DataType {
        return DataType.polygon;
    }
    get name() {
        return 'polygon';
    }
    doCanCast(t: _IType) {
        return t.primary === DataType.text;
    }

    doCast(value: Evaluator<Polygon>, to: _IType) {
        if (to.primary !== DataType.text) {
            throw new QueryError(`Invalid cast to: ` + to.primary);
        }
        return value
            .setConversion((p: Polygon) => {
                const vals = p.map(pointToStr).join(',');
                return '(' + vals + ')';
            }
                , PolygonToTxt => ({ PolygonToTxt }));
    }

    doEquals(a: Polygon, b: Polygon) {
        return a.length === b.length
            && a.every((x, i) => pointEq(x, b[i]));
    }
}


export class CircleType extends TypeBase<Circle> {

    get primary(): DataType {
        return DataType.circle;
    }
    get name() {
        return 'circle';
    }
    doCanCast(t: _IType) {
        return t.primary === DataType.text;
    }

    doCast(value: Evaluator<Circle>, to: _IType) {
        if (to.primary !== DataType.text) {
            throw new QueryError(`Invalid cast to: ` + to.primary);
        }
        return value
            .setConversion((p: Circle) => {
                return `<${pointToStr(p.c)},${p.r}>`
            }
                , CircleToTxt => ({ CircleToTxt }));
    }

    doEquals(a: Circle, b: Circle) {
        return pointEq(a.c, b.c) && a.r === b.r;
    }
}
