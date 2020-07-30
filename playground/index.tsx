import React from 'react';
import { render } from 'react-dom';
import MonacoEditor from 'react-monaco-editor';
import { hot } from 'react-hot-loader/root';
import dedent from 'dedent'
import { newDb, QueryResult } from '../src';
import type monaco from 'monaco-editor';
import type { editor } from 'monaco-editor';
import { StatementLocation, Statement } from '../src/parser/syntax/ast';
import { ErrorDisplay } from './error';
import { ValueDisplay } from './value';
import { DataGrid } from './grid';
import Popup from 'reactjs-popup';
// import ReactDataGrid from 'react-data-grid';
// import 'react-data-grid/dist/react-data-grid.css';

const columns = [
    { key: "id", name: "ID" },
    { key: "title", name: "Title" },
    { key: "complete", name: "Complete" }
];

const rows = [
    { id: 0, title: "Task 1", complete: 20 },
    { id: 1, title: "Task 2", complete: 40 },
    { id: 2, title: "Task 3", complete: 60 }
];
interface State {
    globalError?: Error;
    popup?: React.ReactElement;
}
const App = hot(class extends React.Component<{}, State> {
    private zones = new Map<string, React.ReactElement>();
    private editor: editor.ICodeEditor;
    private monaco: any;
    private oldDecorations: string[];
    private timeout: number;
    private lastValidResultLen: number;
    private lastEditPos: number;
    private code: string;
    constructor(props) {
        super(props);
        this.state = {}
        this.code = localStorage.getItem('code') || dedent`-- create tables
        CREATE TABLE "user" ("id" SERIAL NOT NULL, "name" text NOT NULL, CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY ("id"));
        CREATE TABLE IF NOT EXISTS "user" ("id" SERIAL NOT NULL, "name" text NOT NULL, CONSTRAINT "PK_cace4a159ff9f2512dd42373760" PRIMARY KEY ("id"));
        CREATE TABLE "photo" ("id" SERIAL NOT NULL, "url" text NOT NULL, "userId" integer, CONSTRAINT "PK_723fa50bf70dcfd06fb5a44d4ff" PRIMARY KEY ("id"));
        ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87" FOREIGN KEY ("userId") REFERENCES "user"("id") ON DELETE NO ACTION ON UPDATE NO ACTION;
        ALTER TABLE "user" ADD IF NOT EXISTS "name" text not null;
        ALTER TABLE "user" ADD data jsonb;

        -- insert data
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-me-1.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-me-2.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "user"("name", "data") VALUES ('me', '{"tags":["nice"]}') RETURNING "id";
        UPDATE "photo" SET "userId" = 1 WHERE "id" = 1;
        UPDATE "photo" SET "userId" = 1 WHERE "id" = 2;
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-you-1.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "photo"("url", "userId") VALUES ('photo-of-you-2.jpg', DEFAULT) RETURNING "id";
        INSERT INTO "user"("name") VALUES ('you') RETURNING "id";
        UPDATE "photo" SET "userId" = 2 WHERE "id" = 3;



        UPDATE "photo" SET "userId" = 2 WHERE "id" = 4;

        -- ============== query data ===============

        -- Joins supported, with a best effort to use indinces.
        SELECT "user"."id" AS "user_id", "user"."name" AS "user_name",  "user"."name" AS "user_name2",  "user"."name" AS "user_name3", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
            FROM "user" "user"
            LEFT JOIN "photo" "photo" ON "photo"."userId"="user"."id"
            WHERE "user"."name" = 'me';

        -- JSON queries are supported, although not all operators, and dont expect GIN indices anytime soon :)
        SELECT "user"."id" AS "user_id", "user"."name" AS "user_name",  "user"."name" AS "user_name2",  "user"."name" AS "user_name3", "photo"."id" AS "photo_id", "photo"."url" AS "photo_url", "photo"."userId" AS "photo_userId"
            FROM "user" "user"
            LEFT JOIN "photo" "photo" ON "photo"."userId"="user"."id"
            WHERE "user"."data" @> '{"tags":["nice"]}';

        -- aggregations
        select count(*) as usr from "user";
        select count(*) as cnt, userId from "photo" group by userId;`;
    }
    editorDidMount(editor: editor.ICodeEditor, monaco) {
        this.editor = editor;
        this.monaco = monaco;
        editor.focus();
        editor.onMouseDown((e) => {
            const zid = e.target.detail?.viewZoneId;
            const elt = this.zones.get(zid);
            if (elt) {
                this.setState({
                    ...this.state,
                    popup: elt,
                });
            }
        });
        this.execute(this.code);
    }

    onChange(newValue, e) {
        this.code = newValue;
        clearTimeout(this.timeout);
        this.timeout = setTimeout(() => this.execute(newValue), 300);
    }

    execute(sql: string) {
        localStorage.setItem('code', sql);
        const ret: (QueryResult | Error)[] = [];
        try {
            for (const result of newDb().public.queries(sql)) {
                ret.push(result);
                console.log(result);
            }
        } catch (e) {
            const [_, l, c] = /^Syntax\s+error\s+at\s+line\s+([0-9]+)\s+col\s+([0-9]+):/.exec(e.message) ?? [];
            if (l && c) {
                const offset = this.editor.getModel().getOffsetAt({
                    column: parseInt(c, 10),
                    lineNumber: parseInt(l, 10),
                })
                e['location'] = {
                    start: offset
                };
            }
            ret.push(e);
        }
        this.setResult(sql, ret);
    }

    private setResult(sql: string, results: (QueryResult | Error)[]) {
        this.editor.changeViewZones(changeAccessor => {
            const model = this.editor.getModel();
            let noErase = false;
            if (sql.length > 100 && results.length === 1 && results[0] instanceof Error) {
                const pos = this.editor.getPosition();
                const cursor = model.getOffsetAt(pos);
                if (Math.abs(cursor - sql.length) > 5) {
                    noErase = true;
                }
                if (cursor > this.lastValidResultLen && Math.abs(cursor - this.lastValidResultLen) < 100) {
                    noErase = true;
                }
            }
            if (!noErase) {
                for (const z of this.zones.keys()) {
                    changeAccessor.removeZone(z);
                }
                this.lastValidResultLen = sql.length;
                this.zones = new Map();
            }

            const markers: editor.IMarkerData[] = [];
            const decorations: editor.IModelDeltaDecoration[] = [];
            const computePos = (loc: StatementLocation) => {
                let s = (loc.start ?? 0);
                while (/[\s;\r\n]/.test(sql[s] ?? 'x')) {
                    s++;
                }
                let e = (loc.end ?? sql.length) - 1;
                while (s < e && /[\s;\r\n]/.test(sql[e] ?? 'x')) {
                    e--;
                }
                return {
                    start: model.getPositionAt(s),
                    end: model.getPositionAt(e),
                }
            }
            const addMarker = (loc: StatementLocation, message: string, severity: monaco.MarkerSeverity = this.monaco.MarkerSeverity.Error) => {
                const { start, end } = computePos(loc);
                markers.push({
                    severity,
                    message: message,
                    startLineNumber: start.lineNumber,
                    startColumn: start.column,
                    endLineNumber: end.lineNumber,
                    endColumn: end.column,
                })
            };
            const addDecoration = (loc: StatementLocation, classname: string, message?: string) => {
                const { start, end } = computePos(loc);

                const range: monaco.Range = new this.monaco.Range(start.lineNumber
                    , start.column
                    , end.lineNumber
                    , end.column);
                decorations.push({
                    range,
                    options: {
                        hoverMessage: message && {
                            value: message,
                        },
                        glyphMarginClassName: 'icon ' + classname,
                    },
                })
            }
            this.setState({
                ...this.state,
                globalError: null,
            });
            // create results
            for (const r of results) {
                if (r instanceof Error) {
                    const loc = r['location'] as StatementLocation;
                    if (!loc) {
                        // this.setGlobalError(e);
                        this.setState({
                            ...this.state,
                            globalError: r,
                        });
                    } else {
                        addMarker(loc, r.message);
                        addDecoration(loc, 'error');
                    }
                } else {
                    const { start, end } = computePos(r.location);
                    const addZone = (size: number, react: React.ReactElement, popup?: React.ReactElement) => {
                        if (noErase) {
                            return;
                        }
                        var domNode = document.createElement('div');
                        domNode.classList.add('resultZone')
                        render(react, domNode);
                        this.zones.set(changeAccessor.addZone({
                            afterLineNumber: end.lineNumber,
                            heightInPx: size,
                            domNode: domNode,
                            // onComputedHeight: h => domNode.set
                        }), popup);
                    }
                    if (r.ignored) {
                        addDecoration(r.location, 'ignored');
                        addMarker(r.location, 'Statement ignored', this.monaco.MarkerSeverity.Info)
                    } else if (r.rows.length) {
                        if (r.rows.length === 1 && Object.keys(r.rows[0]).length === 1) {
                            const [[k, v]] = Object.entries(r.rows[0]);
                            addZone(30, <div className="singleResult">
                                <span className="prop">→ {k}:</span> &nbsp;
                                <ValueDisplay value={v} singleLine={true} />
                            </div>, <div className="popupContent">
                                    <div className="popupContainer">
                                        <DataGrid data={r.rows} />
                                    </div>
                                </div>)
                        } else {
                            const data = r.rows;
                            const smallData = data.slice(0, 8);
                            addZone((smallData.length + 1) * 30
                                , <div className="results">
                                    <DataGrid data={smallData} inline={true} />
                                    <div className="singleResult">Click to open in modal</div>
                                </div>
                                , <div className="popupContent">
                                    <div className="popupContainer">
                                        <DataGrid data={data} />
                                    </div>
                                </div>);

                        }
                        addDecoration(r.location, 'okay');
                    } else if (r.rowCount) {
                        addZone(30, <div className="affectedView">→&nbsp; {r.rowCount} rows affected</div>);
                        addDecoration(r.location, 'okay');
                    } else {
                        addDecoration(r.location, 'okay');
                    }
                }
            }

            this.monaco.editor.setModelMarkers(model, "playground", markers);
            if (!noErase) {
                this.oldDecorations = this.editor.deltaDecorations(this.oldDecorations ?? [], decorations);
            }
        });
    }

    closeModal = () => {
        this.setState({
            ...this.state,
            popup: null,
        });
    }
    render() {
        const options = {
            selectOnLineNumbers: true,
            glyphMargin: true,
        };
        return (
            <div>
                <div className="header">
                    Postgres Playground powered by&nbsp;
                    <a href="https://github.com/oguimbal/pg-mem">pg-mem</a>
                </div>
                <MonacoEditor
                    width="100%"
                    height="95vh"
                    language="pgsql"
                    theme="vs-dark"
                    value={this.code}
                    options={options}
                    onChange={this.onChange.bind(this)}
                    editorDidMount={this.editorDidMount.bind(this)}
                />
                {this.state.globalError && <ErrorDisplay error={this.state.globalError} />}

                {
                    !this.state.popup ? null
                        : <Popup open={true}
                            closeOnDocumentClick
                            onClose={this.closeModal} position="right center">
                            {this.state.popup}
                        </Popup>
                }

            </div>
        );
    }
});

render(
    <App />,
    document.getElementById('root')
);
// render(<ReactDataGrid
//     columns={columns}
//     rows={rows}
// />, document.getElementById('root'));