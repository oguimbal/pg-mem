const rimraf = require('rimraf');
const fs = require('fs');
const path = require('path');

const outDir = path.join(__dirname, '.deno');
const excluded = [
    path.join(__dirname, 'src', 'tests'),
    path.join(__dirname, 'src', 'parser', 'syntax', 'spec-utils.ts'),
]
function traverse(dir, relative, perform) {
    const rdir = path.join(outDir, relative);
    if (!(fs.existsSync(rdir))) {
        fs.mkdirSync(rdir)
    }
    for (const iname of fs.readdirSync(dir)) {
        const ipath = path.join(dir, iname);
        if (excluded.includes(ipath)) {
            continue;
        }
        const rpath = path.join(relative, iname);
        const st = fs.statSync(ipath);
        if (st.isDirectory()) {
            traverse(ipath, rpath, perform);
            continue;
        }

        if (/\.spec\.ts$/.test(iname)) {
            // ignore tests
            continue;
        }

        perform(iname, ipath, rpath);
    }
}

if (process.argv.includes('--copy')) {
    // ============== COPY
    rimraf.sync(outDir);
    if (!(fs.existsSync(outDir))) {
        fs.mkdirSync(outDir)
    }

    const settingsJson = path.join(outDir, '.vscode', 'settings.json');
    if (!fs.existsSync(path.dirname(settingsJson))) {
        fs.mkdirSync(path.dirname(settingsJson));
    }
    if (!(fs.existsSync(settingsJson))) {
        fs.writeFileSync(settingsJson, `{
            "deno.enable": true
        }`)
    }


    traverse(path.join(__dirname, 'src'), '', (iname, ipath, rpath) => {
        const ext = path.extname(iname);
        switch (ext) {
            case '.ts':
                fs.copyFileSync(ipath, path.join(outDir, rpath));
                console.log('Copied ' + rpath);
                break;
        }
    });

    fs.writeFileSync(path.join(outDir, 'mod.ts'), `export * from './index';`);
    fs.copyFileSync(path.join(__dirname, 'readme.md'), path.join(outDir, 'readme.md'));

} else if (process.argv.includes('--process')) {
    // ============= TRANSPILE


    const package = JSON.parse(fs.readFileSync(path.join(__dirname, 'package.json')));
    const bindings = {
        'moo': 'https://deno.land/x/moo@0.5.1-deno.2/mod.ts',
        'nearley': 'https://deno.land/x/nearley@2.19.7-deno/mod.ts',
        'lru-cache': 'https://deno.land/x/lru_cache@6.0.0-deno.4/mod.ts',
        'pgsql-ast-parser': 'https://deno.land/x/pgsql_ast_parser@' + package.dependencies['pgsql-ast-parser'].substr(1) + '/mod.ts',
        // 'lru-cache': {
        //     what: x => `{${x}}`,
        //     where: 'https://deno.land/x/lru_cache@6.0.0-deno.3/mod.ts',
        // },
        'object-hash': 'https://deno.land/x/object_hash@2.0.3.1/mod.ts',
        'immutable': 'https://deno.land/x/immutable@4.0.0-rc.12-deno.1/mod.ts',
        'moment': 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts',
        'functional-red-black-tree': 'https://deno.land/x/functional_red_black_tree@1.0.1-deno/mod.ts',
    }
    function handleTs(ipath, rpath) {
        const content = fs.readFileSync(ipath, 'utf-8');
        const newContent = content.replace(/^(import|export)\s+([^\n]+)\s+from\s+['"]([^'"]+)['"];?$/mg, (_, op, what, where) => {
            if (/^\./.test(where)) {
                const asDir = path.join(path.dirname(ipath), where, 'index.ts');
                if (fs.existsSync(asDir)) {
                    where = where + '/index';
                }
                return `${op} ${what} from '${where}.ts';`
            }
            let bound = bindings[where];
            if (!bound) {
                throw new Error('No Deno binding for dependency ' + where + ' in ' + rpath);
            }
            bound = typeof bound === 'string'
                ? { what: x => x, where: bound }
                : bound
            return `${op} ${bound.what(what)} from '${bound.where}';`;
        });
        fs.writeFileSync(ipath, newContent);
        console.log('Transpiled ' + rpath);
    }

    traverse(outDir, '', (iname, ipath, rpath) => {
        const ext = path.extname(iname);
        switch (ext) {
            case '.ts':
                handleTs(ipath, rpath);
                break;
        }
    });
} else {
    throw new Error('Unkown transpile program arg');
}
