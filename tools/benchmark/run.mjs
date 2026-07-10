// Runs the shared workload against two pg-mem builds and prints a comparison.
//   bun tools/benchmark/run.mjs <forkEntry> <prodEntry>
// Each entry is a module exporting { newDb }.

import { runWorkload } from './workload.mjs';

const [, , forkPath, prodPath] = process.argv;

async function load(path) {
    const mod = await import(path);
    return mod.newDb ?? mod.default?.newDb;
}

const forkNewDb = await load(forkPath);
const prodNewDb = await load(prodPath);

// warm up (JIT) then take the best of a few runs to reduce noise
function best(newDb, runs = 3) {
    let acc = null;
    for (let r = 0; r < runs; r++) {
        const t = runWorkload(newDb);
        if (!acc) { acc = t; continue; }
        for (const k of Object.keys(t)) acc[k] = Math.min(acc[k], t[k]);
    }
    return acc;
}

// warmup
runWorkload(forkNewDb);
runWorkload(prodNewDb);

const fork = best(forkNewDb);
const prod = best(prodNewDb);

const rows = Object.keys(prod);
const pad = (s, n) => String(s).padEnd(n);
const padl = (s, n) => String(s).padStart(n);

console.log('\n' + pad('operation', 26) + padl('upstream (ms)', 15) + padl('fork (ms)', 12) + padl('delta', 10));
console.log('-'.repeat(63));
let totalProd = 0, totalFork = 0;
for (const k of rows) {
    totalProd += prod[k];
    totalFork += fork[k];
    const delta = ((fork[k] - prod[k]) / prod[k] * 100);
    const sign = delta >= 0 ? '+' : '';
    console.log(pad(k, 26) + padl(prod[k].toFixed(1), 15) + padl(fork[k].toFixed(1), 12) + padl(sign + delta.toFixed(1) + '%', 10));
}
console.log('-'.repeat(63));
const totalDelta = ((totalFork - totalProd) / totalProd * 100);
console.log(pad('TOTAL', 26) + padl(totalProd.toFixed(1), 15) + padl(totalFork.toFixed(1), 12)
    + padl((totalDelta >= 0 ? '+' : '') + totalDelta.toFixed(1) + '%', 10));
console.log();
