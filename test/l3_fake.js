'use strict';
// A fake L3, so every path in the L3 layer can be tested without a network,
// a container, or a Valkey. Latency and failures are injectable because the
// interesting behaviour lives in what happens WHILE a call is in flight.
const delay = (ms) => new Promise(r => setTimeout(r, ms));

function makeFake(opts = {}) {
    const store = new Map();            // key -> { value, expiresAt }
    const calls = [];                   // [method, key]
    const fail = new Map();             // method -> Error to throw
    const latency = new Map();          // method -> ms
    const wait = async (m) => { const ms = latency.get(m) || 0; if (ms) await delay(ms); };
    const check = (m) => { const e = fail.get(m); if (e) throw e; };

    const adapter = {
        async get(key, o) {
            calls.push(['get', key, o && o.willCache]); await wait('get'); check('get');
            const rec = store.get(key);
            if (rec === undefined) return undefined;
            if (rec.expiresAt && rec.expiresAt <= Date.now()) { store.delete(key); return undefined; }
            return rec.expiresAt ? { value: rec.value, ttlMs: rec.expiresAt - Date.now() } : { value: rec.value };
        },
        async set(key, value, o) {
            calls.push(['set', key, o && o.willCache]); await wait('set'); check('set');
            store.set(key, { value, expiresAt: o && o.ttlMs ? Date.now() + o.ttlMs : 0 });
        },
        async delete(key) { calls.push(['delete', key]); await wait('delete'); check('delete'); store.delete(key); },
        async clear() { calls.push(['clear', null]); await wait('clear'); check('clear'); store.clear(); },
        async has(key) { calls.push(['has', key]); await wait('has'); check('has'); return store.has(key); },
        async close() { calls.push(['close', null]); },
    };
    if (opts.noOptional) { delete adapter.has; delete adapter.close; }
    return { adapter, calls, store, fail, latency };
}
module.exports = { makeFake, delay };
