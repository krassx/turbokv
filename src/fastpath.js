'use strict';
// Source scanning for the JSON fast-path check.
//
// This lives outside the cache class because it is a text utility, not part of
// the API: it was reachable as `TurboKV.callArgCounts`, which made it something
// consumers could call and therefore something we would have to keep. It is
// used by `assertFastCodec` and by the suite's lint over the repository's own
// JSON calls -- one implementation, so the lint tests the shipped scanner
// rather than a copy of it (decision 37b).
//
// An installed consumer cannot reach this file: the `exports` map has no deep
// paths, so `require('turbokv/src/fastpath')` is ERR_PACKAGE_PATH_NOT_EXPORTED.

// Counts top-level arguments of each `name(...)` call in `src`, using balanced
// scanning so nested calls and object literals do not confuse it the way a
// regex would.
function callArgCounts(src, name) {
    const out = [];
    let i = 0;
    while ((i = src.indexOf(name + '(', i)) !== -1) {
        let d = 0, args = 1, j = i + name.length, empty = true;
        for (; j < src.length; j++) {
            const c = src[j];
            if (c === '(' || c === '[' || c === '{') d++;
            else if (c === ')' || c === ']' || c === '}') { d--; if (d === 0) break; }
            else if (c === ',' && d === 1) args++;
            else if (d === 1 && !/\s/.test(c)) empty = false;
        }
        out.push({ index: i, args: empty ? 0 : args, text: src.slice(i, j + 1) });
        i = j + 1;
    }
    return out;
}

module.exports = { callArgCounts };
