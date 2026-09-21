'use strict';
// An adapter is user-supplied code, so it is checked once at the boundary.
// The alternative is a TypeError from inside a promise chain on the first
// cache miss, in a process that has been serving happily for an hour.
const REQUIRED = ['get', 'set', 'delete', 'clear'];
const OPTIONAL = ['has', 'subscribe', 'close'];

function assertAdapter(a) {
    if (a === null || typeof a !== 'object')
        throw new TypeError(`turbokv: the l3 adapter must be an object, got ${a === null ? 'null' : typeof a}`);
    for (const m of REQUIRED) {
        if (typeof a[m] !== 'function') {
            throw new TypeError(a[m] === undefined
                ? `turbokv: the l3 adapter is missing ${m}(); required members are ${REQUIRED.join(', ')}`
                : `turbokv: the l3 adapter's ${m} must be a function, got ${typeof a[m]}`);
        }
    }
    for (const m of OPTIONAL) {
        if (a[m] !== undefined && typeof a[m] !== 'function')
            throw new TypeError(`turbokv: the l3 adapter's ${m} must be a function when present, got ${typeof a[m]}`);
    }
    return a;
}
module.exports = { assertAdapter, REQUIRED, OPTIONAL };
