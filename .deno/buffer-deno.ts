


/*
⛔⛔⛔ WARN ⛔⛔⛔

This file is built for Deno.
Dont be surprised if it yields errors in Node, it's not meant to be used there.

(transpilation replaces buffer-node.ts by buffer-node.ts when building deno version)

*/

export type TBuffer = Uint8Array;


export function bufToString(buf: TBuffer): string {
    // @ts-ignore
    const decoder = new TextDecoder()
    return decoder.decode(buf);
}

export function bufCompare(a: TBuffer, b: TBuffer) {
    if (a === b) {
        return 0;
    }
    if (a.length > b.length) {
        return 1;
    }
    for (let i = 0; i < a.length; i++) {
        const d = a[i] - b[i];
        if (d === 0) {
            continue;
        }
        return d < 0 ? -1 : 1;
    }
    return 0;
}

export function bufFromString(str: string) {
    // @ts-ignore
    const encoder = new TextEncoder()
    const buffer = encoder.encode(str);
    return buffer;
}

export function isBuf(v: any): v is TBuffer {
    return v instanceof Uint8Array;
}

export function bufClone(buf: TBuffer): TBuffer {
    return new Uint8Array(buf);
}
