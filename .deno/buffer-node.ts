export type TBuffer = Buffer;


export function bufToString(buf: TBuffer): string {
    return buf?.toString('utf-8');
}

export function bufCompare(a: TBuffer, b: TBuffer) {
    return Buffer.compare(a, b);
}

export function bufFromString(str: string) {
    return Buffer.from(str);
}

export function isBuf(v: any): v is TBuffer {
    return Buffer.isBuffer(v);
}

export function bufClone(buf: TBuffer): TBuffer {
    const bufcopy = Buffer.alloc(buf.length);
    buf.copy(bufcopy);
    return bufcopy;
}