// Arbitrary-precision decimal backed by BigInt, for postgres `numeric`/`decimal`.
// A value is a BigInt `unscaled` mantissa and a non-negative integer `scale`, so the
// real value is unscaled * 10^-scale. Kept dependency-free (no decimal.js) to preserve
// pg-mem's tiny bundle.
//
// nb: BigInt literals (10n) aren't allowed at this tsconfig target (ES2019), so we use
// the BigInt() constructor throughout.

const ZERO = BigInt(0);
const ONE = BigInt(1);
const TEN = BigInt(10);
const TWO = BigInt(2);

const POW10: bigint[] = [];
function pow10(n: number): bigint {
    if (POW10[n] === undefined) {
        POW10[n] = TEN ** BigInt(n);
    }
    return POW10[n];
}

// postgres computes division to at least this many fractional digits
const DIVISION_SCALE = 20;

export class Decimal {
    private constructor(readonly unscaled: bigint, readonly scale: number) { }

    static fromText(text: string): Decimal {
        const t = text.trim();
        const m = t.match(/^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/);
        if (!m || (!m[2] && !m[3])) {
            throw new Error(`invalid numeric literal: ${text}`);
        }
        const sign = m[1] === '-' ? -ONE : ONE;
        const intPart = m[2] || '0';
        const fracPart = m[3] || '';
        let unscaled = BigInt(intPart + fracPart) * sign;
        let scale = fracPart.length;
        const exp = m[4] ? parseInt(m[4], 10) : 0;
        if (exp > 0) {
            if (exp >= scale) {
                unscaled *= pow10(exp - scale);
                scale = 0;
            } else {
                scale -= exp;
            }
        } else if (exp < 0) {
            scale += -exp;
        }
        return new Decimal(unscaled, scale);
    }

    static fromNumber(n: number): Decimal {
        return Decimal.fromText(n.toString());
    }

    static fromBigInt(n: bigint): Decimal {
        return new Decimal(n, 0);
    }

    private static align(a: Decimal, b: Decimal): [bigint, bigint, number] {
        const scale = Math.max(a.scale, b.scale);
        return [a.unscaled * pow10(scale - a.scale), b.unscaled * pow10(scale - b.scale), scale];
    }

    add(o: Decimal): Decimal {
        const [x, y, scale] = Decimal.align(this, o);
        return new Decimal(x + y, scale);
    }
    sub(o: Decimal): Decimal {
        const [x, y, scale] = Decimal.align(this, o);
        return new Decimal(x - y, scale);
    }
    mul(o: Decimal): Decimal {
        return new Decimal(this.unscaled * o.unscaled, this.scale + o.scale);
    }
    div(o: Decimal): Decimal {
        if (o.unscaled === ZERO) {
            throw new Error('division by zero');
        }
        const resultScale = DIVISION_SCALE;
        const num = this.unscaled * pow10(resultScale + o.scale);
        const den = o.unscaled * pow10(this.scale);
        return new Decimal(roundedDiv(num, den), resultScale).normalize();
    }

    compare(o: Decimal): -1 | 0 | 1 {
        const [x, y] = Decimal.align(this, o);
        return x < y ? -1 : x > y ? 1 : 0;
    }

    /** Round to `scale` fractional digits (half away from zero, like postgres). */
    round(scale: number): Decimal {
        if (scale >= this.scale) {
            return new Decimal(this.unscaled * pow10(scale - this.scale), scale);
        }
        const drop = this.scale - scale;
        return new Decimal(roundedDiv(this.unscaled, pow10(drop)), scale);
    }

    /** Drop trailing-zero fractional digits (keeps the value, tidies the scale). */
    normalize(): Decimal {
        let unscaled = this.unscaled;
        let scale = this.scale;
        while (scale > 0 && unscaled % TEN === ZERO) {
            unscaled /= TEN;
            scale--;
        }
        return new Decimal(unscaled, scale);
    }

    toNumber(): number {
        return Number(this.toString());
    }

    toString(): string {
        const neg = this.unscaled < ZERO;
        let digits = (neg ? -this.unscaled : this.unscaled).toString();
        if (this.scale === 0) {
            return (neg ? '-' : '') + digits;
        }
        if (digits.length <= this.scale) {
            digits = '0'.repeat(this.scale - digits.length + 1) + digits;
        }
        const cut = digits.length - this.scale;
        const frac = digits.slice(cut).replace(/0+$/, '');
        const intp = digits.slice(0, cut);
        return (neg ? '-' : '') + intp + (frac ? '.' + frac : '');
    }
}

/** Integer division of BigInts, rounded half away from zero. */
function roundedDiv(num: bigint, den: bigint): bigint {
    const neg = (num < ZERO) !== (den < ZERO);
    const a = num < ZERO ? -num : num;
    const b = den < ZERO ? -den : den;
    const q = a / b;
    const r = a % b;
    const rounded = r * TWO >= b ? q + ONE : q;
    return neg ? -rounded : rounded;
}
