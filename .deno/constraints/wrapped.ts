import { _IConstraint, _Transaction } from '../interfaces-private.ts';

export class ConstraintWrapper implements _IConstraint {
    constructor(private refs: Map<string, _IConstraint>, private inner: _IConstraint) {
        if (inner.name) {
            refs.set(inner.name, this);
        }
    }
    get name() {
        return this.inner.name;
    }
    uninstall(t: _Transaction): void {
        this.inner.uninstall(t);
        if (this.name) {
            this.refs.delete(this.name);
        }
    }
}
