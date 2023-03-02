import { _IConstraint, _Transaction } from '../interfaces-private.ts';

export class SubscriptionConstraint implements _IConstraint {
    constructor(readonly name: string, readonly uninstall: (t: _Transaction) => void) {
    }
}