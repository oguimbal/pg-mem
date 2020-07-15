import { IMemoryDb } from '../interfaces';
import { assert } from 'chai';

export function preventSeqScan(db: IMemoryDb, table = 'data') {
    db.getTable(table).on('seq-scan', () => {
        assert.fail('Should have used index');
    });
}