/* istanbul ignore file */
import { IMemoryDb } from '../interfaces';
import { assert, expect } from 'chai';

export function preventSeqScan(db: IMemoryDb, table?: string) {
    if (table) {
        db.getTable(table).on('seq-scan', () => {
            assert.fail('Should have used index');
        });
    } else {
        db.on('seq-scan', table => {
            assert.fail('Should have used index when requesting table ' + table);
        });
    }

    db.on('catastrophic-join-optimization', () => {
        assert.fail('Should have used index when performing join');
    });
}

export function preventCataJoin(db: IMemoryDb) {
    db.on('catastrophic-join-optimization', () => {
        assert.fail('Should have used index when performing join');
    });
}

export function watchCataJoins(db: IMemoryDb) {
    let got =0;
    db.on('catastrophic-join-optimization', () => {
        got++;
    });
    return {
        check() {
            expect(got).to.equal(0, 'Should have used index when performing join');
        }
    }
}