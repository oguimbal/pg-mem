
import { _ISchema } from '../interfaces-private'


import { literal } from '../misc/pg-escape'
import { MigrationData, MigrationFile, MigrationParams } from './migrate-interfaces';

declare var __non_webpack_require__: any;
declare var process: any;

export async function readMigrations(migrationPath?: string) {
  const path = __non_webpack_require__('path');
  const fs = __non_webpack_require__('fs');
  const migrationsPath = migrationPath || path.join(process.cwd(), 'migrations')
  const location = path.resolve(migrationsPath)

  // Get the list of migration files, for example:
  //   { id: 1, name: 'initial', filename: '001-initial.sql' }
  //   { id: 2, name: 'feature', filename: '002-feature.sql' }
  const migrationFiles = await new Promise<MigrationFile[]>(
    (resolve, reject) => {
      fs.readdir(location, (err: any, files: string[]) => {
        if (err) {
          return reject(err)
        }

        resolve(
          files
            .map(x => x.match(/^(\d+).(.*?)\.sql$/))
            .filter(x => x !== null)
            .map(x => ({ id: Number(x![1]), name: x![2], filename: x![0] }))
            .sort((a, b) => Math.sign(a.id - b.id))
        )
      })
    }
  )

  if (!migrationFiles.length) {
    throw new Error(`No migration files found in '${location}'.`)
  }

  // Get the list of migrations, for example:
  //   { id: 1, name: 'initial', filename: '001-initial.sql', up: ..., down: ... }
  //   { id: 2, name: 'feature', filename: '002-feature.sql', up: ..., down: ... }
  return Promise.all(
    migrationFiles.map(
      migration =>
        new Promise<MigrationData>((resolve, reject) => {
          const filename = path.join(location, migration.filename)
          fs.readFile(filename, 'utf-8', (err: any, data: string) => {
            if (err) {
              return reject(err)
            }

            const [up, down] = data.split(/^--\s+?down\b/im)

            const migrationData = migration as Partial<MigrationData>
            migrationData.up = up.replace(/^-- .*?$/gm, '').trim() // Remove comments
            migrationData.down = down ? down.trim() : '' // and trim whitespaces
            resolve(migrationData as MigrationData)
          })
        })
    )
  )
}

/**
 * Migrates database schema to the latest version
 */
export async function migrate(db: _ISchema, config: MigrationParams = {}) {
  config.force = config.force || false
  config.table = config.table || 'migrations'

  const { force, table } = config
  const migrations = config.migrations
    ? config.migrations
    : await readMigrations(config.migrationsPath)

  // Create a database table for migrations meta data if it doesn't exist
  await db.none(`CREATE TABLE IF NOT EXISTS "${table}" (
  id   INTEGER PRIMARY KEY,
  name TEXT    NOT NULL,
  up   TEXT    NOT NULL,
  down TEXT    NOT NULL
)`)

  // Get the list of already applied migrations
  let dbMigrations = await db.many(
    `SELECT id, name, up, down FROM "${table}" ORDER BY id ASC`
  )

  // Undo migrations that exist only in the database but not in files,
  // also undo the last migration if the `force` option is enabled.
  const lastMigration = migrations[migrations.length - 1]
  for (const migration of dbMigrations
    .slice()
    .sort((a, b) => Math.sign(b.id - a.id))) {
    if (
      !migrations.some(x => x.id === migration.id) ||
      (force && migration.id === lastMigration.id)
    ) {
      // await db.run('BEGIN')
      try {
        await db.none(migration.down)
        await db.none(`DELETE FROM "${table}" WHERE id = ${migration.id}`)
        // await db.run('COMMIT')
        dbMigrations = dbMigrations.filter(x => x.id !== migration.id)
      } catch (err) {
        // await db.run('ROLLBACK')
        throw err
      }
    } else {
      break
    }
  }

  // Apply pending migrations
  const lastMigrationId = dbMigrations.length
    ? dbMigrations[dbMigrations.length - 1].id
    : 0
  for (const migration of migrations) {
    if (migration.id > lastMigrationId) {
      // await db.run('BEGIN')
      try {
        await db.none(migration.up)
        await db.none(
          `INSERT INTO "${table}" (id, name, up, down) VALUES (
          ${migration.id},
          ${literal(migration.name)},
          ${literal(migration.up)},
          ${literal(migration.down)})`
        )
        // await db.run('COMMIT')
      } catch (err) {
        // await db.run('ROLLBACK')
        throw err
      }
    }
  }
}