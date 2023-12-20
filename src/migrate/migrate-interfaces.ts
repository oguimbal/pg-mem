export interface MigrationParams {
    /**
     * If true, will force the migration API to rollback and re-apply the latest migration over
     * again each time when Node.js app launches.
     */
    force?: boolean
    /**
     * Migrations table name. Default is 'migrations'
     */
    table?: string
    /**
     * Path to the migrations folder. Default is `path.join(process.cwd(), 'migrations')`
     */
    migrationsPath?: string
    /**
     * Migration data read from migrations folder. `migrationsPath` will be ignored if this is
     * provided.
     */
    migrations?: readonly MigrationData[]
}

export interface MigrationFile {
    id: number
    name: string
    filename: string
}

export interface MigrationData {
    id: number
    name: string
    up: string
    down: string
}
