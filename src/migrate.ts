import { configuration } from './infra/config.js';
import { migrate } from './infra/migration.js';
const { pool, schema } = configuration();
try { await migrate(pool, schema); console.log(`Audit migration applied: ${schema}`); }
catch { console.error('Audit migration failed; inspect connection/schema locally.'); process.exitCode = 1; }
finally { await pool.end(); }
