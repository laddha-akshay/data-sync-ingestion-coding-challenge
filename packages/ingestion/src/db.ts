import { Pool } from "pg";
import { from as copyFrom } from "pg-copy-streams";

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

export async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS events (
      id TEXT PRIMARY KEY,
      event_name TEXT,
      user_id TEXT,
      timestamp TIMESTAMPTZ,
      raw JSONB
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ingestion_state (
      id INTEGER PRIMARY KEY DEFAULT 1,
      next_cursor TEXT,
      total_processed BIGINT DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    INSERT INTO ingestion_state (id)
    VALUES (1)
    ON CONFLICT (id) DO NOTHING;
  `);
}

export async function getCursor(): Promise<string | null> {
  const res = await pool.query(
    `SELECT next_cursor FROM ingestion_state WHERE id = 1`
  );
  return res.rows[0]?.next_cursor ?? null;
}

export async function getTotalProcessed(): Promise<number> {
  const res = await pool.query(
    `SELECT total_processed FROM ingestion_state WHERE id = 1`
  );
  return Number(res.rows[0]?.total_processed ?? 0);
}

export async function saveProgress(cursor: string | null, count: number) {
  await pool.query(
    `
    UPDATE ingestion_state
    SET next_cursor = $1,
        total_processed = total_processed + $2,
        updated_at = NOW()
    WHERE id = 1
  `,
    [cursor, count]
  );
}

function normalizeTimestamp(ts: unknown): string {
  if (!ts) return new Date(0).toISOString();
  try {
    const d = new Date(ts as string | number);
    if (isNaN(d.getTime())) return new Date(0).toISOString();
    return d.toISOString();
  } catch {
    return new Date(0).toISOString();
  }
}

function escapeForCopy(val: string): string {
  return val
    .replace(/\\/g, "\\\\")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\t/g, "\\t");
}

export async function bulkInsertEvents(events: any[]): Promise<number> {
  if (events.length === 0) return 0;

  const client = await pool.connect();
  let released = false;

  const release = () => {
    if (!released) {
      released = true;
      client.release();
    }
  };

  try {
    const rows: string[] = [];
    for (const e of events) {
      const id = escapeForCopy(String(e.id ?? ""));
      const eventName = escapeForCopy(String(e.event_name ?? e.type ?? e.name ?? ""));
      const userId = escapeForCopy(String(e.user_id ?? e.userId ?? ""));
      const ts = normalizeTimestamp(e.timestamp ?? e.created_at ?? e.createdAt);
      const raw = escapeForCopy(JSON.stringify(e));
      rows.push(`${id}\t${eventName}\t${userId}\t${ts}\t${raw}`);
    }

    const copyData = rows.join("\n") + "\n";

    await client.query("BEGIN");
    await client.query(`
      CREATE TEMP TABLE events_staging (
        id TEXT,
        event_name TEXT,
        user_id TEXT,
        timestamp TIMESTAMPTZ,
        raw JSONB
      ) ON COMMIT DROP
    `);

    await new Promise<void>((resolve, reject) => {
      const stream = client.query(
        copyFrom("COPY events_staging FROM STDIN WITH (FORMAT text)")
      );
      stream.on("finish", resolve);
      stream.on("error", reject);
      stream.write(copyData);
      stream.end();
    });

    await client.query(`
      INSERT INTO events (id, event_name, user_id, timestamp, raw)
      SELECT id, event_name, user_id, timestamp, raw FROM events_staging
      ON CONFLICT (id) DO NOTHING
    `);

    await client.query("COMMIT");
    release();
    return events.length;
  } catch (err) {
    try { await client.query("ROLLBACK"); } catch {}
    release();
    return fallbackInsert(events);
  }
}

async function fallbackInsert(events: any[]): Promise<number> {
  const CHUNK = 500;
  let inserted = 0;

  for (let i = 0; i < events.length; i += CHUNK) {
    const chunk = events.slice(i, i + CHUNK);
    const values: any[] = [];
    const placeholders: string[] = [];
    let p = 1;

    for (const e of chunk) {
      values.push(
        String(e.id ?? ""),
        String(e.event_name ?? e.type ?? e.name ?? ""),
        String(e.user_id ?? e.userId ?? ""),
        normalizeTimestamp(e.timestamp ?? e.created_at ?? e.createdAt),
        JSON.stringify(e)
      );
      placeholders.push(`($${p},$${p + 1},$${p + 2},$${p + 3},$${p + 4})`);
      p += 5;
    }

    await pool.query(
      `INSERT INTO events (id, event_name, user_id, timestamp, raw)
       VALUES ${placeholders.join(",")}
       ON CONFLICT (id) DO NOTHING`,
      values
    );
    inserted += chunk.length;
  }
  return inserted;
}