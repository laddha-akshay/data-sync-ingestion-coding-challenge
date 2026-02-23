import {
  initDb,
  getCursor,
  getTotalProcessed,
  saveProgress,
  bulkInsertEvents,
  pool,
} from "./db.js";
import { fetchEvents } from "./api.js";

const PAGE_SIZE = Number(process.env.PAGE_SIZE ?? 1000);

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

class Stats {
  private start = Date.now();
  private lastLog = Date.now();
  private lastCount = 0;
  public total = 0;

  update(n: number) {
    this.total += n;
    const now = Date.now();
    if (now - this.lastLog >= 5000) {
      const elapsed = (now - this.start) / 1000;
      const windowRate = Math.round(
        (this.total - this.lastCount) / ((now - this.lastLog) / 1000)
      );
      const overallRate = Math.round(this.total / elapsed);
      const eta = overallRate > 0
        ? Math.round((3_000_000 - this.total) / overallRate)
        : "?";
      console.log(
        `[${new Date().toISOString()}] Ingested: ${this.total.toLocaleString()} / 3,000,000 | ` +
        `${windowRate}/s (avg ${overallRate}/s) | ETA: ${eta}s`
      );
      this.lastLog = now;
      this.lastCount = this.total;
    }
  }
}

async function runIngestion() {
  console.log("Starting ingestion...");
  await initDb();

  const existing = await getTotalProcessed();
  if (existing > 0) {
    console.log(`Resuming — state shows ${existing.toLocaleString()} processed`);
  }

  const resumeCursor = await getCursor();
  console.log(`Cursor: ${resumeCursor ?? "none (fresh start)"}`);

  const stats = new Stats();
  stats.total = existing;

  const fetchQueue: Array<string | undefined> = [resumeCursor ?? undefined];
  const writeQueue: Array<{ events: any[]; cursor: string | null }> = [];

  let allFetched = false;
  let lastCursor: string | null = resumeCursor;

  async function fetcher() {
    while (true) {
      if (fetchQueue.length === 0) {
        if (allFetched) break;
        await sleep(10);
        continue;
      }

      const cursor = fetchQueue.shift();

      try {
        const res = await fetchEvents(cursor, PAGE_SIZE);

        if (res.data.length > 0) {
          writeQueue.push({ events: res.data, cursor: res.nextCursor });
        }

        if (res.hasMore && res.nextCursor) {
          lastCursor = res.nextCursor;
          fetchQueue.push(res.nextCursor);
        } else {
          console.log("Fetch complete — no more pages");
          allFetched = true;
          break;
        }
      } catch (err: any) {
        if (err.message?.startsWith("BAD_CURSOR")) {
          console.error(`Bad cursor, resetting to start: ${err.message}`);
          // Reset cursor and start from beginning
          lastCursor = null;
          fetchQueue.push(undefined);
          await sleep(5000);
          continue;
        }
        console.error("Fetch error:", err.message);
        if (cursor !== undefined) fetchQueue.push(cursor);
        await sleep(3000);
      }
    }
  }

  async function writer() {
    while (true) {
      if (writeQueue.length === 0) {
        if (allFetched) break;
        await sleep(10);
        continue;
      }

      const batch = writeQueue.shift()!;
      try {
        await bulkInsertEvents(batch.events);
        stats.update(batch.events.length);

        if (stats.total % 5000 < batch.events.length) {
          await saveProgress(lastCursor, 0).catch(() => {});
        }
      } catch (err: any) {
        console.error("Write error:", err.message);
        writeQueue.unshift(batch);
        await sleep(1000);
      }
    }
  }

  await Promise.all([fetcher(), writer()]);

  await saveProgress(lastCursor, 0).catch(() => {});

  const result = await pool.query("SELECT COUNT(*) FROM events");
  const total = Number(result.rows[0].count);

  console.log(`\n=== INGESTION COMPLETE ===`);
  console.log(`Total events in DB: ${total.toLocaleString()}`);
  console.log(`=========================\n`);

  process.exit(0);
}

runIngestion().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});