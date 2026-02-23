import { bulkInsertEvents, saveProgress } from "./db.js";

export async function processBatch(events: any[], nextCursor: string | null) {
  if (events.length === 0) return;
  await bulkInsertEvents(events);
  await saveProgress(nextCursor, events.length);
}