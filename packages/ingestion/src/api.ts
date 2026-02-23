import axios from "axios";

const BASE_URL =
  process.env.API_BASE_URL ||
  "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1";

const API_KEY = process.env.TARGET_API_KEY!;

const api = axios.create({
  baseURL: BASE_URL,
  headers: { "X-API-Key": API_KEY },
  timeout: 60000,
});

const MIN_DELAY_MS = Number(process.env.MIN_DELAY_MS ?? 500);

let lastRequestTime = 0;

async function throttledRequest(cursor?: string, limit = 1000) {
  const now = Date.now();
  const gap = now - lastRequestTime;
  if (gap < MIN_DELAY_MS) {
    await sleep(MIN_DELAY_MS - gap);
  }
  lastRequestTime = Date.now();

  const params: Record<string, any> = { limit };
  if (cursor) params.cursor = cursor;
  return api.get("/events", { params });
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

export interface EventsResponse {
  data: any[];
  hasMore: boolean;
  nextCursor: string | null;
}

export async function fetchEvents(
  cursor?: string,
  limit = 1000
): Promise<EventsResponse> {
  let retries = 0;

  while (true) {
    try {
      const res = await throttledRequest(cursor, limit);
      const body = res.data;

      const data = body.data ?? body.events ?? [];
      const nextCursor =
        body.nextCursor ??
        body.next_cursor ??
        body.pagination?.nextCursor ??
        body.pagination?.next_cursor ??
        null;
      const hasMore =
        body.hasMore ??
        body.has_more ??
        body.pagination?.hasMore ??
        body.pagination?.has_more ??
        (nextCursor != null);

      const remaining = res.headers["x-ratelimit-remaining"];
      const resetIn = res.headers["x-ratelimit-reset"] ?? res.headers["retry-after"];
      if (remaining !== undefined && Number(remaining) < 10) {
        console.log(`⚠️  Rate limit low: ${remaining} remaining, resets in ${resetIn}s`);
      }

      return { data, hasMore, nextCursor };
    } catch (err: any) {
      if (err.response?.status === 429) {
        const retryAfter = Number(err.response.headers["retry-after"] ?? 10);
        console.log(`Rate limited — waiting ${retryAfter}s`);
        await sleep(retryAfter * 1000);
        lastRequestTime = Date.now() + retryAfter * 1000;
        continue;
      }

      if (err.response?.status === 400) {
        throw new Error(`BAD_CURSOR:${cursor}`);
      }

      if (err.response?.status === 504 || !err.response) {
        const wait = Math.min(1000 * 2 ** retries, 16000);
        console.warn(`Gateway/network error, retry in ${wait}ms`);
        await sleep(wait);
        retries++;
        continue;
      }

      throw err;
    }
  }
}