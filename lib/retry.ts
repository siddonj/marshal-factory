import { z } from "zod";
import type { JobRow } from "../db/schema";
import type { RetryLimits } from "./config";

// Automatic retries of a run the watchdog failed: its thread went quiet, vanished or waited on an
// approval nobody answers. That says nothing about the issue, so the job is queued again as a
// new attempt after a backoff, min(baseSeconds * 2^(attempt-1), maxBackoffMinutes), until the job
// has had maxAttempts attempts. A failure the agent reported, a stop or a close is never retried:
// those come from someone who meant it. Pure, so the runner, the panel and Slack share the rules.

/** The retry owed to a failed attempt: the attempt it starts, the cap, and when it is due. */
export const autoRetrySchema = z.object({
  attempt: z.number().int().positive(),
  of: z.number().int().positive(),
  at: z.iso.datetime(),
});
export type AutoRetry = z.infer<typeof autoRetrySchema>;

/** How long attempt `failed` waits before the next one starts. */
export const backoffMs = (failed: number, limits: RetryLimits) =>
  Math.min(limits.baseSeconds * 1000 * 2 ** (failed - 1), limits.maxBackoffMinutes * 60_000);

/** The retry a watchdog failure of attempt `failed` earns; null once the job has had maxAttempts. */
export function planRetry(failed: number, limits: RetryLimits, now: Date): AutoRetry | null {
  if (failed >= limits.maxAttempts) return null;
  return { attempt: failed + 1, of: limits.maxAttempts, at: new Date(now.getTime() + backoffMs(failed, limits)).toISOString() };
}

/** The retry still owed to the job's current attempt. One recorded for an earlier attempt is spent. */
export function pendingRetry(job: Pick<JobRow, "stage" | "attempt" | "auto_retry">): AutoRetry | null {
  const retry = autoRetrySchema.safeParse(job.auto_retry).data;
  return job.stage === "failed" && retry?.attempt === job.attempt + 1 ? retry : null;
}

export const retryDue = (retry: AutoRetry, now: number) => Date.parse(retry.at) <= now;

/** What the panel and Slack say about a retry on its way; without a clock, as server-rendered HTML must, no countdown. */
export function retryLine(retry: AutoRetry, now?: number) {
  if (now === undefined) return `Retrying (attempt ${retry.attempt} of ${retry.of})`;
  const seconds = Math.ceil((Date.parse(retry.at) - now) / 1000);
  return `Retrying (attempt ${retry.attempt} of ${retry.of}, ${seconds > 0 ? `in ${seconds}s` : "now"})`;
}
