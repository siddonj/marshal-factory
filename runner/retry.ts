import { and, eq, isNotNull, isNull, or, sql } from "drizzle-orm";
import { db } from "../db/client";
import { jobs, type JobRow } from "../db/schema";
import { pendingRetry, retryDue } from "../lib/retry";
import { parseJobs, sourceFor } from "../lib/sources";
import { SourceGoneError } from "../lib/sources/types";
import { thisMachine } from "./heartbeat";

// The runner's half of lib/retry.ts. While a retry is owed, the failure is not written back: the
// issue keeps its factory label, so taking the label off, closing the issue or closing the job in
// the inbox still reaches the runner as a stop, and cancels the retry instead.

/** lib/retry.ts's pendingRetry as SQL. */
export const pendingRetrySql = sql`(${jobs.stage} = 'failed' and (${jobs.auto_retry}->>'attempt')::int = ${jobs.attempt} + 1)`;

/**
 * Drops the retry owed to the job's current attempt, with why in its summary; false when none was
 * owed. `writtenBack` marks the failure synced, for a source that already says what the human wants.
 */
export async function cancelRetry(job: Pick<JobRow, "id" | "attempt" | "stage_event_id" | "stage_summary">, why: string, { writtenBack }: { writtenBack: boolean }) {
  const cancelled = await db
    .update(jobs)
    .set({
      auto_retry: null,
      stage_summary: `${job.stage_summary ?? "The run failed."} Not retried: ${why}.`,
      ...(writtenBack ? { synced_event_id: job.stage_event_id } : {}),
      updated_at: new Date(),
    })
    .where(and(eq(jobs.id, job.id), eq(jobs.attempt, job.attempt), pendingRetrySql))
    .returning({ id: jobs.id });
  return cancelled.length > 0;
}

/**
 * Puts each failed job whose retry is due back on its source's ready list, so the next claim
 * starts it as a new attempt the way adding the label again does. The source is asked first
 * whether the issue is still wanted; one that lost its label or was closed is not retried.
 * Requeueing twice is harmless, so a claim that can't take the job yet is simply tried again.
 */
export async function requeueDue(now = Date.now()) {
  const me = (await thisMachine()).id;
  const owed = parseJobs(await db.select().from(jobs).where(and(or(eq(jobs.machine_id, me), isNull(jobs.machine_id)), isNotNull(jobs.auto_retry), pendingRetrySql)));
  const done: string[] = [];
  const failed: string[] = [];
  for (const job of owed) {
    const retry = pendingRetry(job);
    if (!retry || !retryDue(retry, now)) continue;
    const source = sourceFor(job.source);
    try {
      const [stop] = await source.stopWanted([job]);
      if (stop) {
        if (await cancelRetry(job, stop.reason, { writtenBack: true })) done.push(`${job.key}:cancelled`);
        continue;
      }
      await source.requeue(job);
      done.push(`${job.key}:requeued`);
    } catch (error) {
      if (error instanceof SourceGoneError) {
        if (await cancelRetry(job, error.message, { writtenBack: true })) done.push(`${job.key}:cancelled`);
        continue;
      }
      failed.push(`${job.key}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  if (failed.length > 0) throw new Error(`could not requeue ${failed.join("; ")} (done ${done.join(", ") || "nothing"})`);
  return done;
}
