import "./test-guard";
import assert from "node:assert/strict";
import { factorySchema } from "../lib/config";
import { watch, type WatchInput } from "../lib/watchdog";
import { backoffMs, pendingRetry, planRetry, retryDue, retryLine } from "../lib/retry";
import { inboxReason, type InboxItem } from "../lib/inbox";
import type { JobRow } from "../db/schema";

const limits = factorySchema.parse({ stuckAfterMinutes: 30, stoppedGraceMinutes: 2, failAfterNudgeMinutes: 30 });
const now = new Date("2026-10-23T12:00:00Z");
const ago = (min: number) => new Date(now.getTime() - min * 60_000);
const base: WatchInput = {
  stage: "implementing",
  lastEventAt: ago(1),
  nudgedAt: null,
  nudgedStage: null,
  thread: "running",
  tracked: true,
  blockedOn: null,
  now,
};

assert.deepEqual(watch(base, limits), { kind: "ok" }, "a running thread emitting events is left alone");
assert.deepEqual(watch({ ...base, lastEventAt: ago(29) }, limits), { kind: "ok" }, "quiet but under the threshold");
assert.deepEqual(watch({ ...base, thread: "stopped" }, limits), { kind: "ok" }, "a turn that just ended gets a grace period");
assert.deepEqual(watch({ ...base, lastEventAt: ago(31) }, limits), { kind: "nudge", idleMinutes: 31 }, "quiet past the threshold");
assert.deepEqual(watch({ ...base, thread: "stopped", lastEventAt: ago(3) }, limits), { kind: "nudge", idleMinutes: 3 }, "an interrupted thread");

const nudged = { ...base, thread: "stopped" as const, nudgedAt: ago(10), nudgedStage: "implementing" as const, lastEventAt: ago(10) };
assert.deepEqual(watch(nudged, limits), { kind: "ok" }, "never nudged twice in the same stage");
assert.equal(watch({ ...nudged, nudgedAt: ago(31), lastEventAt: ago(31) }, limits).kind, "fail", "still stuck after the second threshold");
assert.deepEqual(
  watch({ ...nudged, nudgedAt: ago(40), lastEventAt: ago(5) }, limits),
  { kind: "ok" },
  "activity after the nudge restarts the fail clock",
);
assert.deepEqual(
  watch({ ...base, nudgedAt: ago(40), nudgedStage: "investigating" }, limits),
  { kind: "reset" },
  "moving to another stage clears the nudge",
);
assert.equal(watch({ ...base, thread: "gone" }, limits).kind, "fail", "a vanished thread fails at once");

const untracked = { ...base, tracked: false };
assert.deepEqual(watch({ ...untracked, lastEventAt: ago(240) }, limits), { kind: "ok" }, "an untracked run reports nothing, so quiet is not stuck");
assert.deepEqual(watch({ ...untracked, thread: "stopped", lastEventAt: ago(240) }, limits), { kind: "ok" }, "nor is a stopped thread");
assert.deepEqual(
  watch({ ...untracked, nudgedAt: ago(90), nudgedStage: "implementing", lastEventAt: ago(240) }, limits),
  { kind: "reset" },
  "a nudge left from before is cleared instead of failing the run",
);
assert.equal(watch({ ...untracked, thread: "gone" }, limits).kind, "fail", "an untracked run whose thread is gone still fails");

// RES-13: Claude Code asked to approve `rm -rf "$(cat /tmp/res13-dir)"` even in bypass mode, and nobody answers in a factory run.
const prompt = 'kill 1616 1632; rm -rf "$(cat /tmp/res13-dir)" /tmp/res13-pid; git status --short';
const blocked = watch({ ...base, blockedOn: prompt }, limits);
assert.equal(blocked.kind, "fail", "a thread waiting on an approval fails at once, not after the stuck and nudge windows");
assert.ok(blocked.kind === "fail" && blocked.reason.includes(prompt), `the reason quotes what it waits on: ${JSON.stringify(blocked)}`);
assert.equal(watch({ ...untracked, blockedOn: prompt }, limits).kind, "fail", "an untracked run blocked on an approval fails too");
assert.equal(watch({ ...nudged, blockedOn: prompt }, limits).kind, "fail", "a nudge cannot answer an approval, so a nudged run fails too");
const long = watch({ ...base, blockedOn: "x".repeat(5000) }, limits);
assert.ok(long.kind === "fail" && long.reason.length < 600, "a long command is cut short in the reason");

// RES-60: a watchdog failure is retried after min(base * 2^(attempt-1), max), until the job has had maxAttempts attempts.
const retry = factorySchema.parse({ retry: { maxAttempts: 8, baseSeconds: 10, maxBackoffMinutes: 5 } }).retry;
assert.deepEqual(factorySchema.parse({}).retry, { maxAttempts: 0, baseSeconds: 10, maxBackoffMinutes: 5 }, "retries are off by default");
assert.deepEqual(
  [1, 2, 3, 4, 5, 6, 7].map((attempt) => backoffMs(attempt, retry) / 1000),
  [10, 20, 40, 80, 160, 300, 300],
  "the wait doubles per attempt and stops at maxBackoffMinutes",
);
assert.equal(planRetry(1, factorySchema.parse({}).retry, now), null, "maxAttempts 0 never retries");
const three = { ...retry, maxAttempts: 3 };
assert.deepEqual(planRetry(1, three, now), { attempt: 2, of: 3, at: new Date(now.getTime() + 10_000).toISOString() }, "attempt 1 retries as attempt 2 in 10s");
assert.deepEqual(planRetry(2, three, now), { attempt: 3, of: 3, at: new Date(now.getTime() + 20_000).toISOString() }, "attempt 2 waits twice as long");
assert.equal(planRetry(3, three, now), null, "the third attempt is the last: its failure waits for a human");
assert.equal(planRetry(5, three, now), null, "so is one a human started past the cap");

const owed = planRetry(1, three, now)!;
const failedJob = { stage: "failed" as const, attempt: 1, auto_retry: owed };
assert.deepEqual(pendingRetry(failedJob), owed, "a failed attempt owes the retry recorded for it");
assert.equal(pendingRetry({ ...failedJob, attempt: 2 }), null, "a later attempt ignores an earlier attempt's retry");
assert.equal(pendingRetry({ ...failedJob, stage: "queued" }), null, "a job that left failed owes nothing");
assert.equal(pendingRetry({ ...failedJob, auto_retry: null }), null);
assert.equal(retryDue(owed, now.getTime() + 9_999), false, "not due before the backoff");
assert.equal(retryDue(owed, now.getTime() + 10_000), true, "due once it passed");
assert.equal(retryLine(owed, now.getTime()), "Retrying (attempt 2 of 3, in 10s)");
assert.equal(retryLine(owed, now.getTime() + 7_500), "Retrying (attempt 2 of 3, in 3s)", "the countdown rounds up");
assert.equal(retryLine(owed, now.getTime() + 60_000), "Retrying (attempt 2 of 3, now)", "a due retry says it is starting");
assert.equal(retryLine(owed), "Retrying (attempt 2 of 3)", "without a clock, as in server HTML, no countdown");

const item = (job: Partial<JobRow>) =>
  ({ kind: "failed", decision: null, at: now, job: { stage: "failed", stage_at: now, updated_at: now, stop: null, execution: null, question: null, stage_summary: "Watchdog stopped this run: its T3 thread is gone (stage implementing).", ...job } }) as InboxItem<JobRow>;
assert.equal(
  inboxReason(item({ attempt: 1, auto_retry: owed }), now.getTime()),
  "Retrying (attempt 2 of 3, in 10s) · Watchdog stopped this run: its T3 thread is gone (stage implementing).",
  "the inbox and Slack lead with the retry",
);
assert.equal(inboxReason(item({ attempt: 2, auto_retry: owed }), now.getTime()), "Watchdog stopped this run: its T3 thread is gone (stage implementing).", "and say nothing once it is spent");

console.log("watchdog regressions passed");
