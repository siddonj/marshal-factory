import "./test-guard";
import assert from "node:assert/strict";
import { eq, like } from "drizzle-orm";

// RES-60: a run the watchdog fails is queued again as a new attempt after a backoff, up to
// factory.retry.maxAttempts; a failure someone meant (the agent's own, a stop, a close) never is.

const OWNER = "owner@example.com";
const { db } = await import("../db/client");
const { jobs } = await import("../db/schema");
const { TEST_MACHINE_ID } = await import("../lib/config");
const { eventSchema } = await import("../lib/events");
const { attemptBranch } = await import("../lib/factory");
const { ingest } = await import("../lib/ingest");
const { pendingRetry } = await import("../lib/retry");
const { notifyEvents } = await import("../lib/slack");
const { SOURCES, parseJob } = await import("../lib/sources");
const { claim, config, ingestClient, launchMessage, stopJob, threads, watchdog, writeBack } = await import("../runner/ops");
const { closeJob, retryJob } = await import("../runner/inbox");
const { requeueDue } = await import("../runner/retry");

const stamp = Date.now() % 1_000_000;
const REPO = "ski043/factory-demo";
const keyOf = (name: string) => `WRT-${name}-${stamp}`;
const row = async (key: string) => (await db.select().from(jobs).where(eq(jobs.key, key)))[0]!;

// The source, faked the way Linear behaves: write-back of a failure takes the label off, requeue
// puts it back, a claim moves the issue to In Progress, so it is offered only between the two.
const offered = new Set<string>();
const written: string[] = [];
const requeued: string[] = [];
const unwanted = new Map<string, string>();
SOURCES.linear.writeBack = async (job, { stage }) => {
  written.push(`${job.key}:${stage}`);
  if (stage === "failed") offered.delete(job.key);
  return true;
};
SOURCES.linear.requeue = async (job) => void (requeued.push(job.key), offered.add(job.key));
SOURCES.linear.stopWanted = async (open) => open.flatMap((job) => (unwanted.has(job.key) ? [{ job, reason: unwanted.get(job.key)! }] : []));
SOURCES.linear.confirmStop = async (externalId) => (unwanted.has(externalId) ? { stop: true, reopen: false } : { stop: false });
const source = {
  ...SOURCES.linear,
  ready: async () =>
    [...offered].map((key) => ({
      externalId: key,
      key,
      title: "t",
      url: "u",
      description: "Fix the list.",
      writeback: { kind: "linear" as const, issueId: key },
      sourceState: { state: "Todo", labels: ["factory"], labelGroups: [] },
    })),
};
async function claimReady() {
  const { claimed } = await claim({ sources: [source], repos: async () => [REPO], registry: async () => [] });
  for (const key of claimed) offered.delete(key);
  return claimed;
}

// Threads: gone or running as the test says; the watchdog reads nothing else from T3.
const gone = new Set<string>();
threads.watch = async (threadId) => ({ state: threadId && gone.has(threadId) ? "gone" : "running", blockedOn: null });
ingestClient.post = async (event) => ingest(db, [eventSchema.parse(event)]);

/** Puts the job's current attempt to work on a thread, as a launch and the thread's session start would. */
async function launch(key: string) {
  const { attempt } = await row(key);
  const branch = attemptBranch(key, attempt);
  const sessionId = `wrt-${key}-a${attempt}`;
  await db.update(jobs).set({ machine_id: TEST_MACHINE_ID, t3_thread_id: `thread-${key}-a${attempt}` }).where(eq(jobs.key, key));
  let n = 0;
  const report = (stage: string, extra: Record<string, unknown> = {}) =>
    ingest(db, [eventSchema.parse({ eventId: `${sessionId}-${n++}`, sessionId, at: Date.now() + n, kind: "stage", stage, summary: `${stage} it`, jobKey: key, branch, ...extra })]);
  await ingest(db, [eventSchema.parse({ eventId: `${sessionId}-start`, sessionId, at: Date.now(), kind: "session_start", cwd: "/w", branch, repo: "factory-demo", t3ThreadId: `thread-${key}-a${attempt}` })]);
  await report("triage");
  return { thread: `thread-${key}-a${attempt}`, report };
}

/** Starts a job and has the watchdog fail it because its thread vanished. */
async function watchdogFailed(key: string) {
  const { thread } = await launch(key);
  gone.add(thread);
  assert.ok((await watchdog()).includes(`${key}:failed`), `the watchdog fails ${key}`);
  return row(key);
}

const maxInFlight = config.factory.maxInFlight;
const retryLimits = config.factory.retry;
config.factory.maxInFlight = 1000;
config.factory.retry = { maxAttempts: 3, baseSeconds: 10, maxBackoffMinutes: 5 };

try {
  // --- a vanished thread is retried after the backoff, twice, and the third attempt succeeds
  const A = keyOf("a");
  offered.add(A);
  assert.deepEqual(await claimReady(), [A]);
  const first = await watchdogFailed(A);
  const failedAt = first.stage_at!.getTime();
  assert.deepEqual(pendingRetry(first), { attempt: 2, of: 3, at: new Date(failedAt + 10_000).toISOString() }, "attempt 1 owes attempt 2, 10s out");
  assert.match(first.stage_summary!, /^Watchdog stopped this run: its T3 thread is gone/);

  await writeBack({ only: [A] });
  assert.deepEqual(written.filter((w) => w.startsWith(A)), [], "the failure is not written back while its retry is owed: the issue keeps its label");
  const [event] = notifyEvents([await row(A)], failedAt);
  assert.match(event!.reason, /^Retrying \(attempt 2 of 3, in 10s\) · Watchdog stopped this run/, "Slack says it is retrying");

  assert.deepEqual(await requeueDue(failedAt + 9_999), [], "nothing is requeued before the backoff");
  assert.equal(requeued.length, 0);
  assert.deepEqual(await requeueDue(failedAt + 10_000), [`${A}:requeued`], "the issue goes back on the ready list once it is due");
  assert.deepEqual(await claimReady(), [A], "and the claim starts it");
  const second = await row(A);
  assert.deepEqual([second.attempt, second.stage, pendingRetry(second)], [2, "queued", null], "as attempt 2, which owes nothing yet");
  assert.match(launchMessage(parseJob(second)), /This is attempt 2\. The previous attempt failed: Watchdog stopped this run: its T3 thread is gone/);
  assert.deepEqual(await requeueDue(failedAt + 3_600_000), [], "the spent retry is not requeued again");

  const again = await watchdogFailed(A);
  const againAt = again.stage_at!.getTime();
  assert.deepEqual(pendingRetry(again), { attempt: 3, of: 3, at: new Date(againAt + 20_000).toISOString() }, "the second wait is twice the first");
  assert.deepEqual(await requeueDue(againAt + 19_999), []);
  assert.deepEqual(await requeueDue(againAt + 20_000), [`${A}:requeued`]);
  assert.deepEqual(await claimReady(), [A]);
  assert.equal((await row(A)).attempt, 3);

  const { report } = await launch(A);
  await report("implementing");
  await report("pr", { prUrl: `https://github.com/${REPO}/pull/${stamp}` });
  const succeeded = await row(A);
  assert.deepEqual([succeeded.attempt, succeeded.stage, succeeded.pr_url], [3, "pr", `https://github.com/${REPO}/pull/${stamp}`], "attempt 3 opens its PR");

  // --- at the cap the failure is written back as today, and waits for a human
  const B = keyOf("cap");
  offered.add(B);
  await claimReady();
  await db.update(jobs).set({ attempt: 3 }).where(eq(jobs.key, B));
  const capped = await watchdogFailed(B);
  assert.deepEqual([capped.attempt, pendingRetry(capped), capped.auto_retry], [3, null, null], "the last attempt owes no retry");
  await writeBack({ only: [B] });
  assert.deepEqual(written.filter((w) => w.startsWith(B)), [`${B}:failed`], "its failure goes to the source: factory-failed, label off");
  assert.deepEqual(await requeueDue(Date.now() + 3_600_000), []);
  assert.deepEqual(await claimReady(), [], "and nothing starts it again");

  // --- a failure the agent reported is never retried
  const C = keyOf("agent");
  offered.add(C);
  await claimReady();
  const agent = await launch(C);
  await agent.report("failed", { summary: "The API it needs does not exist." });
  const reported = await row(C);
  assert.deepEqual([reported.stage, pendingRetry(reported)], ["failed", null]);
  await writeBack({ only: [C] });
  assert.deepEqual(written.filter((w) => w.startsWith(C)), [`${C}:failed`], "written back at once");

  // --- the label coming off while the retry waits cancels it (the webhook's stop)
  const D = keyOf("unlabeled");
  offered.add(D);
  await claimReady();
  await watchdogFailed(D);
  unwanted.set(D, "the factory label was removed in Linear");
  assert.equal(await stopJob((await row(D)).id, "the factory label was removed in Linear"), "retry cancelled");
  const unlabeled = await row(D);
  assert.equal(pendingRetry(unlabeled), null);
  assert.match(unlabeled.stage_summary!, /Not retried: the factory label was removed in Linear\.$/);
  assert.equal(unlabeled.synced_event_id, unlabeled.stage_event_id, "the source already says what the human wants, so nothing is written back");
  assert.deepEqual(await requeueDue(Date.now() + 3_600_000), []);
  assert.deepEqual([requeued.includes(D), (await row(D)).attempt], [false, 1], "never requeued");

  // --- and when no webhook came, the source is asked again before the retry
  const E = keyOf("canceled");
  offered.add(E);
  await claimReady();
  const canceled = await watchdogFailed(E);
  unwanted.set(E, "issue moved to Canceled in Linear");
  assert.deepEqual(await requeueDue(canceled.stage_at!.getTime() + 10_000), [`${E}:cancelled`]);
  assert.deepEqual([requeued.includes(E), pendingRetry(await row(E)), await claimReady()], [false, null, []]);

  // --- closing it in the inbox is a human stop: no retry, and the failure is written back
  const F = keyOf("closed");
  offered.add(F);
  await claimReady();
  await watchdogFailed(F);
  assert.deepEqual(await closeJob((await row(F)).id, { actor: OWNER, reason: "Not needed" }), { closed: F, pr: "none" });
  const closed = await row(F);
  assert.equal(pendingRetry(closed), null);
  assert.match(closed.stage_summary!, /Not retried: closed in the inbox by owner@example\.com\.$/);
  await writeBack({ only: [F] });
  assert.deepEqual(written.filter((w) => w.startsWith(F)), [`${F}:failed`]);
  assert.deepEqual(await requeueDue(Date.now() + 3_600_000), []);

  // --- a stop requested while the run works fails it without a retry
  const G = keyOf("stopped");
  offered.add(G);
  await claimReady();
  await launch(G);
  unwanted.set(G, "the factory label was removed in Linear");
  threads.send = async () => {};
  threads.state = async () => "gone";
  assert.equal(await stopJob((await row(G)).id, "the factory label was removed in Linear"), "stopped");
  const stopped = await row(G);
  assert.deepEqual([stopped.stage, pendingRetry(stopped), stopped.auto_retry], ["failed", null, null]);

  // --- Retry in the inbox starts the owed attempt at once instead of waiting for the backoff
  const H = keyOf("manual");
  offered.add(H);
  await claimReady();
  await watchdogFailed(H);
  assert.deepEqual(await retryJob((await row(H)).id, { actor: OWNER, attempt: 1, note: "" }, claimReady), { started: 2 });
  assert.deepEqual(await requeueDue(Date.now() + 3_600_000), [], "the automatic one is spent");
  assert.equal((await row(H)).attempt, 2);

  // --- maxAttempts 0, the default, never retries
  config.factory.retry = { maxAttempts: 0, baseSeconds: 10, maxBackoffMinutes: 5 };
  const I = keyOf("off");
  offered.add(I);
  await claimReady();
  const off = await watchdogFailed(I);
  assert.equal(pendingRetry(off), null);
  await writeBack({ only: [I] });
  assert.deepEqual(written.filter((w) => w.startsWith(I)), [`${I}:failed`]);

  console.log("watchdog retry regressions passed");
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  config.factory.maxInFlight = maxInFlight;
  config.factory.retry = retryLimits;
  await db.delete(jobs).where(like(jobs.key, `WRT-%-${stamp}`));
  process.exit(process.exitCode ?? 0);
}
