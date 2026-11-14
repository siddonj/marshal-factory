import "./test-guard";
import assert from "node:assert/strict";
import { factorySchema } from "../lib/config";
import { watch, type WatchInput } from "../lib/watchdog";

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

console.log("watchdog regressions passed");
