import { strict as assert } from "node:assert";
import { spawn } from "node:child_process";

/**
 * Specification tests for the exec steering registry.
 *
 * /exec blocks until the command finishes, so the registry is the only way
 * a steering stop or pause can reach a run in flight. These tests drive the
 * real module against real processes: a stop must end a detached sleep, a
 * pause must freeze it without ending it, and a finished or unknown id must
 * report not_found rather than signalling anything.
 */

import { registerExec, runningExecCount, signalExec } from "../src/execControl";

function spawnSleep(seconds: number) {
  return spawn(`sleep ${seconds}`, [], { shell: true, detached: true });
}

function waitForClose(proc: ReturnType<typeof spawn>): Promise<void> {
  return new Promise((resolve) => proc.on("close", () => resolve()));
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function run(name: string, fn: () => Promise<void>) {
  try {
    await fn();
    console.log(`ok - ${name}`);
  } catch (err) {
    console.error(`fail - ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

async function main() {
  await run("unknown ids report not_found", async () => {
    assert.strictEqual(signalExec("never-registered", "stop"), "not_found");
  });

  await run("stop ends a running exec", async () => {
    const proc = spawnSleep(30);
    registerExec("stoppable", proc);
    const closed = waitForClose(proc);
    assert.strictEqual(signalExec("stoppable", "stop"), "ok");
    await closed;
    assert.strictEqual(proc.exitCode, null);
    assert.notStrictEqual(proc.signalCode, null);
  });

  await run("pause freezes without ending; resume lets stop through", async () => {
    const proc = spawnSleep(30);
    registerExec("pausable", proc);
    assert.strictEqual(signalExec("pausable", "pause"), "ok");
    await delay(200);
    assert.strictEqual(proc.exitCode, null, "paused exec must still be alive");
    assert.strictEqual(signalExec("pausable", "resume"), "ok");
    const closed = waitForClose(proc);
    assert.strictEqual(signalExec("pausable", "stop"), "ok");
    await closed;
  });

  await run("a finished exec deregisters itself", async () => {
    const proc = spawnSleep(0);
    registerExec("ephemeral", proc);
    await waitForClose(proc);
    // close handlers run in registration order; yield once for the registry's.
    await delay(10);
    assert.strictEqual(signalExec("ephemeral", "stop"), "not_found");
    assert.strictEqual(runningExecCount(), 0);
  });
}

main();
