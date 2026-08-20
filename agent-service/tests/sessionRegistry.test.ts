import { strict as assert } from "node:assert";
import { EventEmitter } from "node:events";

import {
  BrowserSessionUnavailableError,
  registerLiveSession,
  unregisterSession,
} from "../src/sessionRegistry";

function run(name: string, fn: () => void) {
  try {
    fn();
    console.log(`ok - ${name}`);
  } catch (err) {
    console.error(`fail - ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

class FakeContext extends EventEmitter {
  constructor(private connected: boolean) {
    super();
  }

  browser() {
    return { isConnected: () => this.connected };
  }

  disconnect() {
    this.connected = false;
    this.emit("close");
  }
}

class ClosingContext extends FakeContext {
  once(event: string, listener: (...args: any[]) => void) {
    const result = super.once(event, listener);
    this.disconnect();
    return result;
  }
}

function session(connected = true) {
  const context = new FakeContext(connected);
  return {
    context,
    value: {
      agent: {
        context,
        page: { isClosed: () => context.browser().isConnected() === false },
      },
    },
  };
}

run("rejects a browser that disconnected during startup", () => {
  const sessions = new Map<string, ReturnType<typeof session>["value"]>();
  const dead = session(false);

  assert.throws(
    () => registerLiveSession(sessions, "dead", dead.value, () => {}),
    BrowserSessionUnavailableError,
  );
  assert.equal(sessions.size, 0);
});

run("rejects a context that closes while it is being registered", () => {
  const sessions = new Map<string, ReturnType<typeof session>["value"]>();
  const context = new ClosingContext(true);
  const closing = {
    agent: {
      context,
      page: { isClosed: () => context.browser().isConnected() === false },
    },
  };

  assert.throws(
    () => registerLiveSession(sessions, "closing", closing, () => {}),
    BrowserSessionUnavailableError,
  );
  assert.equal(sessions.size, 0);
});

run("evicts a registered session when its browser context closes", () => {
  const sessions = new Map<string, ReturnType<typeof session>["value"]>();
  const live = session();
  const disconnected: string[] = [];

  registerLiveSession(sessions, "live", live.value, () => disconnected.push("live"));
  live.context.disconnect();

  assert.equal(sessions.has("live"), false);
  assert.deepEqual(disconnected, ["live"]);
});

run("does not report an expected close after unregistering", () => {
  const sessions = new Map<string, ReturnType<typeof session>["value"]>();
  const live = session();
  const disconnected: string[] = [];

  registerLiveSession(sessions, "live", live.value, () => disconnected.push("live"));
  assert.equal(unregisterSession(sessions, "live"), live.value);
  live.context.disconnect();

  assert.deepEqual(disconnected, []);
});
