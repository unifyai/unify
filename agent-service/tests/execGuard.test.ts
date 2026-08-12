import { strict as assert } from "node:assert";

/**
 * Specification tests for the `/exec` disable flag.
 *
 * The pod refuses command execution by setting AGENT_SERVICE_DISABLE_EXEC;
 * the remote desktop surfaces leave it unset and keep /exec. The predicate
 * must read the affirmative spellings as disabled and everything else --
 * including unset, empty, and "0"/"false" -- as enabled, so a desktop that
 * never sets it is never accidentally disabled.
 */

import { isExecDisabled } from "../src/execGuard";

async function run(name: string, fn: () => void) {
  try {
    fn();
    console.log(`ok - ${name}`);
  } catch (err) {
    console.error(`not ok - ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

run("unset is enabled", () => {
  assert.equal(isExecDisabled(undefined), false);
});

run("empty and whitespace are enabled", () => {
  assert.equal(isExecDisabled(""), false);
  assert.equal(isExecDisabled("   "), false);
});

run("explicit falsey values stay enabled", () => {
  for (const v of ["0", "false", "no", "off"]) {
    assert.equal(isExecDisabled(v), false, `expected ${v} to be enabled`);
  }
});

run("affirmative values disable, case- and space-insensitive", () => {
  for (const v of ["1", "true", "yes", "on", "TRUE", "Yes", " on "]) {
    assert.equal(isExecDisabled(v), true, `expected ${v} to be disabled`);
  }
});
