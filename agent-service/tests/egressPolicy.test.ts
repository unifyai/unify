import { strict as assert } from "node:assert";

import {
  EgressPolicyError,
  parseEgressPolicy,
  resolveEgress,
  safeHost,
  supportedRegions,
} from "../src/egressPolicy";

/**
 * Tests for egress policy resolution.
 *
 * The behaviours worth pinning are the refusals. A policy that resolves to
 * "direct" when it asked for a specific exit, or a proxied session whose
 * timezone still reports the host's region, are both worse outcomes than an
 * error — the caller cannot tell either has happened.
 */

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

const MANAGED_ENV = {
  UNITY_EGRESS_PROXY_SERVER: "http://gate.example.net:7777",
  UNITY_EGRESS_PROXY_USERNAME: "acct-country-{region}-session-{session}",
  UNITY_EGRESS_PROXY_PASSWORD: "s3cret", // pragma: allowlist secret - test fixture
};

run("absent or direct policy leaves the session unchanged", () => {
  for (const policy of [null, undefined, { mode: "direct" as const }]) {
    const resolved = resolveEgress(policy, MANAGED_ENV);
    assert.equal(resolved.proxy, undefined);
    assert.deepEqual(resolved.args, []);
    assert.deepEqual(resolved.contextOptions, {});
  }
});

run("a managed region resolves proxy, timezone, locale and Accept-Language together", () => {
  const resolved = resolveEgress(
    { mode: "region", region: "gb", sessionKey: "abc123" },
    MANAGED_ENV,
  );
  assert.equal(resolved.proxy?.server, "http://gate.example.net:7777");
  // Geography and stickiness are encoded in the username, which is how
  // residential providers expose them — the caller never sees a credential.
  assert.equal(resolved.proxy?.username, "acct-country-gb-session-abc123");
  assert.equal(resolved.proxy?.password, "s3cret");
  assert.equal(resolved.contextOptions.timezoneId, "Europe/London");
  assert.equal(resolved.contextOptions.locale, "en-GB");
  assert.equal(
    resolved.contextOptions.extraHTTPHeaders?.["Accept-Language"],
    "en-GB,en;q=0.9",
  );
});

run("a non-English region gets a consistent Accept-Language chain", () => {
  const resolved = resolveEgress({ mode: "region", region: "de" }, MANAGED_ENV);
  assert.equal(resolved.contextOptions.locale, "de-DE");
  assert.equal(
    resolved.contextOptions.extraHTTPHeaders?.["Accept-Language"],
    "de-DE,de;q=0.9,en;q=0.8",
  );
});

run("any exit brings WebRTC containment with it", () => {
  // Without this Chromium reveals the host address over STUN regardless of
  // the HTTP proxy, silently undoing the whole policy.
  const resolved = resolveEgress({ mode: "region", region: "gb" }, MANAGED_ENV);
  assert.ok(
    resolved.args.some((a) => a.includes("disable_non_proxied_udp")),
    "expected WebRTC containment args",
  );
});

run("an unconfigured provider refuses rather than falling back to direct", () => {
  assert.throws(
    () => resolveEgress({ mode: "region", region: "gb" }, {}),
    (err: unknown) =>
      err instanceof EgressPolicyError && /refusing to fall back/.test((err as Error).message),
  );
});

run("an unknown region refuses rather than defaulting", () => {
  // A silently-wrong timezone is precisely the mismatch this prevents, so an
  // unrecognised region must not resolve to some default profile.
  assert.throws(
    () => resolveEgress({ mode: "region", region: "zz" }, MANAGED_ENV),
    (err: unknown) => err instanceof EgressPolicyError && /unsupported egress region/.test((err as Error).message),
  );
});

run("region mode requires a region", () => {
  assert.throws(
    () => resolveEgress({ mode: "region" }, MANAGED_ENV),
    (err: unknown) => err instanceof EgressPolicyError,
  );
});

run("byo requires a server and never silently degrades", () => {
  assert.throws(
    () => resolveEgress({ mode: "byo" }, MANAGED_ENV),
    (err: unknown) =>
      err instanceof EgressPolicyError && /refusing to fall back/.test((err as Error).message),
  );
});

run("byo uses the caller's exit and still derives context when a region is given", () => {
  const resolved = resolveEgress(
    {
      mode: "byo",
      region: "ie",
      proxy: { server: "http://corp.example.com:3128", username: "u", password: "p" },
    },
    {},
  );
  assert.equal(resolved.proxy?.server, "http://corp.example.com:3128");
  assert.equal(resolved.contextOptions.timezoneId, "Europe/Dublin");
  assert.ok(resolved.args.length > 0);
});

run("byo without a region proxies but derives no context", () => {
  const resolved = resolveEgress(
    { mode: "byo", proxy: { server: "http://corp.example.com:3128" } },
    {},
  );
  assert.equal(resolved.proxy?.server, "http://corp.example.com:3128");
  assert.deepEqual(resolved.contextOptions, {});
});

run("byo with an unknown region refuses", () => {
  assert.throws(
    () =>
      resolveEgress(
        { mode: "byo", region: "zz", proxy: { server: "http://c.example:3128" } },
        {},
      ),
    (err: unknown) => err instanceof EgressPolicyError,
  );
});

run("the description carries the host but never a credential", () => {
  const resolved = resolveEgress(
    { mode: "region", region: "gb", sessionKey: "abc" },
    MANAGED_ENV,
  );
  assert.ok(resolved.description.includes("gate.example.net:7777"));
  assert.ok(!resolved.description.includes("s3cret"));
  assert.ok(!resolved.description.includes("acct-country"));
});

run("safeHost strips embedded credentials", () => {
  // Embedded credentials are the thing under test here, not a real secret.
  assert.equal(safeHost("http://user:pass@gate.example.net:7777"), "gate.example.net:7777"); // pragma: allowlist secret
  assert.equal(safeHost("gate.example.net:7777"), "gate.example.net:7777");
  assert.equal(safeHost("::::"), "(unparseable)");
});

run("parse rejects an unknown mode", () => {
  assert.throws(
    () => parseEgressPolicy({ mode: "sneaky" }),
    (err: unknown) => err instanceof EgressPolicyError,
  );
});

run("parse rejects a proxy without a server", () => {
  assert.throws(
    () => parseEgressPolicy({ mode: "byo", proxy: { username: "u" } }),
    (err: unknown) => err instanceof EgressPolicyError,
  );
});

run("parse normalises region case and passes through a well-formed policy", () => {
  const parsed = parseEgressPolicy({ mode: "region", region: "GB", sessionKey: "s1" });
  assert.equal(parsed?.mode, "region");
  assert.equal(parsed?.region, "gb");
  assert.equal(parsed?.sessionKey, "s1");
  assert.equal(parseEgressPolicy(undefined), null);
});

run("supported regions are advertised for the console picker", () => {
  const regions = supportedRegions();
  assert.ok(regions.includes("gb"));
  assert.ok(regions.includes("us"));
  assert.deepEqual(regions, [...regions].sort());
});
