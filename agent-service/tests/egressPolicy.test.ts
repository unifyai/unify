import { strict as assert } from "node:assert";

import {
  EgressPolicyError,
  acceptLanguage,
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
  // The handle is region-scoped — see the sticky-session test below.
  assert.equal(resolved.proxy?.username, "acct-country-gb-session-abc123_gb");
  assert.equal(resolved.proxy?.password, "s3cret");
  assert.equal(resolved.contextOptions.timezoneId, "Europe/London");
  assert.equal(resolved.contextOptions.locale, "en-GB");
  assert.equal(
    resolved.contextOptions.extraHTTPHeaders?.["Accept-Language"],
    "en-GB,en;q=0.9",
  );
});

run("a non-English locale gets a consistent Accept-Language chain", () => {
  // Both supported regions are English-speaking, so this covers the branch
  // directly rather than through a region that does not exist yet.
  assert.equal(acceptLanguage("en-GB"), "en-GB,en;q=0.9");
  assert.equal(acceptLanguage("de-DE"), "de-DE,de;q=0.9,en;q=0.8");
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
      region: "gb",
      proxy: { server: "http://corp.example.com:3128", username: "u", password: "p" },
    },
    {},
  );
  assert.equal(resolved.proxy?.server, "http://corp.example.com:3128");
  assert.equal(resolved.contextOptions.timezoneId, "Europe/London");
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

run("the region placeholder is filled in either case", () => {
  // Providers differ on case and the failure is silent: one that wants cc-GB
  // and receives cc-gb may ignore the geo-targeting rather than reject it.
  const resolved = resolveEgress(
    { mode: "region", region: "gb", sessionKey: "s1" },
    { ...MANAGED_ENV, UNITY_EGRESS_PROXY_USERNAME: "customer-acme-cc-{REGION}-sessid-{session}" },
  );
  assert.equal(resolved.proxy?.username, "customer-acme-cc-GB-sessid-s1_gb");
});

run("the sticky handle is scoped by region", () => {
  // Verified against a live endpoint: a sticky session is pinned by its handle
  // alone, so one handle reused across two countries returns the first
  // country's exit and ignores the second country parameter entirely. A
  // caller's handle is stable per identity, so without scoping, changing a
  // region would keep the old exit until the TTL lapsed — silently.
  const gb = resolveEgress({ mode: "region", region: "gb", sessionKey: "alice" }, MANAGED_ENV);
  const us = resolveEgress({ mode: "region", region: "us", sessionKey: "alice" }, MANAGED_ENV);
  assert.notEqual(gb.proxy?.username, us.proxy?.username);
  assert.ok(gb.proxy?.username?.endsWith("_gb"));
  assert.ok(us.proxy?.username?.endsWith("_us"));
});

run("delimiter characters are stripped from the sticky handle", () => {
  // Verified live: these usernames are dash-delimited key-value pairs, so a
  // dash inside a value terminates it and discards the rest — the country and
  // session-duration parameters were both silently lost, collapsing two
  // regions onto one exit. Profile keys like "reader-one" are the obvious way
  // this reaches production.
  const resolved = resolveEgress(
    { mode: "region", region: "gb", sessionKey: "reader-one" },
    { ...MANAGED_ENV, UNITY_EGRESS_PROXY_USERNAME: "customer-acme-cc-{REGION}-sessid-{session}-sesstime-30" },
  );
  assert.equal(resolved.proxy?.username, "customer-acme-cc-GB-sessid-readerone_gb-sesstime-30");
  assert.ok(!resolved.proxy?.username?.includes("reader-one"));
});

run("only contracted regions are advertised", () => {
  // Advertising a country we have no egress for turns a clear refusal at
  // configuration time into a session that starts and then behaves wrongly.
  assert.deepEqual(supportedRegions(), ["gb", "us"]);
});

// A dedicated-IP endpoint: the username is literal and the exit is a fixed
// address, so the region cannot travel to the provider as a parameter.
const DEDICATED_ENV = {
  UNITY_EGRESS_PROXY_SERVER: "http://isp.example.net:8001",
  UNITY_EGRESS_PROXY_USERNAME: "acct_fixed",
  UNITY_EGRESS_PROXY_PASSWORD: "s3cret", // pragma: allowlist secret - test fixture
  UNITY_EGRESS_PROXY_REGIONS: "gb",
};

run("a dedicated endpoint serves its declared region with the username verbatim", () => {
  const resolved = resolveEgress(
    { mode: "region", region: "gb", sessionKey: "alice" },
    DEDICATED_ENV,
  );
  // Nothing to substitute, and nothing invented: a literal username must reach
  // the provider byte-for-byte or it simply fails to authenticate.
  assert.equal(resolved.proxy?.username, "acct_fixed");
  assert.equal(resolved.contextOptions.timezoneId, "Europe/London");
});

run("a dedicated endpoint refuses a region it does not exit from", () => {
  // The exit is one fixed address. Serving `us` from it would pair a London
  // address with America/New_York and en-US — a sharper contradiction than no
  // proxy at all, and one that reports success.
  assert.throws(
    () => resolveEgress({ mode: "region", region: "us" }, DEDICATED_ENV),
    (err: unknown) =>
      err instanceof EgressPolicyError && /serves gb, not "us"/.test((err as Error).message),
  );
});

run("a fixed exit of undeclared geography refuses every region", () => {
  // Without a declaration there is no way to know where the endpoint leaves
  // from, so claiming any particular region is a guess.
  const { UNITY_EGRESS_PROXY_REGIONS: _omitted, ...undeclared } = DEDICATED_ENV;
  assert.throws(
    () => resolveEgress({ mode: "region", region: "gb" }, undeclared),
    (err: unknown) =>
      err instanceof EgressPolicyError && /unknown geography/.test((err as Error).message),
  );
});

run("a geo-targeted username needs no region declaration", () => {
  // The region travels in the username, so the provider itself enforces it and
  // any supported region is servable without further configuration.
  const resolved = resolveEgress({ mode: "region", region: "us" }, MANAGED_ENV);
  assert.ok(resolved.proxy?.username?.includes("-us-"));
});

run("the region declaration tolerates spacing and case", () => {
  const resolved = resolveEgress(
    { mode: "region", region: "us" },
    { ...DEDICATED_ENV, UNITY_EGRESS_PROXY_REGIONS: " GB , US " },
  );
  assert.equal(resolved.contextOptions.locale, "en-US");
});
