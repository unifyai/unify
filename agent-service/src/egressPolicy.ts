/**
 * Egress policy: where a browser session's traffic leaves from.
 *
 * Sessions egress from wherever the host happens to sit — today that is
 * `us-central1` for every assistant on the platform. That is not only a
 * detection concern; it is a *correctness* one. An assistant researching
 * prices, availability or search results on behalf of a user in Germany
 * currently sees what a machine in Iowa sees.
 *
 * This models the answer as a **policy**, not a proxy string, because three
 * things have to move together or the result is worse than not proxying at
 * all:
 *
 *  - **Proxy** — the exit itself.
 *  - **Timezone / locale / Accept-Language** — a browser exiting a UK address
 *    while reporting `America/Chicago` and `en-US` is a sharper contradiction
 *    than the unproxied address was. These are *derived* from the region and
 *    are deliberately not separately settable.
 *  - **WebRTC** — Chromium reveals the host address over STUN regardless of
 *    the HTTP proxy, so an unhandled WebRTC stack silently undoes the proxy.
 *
 * Resolution **fails closed**. A policy that asks for an exit we cannot
 * provide raises rather than quietly falling back to direct egress: a caller
 * that configured an egress and silently got the host's own address is worse
 * off than one that got an error, because it does not know to stop.
 */

/** Managed-region provider configuration, read once from the environment. */
export interface EgressEnv {
  [key: string]: string | undefined;
  /** Proxy endpoint for managed regions, e.g. `http://gate.provider.com:7777`. */
  UNIFY_EGRESS_PROXY_SERVER?: string;
  /**
   * Username template. Residential providers encode both the geography and
   * the sticky-session handle in the username, so `{region}` and `{session}`
   * are substituted here rather than the caller ever seeing a credential.
   *
   * A dedicated-IP endpoint has neither: the username is literal, the exit is
   * whatever address the endpoint owns, and `UNIFY_EGRESS_PROXY_REGIONS` below
   * is what states where that is.
   */
  UNIFY_EGRESS_PROXY_USERNAME?: string;
  UNIFY_EGRESS_PROXY_PASSWORD?: string;
  /**
   * Regions the configured endpoint actually exits from, comma-separated.
   *
   * Required when the username carries no `{region}` placeholder — see
   * `assertRegionServable`.
   */
  UNIFY_EGRESS_PROXY_REGIONS?: string;
}

export interface ProxyConfig {
  server: string;
  username?: string;
  password?: string;
  bypass?: string;
}

export type EgressMode = 'direct' | 'region' | 'byo';

export interface EgressPolicy {
  mode: EgressMode;
  /** ISO 3166-1 alpha-2, lower-case. Required for `region`. */
  region?: string;
  /** Caller-supplied exit. Required for `byo`. */
  proxy?: ProxyConfig;
  /**
   * Stable handle for the sticky session. Residential exits rotate on a TTL
   * unless pinned, and an IP that changes mid-session is itself a signal.
   */
  sessionKey?: string;
}

export interface ResolvedEgress {
  /** Extra Chromium args — WebRTC containment when an exit is in use. */
  args: string[];
  proxy?: ProxyConfig;
  /** Playwright BrowserContext fields derived from the region. */
  contextOptions: {
    timezoneId?: string;
    locale?: string;
    extraHTTPHeaders?: Record<string, string>;
  };
  /** For logging. Never contains credentials. */
  description: string;
}

/**
 * Region → (IANA timezone, BCP 47 locale).
 *
 * Deliberately a small, explicit table rather than a lookup library: an
 * unknown region must fail rather than resolve to a default, because a
 * silently-wrong timezone is exactly the mismatch this exists to prevent.
 *
 * Only the regions we have actually contracted egress for belong here.
 * Advertising a country we cannot serve turns a clear refusal at configuration
 * time into a session that starts and then behaves wrongly. Adding one is two
 * lines plus provider coverage.
 */
const REGION_PROFILES: Record<string, { timezoneId: string; locale: string }> = {
  gb: { timezoneId: 'Europe/London', locale: 'en-GB' },
  us: { timezoneId: 'America/New_York', locale: 'en-US' },
};

export function supportedRegions(): string[] {
  return Object.keys(REGION_PROFILES).sort();
}

export class EgressPolicyError extends Error {}

/**
 * Chromium flags that stop WebRTC revealing the host address behind a proxy.
 *
 * `disable_non_proxied_udp` forces WebRTC through the proxy rather than
 * opening direct UDP, which is the leak every fingerprinting script checks.
 */
const WEBRTC_CONTAINMENT_ARGS = [
  '--force-webrtc-ip-handling-policy=disable_non_proxied_udp',
  '--webrtc-ip-handling-policy=disable_non_proxied_udp',
];

/**
 * `Accept-Language` consistent with the locale, with a plain-English fallback.
 *
 * Exported so the non-English branch stays covered while the supported regions
 * are both English-speaking — otherwise adding the first non-English region
 * would silently ship an untested header.
 */
export function acceptLanguage(locale: string): string {
  const base = locale.split('-')[0];
  return base === 'en' ? `${locale},en;q=0.9` : `${locale},${base};q=0.9,en;q=0.8`;
}

/**
 * Fill the provider's username template.
 *
 * Both cases of the region placeholder are supported because providers differ
 * and the failure is silent: a provider that wants ``cc-GB`` and receives
 * ``cc-gb`` may ignore the geo-targeting rather than reject it, so traffic
 * exits from somewhere other than the region asked for and nothing errors.
 */
/**
 * The sticky handle actually sent to the provider, scoped by region.
 *
 * Verified against a live endpoint: a sticky session is pinned by its handle
 * alone, so reusing one handle across two countries returns the *first*
 * country's exit and silently ignores the second country parameter. Since a
 * caller's natural handle is stable per identity (an assistant, a profile),
 * changing its region would otherwise keep the old exit until the TTL lapsed —
 * config saying one country while traffic left from another, with no error.
 *
 * Scoping the handle by region makes a region change a different session by
 * construction.
 */
function sessionKeyFor(policy: EgressPolicy, region: string): string {
  // Providers encode these usernames as dash-delimited key-value pairs, so a
  // dash *inside* a value terminates it and silently discards everything after
  // — verified live: a handle containing a dash lost both the country and the
  // session-duration parameter, and two regions collapsed onto one exit. Strip
  // anything that could be read as a delimiter, and separate with an
  // underscore, which real accounts already use.
  const base = (policy.sessionKey || 'default').replace(/[^A-Za-z0-9_]/g, '');
  return `${base || 'default'}_${region}`;
}

function renderUsername(template: string, region: string, sessionKey: string): string {
  return template
    .replace(/\{region\}/g, region)
    .replace(/\{REGION\}/g, region.toUpperCase())
    .replace(/\{session\}/g, sessionKey);
}

/** Whether the provider takes the requested region as a per-request parameter. */
function usernameIsGeoTargeted(template: string): boolean {
  return /\{region\}|\{REGION\}/.test(template);
}

/**
 * Refuse a region the configured endpoint cannot actually exit from.
 *
 * A geo-targeted username carries the region to the provider, so any supported
 * region is servable. A **dedicated-IP** endpoint carries nothing: its exit is
 * fixed, and a request for some other region would otherwise resolve to that
 * one fixed address while `contextOptions` reported the region that was asked
 * for. That is not a degraded proxy, it is an actively contradictory browser —
 * a London address insisting it is in New York — which is worse than no proxy
 * at all and, being config-shaped rather than error-shaped, surfaces nowhere.
 *
 * So a non-templated username must declare its coverage, and anything outside
 * that declaration fails closed like every other unmeetable policy here.
 */
function assertRegionServable(env: EgressEnv, template: string, region: string): void {
  if (usernameIsGeoTargeted(template)) return;
  const declared = (env.UNIFY_EGRESS_PROXY_REGIONS || '')
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
  if (!declared.length) {
    throw new EgressPolicyError(
      'managed egress username carries no {region} placeholder, so the exit is fixed; ' +
        'set UNIFY_EGRESS_PROXY_REGIONS to the regions it serves. Refusing to claim ' +
        `region ${r_safe(region)} on an endpoint of unknown geography`,
    );
  }
  if (!declared.includes(region)) {
    throw new EgressPolicyError(
      `configured egress endpoint serves ${declared.join(', ')}, not ${r_safe(region)}; ` +
        'refusing to exit from one region while reporting another',
    );
  }
}

export function parseEgressPolicy(raw: unknown): EgressPolicy | null {
  if (raw === undefined || raw === null) return null;
  if (typeof raw !== 'object') {
    throw new EgressPolicyError('egress must be an object');
  }
  const value = raw as Record<string, unknown>;
  const mode = String(value.mode ?? 'direct') as EgressMode;
  if (!['direct', 'region', 'byo'].includes(mode)) {
    throw new EgressPolicyError(
      `unknown egress mode ${r_safe(mode)}; expected direct | region | byo`,
    );
  }
  const policy: EgressPolicy = { mode };
  if (value.region !== undefined) policy.region = String(value.region).toLowerCase();
  if (value.sessionKey !== undefined) policy.sessionKey = String(value.sessionKey);
  if (value.proxy !== undefined) {
    const proxy = value.proxy as Record<string, unknown>;
    if (!proxy || typeof proxy !== 'object' || !proxy.server) {
      throw new EgressPolicyError('egress.proxy requires a server');
    }
    policy.proxy = {
      server: String(proxy.server),
      username: proxy.username === undefined ? undefined : String(proxy.username),
      password: proxy.password === undefined ? undefined : String(proxy.password),
      bypass: proxy.bypass === undefined ? undefined : String(proxy.bypass),
    };
  }
  return policy;
}

// Small helper so the error message quotes the offending value without
// pulling in a formatting dependency.
function r_safe(value: unknown): string {
  return JSON.stringify(String(value));
}

/**
 * Turn a policy into browser options, or throw.
 *
 * `direct` (and an absent policy) resolves to no proxy and no derived context
 * — the existing behaviour, unchanged.
 */
export function resolveEgress(
  policy: EgressPolicy | null | undefined,
  env: EgressEnv = process.env,
): ResolvedEgress {
  const none: ResolvedEgress = { args: [], contextOptions: {}, description: 'direct' };
  if (!policy || policy.mode === 'direct') return none;

  let proxy: ProxyConfig;
  let region: string | undefined;

  if (policy.mode === 'byo') {
    if (!policy.proxy?.server) {
      throw new EgressPolicyError(
        'egress mode "byo" requires proxy.server; refusing to fall back to direct egress',
      );
    }
    proxy = policy.proxy;
    region = policy.region;
  } else {
    region = policy.region;
    if (!region) {
      throw new EgressPolicyError('egress mode "region" requires a region');
    }
    if (!REGION_PROFILES[region]) {
      throw new EgressPolicyError(
        `unsupported egress region ${r_safe(region)}; supported: ${supportedRegions().join(', ')}`,
      );
    }
    const server = (env.UNIFY_EGRESS_PROXY_SERVER || '').trim();
    if (!server) {
      throw new EgressPolicyError(
        'no managed egress provider configured (UNIFY_EGRESS_PROXY_SERVER); ' +
          'refusing to fall back to direct egress',
      );
    }
    const usernameTemplate = (env.UNIFY_EGRESS_PROXY_USERNAME || '').trim();
    assertRegionServable(env, usernameTemplate, region);
    proxy = {
      server,
      username: usernameTemplate
        ? renderUsername(usernameTemplate, region, sessionKeyFor(policy, region))
        : undefined,
      password: env.UNIFY_EGRESS_PROXY_PASSWORD || undefined,
    };
  }

  // A region is optional for byo — but when one is given, the derived context
  // must follow it, since that is the whole point of binding them together.
  const contextOptions: ResolvedEgress['contextOptions'] = {};
  if (region) {
    const profile = REGION_PROFILES[region];
    if (!profile) {
      throw new EgressPolicyError(
        `unsupported egress region ${r_safe(region)}; supported: ${supportedRegions().join(', ')}`,
      );
    }
    contextOptions.timezoneId = profile.timezoneId;
    contextOptions.locale = profile.locale;
    contextOptions.extraHTTPHeaders = { 'Accept-Language': acceptLanguage(profile.locale) };
  }

  return {
    args: [...WEBRTC_CONTAINMENT_ARGS],
    proxy,
    contextOptions,
    // Host only — never the credential.
    description: `${policy.mode}${region ? `:${region}` : ''} via ${safeHost(proxy.server)}`,
  };
}

/** Host:port of a proxy URL, with any embedded credentials stripped. */
export function safeHost(server: string): string {
  try {
    const url = new URL(server.includes('://') ? server : `http://${server}`);
    return url.host;
  } catch {
    return '(unparseable)';
  }
}
