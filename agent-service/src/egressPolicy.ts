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
  UNITY_EGRESS_PROXY_SERVER?: string;
  /**
   * Username template. Residential providers encode both the geography and
   * the sticky-session handle in the username, so `{region}` and `{session}`
   * are substituted here rather than the caller ever seeing a credential.
   */
  UNITY_EGRESS_PROXY_USERNAME?: string;
  UNITY_EGRESS_PROXY_PASSWORD?: string;
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

function renderUsername(template: string, region: string, sessionKey: string): string {
  return template
    .replace(/\{region\}/g, region)
    .replace(/\{session\}/g, sessionKey);
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
    const server = (env.UNITY_EGRESS_PROXY_SERVER || '').trim();
    if (!server) {
      throw new EgressPolicyError(
        'no managed egress provider configured (UNITY_EGRESS_PROXY_SERVER); ' +
          'refusing to fall back to direct egress',
      );
    }
    const usernameTemplate = (env.UNITY_EGRESS_PROXY_USERNAME || '').trim();
    proxy = {
      server,
      username: usernameTemplate
        ? renderUsername(usernameTemplate, region, policy.sessionKey || 'default')
        : undefined,
      password: env.UNITY_EGRESS_PROXY_PASSWORD || undefined,
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
