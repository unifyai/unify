/**
 * Model-aware observation scaling for native display screenshots.
 *
 * LLM planners emit pixel coordinates in the observation image space we send
 * them. We downscale display captures to that space, then scale coordinates
 * back up before xdotool. Policy is resolved from the configured model so
 * web-vm/desktop stay aligned with the Playwright path and provider limits.
 *
 * The authoritative policy constants live in the shared JSON file
 * ``unify/common/observation_scaling_policy.json``; both this module and the
 * Python ``unify.common.observation_scaling`` read from that single source.
 */

import fs from 'fs';
import path from 'path';

export interface AspectTarget {
  width: number;
  height: number;
}

interface PolicyJson {
  defaultAspectTargets: AspectTarget[];
  providerMaxEdge: Record<string, number>;
  modelObservationMaxEdge: Record<string, number>;
}

function loadPolicy(): PolicyJson {
  const candidates = [
    path.resolve(__dirname, '../../unify/common/observation_scaling_policy.json'),
    path.resolve(__dirname, '../unify/common/observation_scaling_policy.json'),
    path.resolve('/app/unify/common/observation_scaling_policy.json'),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return JSON.parse(fs.readFileSync(candidate, 'utf-8'));
    }
  }
  throw new Error(
    `observation_scaling_policy.json not found. Searched:\n${candidates.join('\n')}`
  );
}

let _cachedPolicy: PolicyJson | null = null;

function getPolicy(): PolicyJson {
  if (!_cachedPolicy) {
    _cachedPolicy = loadPolicy();
  }
  return _cachedPolicy;
}

/** Same targets as magnitude-core WebHarness (Anthropic computer-use reference). */
export function getDefaultAspectTargets(): AspectTarget[] {
  return getPolicy().defaultAspectTargets;
}

export function getProviderMaxEdge(): Record<string, number> {
  return getPolicy().providerMaxEdge;
}

export function getModelObservationMaxEdge(): Record<string, number> {
  return getPolicy().modelObservationMaxEdge;
}

export interface ObservationScalingPolicy {
  /** Aspect-ratio targets tried before falling back to max-edge scaling. */
  aspectTargets: AspectTarget[];
  /** Hard cap on the longest observation edge for this model/provider. */
  maxEdge: number;
  /** Model id used to resolve this policy (for logs). */
  model: string;
  provider: string;
}

export interface NativeObservationScale {
  displayWidth: number;
  displayHeight: number;
  observationWidth: number;
  observationHeight: number;
  model: string;
  provider: string;
}

/** Parse ``name@provider`` or infer provider from model name. */
export function parseModelProvider(model: string): string {
  const trimmed = model.trim().toLowerCase();
  const at = trimmed.lastIndexOf('@');
  if (at >= 0) {
    return trimmed.slice(at + 1);
  }
  if (trimmed.includes('claude')) return 'anthropic';
  if (trimmed.includes('gpt')) return 'openai';
  if (trimmed.includes('gemini')) return 'google';
  if (trimmed.includes('minimax')) return 'minimax';
  return 'default';
}

function resolveMaxEdge(model: string, provider: string): number {
  const policy = getPolicy();
  const normalized = model.trim().toLowerCase();
  if (policy.modelObservationMaxEdge[normalized] !== undefined) {
    return policy.modelObservationMaxEdge[normalized];
  }
  const envOverride = process.env.UNITY_OBSERVATION_MAX_EDGE?.trim();
  if (envOverride) {
    const parsed = Number(envOverride);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }
  return policy.providerMaxEdge[provider] ?? policy.providerMaxEdge.default;
}

export function resolveObservationScalingPolicy(model: string): ObservationScalingPolicy {
  const provider = parseModelProvider(model);
  return {
    aspectTargets: getDefaultAspectTargets(),
    maxEdge: resolveMaxEdge(model, provider),
    model: model.trim(),
    provider,
  };
}

/**
 * Pick the closest aspect-ratio target for a display size (matches WebHarness).
 * Only scales down; returns null when no target fits.
 */
export function pickAspectTarget(
  displayWidth: number,
  displayHeight: number,
  targets: AspectTarget[],
): AspectTarget | null {
  const ratio = displayWidth / displayHeight;
  for (const target of targets) {
    if (Math.abs(target.width / target.height - ratio) < 0.02) {
      if (target.width < displayWidth) {
        return target;
      }
    }
  }
  return null;
}

function capDimensionsToMaxEdge(
  width: number,
  height: number,
  maxEdge: number,
): { width: number; height: number } {
  const longest = Math.max(width, height);
  if (longest <= maxEdge) {
    return { width, height };
  }
  const factor = maxEdge / longest;
  return {
    width: Math.round(width * factor),
    height: Math.round(height * factor),
  };
}

export function computeNativeObservationScale(
  displayWidth: number,
  displayHeight: number,
  policy: ObservationScalingPolicy,
): NativeObservationScale {
  const aspect = pickAspectTarget(displayWidth, displayHeight, policy.aspectTargets);
  let observationWidth = displayWidth;
  let observationHeight = displayHeight;

  if (aspect) {
    observationWidth = aspect.width;
    observationHeight = aspect.height;
  } else {
    const maxEdge = Math.max(displayWidth, displayHeight);
    if (maxEdge > policy.maxEdge) {
      const factor = policy.maxEdge / maxEdge;
      observationWidth = Math.round(displayWidth * factor);
      observationHeight = Math.round(displayHeight * factor);
    }
  }

  const capped = capDimensionsToMaxEdge(observationWidth, observationHeight, policy.maxEdge);
  observationWidth = capped.width;
  observationHeight = capped.height;

  return {
    displayWidth,
    displayHeight,
    observationWidth,
    observationHeight,
    model: policy.model,
    provider: policy.provider,
  };
}

export function scaleObservationCoordsToDisplay(
  x: number,
  y: number,
  scale: NativeObservationScale,
): { x: number; y: number } {
  if (
    scale.observationWidth === scale.displayWidth
    && scale.observationHeight === scale.displayHeight
  ) {
    return { x, y };
  }
  return {
    x: Math.round(x * (scale.displayWidth / scale.observationWidth)),
    y: Math.round(y * (scale.displayHeight / scale.observationHeight)),
  };
}
