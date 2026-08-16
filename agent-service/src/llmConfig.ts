export interface LlmConfigEnv {
  [key: string]: string | undefined;
  UNIFY_UNILLM_URL?: string;
  UNITY_COMMS_URL?: string;
  UNIFY_GATEWAY_URL?: string;
  UNIFY_AGENT_SERVICE_LLM_MODEL?: string;
  UNIFY_KEY?: string;
}

/** Vision-capable default; computer use requires image support. */
const DEFAULT_AGENT_SERVICE_MODEL = 'claude-4.6-sonnet@anthropic';

export function resolveAgentServiceModel(env: LlmConfigEnv = process.env): string {
  return (
    env.UNIFY_AGENT_SERVICE_LLM_MODEL?.trim() ||
    DEFAULT_AGENT_SERVICE_MODEL
  );
}

function cleanUrl(value: string): string {
  return value.trim().replace(/\/+$/, '');
}

function withUnillmPath(baseUrl: string): string {
  return `${cleanUrl(baseUrl)}/unillm`;
}

export function resolveUnillmBaseUrl(env: LlmConfigEnv = process.env): string {
  if (env.UNIFY_UNILLM_URL?.trim()) {
    return cleanUrl(env.UNIFY_UNILLM_URL);
  }
  if (env.UNITY_COMMS_URL?.trim()) {
    return withUnillmPath(env.UNITY_COMMS_URL);
  }
  if (env.UNIFY_GATEWAY_URL?.trim()) {
    return withUnillmPath(env.UNIFY_GATEWAY_URL);
  }
  throw new Error(
    'No UniLLM proxy configured for agent-service. Set UNIFY_UNILLM_URL, ' +
    'UNITY_COMMS_URL, or UNIFY_GATEWAY_URL. Direct provider API fallbacks are disabled.'
  );
}

export function getLlmConfig(env: LlmConfigEnv = process.env): any {
  const apiKey = env.UNIFY_KEY?.trim();
  if (!apiKey) {
    throw new Error('UNIFY_KEY is required for agent-service UniLLM proxy authentication.');
  }

  return {
    provider: 'openai-generic' as const,
    options: {
      model: resolveAgentServiceModel(env),
      baseUrl: resolveUnillmBaseUrl(env),
      headers: {
        'Authorization': `Bearer ${apiKey}`,
      },
      temperature: 0.2,
    },
  };
}
