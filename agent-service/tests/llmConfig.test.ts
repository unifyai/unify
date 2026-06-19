import { strict as assert } from "node:assert";
import {
  getLlmConfig,
  resolveAgentServiceModel,
  resolveUnillmBaseUrl,
} from "../src/llmConfig";

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

run("uses explicit DROID_UNILLM_URL as the proxy base URL", () => {
  const config = getLlmConfig({
    DROID_UNILLM_URL: "https://gateway.example.com/unillm/",
    UNIFY_KEY: "uk-test",
  });

  assert.strictEqual(config.provider, "openai-generic");
  assert.strictEqual(config.options.baseUrl, "https://gateway.example.com/unillm");
  assert.strictEqual(config.options.model, "deepseek-v4-max@deepseek");
  assert.deepStrictEqual(config.options.headers, {
    Authorization: "Bearer uk-test",
  });
});

run("derives the proxy URL from DROID_COMMS_URL", () => {
  assert.strictEqual(
    resolveUnillmBaseUrl({ DROID_COMMS_URL: "https://comms.example.com/" }),
    "https://comms.example.com/unillm",
  );
});

run("derives the proxy URL from DROID_GATEWAY_URL", () => {
  assert.strictEqual(
    resolveUnillmBaseUrl({ DROID_GATEWAY_URL: "http://localhost:8080/" }),
    "http://localhost:8080/unillm",
  );
});

run("prefers UNIFY_MODEL over the Droid production default", () => {
  assert.strictEqual(
    resolveAgentServiceModel({ UNIFY_MODEL: "gpt-5@openai" }),
    "gpt-5@openai",
  );
});

run("allows the agent-service model to be overridden without changing routing", () => {
  const config = getLlmConfig({
    DROID_UNILLM_URL: "https://gateway.example.com/unillm",
    DROID_AGENT_SERVICE_LLM_MODEL: "gpt-5.5@openai",
    UNIFY_KEY: "uk-test",
  });

  assert.strictEqual(config.options.model, "gpt-5.5@openai");
  assert.strictEqual(config.options.baseUrl, "https://gateway.example.com/unillm");
});

run("ignores raw provider keys when a UniLLM proxy is configured", () => {
  const config = getLlmConfig({
    DROID_COMMS_URL: "https://comms.example.com",
    UNIFY_KEY: "uk-test",
    [["ANTHROPIC", "API", "KEY"].join("_")]: "unused-anthropic-provider-credential",
    [["OPENAI", "API", "KEY"].join("_")]: "unused-openai-provider-credential",
  } as any);

  assert.strictEqual(config.options.baseUrl, "https://comms.example.com/unillm");
  assert.strictEqual(config.options.headers.Authorization, "Bearer uk-test");
  assert.notStrictEqual(config.options.baseUrl, "https://api.anthropic.com/v1");
  assert.notStrictEqual(config.options.baseUrl, "https://api.openai.com/v1");
});

run("fails closed when only raw provider keys are configured", () => {
  assert.throws(
    () => getLlmConfig({
      UNIFY_KEY: "uk-test",
      [["ANTHROPIC", "API", "KEY"].join("_")]: "anthropic-direct-credential",
      [["OPENAI", "API", "KEY"].join("_")]: "openai-direct-credential",
    } as any),
    /Direct provider API fallbacks are disabled/,
  );
});

run("requires UNIFY_KEY for proxy authentication", () => {
  assert.throws(
    () => getLlmConfig({ DROID_UNILLM_URL: "https://gateway.example.com/unillm" }),
    /UNIFY_KEY is required/,
  );
});
