import assert from "node:assert/strict";
import test from "node:test";

import { buildWritableSettingsPayload } from "./settingsPayload.ts";

for (const enabled of [false, true]) {
  test(`writable settings payload preserves transient Codex 429 account pause = ${enabled}`, () => {
    const settings = Object.freeze({
      site_name: "CodexProxy",
      codex_transient_429_cooldown_enabled: enabled,
      ignore_usage_limit_status: false,
      max_rate_limit_retries: 1,
      response_cache_config_generation: 7,
    });

    const payload = buildWritableSettingsPayload(settings);

    assert.deepEqual(JSON.parse(JSON.stringify(payload)), {
      site_name: "CodexProxy",
      codex_transient_429_cooldown_enabled: enabled,
      ignore_usage_limit_status: false,
      max_rate_limit_retries: 1,
    });
    assert.equal(settings.codex_transient_429_cooldown_enabled, enabled);
    assert.equal(settings.response_cache_config_generation, 7);
  });
}

for (const enabled of [false, true]) {
  test(`writable settings payload preserves upstream Cookie collection = ${enabled}`, () => {
    const settings = Object.freeze({
      codex_upstream_cookie_log_enabled: enabled,
      usage_log_mode: "errors",
      response_cache_config_generation: 7,
    });

    assert.deepEqual(buildWritableSettingsPayload(settings), {
      codex_upstream_cookie_log_enabled: enabled,
      usage_log_mode: "errors",
    });
    assert.equal(settings.codex_upstream_cookie_log_enabled, enabled);
    assert.deepEqual(buildWritableSettingsPayload({ codex_upstream_cookie_log_enabled: enabled }), {
      codex_upstream_cookie_log_enabled: enabled,
    });
  });
}

test("writable settings payload omits response cache generation regardless of value", () => {
  for (const generation of [7, 0, null, undefined]) {
    const settings = {
      site_name: "CodexProxy",
      response_cache_local_max_bytes: 64 * 1024 * 1024,
      response_cache_local_max_entry_bytes: 8 * 1024 * 1024,
      response_cache_reconstruct_max_bytes: 64 * 1024 * 1024,
      response_cache_config_generation: generation,
      future_setting: "preserved",
      codex_images_main_model: "gpt-5.6-sol",
      codex_images_default_main_model: "gpt-5.6-luna",
      codex_egress: { mode: "resin", resin_enabled: true },
      codex_client_versions: [{ cli_version: "0.158.0-alpha.2.1" }],
    };

    const payload = buildWritableSettingsPayload(settings);

    assert.equal(
      Object.hasOwn(payload, "response_cache_config_generation"),
      false,
    );
    assert.deepEqual(payload, {
      site_name: "CodexProxy",
      response_cache_local_max_bytes: 64 * 1024 * 1024,
      response_cache_local_max_entry_bytes: 8 * 1024 * 1024,
      response_cache_reconstruct_max_bytes: 64 * 1024 * 1024,
      future_setting: "preserved",
      codex_images_main_model: "gpt-5.6-sol",
    });
    assert.equal(
      Object.hasOwn(settings, "response_cache_config_generation"),
      true,
      "the source settings object must not be mutated",
    );
    assert.equal(settings.response_cache_config_generation, generation);
  }
});
