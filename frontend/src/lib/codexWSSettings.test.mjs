import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

import { buildWritableSettingsPayload } from './settingsPayload.ts'

const settings = readFileSync(new URL('../pages/Settings.tsx', import.meta.url), 'utf8')

for (const [field, maximum] of [
  ['codex_ws_downstream_keepalive_slots', 128],
  ['codex_ws_stateless_slots', 32],
]) {
  test(`${field} has independent input bounds and preserves the default`, () => {
    const inputs = [...settings.matchAll(/<DraftNumberInput\b[\s\S]*?\/>/g)]
      .map(([input]) => input)
      .filter((input) => input.includes(`value={settingsForm.${field}}`))
    assert.equal(inputs.length, 1)
    assert.match(inputs[0], /min=\{0\}/)
    assert.match(inputs[0], new RegExp(`max=\\{${maximum}\\}`))
    assert.match(inputs[0], /emptyValue=\{8\}/)
    assert.match(inputs[0], new RegExp(`autoSaveSettingsPatch\\(\\{\\s*${field}: value`))
    assert.match(settings, new RegExp(`${field}: 8,`))
  })
}

for (const locale of ['zh', 'en']) {
  test(`${locale} documents distinct chat and blank retention limits`, () => {
    const { settings: labels } = JSON.parse(readFileSync(new URL(`../locales/${locale}.json`, import.meta.url), 'utf8'))
    assert.match(labels.codexWSDownstreamKeepaliveSlotsDesc, /0[～–]128/)
    assert.match(labels.codexWSStatelessSlotsDesc, /0[～–]32/)
  })
}

test('retention payload keeps extended values, zero and partial updates', () => {
  for (const chat of [0, 8, 32, 48, 64, 96, 128]) {
    const settings = Object.freeze({ codex_ws_downstream_keepalive_slots: chat, codex_ws_stateless_slots: 32 })
    assert.deepEqual(buildWritableSettingsPayload(settings), settings)
    assert.deepEqual(buildWritableSettingsPayload({ codex_ws_downstream_keepalive_slots: chat }), {
      codex_ws_downstream_keepalive_slots: chat,
    })
  }
})
