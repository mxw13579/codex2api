import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

import {
  DEFAULT_BAD_REQUEST_RETRY_SETTINGS,
  buildBadRequestRetryEnabledPatch,
  buildBadRequestRetryMaxRetriesPatch,
  buildBadRequestRetrySwitchAccountPatch,
  createBadRequestRetrySaveQueue,
  omitBadRequestRetrySettings,
  parseBadRequestRetryMaxRetries,
  settleBadRequestRetryMaxRetriesDraft,
} from './badRequestRetrySettings.ts'

test('Bad Request retry defaults are disabled, same account and one extra retry', () => {
  assert.deepEqual(DEFAULT_BAD_REQUEST_RETRY_SETTINGS, {
    bad_request_retry_enabled: false,
    bad_request_retry_max_retries: 1,
    bad_request_retry_switch_account: false,
  })
})

test('extra retry input is bounded and explicit zero never becomes the default', () => {
  for (const [raw, want] of [
    ['', 1], [' ', 1], ['invalid', 1], ['Infinity', 1],
    ['-1', 0], ['0', 0], ['1', 1], ['3.8', 3], ['10', 10], ['11', 10],
  ]) {
    assert.equal(parseBadRequestRetryMaxRetries(raw), want, raw)
    assert.deepEqual(buildBadRequestRetryMaxRetriesPatch(raw), { bad_request_retry_max_retries: want })
  }
})

test('each partial update changes only the edited field and retains disabled preferences', () => {
  assert.deepEqual(buildBadRequestRetryEnabledPatch(true), { bad_request_retry_enabled: true })
  assert.deepEqual(buildBadRequestRetryEnabledPatch(false), { bad_request_retry_enabled: false })
  assert.deepEqual(buildBadRequestRetrySwitchAccountPatch(true), { bad_request_retry_switch_account: true })
  assert.deepEqual(buildBadRequestRetrySwitchAccountPatch(false), { bad_request_retry_switch_account: false })
  const preferences = {
    ...DEFAULT_BAD_REQUEST_RETRY_SETTINGS,
    bad_request_retry_max_retries: 0,
    bad_request_retry_switch_account: true,
  }
  assert.deepEqual({ ...preferences, ...buildBadRequestRetryEnabledPatch(false) }, preferences)
})

test('ordinary full settings payload cannot replay a stale independent policy', () => {
  const settings = {
    ...DEFAULT_BAD_REQUEST_RETRY_SETTINGS,
    max_retries: 5,
    max_rate_limit_retries: 7,
    continuous_retry_enabled: true,
    site_name: 'synthetic settings',
  }
  const before = { ...settings }
  assert.deepEqual(omitBadRequestRetrySettings(settings), {
    max_retries: 5,
    max_rate_limit_retries: 7,
    continuous_retry_enabled: true,
    site_name: 'synthetic settings',
  })
  assert.deepEqual(settings, before, 'payload filtering must not mutate the form')
})

test('policy save queue preserves action order and recovers after a failed save', async () => {
  const enqueue = createBadRequestRetrySaveQueue()
  const events = []
  let release
  const gate = new Promise((resolve) => { release = resolve })
  const first = enqueue(async () => {
    events.push('first:start')
    await gate
    events.push('first:failed')
    throw new Error('synthetic save failure')
  })
  const rejected = assert.rejects(first, /synthetic save failure/)
  const second = enqueue(async () => {
    events.push('second')
    return 0
  })
  await Promise.resolve()
  assert.deepEqual(events, ['first:start'])
  release()
  await rejected
  assert.equal(await second, 0)
  assert.deepEqual(events, ['first:start', 'first:failed', 'second'])
})

// Headless form fixture: use the production queue and draft reconciliation.
// The deferred response stands in for autoSaveSettingsPatch's commit/rollback.
function budgetDraftFixture() {
  const enqueue = createBadRequestRetrySaveQueue()
  let formValue = 1
  let draft = null
  return {
    enqueue,
    edit(value) { draft = { value } },
    sync(value) { formValue = value },
    get value() { return draft?.value ?? String(formValue) },
    get formValue() { return formValue },
    save(response) {
      const submitted = draft
      const patch = buildBadRequestRetryMaxRetriesPatch(draft?.value ?? String(formValue))
      return enqueue(async () => {
        const previous = formValue
        formValue = patch.bad_request_retry_max_retries
        try {
          formValue = await response
        } catch {
          formValue = previous
        }
        draft = settleBadRequestRetryMaxRetriesDraft(draft, submitted)
      })
    },
  }
}

for (const outcome of ['success', 'failure']) {
  test(`saving 3 then typing 7 preserves the new draft on ${outcome}`, async () => {
    const form = budgetDraftFixture()
    const response = Promise.withResolvers()
    form.edit('3')
    const pending = form.save(response.promise)
    await Promise.resolve()
    assert.equal(form.formValue, 3, 'the save has started')
    form.edit('7')
    if (outcome === 'success') response.resolve(3)
    else response.reject(new Error('synthetic save failure'))
    await pending
    assert.equal(form.formValue, outcome === 'success' ? 3 : 1)
    assert.equal(form.value, '7', 'completion/rollback must not overwrite the new draft')
    form.sync(5)
    assert.equal(form.value, '7', 'settings synchronization must not overwrite an edit')
    await form.save(Promise.resolve(7))
    assert.equal(form.value, '7')
    form.sync(8)
    assert.equal(form.value, '8', 'a settled draft follows form updates again')
  })

  test(`unchanged draft follows normalized form or rollback on ${outcome}`, async () => {
    const form = budgetDraftFixture()
    const response = Promise.withResolvers()
    form.edit('3.8')
    const pending = form.save(response.promise)
    await Promise.resolve()
    if (outcome === 'success') response.resolve(3)
    else response.reject(new Error('synthetic save failure'))
    await pending
    assert.equal(form.value, outcome === 'success' ? '3' : '1')
  })
}

test('an unchanged rejected draft resets even if the form value never changed', async () => {
  const form = budgetDraftFixture()
  const response = Promise.withResolvers()
  form.edit('')
  const pending = form.save(response.promise)
  await Promise.resolve()
  assert.equal(form.formValue, 1)
  response.reject(new Error('synthetic save failure'))
  await pending
  assert.equal(form.value, '1')
})

test('queued saves capture the submitted draft before starting, including repeated text', async () => {
  const form = budgetDraftFixture()
  const earlierResponse = Promise.withResolvers()
  const earlier = form.enqueue(() => earlierResponse.promise)
  const firstResponse = Promise.withResolvers()
  const secondResponse = Promise.withResolvers()
  form.edit('3')
  const first = form.save(firstResponse.promise)
  form.edit('7')
  const second = form.save(secondResponse.promise)
  form.edit('3') // Same text as the first save, but a different edit.
  earlierResponse.resolve()
  await earlier
  firstResponse.resolve(3)
  await first
  assert.equal(form.value, '3')
  secondResponse.resolve(7)
  await second
  assert.equal(form.formValue, 7)
  assert.equal(form.value, '3', 'neither queued completion owns the latest edit')
  form.sync(9)
  assert.equal(form.value, '3')
})

test('settings UI wires the independent controls and documents their safety boundary', () => {
  const source = readFileSync(new URL('../pages/Settings.tsx', import.meta.url), 'utf8')
  assert.match(source, /omitBadRequestRetrySettings\(buildWritableSettingsPayload\(normalized\)\)/)
  assert.match(source, /autoSaveBadRequestRetryPatch\(buildBadRequestRetryEnabledPatch\(checked\)\)/)
  assert.match(source, /autoSaveBadRequestRetryPatch\(buildBadRequestRetrySwitchAccountPatch\(checked\)\)/)
  assert.match(source, /autoSaveBadRequestRetryPatch\(buildBadRequestRetryMaxRetriesPatch\(event.target.value\)\)/)
  assert.match(source, /settleBadRequestRetryMaxRetriesDraft\(current, submittedDraft\)/)
  assert.match(source, /value=\{badRequestRetryMaxRetriesDraft\?\.value \?\? String\(settingsForm.bad_request_retry_max_retries\)\}/)
  for (const text of ['HTTP 400', '{"detail":"Bad Request"}', '明确参数错误或安全错误', '独立的额外重试预算', '0 表示不重试', '保留真实错误日志', '不代表已找到根因', '固定账号请求', '无法安全重放的续链不会绕过约束']) {
    assert.ok(source.includes(text), `missing UI explanation: ${text}`)
  }
})
