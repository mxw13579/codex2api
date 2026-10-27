import test from 'node:test'
import assert from 'node:assert/strict'
import { api, AdminAPIError } from '../api.ts'
import { intelligenceTargetConflict } from './intelligenceCheck.ts'

test('settings API preserves structured conflicts while retaining legacy error messages', async t => {
  const originalStorage = Object.getOwnPropertyDescriptor(globalThis, 'localStorage')
  Object.defineProperty(globalThis, 'localStorage', {configurable:true, value:{getItem:()=> 'fixture'}})
  t.after(() => {
    if (originalStorage) Object.defineProperty(globalThis, 'localStorage', originalStorage)
    else delete globalThis.localStorage
  })
  const details = {error:'incompatible target', code:'intelligence_check_target_conflict',target_group_id:8,target_group_channel:'codex',conflict_count:1,conflicts:[{account_id:68876,account_name:'Claude',channel:'claude'}]}
  let body = JSON.stringify(details)
  t.mock.method(globalThis, 'fetch', async () => new Response(body, {status:400}))
  await assert.rejects(api.saveIntelligenceSettings({revision:1}), error => {
    assert.ok(error instanceof AdminAPIError)
    assert.equal(error.status,400)
    assert.equal(error.message,'incompatible target')
    assert.deepEqual(intelligenceTargetConflict(error), details)
    return true
  })
  body = 'upstream unavailable'
  await assert.rejects(api.saveIntelligenceSettings({revision:1}), error => {
    assert.equal(error.message,body)
    assert.equal(intelligenceTargetConflict(error),null)
    return true
  })
})
