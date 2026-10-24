import assert from 'node:assert/strict'
import test from 'node:test'

import { buildModelPricingPreview } from './modelPricingPreview.ts'

test('single-tier models still receive a complete billing preview', () => {
  const preview = buildModelPricingPreview({
    input: 2.5,
    cached_input: 0.25,
    output: 15,
  })

  assert.equal(preview.mode, 'single')
  assert.deepEqual(preview.standard, { input: 2.5, cached: 0.25, output: 15 })
  assert.equal(preview.long, null)
  assert.equal(preview.priority, null)
  assert.match(preview.expression, /p \* 2\.5/)
})

test('long-context models expose threshold and both tier rates', () => {
  const preview = buildModelPricingPreview({
    input: 2.5,
    cached_input: 0.25,
    output: 15,
    input_long: 5,
    cached_input_long: 0.5,
    output_long: 22.5,
    long_context_threshold_tokens: 272000,
  })

  assert.equal(preview.mode, 'tiered')
  assert.equal(preview.threshold, 272000)
  assert.deepEqual(preview.long, { input: 5, cached: 0.5, output: 22.5 })
  assert.match(preview.expression, /len < 272000/)
  assert.match(preview.expression, /long_context/)
})

test('priority and flex conventions are represented for every supported tier', () => {
  const preview = buildModelPricingPreview({
    input: 5,
    cached_input: 0.5,
    output: 30,
    input_priority: 10,
    cached_input_priority: 1,
    output_priority: 60,
  })

  assert.deepEqual(preview.priority, { input: 10, cached: 1, output: 60 })
  assert.equal(preview.flexMultiplier, 0.5)
  assert.match(preview.expression, /service_tier.*flex/)
  assert.match(preview.expression, /service_tier.*priority/)
})


test('image pricing preview splits text and image inputs without long-context multipliers', () => {
  const preview = buildModelPricingPreview({ input: 5, cached_input: 1.25, image_input: 8, cached_image_input: 2, output: 30 })
  assert.deepEqual(preview.image, { input: 8, cached: 2, output: 30 })
  assert.match(preview.expression, /text_input \* 5 \+ image_input \* 8/)
  assert.match(preview.expression, /cached_image \* 2 \+ image_output \* 30/)
  assert.equal(preview.long, null)
})

test('service-tier multipliers apply to both context tiers, not only the long one', () => {
  const preview = buildModelPricingPreview({
    input: 10,
    cached_input: 1,
    output: 50,
    input_long: 20,
    cached_input_long: 2,
    output_long: 75,
    long_context_threshold_tokens: 272000,
  })
  assert.equal(
    preview.expression,
    '(len < 272000 ? tier("standard", p * 10 + c * 50 + cr * 1) : tier("long_context", p * 20 + c * 75 + cr * 2)) * (param("service_tier") == "flex" ? 0.5 : 1) * (param("service_tier") == "priority" ? 2 : 1)',
  )
})
