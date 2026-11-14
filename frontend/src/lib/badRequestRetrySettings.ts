export interface BadRequestRetrySettings {
  bad_request_retry_enabled: boolean
  bad_request_retry_max_retries: number
  bad_request_retry_switch_account: boolean
}

export const DEFAULT_BAD_REQUEST_RETRY_SETTINGS: Readonly<BadRequestRetrySettings> = {
  bad_request_retry_enabled: false,
  bad_request_retry_max_retries: 1,
  bad_request_retry_switch_account: false,
}

export function parseBadRequestRetryMaxRetries(raw: string): number {
  const value = Number(raw)
  if (raw.trim() === '' || !Number.isFinite(value)) return 1
  return Math.min(10, Math.max(0, Math.trunc(value)))
}

export function buildBadRequestRetryEnabledPatch(enabled: boolean) {
  return { bad_request_retry_enabled: enabled }
}

export function buildBadRequestRetrySwitchAccountPatch(switchAccount: boolean) {
  // Changing account preference must not implicitly enable retries.
  return { bad_request_retry_switch_account: switchAccount }
}

export function buildBadRequestRetryMaxRetriesPatch(raw: string) {
  return { bad_request_retry_max_retries: parseBadRequestRetryMaxRetries(raw) }
}

export interface BadRequestRetryMaxRetriesDraft {
  value: string
}

// Each edit has its own identity, even when the user retypes the same value.
// null follows the form; an older save must not clear a newer local draft.
export function settleBadRequestRetryMaxRetriesDraft(
  current: BadRequestRetryMaxRetriesDraft | null,
  submitted: BadRequestRetryMaxRetriesDraft | null,
): BadRequestRetryMaxRetriesDraft | null {
  return current === submitted ? null : current
}

// This policy is saved by its own partial updates, never by a stale full form.
export function omitBadRequestRetrySettings<T extends Partial<BadRequestRetrySettings>>(settings: T): Omit<T, keyof BadRequestRetrySettings> {
  const {
    bad_request_retry_enabled: _enabled,
    bad_request_retry_max_retries: _maxRetries,
    bad_request_retry_switch_account: _switchAccount,
    ...rest
  } = settings
  return rest
}

export function createBadRequestRetrySaveQueue() {
  let tail: Promise<void> = Promise.resolve()
  return function enqueue<T>(task: () => Promise<T>): Promise<T> {
    const result = tail.then(task, task)
    tail = result.then(() => undefined, () => undefined)
    return result
  }
}
