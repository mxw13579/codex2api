import { useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Cookie, Copy, LoaderCircle } from 'lucide-react'
import { AdminAPIError, api } from '../api'
import type { UpstreamCookieEntry, UsageLog, UsageLogUpstreamCookiesResponse } from '../types'
import { formatBeijingTime } from '../utils/time'
import { Badge } from './ui/badge'
import { Button } from './ui/button'
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from './ui/dialog'

const translationKey = 'usage.upstreamCookies'

export default function UpstreamCookieDetails({ log }: { log: Pick<UsageLog, 'id' | 'channel'> }) {
  // Historical logs may have no channel. Keep their on-demand empty state accessible.
  if (log.channel && log.channel !== 'codex') return null
  return <CookieDetailsDialog key={log.id} logId={log.id} />
}

function CookieDetailsDialog({ logId }: { logId: number }) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="xs"
          aria-label={t(`${translationKey}.open`, { id: logId })}
          title={t(`${translationKey}.open`, { id: logId })}
        >
          <Cookie className="size-3" aria-hidden="true" />
          {t(`${translationKey}.button`)}
        </Button>
      </DialogTrigger>
      <DialogContent className="min-w-0 overflow-x-hidden sm:max-w-4xl" showCloseButton={false}>
        <DialogHeader>
          <DialogTitle>{t(`${translationKey}.title`, { id: logId })}</DialogTitle>
          <DialogDescription>{t(`${translationKey}.description`)}</DialogDescription>
        </DialogHeader>
        {/* Unmount on close: abort the request and discard raw data. Row renders never fetch. */}
        {open && <CookieDetailsContent logId={logId} />}
        <div className="flex justify-end">
          <DialogClose asChild>
            <Button type="button" variant="outline" size="sm">{t('common.close')}</Button>
          </DialogClose>
        </div>
      </DialogContent>
    </Dialog>
  )
}

type LoadState =
  | { status: 'loading' }
  | { status: 'loaded'; data: UsageLogUpstreamCookiesResponse }
  | { status: 'error'; notFound: boolean }

function CookieDetailsContent({ logId }: { logId: number }) {
  const { t } = useTranslation()
  const [state, setState] = useState<LoadState>({ status: 'loading' })
  const [retry, setRetry] = useState(0)

  useEffect(() => {
    const controller = new AbortController()
    let active = true
    setState({ status: 'loading' })
    void api.getUsageLogUpstreamCookies(logId, controller.signal).then((data) => {
      if (active && !controller.signal.aborted) setState({ status: 'loaded', data })
    }).catch((error: unknown) => {
      if (active && !controller.signal.aborted) {
        // Display only fixed messages; upstream error bodies may include raw headers.
        setState({ status: 'error', notFound: error instanceof AdminAPIError && error.status === 404 })
      }
    })
    return () => {
      active = false
      controller.abort()
    }
  }, [logId, retry])

  if (state.status === 'loading') {
    return (
      <div role="status" className="flex items-center justify-center gap-2 py-8 text-sm text-muted-foreground">
        <LoaderCircle className="size-4 animate-spin" aria-hidden="true" />
        {t('common.loading')}
      </div>
    )
  }
  if (state.status === 'error') {
    return (
      <div className="space-y-3 rounded-lg border border-destructive/20 bg-destructive/5 p-4">
        <p role="alert" className="text-sm text-destructive">
          {t(`${translationKey}.${state.notFound ? 'notFound' : 'loadFailed'}`)}
        </p>
        <Button type="button" variant="outline" size="sm" onClick={() => setRetry((value) => value + 1)}>
          {t('common.retry')}
        </Button>
      </div>
    )
  }

  return (
    <div className="min-w-0 space-y-3">
      {state.data.truncated && (
        <p role="status" className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-xs text-amber-700 dark:text-amber-300">
          {t(`${translationKey}.truncated`)}
        </p>
      )}
      {state.data.entries.length === 0 ? (
        <p role="status" className="rounded-lg border border-border bg-muted/20 p-5 text-sm text-muted-foreground">
          {t(`${translationKey}.empty`)}
        </p>
      ) : (
        <>
          <p className="text-xs text-muted-foreground">{t(`${translationKey}.entryCount`, { count: state.data.entries.length })}</p>
          <ol className="space-y-3">
            {state.data.entries.map((entry, index) => (
              <li key={index}><CookieEntry entry={entry} index={index} /></li>
            ))}
          </ol>
        </>
      )}
    </div>
  )
}

function CookieEntry({ entry, index }: { entry: UpstreamCookieEntry; index: number }) {
  const { t } = useTranslation()
  const [expanded, setExpanded] = useState(false)
  // Event names stay visible verbatim; unknown events are never assigned a turn outcome.
  const event = entry.event.toLowerCase()
  const phase = event.includes('handshake') ? 'handshake' : event.includes('turn') ? 'turn' : 'exchange'
  const fields: Array<[string, string | number | undefined]> = [
    ['model', entry.model],
    ['request_id', entry.request_id],
    ['attempt_id', entry.attempt_id],
    ['connection_id', entry.connection_id],
    ['response_id', entry.response_id],
    ['upstream_request_id', entry.upstream_request_id],
    ['cf_ray', entry.cf_ray],
    ['error_code', entry.error_code],
    ['error_type', entry.error_type],
    ['proxy_id', entry.proxy_id],
    ['proxy_name', entry.proxy_name],
  ]

  return (
    <details className="min-w-0 rounded-lg border border-border bg-card/60" onToggle={(event) => setExpanded(event.currentTarget.open)}>
      <summary className="cursor-pointer rounded-lg p-3 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
        <span className="ml-1 font-semibold">#{index + 1} · {t(`${translationKey}.${phase}`)}</span>
        <span className="mt-2 flex flex-wrap items-center gap-1.5">
          <Badge variant="outline" className="max-w-full break-all whitespace-normal">{entry.transport}</Badge>
          <Badge variant="secondary" className="max-w-full break-all whitespace-normal">{entry.event}</Badge>
          {entry.http_status !== undefined && <Badge variant="outline">HTTP {entry.http_status}</Badge>}
          <Badge variant="outline" className="max-w-full break-all whitespace-normal">
            {t(`${translationKey}.status`)}: {entry.status || t(`${translationKey}.notRecorded`)}
          </Badge>
          {entry.error_code && <Badge variant="outline" className="max-w-full break-all whitespace-normal text-destructive">{entry.error_code}</Badge>}
          {entry.cookie_truncated && <Badge variant="outline" className="text-amber-700 dark:text-amber-300">{t(`${translationKey}.entryTruncated`)}</Badge>}
        </span>
        <span className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
          <span>{t('usage.tableAccount')}: #{entry.account_id}</span>
          <time dateTime={entry.time} title={entry.time}>{formatBeijingTime(entry.time)}</time>
        </span>
        <span className="mt-1 block break-all text-xs text-muted-foreground">
          {t(`${translationKey}.unifiedHost`)}: {entry.unified_host || t(`${translationKey}.notRecorded`)}
          {' · '}{t(`${translationKey}.unifiedSource`)}: {entry.unified_source || t(`${translationKey}.notRecorded`)}
        </span>
        {entry.http_status === 101 && <span className="mt-2 block text-xs text-muted-foreground">{t(`${translationKey}.upgradeOnly`)}</span>}
        <span className="mt-2 block text-xs font-medium text-primary">{t(`${translationKey}.expand`)}</span>
      </summary>
      {expanded && (
        <div className="min-w-0 space-y-4 border-t border-border p-3">
          <div className="flex justify-end">
            <CopyValue label={t(`${translationKey}.entryJson`)} getText={() => JSON.stringify(entry, null, 2)} />
          </div>
          <dl className="grid min-w-0 gap-2 text-xs sm:grid-cols-2">
            {fields.map(([label, value]) => value !== undefined && value !== '' ? <EntryField key={label} label={label} value={value} /> : null)}
          </dl>
          {(entry.handshake_cf_ray || entry.handshake_request_id) && (
            <section className="rounded-md border border-border bg-muted/20 p-3">
              <h4 className="mb-2 text-xs font-semibold">{t(`${translationKey}.handshakeContext`)}</h4>
              <dl className="grid min-w-0 gap-2 text-xs sm:grid-cols-2">
                {entry.handshake_cf_ray && <EntryField label="handshake_cf_ray" value={entry.handshake_cf_ray} />}
                {entry.handshake_request_id && <EntryField label="handshake_request_id" value={entry.handshake_request_id} />}
              </dl>
            </section>
          )}
          <RawValues label="Cookie" values={entry.cookie} />
          <RawValues label="Set-Cookie" values={entry.set_cookie} />
          <section className="min-w-0 space-y-2">
            <h4 className="text-xs font-semibold">__oailb</h4>
            {entry.__oailb?.length ? entry.__oailb.map((cookie, cookieIndex) => (
              <div key={cookieIndex} className="min-w-0 space-y-3 rounded-md border border-border p-3">
                <dl className="grid min-w-0 gap-2 text-xs sm:grid-cols-2">
                  <EntryField label={t(`${translationKey}.source`)} value={cookie.source} />
                  {cookie.host !== undefined && <EntryField label="host" value={cookie.host} />}
                  {cookie.iat !== undefined && <EntryField label="iat" value={cookie.iat} />}
                  {cookie.exp !== undefined && <EntryField label="exp" value={cookie.exp} />}
                </dl>
                <RawValues label="__oailb" values={[cookie.value]} />
              </div>
            )) : <p className="text-xs text-muted-foreground">{t(`${translationKey}.notRecorded`)}</p>}
          </section>
        </div>
      )}
    </details>
  )
}

function EntryField({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="min-w-0">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-0.5 break-all font-mono">{value}</dd>
    </div>
  )
}

function RawValues({ label, values }: { label: string; values: string[] }) {
  const { t } = useTranslation()
  return (
    <section className="min-w-0 space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h4 className="text-xs font-semibold">{label}</h4>
        {values.length > 0 && <CopyValue label={label} getText={() => values.join('\n')} />}
      </div>
      {values.length > 0 ? values.map((value, index) => (
        <pre key={index} className="max-h-64 overflow-auto whitespace-pre-wrap rounded-md border border-border bg-muted/30 p-3 font-mono text-xs leading-relaxed [overflow-wrap:anywhere]">
          {value}
        </pre>
      )) : <p className="text-xs text-muted-foreground">{t(`${translationKey}.notRecorded`)}</p>}
    </section>
  )
}

function CopyValue({ label, getText }: { label: string; getText: () => string }) {
  const { t } = useTranslation()
  const [feedback, setFeedback] = useState<'copied' | 'copyFailed' | null>(null)
  const mounted = useRef(false)
  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(getText())
      if (mounted.current) setFeedback('copied')
    } catch {
      if (mounted.current) setFeedback('copyFailed')
    }
  }

  return (
    <span className="inline-flex flex-wrap items-center gap-2">
      <Button type="button" variant="outline" size="xs" onClick={() => void copy()} aria-label={t(`${translationKey}.copy`, { label })}>
        <Copy className="size-3" aria-hidden="true" />
        {t('common.copy')}
      </Button>
      {feedback && <span role="status" className="text-xs text-muted-foreground">{t(`common.${feedback}`)}</span>}
    </span>
  )
}
