import { useId } from 'react'
import { useTranslation } from 'react-i18next'
import {
  AlertCircle,
  ArrowUpRight,
  Check,
  Clock3,
  CloudDownload,
  FileJson,
  Info,
  Link2,
  Loader2,
  Save,
  ShieldCheck,
  Sparkles,
  Wand2,
} from 'lucide-react'

import ChannelLogo from '@/components/ChannelLogo'
import Modal from '@/components/Modal'
import { Button } from '@/components/ui/button'
import { DialogDescription } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import type { OfficialPricingSyncConfig } from '@/types'
import './pricing-sync-panel.css'

interface PricingSyncPanelProps {
  open: boolean
  onClose: () => void
  syncUrl: string
  onSyncUrlChange: (url: string) => void
  urls: {
    default: string
    litellm: string
    modelsDev: string
    openAI: string
    xAI: string
    claude: string
  }
  config: OfficialPricingSyncConfig
  onConfigChange: (config: OfficialPricingSyncConfig) => void
  syncing: boolean
  officialSyncing: boolean
  officialSaving: boolean
  onSync: () => void
  onSyncOfficial: () => void
  onSaveOfficial: () => void
}

export default function PricingSyncPanel({
  open,
  onClose,
  syncUrl,
  onSyncUrlChange,
  urls,
  config,
  onConfigChange,
  syncing,
  officialSyncing,
  officialSaving,
  onSync,
  onSyncOfficial,
  onSaveOfficial,
}: PricingSyncPanelProps) {
  const { t } = useTranslation()
  const id = useId()
  const noOfficialSources = !config.include_openai && !config.include_grok && !config.include_claude
  const trimmedUrl = syncUrl.trim()
  const activePreset = trimmedUrl === '' || trimmedUrl === urls.default
    ? 'default'
    : urls.litellm && trimmedUrl === urls.litellm
      ? 'litellm'
      : urls.modelsDev && trimmedUrl === urls.modelsDev
        ? 'modelsdev'
        : 'custom'
  const providers = [
    { key: 'include_openai', channel: 'codex', label: 'OpenAI / Codex', source: 'OpenAI', url: urls.openAI },
    { key: 'include_grok', channel: 'grok', label: 'xAI / Grok', source: 'xAI', url: urls.xAI },
    { key: 'include_claude', channel: 'claude', label: 'Anthropic / Claude', source: 'Anthropic', url: urls.claude },
  ] as const

  return (
    <Modal
      show={open}
      onClose={onClose}
      title={
        <span className="pricing-sync-title">
          <span className="pricing-sync-title-icon" aria-hidden="true"><CloudDownload size={19} /></span>
          {t('settings.pricing.syncTitle')}
        </span>
      }
      contentClassName="pricing-sync-modal sm:max-w-[1040px]"
      bodyClassName="pricing-sync-body"
      footer={<Button type="button" variant="outline" onClick={onClose}>{t('common.close')}</Button>}
    >
      <DialogDescription className="pricing-sync-intro">
        {t('settings.pricing.syncSubtitle')}
      </DialogDescription>

      <div className="pricing-sync-layout">
        <section className="pricing-sync-official" aria-labelledby={`${id}-official-title`}>
          <div className="pricing-sync-section-heading">
            <span className="pricing-sync-section-icon" aria-hidden="true"><ShieldCheck size={21} /></span>
            <div>
              <div className="pricing-sync-heading-line">
                <h3 id={`${id}-official-title`}>{t('settings.pricing.officialTitle')}</h3>
                <span className="pricing-sync-authority">{t('settings.pricing.authoritative')}</span>
              </div>
              <p>{t('settings.pricing.officialDesc')}</p>
            </div>
          </div>

          <div className="pricing-sync-providers">
            {providers.map((provider) => (
              <div className="pricing-sync-provider" key={provider.key} data-selected={config[provider.key]}>
                <label className="pricing-sync-provider-label" htmlFor={`${id}-${provider.key}`}>
                  <span className="pricing-sync-provider-logo"><ChannelLogo channel={provider.channel} size={22} /></span>
                  <span>{provider.label}</span>
                </label>
                <div className="pricing-sync-provider-actions">
                  <a
                    href={provider.url}
                    target="_blank"
                    rel="noreferrer"
                    aria-label={`${provider.source} · ${t('settings.pricing.officialTitle')}`}
                    title={`${provider.source} · ${t('settings.pricing.officialTitle')}`}
                  >
                    <ArrowUpRight size={15} aria-hidden="true" />
                  </a>
                  <Switch
                    id={`${id}-${provider.key}`}
                    checked={config[provider.key]}
                    onCheckedChange={(checked) => onConfigChange({ ...config, [provider.key]: checked })}
                  />
                </div>
              </div>
            ))}
          </div>

          <Button
            type="button"
            className="pricing-sync-official-action"
            onClick={onSyncOfficial}
            disabled={officialSyncing || noOfficialSources}
          >
            {officialSyncing ? <Loader2 className="size-4 animate-spin" /> : <CloudDownload className="size-4" />}
            {officialSyncing ? t('settings.pricing.syncing') : t('settings.pricing.officialSyncNow')}
          </Button>

          <div className="pricing-sync-schedule">
            <div className="pricing-sync-schedule-heading">
              <Clock3 size={17} aria-hidden="true" />
              <label htmlFor={`${id}-enabled`}>{t('settings.pricing.autoOfficialSync')}</label>
              <Switch
                id={`${id}-enabled`}
                checked={config.enabled}
                onCheckedChange={(enabled) => onConfigChange({ ...config, enabled })}
                aria-describedby={`${id}-schedule-hint`}
              />
            </div>
            <p id={`${id}-schedule-hint`}>{t('settings.pricing.autoOfficialSyncHint')}</p>
            <div className="pricing-sync-schedule-controls">
              <label htmlFor={`${id}-interval`}>{t('settings.pricing.intervalMinutes')}</label>
              <Input
                id={`${id}-interval`}
                type="number"
                min={60}
                max={10080}
                value={config.interval_minutes}
                onChange={(event) => onConfigChange({ ...config, interval_minutes: Number(event.target.value) })}
              />
              <Button
                type="button"
                variant="outline"
                onClick={onSaveOfficial}
                disabled={officialSaving || noOfficialSources}
              >
                {officialSaving ? <Loader2 className="size-3.5 animate-spin" /> : <Save className="size-3.5" />}
                {t('common.save')}
              </Button>
            </div>
          </div>

          {config.last_success_at || config.last_error || config.last_warning ? (
            <div className="pricing-sync-status">
              {config.last_success_at ? (
                <p className="pricing-sync-status-success">
                  <Check size={14} aria-hidden="true" />
                  <span>{t('settings.pricing.lastOfficialSuccess')}: <time dateTime={config.last_success_at}>{new Date(config.last_success_at).toLocaleString()}</time></span>
                </p>
              ) : null}
              {config.last_error ? (
                <p className="pricing-sync-status-error" role="alert">
                  <AlertCircle size={14} aria-hidden="true" />
                  <span>{config.last_error}</span>
                </p>
              ) : null}
              {config.last_warning ? (
                <p className="pricing-sync-status-warning">
                  <Info size={14} aria-hidden="true" />
                  <span>{t('settings.pricing.lastWarning')}: {config.last_warning}</span>
                </p>
              ) : null}
            </div>
          ) : null}
        </section>

        <section className="pricing-sync-reference" aria-labelledby={`${id}-reference-title`}>
          <div className="pricing-sync-section-heading">
            <span className="pricing-sync-section-icon" aria-hidden="true"><FileJson size={21} /></span>
            <div>
              <h3 id={`${id}-reference-title`}>{t('settings.pricing.referenceTitle')}</h3>
              <span className="pricing-sync-preset-badge">
                {activePreset === 'default'
                  ? t('settings.pricing.presetDefault')
                  : activePreset === 'litellm'
                    ? 'LiteLLM'
                    : activePreset === 'modelsdev'
                      ? 'models.dev'
                      : t('settings.pricing.presetCustom')}
              </span>
            </div>
          </div>

          <div className="pricing-sync-reference-form">
            <label className="pricing-sync-field-label" htmlFor={`${id}-url`}>{t('settings.pricing.syncUrl')}</label>
            <div className="pricing-sync-url-input">
              <Link2 size={15} aria-hidden="true" />
              <Input
                id={`${id}-url`}
                value={syncUrl}
                placeholder={urls.default}
                onChange={(event) => onSyncUrlChange(event.target.value)}
                spellCheck={false}
              />
            </div>

            <div className="pricing-sync-presets" role="group" aria-label={t('settings.pricing.presets')}>
              <span>{t('settings.pricing.presets')}</span>
              <div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  aria-pressed={activePreset === 'default'}
                  onClick={() => onSyncUrlChange('')}
                >
                  <Sparkles className="size-3.5" />
                  {t('settings.pricing.presetDefault')}
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  aria-pressed={activePreset === 'litellm'}
                  onClick={() => onSyncUrlChange(urls.litellm)}
                  disabled={!urls.litellm}
                >
                  <FileJson className="size-3.5" />
                  LiteLLM
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  aria-pressed={activePreset === 'modelsdev'}
                  onClick={() => onSyncUrlChange(urls.modelsDev)}
                  disabled={!urls.modelsDev}
                >
                  <Wand2 className="size-3.5" />
                  models.dev
                </Button>
              </div>
            </div>

            <Button type="button" variant="outline" className="pricing-sync-reference-action" onClick={onSync} disabled={syncing}>
              {syncing ? <Loader2 className="size-4 animate-spin" /> : <CloudDownload className="size-4" />}
              {syncing ? t('settings.pricing.syncing') : t('settings.pricing.syncNow')}
            </Button>
          </div>

          <div className="pricing-sync-reference-note">
            <Info size={16} aria-hidden="true" />
            <p>{t('settings.pricing.hint')}</p>
          </div>
        </section>
      </div>
    </Modal>
  )
}
