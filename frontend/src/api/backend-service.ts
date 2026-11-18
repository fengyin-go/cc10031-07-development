import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 页面模型工作台：源端配置版本 ----

export type SourceConfigSnapshotRow = {
  snapshot_id: number
  snapshot_ref: string
  processing_item: string
  model_id: number
  model_name: string
  target_site: string
  model_version: string
  snapshot_version: string
  page_structure: string
  inferred_fields: string[]
  field_mapping: {
    field: string
    source_path: string
    target_field: string
    data_type: string
    inferred: boolean
  }[]
  status: string
  terminal_state: number
  terminal_state_label: string
  created_at: string
  bound_batches: string[]
  conflict_batches: string[]
  accepting_new_batches: boolean
}

export type BatchBindingRow = {
  batch_ref: string
  site: string
  model_id: number
  model_version: string
  snapshot_id: number
  snapshot_ref: string
  snapshot_version: string
  processing_item: string
  terminal_state: number
  terminal_state_label: string
  batch_status: string | null
  todo: string | null
  field_mapping: {
    field: string
    source_path: string
    target_field: string
    data_type: string
    inferred: boolean
    rule_name: string
  }[]
}

export type SnapshotPage = {
  items: SourceConfigSnapshotRow[]
  total: number
  page: number
  size: number
}

export async function listSourceConfigs(
  filters: { site?: string; version?: string; status?: string; keyword?: string },
  page = 1,
  size = 20,
): Promise<SnapshotPage> {
  const query = new URLSearchParams()
  Object.entries(filters).forEach(([key, value]) => {
    if (value) query.set(key, value)
  })
  query.set('page', String(page))
  query.set('size', String(size))
  const response = await request(`/api/source-configs?${query.toString()}`)
  if (!response.ok) throw new Error('配置版本读取失败')
  return await response.json()
}

export async function listBatchBindings(
  filters: { site?: string; version?: string },
  page = 1,
  size = 20,
): Promise<{ items: BatchBindingRow[]; total: number; page: number; size: number }> {
  const query = new URLSearchParams()
  Object.entries(filters).forEach(([key, value]) => {
    if (value) query.set(key, value)
  })
  query.set('page', String(page))
  query.set('size', String(size))
  const response = await request(`/api/source-configs/bindings?${query.toString()}`)
  if (!response.ok) throw new Error('批次配置绑定读取失败')
  return await response.json()
}

export async function resolveConfigConflict(
  batchRef: string,
  strategy: 'continue' | 'abort',
): Promise<{ ok: boolean; message?: string; reused?: boolean }> {
  const response = await request(
    `/api/source-configs/conflicts/${encodeURIComponent(batchRef)}/resolve`,
    { method: 'POST', body: JSON.stringify({ strategy }) },
  )
  return await response.json()
}

