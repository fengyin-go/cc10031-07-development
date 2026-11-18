<template>
  <section class="page config-page" data-testid="source-config-workbench">
    <header class="page-head">
      <div>
        <h2>配置版本快照</h2>
        <p class="page-desc">
          页面模型发布后冻结源端配置版本；采集批次与配置版本同时绑定，模型再次发布不会覆盖已开始批次。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-configs" type="button" @click="reloadAll">
          刷新台账
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in statCards" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reloadAll">
      <label class="filter-item">
        <span>站点名称</span>
        <input v-model="filters.site" data-testid="filter-site" placeholder="按站点名称检索" />
      </label>
      <label class="filter-item">
        <span>版本</span>
        <input v-model="filters.version" data-testid="filter-version" placeholder="按版本检索" />
      </label>
      <label class="filter-item">
        <span>排序字段</span>
        <select v-model="sortBy" data-testid="sort-by">
          <option value="snapshot_id">创建顺序</option>
          <option value="created_at">创建时间</option>
          <option value="site">站点名称</option>
          <option value="version">配置版本</option>
          <option value="batch_count">绑定批次数</option>
        </select>
      </label>
      <label class="filter-item">
        <span>排序方向</span>
        <select v-model="sortOrder" data-testid="sort-order">
          <option value="desc">降序</option>
          <option value="asc">升序</option>
        </select>
      </label>
      <button class="btn primary" data-testid="query-configs" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset-configs" type="button" @click="resetFilters">
        重置条件
      </button>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="config-error">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>配置快照</th>
          <th>站点名称</th>
          <th>模型版本</th>
          <th>配置版本</th>
          <th>处理项</th>
          <th>处理终态</th>
          <th>字段映射</th>
          <th>已绑定批次</th>
          <th>可承接新批次</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in snapshots"
          :key="row.snapshot_id"
          :data-testid="`config-row-${row.snapshot_id}`"
          :data-terminal="row.terminal_state"
        >
          <td>{{ row.snapshot_ref }}</td>
          <td>{{ row.target_site }}</td>
          <td>{{ row.model_version }}</td>
          <td>{{ row.snapshot_version }}</td>
          <td>{{ row.processing_item }}</td>
          <td>
            <span :class="['terminal-tag', `terminal-${row.terminal_state}`]">
              {{ row.terminal_state }}·{{ row.terminal_state_label }}
            </span>
          </td>
          <td>
            <span class="mapping-summary">{{ fieldSummary(row) }}</span>
          </td>
          <td>{{ row.bound_batches.length }}</td>
          <td>{{ row.accepting_new_batches ? '是' : '否（已冻结）' }}</td>
        </tr>
        <tr v-if="!snapshots.length">
          <td colspan="9" class="empty-state">暂无配置版本，先到页面建模发布模型</td>
        </tr>
      </tbody>
    </table>

    <p class="page-foot">
      <span>共 {{ snapshotTotal }} 个配置版本，第 {{ page }} 页</span>
      <span class="pager">
        <button class="link" type="button" :disabled="page <= 1" @click="changePage(-1)">上一页</button>
        <button class="link" type="button" :disabled="page * size >= snapshotTotal" @click="changePage(1)">
          下一页
        </button>
      </span>
    </p>

    <h3 class="section-title">采集批次 × 配置版本绑定核对</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>采集批次</th>
          <th>站点名称</th>
          <th>配置版本</th>
          <th>字段映射（快照口径）</th>
          <th>处理终态</th>
          <th>待办</th>
          <th>冲突处理</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in bindings" :key="`${row.batch_ref}-${row.snapshot_id}`" :data-testid="`binding-${row.batch_ref}`">
          <td>{{ row.batch_ref }}</td>
          <td>{{ row.site }}</td>
          <td>{{ row.snapshot_ref }} / {{ row.snapshot_version }}</td>
          <td>
            <span v-for="item in row.field_mapping" :key="item.field" class="field-chip">
              {{ item.field }}{{ item.inferred ? '（推断）' : '' }}
            </span>
            <span v-if="!row.field_mapping.length" class="muted">未写入明细</span>
          </td>
          <td>
            <span :class="['terminal-tag', `terminal-${row.terminal_state}`]">
              {{ row.terminal_state }}·{{ row.terminal_state_label }}
            </span>
          </td>
          <td>{{ row.todo || '—' }}</td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`conflict-continue-${row.batch_ref}`"
              @click="resolve(row.batch_ref, 'continue')"
            >
              沿用旧快照
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`conflict-abort-${row.batch_ref}`"
              @click="resolve(row.batch_ref, 'abort')"
            >
              中止并重启
            </button>
          </td>
        </tr>
        <tr v-if="!bindings.length">
          <td colspan="7" class="empty-state">暂无批次绑定配置版本</td>
        </tr>
      </tbody>
    </table>

    <p v-if="conflictMessage" class="status-legend" data-testid="conflict-message">{{ conflictMessage }}</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  listBatchBindings,
  listSourceConfigs,
  resolveConfigConflict,
  type BatchBindingRow,
  type SourceConfigSnapshotRow,
} from '@/api/backend-service'

const filters = ref({ site: '', version: '' })
const sortBy = ref('snapshot_id')
const sortOrder = ref('desc')
const page = ref(1)
const size = 10
const snapshots = ref<SourceConfigSnapshotRow[]>([])
const snapshotTotal = ref(0)
const bindings = ref<BatchBindingRow[]>([])
const errorMessage = ref('')
const conflictMessage = ref('')
const statCards = ref<{ label: string; value: number }[]>([])

function fieldSummary(row: SourceConfigSnapshotRow): string {
  const fields = row.inferred_fields.slice(0, 4).join('、')
  const suffix = row.inferred_fields.length > 4 ? ` 等${row.inferred_fields.length}项` : ''
  return fields ? `${fields}${suffix}` : '未推断出字段'
}

async function reloadSnapshots() {
  const payload = await listSourceConfigs(
    { site: filters.value.site, version: filters.value.version },
    page.value,
    size,
  )
  snapshots.value = payload.items
  snapshotTotal.value = payload.total
  statCards.value = [
    { label: '配置版本总数', value: payload.total },
    { label: '已绑定批次', value: payload.items.filter((row) => row.bound_batches.length > 0).length },
    { label: '冲突终止', value: payload.items.filter((row) => row.terminal_state === 7).length },
  ]
}

async function reloadBindings() {
  const payload = await listBatchBindings(
    { site: filters.value.site, version: filters.value.version },
    1,
    50,
  )
  bindings.value = payload.items
}

async function reloadAll() {
  errorMessage.value = ''
  conflictMessage.value = ''
  try {
    await Promise.all([reloadSnapshots(), reloadBindings()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '配置版本读取失败'
  }
}

function changePage(delta: number) {
  if (page.value + delta < 1) return
  page.value += delta
  void reloadSnapshots().catch((error: unknown) => {
    errorMessage.value = error instanceof Error ? error.message : '配置版本读取失败'
  })
}

function resetFilters() {
  filters.value = { site: '', version: '' }
  page.value = 1
  void reloadAll()
}

async function resolve(batchRef: string, strategy: 'continue' | 'abort') {
  conflictMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await resolveConfigConflict(batchRef, strategy)
    if (!result.ok) throw new Error(result.message || '冲突处理失败')
    conflictMessage.value = result.reused
      ? `批次 ${batchRef} 冲突已处理过，本次未重复写入（只保留一条结果）`
      : `批次 ${batchRef} 已按「${strategy === 'continue' ? '沿用旧快照' : '中止并重启'}」处理，冲突落到 7 号处理终态`
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '冲突处理失败'
  }
}

onMounted(reloadAll)
</script>

<style scoped>
.section-title { font-size: 15px; margin: 22px 0 10px; }
.terminal-tag { font-size: 12px; padding: 1px 6px; border-radius: 10px; background: #eef2f7; }
.terminal-7 { background: #fee4e2; color: #b42318; }
.terminal-3, .terminal-4 { background: #dcfae6; color: #067647; }
.terminal-5 { background: #f2f4f7; color: #475467; }
.field-chip { display: inline-block; font-size: 12px; padding: 0 6px; margin: 0 4px 2px 0; background: #eff6ff; border-radius: 4px; }
.mapping-summary { font-size: 12px; }
.muted { color: var(--muted); }
.pager { display: flex; gap: 12px; }
</style>
