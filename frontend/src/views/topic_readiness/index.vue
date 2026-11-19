<template>
  <section class="page" data-module="topic_readiness">
    <header class="page-head">
      <div>
        <h2>主题上线就绪数据包</h2>
        <p class="page-desc">
          按业务场景展示上线就绪包，并列出来源、主题版本与接收端版本；主题配置与接收端版本在同一发布边界一起可见。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh" type="button" @click="reload">刷新就绪包</button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>业务场景</span>
        <select v-model="filters.scenario" data-testid="filter-scenario">
          <option value="">全部场景</option>
          <option v-for="item in scenarios" :key="item.name" :value="item.name">
            {{ item.name }}（{{ item.count }}）
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>来源</span>
        <select v-model="filters.source" data-testid="filter-source">
          <option value="">全部来源</option>
          <option v-for="item in sources" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>状态</span>
        <select v-model="filters.status" data-testid="filter-status">
          <option value="">全部状态</option>
          <option v-for="item in statuses" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>版本排列</span>
        <select v-model="filters.sort" data-testid="filter-sort">
          <option value="version_desc">按版本倒序</option>
          <option value="version_asc">按版本正序</option>
        </select>
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <div class="scenario-board" data-testid="scenario-board">
      <article
        v-for="group in grouped"
        :key="group.name"
        class="scenario-card"
        :data-testid="`scenario-${group.name}`"
      >
        <div class="scenario-head">
          <span class="scenario-name">{{ group.name }}</span>
          <span class="scenario-count">{{ group.items.length }} 份就绪包</span>
        </div>
        <div
          v-for="item in group.items"
          :key="item.id"
          class="readiness-item"
          :class="{ selected: selected?.id === item.id }"
          :data-testid="`package-${item.id}`"
          @click="select(item)"
        >
          <div class="readiness-line">
            <span class="readiness-pkg">{{ item.package_no }}</span>
            <span class="badge" :class="badgeClass(item.status)">{{ item.status }}</span>
          </div>
          <div class="readiness-line">
            <span>
              来源 {{ item.source }} ·
              主题 <span class="version-pill">{{ item.topic_version || '—' }}</span>
              接收端 <span class="version-pill">{{ item.receiver_version || '—' }}</span>
            </span>
            <span>消费组 {{ item.consumer_group || '—' }}</span>
          </div>
          <p class="readiness-overview">{{ item.overview }}</p>
        </div>
      </article>
      <p v-if="!grouped.length && !message" class="empty-state">暂无可展示的上线就绪数据包</p>
    </div>

    <section v-if="selected" class="scenario-card" data-testid="package-detail">
      <div class="scenario-head">
        <span class="scenario-name">{{ selected.package_no }} · {{ selected.package_version }}</span>
        <span class="badge" :class="badgeClass(selected.status)">{{ selected.status }}</span>
      </div>
      <dl class="detail-grid">
        <div><dt>业务场景</dt><dd>{{ selected.business_scenario }}</dd></div>
        <div><dt>来源</dt><dd>{{ selected.source }}</dd></div>
        <div><dt>主题</dt><dd>{{ selected.topic_code }} / {{ selected.topic_name }}</dd></div>
        <div><dt>主题版本</dt><dd data-testid="detail-topic-version">{{ selected.topic_version }}</dd></div>
        <div><dt>接收端版本</dt><dd data-testid="detail-receiver-version">{{ selected.receiver_version }}</dd></div>
        <div><dt>消费组</dt><dd>{{ selected.consumer_group || '—' }}（{{ selected.consumer_status || '—' }}）</dd></div>
        <div><dt>发布边界</dt><dd>{{ selected.release_boundary }}</dd></div>
        <div><dt>版本先后</dt><dd>{{ selected.version_order || '待确认' }}</dd></div>
        <div><dt>可消费</dt><dd>{{ selected.consumable ? '是' : '否' }}</dd></div>
      </dl>

      <div class="batch-row" data-testid="batch-row">
        <span
          v-for="batch in selected.batches"
          :key="batch.batch"
          class="batch-chip"
          :class="batch.status === '已完成' ? 'done' : 'todo'"
        >
          第 {{ batch.batch }} 批 · {{ batch.label }} · {{ batch.status }}
        </span>
      </div>

      <div
        v-if="selected.source === '补报' && selected.conflict && !selected.version_order"
        class="compare-grid"
        data-testid="conflict-box"
      >
        <div>
          <p class="readiness-overview">{{ selected.conflict.summary }}</p>
          <div class="page-actions">
            <button class="btn" data-testid="resolve-topic-first" type="button" @click="resolve('主题优先')">主题优先</button>
            <button class="btn" data-testid="resolve-receiver-first" type="button" @click="resolve('接收端优先')">接收端优先</button>
          </div>
        </div>
        <div>
          <p class="readiness-overview">
            上一份可消费包 {{ selected.conflict.baseline_package_no }}：
            主题 {{ selected.conflict.baseline_topic_version }} / 接收端 {{ selected.conflict.baseline_receiver_version }}。
            无论选哪种先后，同一主题只保留一份可消费就绪包。
          </p>
        </div>
      </div>

      <div v-if="selected.remaining_batches.length" class="page-actions" style="margin-bottom: 12px">
        <button class="btn primary" data-testid="resume" type="button" @click="resume">续做未完成批次</button>
        <span class="readiness-overview">只补齐第 {{ selected.remaining_batches.join('、') }} 批，已完成检查项保持原样。</span>
      </div>

      <table class="data-table" data-testid="trace-table">
        <thead>
          <tr>
            <th>批次</th>
            <th>检查项</th>
            <th>主题版本</th>
            <th>接收端版本</th>
            <th>结论</th>
            <th>说明</th>
            <th>检查时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in selected.trace" :key="String(item.item)">
            <td>{{ batchOf(item.item) }}</td>
            <td>{{ item.item }}</td>
            <td>{{ item.topic_version || '—' }}</td>
            <td>{{ item.receiver_version || '—' }}</td>
            <td>{{ item.status }}</td>
            <td>{{ checkDetail(item.item) }}</td>
            <td>{{ item.checked_at || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 份主题上线就绪数据包</span>
      <span v-if="message" :class="messageIsError ? 'error-text' : ''" data-testid="message">{{ message }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Batch = { batch: number; label: string; status: string }
type Check = { key: string; label: string; batch: number; status: string; detail: string }
type Trace = {
  item: string
  topic_version: string | null
  receiver_version: string | null
  status: string
  checked_at: string | null
}
type Conflict = {
  summary: string
  baseline_package_no: string
  baseline_topic_version: string
  baseline_receiver_version: string
}
type Package = {
  id: number
  package_no: string
  package_version: string
  business_scenario: string
  source: string
  topic_code: string
  topic_name: string
  topic_version: string
  receiver_version: string
  consumer_group: string | null
  consumer_status: string | null
  release_boundary: string
  version_order: string | null
  overview: string
  status: string
  consumable: boolean
  conflict: Conflict | null
  checks: Check[]
  batches: Batch[]
  trace: Trace[]
  remaining_batches: number[]
}
type Scenario = { name: string; count: number }
type QueryPreset = { scenario?: string; source?: string; status?: string; sort?: string; topic?: string }

const ENDPOINT = '/api/topic_readiness'
const sources = ['首次上线', '补报']
const statuses = ['就绪', '校验失败', '检查中断', '冲突待裁决']

const filters = ref<{ scenario: string; source: string; status: string; sort: string }>({
  scenario: '',
  source: '',
  status: '',
  sort: 'version_desc',
})
const packages = ref<Package[]>([])
const scenarios = ref<Scenario[]>([])
const total = ref(0)
const selected = ref<Package | null>(null)
const message = ref('')
const messageIsError = ref(false)

const grouped = computed(() => {
  const groups = new Map<string, Package[]>()
  for (const item of packages.value) {
    const key = item.business_scenario || '未分类'
    groups.set(key, [...(groups.get(key) ?? []), item])
  }
  return [...groups.entries()].map(([name, items]) => ({ name, items }))
})

function badgeClass(status: string) {
  if (status === '就绪') return 'ready'
  if (status === '检查中断') return 'interrupted'
  if (status === '冲突待裁决') return 'conflict'
  return 'failed'
}

function batchOf(label: string) {
  const hit = selected.value?.checks.find((item) => item.label === label)
  return hit ? hit.batch : '—'
}

function checkDetail(label: string) {
  return selected.value?.checks.find((item) => item.label === label)?.detail ?? '—'
}

function presetFromUrl(): QueryPreset {
  const params = new URLSearchParams(window.location.search)
  return {
    scenario: params.get('scenario') ?? undefined,
    source: params.get('source') ?? undefined,
    status: params.get('status') ?? undefined,
    sort: params.get('sort') ?? undefined,
    topic: params.get('topic') ?? undefined,
  }
}

function select(item: Package) {
  selected.value = item
}

function resetFilters() {
  filters.value = { scenario: '', source: '', status: '', sort: 'version_desc' }
  void reload()
}

async function reload() {
  messageIsError.value = false
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(filters.value)) {
    if (value) params.set(key, value)
  }
  try {
    const response = await request(`${ENDPOINT}?${params.toString()}`)
    if (!response.ok) throw new Error('上线就绪数据包读取失败')
    const payload = await response.json()
    packages.value = payload.items ?? []
    total.value = payload.total ?? packages.value.length
    const scenarioResponse = await request(`${ENDPOINT}/scenarios`)
    if (scenarioResponse.ok) {
      const scenarioPayload = await scenarioResponse.json()
      scenarios.value = scenarioPayload.items ?? []
    }
    const preset = presetFromUrl()
    const keepId = selected.value?.id
    const target =
      packages.value.find((item) => item.id === keepId) ??
      packages.value.find((item) => preset.topic && item.topic_code === preset.topic) ??
      packages.value[0] ??
      null
    if (target) await selectPackage(target.id)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '上线就绪数据包读取失败'
    messageIsError.value = true
  }
}

async function selectPackage(entryId: number) {
  const response = await request(`${ENDPOINT}/${entryId}`)
  if (!response.ok) throw new Error('就绪包明细读取失败')
  const detail = (await response.json()) as Package
  selected.value = detail
  packages.value = packages.value.map((item) => (item.id === detail.id ? { ...item, ...detail } : item))
}

async function resume() {
  if (!selected.value) return
  await postAction(`${ENDPOINT}/${selected.value.id}/resume`, {}, '就绪包已续做未完成批次')
}

async function resolve(policy: string) {
  if (!selected.value) return
  await postAction(`${ENDPOINT}/${selected.value.id}/resolve`, { conflict_policy: policy }, `已按「${policy}」记录版本先后`)
}

async function postAction(url: string, body: Record<string, unknown>, fallback: string) {
  message.value = ''
  messageIsError.value = false
  try {
    const response = await request(url, { method: 'POST', body: JSON.stringify(body) })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || fallback)
    message.value = payload.message || fallback
    if (payload.entry?.id) {
      selected.value = payload.entry as Package
      await reload()
    }
  } catch (error) {
    message.value = error instanceof Error ? error.message : fallback
    messageIsError.value = true
  }
}

onMounted(async () => {
  const preset = presetFromUrl()
  filters.value = {
    scenario: preset.scenario ?? '',
    source: preset.source ?? '',
    status: preset.status ?? '',
    sort: preset.sort ?? 'version_desc',
  }
  await reload()
})
</script>
