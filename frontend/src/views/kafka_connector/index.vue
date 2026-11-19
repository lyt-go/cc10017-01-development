<template>
  <section class="page" data-module="kafka_connector">
    <header class="page-head">
      <div>
        <h2>消息接入管理</h2>
        <p class="page-desc">维护Kafka消费端的连接、偏移与反序列化配置，为消息接入提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记接入配置</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出消息接入清单</button>
        <RouterLink class="btn" data-testid="open-readiness-board" to="/topic_readiness">查看上线就绪数据包</RouterLink>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload()">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无消息接入数据，可先登记接入配置</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记接入配置</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <section v-if="readiness" class="scenario-card" data-testid="readiness-panel">
      <div class="scenario-head">
        <span class="scenario-name">同一版本的就绪结果 · {{ readiness.package_no }}</span>
        <span class="badge" :class="readiness.consumable ? 'ready' : 'interrupted'">{{ readiness.status }}</span>
      </div>
      <dl class="detail-grid">
        <div><dt>主题</dt><dd>{{ readiness.topic_code }} / {{ readiness.topic_name }}</dd></div>
        <div><dt>主题版本</dt><dd data-testid="readiness-topic-version">{{ readiness.topic_version }}</dd></div>
        <div><dt>接收端版本</dt><dd data-testid="readiness-receiver-version">{{ readiness.receiver_version }}</dd></div>
        <div><dt>消费组</dt><dd>{{ readiness.consumer_group || '—' }}（{{ readiness.consumer_status || '—' }}）</dd></div>
        <div><dt>发布边界</dt><dd>{{ readiness.release_boundary }}</dd></div>
        <div><dt>就绪概览</dt><dd>{{ readiness.overview }}</dd></div>
      </dl>
      <table class="data-table">
        <thead><tr><th>检查项</th><th>主题版本</th><th>接收端版本</th><th>结论</th></tr></thead>
        <tbody>
          <tr v-for="item in readiness.trace" :key="String(item.item)">
            <td>{{ item.item }}</td>
            <td>{{ item.topic_version || '—' }}</td>
            <td>{{ item.receiver_version || '—' }}</td>
            <td>{{ item.status }}</td>
          </tr>
        </tbody>
      </table>
      <p class="page-foot">
        <RouterLink class="link" :to="`/topic_readiness?topic=${readiness.topic_code}`">去上线就绪数据包视图核对</RouterLink>
      </p>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条消息接入记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span v-else-if="notice" data-testid="readiness-notice">{{ notice }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type ReadinessTrace = {
  item: string
  topic_version: string | null
  receiver_version: string | null
  status: string
}
type ReadinessPackage = {
  id: number
  package_no: string
  connector_code: string | null
  topic_code: string
  topic_name: string
  topic_version: string
  receiver_version: string
  consumer_group: string | null
  consumer_status: string | null
  release_boundary: string
  overview: string
  status: string
  consumable: boolean
  trace: ReadinessTrace[]
}

const ENDPOINT = '/api/kafka_connector'
const columns = ["配置编号", "集群地址", "消费组名", "偏移策略", "反序列化器", "批量大小", "超时时间", "死信队列"]
const actions = ["提交校验", "标记生效", "停用配置"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["接入配置总数", "待校验配置", "已生效配置"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const readiness = ref<ReadinessPackage | null>(null)
const notice = ref('')

const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'kafka_connector.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('消息接入动作未生效，请稍后重试')
    }
    const payload = await response.json()
    const packageNo = payload.entry?._readiness_package_no
    notice.value = packageNo ? `检查结果已写入就绪包 ${packageNo}` : ''
    await reload(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '消息接入操作失败'
  }
}

async function loadReadiness(row: Row | null) {
  readiness.value = null
  const connectorCode = row?.['配置编号']
  if (!connectorCode) return
  try {
    // 先用列表接口确认该接入配置已有就绪包，再读同一版本的明细，避免 404。
    const listResponse = await request(
      `/api/topic_readiness?keyword=${encodeURIComponent(String(connectorCode))}&size=50`,
    )
    if (!listResponse.ok) return
    const payload = await listResponse.json()
    const hit = (payload.items ?? []).find(
      (item: ReadinessPackage) => item.connector_code === connectorCode,
    )
    if (!hit) return
    const response = await request(`/api/topic_readiness/${hit.id}`)
    if (!response.ok) return
    readiness.value = await response.json()
  } catch {
    readiness.value = null
  }
}

async function reload(focusRow?: Row) {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('接入配置列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadReadiness(focusRow ?? rows.value[0] ?? null)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '消息接入列表读取失败'
  }
}

onMounted(reload)
</script>
