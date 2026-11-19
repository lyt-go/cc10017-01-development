<template>
  <section class="page" data-module="topic_registry">
    <header class="page-head">
      <div>
        <h2>主题注册管理</h2>
        <p class="page-desc">维护实时数据主题的消息结构、分区与保留策略注册，为主题注册提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记数据主题</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出主题注册清单</button>
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
            <button
              class="link"
              type="button"
              :data-testid="`readiness-${row.id}`"
              @click="runReadinessCheck('首次上线', row)"
            >
              上线检查
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`readiness-backfill-${row.id}`"
              @click="runReadinessCheck('补报', row)"
            >
              补报检查
            </button>
            <RouterLink class="link" :to="`/topic_readiness?topic=${row['主题编号']}`">就绪包</RouterLink>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无主题注册数据，可先登记数据主题</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记数据主题</h3>
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
        <span class="scenario-name">上线就绪数据包 · {{ readiness.package_no }}</span>
        <span class="badge" :class="readiness.consumable ? 'ready' : 'interrupted'">{{ readiness.status }}</span>
      </div>
      <dl class="detail-grid">
        <div><dt>主题</dt><dd>{{ readiness.topic_code }}</dd></div>
        <div><dt>主题版本</dt><dd>{{ readiness.topic_version }}</dd></div>
        <div><dt>接收端版本</dt><dd>{{ readiness.receiver_version }}</dd></div>
        <div><dt>消费组</dt><dd>{{ readiness.consumer_group || '—' }}（{{ readiness.consumer_status || '—' }}）</dd></div>
        <div><dt>发布边界</dt><dd>{{ readiness.release_boundary }}</dd></div>
        <div><dt>就绪概览</dt><dd>{{ readiness.overview }}</dd></div>
      </dl>
      <RouterLink class="link" :to="`/topic_readiness?topic=${readiness.topic_code}`">去就绪包视图核对逐项版本</RouterLink>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条主题注册记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span v-else-if="notice" data-testid="readiness-notice">{{ notice }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type ReadinessPackage = {
  id: number
  package_no: string
  topic_code: string
  topic_version: string
  receiver_version: string
  consumer_group: string | null
  consumer_status: string | null
  release_boundary: string
  overview: string
  status: string
  consumable: boolean
}

const ENDPOINT = '/api/topic_registry'
const columns = ["主题编号", "主题名称", "消息格式", "序列化协议", "分区键列", "保留时长", "压缩类型", "Schema版本"]
const actions = ["提交校验", "确认上线", "标记下线"]
const statuses = ["草稿", "待校验", "已上线", "已下线"]
const metricNames = ["数据主题总数", "待校验主题", "已上线主题"]

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
    link.download = 'topic_registry.csv'
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
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('主题注册动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '主题注册操作失败'
  }
}

async function runReadinessCheck(source: string, row: Row) {
  errorMessage.value = ''
  notice.value = ''
  try {
    const response = await request('/api/topic_readiness/check', {
      method: 'POST',
      body: JSON.stringify({ topic_code: row['主题编号'], source }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '上线检查未通过')
    readiness.value = payload.entry ?? null
    notice.value = payload.message
    await reload(row['主题编号'])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '上线检查失败'
  }
}

async function loadReadiness(topicCode: string | number | null) {
  readiness.value = null
  if (!topicCode) return
  try {
    // 先用列表接口确认该主题已有就绪包，避免对尚未检查的主题打 404 明细请求。
    const listResponse = await request(
      `/api/topic_readiness?keyword=${encodeURIComponent(String(topicCode))}&size=50`,
    )
    if (!listResponse.ok) return
    const payload = await listResponse.json()
    const hit = (payload.items ?? []).find((item: ReadinessPackage) => item.topic_code === topicCode)
    if (!hit) return
    const response = await request(`/api/topic_readiness/${hit.id}`)
    if (!response.ok) return
    readiness.value = await response.json()
  } catch {
    readiness.value = null
  }
}

async function reload(focusTopic?: string | number | null) {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('数据主题列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
    await loadReadiness(focusTopic ?? rows.value[0]?.['主题编号'] ?? null)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '主题注册列表读取失败'
  }
}

onMounted(reload)
</script>
