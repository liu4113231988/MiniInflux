<script setup>
import { computed, onMounted, ref } from 'vue'

const tabs = [
  { key: 'overview', label: '概览' },
  { key: 'databases', label: '数据库' },
  { key: 'query', label: '查询' },
  { key: 'queries', label: 'CQ' },
  { key: 'tokens', label: '令牌' },
  { key: 'ops', label: '运维' }
]

const activeTab = ref('overview')
const busy = ref(false)
const initialized = ref(false)
const credentials = ref({ user: '', password: '' })
const authorization = ref(sessionStorage.getItem('miniinflux.admin.authorization') || '')
const session = ref({
  requiresAuthentication: false,
  authenticated: false,
  userName: null,
  rateLimited: false,
  retryAfterSeconds: null
})
const error = ref('')
const notice = ref('')
const overview = ref(null)
const databases = ref([])
const queries = ref([])
const backupPath = ref('./backup/admin-snapshot')
const restorePath = ref('./backup/admin-snapshot')
const tokens = ref([])
const newTokenName = ref('')
const createdToken = ref(null)
const newDbName = ref('')
const newRp = ref({ db: '', name: '', duration: '7d', isDefault: false })
const newCq = ref({ db: '', name: '', text: '' })
const shardsInfo = ref([])
const cacheStats = ref(null)
const queryDb = ref('')
const queryText = ref('')
const queryResult = ref(null)
const queryError = ref('')
const queryPage = ref(1)
const queryPageSize = ref(100)
const queryPagingEnabled = ref(false)
const queryHasNextPage = ref(false)
const queryBaseStatement = ref('')

const signedIn = computed(() =>
  !session.value.requiresAuthentication
  || session.value.authenticated
)
const accountLabel = computed(() =>
  session.value.userName || (session.value.requiresAuthentication ? '未登录' : '本地匿名管理员')
)
const totalSegments = computed(() => databases.value.reduce((sum, db) => sum + db.segmentCount, 0))
const totalShards = computed(() => databases.value.reduce((sum, db) => sum + db.shardCount, 0))

function statValue(name) {
  const stats = overview.value?.stats
  if (!stats) return 0
  return stats[name] ?? stats[`${name.charAt(0).toUpperCase()}${name.slice(1)}`] ?? 0
}

function encodeBasic(user, password) {
  const bytes = new TextEncoder().encode(`${user}:${password}`)
  let binary = ''
  bytes.forEach((byte) => { binary += String.fromCharCode(byte) })
  return `Basic ${btoa(binary)}`
}

function buildHeaders(includeJson = false) {
  return {
    Accept: 'application/json',
    ...(authorization.value ? { Authorization: authorization.value } : {}),
    ...(includeJson ? { 'Content-Type': 'application/json' } : {})
  }
}

function clearProtectedData() {
  overview.value = null
  databases.value = []
  queries.value = []
  activeTab.value = 'overview'
}

function clearAuthentication(message = '') {
  authorization.value = ''
  sessionStorage.removeItem('miniinflux.admin.authorization')
  session.value = {
    ...session.value,
    authenticated: false,
    userName: null
  }
  credentials.value.password = ''
  clearProtectedData()
  if (message) error.value = message
}

async function readError(response) {
  try {
    const payload = await response.json()
    if (payload?.error || payload?.Error) return payload.error || payload.Error
  } catch {
    // Keep the status-based fallback.
  }
  return `请求失败 (${response.status})`
}

async function loadSession() {
  const response = await fetch('/admin/api/session', {
    headers: buildHeaders(),
    cache: 'no-store'
  })
  if (!response.ok) throw new Error(await readError(response))

  session.value = await response.json()
  if (authorization.value && !session.value.authenticated) {
    clearAuthentication(sessionFailureMessage(session.value, '登录信息已失效，请重新登录'))
  }
  return session.value
}

function formatRetryAfter(seconds) {
  if (!seconds || seconds <= 0) return '稍后再试'
  if (seconds < 60) return `${seconds} 秒后再试`
  const minutes = Math.ceil(seconds / 60)
  return `${minutes} 分钟后再试`
}

function formatBytes(bytes) {
  const value = Number(bytes) || 0
  if (value < 1024) return `${value} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let size = value / 1024
  let unit = 0
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024
    unit++
  }
  return `${size.toFixed(size >= 10 || unit === 0 ? 0 : 1)} ${units[unit]}`
}

function sessionFailureMessage(currentSession, fallbackMessage) {
  if (currentSession?.rateLimited) {
    return `登录失败次数过多，请在 ${formatRetryAfter(currentSession.retryAfterSeconds)}。`
  }
  return fallbackMessage
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    cache: 'no-store',
    headers: {
      ...buildHeaders(Boolean(options.body)),
      ...(options.headers ?? {})
    }
  })

  if (response.status === 401) {
    clearAuthentication('登录信息已失效，请重新登录')
    throw new Error('登录信息已失效，请重新登录')
  }
  if (response.status === 403) throw new Error('请求被拒绝')
  if (!response.ok) throw new Error(await readError(response))
  if (response.status === 204) return null
  return response.json()
}

async function loadOverview() {
  overview.value = await api('/admin/api/overview')
}

async function loadDatabases() {
  databases.value = await api('/admin/api/databases')
}

async function loadQueries() {
  queries.value = await api('/admin/api/continuous-queries')
}

async function loadTokens() {
  tokens.value = await api('/admin/api/tokens')
}

async function createToken() {
  if (!newTokenName.value.trim()) { error.value = '请输入令牌名称（A-Za-z0-9 _ -，1..64）'; return }
  busy.value = true; error.value = ''; notice.value = ''
  try {
    const rec = await api('/admin/api/tokens', { method: 'POST', body: JSON.stringify({ name: newTokenName.value.trim() }) })
    createdToken.value = rec
    newTokenName.value = ''
    await loadTokens()
    notice.value = `令牌 ${rec.name} 已创建，请立即复制 token（仅显示一次）`
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

async function revokeToken(id, name) {
  if (!confirm(`确认吊销令牌 ${name} ？`)) return
  busy.value = true; error.value = ''; notice.value = ''
  try {
    await api(`/admin/api/tokens/${encodeURIComponent(id)}`, { method: 'DELETE' })
    await loadTokens()
    notice.value = `令牌 ${name} 已吊销`
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

function copyText(text) {
  if (navigator.clipboard) navigator.clipboard.writeText(text).then(() => notice.value = '已复制').catch(() => {})
  else {
    const ta = document.createElement('textarea'); ta.value = text; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); ta.remove(); notice.value = '已复制'
  }
}

async function execAdminQuery(q) {
  return api('/admin/api/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ q }).toString()
  })
}

async function execAdminCommand(q) {
  return api('/admin/api/command', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({ q }).toString()
  })
}

async function createDatabase() {
  if (!newDbName.value.trim()) { error.value = '请输入数据库名'; return }
  busy.value = true; error.value=''; notice.value=''
  try {
    await execAdminCommand(`CREATE DATABASE "${newDbName.value.trim()}"`)
    newDbName.value = ''
    await Promise.all([loadOverview(), loadDatabases()])
    notice.value = '数据库已创建'
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

async function dropDatabase(name) {
  if (!confirm(`确认删除数据库 ${name}？数据将不可恢复`)) return
  busy.value = true; error.value=''; notice.value=''
  try {
    await execAdminCommand(`DROP DATABASE "${name}"`)
    await Promise.all([loadOverview(), loadDatabases()])
    notice.value = `数据库 ${name} 已删除`
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

async function loadShards() {
  const res = await execAdminQuery('SHOW SHARDS')
  const rows = res?.results?.[0]?.series?.[0]?.values || []
  const cols = res?.results?.[0]?.series?.[0]?.columns || []
  shardsInfo.value = rows.map(r => Object.fromEntries(cols.map((c,i) => [c, r[i]])))
}

async function loadCacheStats() {
  busy.value = true; error.value = ''
  try {
    cacheStats.value = await api('/admin/api/maintenance/cache-stats')
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

async function createRp() {
  if (!newRp.value.db || !newRp.value.name) { error.value = '请选择数据库并输入 RP 名称'; return }
  const dur = newRp.value.duration?.trim() || '7d'
  const def = newRp.value.isDefault ? ' DEFAULT' : ''
  const q = `CREATE RETENTION POLICY "${newRp.value.name}" ON "${newRp.value.db}" DURATION ${dur} REPLICATION 1${def}`
  busy.value = true; error.value=''; notice.value=''
  try {
    await execAdminCommand(q)
    newRp.value.name = ''; newRp.value.duration='7d'; newRp.value.isDefault=false
    await loadDatabases()
    notice.value = 'Retention Policy 已创建'
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

async function createCq() {
  if (!newCq.value.db || !newCq.value.name.trim() || !newCq.value.text.trim()) {
    error.value = '请选择数据库，并输入 CQ 名称和完整查询'
    return
  }
  busy.value = true; error.value = ''; notice.value = ''
  try {
    await execAdminCommand(`CREATE CONTINUOUS QUERY "${newCq.value.name.trim()}" ON "${newCq.value.db}" BEGIN ${newCq.value.text.trim()} END`)
    newCq.value.name = ''; newCq.value.text = ''
    await loadQueries()
    notice.value = 'Continuous Query 已创建'
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

async function dropCq(database, name) {
  if (!confirm(`确认删除 Continuous Query ${name}？`)) return
  busy.value = true; error.value = ''; notice.value = ''
  try {
    await execAdminCommand(`DROP CONTINUOUS QUERY "${name}" ON "${database}"`)
    await loadQueries()
    notice.value = `Continuous Query ${name} 已删除`
  } catch (ex) { error.value = ex.message } finally { busy.value = false }
}

async function loadProtectedData() {
  await loadOverview()
  await Promise.all([loadDatabases(), loadQueries(), loadTokens().catch(() => tokens.value = [])])
}

const querySeriesList = computed(() => {
  const results = queryResult.value?.results || []
  const series = []
  for (const result of results) {
    if (result.error) return { error: result.error }
    for (const s of result.series || []) series.push(s)
  }
  return series
})

const queryPageRows = computed(() => Array.isArray(querySeriesList.value)
  ? querySeriesList.value.reduce((total, series) => total + (series.values?.length || 0), 0)
  : 0)

async function loadQueryPage(page) {
  busy.value = true
  queryError.value = ''
  try {
    const pageSize = Math.max(1, Math.min(500, Number(queryPageSize.value) || 100))
    const statement = queryPagingEnabled.value
      ? `${queryBaseStatement.value} LIMIT ${pageSize} OFFSET ${(page - 1) * pageSize}`
      : queryBaseStatement.value
    queryResult.value = await api('/admin/api/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        q: statement,
        ...(queryDb.value ? { db: queryDb.value } : {})
      }).toString()
    })
    queryPage.value = page
    // InfluxQL applies LIMIT/OFFSET during execution. A full page means another page may exist;
    // selecting it issues a fresh server query instead of retaining the prior pages in the browser.
    queryHasNextPage.value = queryPagingEnabled.value && queryPageRows.value >= pageSize
  } catch (ex) {
    queryError.value = ex.message
  } finally {
    busy.value = false
  }
}

async function runQuery() {
  queryResult.value = null
  queryPage.value = 1
  queryHasNextPage.value = false
  if (!queryText.value.trim()) {
    queryError.value = '请输入查询语句'
    return
  }
  queryBaseStatement.value = queryText.value.trim().replace(/;+\s*$/, '')
  queryPagingEnabled.value = /^select\b/i.test(queryBaseStatement.value)
    && !/\b(?:limit|offset)\b/i.test(queryBaseStatement.value)
  await loadQueryPage(1)
}

async function changeQueryPage(page) {
  if (!queryPagingEnabled.value || page < 1 || busy.value) return
  await loadQueryPage(page)
}

function exportQueryCsv() {
  const series = querySeriesList.value
  if (Array.isArray(series)) {
    const parts = []
    for (const s of series) {
      const cols = s.columns || []
      const header = cols.join(',')
      parts.push(`# series: ${s.name || ''}${s.tags ? ' ' + JSON.stringify(s.tags) : ''}\n${header}`)
      for (const row of s.values || []) {
        parts.push(cols.map((_, i) => csvCell(row[i])).join(','))
      }
    }
    downloadText(parts.join('\n'), 'query-result.csv', 'text/csv')
    return
  }
  error.value = '没有可导出的结果'
}

function csvCell(value) {
  if (value === null || value === undefined) return ''
  const text = String(value)
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`
  return text
}

function downloadText(content, filename, mime) {
  const blob = new Blob([content], { type: mime })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  link.click()
  URL.revokeObjectURL(url)
}

async function refreshAll() {
  error.value = ''
  notice.value = ''
  busy.value = true
  try {
    await loadSession()
    if (signedIn.value) await loadProtectedData()
  } catch (ex) {
    error.value = ex.message
  } finally {
    busy.value = false
  }
}

async function submitLogin() {
  error.value = ''
  notice.value = ''
  if (!credentials.value.user || !credentials.value.password) {
    error.value = '请输入用户名和密码'
    return
  }

  busy.value = true
  const candidate = encodeBasic(credentials.value.user, credentials.value.password)
  authorization.value = candidate
  try {
    const current = await loadSession()
    if (!current.authenticated) throw new Error(sessionFailureMessage(current, '用户名或密码错误'))

    sessionStorage.setItem('miniinflux.admin.authorization', candidate)
    credentials.value.password = ''
    await loadProtectedData()
    notice.value = `已登录：${current.userName}`
  } catch (ex) {
    clearAuthentication()
    error.value = ex.message
  } finally {
    busy.value = false
  }
}

function logout() {
  notice.value = ''
  clearAuthentication()
}

async function runAction(path, payload, successText) {
  notice.value = ''
  error.value = ''
  busy.value = true
  try {
    const result = await api(path, {
      method: 'POST',
      body: payload ? JSON.stringify(payload) : undefined
    })
    await Promise.all([loadOverview(), loadDatabases(), loadQueries()])
    notice.value = result?.message || successText
  } catch (ex) {
    error.value = ex.message
  } finally {
    busy.value = false
  }
}

onMounted(async () => {
  await refreshAll()
  initialized.value = true
})
</script>

<template>
  <div v-if="!initialized" class="loading-screen">
    <div class="loading-mark">MiniInflux</div>
    <div class="subtle">正在连接管理服务...</div>
  </div>

  <div v-else-if="!signedIn" class="login-screen">
    <section class="login-panel">
      <div class="brand dark">MiniInflux</div>
      <div class="login-heading">管理员登录</div>
      <p>当前实例已启用认证，请使用配置文件中的超级管理员账号登录。</p>

      <form class="stack" @submit.prevent="submitLogin">
        <label>
          <span>用户名</span>
          <input v-model.trim="credentials.user" autocomplete="username" autofocus />
        </label>
        <label>
          <span>密码</span>
          <input v-model="credentials.password" type="password" autocomplete="current-password" />
        </label>
        <div v-if="error" class="banner error">{{ error }}</div>
        <button class="primary login-button" type="submit" :disabled="busy">
          {{ busy ? '正在验证...' : '登录' }}
        </button>
      </form>
    </section>
  </div>

  <div v-else class="shell">
    <aside class="sidebar">
      <div>
        <div class="brand">MiniInflux</div>
        <div class="subtle">Admin Console</div>
      </div>

      <nav class="tabs" aria-label="管理菜单">
        <button
          v-for="tab in tabs"
          :key="tab.key"
          :class="['tab', { active: activeTab === tab.key }]"
          @click="activeTab = tab.key"
        >
          {{ tab.label }}
        </button>
      </nav>

      <section class="account">
        <div class="section-title">当前身份</div>
        <strong>{{ accountLabel }}</strong>
        <span class="subtle">{{ session.requiresAuthentication ? '管理员' : '认证未启用' }}</span>
        <button v-if="session.requiresAuthentication" @click="logout">退出登录</button>
      </section>
    </aside>

    <main class="main">
      <header class="toolbar">
        <div>
          <h1>管理控制台</h1>
          <p>当前实例的运行、权限与数据管理。</p>
        </div>
        <div class="toolbar-actions">
          <button :disabled="busy" @click="refreshAll">{{ busy ? '刷新中...' : '刷新' }}</button>
        </div>
      </header>

      <div v-if="error" class="banner error">{{ error }}</div>
      <div v-if="notice" class="banner success">{{ notice }}</div>

      <section v-if="activeTab === 'overview'" class="page">
        <div class="grid two">
          <article class="panel metric"><span>数据目录</span><strong>{{ overview?.dataPath || '-' }}</strong></article>
          <article class="panel metric"><span>监听地址</span><strong>{{ overview?.httpBindAddress || '-' }}</strong></article>
          <article class="panel metric"><span>数据库</span><strong>{{ overview?.databaseCount ?? 0 }}</strong></article>
          <article class="panel metric"><span>CQ 数量</span><strong>{{ overview?.continuousQueryCount ?? 0 }}</strong></article>
          <article class="panel metric"><span>Shard / Segment</span><strong>{{ totalShards }} / {{ totalSegments }}</strong></article>
          <article class="panel metric"><span>内存 Buffer</span><strong>{{ statValue('memoryBufferPoints') }} points</strong></article>
        </div>

        <div class="grid two">
          <article class="panel">
            <div class="section-title">运行状态</div>
            <dl class="detail-list">
              <div><dt>管理台认证</dt><dd>{{ session.requiresAuthentication ? '开启' : '关闭' }}</dd></div>
              <div><dt>数据 API 认证</dt><dd>{{ overview?.authEnabled ? '开启' : '关闭' }}</dd></div>
              <div><dt>TLS</dt><dd>{{ overview?.tlsEnabled ? '开启' : '关闭' }}</dd></div>
              <div><dt>CQ 数量</dt><dd>{{ overview?.continuousQueryCount ?? 0 }}</dd></div>
              <div><dt>待恢复目录</dt><dd>{{ overview?.restorePending ? '存在' : '无' }}</dd></div>
            </dl>
          </article>
          <article class="panel">
            <div class="section-title">查询与压缩</div>
            <dl class="detail-list">
              <div><dt>Query Total</dt><dd>{{ statValue('queryTotal') }}</dd></div>
              <div><dt>Query Errors</dt><dd>{{ statValue('queryErrorTotal') }}</dd></div>
              <div><dt>Compaction Runs</dt><dd>{{ statValue('compactionCount') }}</dd></div>
              <div><dt>Compaction Queue</dt><dd>{{ statValue('compactionQueuedTasks') }}</dd></div>
              <div><dt>CQ Errors</dt><dd>{{ statValue('continuousQueryErrorsTotal') }}</dd></div>
            </dl>
          </article>
        </div>
      </section>

      <section v-else-if="activeTab === 'query'" class="page">
        <article class="panel">
          <div class="section-title">数据查询（只读）</div>
          <div class="subtle">仅允许 SELECT / SHOW 类语句，DELETE、DROP、CREATE 等变更语句会被拒绝。</div>
          <div class="query-form">
            <select v-model="queryDb" class="query-db">
              <option value="">默认数据库</option>
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
            <input
              v-model.trim="queryText"
              class="query-input"
              placeholder="例如：SELECT * FROM cpu WHERE time > now() - 1h"
              @keyup.ctrl.enter="runQuery"
            />
            <button class="primary" :disabled="busy" @click="runQuery">执行</button>
            <button :disabled="!queryResult || busy" @click="exportQueryCsv">导出 CSV</button>
          </div>
          <div v-if="queryError" class="banner error">{{ queryError }}</div>
          <div v-else-if="!queryResult" class="empty compact-empty">暂无结果，请输入查询语句后点击「执行」。</div>
          <div v-else>
            <div v-if="Array.isArray(querySeriesList)" class="result-scroll">
              <div v-if="queryPagingEnabled && queryPageRows > 0" class="pagination" aria-label="查询结果分页">
                <span>第 {{ queryPage }} 页，本页 {{ queryPageRows }} 行</span>
                <label>每页
                  <select v-model.number="queryPageSize" @change="runQuery">
                    <option :value="50">50</option>
                    <option :value="100">100</option>
                    <option :value="250">250</option>
                    <option :value="500">500</option>
                  </select>
                  行
                </label>
                <button :disabled="busy || queryPage <= 1" @click="changeQueryPage(queryPage - 1)">上一页</button>
                <button :disabled="busy || !queryHasNextPage" @click="changeQueryPage(queryPage + 1)">下一页</button>
              </div>
              <div v-else-if="!queryPagingEnabled" class="subtle">该语句包含 LIMIT 或 OFFSET，按语句指定的服务端分页执行。</div>
              <template v-for="(s, si) in querySeriesList" :key="si">
                <div class="result-series-head">{{ s.name || 'result' }}<span v-if="s.tags"> {{ JSON.stringify(s.tags) }}</span></div>
                <div class="table-wrap">
                  <table class="table">
                    <thead><tr><th v-for="(col, ci) in s.columns" :key="ci">{{ col }}</th></tr></thead>
                    <tbody>
                      <tr v-for="(row, ri) in (s.values || [])" :key="ri">
                        <td v-for="(cell, ci) in row" :key="ci">{{ cell === null ? 'null' : cell }}</td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </template>
              <div v-if="queryPageRows === 0" class="empty compact-empty">查询未返回数据行。</div>
            </div>
            <div v-else class="banner error">{{ querySeriesList.error }}</div>
          </div>
        </article>
      </section>

      <section v-else-if="activeTab === 'databases'" class="page">
        <article class="panel">
          <div class="section-title">数据库管理</div>
          <div class="query-form" style="grid-template-columns: 1fr auto">
            <input v-model.trim="newDbName" placeholder="新数据库名（A-Za-z0-9 _ -）" @keyup.enter="createDatabase" />
            <button class="primary" :disabled="busy" @click="createDatabase">创建数据库</button>
          </div>
        </article>
        <article class="panel">
          <div class="section-title">Retention Policy 管理</div>
          <div class="subtle">CREATE RETENTION POLICY 语法，Duration 支持 1h/7d/30d/INF(0)</div>
          <div class="query-form" style="grid-template-columns: 200px 1fr 140px auto auto">
            <select v-model="newRp.db" class="query-db">
              <option value="">选择数据库</option>
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
            <input v-model.trim="newRp.name" placeholder="RP 名称" />
            <input v-model.trim="newRp.duration" placeholder="Duration 7d" />
            <label class="checkbox"><input type="checkbox" v-model="newRp.isDefault" /> <span>DEFAULT</span></label>
            <button class="primary" :disabled="busy" @click="createRp">创建 RP</button>
          </div>
        </article>
        <div v-if="databases.length === 0" class="empty">暂无数据库</div>
        <article v-for="db in databases" :key="db.name" class="panel">
          <div class="row between">
            <div>
              <div class="section-title">{{ db.name }}</div>
              <div class="subtle">默认 RP: {{ db.defaultRetentionPolicy }}</div>
            </div>
            <div class="pill-row">
              <span class="pill">{{ db.measurementCount }} measurements</span>
              <span class="pill">{{ db.seriesCardinality }} series</span>
              <span class="pill">{{ db.shardCount }} shards</span>
              <span class="pill">{{ db.segmentCount }} segments</span>
              <span class="pill">{{ formatBytes(db.sizeBytes) }}</span>
              <button class="danger" style="min-height:30px; padding:6px 10px" :disabled="busy" @click="dropDatabase(db.name)">删除</button>
            </div>
          </div>
          <div class="table-wrap">
            <table class="table">
              <thead><tr><th>Retention Policy</th><th>Duration(ns)</th><th>Default</th><th>Shards</th><th>Segments</th><th>占用大小</th></tr></thead>
              <tbody>
                <tr v-for="rp in db.retentionPolicies" :key="rp.name">
                  <td>{{ rp.name }}</td><td>{{ rp.durationNs }}</td><td>{{ rp.isDefault ? 'yes' : 'no' }}</td>
                  <td>{{ rp.shardCount }}</td><td>{{ rp.segmentCount }}</td><td>{{ formatBytes(rp.sizeBytes) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </article>
      </section>

      <section v-else-if="activeTab === 'queries'" class="page">
        <article class="panel">
          <div class="section-title">创建 Continuous Query</div>
          <div class="subtle">示例：SELECT mean(value) INTO "db"."autogen"."cpu_mean" FROM "db"."autogen"."cpu" GROUP BY time(1m)</div>
          <div class="query-form" style="grid-template-columns: 160px 180px 1fr auto; margin-top:10px">
            <select v-model="newCq.db" class="query-db">
              <option value="">选择数据库</option>
              <option v-for="db in databases" :key="db.name" :value="db.name">{{ db.name }}</option>
            </select>
            <input v-model.trim="newCq.name" placeholder="CQ 名称" />
            <input v-model.trim="newCq.text" placeholder='完整 SELECT ... INTO ... GROUP BY time(...)' />
            <button class="primary" :disabled="busy" @click="createCq">创建</button>
          </div>
        </article>
        <article class="panel">
          <div class="row between">
            <div class="section-title">Continuous Queries</div>
            <button class="primary" :disabled="busy" @click="runAction('/admin/api/maintenance/cq/run', null, '已触发 CQ 调度')">执行一轮</button>
          </div>
          <div v-if="queries.length === 0" class="empty compact-empty">暂无 Continuous Query</div>
          <div v-else class="table-wrap">
            <table class="table">
              <thead><tr><th>DB</th><th>Name</th><th>Every(ns)</th><th>For(ns)</th><th>Recompute</th><th>Last Bucket</th><th>操作</th></tr></thead>
              <tbody>
                <tr v-for="cq in queries" :key="`${cq.database}/${cq.name}`">
                  <td>{{ cq.database }}</td><td>{{ cq.name }}</td><td>{{ cq.everyNs }}</td><td>{{ cq.forNs }}</td>
                  <td>{{ cq.recomputeRecentBuckets }}</td><td>{{ cq.lastCompletedBucketStartNs ?? '-' }}</td>
                  <td><button class="danger" style="min-height:30px" :disabled="busy" @click="dropCq(cq.database, cq.name)">删除</button></td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="query-list">
            <article v-for="cq in queries" :key="`text-${cq.database}/${cq.name}`" class="query-card">
              <div class="query-head">{{ cq.database }} / {{ cq.name }} <button class="danger" style="float:right; min-height:28px; padding:4px 8px" @click="dropCq(cq.database, cq.name)">删除</button></div><pre>{{ cq.queryText }}</pre>
            </article>
          </div>
        </article>
      </section>

      <section v-else-if="activeTab === 'tokens'" class="page">
        <article class="panel">
          <div class="section-title">令牌管理（等权 Bearer Token，与 Basic 并存）</div>
          <div class="subtle">创建后 token 仅显示一次，请立即复制；列表仅展示前缀。用于 <code>Authorization: Bearer &lt;token&gt;</code> 或 <code>Token &lt;token&gt;</code>。</div>
          <div v-if="createdToken" class="banner success" style="margin-top:12px; word-break:break-all">
            <div><strong>新令牌：{{ createdToken.name }}</strong> <span class="subtle">({{ createdToken.prefix }}...)</span></div>
            <div style="margin:8px 0; font-family:monospace; background:#f8fafc; padding:8px; border-radius:6px; border:1px solid #e2e8f0">{{ createdToken.token }}</div>
            <button @click="copyText(createdToken.token)">复制 Token</button>
            <button style="margin-left:8px" @click="createdToken=null">关闭</button>
          </div>
          <div class="query-form" style="grid-template-columns: 1fr auto; margin-top:14px">
            <input v-model.trim="newTokenName" placeholder="新令牌名称（A-Za-z0-9 _ -，1..64）" @keyup.enter="createToken" />
            <button class="primary" :disabled="busy" @click="createToken">创建令牌</button>
          </div>
          <div class="table-wrap">
            <table class="table">
              <thead><tr><th>名称</th><th>前缀</th><th>ID</th><th>创建时间(ns)</th><th>操作</th></tr></thead>
              <tbody>
                <tr v-for="t in tokens" :key="t.id">
                  <td>{{ t.name }}</td><td style="font-family:monospace">{{ t.prefix }}</td><td style="font-family:monospace; font-size:11px">{{ t.id.slice(0,8) }}…</td><td>{{ t.createdAtNs }}</td>
                  <td><button class="danger" :disabled="busy" @click="revokeToken(t.id, t.name)">吊销</button></td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-if="tokens.length===0" class="empty compact-empty">暂无令牌</div>
        </article>
      </section>

      <section v-else class="page">
        <div class="grid two">
          <article class="panel"><div class="section-title">在线维护</div><div class="stack action-stack">
            <button class="primary" :disabled="busy" @click="runAction('/admin/api/maintenance/flush', null, '已执行 flush')">Flush All</button>
            <button class="primary" :disabled="busy" @click="runAction('/admin/api/maintenance/compact', null, '已执行 compaction')">Flush + Compact</button>
          </div></article>
          <article class="panel"><div class="section-title">备份</div><div class="stack action-stack">
            <input v-model.trim="backupPath" placeholder="备份目录" />
            <button class="primary" :disabled="busy" @click="runAction('/admin/api/backup', { path: backupPath }, '备份完成')">创建备份</button>
          </div></article>
          <article class="panel"><div class="section-title">恢复预置</div><div class="stack action-stack">
            <input v-model.trim="restorePath" placeholder="备份目录" />
            <button class="danger" :disabled="busy" @click="runAction('/admin/api/restore', { path: restorePath }, '恢复已准备，需重启生效')">准备恢复</button>
          </div></article>
        </div>
        <div class="grid two" style="margin-top:16px">
          <article class="panel">
            <div class="row between"><div class="section-title">Shard 诊断</div><button :disabled="busy" @click="loadShards()">刷新</button></div>
            <div v-if="shardsInfo.length===0" class="empty compact-empty">点击刷新加载 SHOW SHARDS</div>
            <div v-else class="table-wrap">
              <table class="table">
                <thead><tr><th v-for="k in Object.keys(shardsInfo[0]||{})" :key="k">{{ k }}</th></tr></thead>
                <tbody><tr v-for="(r,i) in shardsInfo" :key="i"><td v-for="k in Object.keys(r)" :key="k">{{ r[k] }}</td></tr></tbody>
              </table>
            </div>
          </article>
          <article class="panel">
            <div class="row between"><div class="section-title">缓存与统计</div><button :disabled="busy" @click="loadCacheStats()">刷新</button></div>
            <div v-if="!cacheStats" class="empty compact-empty">点击刷新加载 cache-stats</div>
            <div v-else>
              <dl class="detail-list">
                <div><dt>Metadata Cache Hits</dt><dd>{{ cacheStats.hits }}</dd></div>
                <div><dt>Misses</dt><dd>{{ cacheStats.misses }}</dd></div>
                <div><dt>Cached</dt><dd>{{ cacheStats.cachedCount }}</dd></div>
              </dl>
            </div>
          </article>
        </div>
      </section>
    </main>
  </div>
</template>

<style>
:root { color-scheme: light; font-family: Inter, "Segoe UI", system-ui, sans-serif; color: #1f2937; background: #f3f4f6; }
* { box-sizing: border-box; }
body { margin: 0; }
button, input, select { font: inherit; }
button { min-height: 40px; border: 1px solid #cbd5e1; background: #fff; color: #111827; border-radius: 8px; padding: 9px 14px; cursor: pointer; }
button.primary { background: #1d4ed8; color: #fff; border-color: #1d4ed8; }
button.danger { background: #b91c1c; color: #fff; border-color: #b91c1c; }
button:disabled { opacity: .6; cursor: default; }
input, select { width: 100%; min-height: 42px; border: 1px solid #cbd5e1; border-radius: 8px; padding: 9px 12px; background: #fff; }
label > span { display: block; margin-bottom: 6px; color: #475569; font-size: 13px; font-weight: 600; }
pre { margin: 0; white-space: pre-wrap; word-break: break-word; font-family: ui-monospace, SFMono-Regular, Consolas, monospace; font-size: 12px; color: #0f172a; }
.loading-screen, .login-screen { min-height: 100vh; display: grid; place-items: center; padding: 24px; }
.loading-screen { align-content: center; gap: 8px; }
.loading-mark { font-size: 28px; font-weight: 750; color: #0f172a; }
.login-screen { background: #e8edf3; }
.login-panel { width: min(420px, 100%); background: #fff; border: 1px solid #dbe2ea; border-radius: 8px; padding: 32px; box-shadow: 0 18px 50px rgba(15, 23, 42, .12); }
.login-heading { margin-top: 28px; font-size: 24px; font-weight: 750; color: #0f172a; }
.login-panel p { margin: 8px 0 24px; color: #64748b; line-height: 1.6; }
.login-button { width: 100%; margin-top: 4px; }
.shell { min-height: 100vh; display: grid; grid-template-columns: 248px minmax(0, 1fr); }
.sidebar { background: #0f172a; color: #e5e7eb; padding: 24px 18px; display: flex; flex-direction: column; gap: 24px; }
.brand { font-size: 22px; font-weight: 750; }
.brand.dark { color: #0f172a; }
.subtle { color: #94a3b8; font-size: 13px; }
.tabs, .stack { display: flex; flex-direction: column; gap: 10px; }
.tab { text-align: left; background: transparent; color: #cbd5e1; border-color: #1e293b; }
.tab.active { background: #1e293b; color: #fff; }
.section-title { font-size: 14px; font-weight: 750; }
.account { margin-top: auto; display: flex; flex-direction: column; gap: 7px; padding-top: 18px; border-top: 1px solid #1e293b; }
.account button { margin-top: 6px; background: transparent; border-color: #334155; color: #e2e8f0; }
.main { min-width: 0; padding: 24px; }
.toolbar, .row { display: flex; align-items: flex-start; gap: 16px; }
.toolbar { justify-content: space-between; margin-bottom: 20px; }
.toolbar h1 { margin: 0 0 6px; font-size: 28px; }
.toolbar p { margin: 0; color: #64748b; }
.toolbar-actions { display: flex; gap: 10px; }
.banner { border-radius: 8px; padding: 12px 14px; margin-bottom: 16px; }
.banner.error { background: #fee2e2; color: #991b1b; }
.banner.success { background: #dcfce7; color: #166534; }
.page { display: flex; flex-direction: column; gap: 16px; }
.grid.two { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
.panel { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 18px; }
.metric span { display: block; font-size: 13px; color: #64748b; margin-bottom: 8px; }
.metric strong { display: block; font-size: 20px; overflow-wrap: anywhere; }
.detail-list { display: grid; gap: 10px; margin: 14px 0 0; }
.detail-list div { display: flex; justify-content: space-between; gap: 16px; }
.detail-list dt { color: #64748b; }
.detail-list dd { margin: 0; font-weight: 650; }
.between { justify-content: space-between; }
.pill-row { display: flex; flex-wrap: wrap; gap: 8px; justify-content: flex-end; }
.pill { display: inline-flex; align-items: center; min-height: 30px; padding: 0 10px; border-radius: 999px; background: #e8f0fe; color: #1d4ed8; font-size: 12px; font-weight: 650; }
.table-wrap { width: 100%; overflow-x: auto; }
.table { width: 100%; border-collapse: collapse; margin-top: 14px; }
.table th, .table td { text-align: left; padding: 10px 8px; border-bottom: 1px solid #e5e7eb; vertical-align: top; white-space: nowrap; }
.query-list { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; margin-top: 16px; }
.query-card { border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; background: #f8fafc; }
.query-head { font-weight: 750; margin-bottom: 8px; }
.form-grid { display: grid; grid-template-columns: minmax(160px, 1fr) minmax(160px, 1fr) 120px 120px; gap: 10px; align-items: center; margin-top: 14px; }
.form-grid.compact { grid-template-columns: minmax(220px, 1fr) 140px 100px; margin-top: 12px; }
.checkbox { display: flex; align-items: center; gap: 8px; }
.checkbox input { width: auto; min-height: auto; }
.checkbox span { margin: 0; color: inherit; }
.actions { width: 90px; }
.action-stack { margin-top: 14px; }
.empty { padding: 32px; border: 1px dashed #cbd5e1; border-radius: 8px; text-align: center; color: #64748b; background: rgba(255,255,255,.55); }
.compact-empty { margin-top: 16px; padding: 24px; }
.query-form { display: grid; grid-template-columns: 200px minmax(0, 1fr) auto auto; gap: 10px; align-items: stretch; margin-top: 14px; }
.query-form .query-input { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
.query-form .query-limit { width: auto; }
.result-scroll { margin-top: 18px; display: flex; flex-direction: column; gap: 18px; }
.result-series-head { font-size: 13px; font-weight: 700; color: #334155; margin-bottom: 6px; }
.result-series-head span { color: #94a3b8; font-weight: 500; }
.pagination { display: flex; align-items: center; flex-wrap: wrap; gap: 8px; color: #475569; font-size: 13px; }
.pagination label { display: inline-flex; align-items: center; gap: 5px; }
.pagination select { min-height: 32px; padding: 4px 26px 4px 8px; }
@media (max-width: 900px) {
  .shell { grid-template-columns: 1fr; }
  .sidebar { gap: 16px; }
  .tabs { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); }
  .tab { text-align: center; padding-inline: 6px; }
  .account { margin-top: 0; }
  .grid.two, .form-grid, .form-grid.compact { grid-template-columns: 1fr; }
  .query-form { grid-template-columns: 1fr; }
  .row.between { flex-direction: column; }
  .pill-row { justify-content: flex-start; }
}
@media (max-width: 560px) {
  .main, .sidebar { padding: 18px 14px; }
  .toolbar { align-items: flex-start; }
  .toolbar h1 { font-size: 23px; }
  .toolbar p { display: none; }
  .tabs { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .query-form { grid-template-columns: 1fr; }
  .login-panel { padding: 24px 20px; }
}
</style>
