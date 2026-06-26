<script setup>
import { computed, onMounted, ref } from 'vue'

const tabs = [
  { key: 'overview', label: '概览' },
  { key: 'databases', label: '数据库' },
  { key: 'queries', label: 'CQ' },
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

async function loadProtectedData() {
  await loadOverview()
  await Promise.all([loadDatabases(), loadQueries()])
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

      <section v-else-if="activeTab === 'databases'" class="page">
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
            </div>
          </div>
          <div class="table-wrap">
            <table class="table">
              <thead><tr><th>Retention Policy</th><th>Duration(ns)</th><th>Default</th><th>Shards</th><th>Segments</th></tr></thead>
              <tbody>
                <tr v-for="rp in db.retentionPolicies" :key="rp.name">
                  <td>{{ rp.name }}</td><td>{{ rp.durationNs }}</td><td>{{ rp.isDefault ? 'yes' : 'no' }}</td>
                  <td>{{ rp.shardCount }}</td><td>{{ rp.segmentCount }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </article>
      </section>

      <section v-else-if="activeTab === 'queries'" class="page">
        <article class="panel">
          <div class="row between">
            <div class="section-title">Continuous Queries</div>
            <button class="primary" :disabled="busy" @click="runAction('/admin/api/maintenance/cq/run', null, '已触发 CQ 调度')">执行一轮</button>
          </div>
          <div v-if="queries.length === 0" class="empty compact-empty">暂无 Continuous Query</div>
          <div v-else class="table-wrap">
            <table class="table">
              <thead><tr><th>DB</th><th>Name</th><th>Every(ns)</th><th>For(ns)</th><th>Recompute</th><th>Last Bucket</th></tr></thead>
              <tbody>
                <tr v-for="cq in queries" :key="`${cq.database}/${cq.name}`">
                  <td>{{ cq.database }}</td><td>{{ cq.name }}</td><td>{{ cq.everyNs }}</td><td>{{ cq.forNs }}</td>
                  <td>{{ cq.recomputeRecentBuckets }}</td><td>{{ cq.lastCompletedBucketStartNs ?? '-' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div class="query-list">
            <article v-for="cq in queries" :key="`text-${cq.database}/${cq.name}`" class="query-card">
              <div class="query-head">{{ cq.database }} / {{ cq.name }}</div><pre>{{ cq.queryText }}</pre>
            </article>
          </div>
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
@media (max-width: 900px) {
  .shell { grid-template-columns: 1fr; }
  .sidebar { gap: 16px; }
  .tabs { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .tab { text-align: center; padding-inline: 6px; }
  .account { margin-top: 0; }
  .grid.two, .form-grid, .form-grid.compact { grid-template-columns: 1fr; }
  .row.between { flex-direction: column; }
  .pill-row { justify-content: flex-start; }
}
@media (max-width: 560px) {
  .main, .sidebar { padding: 18px 14px; }
  .toolbar { align-items: flex-start; }
  .toolbar h1 { font-size: 23px; }
  .toolbar p { display: none; }
  .tabs { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .login-panel { padding: 24px 20px; }
}
</style>
