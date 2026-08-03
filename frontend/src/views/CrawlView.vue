<script setup lang="ts">
import { ref, reactive, computed, watch, onMounted, onUnmounted } from 'vue'
import { useApi } from '@/composables/useApi'
import { useWebSocket } from '@/composables/useWebSocket'
import type { TaskType, WsEvent, Platform, TaskProgress } from '@/types'

const api = useApi()

// ---- 模式选择 ----
const modes = [
  { value: 'broad_topic' as TaskType, label: '广义话题', desc: '仅提取热点话题和关键词，不爬取内容', icon: '⊞' },
  { value: 'deep_sentiment' as TaskType, label: '深度舆情', desc: '从已有话题读取关键词，在指定平台爬取内容', icon: '⚡' },
  { value: 'pipeline' as TaskType, label: '完整流水线', desc: '先提取话题 → 再爬取内容，全自动流程', icon: '◉' },
]
const selectedMode = ref<TaskType>('deep_sentiment')

// ---- 配置 ----
const platforms = ref<string[]>([])
const platformList = ref<Platform[]>([])
const config = reactive({
  target_date: new Date().toISOString().slice(0, 10),
  sources: [] as string[],
  max_keywords: 5,
  max_notes: 20,
  login_type: 'qrcode',
})

// ---- 任务状态 ----
const taskId = ref<string | null>(null)
const taskRunning = ref(false)
const taskType = ref<TaskType>('deep_sentiment')
const progress = reactive<TaskProgress>({
  stage: '', current_platform: '', platforms_completed: [],
  keywords_total: 0, keywords_processed: 0, notes_collected: 0, elapsed_seconds: 0,
})
const logs = ref<{ level: string; message: string; timestamp: string; elapsed: number }[]>([])
const result = ref<any>(null)
const taskError = ref<string | null>(null)

// WebSocket
const wsConnected = ref(false)
const wsReconnecting = ref(false)
let wsDisconnect: (() => void) | null = null

// 耗时计时器（独立于 WS 更新，每秒刷新）
const displayElapsed = ref('00:00:00')
let elapsedTimer: ReturnType<typeof setInterval> | null = null
const taskStartTime = ref<number>(0)

function formatElapsed(seconds: number): string {
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.floor(seconds % 60)
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`
}

function startElapsedTimer() {
  if (elapsedTimer) { clearInterval(elapsedTimer); elapsedTimer = null }
  taskStartTime.value = Date.now()
  elapsedTimer = setInterval(() => {
    const sec = (Date.now() - taskStartTime.value) / 1000
    displayElapsed.value = formatElapsed(sec)
  }, 200)
}

function stopElapsedTimer(finalSeconds?: number) {
  if (elapsedTimer) { clearInterval(elapsedTimer); elapsedTimer = null }
  if (finalSeconds !== undefined) {
    displayElapsed.value = formatElapsed(finalSeconds)
  }
}

onUnmounted(() => stopElapsedTimer())

// ---- 关键词预览 ----
const previewKeywords = ref<string[]>([])
const previewLoading = ref(false)
watch(() => config.target_date, async (d) => {
  if (!d) return
  previewLoading.value = true
  try {
    const kw = await api.fetchTopicKeywords(d)
    previewKeywords.value = kw.keywords || []
  } catch { previewKeywords.value = [] }
  finally { previewLoading.value = false }
}, { immediate: true })

// ---- 平台列表 ----
onMounted(async () => {
  try {
    const opts = await api.fetchConfigOptions()
    platformList.value = opts.supported_platforms
    config.max_keywords = opts.default_max_keywords
    config.max_notes = opts.default_max_notes
    config.login_type = opts.default_login_type
  } catch (e) { console.error(e) }
})

const showPlatforms = computed(() => selectedMode.value !== 'broad_topic')

function togglePlatform(p: string) {
  const i = platforms.value.indexOf(p)
  if (i >= 0) platforms.value.splice(i, 1)
  else platforms.value.push(p)
}

const effectivePlatforms = computed(() => {
  return platforms.value.length > 0 ? platforms.value : platformList.value.map(p => p.value)
})

// ---- 操作 ----
const isRunning = ref(false)

async function handleStart() {
  if (isRunning.value) return
  resetMonitor()
  isRunning.value = true
  taskRunning.value = true
  taskType.value = selectedMode.value

  try {
    const res = await api.startCrawl(selectedMode.value, {
      target_date: config.target_date,
      platforms: effectivePlatforms.value,
      sources: config.sources.length > 0 ? config.sources : undefined,
      max_keywords: config.max_keywords,
      max_notes: config.max_notes,
      login_type: config.login_type,
    })
    taskId.value = res.task_id

    const { disconnect } = useWebSocket(res.task_id, handleWsEvent)
    wsDisconnect = disconnect

  } catch (e: any) {
    taskError.value = e.message
    taskRunning.value = false
    isRunning.value = false
  }
}

async function handleStop() {
  if (!taskId.value) return
  try {
    await api.stopTask(taskId.value)
  } catch (e) { console.error(e) }
  stopElapsedTimer(progress.elapsed_seconds)
  taskRunning.value = false
  isRunning.value = false
  if (wsDisconnect) { wsDisconnect(); wsDisconnect = null }
}

function handleWsEvent(e: WsEvent) {
  if (e.type === 'stage') {
    progress.stage = e.stage || ''
    addLog('stage', e.message || e.stage || '')
  } else if (e.type === 'progress') {
    if (e.platform) progress.current_platform = e.platform
    if (e.keywords_done !== undefined) progress.keywords_processed = e.keywords_done
    if (e.keywords_total !== undefined) progress.keywords_total = e.keywords_total
    if (e.notes !== undefined) progress.notes_collected = e.notes
    if (e.elapsed_seconds !== undefined) {
      progress.elapsed_seconds = e.elapsed_seconds
      stopElapsedTimer()
      displayElapsed.value = formatElapsed(e.elapsed_seconds)
      startElapsedTimer()
    }
  } else if (e.type === 'log') {
    addLog(e.level || 'info', e.message || '')
  } else if (e.type === 'result') {
    if (e.success) {
      result.value = e.data
      addLog('stage', '任务完成')
    } else {
      taskError.value = e.error || '未知错误'
      addLog('error', '任务失败: ' + (e.error || '未知错误'))
    }
    stopElapsedTimer(e.elapsed_seconds)
    taskRunning.value = false
    isRunning.value = false
    if (wsDisconnect) { wsDisconnect(); wsDisconnect = null }
  } else if (e.type === 'status_change') {
    if (e.to === 'stopping') addLog('warn', '正在停止任务...')
  }

  if (e.elapsed_seconds !== undefined && progress.elapsed_seconds === 0) {
    progress.elapsed_seconds = e.elapsed_seconds
  }
}

function addLog(level: string, message: string) {
  logs.value.push({
    level,
    message,
    timestamp: new Date().toLocaleTimeString(),
    elapsed: progress.elapsed_seconds,
  })
  if (logs.value.length > 500) logs.value = logs.value.slice(-500)
}

function resetMonitor() {
  progress.stage = ''
  progress.current_platform = ''
  progress.platforms_completed = []
  progress.keywords_total = 0
  progress.keywords_processed = 0
  progress.notes_collected = 0
  progress.elapsed_seconds = 0
  logs.value = []
  result.value = null
  taskError.value = null
  stopElapsedTimer()
  startElapsedTimer()
}

const logContainer = ref<HTMLElement | null>(null)
watch(logs, () => {
  setTimeout(() => {
    if (logContainer.value) {
      logContainer.value.scrollTop = logContainer.value.scrollHeight
    }
  }, 50)
}, { deep: true })
</script>

<template>
  <div class="p-6 space-y-5 animate-fade-in">
    <div>
      <h2 class="text-lg font-display font-semibold text-terminal-text">爬虫控制台</h2>
      <p class="text-sm text-terminal-muted mt-0.5">选择模式，配置参数，启动爬取</p>
    </div>

    <!-- 模式选择 -->
    <div>
      <h3 class="text-xs font-display text-terminal-muted uppercase tracking-wider mb-3">运行模式</h3>
      <div class="grid grid-cols-3 gap-3">
        <button
          v-for="m in modes" :key="m.value"
          @click="selectedMode = m.value"
          class="text-left p-4 rounded-lg border-2 transition-all duration-200"
          :class="selectedMode === m.value
            ? 'border-terminal-green bg-terminal-green/5 mode-card-active'
            : 'border-terminal-border bg-terminal-panel hover:border-terminal-border/60'"
        >
          <span class="font-display text-xl"
            :class="selectedMode === m.value ? 'text-terminal-green' : 'text-terminal-muted'"
          >{{ m.icon }}</span>
          <div class="font-semibold text-sm mt-1.5"
            :class="selectedMode === m.value ? 'text-terminal-green' : 'text-terminal-text'"
          >{{ m.label }}</div>
          <div class="text-xs text-terminal-muted mt-1 leading-relaxed">{{ m.desc }}</div>
        </button>
      </div>
    </div>

    <!-- 配置区 -->
    <div class="grid grid-cols-2 gap-5">
      <!-- 平台选择 -->
      <div v-if="showPlatforms" class="bg-terminal-panel border border-terminal-border rounded-lg p-4">
        <h3 class="text-xs font-display text-terminal-muted uppercase tracking-wider mb-3">
          目标平台 <span class="text-terminal-green">(不选=全平台)</span>
        </h3>
        <div class="flex flex-wrap gap-2">
          <label
            v-for="p in platformList" :key="p.value"
            class="cursor-pointer"
          >
            <input
              type="checkbox"
              :value="p.value"
              :checked="platforms.includes(p.value)"
              @change="togglePlatform(p.value)"
              class="hidden platform-checkbox"
            />
            <span
              class="inline-block px-3 py-1.5 rounded border text-xs font-body transition-all"
              :class="platforms.includes(p.value)
                ? 'border-terminal-green bg-terminal-green/10 text-terminal-green'
                : 'border-terminal-border text-terminal-muted hover:border-terminal-muted'"
            >{{ p.label }}</span>
          </label>
        </div>
        <p class="text-[10px] text-terminal-muted mt-2">
          当前: {{ effectivePlatforms.length }} 个平台 · {{ effectivePlatforms.join(', ') }}
        </p>
      </div>

      <!-- 参数配置 -->
      <div class="bg-terminal-panel border border-terminal-border rounded-lg p-4"
        :class="showPlatforms ? '' : 'col-span-2'">
        <h3 class="text-xs font-display text-terminal-muted uppercase tracking-wider mb-3">爬取参数</h3>
        <div class="grid grid-cols-2 gap-3">
          <div>
            <label class="text-xs text-terminal-muted">目标日期</label>
            <input
              v-model="config.target_date"
              type="date"
              class="w-full mt-1 bg-terminal-bg border border-terminal-border rounded px-3 py-1.5 text-sm font-mono text-terminal-text focus:border-terminal-green focus:outline-none"
            />
          </div>
          <div>
            <label class="text-xs text-terminal-muted">登录方式</label>
            <select
              v-model="config.login_type"
              class="w-full mt-1 bg-terminal-bg border border-terminal-border rounded px-3 py-1.5 text-sm font-mono text-terminal-text focus:border-terminal-green focus:outline-none"
            >
              <option value="qrcode">扫码登录</option>
              <option value="phone">手机号登录</option>
              <option value="cookie">Cookie</option>
            </select>
          </div>
          <div>
            <label class="text-xs text-terminal-muted">最大关键词数</label>
            <input
              v-model.number="config.max_keywords"
              type="number" min="1" max="50"
              class="w-full mt-1 bg-terminal-bg border border-terminal-border rounded px-3 py-1.5 text-sm font-mono text-terminal-text focus:border-terminal-green focus:outline-none"
            />
          </div>
          <div v-if="selectedMode !== 'broad_topic'">
            <label class="text-xs text-terminal-muted">最大笔记数/平台</label>
            <input
              v-model.number="config.max_notes"
              type="number" min="5" max="200"
              class="w-full mt-1 bg-terminal-bg border border-terminal-border rounded px-3 py-1.5 text-sm font-mono text-terminal-text focus:border-terminal-green focus:outline-none"
            />
          </div>
        </div>
      </div>
    </div>

    <!-- 关键词预览 -->
    <div class="bg-terminal-panel border border-terminal-border rounded-lg p-4">
      <h3 class="text-xs font-display text-terminal-muted uppercase tracking-wider mb-3">
        关键词预览
        <span v-if="previewLoading" class="text-terminal-amber ml-2">加载中...</span>
        <span v-else class="text-terminal-green ml-2">{{ previewKeywords.length }} 个</span>
      </h3>
      <div v-if="previewKeywords.length === 0" class="text-sm text-terminal-muted">
        该日期暂无话题关键词，请先运行"广义话题提取"
      </div>
      <div v-else class="flex flex-wrap gap-1.5">
        <span
          v-for="k in previewKeywords" :key="k"
          class="px-2 py-0.5 bg-terminal-bg border border-terminal-border rounded text-xs font-mono text-terminal-text"
        >{{ k }}</span>
      </div>
    </div>

    <!-- 操作按钮 -->
    <div class="flex items-center gap-3">
      <button
        v-if="!isRunning"
        @click="handleStart"
        class="px-6 py-2.5 bg-terminal-green text-black text-sm font-semibold rounded hover:bg-terminal-green/90 transition-colors font-display disabled:opacity-50 disabled:cursor-not-allowed"
      >
        开始爬取
      </button>
      <button
        v-else
        @click="handleStop"
        class="px-6 py-2.5 bg-terminal-red text-white text-sm font-semibold rounded hover:bg-terminal-red/90 transition-colors font-display"
      >
        停止爬取
      </button>
      <span v-if="taskError" class="text-sm text-terminal-red font-mono">{{ taskError }}</span>
    </div>

    <!-- 实时监视器 -->
    <div v-if="taskRunning || result || logs.length > 0" class="space-y-4">
      <!-- 耗时 + 阶段 -->
      <div class="flex items-center gap-4">
        <div class="bg-terminal-panel border border-terminal-border rounded-lg px-4 py-2.5 flex items-center gap-3">
          <span class="text-xs text-terminal-muted">耗时</span>
          <span class="text-xl font-display font-semibold text-terminal-green tabular-nums">{{ displayElapsed }}</span>
        </div>
        <div v-if="progress.stage" class="bg-terminal-panel border border-terminal-border rounded-lg px-4 py-2.5">
          <span class="text-xs text-terminal-muted mr-2">阶段</span>
          <span class="text-sm font-mono text-terminal-amber">{{ progress.stage }}</span>
        </div>
        <div v-if="progress.keywords_total > 0" class="bg-terminal-panel border border-terminal-border rounded-lg px-4 py-2.5">
          <span class="text-xs text-terminal-muted mr-2">关键词</span>
          <span class="text-sm font-mono text-terminal-text">
            {{ progress.keywords_processed }}/{{ progress.keywords_total }}
          </span>
        </div>
        <div v-if="progress.notes_collected > 0" class="bg-terminal-panel border border-terminal-border rounded-lg px-4 py-2.5">
          <span class="text-xs text-terminal-muted mr-2">笔记</span>
          <span class="text-sm font-mono text-terminal-blue">{{ progress.notes_collected }}</span>
        </div>
      </div>

      <!-- 进度条 -->
      <div v-if="progress.keywords_total > 0" class="bg-terminal-panel border border-terminal-border rounded-lg p-4">
        <div class="flex justify-between text-xs text-terminal-muted mb-2">
          <span>爬取进度</span>
          <span>{{ Math.round((progress.keywords_processed / progress.keywords_total) * 100) }}%</span>
        </div>
        <div class="h-1.5 bg-terminal-bg rounded-full overflow-hidden">
          <div
            class="h-full bg-terminal-green rounded-full transition-all duration-300"
            :style="{ width: (progress.keywords_processed / progress.keywords_total * 100) + '%' }"
          ></div>
        </div>
      </div>

      <!-- 平台状态 -->
      <div v-if="effectivePlatforms.length > 0" class="flex gap-2 flex-wrap">
        <div
          v-for="p in effectivePlatforms" :key="p"
          class="px-3 py-1 rounded border text-xs font-mono transition-all"
          :class="progress.current_platform === p
            ? 'border-terminal-blue bg-terminal-blue/10 text-terminal-blue'
            : progress.platforms_completed.includes(p)
              ? 'border-terminal-green bg-terminal-green/5 text-terminal-green'
              : 'border-terminal-border text-terminal-muted'"
        >
          {{ p }}
          <span v-if="progress.current_platform === p" class="ml-1 spinner inline-block">◌</span>
          <span v-else-if="progress.platforms_completed.includes(p)" class="ml-1">✓</span>
        </div>
      </div>

      <!-- 日志终端 -->
      <div class="bg-terminal-bg border border-terminal-border rounded-lg overflow-hidden">
        <div class="flex items-center justify-between px-3 py-1.5 bg-terminal-panel border-b border-terminal-border">
          <span class="text-[10px] font-display text-terminal-muted uppercase tracking-wider">终端日志</span>
          <span class="text-[10px] text-terminal-muted font-mono">{{ logs.length }} 行</span>
        </div>
        <div
          ref="logContainer"
          class="log-terminal h-60 p-3"
        >
          <div v-if="logs.length === 0" class="text-terminal-muted text-sm">等待任务启动...</div>
          <div
            v-for="(l, i) in logs" :key="i"
            class="log-line"
            :class="'log-' + l.level"
          >
            <span class="text-terminal-muted mr-2">[{{ l.timestamp }}]</span>
            {{ l.message }}
          </div>
          <div v-if="taskRunning" class="log-line">
            <span class="log-cursor text-terminal-green">█</span>
          </div>
        </div>
      </div>

      <!-- 结果面板 -->
      <div v-if="result" class="bg-terminal-green/5 border border-terminal-green rounded-lg p-4">
        <h3 class="text-sm font-display font-semibold text-terminal-green mb-2">任务完成</h3>
        <pre class="text-xs font-mono text-terminal-text overflow-x-auto">{{ JSON.stringify(result, null, 2) }}</pre>
      </div>
    </div>
  </div>
</template>
