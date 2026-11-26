/**
 * Edge「大声朗读」在线语音合成客户端（听书优化版）。
 *
 * 协议：一个 WSS 长连接，先发一次 `speech.config`，之后每句话是一轮 turn：
 *   → 文本帧 Path:ssml
 *   ← 二进制帧 Path:audio（MP3 分片，边合成边推）
 *   ← 文本帧 Path:turn.end
 *
 * 相比 msedge-tts 多做的事：
 * - 双 host：`api.msedgeservices.com`（2026-10 起的 Azure 新入口）优先，
 *   `speech.platform.bing.com` 兜底；某个 host 连不上会冷却 60s 再去试。
 * - 流式：分片一到达就往下游推，不再等整句合成完。
 * - 连接保活：ping/pong + 空闲回收，避免"下一句要重新握手"。
 * - 失败快速熔断：连续失败直接抛错，让上层立刻回落本机音色，而不是卡 15 秒。
 * - 句子级缓存 + 同一句的并发请求合并（预取和播放撞车时只合成一次）。
 */
import crypto from 'node:crypto'
import { createRequire } from 'node:module'
import { Readable } from 'node:stream'

const require = createRequire(import.meta.url)
const WebSocket = require('ws')
const { hashSecMsGecV2 } = require('msedge-tts/dist/SecMsGec.js')

const TRUSTED_CLIENT_TOKEN = '6A5AA1D4EAFF4E9FB37E23D68491D6F4'
const SEC_MS_GEC_VERSION = '2-143.0.3650.96'
const OUTPUT_FORMAT = 'audio-24khz-48kbitrate-mono-mp3'
const EDGE_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0'

const ENDPOINTS = [
  {
    name: 'msedgeservices',
    url: 'wss://api.msedgeservices.com/tts/cognitiveservices/websocket/v1',
    subscriptionKey: true,
  },
  {
    name: 'bing',
    url: 'wss://speech.platform.bing.com/consumer/speech/synthesize/readaloud/edge/v1',
    subscriptionKey: false,
  },
]

const CONNECT_TIMEOUT = 6000
const TURN_IDLE_TIMEOUT = 9000
const KEEPALIVE_MS = 15000
const PONG_TIMEOUT = 5000
const MAX_ACTIVE_TURNS = 4
const IDLE_CLOSE_MS = 10 * 60_000
const HOST_COOLDOWN_MS = 60_000
const CACHE_MAX = 240
const CACHE_TTL = 30 * 60_000

let clockOffsetMs = 0

/** 连接状态：单例 socket，所有句子复用它。 */
const state = {
  ws: null,
  connecting: null,
  endpoint: 0,
  turns: new Map(),
  lastUsed: 0,
  keepalive: null,
  idleTimer: null,
  lastError: '',
  failures: 0,
  unhealthyUntil: 0,
  pongAt: 0,
  pingAt: 0,
}

/** 合成槽位：最多 4 句同时在合成，多的排队，避免把一条连接挤爆。 */
let activeTurns = 0
const turnWaiters = []

/** 最近若干轮的合成耗时（诊断用）。 */
const recentTurns = []
const RECENT_MAX = 200

const slotsHeld = new Set()

function acquireSlot(id) {
  if (activeTurns < MAX_ACTIVE_TURNS) {
    activeTurns += 1
    slotsHeld.add(id)
    return Promise.resolve()
  }
  return new Promise((resolve) => {
    turnWaiters.push(() => {
      activeTurns += 1
      slotsHeld.add(id)
      resolve()
    })
  })
}

function releaseSlot(id) {
  if (!slotsHeld.delete(id)) return
  const next = turnWaiters.shift()
  if (next) next()
  else activeTurns = Math.max(0, activeTurns - 1)
}

function recordTurn(entry) {
  recentTurns.push({ ...entry, at: Date.now() })
  if (recentTurns.length > RECENT_MAX) recentTurns.shift()
}

const hostCooldown = new Map()

/** 句子级缓存：key = voice|rate|text。 */
const cache = new Map()

function cacheGet(key) {
  const hit = cache.get(key)
  if (!hit) return null
  if (Date.now() - hit.at > CACHE_TTL) {
    cache.delete(key)
    return null
  }
  cache.delete(key)
  cache.set(key, hit)
  return hit.buf
}

function cacheSet(key, buf) {
  if (!buf || buf.length === 0) return
  cache.delete(key)
  cache.set(key, { buf, at: Date.now() })
  while (cache.size > CACHE_MAX) {
    const first = cache.keys().next().value
    cache.delete(first)
  }
}

async function secMsGec() {
  const ticks = Math.floor((Date.now() + clockOffsetMs) / 1000) + 11644473600
  const rounded = ticks - (ticks % 300)
  return hashSecMsGecV2(`${rounded * 10_000_000}${TRUSTED_CLIENT_TOKEN}`)
}

function buildUrl(endpoint) {
  return (async () => {
    const url = new URL(endpoint.url)
    url.searchParams.set('TrustedClientToken', TRUSTED_CLIENT_TOKEN)
    url.searchParams.set('Sec-MS-GEC', await secMsGec())
    url.searchParams.set('Sec-MS-GEC-Version', SEC_MS_GEC_VERSION)
    url.searchParams.set('ConnectionId', crypto.randomUUID())
    if (endpoint.subscriptionKey) url.searchParams.set('Ocp-Apim-Subscription-Key', TRUSTED_CLIENT_TOKEN)
    return url.toString()
  })()
}

function stopKeepalive() {
  if (state.keepalive) {
    clearInterval(state.keepalive)
    state.keepalive = null
  }
  if (state.idleTimer) {
    clearTimeout(state.idleTimer)
    state.idleTimer = null
  }
}

function scheduleIdleClose() {
  if (state.idleTimer) clearTimeout(state.idleTimer)
  state.idleTimer = setTimeout(() => {
    if (state.turns.size === 0) closeSocket()
  }, IDLE_CLOSE_MS)
  state.idleTimer.unref?.()
}

function closeSocket() {
  stopKeepalive()
  const ws = state.ws
  state.ws = null
  state.connecting = null
  if (!ws) return
  try {
    ws.removeAllListeners()
    ws.terminate()
  } catch {}
  failTurns(new Error('Edge TTS 连接已关闭'))
}

function failTurns(error) {
  for (const [id, turn] of state.turns) {
    state.turns.delete(id)
    turn.fail(error)
  }
}

function noteFailure(reason) {
  state.failures += 1
  state.lastError = reason
  const endpoint = ENDPOINTS[state.endpoint]
  if (endpoint) hostCooldown.set(endpoint.name, Date.now() + HOST_COOLDOWN_MS)
  if (state.failures >= 2) state.unhealthyUntil = Date.now() + HOST_COOLDOWN_MS
}

function noteSuccess() {
  state.failures = 0
  state.lastError = ''
  state.unhealthyUntil = 0
}

async function createSocket(endpointIndex) {
  const endpoint = ENDPOINTS[endpointIndex]
  const url = await buildUrl(endpoint)
  const ws = new WebSocket(url, {
    headers: {
      'User-Agent': EDGE_UA,
      Origin: 'chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold',
    },
    handshakeTimeout: CONNECT_TIMEOUT,
  })
  // 常驻 error 监听：ws 在握手超时 / 被 terminate 时会 emit('error')，
  // 没有监听器就会冒成 uncaughtException，直接把 DSH 主进程带崩（2026-11-26 的崩溃）。
  ws.on('error', (error) => {
    state.lastError = String(error?.message ?? error)
  })

  await new Promise((resolve, reject) => {
    let settled = false
    const done = (error) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      ws.removeListener('open', onOpen)
      ws.removeListener('error', onError)
      ws.removeListener('unexpected-response', onUnexpected)
      if (error) reject(error)
      else resolve()
    }
    const timer = setTimeout(() => done(new Error(`${endpoint.name} 连接超时`)), CONNECT_TIMEOUT)
    const onOpen = () => done(null)
    const onError = (error) => done(error instanceof Error ? error : new Error(String(error?.message ?? error)))
    const onUnexpected = (request, response) => {
      // 403 多半是本机时钟和微软差得太多：按服务器 Date 校正 token 后重来一次
      const serverTime = Date.parse(response?.headers?.date ?? '')
      if (response?.statusCode === 403 && Number.isFinite(serverTime)) {
        clockOffsetMs = serverTime - Date.now()
      }
      response?.resume?.()
      done(new Error(`${endpoint.name} 握手失败 ${response?.statusCode ?? '?'}`))
    }
    ws.once('open', onOpen)
    ws.once('error', onError)
    ws.once('unexpected-response', onUnexpected)
  })

  return ws
}

/** 把胜出的 socket 挂到全局状态上，并补发 speech.config。 */
function commitSocket(endpointIndex, ws) {
  ws.on('error', (error) => {
    state.lastError = String(error?.message ?? error)
  })
  ws.on('close', () => {
    if (state.ws === ws) {
      state.ws = null
      stopKeepalive()
      failTurns(new Error('Edge TTS 连接意外断开'))
    }
  })
  ws.on('message', (data, isBinary) => handleFrame(data, isBinary))

  state.ws = ws
  state.endpoint = endpointIndex
  state.lastUsed = Date.now()

  // 每条连接只发一次合成配置
  ws.send(
    `X-Timestamp:${new Date().toISOString()}\r\nContent-Type:application/json; charset=utf-8\r\nPath:speech.config\r\n\r\n` +
      JSON.stringify({
        context: {
          synthesis: {
            audio: {
              metadataoptions: { sentenceBoundaryEnabled: 'false', wordBoundaryEnabled: 'false' },
              outputFormat: OUTPUT_FORMAT,
            },
          },
        },
      }) +
      '\r\n',
  )

  ws.on('pong', () => {
    state.pongAt = Date.now()
  })
  state.keepalive = setInterval(() => {
    if (state.ws !== ws || ws.readyState !== WebSocket.OPEN) return
    const sentAt = Date.now()
    state.pingAt = sentAt
    try {
      ws.ping()
    } catch {}
    // 半个死连接：ping 发出去 5 秒没有 pong 就掐掉，下一句自动重连
    const check = setTimeout(() => {
      if (state.ws !== ws || state.pongAt >= sentAt) return
      state.lastError = 'Edge TTS 连接心跳超时'
      try {
        ws.terminate()
      } catch {}
    }, PONG_TIMEOUT)
    check.unref?.()
  }, KEEPALIVE_MS)
  state.keepalive.unref?.()
  scheduleIdleClose()
  noteSuccess()
}

/** 建立（或复用）连接；两个 host 同时握手，谁先通用谁，另一个直接掐掉。 */
async function ensureSocket() {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) return
  if (state.connecting) return state.connecting
  if (Date.now() < state.unhealthyUntil) throw new Error(state.lastError || 'Edge TTS 暂时不可用')

  const order = [state.endpoint, 1 - state.endpoint].filter((index) => index >= 0 && index < ENDPOINTS.length)
  const candidates = order.filter((index) => (hostCooldown.get(ENDPOINTS[index].name) ?? 0) <= Date.now())
  if (candidates.length === 0) throw new Error(state.lastError || 'Edge TTS 所有入口都在冷却')

  state.connecting = new Promise((resolve, reject) => {
    let pending = candidates.length
    let settled = false
    let lastError = null
    for (const index of candidates) {
      createSocket(index)
        .then((ws) => {
          if (settled) {
            try {
              ws.removeAllListeners('error')
              ws.on('error', () => {})
              ws.terminate()
            } catch {}
            return
          }
          settled = true
          commitSocket(index, ws)
          resolve()
        })
        .catch((error) => {
          lastError = error
          hostCooldown.set(ENDPOINTS[index].name, Date.now() + HOST_COOLDOWN_MS)
          pending -= 1
          if (pending === 0 && !settled) {
            settled = true
            state.failures += 1
            if (state.failures >= 2) state.unhealthyUntil = Date.now() + HOST_COOLDOWN_MS
            state.lastError = lastError?.message ?? 'Edge TTS 连接失败'
            reject(lastError ?? new Error(state.lastError))
          }
        })
    }
  })

  try {
    await state.connecting
  } finally {
    state.connecting = null
  }
}

function frameHeader(buffer) {
  if (buffer.length < 2) return null
  const length = buffer.readUInt16BE(0)
  if (buffer.length < 2 + length) return null
  const header = buffer.subarray(2, 2 + length).toString('latin1')
  const id = /X-RequestId:([0-9a-f]+)/i.exec(header)?.[1] ?? ''
  return { header, id, offset: 2 + length }
}

function handleFrame(data, isBinary) {
  if (!isBinary) {
    const text = Buffer.isBuffer(data) ? data.toString('utf8') : String(data)
    if (text.includes('Path:turn.end')) {
      const id = /X-RequestId:([0-9a-f]+)/i.exec(text)?.[1]
      finishTurn(id ?? '', null)
    }
    return
  }
  const buffer = Buffer.isBuffer(data) ? data : Buffer.from(data)
  const frame = frameHeader(buffer)
  if (!frame) return
  if (!frame.header.includes('Path:audio')) return
  const audio = buffer.subarray(frame.offset)
  const turn = state.turns.get(frame.id)
  if (!turn) return
  turn.push(audio)
}

function finishTurn(id, error) {
  const turn = state.turns.get(id)
  if (!turn) return
  state.turns.delete(id)
  state.lastUsed = Date.now()
  scheduleIdleClose()
  releaseSlot(id)
  if (error) turn.fail(error)
  else turn.end()
}

function escapeXml(value) {
  return String(value)
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F]/g, '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;')
}

function ratePercent(rate) {
  const value = Number(rate)
  const speed = Number.isFinite(value) && value > 0 ? Math.min(2, Math.max(0.5, value)) : 1
  const percent = Math.round((speed - 1) * 100)
  return percent >= 0 ? `+${percent}%` : `${percent}%`
}

/**
 * 一次合成 turn，返回一个「分发器」：可挂多个读者，同时把分片写进缓存。
 */
function startTurn({ text, voice, rate }) {
  const id = crypto.randomUUID().replace(/-/g, '')
  const createdAt = Date.now()
  let firstChunkAt = 0
  const chunks = []
  const readers = new Set()
  let ended = false
  let closed = false
  let timer = null

  const dispatch = (chunk) => {
    chunks.push(chunk)
    for (const reader of readers) {
      try {
        reader.push(chunk)
      } catch {}
    }
  }

  const settle = () => {
    if (timer) clearTimeout(timer)
    timer = null
    for (const reader of readers) {
      try {
        reader.push(null)
      } catch {}
    }
    readers.clear()
  }

  const turn = {
    push(chunk) {
      if (ended || closed) return
      if (!firstChunkAt) firstChunkAt = Date.now()
      if (timer) clearTimeout(timer)
      timer = setTimeout(() => finishTurn(id, new Error('Edge TTS 合成超时')), TURN_IDLE_TIMEOUT)
      timer.unref?.()
      dispatch(chunk)
    },
    end() {
      if (ended || closed) return
      if (chunks.length === 0) {
        turn.fail(new Error('Edge TTS 没有返回音频'))
        return
      }
      ended = true
      const audio = Buffer.concat(chunks)
      cacheSet(`${voice}|${rate}|${text}`, audio)
      recordTurn({
        text: text.slice(0, 16),
        voice,
        ok: true,
        ttfb: firstChunkAt ? firstChunkAt - createdAt : -1,
        total: Date.now() - createdAt,
        bytes: audio.length,
      })
      settle()
    },
    fail(error) {
      if (ended || closed) return
      ended = true
      state.lastError = String(error?.message ?? error)
      recordTurn({
        text: text.slice(0, 16),
        voice,
        ok: false,
        ttfb: firstChunkAt ? firstChunkAt - createdAt : -1,
        total: Date.now() - createdAt,
        bytes: 0,
        error: state.lastError,
      })
      for (const reader of readers) {
        try {
          reader.destroy(error)
        } catch {}
      }
      readers.clear()
      if (timer) clearTimeout(timer)
      timer = null
    },
    attach() {
      const stream = new Readable({ read() {} })
      for (const chunk of chunks) stream.push(chunk)
      if (ended) stream.push(null)
      else {
        readers.add(stream)
        stream.on('close', () => readers.delete(stream))
      }
      return stream
    },
    cancel() {
      closed = true
      if (timer) clearTimeout(timer)
      timer = null
      readers.clear()
      state.turns.delete(id)
      releaseSlot(id)
    },
  }

  timer = setTimeout(() => finishTurn(id, new Error('Edge TTS 合成超时')), TURN_IDLE_TIMEOUT)
  timer.unref?.()
  state.turns.set(id, turn)

  const ssml =
    `<speak version="1.0" xmlns="http://www.w3.org/2001/10/synthesis" xml:lang="zh-CN">` +
    `<voice name="${escapeXml(voice)}"><prosody pitch="+0Hz" rate="${ratePercent(rate)}" volume="+0%">${escapeXml(
      text,
    )}</prosody></voice></speak>`

  return { id, turn, ssml }
}

/** 同一句话正在合成时，后来的请求直接挂在同一条 turn 上，不重复发。 */
const inflight = new Map()

/**
 * 流式合成一句话，返回 Node Readable（MP3 分片）。
 * 命中缓存时立即吐出整段，不建连接。
 */
export async function edgeStream({ text, voice, rate, signal }) {
  const content = String(text ?? '').trim()
  if (content === '') throw new Error('缺少文本')
  const speed = Number(rate) > 0 ? Number(rate) : 1
  const key = `${voice}|${speed}|${content}`

  const cached = cacheGet(key)
  if (cached) return Readable.from([cached])

  const running = inflight.get(key)
  if (running) return running.turn.attach()

  // 先登记再连：两个请求同时命中同一句时，后到的直接挂到同一条 turn 上，
  // 不会各开一条连接、各合成一遍。
  const { id, turn, ssml } = startTurn({ text: content, voice, rate: speed })
  inflight.set(key, { id, turn })
  const stream = turn.attach()

  const release = () => inflight.delete(key)
  stream.once('end', release)
  stream.once('error', release)
  stream.once('close', () => {
    release()
    if (!stream.readableEnded) turn.cancel()
  })

  if (signal) {
    const abort = () => {
      turn.cancel()
      inflight.delete(key)
      try {
        stream.destroy()
      } catch {}
    }
    if (signal.aborted) abort()
    else signal.addEventListener('abort', abort, { once: true })
  }

  try {
    await ensureSocket()
    if (!state.ws || state.ws.readyState !== WebSocket.OPEN) throw new Error('Edge TTS 连接不可用')
    await acquireSlot(id)
    // 排队期间用户可能已经切句/关页
    if (!state.turns.has(id)) {
      releaseSlot(id)
      throw new Error('合成已取消')
    }
  } catch (error) {
    releaseSlot(id)
    turn.fail(error)
    inflight.delete(key)
    throw error
  }

  try {
    state.ws.send(
      `X-RequestId:${id}\r\nContent-Type:application/ssml+xml\r\nX-Timestamp:${new Date().toISOString()}Z\r\nPath:ssml\r\n\r\n${ssml}`,
    )
  } catch (error) {
    turn.fail(error)
    inflight.delete(key)
    throw error
  }
  state.lastUsed = Date.now()
  return stream
}

/** 缓冲式合成（给不支持流式的调用方用）。 */
export async function edgeBuffer({ text, voice, rate }) {
  const stream = await edgeStream({ text, voice, rate })
  const chunks = []
  for await (const chunk of stream) chunks.push(chunk)
  if (chunks.length === 0) throw new Error('Edge TTS 没有返回音频')
  return Buffer.concat(chunks)
}

/** 预连接：用户按下播放时提前握手，省掉第一句的 1–3 秒。 */
export async function edgeWarmup() {
  try {
    await ensureSocket()
    return true
  } catch {
    return false
  }
}

/** 最近若干轮的合成耗时，排障用。 */
export function edgeRecent(limit = 80) {
  return recentTurns.slice(-limit)
}

export function edgeStatus() {
  return {
    connected: Boolean(state.ws && state.ws.readyState === WebSocket.OPEN),
    endpoint: ENDPOINTS[state.endpoint]?.name ?? null,
    turns: state.turns.size,
    cached: cache.size,
    lastError: state.lastError,
    unhealthy: Date.now() < state.unhealthyUntil,
  }
}
