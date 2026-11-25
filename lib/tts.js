/**
 * 听书语音合成：macOS 系统音色（say → WAV）+ 可选的 Kokoro 本地服务。
 *
 * - `yue` / `han` / `lilian` / `tingting` / `meijia`：本机 `say` 合成，非 darwin 返回 null。
 * - `kokoro_*`：请求本机 Kokoro HTTP 服务（默认 127.0.0.1:8731），
 *   服务不存在时按需拉起 `~/code/dsh-tts-kokoro/service/server.py`。
 *
 * 不落盘缓存：每句按需合成，返回 Buffer。
 */
import { spawn } from 'node:child_process'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

/** 允许通过 query 选择的 macOS 音色：key 是短名，value 是 say 里的实际名称。 */
export const MACOS_VOICES = {
  yue: 'Yue (Premium)',
  han: 'Han (Premium)',
  lilian: 'Lilian (Premium)',
  tingting: 'Tingting',
  meijia: 'Meijia',
}

/** Kokoro 音色：key 是前端短名，value 是服务里的音色编号。 */
export const KOKORO_VOICES = {
  kokoro_f1: 'zf_044',
  kokoro_f2: 'zf_086',
  kokoro_f3: 'zf_039',
  kokoro_m1: 'zm_029',
  kokoro_m2: 'zm_033',
  kokoro_m3: 'zm_080',
}

const KOKORO_DIR = process.env.LAMP_KOKORO_DIR || path.join(os.homedir(), 'code', 'dsh-tts-kokoro')
const KOKORO_URL = process.env.LAMP_KOKORO_URL || 'http://127.0.0.1:8731'
const KOKORO_PYTHON = path.join(KOKORO_DIR, '.venv', 'bin', 'python')
const KOKORO_SERVER = path.join(KOKORO_DIR, 'service', 'server.py')

let kokoroChild = null
let kokoroStarting = null

export function macosVoiceSupported() {
  return process.platform === 'darwin'
}

export function isKokoroVoice(voice) {
  return Object.prototype.hasOwnProperty.call(KOKORO_VOICES, String(voice ?? ''))
}

async function kokoroHealthy() {
  try {
    const response = await fetch(`${KOKORO_URL}/health`, { signal: AbortSignal.timeout(800) })
    return response.ok
  } catch {
    return false
  }
}

async function ensureKokoro() {
  if (await kokoroHealthy()) return true
  if (kokoroStarting) return kokoroStarting
  kokoroStarting = (async () => {
    try {
      await fsp.access(KOKORO_PYTHON)
      await fsp.access(KOKORO_SERVER)
    } catch {
      return false
    }
    if (!kokoroChild) {
      kokoroChild = spawn(KOKORO_PYTHON, [KOKORO_SERVER], {
        cwd: KOKORO_DIR,
        stdio: 'ignore',
        detached: false,
        env: { ...process.env, KOKORO_MODEL_DIR: path.join(KOKORO_DIR, 'models', 'kokoro') },
      })
      kokoroChild.on('exit', () => { kokoroChild = null })
    }
    for (let i = 0; i < 60; i += 1) {
      if (await kokoroHealthy()) return true
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
    return false
  })().finally(() => { kokoroStarting = null })
  return kokoroStarting
}

async function kokoroWav(text, voice, rate) {
  const ready = await ensureKokoro()
  if (!ready) return null
  const response = await fetch(`${KOKORO_URL}/tts`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ text, voice: KOKORO_VOICES[voice], speed: rate }),
    signal: AbortSignal.timeout(60000),
  })
  if (!response.ok) return null
  return Buffer.from(await response.arrayBuffer())
}

async function macosWav(text, voice, rate) {
  if (!macosVoiceSupported()) return null
  const name = MACOS_VOICES[String(voice ?? 'yue')] ?? MACOS_VOICES.yue
  const wpm = Math.round(180 * (rate > 0 ? rate : 1))
  const file = path.join(os.tmpdir(), `lamplit-tts-${Date.now()}-${Math.random().toString(36).slice(2)}.wav`)
  try {
    await new Promise((resolve, reject) => {
      const child = spawn('say', ['-v', name, '-r', String(wpm), '-o', file, '--file-format=WAVE', '--data-format=LEI16@22050', text], {
        stdio: 'ignore',
      })
      child.on('error', reject)
      child.on('exit', (code) => (code === 0 ? resolve() : reject(new Error(`say 退出码 ${code}`))))
    })
    return await fsp.readFile(file)
  } catch {
    return null
  } finally {
    fsp.rm(file, { force: true }).catch(() => {})
  }
}

/**
 * 合成一句文本为 WAV。
 * @param {{text: string, voice?: string, rate?: number}} input
 * @returns {Promise<Buffer | null>} 不支持或失败时返回 null
 */
export async function synthesizeWav({ text, voice, rate }) {
  const content = String(text ?? '').trim().slice(0, 600)
  if (content === '') return null
  const speed = Number(rate) > 0 ? Number(rate) : 1
  const key = String(voice ?? 'yue')
  if (isKokoroVoice(key)) {
    try {
      return await kokoroWav(content, key, speed)
    } catch {
      return null
    }
  }
  return macosWav(content, key, speed)
}
