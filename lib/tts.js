/**
 * macOS 系统音色合成（say → WAV）。
 *
 * 只在本机可用：非 darwin 平台返回空结果，调用方回退到浏览器 speechSynthesis。
 * 不落盘缓存：每句按需合成，返回 Buffer。
 */
import { spawn } from 'node:child_process'
import fsp from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

/** 允许通过 query 选择的音色：key 是短名，value 是 say 里的实际名称。 */
export const MACOS_VOICES = {
  yue: 'Yue (Premium)',
  han: 'Han (Premium)',
  lilian: 'Lilian (Premium)',
  tingting: 'Tingting',
  meijia: 'Meijia',
}

export function macosVoiceSupported() {
  return process.platform === 'darwin'
}

/**
 * 合成一句文本为 WAV。
 * @param {{text: string, voice?: string, rate?: number}} input
 * @returns {Promise<Buffer | null>} 非 macOS 或失败时返回 null
 */
export async function synthesizeWav({ text, voice, rate }) {
  if (!macosVoiceSupported()) return null
  const content = String(text ?? '').trim().slice(0, 600)
  if (content === '') return null
  const name = MACOS_VOICES[String(voice ?? 'yue')] ?? MACOS_VOICES.yue
  const wpm = Math.round(180 * (Number(rate) > 0 ? Number(rate) : 1))
  const file = path.join(os.tmpdir(), `lamplit-tts-${Date.now()}-${Math.random().toString(36).slice(2)}.wav`)
  try {
    await new Promise((resolve, reject) => {
      const child = spawn('say', ['-v', name, '-r', String(wpm), '-o', file, '--file-format=WAVE', '--data-format=LEI16@22050', content], {
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
