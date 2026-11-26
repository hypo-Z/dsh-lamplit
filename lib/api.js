/**
 * `/lamplit/api/*` 路由。
 *
 * 只有同源 fetch 会打到这里；不需要 CORS，也不往外网暴露额外信息。
 * 响应统一 `{ok:true, ...}` / `{ok:false, code, error}`。
 */
import fsp from 'node:fs/promises'
import path from 'node:path'
import crypto from 'node:crypto'

import { createAi } from './ai.js'
import { parseEpub } from './epub.js'
import { ProviderError } from './http.js'
import { parseMarkdown } from './markdown.js'
import { Notes, notesToMarkdown, shelfToMarkdown } from './notes.js'
import { Store } from './store.js'
import { parsePlainText } from './txt.js'
import * as github from './sources/github.js'
import { completeQrLogin, startQrLogin } from './vendor/weread/qr-login.js'
import { createWereadProvider } from './providers/weread.js'
import { Readable } from 'node:stream'
import { edgeRecent, edgeStatus, edgeWarmup, isEdgeVoice, isKokoroVoice, macosVoiceSupported, synthesizeStream, synthesizeWav } from './tts.js'

const PREFIX = '/lamplit'

/** 客户端上报的句间停顿（毫秒），只留在内存里，用于排查听书卡顿。 */
const ttsGaps = []
const TTS_GAPS_MAX = 200
const MAX_BODY_BYTES = 120 * 1024 * 1024

/** PDF 分析器按需加载：没导入 PDF 的书不需要付这份启动成本。 */
let pdfAnalyzer
async function loadPdfAnalyzer() {
  if (pdfAnalyzer === undefined) {
    const module = await import('./pdf.js')
    pdfAnalyzer = module.analyzePdf
  }
  return pdfAnalyzer
}


/** 自己的版本号，以及 npm 上的最新版（6 小时缓存，失败不影响任何功能）。 */
const packageFile = new URL('../package.json', import.meta.url)
let ownVersion = null
async function currentVersion() {
  if (ownVersion !== null) return ownVersion
  try {
    ownVersion = String(JSON.parse(await fsp.readFile(packageFile, 'utf8')).version || '0.0.0')
  } catch {
    ownVersion = '0.0.0'
  }
  return ownVersion
}

let versionProbe = { at: 0, latest: '', error: null }
const VERSION_TTL = 6 * 60 * 60 * 1000

function compareVersions(left, right) {
  const parse = (value) => String(value || '').split('-')[0].split('.').map((part) => Number.parseInt(part, 10) || 0)
  const a = parse(left)
  const b = parse(right)
  for (let index = 0; index < 3; index += 1) {
    if ((a[index] || 0) > (b[index] || 0)) return 1
    if ((a[index] || 0) < (b[index] || 0)) return -1
  }
  return 0
}

async function probeLatestVersion() {
  const now = Date.now()
  if (versionProbe.at > 0 && now - versionProbe.at < VERSION_TTL) return versionProbe
  try {
    const response = await fetch('https://registry.npmjs.org/dsh-lamplit/latest', {
      headers: { accept: 'application/json' },
      signal: AbortSignal.timeout(5000),
    })
    if (!response.ok) throw new Error(`HTTP ${response.status}`)
    const payload = await response.json()
    versionProbe = { at: now, latest: String(payload?.version ?? ''), error: null }
  } catch (error) {
    versionProbe = { at: now, latest: versionProbe.latest, error: error?.message ?? String(error) }
  }
  return versionProbe
}

function json(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    },
  })
}

function fail(code, message, status = 400, extra = {}) {
  // 额外字段在前：`details.code` 之类的上游信息不能盖住我们自己的 code。
  return json({ ...extra, ok: false, code, error: message }, status)
}

function tocOf(book, chapters) {
  return chapters.map((chapter, index) => ({
    index,
    title: chapter.title || `第 ${index + 1} 章`,
    l: 1,
    chars: chapter.chars ?? 0,
    locked: chapter.locked === true,
  }))
}

function stripExtension(name) {
  return String(name ?? '')
    .replace(/\.[^.]+$/, '')
    .trim()
}

function kindOf(name) {
  const extension = String(name ?? '').toLowerCase().match(/\.([a-z0-9]+)$/)?.[1] ?? ''
  if (extension === 'md' || extension === 'markdown' || extension === 'mdown') return 'md'
  if (extension === 'txt' || extension === 'text') return 'txt'
  if (extension === 'pdf') return 'pdf'
  if (extension === 'epub') return 'epub'
  if (extension === 'mobi' || extension === 'azw' || extension === 'azw3' || extension === 'kfx' || extension === 'lit') {
    return 'kindle'
  }
  return ''
}

/** 从最近在读的书里挑一句 12–80 字的正文，按天固定，不重复。 */
async function dailyQuote(store) {
  const books = (await store.books()).slice().sort((left, right) => (right.lastOpenedAt || right.addedAt) - (left.lastOpenedAt || left.addedAt))
  const today = new Date()
  const seed = today.getFullYear() * 1000 + today.getMonth() * 40 + today.getDate()
  for (const book of books.slice(0, 6)) {
    const loaded = await store.readBook(book.id)
    const chapters = loaded?.chapters ?? []
    if (chapters.length === 0) continue
    const order = [chapters[book.progress?.chapterIndex ?? 0], ...chapters].filter(Boolean)
    for (const chapter of order.slice(0, 8)) {
      const lines = (chapter.blocks ?? [])
        .filter((block) => block.t === 'p' && typeof block.x === 'string' && block.x.length >= 12 && block.x.length <= 80)
        .map((block) => block.x)
      if (lines.length === 0) continue
      return {
        text: lines[seed % lines.length],
        bookId: book.id,
        bookTitle: book.title,
        author: book.author ?? '',
        chapterTitle: chapter.title ?? '',
      }
    }
  }
  return null
}

function importedBook(parsed, { name, source = 'local', sourceId = null, format, sourceFile = null }) {
  return {
    title: parsed.title || stripExtension(name) || '未命名',
    author: parsed.author ?? '',
    source,
    sourceId,
    format,
    sourceFile,
    chapters: parsed.chapters,
  }
}

export function createApi({ ctx, config = {} }) {
  const store = new Store({ root: typeof config.root === 'string' && config.root !== '' ? config.root : undefined })
  const notes = new Notes(store.storageDir)
  const ai = createAi({ ctx })
  const providers = {
    weread: createWereadProvider({
      readRecord: () => store.providerRecord('weread'),
      persistCookie: (cookie) => store.patchProvider('weread', { cookie }).catch(() => {}),
    }),
  }
  const browseCache = { weread: null }

  /**
   * 首次使用时预置一个公版书仓库。
   * 为什么不预置那些几万星的「XX 全集」：里面基本是未授权的扫描件，
   * 内置成默认等于替人分发。要接哪个仓库，设置里加一行就行。
   */
  /** 仓库在界面上要拆成 owner / repo / path 三段显示。 */
  const repoView = (spec) => {
    try {
      const parsed = github.parseSpec(spec)
      return { spec: github.specOf(parsed), owner: parsed.owner, repo: parsed.repo, path: parsed.path }
    } catch {
      const [owner = '', repo = ''] = String(spec).split('/')
      return { spec: String(spec), owner, repo, path: '' }
    }
  }
  const repoViews = (list) => list.map(repoView)

  const DEFAULT_GITHUB_REPOS = [
    'Dujltqzv/Some-Many-Books',        // 25.8k★ · 109 PDF
    '0voice/expert_readed_books',      // 12.8k★ · 369 PDF + 100 EPUB + 24 TXT
    'CyberCommy/geektime-books',       // 13.4k★ · 153 EPUB
    'hehonghui/awesome-english-ebooks',// 39.3k★ · 163 EPUB + 143 PDF + 157 MD
  ]
  let seededGithub = false
  async function ensureGithubDefaults() {
    if (seededGithub) return
    seededGithub = true
    const config = await store.githubConfig()
    if (config.seeded) return
    await store.patchGithub({ repos: [...new Set([...DEFAULT_GITHUB_REPOS, ...config.repos])], seeded: true })
  }

  const logger = ctx?.logger

  /** 扫码会话：uid → 后台登录任务的状态。 */
  const qrSessions = new Map()

  /**
   * 在线目录清洗：去掉「封面/版权信息」这类占位章，并把连续同名章合并成最后一条
   * （微信读书里常见「南山经（标题页）+ 南山经（正文）」两连，留着两个目录项很别扭）。
   */
  function normalizeOnlineToc(chapters) {
    const skip = new Set(['封面', '版权信息', '扉页', '插页', '目录'])
    const flat = (chapters || []).filter((chapter) => !skip.has(String(chapter.title || '').trim()))
    const out = []
    for (const chapter of flat) {
      const previous = out[out.length - 1]
      if (previous && String(previous.title).trim() === String(chapter.title).trim()) out[out.length - 1] = chapter
      else out.push(chapter)
    }
    return out
  }

  /** 在线来源的书第一次被打开时，把目录补上（同步阶段只写 metadata）。 */
  async function ensureToc(book) {
    if (book.source === 'local' || !book.sourceId) return null
    const provider = providers[book.source]
    if (!provider) return null
    const toc = await provider.toc({ bookId: book.sourceId })
    const chapters = normalizeOnlineToc(toc.chapters)
    // 微信那边读到哪一章，就用它当本地起点（本地已有进度则不覆盖）。
    const remote = book.remote
    let seed = null
    if (remote && remote.chapterUid > 0) {
      const index = chapters.findIndex((chapter) => String(chapter.chapterId) === String(remote.chapterUid))
      if (index >= 0) seed = { index, percent: Math.max(0, Math.min(100, Number(remote.percent) || 0)) / 100 }
    }
    await store.putBook({
      id: book.id,
      title: book.title,
      author: book.author,
      cover: book.cover,
      source: book.source,
      sourceId: book.sourceId,
      format: 'online',
      chapters: chapters.map((chapter) => ({
        title: chapter.title,
        chapterId: chapter.chapterId,
        locked: chapter.locked === true,
        blocks: null,
      })),
    })
    if (seed !== null && (book.progress?.chapterIndex ?? 0) === 0 && (book.progress?.percent ?? 0) === 0) {
      await store.updateProgress({ bookId: book.id, chapterIndex: seed.index, percent: seed.percent, seconds: 0 })
    }
    return { ...toc, chapters }
  }

  /** 拉一次书架并把没见过的书写进书架（扫码成功后也自动跑一次）。 */
  async function syncShelf(providerId) {
    const provider = providers[providerId]
    let shelf = []
    try {
      shelf = await provider.shelf()
    } catch (error) {
      const code = String(error?.code ?? '')
      const message = error?.message ?? String(error)
      const expired = /-2012|-2013|session-expired|no-session|bad-cookie/.test(code + ' ' + message)
      await store.patchProvider(providerId, expired
        ? { error: message, sessionValid: false }
        : { error: message })
      throw error
    }
    const written = await store.putOnlineBooks(providerId, shelf)
    const added = written.filter((entry) => entry.created).length
    const state = await store.patchProvider(providerId, { error: null, lastSyncAt: Date.now() })
    return { provider: state, added, updated: written.length - added, shelf }
  }

  /**
   * 用户开启「同步进度到微信读书」后把位置写回云端。
   * 平时 60 秒一次（force 时 10 秒），上报的是这段时间真实读/听的秒数。
   */
  const wereadProgressSyncAt = new Map()
  const wereadChapterUids = new Map()
  const wereadPendingSeconds = new Map()
  const WEREAD_HEARTBEAT_MS = 60 * 1000
  /** 最近的写回尝试（排查“为什么没同步”用）。 */
  const wereadSyncLog = []
  /** 客户端上报的关键动作（打开、挂载、听书跳章）。 */
  const clientLog = []
  /** 每一次本地进度写入：谁写的（reader/tts）、章节、旧值。 */
  const progressLog = []
  function noteWereadSync(entry) {
    wereadSyncLog.push({ at: Date.now(), ...entry })
    if (wereadSyncLog.length > 40) wereadSyncLog.shift()
  }
  const WEREAD_FORCE_GAP_MS = 10 * 1000
  const CLOUD_ADOPT_WINDOW_MS = 2 * 60 * 1000
  async function syncWereadProgress(book, options = {}) {
    try {
      const settings = store.state?.settings ?? {}
      if (settings.syncWereadProgress !== true) return
      if (!book || book.source !== 'weread' || !book.sourceId || !book.progress) return
      const force = options.force === true
      const last = wereadProgressSyncAt.get(book.id) ?? 0
      if (Date.now() - last < (force ? WEREAD_FORCE_GAP_MS : WEREAD_HEARTBEAT_MS)) return
      const pending = wereadPendingSeconds.get(book.id) ?? 0
      // 既没有新位置也没有新时长，就别白跑一趟云端
      if (!force && pending <= 0) return
      // 云端章节比本地靠前时，不要用本地位置把它覆盖回去（这是“明明跟到 37 章又跳回 29 章”的根因）
      const remoteIndex = Number(book.remote?.chapterIndex)
      const localIndex = Number(book.progress.chapterIndex) || 0
      if (Number.isFinite(remoteIndex) && remoteIndex > localIndex) {
        noteWereadSync({ ok: false, skipped: 'cloud-ahead', bookId: book.id, title: book.title, localIndex, remoteIndex })
        return
      }
      wereadProgressSyncAt.set(book.id, Date.now())
      wereadPendingSeconds.set(book.id, 0)
      const loaded = await store.readBook(book.id)
      const index = book.progress.chapterIndex
      const chapter = loaded?.chapters?.[index]
      let chapterUid = chapter?.chapterId
      if (chapterUid == null) {
        // 早期入库的章节没有 chapterId：从目录接口补一次，并按书缓存
        let ids = wereadChapterUids.get(book.id)
        if (!ids) {
          const toc = await providers.weread.toc({ bookId: book.sourceId })
          ids = (toc.chapters ?? []).map((item) => item.chapterId)
          wereadChapterUids.set(book.id, ids)
        }
        chapterUid = ids[index]
      }
      if (chapterUid == null) return
      await providers.weread.reportProgress({
        bookId: book.sourceId,
        chapterUid,
        chapterIdx: book.progress.chapterIndex + 1,
        percent: book.progress.percent,
        // 上报这段时间真实读/听的秒数（原来写死 60，阅读时长会被低估）
        seconds: Math.max(1, Math.min(300, Math.round(pending) || 1)),
      })
      noteWereadSync({
        ok: true,
        bookId: book.id,
        title: book.title,
        chapterIndex: book.progress.chapterIndex,
        percent: book.progress.percent,
        seconds: Math.round(pending),
        force,
      })
    } catch (error) {
      // 同步失败不影响阅读：把剩余秒数还回去，下一次进度变化立刻重试
      noteWereadSync({
        ok: false,
        bookId: book?.id,
        title: book?.title,
        error: error?.message ?? String(error),
      })
      wereadProgressSyncAt.delete(book.id)
      if (pending > 0) wereadPendingSeconds.set(book.id, (wereadPendingSeconds.get(book.id) ?? 0) + pending)
    }
  }

  async function handle(request) {
    let url
    try {
      url = new URL(request.url)
    } catch {
      return fail('BADREQ', '请求地址不合法。', 400)
    }
    if (!url.pathname.startsWith(PREFIX)) return fail('NOTFOUND', '没有这个接口。', 404)
    const route = url.pathname.slice(PREFIX.length) || '/'
    const method = request.method.toUpperCase()

    try {
      return await dispatch({ method, route, url, request })
    } catch (error) {
      if (error instanceof ProviderError) {
        return fail(error.code, error.message, error.code === 'AUTH' ? 401 : 400, error.details)
      }
      logger?.warn?.('[lamplit] %s %s 失败：%s', method, route, error?.stack ?? String(error))
      return fail('INTERNAL', error?.message ?? String(error), 500)
    }
  }

  async function readBody(request) {
    const length = Number(request.headers.get('content-length') ?? 0)
    if (Number.isFinite(length) && length > MAX_BODY_BYTES) {
      throw new ProviderError('BADREQ', '文件太大了（上限 120MB）。')
    }
    const text = await request.text()
    if (text.trim() === '') return {}
    try {
      return JSON.parse(text)
    } catch {
      throw new ProviderError('BADREQ', '请求体不是合法 JSON。')
    }
  }

  /** 解析一个文件；返回 { book, toc, warnings, pages } 或抛 ProviderError。 */
  async function importBuffer(buffer, name, extra = {}) {
    const meta = { source: 'local', sourceId: null, ...extra }
    const kind = kindOf(name)
    const fallbackTitle = stripExtension(name)

    if (kind === 'kindle') {
      throw new ProviderError(
        'FORMAT',
        'MOBI / AZW3 / KFX 是亚马逊私有容器，常规阅读器也是先转格式再读。用 Calibre 的 ebook-convert 把它转成 EPUB，再导入灯下。',
      )
    }
    if (kind === '') throw new ProviderError('FORMAT', '支持 .md / .txt / .pdf / .epub。')

    if (kind === 'md') {
      const parsed = parseMarkdown(buffer.toString('utf8'), { fallbackTitle })
      const book = await store.putBook(importedBook(parsed, { name, format: 'md', ...meta }))
      return { book, parsed }
    }
    if (kind === 'txt') {
      const parsed = parsePlainText(buffer.toString('utf8'), { fallbackTitle })
      const book = await store.putBook(importedBook(parsed, { name, format: 'txt', ...meta }))
      return { book, parsed }
    }
    if (kind === 'epub') {
      let parsed
      try {
        parsed = parseEpub(buffer)
      } catch (error) {
        throw new ProviderError('FORMAT', `这本 EPUB 读不动：${error?.message ?? String(error)}`)
      }
      const book = await store.putBook(importedBook(parsed, { name, format: 'epub', ...meta }))
      return { book, parsed }
    }

    // PDF：有文字层就排版，没有就走影印渲染。
    const analyze = await loadPdfAnalyzer()
    const analysis = analyze(new Uint8Array(buffer), { fallbackTitle })
    if (analysis.mode === 'scan') {
      const book = await store.putBook({
        title: analysis.title || fallbackTitle || '影印 PDF',
        author: '',
        source: meta.source,
        sourceId: meta.sourceId,
        format: 'pdf-scan',
        chapters: [],
        pages: analysis.pageCount,
      })
      await store.saveFile(book.id, buffer, 'pdf')
      return { book: { ...book, pages: analysis.pageCount }, parsed: { chapters: [], warnings: analysis.warnings } }
    }
    const book = await store.putBook({
      ...importedBook(analysis, { name, format: 'pdf', ...meta }),
      pages: analysis.pageCount,
    })
    return { book, parsed: { ...analysis, warnings: analysis.warnings } }
  }

  async function dispatch({ method, route, url, request }) {
    // ---- 状态与设置 ---------------------------------------------------------
    if (method === 'GET' && route === '/api/state') {
      const snapshot = await store.stateSnapshot()
      const counts = await notes.counts()
      return json({
        ok: true,
        ...snapshot,
        shelf: snapshot.shelf.map((book) => ({ ...book, notes: counts.get(book.id) ?? 0 })),
      })
    }
    if (method === 'POST' && route === '/api/settings') {
      const body = await readBody(request)
      const settings = await store.patchSettings(body.patch ?? body)
      return json({ ok: true, settings })
    }

    // ---- 导入 ---------------------------------------------------------------
    if (method === 'POST' && route === '/api/import') {
      const body = await readBody(request)
      const name = String(body.name ?? '').trim()
      const base64 = String(body.dataBase64 ?? '')
      if (base64 === '') throw new ProviderError('BADREQ', '文件内容是空的。')
      const buffer = Buffer.from(base64, 'base64')
      if (buffer.byteLength === 0) throw new ProviderError('BADREQ', '文件解码后是空的。')
      const { book, parsed } = await importBuffer(buffer, name)
      const loaded = await store.readBook(book.id)
      return json({
        ok: true,
        book,
        toc: tocOf(book, loaded?.chapters ?? []),
        warnings: parsed.warnings ?? [],
        pages: parsed.pageCount ?? book.pages ?? 0,
      })
    }

    if (method === 'POST' && route === '/api/import-path') {
      const body = await readBody(request)
      const filePath = String(body.path ?? '').trim()
      if (filePath === '') throw new ProviderError('BADREQ', '缺少文件路径。')
      const stats = await fsp.stat(filePath).catch(() => null)
      if (!stats || !stats.isFile()) throw new ProviderError('NOTFOUND', `找不到文件：${filePath}`)
      if (stats.size > MAX_BODY_BYTES) throw new ProviderError('BADREQ', '文件超过 120MB。')
      const buffer = await fsp.readFile(filePath)
      const { book, parsed } = await importBuffer(buffer, path.basename(filePath))
      const loaded = await store.readBook(book.id)
      return json({ ok: true, book, toc: tocOf(book, loaded?.chapters ?? []), warnings: parsed.warnings ?? [] })
    }

    // 粘贴正文：番茄正文有字体反爬时的逃生口，也方便贴一段文章。
    if (method === 'POST' && route === '/api/import-text') {
      const body = await readBody(request)
      const text = String(body.text ?? '').trim()
      if (text === '') throw new ProviderError('BADREQ', '正文是空的。')
      const title = String(body.title ?? '').trim() || '粘贴的一章'
      const parsed = parsePlainText(text, { fallbackTitle: title, author: String(body.author ?? '').trim() })
      const book = await store.putBook(importedBook(parsed, { name: `${title}.txt`, format: 'txt', source: 'local' }))
      const loaded = await store.readBook(book.id)
      return json({ ok: true, book, toc: tocOf(book, loaded?.chapters ?? []) })
    }

    // ---- 原始文件（影印 PDF 用） -------------------------------------------
    const fileMatch = /^\/api\/book\/([A-Za-z0-9_]+)\/file$/.exec(route)
    if (fileMatch && method === 'GET') {
      const id = fileMatch[1]
      const book = await store.book(id)
      if (book === null) return fail('NOTFOUND', '这本书已经不在书库里了（可能被书架同步移除）。', 404)
      const file = store.filePath(id, 'pdf')
      const stats = await fsp.stat(file).catch(() => null)
      if (!stats) return fail('NOTFOUND', '这本书没有保存原始文件。', 404)
      const data = await fsp.readFile(file)
      const total = data.byteLength
      const range = /bytes=(\d*)-(\d*)/.exec(request.headers.get('range') ?? '')
      if (range) {
        const start = range[1] === '' ? Math.max(total - Number(range[2] || 0), 0) : Number(range[1])
        const end = range[2] === '' ? total - 1 : Math.min(Number(range[2]), total - 1)
        if (start > end || start >= total) {
          return new Response(null, { status: 416, headers: { 'content-range': `bytes */${total}` } })
        }
        const slice = data.subarray(start, end + 1)
        return new Response(slice, {
          status: 206,
          headers: {
            'content-type': 'application/pdf',
            'content-length': String(slice.byteLength),
            'content-range': `bytes ${start}-${end}/${total}`,
            'accept-ranges': 'bytes',
            'cache-control': 'private, max-age=3600',
          },
        })
      }
      return new Response(data, {
        status: 200,
        headers: {
          'content-type': 'application/pdf',
          'content-length': String(total),
          'content-disposition': `inline; filename="${encodeURIComponent(book.title)}.pdf"`,
          'accept-ranges': 'bytes',
          'cache-control': 'private, max-age=3600',
        },
      })
    }

    // ---- 今日一句（从你自己在读的书里取，不编） ----------------------------
    if (method === 'GET' && route === '/api/quote') {
      const quote = await dailyQuote(store)
      return json({ ok: true, quote })
    }

    // ---- 导出 ---------------------------------------------------------------
    if (method === 'GET' && route === '/api/notes/export') {
      const bookId = url.searchParams.get('bookId') ?? undefined
      const items = await notes.list({ bookId })
      const book = bookId ? await store.book(bookId) : null
      const markdown = notesToMarkdown(items, { bookTitle: book?.title })
      const stamp = new Date().toISOString().slice(0, 10)
      return json({
        ok: true,
        count: items.length,
        filename: `${book ? book.title : '灯下'}-笔记-${stamp}.md`,
        markdown,
      })
    }
    if (method === 'GET' && route === '/api/shelf/export') {
      const shelf = (await store.books()).map((book) => ({ ...book })).sort((left, right) => (right.lastOpenedAt || right.addedAt) - (left.lastOpenedAt || left.addedAt))
      const stamp = new Date().toISOString().slice(0, 10)
      return json({ ok: true, filename: `灯下书单-${stamp}.md`, markdown: shelfToMarkdown(shelf) })
    }

    // ---- 笔记 ---------------------------------------------------------------
    if (route === '/api/notes' && method === 'GET') {
      const bookId = url.searchParams.get('bookId') ?? undefined
      const items = await notes.list({ bookId })
      return json({ ok: true, notes: items })
    }
    if (route === '/api/notes' && method === 'POST') {
      const body = await readBody(request)
      if (!body.bookId) return fail('BADREQ', '缺少 bookId。', 400)
      if (String(body.quote ?? '').trim() === '' && String(body.note ?? '').trim() === '') {
        return fail('BADREQ', '引文和笔记都是空的。', 400)
      }
      const book = await store.book(String(body.bookId))
      // 章节名缺了就补上：导出和笔记列表都要靠它定位。
      let chapterTitle = String(body.chapterTitle ?? '').trim()
      const chapterIndex = Number(body.chapterIndex) || 0
      if (chapterTitle === '') {
        const loaded = await store.readBook(String(body.bookId))
        chapterTitle = loaded?.chapters?.[chapterIndex]?.title ?? ''
      }
      const note = await notes.add({
        ...body,
        chapterIndex,
        chapterTitle,
        bookTitle: book?.title ?? '',
      })
      return json({ ok: true, note })
    }
    const noteMatch = /^\/api\/notes\/([A-Za-z0-9_]+)$/.exec(route)
    if (noteMatch && method === 'DELETE') {
      const removed = await notes.remove(noteMatch[1])
      if (!removed) return fail('NOTFOUND', '没有这条笔记。', 404)
      return json({ ok: true })
    }

    // ---- 划词 AI ------------------------------------------------------------
    if (method === 'POST' && route === '/api/ai/ask') {
      const body = await readBody(request)
      // 界面只带 bookId，书名在这里补齐，提示词里才有上下文。
      let bookTitle = String(body.bookTitle ?? '').trim()
      if (bookTitle === '' && body.bookId) {
        const book = await store.book(String(body.bookId))
        bookTitle = book?.title ?? ''
      }
      let chapterTitle = String(body.contextTitle ?? body.chapterTitle ?? '')
      if (chapterTitle === '' && body.bookId !== undefined) {
        const loaded = await store.readBook(String(body.bookId))
        const index = Number(body.chapterIndex)
        if (Number.isFinite(index)) chapterTitle = loaded?.chapters?.[index]?.title ?? ''
      }
      const result = await ai.ask({
        text: body.text,
        mode: body.mode,
        bookTitle,
        chapterTitle,
      })
      return json({ ok: true, ...result })
    }

    // ---- 云端进度：打开一本书时读一次，和本地比时间戳 ------------------------
    const cloudProgressMatch = /^\/api\/book\/([A-Za-z0-9_]+)\/cloud-progress$/.exec(route)
    if (cloudProgressMatch && (method === 'GET' || method === 'POST')) {
      const id = cloudProgressMatch[1]
      const forceAdopt = method === 'POST' ? (await readBody(request)).force === true : false
      const book = await store.book(id)
      if (book === null) return fail('NOTFOUND', '这本书已经不在书库里了（可能被书架同步移除）。', 404)
      const settings = store.state?.settings ?? {}
      if (book.source !== 'weread' || !book.sourceId || settings.syncWereadProgress !== true) {
        return json({ ok: true, skipped: true, adopted: false })
      }
      let remote
      try {
        remote = await providers.weread.progress({ bookId: book.sourceId })
      } catch (error) {
        // 用户明确点了「跟随云端」时不能静默失败，否则会以为采纳了、其实还停在本地
        if (forceAdopt) return fail('UPSTREAM', `微信读书暂时读不到进度：${error?.message ?? error}`, 502)
        return json({ ok: true, skipped: true, adopted: false, error: String(error?.message ?? error) })
      }
      // 快照总是刷新，书架卡片才有最新百分比
      await store.patchRemote(id, {
        percent: remote.percent,
        chapterUid: remote.chapterUid,
        chapterIndex: -1,
        readingTime: remote.readingTime,
        updatedAt: remote.updateTime,
      })
      const cloudMs = remote.updateTime * 1000
      const localMs = Number(book.progress?.updatedAt) || 0
      const localIndex = Number(book.progress?.chapterIndex) || 0
      let loaded = await store.readBook(id)
      if (!loaded || (loaded.chapters ?? []).length === 0) {
        try {
          await ensureToc(book)
          loaded = await store.readBook(id)
        } catch {}
      }
      let index = (loaded?.chapters ?? []).findIndex((item) => String(item.chapterId) === String(remote.chapterUid))
      if (index < 0) {
        try {
          const toc = await providers.weread.toc({ bookId: book.sourceId })
          index = (toc.chapters ?? []).findIndex((item) => String(item.chapterId) === String(remote.chapterUid))
        } catch {}
      }
      if (index < 0) return json({ ok: true, adopted: false, remote })
      // 采纳规则：**云端为主**。
      //   1) 显式点「跟随云端」——无条件采纳；
      //   2) 云端章节和本地章节不一致——直接跟云端（不再看时间戳：
      //      时间戳会被本机的写回搅乱，导致明明位置不同却判成“本地更新”）。
      // 时间戳只在两端章节相同时用来判断更新鲜的一方。
      const cloudDifferentChapter = index !== localIndex
      const cloudNewer = cloudMs > 0 && cloudMs - localMs >= CLOUD_ADOPT_WINDOW_MS
      if (!forceAdopt && !cloudDifferentChapter && !cloudNewer) {
        return json({ ok: true, adopted: false, remote, chapterIndex: index })
      }
      // 云端只有整书百分比 + 章 uid：落到章首，章内位置归零（已知损耗）
      const updated = await store.updateProgress({ bookId: id, chapterIndex: index, percent: 0, seconds: 0 })
      await store.markAdopted(id, index)
      await store.patchRemote(id, {
        percent: remote.percent,
        chapterUid: remote.chapterUid,
        chapterIndex: index,
        readingTime: remote.readingTime,
        updatedAt: remote.updateTime,
      })
      return json({ ok: true, adopted: true, book: updated ?? book, chapterIndex: index, remote })
    }

    // ---- 单本书 -------------------------------------------------------------
    const bookMatch = /^\/api\/book\/([A-Za-z0-9_]+)(\/chapter\/(\d+))?(\/finished)?$/.exec(route)
    if (bookMatch) {
      const id = bookMatch[1]
      if (bookMatch[4] && method === 'POST') {
        const body = await readBody(request)
        const book = await store.setFinished(id, body.finished === true)
        if (book === null) return fail('NOTFOUND', '这本书已经不在书库里了（可能被书架同步移除）。', 404)
        return json({ ok: true, book })
      }
      if (bookMatch[2] && method === 'GET') {
        const index = Number(bookMatch[3])
        const loaded = await store.readBook(id)
        if (loaded === null) return fail('NOTFOUND', '这本书已经不在书库里了（可能被书架同步移除）。', 404)
        const { book, chapters } = loaded
        let chapter = chapters[index]
        if (!chapter) return fail('NOTFOUND', `没有第 ${index + 1} 章。`, 404)

        if (chapter.blocks === null) {
          if (book.source === 'local') {
            chapter = { ...chapter, blocks: [] }
          } else {
            const provider = providers[book.source]
            if (!provider) throw new ProviderError('UPSTREAM', `不认识来源：${book.source}`)
            const fetched = await provider.chapter({ book, chapter })
            chapter = (await store.patchChapter(id, index, fetched.blocks, fetched.title)) ?? {
              ...chapter,
              blocks: fetched.blocks,
            }
          }
        }
        const bookNotes = await notes.list({ bookId: id })
        return json({
          ok: true,
          book,
          chapter: {
            index,
            title: chapter.title,
            blocks: chapter.blocks ?? [],
            chars: chapter.chars ?? 0,
            locked: chapter.locked === true,
          },
          prev: index > 0 ? index - 1 : null,
          next: index + 1 < chapters.length ? index + 1 : null,
          total: chapters.length,
          notes: bookNotes.filter((note) => note.chapterIndex === index),
        })
      }
      if (method === 'GET') {
        let loaded = await store.readBook(id)
        if (loaded === null) return fail('NOTFOUND', '这本书已经不在书库里了（可能被书架同步移除）。', 404)
        let tocError = null
        const online = loaded.book.source !== 'local'
        const needsToc = loaded.chapters.length === 0
        // 旧目录里可能残留「封面」或重复标题（早期同步留下的），顺手洗一遍。
        const needsClean =
          online &&
          !needsToc &&
          (new Set(['封面', '版权信息', '扉页', '插页', '目录']).has(String(loaded.chapters[0]?.title ?? '').trim()) ||
            loaded.chapters.some((chapter, index) => index > 0 && String(chapter.title) === String(loaded.chapters[index - 1].title)))
        if (online && (needsToc || needsClean)) {
          // 同步只存了书目：第一次打开时补目录，别让用户看到一本空书。
          try {
            await ensureToc(loaded.book)
            loaded = await store.readBook(id)
          } catch (error) {
            tocError = error?.message ?? String(error)
          }
        }
        await store.touch(id)
        return json({
          ok: true,
          book: loaded.book,
          toc: tocOf(loaded.book, loaded.chapters),
          chaptersReady: loaded.chapters.filter((chapter) => Array.isArray(chapter.blocks)).length,
          tocError,
        })
      }
      if (method === 'DELETE') {
        const removed = await store.deleteBook(id)
        if (!removed) return fail('NOTFOUND', '这本书已经不在书库里了（可能被书架同步移除）。', 404)
        await notes.removeBook(id)
        return json({ ok: true })
      }
    }

    if (method === 'POST' && route === '/api/progress') {
      const body = await readBody(request)
      const collected = Math.max(0, Math.min(3600, Number(body.seconds) || 0))
      const before = await store.book(String(body.bookId ?? ''))
      progressLog.push({
        at: Date.now(),
        src: String(body.src ?? 'unknown'),
        epoch: Number(body.epoch) || 0,
        chapterIndex: Number(body.chapterIndex) || 0,
        percent: Number(body.percent) || 0,
        was: Number(before?.progress?.chapterIndex) || 0,
      })
      if (progressLog.length > 120) progressLog.shift()
      const book = await store.updateProgress({
        bookId: String(body.bookId ?? ''),
        chapterIndex: body.chapterIndex,
        percent: body.percent,
        seconds: collected,
      })
      if (book === null) return fail('NOTFOUND', '这本书已经不在书库里了（可能被书架同步移除）。', 404)
      if (collected > 0) wereadPendingSeconds.set(book.id, (wereadPendingSeconds.get(book.id) ?? 0) + collected)
      syncWereadProgress(book, { force: body.force === true })
      return json({ ok: true, book })
    }

    // ---- GitHub 书库 -------------------------------------------------------
    const githubMatch = /^\/api\/discover\/github\/(repos|files|grab|token)$/.exec(route)
    if (githubMatch) {
      const action = githubMatch[1]
      await ensureGithubDefaults()
      const config = await store.githubConfig()

      if (action === 'repos') {
        if (method === 'GET') return json({ ok: true, repos: repoViews(config.repos), hasToken: config.token !== '' })
        if (method === 'POST') {
          const body = await readBody(request)
          const parsed = github.parseSpec(body.spec)
          const normalized = github.specOf(parsed)
          // 加之前先确认仓库与路径真的能列（一次请求，失败就不入库）
          const listed = await github.listFiles({ ...parsed, token: config.token })
          if (listed.files.length === 0) {
            throw new ProviderError('FORMAT', '这个仓库（或这个子目录）里没有 PDF / EPUB / TXT / MD。')
          }
          const saved = await store.patchGithub({ repos: [...config.repos, normalized] })
          return json({ ok: true, repos: repoViews(saved.repos), hasToken: saved.token !== '', files: listed.files.length })
        }
        if (method === 'DELETE') {
          const spec = (url.searchParams.get('spec') ?? '').trim()
          const saved = await store.patchGithub({ repos: config.repos.filter((item) => item !== spec) })
          return json({ ok: true, repos: repoViews(saved.repos), hasToken: saved.token !== '' })
        }
      }

      if (action === 'files' && method === 'GET') {
        const repoSpec = url.searchParams.get('repo') ?? ''
        const subPath = url.searchParams.get('path') ?? ''
        const parsed = github.parseSpec(subPath ? `${repoSpec}#${subPath}` : repoSpec)
        const listed = await github.listFiles({ ...parsed, token: config.token })
        return json({ ok: true, spec: github.specOf(parsed), ...listed })
      }

      if (action === 'grab' && method === 'POST') {
        const body = await readBody(request)
        const parsed = github.parseSpec(String(body.spec ?? body.repo ?? ''))
        const filePath = String(body.path ?? '').trim()
        if (filePath === '') return fail('BADREQ', '缺少文件路径。', 400)
        const listed = await github.listFiles({ ...parsed, token: config.token })
        const file = listed.files.find((item) => item.path === filePath)
        if (!file) return fail('NOTFOUND', '这个文件不在仓库里（或超过 120MB）。', 404)
        const buffer = await github.download({
          owner: parsed.owner,
          repo: parsed.repo,
          branch: listed.branch,
          path: filePath,
          token: config.token,
        })
        const imported = await importBuffer(buffer, file.name, {
          source: 'local',
          sourceId: `github:${parsed.owner}/${parsed.repo}:${filePath}`,
          sourceFile: `https://github.com/${parsed.owner}/${parsed.repo}/blob/${listed.branch}/${filePath}`,
        })
        const loaded = await store.readBook(imported.book.id)
        return json({ ok: true, book: imported.book, toc: tocOf(imported.book, loaded?.chapters ?? []) })
      }

      if (action === 'token') {
        if (method === 'GET') return json({ ok: true, hasToken: config.token !== '' })
        if (method === 'POST') {
          const body = await readBody(request)
          const saved = await store.patchGithub({ token: String(body.token ?? '') })
          return json({ ok: true, hasToken: saved.token !== '' })
        }
      }

      return fail('NOTFOUND', '不支持的 GitHub 请求。', 404)
    }

    // ---- 来源 ---------------------------------------------------------------
    const providerMatch = /^\/api\/provider\/(weread)(\/([a-z-]+))?$/.exec(route)
    if (providerMatch) {
      const providerId = providerMatch[1]
      const provider = providers[providerId]
      const action = providerMatch[3] ?? ''
      if (!provider) return fail('NOTFOUND', '未知来源。', 404)

      if (action === 'login' && method === 'POST') {
        const body = await readBody(request)
        const result = await provider.login({ cookie: String(body.cookie ?? ''), apiKey: String(body.apiKey ?? '') })
        const savedCookie = String(body.cookie ?? '').trim()
        const state = await store.patchProvider(providerId, {
          cookie: savedCookie,
          apiKey: String(body.apiKey ?? '').trim(),
          account: result.account ?? '',
          error: null,
          lastSyncAt: Date.now(),
          refreshToken: /wr_rt=/.test(savedCookie),
        })
        return json({ ok: true, provider: state, shelfCount: result.shelfCount ?? 0 })
      }

      if (action === 'logout' && method === 'POST') {
        const state = await store.patchProvider(providerId, { cookie: '', apiKey: '', account: '', error: null })
        return json({ ok: true, provider: state })
      }

      if (action === 'sync' && method === 'POST') {
        const result = await syncShelf(providerId)
        return json({ ok: true, ...result })
      }

      if (action === 'status' && method === 'GET') {
        if (typeof provider.check !== 'function') return json({ ok: true, valid: null })
        let result
        try {
          result = await provider.check()
        } catch (error) {
          result = { valid: false, reason: error?.message ?? String(error) }
        }
        const state = await store.patchProvider(providerId, {
          sessionValid: result.valid !== false,
          error: result.valid === false ? result.reason ?? '登录已失效' : null,
        })
        return json({ ok: true, valid: result.valid !== false, reason: result.reason ?? '', provider: state })
      }

      // ---- 扫码登录（微信读书）------------------------------------------
      if (providerId === 'weread' && action === 'qr-start' && method === 'POST') {
        for (const [key, entry] of qrSessions) {
          if (Date.now() - entry.startedAt > 6 * 60 * 1000) qrSessions.delete(key)
        }
        let started
        try {
          started = await startQrLogin(undefined)
        } catch (error) {
          throw new ProviderError('NET', `拿不到微信读书登录二维码：${error?.message ?? String(error)}`)
        }
        const entry = { status: 'waiting', startedAt: Date.now(), provider: null, error: null, added: 0 }
        qrSessions.set(started.uid, entry)
        completeQrLogin(started.uid, AbortSignal.timeout(240_000))
          .then(async (result) => {
            const vid = result.vid === undefined || result.vid === null ? '' : String(result.vid)
            await store.patchProvider('weread', {
              cookie: result.cookie,
              apiKey: '',
              account: vid === '' ? '已连接' : `账号 ${vid.slice(0, 6)}…`,
              error: null,
              lastSyncAt: Date.now(),
              refreshToken: /wr_rt=/.test(result.cookie),
            })
            // 扫完先把「已登录」告诉界面，再去拉书架：70 多本要几秒，别让人干等。
            entry.provider = await store.patchProvider('weread', { error: null, lastSyncAt: Date.now() })
            entry.status = 'authed'
            try {
              entry.status = 'syncing'
              const synced = await syncShelf('weread')
              entry.provider = synced.provider
              entry.added = synced.added
              entry.status = 'done'
            } catch (error) {
              entry.provider = await store.patchProvider('weread', {
                error: error?.message ?? String(error),
                sessionValid: error?.code === 'AUTH' ? false : undefined,
              })
              entry.status = 'done'
            }
          })
          .catch((error) => {
            if (entry.status === 'authed' || entry.status === 'syncing') {
              entry.error = error?.message ?? String(error)
              entry.status = 'authed'
            } else {
              entry.status = 'error'
              entry.error = error?.message ?? String(error)
            }
          })
        return json({ ok: true, uid: started.uid, qrSvg: started.qrSvg, expiresAt: started.expiresAt })
      }

      if (providerId === 'weread' && action === 'qr-status' && method === 'GET') {
        const uid = url.searchParams.get('uid') ?? ''
        const entry = qrSessions.get(uid)
        if (!entry) return json({ ok: true, status: 'expired' })
        if (entry.status === 'done') {
          qrSessions.delete(uid)
          return json({ ok: true, status: 'done', provider: entry.provider, added: entry.added })
        }
        if (entry.status === 'authed' || entry.status === 'syncing') {
          return json({ ok: true, status: 'syncing', provider: entry.provider, added: entry.added })
        }
        if (entry.status === 'error') {
          qrSessions.delete(uid)
          return json({ ok: false, status: 'error', code: 'AUTH', error: entry.error })
        }
        return json({ ok: true, status: 'waiting' })
      }

      if (action === 'toc' && method === 'GET') {
        const bookId = url.searchParams.get('bookId') ?? ''
        if (bookId === '') return fail('BADREQ', '缺少 bookId。', 400)
        const toc = await provider.toc({ bookId })
        return json({ ok: true, ...toc })
      }

      if (action === 'open' && method === 'POST') {
        const body = await readBody(request)
        const raw = String(body.bookId ?? '').trim()
        const bookId = raw
        if (bookId === '') return fail('BADREQ', '缺少 bookId 或链接。', 400)
        const toc = await provider.toc({ bookId })
        const cleaned = normalizeOnlineToc(toc.chapters)
        const book = await store.putBook({
          title: body.title || toc.title || bookId,
          author: body.author || toc.author || '',
          cover: toc.cover ?? body.cover ?? null,
          source: providerId,
          sourceId: String(bookId),
          format: 'online',
          chapters: cleaned.map((chapter) => ({
            title: chapter.title,
            chapterId: chapter.chapterId,
            locked: chapter.locked === true,
            blocks: null,
          })),
        })
        const loaded = await store.readBook(book.id)
        return json({ ok: true, book, toc: tocOf(book, loaded?.chapters ?? []) })
      }

      if (action === 'search' && method === 'GET') {
        const query = (url.searchParams.get('q') ?? '').trim()
        if (query === '') return fail('BADREQ', '缺少关键词。', 400)
        if (typeof provider.search !== 'function') {
          return fail('UNSUPPORTED', '这个来源暂不支持站内搜索。', 400)
        }
        const results = await provider.search(query)
        return json({ ok: true, results })
      }

      // 图书广场：微信读书分类页（真实书目，30 分钟缓存）
      if (action === 'browse' && method === 'GET') {
        if (typeof provider.browse !== 'function') return fail('UNSUPPORTED', '这个来源没有书城。', 400)
        const cache = browseCache.weread
        if (cache && Date.now() - cache.at < 30 * 60 * 1000) {
          return json({ ok: true, books: cache.books, cached: true })
        }
        const books = await provider.browse()
        browseCache.weread = { at: Date.now(), books }
        return json({ ok: true, books })
      }

      // 图书广场：微信读书书城的搜索（页面内嵌 JSON，不需要 API Key）
      if (action === 'store' && method === 'GET') {
        const query = (url.searchParams.get('q') ?? '').trim()
        if (query === '') return fail('BADREQ', '缺少关键词。', 400)
        if (typeof provider.search !== 'function') return fail('UNSUPPORTED', '这个来源没有书城。', 400)
        const results = await provider.search(query)
        return json({ ok: true, query, results })
      }

      return fail('NOTFOUND', `来源接口不存在：${providerId}/${action}`, 404)
    }

    // 诊断：宿主是不是 Electron、能不能开登录窗口。给「扫码/一键登录」用。
    if (method === 'GET' && route === '/api/diag') {
      let electron = null
      try {
        const mod = await import('electron')
        const face = mod && typeof mod.default === 'object' && mod.default !== null ? mod.default : mod
        electron = {
          browserWindow: typeof face.BrowserWindow === 'function',
          session: typeof face.session === 'object',
          viaDefault: face !== mod,
          keys: Object.keys(face).slice(0, 12),
        }
      } catch (error) {
        electron = { error: String(error?.message ?? error) }
      }
      return json({
        ok: true,
        versions: { electron: process.versions.electron ?? null, node: process.versions.node },
        electron,
      })
    }

    if (method === 'GET' && route === '/api/version') {
      const current = await currentVersion()
      const probe = await probeLatestVersion()
      const latest = probe.latest
      return json({
        ok: true,
        current,
        latest: latest || null,
        updateAvailable: latest === '' ? false : compareVersions(latest, current) > 0,
        profile: process.env.DSH_PROFILE ?? null,
        checkedAt: probe.at,
        error: probe.error,
      })
    }

    if (method === 'GET' && route === '/api/health') {
      return json({ ok: true, at: Date.now(), random: crypto.randomBytes(4).toString('hex') })
    }

    // ---- 进度对照：首页「继续阅读」用，只读不采纳 ----------------------------
    if (route === '/api/weread/compare' && method === 'GET') {
      const bookId = url.searchParams.get('bookId') ?? ''
      const book = await store.book(bookId)
      if (book === null) return fail('NOTFOUND', '这本书已经不在书库里了。', 404)
      const chapterCount = Math.max(1, Number(book.chapterCount) || 1)
      const progress = book.progress ?? {}
      const inner = Number(progress.percent) || 0
      const localPercent = Math.round(((Number(progress.chapterIndex) || 0) + inner) / chapterCount * 100)
      const local = {
        chapterIndex: Number(progress.chapterIndex) || 0,
        percent: inner,
        bookPercent: Math.max(0, Math.min(100, localPercent)),
        updatedAt: Number(progress.updatedAt) || 0,
      }
      if (book.source !== 'weread' || !book.sourceId) return json({ ok: true, local, cloud: null })
      let cloud = null
      try {
        const remote = await providers.weread.progress({ bookId: book.sourceId })
        // 云端章 uid → 本地章下标，首页才能显示“云端第几章”
        let chapterIndex = -1
        const loaded = await store.readBook(book.id)
        chapterIndex = (loaded?.chapters ?? []).findIndex((item) => String(item.chapterId) === String(remote.chapterUid))
        if (chapterIndex < 0) {
          try {
            const toc = await providers.weread.toc({ bookId: book.sourceId })
            chapterIndex = (toc.chapters ?? []).findIndex((item) => String(item.chapterId) === String(remote.chapterUid))
          } catch {}
        }
        cloud = {
          chapterUid: remote.chapterUid,
          chapterIdx: remote.chapterIdx,
          chapterIndex,
          percent: remote.percent,
          readingTime: remote.readingTime,
          updateTime: remote.updateTime,
        }
        await store.patchRemote(book.id, {
          percent: remote.percent,
          chapterUid: remote.chapterUid,
          chapterIndex,
          readingTime: remote.readingTime,
          updatedAt: remote.updateTime,
        })
      } catch {
        // 读不到云端就只显示本地
      }
      return json({ ok: true, local, cloud })
    }

    // ---- 客户端行为日志（排查“点了没跳”用，只在内存里） ----------------------
    if (route === '/api/client-log' && method === 'POST') {
      const body = await readBody(request)
      clientLog.push({ at: Date.now(), ...body })
      if (clientLog.length > 120) clientLog.shift()
      return json({ ok: true })
    }

    if (route === '/api/client-log' && method === 'GET') {
      return json({ ok: true, log: clientLog.slice(-60), progress: progressLog.slice(-60) })
    }

    if (route === '/api/weread/sync-log' && method === 'GET') {
      const settings = store.state?.settings ?? {}
      const provider = store.state?.providers?.weread ?? {}
      return json({
        ok: true,
        enabled: settings.syncWereadProgress === true,
        provider: { sessionValid: provider.sessionValid, error: provider.error, lastSyncAt: provider.lastSyncAt },
        log: wereadSyncLog.slice(-20),
      })
    }

    if (route === '/api/tts/stats' && method === 'GET') {
      return json({ ok: true, edge: edgeStatus(), turns: edgeRecent(60), gaps: ttsGaps.slice(-80) })
    }

    if (route === '/api/tts/stats' && method === 'POST') {
      const body = await readBody(request)
      const gaps = Array.isArray(body.gaps) ? body.gaps.slice(0, 80) : []
      for (const gap of gaps) {
        if (typeof gap?.ms !== 'number') continue
        ttsGaps.push({ ms: Math.round(gap.ms), chars: Number(gap.chars) || 0, index: Number(gap.index) || 0, at: Date.now() })
      }
      if (ttsGaps.length > TTS_GAPS_MAX) ttsGaps.splice(0, ttsGaps.length - TTS_GAPS_MAX)
      return json({ ok: true })
    }

    if (route === '/api/tts/warm' && method === 'GET') {
      const voice = url.searchParams.get('voice') ?? ''
      if (!isEdgeVoice(voice)) return json({ ok: true, skipped: true, status: edgeStatus() })
      const warmed = await edgeWarmup()
      return json({ ok: warmed, status: edgeStatus() })
    }

    if (route === '/api/tts' && method === 'GET') {
      const voice = url.searchParams.get('voice') ?? 'yue'
      if (!macosVoiceSupported() && !isKokoroVoice(voice) && !isEdgeVoice(voice)) {
        return fail('UNSUPPORTED', '当前系统不支持该音色，使用浏览器朗读。', 400)
      }
      const text = (url.searchParams.get('text') ?? '').trim()
      if (text === '') return fail('BADREQ', '缺少 text。', 400)
      const rate = Number(url.searchParams.get('rate') ?? '1') || 1
      // stream=1：边合成边回包，长句的首字延迟明显更低
      if (url.searchParams.get('stream') === '1') {
        const result = await synthesizeStream({ text, voice, rate })
        if (result === null) return fail('UPSTREAM', '语音合成失败。', 502)
        return new Response(Readable.toWeb(result.stream), {
          status: 200,
          headers: {
            'content-type': result.mime,
            'cache-control': 'no-store',
            'x-accel-buffering': 'no',
          },
        })
      }
      const result = await synthesizeWav({ text, voice, rate })
      if (result === null) return fail('UPSTREAM', '语音合成失败。', 502)
      return new Response(result.audio, {
        status: 200,
        headers: {
          'content-type': result.mime,
          'cache-control': 'no-store',
          'content-length': String(result.audio.length),
        },
      })
    }

    return fail('NOTFOUND', `没有这个接口：${method} ${route}`, 404)
  }

  return {
    storageDir: store.storageDir,
    handle,
  }
}
