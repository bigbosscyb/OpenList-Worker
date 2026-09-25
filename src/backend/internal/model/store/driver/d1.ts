/**
 * Cloudflare D1 驱动（SQLite）
 *
 * 环境变量：
 * - DB (Cloudflare D1 binding)
 * - OPENLIST_DB (别名)
 *
 * ── 读复制（Read Replication）────────────────────────────────────────────
 * 开启 D1 读复制后，**只有走 Sessions API 的查询才会落到就近副本**；直接拿
 * 绑定对象发的查询仍然全部打主库。主库是单区域的，本 Worker 跑在 HKG 边缘，
 * 而配置读取（sqlFormat.load，见 format/sql.ts）是 7 条串行 SELECT
 * （1 条 schema_info + 6 张表），跨区域单程约 230ms —— 合计约 1.6s，
 * 每一轮请求都要重付一次（getDb 只有 1s 缓存）。
 *
 * 因此本驱动：
 *   - 读（query / get / list / health）走 withSession(bookmark ?? "first-unconstrained")
 *   - 写（put / delete / execute / batch）由 D1 自动转发主库，写后调
 *     getBookmark() 更新本 isolate 的 bookmark，让后续读至少和这次写一样新
 *
 * 取舍：bookmark 只在本 isolate 内有效。别的 isolate 刚写完、本 isolate 又从未
 * 写过时，可能读到略旧的副本（副本是异步复制的，滞后通常远小于 1s）。
 * 本项目的配置写操作只发生在管理员手动改动时，影响面小。要彻底消除只能把
 * bookmark 经响应头回传客户端再带回，本驱动没有 request 上下文，做不到。
 *
 * 若线上库没开读复制，withSession 依然可用 —— 所有会话查询照样打主库，
 * 只是拿不到加速，不会报错。
 */
import type { Driver } from "../types"
import { buildDdl, KV_SCHEMA_SQLITE } from "../schema"

/**
 * 判断对象是否具备 D1 绑定接口形态。
 *
 * 必须校验：环境变量 `DB` 可能只是「绑定名」字符串，而非绑定对象，
 * 直接使用会得到 "db.prepare is not a function"。
 */
function isD1Like(b: any): boolean {
  if (!b || typeof b !== "object") return false
  try {
    return typeof b.prepare === "function"
  } catch {
    return false
  }
}

/**
 * 获取 D1 绑定。
 *
 * env 与 globalThis 独立检查：env 为真值时不阻断对 globalThis 的探测
 * （EdgeOne Edge Functions 会把绑定注入为全局标识符）。
 */
function getD1(env?: any): any | null {
  const g = typeof globalThis !== "undefined" ? (globalThis as any) : {}

  for (const name of ["DB", "OPENLIST_DB"]) {
    const fromEnv = env?.[name]
    if (isD1Like(fromEnv)) return fromEnv
    const fromGlobal = g?.[name]
    if (isD1Like(fromGlobal)) return fromGlobal
  }

  return null
}

/**
 * 本 isolate 最近一次写产生的 bookmark；null 表示本 isolate 还没写过。
 * 用它开新会话，保证「读到自己刚写的数据」。
 */
let lastBookmark: string | null = null

/**
 * 会话对象按 (db, bookmark) 缓存，**不能每次调用都新建**：
 * ensureSchema() 用 WeakMap 按 db 对象记「已建表」，若每次返回新对象，
 * 那些 CREATE TABLE 会在每个操作上重跑一遍。
 */
let sessionCache: { db: any; bookmark: string | null; session: any } | null = null

/** 取本 isolate 的会话对象；不是真 D1 绑定（测试桩等）或运行时太旧时回退直连。 */
function sessionOf(db: any): any {
  if (typeof db?.withSession !== "function") return db
  if (
    sessionCache &&
    sessionCache.db === db &&
    sessionCache.bookmark === lastBookmark
  ) {
    return sessionCache.session
  }
  try {
    const session = db.withSession(lastBookmark ?? "first-unconstrained")
    sessionCache = { db, bookmark: lastBookmark, session }
    return session
  } catch {
    return db
  }
}

/** 写操作后记录 bookmark，供后续读使用。读操作也会带 bookmark，但内容等价。 */
function rememberBookmark(session: any): void {
  try {
    const bm = session?.getBookmark?.()
    if (bm) lastBookmark = bm
  } catch {
    // 拿不到 bookmark 不影响正确性，只是下一轮读要重新对齐
  }
}

/**
 * 建表。一个 isolate 只需跑一次，但**必须**跑 —— 新 isolate 随时可能第一个
 * 碰到还没建表的库。
 *
 * 缓存的为什么是 Promise 而不是 boolean：并发请求要能共享同一次建表，
 * 否则冷 isolate 上 N 个并发请求会各跑一遍。
 *
 * 为什么必须 batch：KV 表 + schema_info + 7 张列式表 = 9 条 DDL，每条都是一次
 * 独立的跨区域往返（主库在 WNAM，单程约 230ms），串行就是约 2s。而 Cloudflare
 * 会不断新建 isolate，实测约一半请求撞上冷 isolate，表现为同一个接口在
 * 0.25s / 2.5s 之间双峰。batch() 把它们合成一次往返。
 */
const d1Inited = new WeakMap<object, Promise<void>>()

async function ensureSchema(db: any, env?: any): Promise<void> {
  const inited = d1Inited.get(db)
  if (inited) return inited

  // KV 表（map/key 格式）+ 列式表（sql 格式）一并创建
  const ddls = [...KV_SCHEMA_SQLITE, ...buildDdl("sqlite", env)]

  const run = async () => {
    if (typeof db.batch === "function") {
      try {
        await db.batch(ddls.map((ddl: string) => db.prepare(ddl)))
        return
      } catch (err) {
        // batch 走隐式事务；万一遇到不允许 DDL 入批的运行时，退回逐条执行 ——
        // 建表失败会让整个站点不可用，这里不能只指望 batch。
        console.warn("D1 ensureSchema: batch 建表失败，回退逐条执行", err)
      }
    }
    for (const ddl of ddls) await db.prepare(ddl).run()
  }

  const pending = run()
  d1Inited.set(db, pending)
  try {
    await pending
  } catch (err) {
    d1Inited.delete(db) // 失败不留坏缓存，下次请求可以重试
    throw err
  }
}

export const d1Driver: Driver = {
  name: "d1",

  async isAvailable(env?: any): Promise<boolean> {
    return getD1(env) != null
  },

  async init(env?: any): Promise<void> {
    const db = getD1(env)
    if (db) await ensureSchema(db)
  },

  async get(key: string, env?: any): Promise<string | null> {
    const db = getD1(env)
    if (!db) throw new Error("D1 binding not found")

    await ensureSchema(db, env)
    const result = await sessionOf(db)
      .prepare("SELECT value FROM kv WHERE key = ?")
      .bind(key)
      .first()
    return result?.value || null
  },

  async put(key: string, value: string, env?: any): Promise<void> {
    const db = getD1(env)
    if (!db) throw new Error("D1 binding not found")

    await ensureSchema(db, env)
    const session = sessionOf(db)
    await session
      .prepare("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)")
      .bind(key, value)
      .run()
    rememberBookmark(session)
  },

  async delete(key: string, env?: any): Promise<void> {
    const db = getD1(env)
    if (!db) throw new Error("D1 binding not found")

    await ensureSchema(db, env)
    const session = sessionOf(db)
    await session.prepare("DELETE FROM kv WHERE key = ?").bind(key).run()
    rememberBookmark(session)
  },

  async list(prefix: string, env?: any): Promise<string[]> {
    const db = getD1(env)
    if (!db) throw new Error("D1 binding not found")

    await ensureSchema(db, env)
    const result = await sessionOf(db)
      .prepare("SELECT key FROM kv WHERE key LIKE ? ORDER BY key")
      .bind(`${prefix}%`)
      .all()
    return (result.results || []).map((r: any) => r.key)
  },

  async query(sql: string, params: any[], env?: any): Promise<any[]> {
    const db = getD1(env)
    if (!db) throw new Error("D1 binding not found")

    await ensureSchema(db, env)
    const stmt = sessionOf(db).prepare(sql)
    const result = await stmt.bind(...params).all()
    return result.results || []
  },

  async execute(sql: string, params: any[], env?: any): Promise<void> {
    const db = getD1(env)
    if (!db) throw new Error("D1 binding not found")

    await ensureSchema(db, env)
    const session = sessionOf(db)
    const stmt = session.prepare(sql)
    await stmt.bind(...params).run()
    rememberBookmark(session)
  },

  async batch(
    statements: Array<{ sql: string; params: any[] }>,
    env?: any
  ): Promise<void> {
    const db = getD1(env)
    if (!db) throw new Error("D1 binding not found")

    await ensureSchema(db, env)

    // D1 batch 单次语句数上限约 100，分批提交
    const BATCH = 100
    const session = sessionOf(db)
    const stmts = statements.map((s) => session.prepare(s.sql).bind(...s.params))

    for (let i = 0; i < stmts.length; i += BATCH) {
      await session.batch(stmts.slice(i, i + BATCH))
    }
    rememberBookmark(session)
  },

  async health(env?: any): Promise<any> {
    const db = getD1(env)
    if (!db) {
      return {
        configured: false,
        connected: false,
        platform: "Cloudflare D1",
        mode: "d1",
        error: "D1 binding not found (expected env.DB or env.OPENLIST_DB)",
      }
    }

    try {
      await sessionOf(db).prepare("SELECT 1").first()
      return {
        configured: true,
        connected: true,
        platform: "Cloudflare D1",
        mode: "d1",
      }
    } catch (err: any) {
      return {
        configured: true,
        connected: false,
        platform: "Cloudflare D1",
        mode: "d1",
        error: err?.message || String(err),
      }
    }
  },
}
