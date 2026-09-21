/**
 * Compatibility shim for the legacy `session/set_model` request.
 *
 * Current ACP SDKs dispatch `session/set_config_option` instead of
 * `session/set_model`, so older clients that call `session/set_model` get
 * "Method not found" even though the agent can change models. Vibe Kanban
 * (Rust `agent-client-protocol` 0.8) is one of those clients, so translate the
 * request here rather than making every client upgrade.
 *
 * `modelId` may carry a thinking level as a `provider/model:level` suffix, which
 * the pi CLI accepts but the session API does not; the suffix is split off and
 * applied through the `thought_level` config option.
 */

const SET_MODEL = 'session/set_model'
const SET_CONFIG_OPTION = 'session/set_config_option'

type Json = Record<string, unknown>

type Pending = {
  /** The client's original request id. */
  clientId: unknown
  /** Synthetic ids still awaiting a response. */
  remaining: string[]
  /** First error seen for a synthetic request, if any. */
  error?: unknown
}

function parse(line: string): Json | undefined {
  try {
    const value = JSON.parse(line)
    return typeof value === 'object' && value !== null ? (value as Json) : undefined
  } catch {
    return undefined
  }
}

function splitModelId(modelId: string): { model: string; thoughtLevel?: string } {
  const at = modelId.lastIndexOf(':')
  if (at <= 0) return { model: modelId }
  const level = modelId.slice(at + 1)
  return level ? { model: modelId.slice(0, at), thoughtLevel: level } : { model: modelId }
}

function configOption(id: string, sessionId: string, configId: string, value: string): Json {
  return { jsonrpc: '2.0', id, method: SET_CONFIG_OPTION, params: { sessionId, configId, value } }
}

/**
 * Wrap the ACP streams, rewriting `session/set_model` into config-option calls.
 *
 * Returns the same shapes `ndJsonStream` expects, so the agent stays unaware.
 */
export function withSetModelShim(
  encode: (message: Json) => string,
  toAgent: ReadableStream<Uint8Array>,
  fromAgent: WritableStream<Uint8Array>
): { toAgent: ReadableStream<Uint8Array>; fromAgent: WritableStream<Uint8Array> } {
  const pending = new Map<string, Pending>()
  let counter = 0

  const decoder = new TextDecoder()
  const reader = toAgent.getReader()
  const writer = fromAgent.getWriter()

  const rewritten = new ReadableStream<Uint8Array>({
    async start(controller) {
      for (;;) {
        const { value, done } = await reader.read()
        if (done) break
        for (const line of decoder.decode(value).split('\n')) {
          const trimmed = line.trim()
          if (!trimmed) continue
          const message = parse(trimmed)
          if (!message || message.method !== SET_MODEL) {
            controller.enqueue(new TextEncoder().encode(`${trimmed}\n`))
            continue
          }
          const params = (message.params ?? {}) as Json
          const sessionId = String(params.sessionId ?? '')
          const { model, thoughtLevel } = splitModelId(String(params.modelId ?? ''))
          const rewrittenMessages: Json[] = [configOption(`__set-model-${counter}-model`, sessionId, 'model', model)]
          if (thoughtLevel) {
            rewrittenMessages.push(
              configOption(`__set-model-${counter}-thought_level`, sessionId, 'thought_level', thoughtLevel)
            )
          }
          pending.set(String(message.id), {
            clientId: message.id,
            remaining: rewrittenMessages.map(entry => String(entry.id))
          })
          counter += 1
          controller.enqueue(new TextEncoder().encode(`${rewrittenMessages.map(entry => encode(entry)).join('\n')}\n`))
        }
      }
      controller.close()
    }
  })

  const intercepted = new WritableStream<Uint8Array>({
    async write(chunk) {
      const lines = decoder
        .decode(chunk)
        .split('\n')
        .filter(line => line.trim())
      const forwarded: Json[] = []
      const responses: Json[] = []
      for (const line of lines) {
        const message = parse(line)
        const synthetic = message && typeof message.id === 'string' ? message.id : undefined
        const target = synthetic?.startsWith('__set-model-')
          ? [...pending.values()].find(entry => entry.remaining.includes(synthetic))
          : undefined
        if (!message || !synthetic || !target) {
          forwarded.push(message ?? (JSON.parse(line) as Json))
          continue
        }
        target.remaining = target.remaining.filter(id => id !== synthetic)
        if (message.error && !target.error) target.error = message.error
        if (target.remaining.length === 0) {
          const entry = [...pending.entries()].find(([, value]) => value === target)
          if (entry) pending.delete(entry[0])
          responses.push(
            target.error
              ? { jsonrpc: '2.0', id: target.clientId, error: target.error }
              : { jsonrpc: '2.0', id: target.clientId, result: {} }
          )
        }
      }
      const out = [...forwarded, ...responses].map(encode).join('\n')
      if (out) await writer.write(new TextEncoder().encode(`${out}\n`))
    },
    async close() {
      await writer.close()
    }
  })

  return { toAgent: rewritten, fromAgent: intercepted }
}
