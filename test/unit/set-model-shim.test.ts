import test from 'node:test'
import assert from 'node:assert/strict'
import { withSetModelShim } from '../../src/acp/set-model-shim.js'

const encode = (message: unknown) => JSON.stringify(message)

function streamOf(lines: unknown[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const line of lines) controller.enqueue(new TextEncoder().encode(`${JSON.stringify(line)}\n`))
      controller.close()
    }
  })
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<any[]> {
  const out: any[] = []
  const decoder = new TextDecoder()
  for await (const chunk of stream) {
    for (const line of decoder.decode(chunk).split('\n')) if (line.trim()) out.push(JSON.parse(line))
  }
  return out
}

function sink(): { stream: WritableStream<Uint8Array>; sent: any[] } {
  const sent: any[] = []
  const decoder = new TextDecoder()
  return {
    sent,
    stream: new WritableStream<Uint8Array>({
      write(chunk) {
        for (const line of decoder.decode(chunk).split('\n')) if (line.trim()) sent.push(JSON.parse(line))
      }
    })
  }
}

test('set_model shim: translates to model config option and passes other messages through', async () => {
  const toAgent = streamOf([
    { jsonrpc: '2.0', id: 1, method: 'session/new', params: { cwd: '/tmp' } },
    { jsonrpc: '2.0', id: 2, method: 'session/set_model', params: { sessionId: 's-1', modelId: 'kimi-coding/k3' } }
  ])
  const back = sink()
  const shimmed = withSetModelShim(encode, toAgent, back.stream)
  const forwarded = await collect(shimmed.toAgent)

  assert.deepEqual(forwarded[0], { jsonrpc: '2.0', id: 1, method: 'session/new', params: { cwd: '/tmp' } })
  assert.equal(forwarded[1].method, 'session/set_config_option')
  assert.deepEqual(forwarded[1].params, { sessionId: 's-1', configId: 'model', value: 'kimi-coding/k3' })
  assert.equal(forwarded.length, 2, 'one legacy request becomes exactly one config-option call')
})

test('set_model shim: applies a :level suffix as the thought_level option', async () => {
  const toAgent = streamOf([
    {
      jsonrpc: '2.0',
      id: 7,
      method: 'session/set_model',
      params: { sessionId: 's-2', modelId: 'kimi-coding/k3:xhigh' }
    }
  ])
  const back = sink()
  const shimmed = withSetModelShim(encode, toAgent, back.stream)
  const forwarded = await collect(shimmed.toAgent)

  assert.equal(forwarded.length, 2)
  assert.deepEqual(forwarded[0].params, { sessionId: 's-2', configId: 'model', value: 'kimi-coding/k3' })
  assert.deepEqual(forwarded[1].params, { sessionId: 's-2', configId: 'thought_level', value: 'xhigh' })
})

test('set_model shim: answers the client once both option calls have completed', async () => {
  const toAgent = streamOf([
    { jsonrpc: '2.0', id: 9, method: 'session/set_model', params: { sessionId: 's-3', modelId: 'kimi-coding/k3:high' } }
  ])
  const back = sink()
  const shimmed = withSetModelShim(encode, toAgent, back.stream)
  const forwarded = await collect(shimmed.toAgent)
  const writer = shimmed.fromAgent.getWriter()

  // An unrelated agent notification must not be swallowed.
  await writer.write(new TextEncoder().encode(`${JSON.stringify({ jsonrpc: '2.0', method: 'session/update' })}\n`))
  for (const message of forwarded) {
    await writer.write(new TextEncoder().encode(`${JSON.stringify({ jsonrpc: '2.0', id: message.id, result: {} })}\n`))
  }
  await writer.close()

  assert.deepEqual(back.sent[0], { jsonrpc: '2.0', method: 'session/update' })
  assert.deepEqual(back.sent[1], { jsonrpc: '2.0', id: 9, result: {} })
  assert.equal(back.sent.length, 2, 'both synthetic responses collapse into one client response')
})
