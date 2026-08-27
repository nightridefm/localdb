// Polls the bench page over CDP until window.__bench or window.__error is set.
// Prints progress tail lines; exits 0 on done, 1 on error/timeout.
const DEADLINE = Date.now() + 15 * 60 * 1000

async function main() {
	const targets = await (await fetch('http://127.0.0.1:9222/json')).json()
	const page = targets.find((t) => t.type === 'page' && t.url.includes('127.0.0.1:8077'))
	if (!page) throw new Error('bench page not found')
	const ws = new WebSocket(page.webSocketDebuggerUrl)
	await new Promise((res, rej) => ((ws.onopen = res), (ws.onerror = rej)))
	let id = 0
	const pending = new Map()
	ws.onmessage = (e) => {
		const m = JSON.parse(e.data)
		if (m.id && pending.has(m.id)) {
			pending.get(m.id)(m)
			pending.delete(m.id)
		}
	}
	const evaluate = (expr) =>
		new Promise((res) => {
			const mid = ++id
			pending.set(mid, res)
			ws.send(JSON.stringify({ id: mid, method: 'Runtime.evaluate', params: { expression: expr, returnByValue: true } }))
		})

	let lastLen = 0
	while (Date.now() < DEADLINE) {
		const r = await evaluate(
			'JSON.stringify({e: window.__error, d: window.__bench, n: (window.__progress||[]).length, tail: (window.__progress||[]).slice(-2)})'
		)
		const v = JSON.parse(r.result?.result?.value || '{}')
		if (v.n && v.n > lastLen) {
			console.log(v.tail.join(' | '))
			lastLen = v.n
		}
		if (v.e) {
			console.log('BENCH-ERROR: ' + v.e)
			process.exit(1)
		}
		if (v.d) {
			console.log('BENCH-DONE ' + JSON.stringify(v.d))
			process.exit(0)
		}
		await new Promise((s) => setTimeout(s, 5000))
	}
	console.log('BENCH-TIMEOUT')
	process.exit(1)
}
main().catch((e) => {
	console.log('POLL-FAIL: ' + e.message)
	process.exit(1)
})
