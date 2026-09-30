import { spawn } from 'node:child_process'
import { realpath } from 'node:fs/promises'
import { isAbsolute, join, relative, resolve, sep } from 'node:path'

const MAX_OUTPUT = 1024 * 1024

function cargo(args, cwd, signal) {
  return new Promise(done => {
    const child = spawn('cargo', args, { cwd, signal, shell: false })
    let stdout = ''
    let stderr = ''
    let error
    for (const [stream, append] of [
      [child.stdout, chunk => { stdout += chunk }],
      [child.stderr, chunk => { stderr += chunk }],
    ]) {
      stream.on('data', chunk => {
        append(chunk.toString('utf8'))
        if (stdout.length + stderr.length > MAX_OUTPUT && error === undefined) {
          error = 'cargo output exceeded 1 MiB'
          child.kill()
        }
      })
    }
    child.on('error', cause => { error = cause.message })
    child.on('close', (code, terminatedBy) => {
      done({ code, stdout, stderr, error: error ?? (terminatedBy ? `cargo terminated by ${terminatedBy}` : undefined) })
    })
  })
}

function parseSelector(selector) {
  const separator = selector.indexOf('::')
  if (separator < 1) throw new Error('expected <crate-directory>::<fully-qualified-test-name>')
  const directory = selector.slice(0, separator)
  const test = selector.slice(separator + 2)
  if (isAbsolute(directory) || directory.split('/').some(part => !part || part === '.' || part === '..') ||
      !/^[A-Za-z_][A-Za-z0-9_:]*$/.test(test)) {
    throw new Error('expected a project-relative crate directory and a fully qualified Rust test name')
  }
  return { directory, test }
}

async function select(request, selector) {
  const { directory, test } = parseSelector(selector)
  const root = await realpath(request.projectRoot)
  const manifest = await realpath(join(root, directory, 'Cargo.toml'))
  const pathFromRoot = relative(root, manifest)
  if (pathFromRoot === '..' || pathFromRoot.startsWith(`..${sep}`) || isAbsolute(pathFromRoot)) {
    throw new Error('crate manifest escapes project root')
  }
  const args = ['test', '--manifest-path', manifest, '--all-features', '--lib', '--tests', '--', '--list']
  const listed = await cargo(args, request.cwd, request.signal)
  if (listed.error || listed.code !== 0) throw new Error(listed.error ?? (listed.stderr.trim() || 'cargo test --list failed'))
  const matches = listed.stdout.split(/\r?\n/).filter(line => line === `${test}: test`)
  if (matches.length !== 1) throw new Error(`expected exactly one test ${test}, found ${matches.length}`)
  return { selector, targetId: selector, displayName: test }
}

function outcome(run, test) {
  if (run.error) return { status: 'fail', diagnostic: run.error }
  const escaped = test.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const report = new RegExp(`^test ${escaped} \\.\\.\\. (ok|FAILED|ignored(?:,.*)?)$`, 'gm')
  const statuses = [...run.stdout.matchAll(report)]
  if (statuses.length === 1 && statuses[0][1] === 'ok' && run.code === 0) return { status: 'pass' }
  if (statuses.length === 1 && statuses[0][1].startsWith('ignored') && run.code === 0) return { status: 'skip' }
  const diagnostic = (run.stderr + '\n' + run.stdout).trim().slice(-4096)
  return { status: 'fail', diagnostic: diagnostic || `cargo exited with ${run.code}; selected test was not reported` }
}

export default {
  apiVersion: 1,
  async resolve(request) {
    const targets = []
    const errors = []
    for (const selector of request.selectors) {
      try {
        targets.push(await select(request, selector))
      } catch (error) {
        errors.push({ selector, message: error.message })
      }
    }
    return { targets, errors }
  },
  async run(request) {
    const results = []
    for (const target of request.targets) {
      try {
        const selected = await select(request, target.selector)
        if (selected.targetId !== target.targetId) throw new Error('selected test identity changed')
        const { directory, test } = parseSelector(target.selector)
        if (directory === 'plugins/connector/nats' && !test.startsWith('tests::') && process.env.FUJIN_NATS_E2E !== '1') {
          throw new Error('broker-backed NATS evidence requires FUJIN_NATS_E2E=1')
        }
        const manifest = resolve(request.projectRoot, directory, 'Cargo.toml')
        const args = ['test', '--manifest-path', manifest, '--all-features', '--lib', '--tests', '--', '--exact', test, '--test-threads=1']
        const executed = await cargo(args, request.cwd, request.signal)
        results.push({ targetId: target.targetId, ...outcome(executed, test) })
      } catch (error) {
        results.push({ targetId: target.targetId, status: 'fail', diagnostic: error.message })
      }
    }
    return { results }
  },
}
