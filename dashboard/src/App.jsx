import { useCallback, useEffect, useRef, useState } from 'react'
import Pulse from './Pulse'

const REFRESH_MS = 15000

/** Poll one endpoint on an interval. Keeps the last good data on a failed refresh. */
function useEndpoint(path, value, param = 'limit') {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [updatedAt, setUpdatedAt] = useState(null)

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${path}?${param}=${value}`)
      if (!res.ok) throw new Error(`request failed with ${res.status}`)
      setData(await res.json())
      setError(null)
      setUpdatedAt(Date.now())
    } catch (err) {
      setError(err.message)
    }
  }, [path, value, param])

  useEffect(() => {
    load()
    const id = setInterval(load, REFRESH_MS)
    return () => clearInterval(id)
  }, [load])

  return { data, error, updatedAt }
}

/** Re-render once a second so elapsed time stays honest. */
function useElapsed(since) {
  const [, tick] = useState(0)
  useEffect(() => {
    const id = setInterval(() => tick((n) => n + 1), 1000)
    return () => clearInterval(id)
  }, [])
  if (!since) return null
  return Math.floor((Date.now() - since) / 1000)
}

/** Track which keys are new since the previous poll, so arrivals can be marked. */
function useArrivals(items, keyOf) {
  const seen = useRef(null)
  const [fresh, setFresh] = useState(() => new Set())

  useEffect(() => {
    if (!items) return
    const keys = new Set(items.map(keyOf))
    if (seen.current === null) {
      seen.current = keys
      return
    }
    const added = [...keys].filter((k) => !seen.current.has(k))
    seen.current = keys
    if (added.length === 0) return
    setFresh(new Set(added))
    const id = setTimeout(() => setFresh(new Set()), 2000)
    return () => clearTimeout(id)
  }, [items, keyOf])

  return fresh
}

const clock = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
})

const time = (iso) => clock.format(new Date(iso))

const bytes = (n) => `${n > 0 ? '+' : n < 0 ? '\u2212' : ''}${Math.abs(n).toLocaleString()}`

const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`

function Flagged({ rows, error }) {
  const fresh = useArrivals(rows, (r) => `${r.title}|${r.bucket}`)

  if (error) {
    return (
      <p className="notice">
        Can’t reach the API. Retrying every {REFRESH_MS / 1000} seconds.
      </p>
    )
  }
  if (rows === null) return <p className="notice">Loading.</p>
  if (rows.length === 0) {
    return (
      <p className="notice">
        Nothing flagged yet. Pages clear the threshold only when several editors
        converge inside the same hour.
      </p>
    )
  }

  return (
    <ol className="flagged">
      {rows.map((row) => {
        const key = `${row.title}|${row.bucket}`
        return (
          <li
            key={key}
            className={fresh.has(key) ? 'flag is-new' : 'flag'}
          >
            <div className="flag-head">
              <h3 className="flag-title">{row.title}</h3>
              <span className="flag-count">{row.edit_count}</span>
            </div>
            <p className="flag-meta">
              {time(row.bucket)} &nbsp;·&nbsp; {plural(row.distinct_users, 'editor')}
              &nbsp;·&nbsp; {bytes(row.bytes_delta_sum)} bytes &nbsp;·&nbsp; over{' '}
              {row.threshold} at p{row.percentile}
            </p>
          </li>
        )
      })}
    </ol>
  )
}

function Active({ rows, error }) {
  if (error) return <p className="notice">Can’t reach the API.</p>
  if (rows === null) return <p className="notice">Loading.</p>
  if (rows.length === 0) return <p className="notice">No edits in the window.</p>

  return (
    <table className="active">
      <thead>
        <tr>
          <th scope="col">Page</th>
          <th scope="col" className="num">Edits</th>
          <th scope="col" className="num">Editors</th>
          <th scope="col" className="num">Bytes</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={`${row.title}|${row.bucket}`}>
            <th scope="row">
              <span className="page">{row.title}</span>
              <span className="stamp">{time(row.bucket)}</span>
            </th>
            <td className="num">{row.edit_count}</td>
            <td className="num">{row.distinct_users}</td>
            <td
              className={
                row.bytes_delta_sum > 0
                  ? 'num gain'
                  : row.bytes_delta_sum < 0
                    ? 'num loss'
                    : 'num'
              }
            >
              {bytes(row.bytes_delta_sum)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

export default function App() {
  const anomalies = useEndpoint('/api/anomalies', 12)
  const velocity = useEndpoint('/api/velocity', 15)
  const timeline = useEndpoint('/api/timeline', 120, 'minutes')

  const newest = Math.max(anomalies.updatedAt ?? 0, velocity.updatedAt ?? 0)
  const elapsed = useElapsed(newest || null)
  const stale = anomalies.error || velocity.error

  return (
    <div className="shell">
      <header className="masthead">
        <h1 className="wordmark">wiki&#8202;pulse</h1>
        <p className="status">
          <span className={stale ? 'dot dot-stale' : 'dot'} aria-hidden="true" />
          {stale
            ? 'Reconnecting'
            : elapsed === null
              ? 'Connecting'
              : `Updated ${elapsed}s ago`}
        </p>
      </header>

      <p className="lead">
        Every edit to English Wikipedia, counted by the minute. When several
        people edit the same page within the hour, something usually happened.
      </p>
      
      <Pulse points={timeline.data} error={timeline.error} />
      
      <main className="grid">
        <section className="col col-primary" aria-labelledby="flagged-heading">
          <h2 id="flagged-heading" className="section-title">
            Flagged
          </h2>
          <Flagged rows={anomalies.data} error={anomalies.error} />
        </section>

        <section className="col" aria-labelledby="active-heading">
          <h2 id="active-heading" className="section-title">
            Most active
          </h2>
          <Active rows={velocity.data} error={velocity.error} />
        </section>
      </main>
    </div>
  )
}