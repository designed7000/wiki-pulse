import { useMemo } from 'react'

/**
 * Edit volume per minute, drawn as a filled area with the most recent minute
 * marked. Hand-drawn rather than charted: one series, no axes, no legend.
 */
export default function Pulse({ points, error }) {
  const shape = useMemo(() => {
    if (!points || points.length < 2) return null

    const w = 1000
    const h = 120
    const peak = Math.max(...points.map((p) => p.edits))
    const step = w / (points.length - 1)

    const coords = points.map((p, i) => [
      i * step,
      h - (p.edits / peak) * (h - 8),
    ])

    const line = coords
      .map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x.toFixed(1)} ${y.toFixed(1)}`)
      .join(' ')

    return {
      w,
      h,
      peak,
      line,
      area: `${line} L${w} ${h} L0 ${h} Z`,
      last: coords[coords.length - 1],
    }
  }, [points])

  if (error || !shape) {
    return <div className="pulse pulse-empty" aria-hidden="true" />
  }

  const total = points.reduce((sum, p) => sum + p.edits, 0)
  const perMinute = Math.round(total / points.length)

  return (
    <figure className="pulse">
      <svg
        viewBox={`0 0 ${shape.w} ${shape.h}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={`Edit volume over the last ${points.length} minutes, averaging ${perMinute} edits a minute.`}
      >
        <path className="pulse-area" d={shape.area} />
        <path className="pulse-line" d={shape.line} />
        <circle className="pulse-now" cx={shape.last[0]} cy={shape.last[1]} r="4" />
      </svg>
      <figcaption className="pulse-caption">
        <span>{points.length} minutes</span>
        <span>{perMinute} edits a minute</span>
        <span>peak {shape.peak}</span>
      </figcaption>
    </figure>
  )
}