CREATE TABLE IF NOT EXISTS edits (
    -- Consumer inserts use ON CONFLICT (event_id) DO NOTHING for replay tolerance.
    event_id uuid PRIMARY KEY,
    title text NOT NULL,
    username text NOT NULL,
    change_type text NOT NULL CHECK (change_type IN ('edit', 'new')),
    event_ts timestamptz NOT NULL,
    bytes_delta integer NOT NULL,
    minor boolean NOT NULL DEFAULT false,
    title_url text,
    comment text, 
    ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_edits_title_ts ON edits (title, event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_edits_ts ON edits (event_ts DESC);

CREATE OR REPLACE VIEW topic_velocity AS
SELECT
    title,
    date_trunc('minute', event_ts) AS bucket,
    count(*) as edit_count,
    sum(bytes_delta) as bytes_delta_sum,
    count(DISTINCT username) as distinct_users
FROM edits
WHERE event_ts >= now() - interval '24 hours'
GROUP BY title, date_trunc('minute', event_ts);