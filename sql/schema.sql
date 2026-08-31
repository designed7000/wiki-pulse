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

CREATE TABLE IF NOT EXISTS anomalies (
    title text              NOT NULL,
    bucket timestamptz      NOT NULL,
    edit_count bigint       NOT NULL, 
    bytes_delta_sum bigint  NOT NULL, 
    distinct_users bigint   NOT NULL,
    threshold numeric       NOT NULL,
    percentile numeric      NOT NULL,
    lookback_hours integer  NOT NULL,
    min_editors integer     NOT NULL,
    flagged_at timestamptz  NOT NULL DEFAULT now(),
    -- Composite key makes DAG re-runs idempotent via ON CONFLICT DO NOTHING.
    PRIMARY KEY (title, bucket)
);
CREATE INDEX IF NOT EXISTS idx_anomalies_bucket ON anomalies (bucket DESC);
-- we index by bucket desc -> meaning that we sort by the most recent ones to be first
-- super helpfull if we want to look for the most recent ones