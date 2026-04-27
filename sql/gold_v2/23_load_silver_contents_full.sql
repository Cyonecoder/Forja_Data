BEGIN;

DELETE FROM silver_contents;

-- On importe d'abord tous les contents bruts
CREATE TEMP TABLE tmp_contents AS
SELECT
    id,
    slug,
    type,
    program_id,
    name_short,
    name_long,
    created_at,
    updated_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT id, slug, type, program_id, name_short, name_long, created_at, updated_at FROM contents'
) AS t(
    id BIGINT,
    slug TEXT,
    type TEXT,
    program_id BIGINT,
    name_short JSONB,
    name_long JSONB,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- Self-join pour reconstruire la hiérarchie
WITH parent AS (
    SELECT
        c.id,
        c.slug AS episode_slug,
        c.type,
        c.program_id,
        p.slug AS season_slug,
        p.program_id AS series_id
    FROM tmp_contents c
    LEFT JOIN tmp_contents p ON c.program_id = p.id
),
series AS (
    SELECT
        p.id,
        p.episode_slug,
        p.type,
        p.program_id,
        p.season_slug,
        s.slug AS series_slug
    FROM parent p
    LEFT JOIN tmp_contents s ON p.series_id = s.id
)
INSERT INTO silver_contents (
    content_id,
    slug,
    type,
    program_id,
    series_slug,
    season_slug,
    episode_slug,
    name_short,
    name_long,
    created_at,
    updated_at
)
SELECT
    c.id,
    c.slug,
    c.type,
    c.program_id,
    s.series_slug,
    s.season_slug,
    s.episode_slug,
    c.name_short,
    c.name_long,
    c.created_at,
    c.updated_at
FROM tmp_contents c
LEFT JOIN series s ON c.id = s.id;

DROP TABLE tmp_contents;

COMMIT;
