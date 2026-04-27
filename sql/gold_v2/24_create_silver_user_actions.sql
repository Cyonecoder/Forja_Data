CREATE TABLE IF NOT EXISTS silver_user_likes (
    id          BIGINT PRIMARY KEY,
    user_id     BIGINT,
    content_id  BIGINT,
    created_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS silver_user_favs (
    id          BIGINT PRIMARY KEY,
    user_id     BIGINT,
    content_id  BIGINT,
    created_at  TIMESTAMPTZ
);
