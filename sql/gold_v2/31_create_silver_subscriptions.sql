CREATE TABLE IF NOT EXISTS silver_subscriptions (
    subscription_id   BIGINT PRIMARY KEY,
    user_id           BIGINT,
    subscription_type TEXT,
    started_at        TIMESTAMPTZ,
    ended_at          TIMESTAMPTZ,
    created_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ
);
