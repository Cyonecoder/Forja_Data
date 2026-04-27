BEGIN;


DELETE FROM silver_users;


INSERT INTO silver_users (
    user_id,
    email,
    phone,
    role,
    is_active,
    customer_id,
    subscription_id,
    subscription_start,
    subscription_renewal,
    subscription_end,
    cancel_subscription,
    free_subscriptions,
    lang,
    profile,
    credits,
    owner_id,
    cdn_quota,
    encoder_quota,
    cdn_usage,
    access_id,
    created_at,
    updated_at
)
SELECT
    id,
    email,
    phone,
    role,
    is_active,
    customer_id,
    subscription_id,
    subscription_start,
    subscription_renewal,
    subscription_end,
    cancel_subscription,
    free_subscriptions,
    lang,
    profile,
    credits,
    owner_id,
    cdn_quota,
    encoder_quota,
    cdn_usage,
    access_id,
    created_at,
    updated_at
FROM dblink(
    'host=169.255.179.24 port=5432 dbname=snrt_stats user=snrt_readonly password=6AOd3Dm2',
    'SELECT
        id,
        email,
        phone,
        role,
        is_active,
        customer_id,
        subscription_id,
        subscription_start,
        subscription_renewal,
        subscription_end,
        cancel_subscription,
        free_subscriptions,
        lang,
        profile,
        credits,
        owner_id,
        cdn_quota,
        encoder_quota,
        cdn_usage,
        access_id,
        created_at,
        updated_at
    FROM users'
) AS t(
    id                   BIGINT,
    email                TEXT,
    phone                TEXT,
    role                 TEXT,
    is_active            BOOLEAN,
    customer_id          TEXT,
    subscription_id      BIGINT,
    subscription_start   TIMESTAMPTZ,
    subscription_renewal TIMESTAMPTZ,
    subscription_end     TIMESTAMPTZ,
    cancel_subscription  BOOLEAN,
    free_subscriptions   INT,
    lang                 TEXT,
    profile              JSONB,
    credits              REAL,
    owner_id             BIGINT,
    cdn_quota            BIGINT,
    encoder_quota        BIGINT,
    cdn_usage            BIGINT,
    access_id            BIGINT,
    created_at           TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ
);


COMMIT;