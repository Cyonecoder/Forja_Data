CREATE TABLE IF NOT EXISTS silver_users (
    user_id         BIGINT PRIMARY KEY,
    email           TEXT,
    phone           TEXT,
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
    -- ajoute ici d'autres colonnes utiles si nécessaire
);
