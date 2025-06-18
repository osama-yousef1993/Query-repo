CREATE TABLE IF NOT EXISTS categories_fundamentals
(
    id TEXT,
    name TEXT,
    total_tokens INTEGER,
    index_percentage_24h FLOAT,
    volume_24h FLOAT,
    price_24h FLOAT,
    market_cap FLOAT,
    price_weight_index FLOAT,
    market_cap_weight_index FLOAT,
    market_cap_index_value_24h FLOAT,
    market_cap_index_percentage_24h FLOAT,
    divisor FLOAT,
    top_gainers JSON,
    last_updated TIMESTAMPTZ DEFAULT Now(),
    PRIMARY KEY (id)
);