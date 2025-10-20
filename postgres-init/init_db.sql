CREATE DATABASE backend;

\connect backend;

CREATE TABLE IF NOT EXISTS public.order_events (
            id SERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            status VARCHAR(50) NOT NULL,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE DATABASE metadata;

\connect metadata;

CREATE TABLE public.s3_max_dates (
    table_name TEXT PRIMARY KEY,
    max_date DATE,
    updated_at TIMESTAMP
);
