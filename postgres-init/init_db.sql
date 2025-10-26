CREATE DATABASE backend;

\connect backend;

CREATE TABLE IF NOT EXISTS public.source (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            column1 VARCHAR(50),
            i_column2 INTEGER,
            column3 VARCHAR(50),
            i_column4 INTEGER,
            column5 VARCHAR(50),
            i_column6 INTEGER,
);

CREATE TABLE IF NOT EXISTS public.target (
            id SERIAL PRIMARY KEY,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            column7 VARCHAR(50),
            i_column8 INTEGER,
            column9 VARCHAR(50),
            i_column10 INTEGER,
            column11 VARCHAR(50),
            i_column12 INTEGER,
);

CREATE DATABASE metadata;

\connect metadata;

CREATE TABLE public.transfer_max_dates (
    table_name TEXT PRIMARY KEY,
    max_date DATE,
    updated_at TIMESTAMP,
);

CREATE TABLE public.transfer_metadata (
    id SERIAL PRIMARY KEY,
    source_name TEXT,
    target_name TEXT,
);
