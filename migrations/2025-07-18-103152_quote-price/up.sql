-- Your SQL goes here
CREATE TABLE intent_quotes (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL UNIQUE,
    quotes JSONB NOT NULL,
    "timestamp" timestamp without time zone NOT NULL 
)