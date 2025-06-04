-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS pools; -- have to drop table which uses these enums first

DROP TYPE IF EXISTS pool_type_enum;
DROP TYPE IF EXISTS token_launch_type;