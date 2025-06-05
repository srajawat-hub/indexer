--
-- PostgreSQL database dump
--

-- Dumped from database version 16.8
-- Dumped by pg_dump version 16.8 (Ubuntu 16.8-1.pgdg22.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_cron; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_cron WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION pg_cron; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_cron IS 'Job scheduler for PostgreSQL';


--
-- Name: hdb_catalog; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA hdb_catalog;


--
-- Name: hdb_pro_catalog; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA hdb_pro_catalog;


--
-- Name: hdb_views; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA hdb_views;


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: gen_hasura_uuid(); Type: FUNCTION; Schema: hdb_catalog; Owner: -
--

CREATE FUNCTION hdb_catalog.gen_hasura_uuid() RETURNS uuid
    LANGUAGE sql
    AS $$select gen_random_uuid()$$;


--
-- Name: insert_event_log(text, text, text, text, json); Type: FUNCTION; Schema: hdb_catalog; Owner: -
--

CREATE FUNCTION hdb_catalog.insert_event_log(schema_name text, table_name text, trigger_name text, op text, row_data json) RETURNS text
    LANGUAGE plpgsql
    AS $$
  DECLARE
    id text;
    payload json;
    session_variables json;
    server_version_num int;
    trace_context json;
  BEGIN
    id := gen_random_uuid();
    server_version_num := current_setting('server_version_num');
    IF server_version_num >= 90600 THEN
      -- In some cases postgres sets the setting to an empty string, which is not a valid json.
      -- NULLIF will convert the empty string to NULL.
      -- Ref: https://github.com/hasura/graphql-engine/issues/8498
      session_variables := NULLIF(current_setting('hasura.user', 't'), '');
      trace_context := NULLIF(current_setting('hasura.tracecontext', 't'), '');
    ELSE
      BEGIN
        session_variables := current_setting('hasura.user');
      EXCEPTION WHEN OTHERS THEN
                  session_variables := NULL;
      END;
      BEGIN
        trace_context := current_setting('hasura.tracecontext');
      EXCEPTION WHEN OTHERS THEN
        trace_context := NULL;
      END;
    END IF;
    payload := json_build_object(
      'op', op,
      'data', row_data,
      'session_variables', session_variables,
      'trace_context', trace_context
    );
    INSERT INTO hdb_catalog.event_log
                (id, schema_name, table_name, trigger_name, payload)
    VALUES
    (id, schema_name, table_name, trigger_name, payload);
    RETURN id;
  END;
$$;


--
-- Name: notify_hasura_intent_changed_INSERT(); Type: FUNCTION; Schema: hdb_catalog; Owner: -
--

CREATE FUNCTION hdb_catalog."notify_hasura_intent_changed_INSERT"() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  DECLARE
    _old record;
    _new record;
    _data json;
  BEGIN
    IF TG_OP = 'UPDATE' THEN
      _old := row((SELECT  "e"  FROM  (SELECT  OLD."gas_fees" , OLD."chain_id" , OLD."id" , OLD."timestamp" , OLD."intent_id" , OLD."transaction_hash" , OLD."transaction_cost_usd" , OLD."transaction_cost" , OLD."gas_token" , OLD."stage" , OLD."version" , OLD."initiator_address" , OLD."order_id"        ) AS "e"      ) );
      _new := row((SELECT  "e"  FROM  (SELECT  NEW."gas_fees" , NEW."chain_id" , NEW."id" , NEW."timestamp" , NEW."intent_id" , NEW."transaction_hash" , NEW."transaction_cost_usd" , NEW."transaction_cost" , NEW."gas_token" , NEW."stage" , NEW."version" , NEW."initiator_address" , NEW."order_id"        ) AS "e"      ) );
    ELSE
    /* initialize _old and _new with dummy values for INSERT and UPDATE events*/
      _old := row((select 1));
      _new := row((select 1));
    END IF;
    _data := json_build_object(
      'old', NULL,
      'new', row_to_json((SELECT  "e"  FROM  (SELECT  NEW."gas_fees" , NEW."chain_id" , NEW."id" , NEW."timestamp" , NEW."intent_id" , NEW."transaction_hash" , NEW."transaction_cost_usd" , NEW."transaction_cost" , NEW."gas_token" , NEW."stage" , NEW."version" , NEW."initiator_address" , NEW."order_id"        ) AS "e"      ) )
    );
    BEGIN
    /* NOTE: formerly we used TG_TABLE_NAME in place of tableName here. However in the case of
    partitioned tables this will give the name of the partitioned table and since we use the table name to
    get the event trigger configuration from the schema, this fails because the event trigger is only created
    on the original table.  */
      IF (TG_OP <> 'UPDATE') OR (_old <> _new) THEN
        PERFORM hdb_catalog.insert_event_log(CAST('public' AS text), CAST('intent_state' AS text), CAST('intent_changed' AS text), TG_OP, _data);
      END IF;
      EXCEPTION WHEN undefined_function THEN
        IF (TG_OP <> 'UPDATE') OR (_old *<> _new) THEN
          PERFORM hdb_catalog.insert_event_log(CAST('public' AS text), CAST('intent_state' AS text), CAST('intent_changed' AS text), TG_OP, _data);
        END IF;
    END;

    RETURN NULL;
  END;
$$;


--
-- Name: notify_hasura_intent_changed_UPDATE(); Type: FUNCTION; Schema: hdb_catalog; Owner: -
--

CREATE FUNCTION hdb_catalog."notify_hasura_intent_changed_UPDATE"() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
  DECLARE
    _old record;
    _new record;
    _data json;
  BEGIN
    IF TG_OP = 'UPDATE' THEN
      _old := row((SELECT  "e"  FROM  (SELECT  OLD."intent_id" , OLD."version" , OLD."initiator_address"        ) AS "e"      ) );
      _new := row((SELECT  "e"  FROM  (SELECT  NEW."intent_id" , NEW."version" , NEW."initiator_address"        ) AS "e"      ) );
    ELSE
    /* initialize _old and _new with dummy values for INSERT and UPDATE events*/
      _old := row((select 1));
      _new := row((select 1));
    END IF;
    _data := json_build_object(
      'old', row_to_json((SELECT  "e"  FROM  (SELECT  OLD."gas_fees" , OLD."chain_id" , OLD."id" , OLD."timestamp" , OLD."intent_id" , OLD."transaction_hash" , OLD."transaction_cost_usd" , OLD."transaction_cost" , OLD."gas_token" , OLD."stage" , OLD."version" , OLD."initiator_address" , OLD."order_id"        ) AS "e"      ) ),
      'new', row_to_json((SELECT  "e"  FROM  (SELECT  NEW."gas_fees" , NEW."chain_id" , NEW."id" , NEW."timestamp" , NEW."intent_id" , NEW."transaction_hash" , NEW."transaction_cost_usd" , NEW."transaction_cost" , NEW."gas_token" , NEW."stage" , NEW."version" , NEW."initiator_address" , NEW."order_id"        ) AS "e"      ) )
    );
    BEGIN
    /* NOTE: formerly we used TG_TABLE_NAME in place of tableName here. However in the case of
    partitioned tables this will give the name of the partitioned table and since we use the table name to
    get the event trigger configuration from the schema, this fails because the event trigger is only created
    on the original table.  */
      IF (TG_OP <> 'UPDATE') OR (_old <> _new) THEN
        PERFORM hdb_catalog.insert_event_log(CAST('public' AS text), CAST('intent_state' AS text), CAST('intent_changed' AS text), TG_OP, _data);
      END IF;
      EXCEPTION WHEN undefined_function THEN
        IF (TG_OP <> 'UPDATE') OR (_old *<> _new) THEN
          PERFORM hdb_catalog.insert_event_log(CAST('public' AS text), CAST('intent_state' AS text), CAST('intent_changed' AS text), TG_OP, _data);
        END IF;
    END;

    RETURN NULL;
  END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: event_invocation_logs; Type: TABLE; Schema: hdb_catalog; Owner: -
--

CREATE TABLE hdb_catalog.event_invocation_logs (
    id text DEFAULT hdb_catalog.gen_hasura_uuid() NOT NULL,
    trigger_name text,
    event_id text,
    status integer,
    request json,
    response json,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: event_log; Type: TABLE; Schema: hdb_catalog; Owner: -
--

CREATE TABLE hdb_catalog.event_log (
    id text DEFAULT hdb_catalog.gen_hasura_uuid() NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    trigger_name text NOT NULL,
    payload jsonb NOT NULL,
    delivered boolean DEFAULT false NOT NULL,
    error boolean DEFAULT false NOT NULL,
    tries integer DEFAULT 0 NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    locked timestamp with time zone,
    next_retry_at timestamp without time zone,
    archived boolean DEFAULT false NOT NULL
);


--
-- Name: hdb_event_log_cleanups; Type: TABLE; Schema: hdb_catalog; Owner: -
--

CREATE TABLE hdb_catalog.hdb_event_log_cleanups (
    id text DEFAULT hdb_catalog.gen_hasura_uuid() NOT NULL,
    trigger_name text NOT NULL,
    scheduled_at timestamp without time zone NOT NULL,
    deleted_event_logs integer,
    deleted_event_invocation_logs integer,
    status text NOT NULL,
    CONSTRAINT hdb_event_log_cleanups_status_check CHECK ((status = ANY (ARRAY['scheduled'::text, 'paused'::text, 'completed'::text, 'dead'::text])))
);


--
-- Name: hdb_source_catalog_version; Type: TABLE; Schema: hdb_catalog; Owner: -
--

CREATE TABLE hdb_catalog.hdb_source_catalog_version (
    version text NOT NULL,
    upgraded_on timestamp with time zone NOT NULL
);


--
-- Name: acknowledgement; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.acknowledgement (
    id bigint NOT NULL,
    intent_id bigint NOT NULL,
    sender_address character varying(44) NOT NULL,
    result boolean NOT NULL,
    error_message text,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    order_id bigint NOT NULL,
    metadata text DEFAULT ''::text
);


--
-- Name: acknowledgement_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.acknowledgement_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: acknowledgement_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.acknowledgement_id_seq OWNED BY public.acknowledgement.id;


--
-- Name: chain_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chain_metadata (
    chain_id text NOT NULL,
    network_name text NOT NULL
);


--
-- Name: deposit_received; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.deposit_received (
    id bigint NOT NULL,
    user_address text NOT NULL,
    token_address text NOT NULL,
    chain_id text NOT NULL,
    amount text NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    source_transaction_hash text,
    message_id text,
    status integer
);


--
-- Name: deposit_received_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.deposit_received_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: deposit_received_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.deposit_received_id_seq OWNED BY public.deposit_received.id;


--
-- Name: intent; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.intent (
    id bigint NOT NULL,
    intent_id bigint NOT NULL,
    owner_address character varying(66) NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    feeamount text DEFAULT ''::text
);


--
-- Name: intent_fees; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.intent_fees (
    id integer NOT NULL,
    intent_id bigint NOT NULL,
    fees jsonb NOT NULL
);


--
-- Name: intent_fees_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.intent_fees_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: intent_fees_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.intent_fees_id_seq OWNED BY public.intent_fees.id;


--
-- Name: intent_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.intent_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: intent_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.intent_id_seq OWNED BY public.intent.id;


--
-- Name: intent_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.intent_state (
    id bigint NOT NULL,
    intent_id bigint NOT NULL,
    version integer NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    stage text NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    gas_fees bigint,
    gas_token text,
    order_id bigint,
    chain_id bigint NOT NULL,
    initiator_address character varying(66) DEFAULT '0x'::character varying NOT NULL,
    transaction_cost text DEFAULT '0'::text,
    transaction_cost_usd text DEFAULT '0'::text
);


--
-- Name: intent_state_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.intent_state_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: intent_state_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.intent_state_id_seq OWNED BY public.intent_state.id;


--
-- Name: message_dispatched_from_vault; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.message_dispatched_from_vault (
    id bigint NOT NULL,
    intent_id bigint NOT NULL,
    sender_address character varying(66) NOT NULL,
    destination_domain_id integer NOT NULL,
    provider integer NOT NULL,
    message text NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    order_id bigint NOT NULL
);


--
-- Name: message_dispatched_from_vault_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.message_dispatched_from_vault_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: message_dispatched_from_vault_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.message_dispatched_from_vault_id_seq OWNED BY public.message_dispatched_from_vault.id;


--
-- Name: order_created; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.order_created (
    id bigint NOT NULL,
    intent_id bigint NOT NULL,
    creator_address character varying(66) NOT NULL,
    token_in character varying(66) NOT NULL,
    token_out character varying(66) NOT NULL,
    amount_in text NOT NULL,
    amount_out text NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    order_id bigint NOT NULL,
    source_chain_id text NOT NULL,
    destination_chain_id text NOT NULL,
    multi_leg boolean DEFAULT false NOT NULL,
    order_payload text DEFAULT ''::text NOT NULL,
    solution_type integer,
    receiver_type integer DEFAULT 0 NOT NULL,
    receiver_address text DEFAULT ''::text,
    amount_in_usd text DEFAULT '0'::text,
    amount_out_usd text DEFAULT '0'::text
);


--
-- Name: order_created_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.order_created_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: order_created_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.order_created_id_seq OWNED BY public.order_created.id;


--
-- Name: received_message_on_vault; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.received_message_on_vault (
    id bigint NOT NULL,
    intent_id bigint NOT NULL,
    origin_domain_id integer NOT NULL,
    sender_address character varying(44) NOT NULL,
    message text NOT NULL,
    provider integer NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    chain_id bigint NOT NULL,
    order_id bigint NOT NULL,
    dln_order_id character varying(255),
    timeout_unix_timestamp_in_sec bigint DEFAULT 0
);


--
-- Name: received_message_on_vault_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.received_message_on_vault_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: received_message_on_vault_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.received_message_on_vault_id_seq OWNED BY public.received_message_on_vault.id;


--
-- Name: sanction_address_list; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sanction_address_list (
    id integer NOT NULL,
    address text NOT NULL
);


--
-- Name: sanction_address_list_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sanction_address_list_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sanction_address_list_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sanction_address_list_id_seq OWNED BY public.sanction_address_list.id;


--
-- Name: solution; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.solution (
    id bigint NOT NULL,
    intent_id bigint NOT NULL,
    solver_address character varying(44) NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);


--
-- Name: solution_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.solution_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: solution_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.solution_id_seq OWNED BY public.solution.id;


--
-- Name: token_chains; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.token_chains (
    id integer NOT NULL,
    token_id text,
    address text,
    decimals integer,
    network text,
    address_bytes32 text
);


--
-- Name: token_chains_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.token_chains_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: token_chains_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.token_chains_id_seq OWNED BY public.token_chains.id;


--
-- Name: tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tokens (
    id text NOT NULL,
    ticker text,
    full_name text,
    is_stable boolean,
    is_tradable boolean,
    price_usd numeric,
    description text,
    launch_date timestamp without time zone,
    website text,
    cmc_id text
);


--
-- Name: acknowledgement id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acknowledgement ALTER COLUMN id SET DEFAULT nextval('public.acknowledgement_id_seq'::regclass);


--
-- Name: deposit_received id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.deposit_received ALTER COLUMN id SET DEFAULT nextval('public.deposit_received_id_seq'::regclass);


--
-- Name: intent id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.intent ALTER COLUMN id SET DEFAULT nextval('public.intent_id_seq'::regclass);


--
-- Name: intent_fees id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.intent_fees ALTER COLUMN id SET DEFAULT nextval('public.intent_fees_id_seq'::regclass);


--
-- Name: intent_state id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.intent_state ALTER COLUMN id SET DEFAULT nextval('public.intent_state_id_seq'::regclass);


--
-- Name: message_dispatched_from_vault id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_dispatched_from_vault ALTER COLUMN id SET DEFAULT nextval('public.message_dispatched_from_vault_id_seq'::regclass);


--
-- Name: order_created id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.order_created ALTER COLUMN id SET DEFAULT nextval('public.order_created_id_seq'::regclass);


--
-- Name: received_message_on_vault id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.received_message_on_vault ALTER COLUMN id SET DEFAULT nextval('public.received_message_on_vault_id_seq'::regclass);


--
-- Name: sanction_address_list id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sanction_address_list ALTER COLUMN id SET DEFAULT nextval('public.sanction_address_list_id_seq'::regclass);


--
-- Name: solution id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.solution ALTER COLUMN id SET DEFAULT nextval('public.solution_id_seq'::regclass);


--
-- Name: token_chains id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.token_chains ALTER COLUMN id SET DEFAULT nextval('public.token_chains_id_seq'::regclass);


--
-- Name: event_invocation_logs event_invocation_logs_pkey; Type: CONSTRAINT; Schema: hdb_catalog; Owner: -
--

ALTER TABLE ONLY hdb_catalog.event_invocation_logs
    ADD CONSTRAINT event_invocation_logs_pkey PRIMARY KEY (id);


--
-- Name: event_log event_log_pkey; Type: CONSTRAINT; Schema: hdb_catalog; Owner: -
--

ALTER TABLE ONLY hdb_catalog.event_log
    ADD CONSTRAINT event_log_pkey PRIMARY KEY (id);


--
-- Name: hdb_event_log_cleanups hdb_event_log_cleanups_pkey; Type: CONSTRAINT; Schema: hdb_catalog; Owner: -
--

ALTER TABLE ONLY hdb_catalog.hdb_event_log_cleanups
    ADD CONSTRAINT hdb_event_log_cleanups_pkey PRIMARY KEY (id);


--
-- Name: hdb_event_log_cleanups hdb_event_log_cleanups_trigger_name_scheduled_at_key; Type: CONSTRAINT; Schema: hdb_catalog; Owner: -
--

ALTER TABLE ONLY hdb_catalog.hdb_event_log_cleanups
    ADD CONSTRAINT hdb_event_log_cleanups_trigger_name_scheduled_at_key UNIQUE (trigger_name, scheduled_at);


--
-- Name: acknowledgement acknowledgement_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.acknowledgement
    ADD CONSTRAINT acknowledgement_pkey PRIMARY KEY (id);


--
-- Name: chain_metadata chain_metadata_network_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chain_metadata
    ADD CONSTRAINT chain_metadata_network_name_key UNIQUE (network_name);


--
-- Name: chain_metadata chain_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chain_metadata
    ADD CONSTRAINT chain_metadata_pkey PRIMARY KEY (chain_id);


--
-- Name: deposit_received deposit_received_message_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.deposit_received
    ADD CONSTRAINT deposit_received_message_id_key UNIQUE (message_id);


--
-- Name: deposit_received deposit_received_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.deposit_received
    ADD CONSTRAINT deposit_received_pkey PRIMARY KEY (id);


--
-- Name: intent_fees intent_fees_intent_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.intent_fees
    ADD CONSTRAINT intent_fees_intent_id_key UNIQUE (intent_id);


--
-- Name: intent_fees intent_fees_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.intent_fees
    ADD CONSTRAINT intent_fees_pkey PRIMARY KEY (id);


--
-- Name: intent intent_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.intent
    ADD CONSTRAINT intent_pkey PRIMARY KEY (id);


--
-- Name: intent_state intent_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.intent_state
    ADD CONSTRAINT intent_state_pkey PRIMARY KEY (id);


--
-- Name: message_dispatched_from_vault message_dispatched_from_vault_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.message_dispatched_from_vault
    ADD CONSTRAINT message_dispatched_from_vault_pkey PRIMARY KEY (id);


--
-- Name: order_created order_created_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.order_created
    ADD CONSTRAINT order_created_pkey PRIMARY KEY (id);


--
-- Name: received_message_on_vault received_message_on_vault_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.received_message_on_vault
    ADD CONSTRAINT received_message_on_vault_pkey PRIMARY KEY (id);


--
-- Name: sanction_address_list sanction_address_list_address_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sanction_address_list
    ADD CONSTRAINT sanction_address_list_address_key UNIQUE (address);


--
-- Name: sanction_address_list sanction_address_list_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sanction_address_list
    ADD CONSTRAINT sanction_address_list_pkey PRIMARY KEY (id);


--
-- Name: solution solution_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.solution
    ADD CONSTRAINT solution_pkey PRIMARY KEY (id);


--
-- Name: token_chains token_chains_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.token_chains
    ADD CONSTRAINT token_chains_pkey PRIMARY KEY (id);


--
-- Name: tokens tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT tokens_pkey PRIMARY KEY (id);


--
-- Name: event_invocation_logs_event_id_idx; Type: INDEX; Schema: hdb_catalog; Owner: -
--

CREATE INDEX event_invocation_logs_event_id_idx ON hdb_catalog.event_invocation_logs USING btree (event_id);


--
-- Name: event_log_fetch_events; Type: INDEX; Schema: hdb_catalog; Owner: -
--

CREATE INDEX event_log_fetch_events ON hdb_catalog.event_log USING btree (locked NULLS FIRST, next_retry_at NULLS FIRST, created_at) WHERE ((delivered = false) AND (error = false) AND (archived = false));


--
-- Name: event_log_trigger_name_idx; Type: INDEX; Schema: hdb_catalog; Owner: -
--

CREATE INDEX event_log_trigger_name_idx ON hdb_catalog.event_log USING btree (trigger_name);


--
-- Name: hdb_source_catalog_version_one_row; Type: INDEX; Schema: hdb_catalog; Owner: -
--

CREATE UNIQUE INDEX hdb_source_catalog_version_one_row ON hdb_catalog.hdb_source_catalog_version USING btree (((version IS NOT NULL)));


--
-- Name: intent_state notify_hasura_intent_changed_INSERT; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER "notify_hasura_intent_changed_INSERT" AFTER INSERT ON public.intent_state FOR EACH ROW EXECUTE FUNCTION hdb_catalog."notify_hasura_intent_changed_INSERT"();


--
-- Name: intent_state notify_hasura_intent_changed_UPDATE; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER "notify_hasura_intent_changed_UPDATE" AFTER UPDATE ON public.intent_state FOR EACH ROW EXECUTE FUNCTION hdb_catalog."notify_hasura_intent_changed_UPDATE"();


--
-- Name: token_chains token_chains_token_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.token_chains
    ADD CONSTRAINT token_chains_token_id_fkey FOREIGN KEY (token_id) REFERENCES public.tokens(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

CREATE TABLE public.pools (
    id                  BIGSERIAL PRIMARY KEY,
    pool_address        TEXT       NOT NULL UNIQUE,
    chain_id            BIGINT     NOT NULL,
    token_0_address     TEXT       NOT NULL,
    token_1_address     TEXT       NOT NULL,
    fee                 NUMERIC    NOT NULL,
    tick_spacing        INTEGER,
    pool_type           TEXT       NOT NULL,
    project_manager     TEXT       NOT NULL,
    block_number        BIGINT     NOT NULL,
    created_at          TIMESTAMP  NOT NULL,
    metadata            JSONB,
    etp_start_time      TIMESTAMP  NOT NULL,
    etp_close_time      TIMESTAMP  NOT NULL,
    launch_type         TEXT       NOT NULL,
    initial_sqrt_price  BIGINT,
    initial_tick        INTEGER,
    token_supply        BIGINT
);

CREATE TABLE public.liquidity (
    id               BIGSERIAL PRIMARY KEY,
    pool_address     TEXT       NOT NULL,
    user_address     TEXT       NOT NULL,
    is_add           BOOLEAN    NOT NULL,
    position_id      TEXT,
    token_0_amount    INTEGER,
    token_1_amount    INTEGER,
    chain_id         BIGINT     NOT NULL,
    timestamp        TIMESTAMP  NOT NULL,
    transaction_hash TEXT       NOT NULL,
    is_manager       BOOLEAN    NOT NULL,
    liquidity        INTEGER,
    fee_amount_0     INTEGER,
    fee_amount_1     INTEGER,
    is_vault         BOOLEAN
);

CREATE TABLE public.swap_amm (
    id                       BIGSERIAL PRIMARY KEY,
    pool_address             TEXT       NOT NULL REFERENCES public.pools(pool_address),
    token_in                 TEXT       NOT NULL,
    token_out                TEXT       NOT NULL,
    amount_in                NUMERIC    NOT NULL,
    amount_out               NUMERIC    NOT NULL,
    amount_in_usd            NUMERIC,
    amount_out_usd           NUMERIC,
    initiator_user_address   TEXT       NOT NULL,
    price                    NUMERIC,
    transaction_hash         TEXT       NOT NULL,
    block_number             BIGINT     NOT NULL,
    timestamp                TIMESTAMP  NOT NULL,
    chain_id                 BIGINT     NOT NULL,
    is_vault_initiated       BOOLEAN    NOT NULL DEFAULT FALSE,
    sqrt_price               BIGINT,
    liquidity                BIGINT,
    tick                     INTEGER
);

CREATE TABLE public.ohlc_price_tables (
    id               BIGSERIAL PRIMARY KEY,
    token_address    TEXT       NOT NULL,
    chain_id         BIGINT     NOT NULL,
    interval         TEXT       NOT NULL,  -- e.g. '1m', '1h', '1d'
    open_price       NUMERIC    NOT NULL,
    high_price       NUMERIC    NOT NULL,
    low_price        NUMERIC    NOT NULL,
    close_price      NUMERIC    NOT NULL,
    volume_token     NUMERIC    NOT NULL,
    volume_usd       NUMERIC,
    timestamp_bucket TIMESTAMP  NOT NULL,  -- start of candle
    pool_address     TEXT       NOT NULL
);

ALTER TABLE public.ohlc_price_tables
    ADD CONSTRAINT ohlc_unique
        UNIQUE(token_address, chain_id, interval, timestamp_bucket, pool_address);


CREATE OR REPLACE PROCEDURE public.refresh_ohlc(p_interval TEXT)
    LANGUAGE plpgsql
AS $$
DECLARE
    v_trunc_unit   TEXT;
    v_bucket_expr  TEXT;
    v_last_bucket  TIMESTAMP;
BEGIN
    -------------------------------------------------------------------
    -- 1) Determine the date_trunc unit (and special‐case 5m)
    -------------------------------------------------------------------
    v_trunc_unit := CASE p_interval
                        WHEN '1m' THEN 'minute'
                        WHEN '5m' THEN 'minute'   -- we’ll adjust to multiples of 5 minutes below
                        WHEN '1h' THEN 'hour'
                        WHEN '1d' THEN 'day'
                        ELSE NULL
        END;

    IF v_trunc_unit IS NULL THEN
        RAISE EXCEPTION 'Unsupported interval: %', p_interval;
    END IF;

    IF p_interval = '5m' THEN
        -- Round down to the nearest 5-minute boundary:
        v_bucket_expr :=
                'date_trunc(''minute'', sa.timestamp)'
                    || ' - (extract(minute from sa.timestamp)::int % 5) * ''1 minute''::interval';
    ELSE
        -- Simple truncation by minute/hour/day:
        v_bucket_expr := format(
                'date_trunc(''%s'', sa.timestamp)',
                v_trunc_unit
        );
    END IF;

    -------------------------------------------------------------------
    -- 2) Find the last candle we wrote for this interval
    -------------------------------------------------------------------
    SELECT COALESCE(MAX(timestamp_bucket), '1970-01-01'::timestamp)
    INTO v_last_bucket
    FROM public.ohlc_price_tables
    WHERE interval = p_interval;

    -------------------------------------------------------------------
    -- 3) Build & run a single dynamic‐SQL block that:
    --    a) selects all swap_amm rows into a CTE “trades”,
    --       computing both the bucket and the ``token_reserve``
    --    b) groups by (token_reserve, chain_id, pool_address, bucket)
    --    c) inserts one candle per group
    -------------------------------------------------------------------
    EXECUTE format($SQL$
    WITH trades AS (
      SELECT
        sa.*,
        -- 1) Compute the bucket (minute/hour/day or 5m)
        %s AS bucket,
        -- 2) Compute the “reserve token” (the lexicographically smaller of the pair)
        LEAST(sa.token_in, sa.token_out) AS token_reserve
      FROM public.swap_amm sa
    )
    INSERT INTO public.ohlc_price_tables (
      token_address, chain_id, interval,
      open_price, high_price, low_price, close_price,
      volume_token, volume_usd,
      timestamp_bucket, pool_address
    )
    SELECT
      t.token_reserve        AS token_address,
      t.chain_id,
      %L                     AS interval,
      -- open_price: the price of the earliest trade in this bucket
      (
        SELECT x.price
        FROM trades x
        WHERE x.token_reserve = t.token_reserve
          AND x.chain_id     = t.chain_id
          AND x.pool_address = t.pool_address
          AND x.bucket       = t.bucket
        ORDER BY x.timestamp
        LIMIT 1
      )                     AS open_price,
      MAX(t.price)          AS high_price,
      MIN(t.price)          AS low_price,
      -- close_price: the price of the latest trade in this bucket
      (
        SELECT x.price
        FROM trades x
        WHERE x.token_reserve = t.token_reserve
          AND x.chain_id     = t.chain_id
          AND x.pool_address = t.pool_address
          AND x.bucket       = t.bucket
        ORDER BY x.timestamp DESC
        LIMIT 1
      )                     AS close_price,
      SUM(t.amount_in)      AS volume_token,
      SUM(t.amount_in_usd)  AS volume_usd,
      t.bucket              AS timestamp_bucket,
      t.pool_address
    FROM trades t
    WHERE t.bucket > %L
    GROUP BY
      t.token_reserve,
      t.chain_id,
      t.pool_address,
      t.bucket
    ON CONFLICT ON CONSTRAINT ohlc_unique DO NOTHING;
  $SQL$,
    -- format() parameters, in order:
               v_bucket_expr,   -- %s → bucket expression (date_trunc or 5-minute logic)
               p_interval,      -- %L → literal interval value (’1m’, ’5m’, ’1h’, ’1d’)
               v_last_bucket    -- %L → literal last_bucket timestamp
        );

END;
$$;


