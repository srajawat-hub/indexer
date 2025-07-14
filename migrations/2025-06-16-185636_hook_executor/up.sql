CREATE TABLE public.hook_executor_orders (
    id bigserial NOT NULL,
    protocol_id integer NOT NULL,
    order_hash character varying(66) NOT NULL,
    order_id bigint NOT NULL,
    recipient character varying(66) NOT NULL,
    token character varying(66) NOT NULL,
    amount NUMERIC NOT NULL,
    timeout_timestamp bigint NOT NULL,
    reason text,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    status integer DEFAULT 0 NOT NULL,
    destination_chain_id bigint NOT NULL,
    additional_data text
);

--
-- Name: hook_executor_orders hook_executor_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hook_executor_orders
    ADD CONSTRAINT hook_executor_orders_pkey PRIMARY KEY (id);

--
-- Name: hook_executor_orders hook_executor_orders_order_hash_address_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hook_executor_orders
    ADD CONSTRAINT hook_executor_orders_order_hash_key UNIQUE (order_hash);

--
-- Name: hook_executor_orders_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.hook_executor_orders_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: hook_executor_orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.hook_executor_orders_id_seq OWNED BY public.hook_executor_orders.id;

--
-- Name: hook_executor_orders id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hook_executor_orders ALTER COLUMN id SET DEFAULT nextval('public.hook_executor_orders_id_seq'::regclass);
