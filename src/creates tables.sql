create table cdm.dm_courier_ledger(
    id serial primary key,
    courier_id  text unique ,
    courier_name text,
    settlement_year int,
    settlement_month int check (settlement_month > 0 and settlement_month < 13),
    orders_count int,
    orders_total_sum numeric(14,2),
    rate_avg numeric(14,2),
    order_processing_fee numeric(14,2),
    courier_order_sum numeric(14,2),
    courier_tips_sum numeric(14,2),
    courier_reward_sum numeric(14,2)
);

create table stg.deliveries
(
    order_id    text primary key,
    order_ts    timestamp,
    delivery_id text,
    courier_id  text,
    address     text,
    delivery_ts timestamp,
    rate        integer,
    sum         numeric(14, 2),
    tip_sum     numeric(14, 2)
);

create table stg.couriers
(
    id   text primary key,
    name text
);

create table stg.restaurants
(
    id   text primary key,
    name text
);

create table dds.time(
    id serial primary key,
    order_ts timestamp,
    delivery_ts timestamp,
    year int,
    month int check (month > 0 and month < 13)

);

create table dds.courier(
    id serial primary key,
    courier_id text,
    name text,
    rate int
);

create table dds.restaurants(
    id serial primary key,
    restaurants_id text,
    name text
);

create table dds.orders(
    id serial primary key,
    order_id text unique,
    courier_id int,
    ts_id int,
    delivery_id text,
    restaurants_id int,
    address text,
    sum numeric(14,2),
    tip_sum numeric(14,2),

    foreign key (ts_id) references dds.time (id),
    foreign key (courier_id) references dds.couriers (id),
    foreign key (restaurants_id) references dds.restaurants (id)
);

