drop table if exists public.yellow_tripdata;

create table if not exists public.yellow_tripdata
(
    index                 bigint,
    "VendorID"            integer,
    tpep_pickup_datetime  timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count       double precision,
    trip_distance         double precision,
    "RatecodeID"          double precision,
    store_and_fwd_flag    text,
    "PULocationID"        integer,
    "DOLocationID"        integer,
    payment_type          bigint,
    fare_amount           double precision,
    extra                 double precision,
    mta_tax               double precision,
    tip_amount            double precision,
    tolls_amount          double precision,
    improvement_surcharge double precision,
    total_amount          double precision,
    congestion_surcharge  double precision,
    "Airport_fee"         double precision
);

alter table public.yellow_tripdata
    owner to postgres;

create index if not exists ix_public_yellow_tripdata_index
    on public.yellow_tripdata (index);
