CREATE SCHEMA IF NOT EXISTS bms;

CREATE TABLE IF NOT EXISTS bms.enriched_transactions (
    booking_id             VARCHAR(64)   NOT NULL,
    booking_event_id       VARCHAR(64),
    payment_event_id       VARCHAR(64),
    payment_id             VARCHAR(64),
    user_id                VARCHAR(32),
    booking_ts             TIMESTAMP,
    payment_ts             TIMESTAMP,
    show_ts                TIMESTAMP,
    show_id                VARCHAR(96),
    movie_id               VARCHAR(32),
    movie_name             VARCHAR(128),
    genre                  VARCHAR(64),
    language               VARCHAR(32),
    certificate            VARCHAR(16),
    city                   VARCHAR(64),
    venue_id               VARCHAR(32),
    venue_name             VARCHAR(128),
    screen_name            VARCHAR(64),
    seats                  VARCHAR(256),
    seat_count             INTEGER,
    ticket_category        VARCHAR(32),
    ticket_price           DOUBLE PRECISION,
    convenience_fee        DOUBLE PRECISION,
    taxes                  DOUBLE PRECISION,
    discount_amount        DOUBLE PRECISION,
    booking_amount         DOUBLE PRECISION,
    payment_amount         DOUBLE PRECISION,
    payment_method         VARCHAR(32),
    payment_provider       VARCHAR(32),
    payment_status         VARCHAR(32),
    bank_name              VARCHAR(64),
    upi_app                VARCHAR(64),
    failure_reason         VARCHAR(64),
    channel                VARCHAR(32),
    device_type            VARCHAR(32),
    currency               VARCHAR(8)
)
DISTSTYLE AUTO
SORTKEY (booking_ts);

