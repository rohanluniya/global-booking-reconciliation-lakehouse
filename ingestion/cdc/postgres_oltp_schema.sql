-- PostgreSQL 17 OLTP Booking, Payment & Transactional Outbox DDL
-- Designed for Debezium 3.7.1.Final logical replication (wal_level = logical).

CREATE SCHEMA IF NOT EXISTS booking_core;

CREATE TABLE IF NOT EXISTS booking_core.listings (
    listing_id         VARCHAR(64) PRIMARY KEY,
    host_id            VARCHAR(64) NOT NULL,
    jurisdiction       VARCHAR(32) NOT NULL,
    currency           CHAR(3)     NOT NULL,
    host_timezone      VARCHAR(64) NOT NULL,
    nightly_rate_minor BIGINT      NOT NULL CHECK (nightly_rate_minor > 0),
    platform_fee_bps   INTEGER     NOT NULL CHECK (platform_fee_bps >= 0),
    host_fee_bps       INTEGER     NOT NULL CHECK (host_fee_bps >= 0),
    status             VARCHAR(24) NOT NULL DEFAULT 'ACTIVE',
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS booking_core.tax_policies (
    jurisdiction       VARCHAR(32) NOT NULL,
    tax_policy_version VARCHAR(64) NOT NULL,
    tax_bps            INTEGER     NOT NULL CHECK (tax_bps >= 0),
    effective_from     TIMESTAMPTZ NOT NULL,
    recorded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (jurisdiction, tax_policy_version)
);

CREATE TABLE IF NOT EXISTS booking_core.bookings (
    booking_id          VARCHAR(64) PRIMARY KEY,
    listing_id          VARCHAR(64) NOT NULL REFERENCES booking_core.listings(listing_id),
    guest_id            VARCHAR(64) NOT NULL,
    host_id             VARCHAR(64) NOT NULL,
    booking_status      VARCHAR(32) NOT NULL,
    sequence_num        INTEGER     NOT NULL DEFAULT 1,
    currency            CHAR(3)     NOT NULL,
    nights              INTEGER     NOT NULL CHECK (nights > 0),
    room_subtotal_minor BIGINT      NOT NULL,
    platform_fee_minor  BIGINT      NOT NULL,
    host_fee_minor      BIGINT      NOT NULL,
    tax_minor           BIGINT      NOT NULL,
    gross_total_minor   BIGINT      NOT NULL,
    tax_policy_version  VARCHAR(64) NOT NULL,
    effective_at        TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS booking_core.payments (
    payment_reference   VARCHAR(64) PRIMARY KEY,
    booking_id          VARCHAR(64) NOT NULL REFERENCES booking_core.bookings(booking_id),
    payment_status      VARCHAR(32) NOT NULL,
    currency            CHAR(3)     NOT NULL,
    captured_minor      BIGINT      NOT NULL DEFAULT 0,
    refunded_minor      BIGINT      NOT NULL DEFAULT 0,
    chargeback_minor    BIGINT      NOT NULL DEFAULT 0,
    retry_attempts      INTEGER     NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Transactional Outbox table written in the SAME ACID transaction as booking/payment mutations
CREATE TABLE IF NOT EXISTS booking_core.outbox_events (
    id                  UUID        PRIMARY KEY,
    aggregatetype       VARCHAR(64) NOT NULL,
    aggregateid         VARCHAR(64) NOT NULL,
    type                VARCHAR(64) NOT NULL,
    payload             JSONB       NOT NULL,
    effective_at        TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_created_at ON booking_core.outbox_events (created_at);

-- Publication for Debezium pgoutput logical decoding
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'booking_outbox_pub') THEN
        CREATE PUBLICATION booking_outbox_pub FOR TABLE
            booking_core.outbox_events,
            booking_core.listings,
            booking_core.tax_policies;
    END IF;
END $$;
