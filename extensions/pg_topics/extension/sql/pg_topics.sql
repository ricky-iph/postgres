CREATE TABLE topic.topic_config (
    schema_name        text     NOT NULL,
    topic              text     NOT NULL,
    band_count         smallint NOT NULL DEFAULT 4 CHECK (band_count BETWEEN 1 AND 1024),
    retention_interval interval NOT NULL,
    min_durability     text     NOT NULL DEFAULT 'durable'
        CHECK (min_durability IN ('relaxed', 'durable', 'replicated')),
    max_backlog_age    interval NOT NULL DEFAULT '60 seconds',
    offset_retention   interval,
    partition_interval interval NOT NULL DEFAULT '1 day'
        CHECK (partition_interval >= interval '1 minute'),
    detaching          regclass,
    sync_table         regclass,
    sync_key           text,
    sync_enabled       boolean  NOT NULL DEFAULT false,
    shape_version      integer  NOT NULL DEFAULT 0,
    backlog_age        interval NOT NULL DEFAULT '0',
    PRIMARY KEY (schema_name, topic),
    CHECK ((sync_table IS NULL) = (sync_key IS NULL)),
    CHECK (NOT sync_enabled OR sync_table IS NOT NULL),
    CHECK (retention_interval > interval '0'),
    CHECK (max_backlog_age > interval '0')
) WITH (fillfactor = 50);

CREATE TABLE topic.topic_band_position (
    schema_name   text     NOT NULL,
    topic         text     NOT NULL,
    band          smallint NOT NULL,
    next_offset   bigint   NOT NULL DEFAULT 0,
    oldest_offset bigint   NOT NULL DEFAULT 0,
    stamped_by    text,
    PRIMARY KEY (schema_name, topic, band),
    FOREIGN KEY (schema_name, topic)
        REFERENCES topic.topic_config (schema_name, topic) ON DELETE CASCADE
) WITH (fillfactor = 50);

CREATE TABLE topic.topic_groups (
    group_name       text    PRIMARY KEY,
    owner_role       name    NOT NULL,
    generation_id    integer NOT NULL DEFAULT 0,
    leader_member_id text,
    protocol_type    text,
    protocol_name    text,
    state            text    NOT NULL DEFAULT 'Empty'
        CHECK (state IN ('Empty', 'PreparingRebalance', 'CompletingRebalance', 'Stable', 'Dead')),
    expired_members  bigint  NOT NULL DEFAULT 0
);

CREATE TABLE topic.topic_group_members (
    group_name         text        NOT NULL REFERENCES topic.topic_groups (group_name) ON DELETE CASCADE,
    member_id          text        NOT NULL,
    owner_role         name        NOT NULL,
    client_id          text,
    session_timeout_ms integer     NOT NULL,
    rebalance_ms       integer     NOT NULL,
    subscription       bytea,
    assignment         bytea,
    last_heartbeat_at  timestamptz NOT NULL DEFAULT now(),
    joined_generation  integer,
    PRIMARY KEY (group_name, member_id)
);

CREATE INDEX ON topic.topic_group_members (last_heartbeat_at);

CREATE TABLE topic.topic_offsets (
    schema_name      text     NOT NULL,
    topic            text     NOT NULL,
    group_name       text     NOT NULL REFERENCES topic.topic_groups (group_name) ON DELETE CASCADE,
    band             smallint NOT NULL,
    owner_role       name     NOT NULL,
    committed_offset bigint   NOT NULL DEFAULT -1,
    generation_id    integer  NOT NULL DEFAULT 0,
    PRIMARY KEY (schema_name, topic, group_name, band),
    FOREIGN KEY (schema_name, topic, band)
        REFERENCES topic.topic_band_position (schema_name, topic, band) ON DELETE CASCADE
);

CREATE TABLE topic.topic_producers (
    schema_name    text        NOT NULL,
    topic          text        NOT NULL,
    producer_id    bigint      NOT NULL,
    producer_epoch smallint    NOT NULL,
    band           smallint    NOT NULL,
    slot           smallint    NOT NULL CHECK (slot BETWEEN 0 AND 4),
    first_sequence integer     NOT NULL,
    last_sequence  integer     NOT NULL,
    base_offset    bigint      NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, topic, producer_id, band, slot),
    FOREIGN KEY (schema_name, topic, band)
        REFERENCES topic.topic_band_position (schema_name, topic, band) ON DELETE CASCADE
);

ALTER TABLE topic.topic_groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE topic.topic_group_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE topic.topic_offsets ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_reads ON topic.topic_groups FOR SELECT
    USING (pg_catalog.pg_has_role(current_user, owner_role, 'member'));
CREATE POLICY owner_reads ON topic.topic_group_members FOR SELECT
    USING (pg_catalog.pg_has_role(current_user, owner_role, 'member'));
CREATE POLICY owner_reads ON topic.topic_offsets FOR SELECT
    USING (pg_catalog.pg_has_role(current_user, owner_role, 'member'));

CREATE FUNCTION topic.band_count(schema_name text, topic text) RETURNS smallint
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    SELECT c.band_count FROM topic.topic_config c WHERE c.schema_name = $1 AND c.topic = $2
$$;

CREATE FUNCTION topic.refuse_forged_insert() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RAISE EXCEPTION 'topic: an insert into %.% must not set log_offset or published_by',
        TG_TABLE_SCHEMA, TG_TABLE_NAME;
END
$$;

CREATE FUNCTION topic.publish_floor() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    c topic.topic_config;
    levels text[] := ARRAY['off', 'local', 'remote_write', 'on', 'remote_apply'];
    wanted text;
BEGIN
    SELECT * INTO c FROM topic.topic_config t
    WHERE t.schema_name = TG_TABLE_SCHEMA AND t.topic = TG_TABLE_NAME;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic: %.% has no topic_config row', TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;
    IF c.backlog_age > c.max_backlog_age THEN
        RAISE EXCEPTION 'topic: %.% backlog_age % is above max_backlog_age %',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, c.backlog_age, c.max_backlog_age;
    END IF;
    wanted := CASE c.min_durability WHEN 'relaxed' THEN 'off' WHEN 'durable' THEN 'on' ELSE 'remote_apply' END;
    IF array_position(levels, wanted) > array_position(levels, current_setting('synchronous_commit')) THEN
        PERFORM set_config('synchronous_commit', wanted, true);
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION topic.create_topic(
    topic text,
    band_count int DEFAULT 4,
    retention interval DEFAULT '7 days',
    min_durability text DEFAULT 'durable',
    partition_interval interval DEFAULT '1 day'
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp SET DateStyle = ISO SET TimeZone = UTC
AS $$
DECLARE
    s text := split_part(create_topic.topic, '.', 1);
    t text := substr(create_topic.topic, length(s) + 2);
    owner_name text := topic.caller();
    lo timestamptz := date_bin(partition_interval, now(), timestamptz '2000-01-01 00:00:00+00');
    p text;
BEGIN
    IF s !~ '^[A-Za-z0-9_-]{1,63}$' OR t !~ '^[A-Za-z0-9_-]{1,47}$' OR right(t, 2) <> '_q' THEN
        RAISE EXCEPTION 'topic.create_topic: % is not a valid topic name', create_topic.topic
            USING HINT = 'Use schema.table. The table ends in _q and has at most 47 characters from A-Z, a-z, 0-9, _ and -.';
    END IF;
    IF NOT has_schema_privilege(owner_name, s, 'CREATE') THEN
        RAISE EXCEPTION 'topic.create_topic: role % has no CREATE privilege on schema %', owner_name, s;
    END IF;
    IF min_durability IN ('durable', 'replicated') AND NOT current_setting('pg_topics.failover_is_fenced')::bool THEN
        RAISE EXCEPTION 'topic.create_topic: min_durability % needs pg_topics.failover_is_fenced = on', min_durability;
    END IF;
    IF min_durability = 'replicated' AND current_setting('synchronous_standby_names') = '' THEN
        RAISE EXCEPTION 'topic.create_topic: min_durability replicated needs synchronous_standby_names';
    END IF;

    INSERT INTO topic.topic_config (schema_name, topic, band_count, retention_interval, min_durability, partition_interval)
    VALUES (s, t, band_count, retention, min_durability, partition_interval);
    INSERT INTO topic.topic_band_position (schema_name, topic, band)
    SELECT s, t, b FROM generate_series(0, band_count - 1) b;

    EXECUTE format(
        'CREATE TABLE %I.%I (
            seq                bigint      NOT NULL GENERATED ALWAYS AS IDENTITY,
            log_offset         bigint,
            band               smallint    NOT NULL CHECK (band BETWEEN 0 AND %s),
            key                varchar(40),
            value              jsonb,
            headers            jsonb,
            published_by       name        NOT NULL DEFAULT current_user,
            published_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
            producer_timestamp timestamptz,
            PRIMARY KEY (published_at, seq)
        ) PARTITION BY RANGE (published_at)', s, t, band_count - 1);
    EXECUTE format('CREATE INDEX ON %I.%I (seq) WHERE log_offset IS NULL', s, t);
    EXECUTE format('CREATE INDEX ON %I.%I USING brin (log_offset)', s, t);
    EXECUTE format(
        'CREATE TRIGGER topic_refuse_forged BEFORE INSERT ON %I.%I FOR EACH ROW
         WHEN (NEW.log_offset IS NOT NULL OR NEW.published_by <> current_user)
         EXECUTE FUNCTION topic.refuse_forged_insert()', s, t);
    EXECUTE format(
        'CREATE TRIGGER topic_publish_floor BEFORE INSERT ON %I.%I FOR EACH STATEMENT
         EXECUTE FUNCTION topic.publish_floor()', s, t);
    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, t, owner_name);

    FOR i IN 0..1 LOOP
        p := t || '_p' || to_char((lo + i * partition_interval) AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS');
        EXECUTE format('CREATE TABLE %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
            s, p, s, t, lo + i * partition_interval, lo + (i + 1) * partition_interval);
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I (band, log_offset) WHERE log_offset IS NOT NULL', s, p);
        EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, p, owner_name);
    END LOOP;
END
$$;

CREATE FUNCTION topic.publish(topic text, value jsonb, key text DEFAULT NULL, headers jsonb DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(publish.topic, '.', 1);
    t text := substr(publish.topic, length(s) + 2);
    n int := topic.band_count(s, t);
    rr bigint;
    b int;
BEGIN
    IF n IS NULL THEN
        RAISE EXCEPTION 'topic.publish: topic % does not exist', publish.topic;
    END IF;
    IF key IS NULL THEN
        rr := coalesce(nullif(current_setting('pg_topics.round_robin', true), ''), '0')::bigint;
        PERFORM set_config('pg_topics.round_robin', (rr + 1)::text, false);
        b := rr % n;
    ELSE
        b := topic.band_for(key, n);
    END IF;
    EXECUTE format('INSERT INTO %I.%I (band, key, value, headers) VALUES ($1, $2, $3, $4)', s, t)
        USING b, key, value, headers;
END
$$;

GRANT USAGE ON SCHEMA topic TO PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA topic FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('topic.topic_config', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_band_position', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_groups', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_group_members', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_offsets', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_producers', '');
GRANT SELECT ON topic.topic_groups, topic.topic_group_members, topic.topic_offsets TO PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.stamp_topic(text, text, int) FROM PUBLIC;
