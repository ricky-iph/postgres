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
    detaching_name     text,
    detaching_bound    text,
    retention_hold_until timestamptz,
    sync_table         regclass,
    sync_key           text,
    sync_enabled       boolean  NOT NULL DEFAULT false,
    backlog_age        interval NOT NULL DEFAULT '0',
    stamped_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, topic),
    CHECK ((sync_table IS NULL) = (sync_key IS NULL)),
    CHECK (NOT sync_enabled OR sync_table IS NOT NULL),
    CHECK (retention_interval > interval '0'),
    CHECK (max_backlog_age >= interval '2 seconds')
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
    expired_members  bigint  NOT NULL DEFAULT 0,
    updated_at       timestamptz NOT NULL DEFAULT now()
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
    RAISE EXCEPTION 'topic: an insert into %.% must not set log_offset or published_by, or a published_at later than the current time',
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
    IF clock_timestamp() - c.stamped_at > c.max_backlog_age THEN
        RAISE EXCEPTION 'topic: the stamper has not run on %.% since %, which is longer than max_backlog_age %',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, c.stamped_at, c.max_backlog_age;
    END IF;
    wanted := CASE c.min_durability WHEN 'relaxed' THEN 'off' WHEN 'durable' THEN 'on' ELSE 'remote_apply' END;
    IF array_position(levels, wanted) > array_position(levels, current_setting('synchronous_commit')) THEN
        PERFORM set_config('synchronous_commit', wanted, true);
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION topic.ensure_partitions(schema_name text, topic text, ahead int) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp SET DateStyle = ISO SET TimeZone = UTC
AS $$
DECLARE
    c topic.topic_config;
    parent regclass;
    owner_name name;
    lo timestamptz;
    p text;
    existing regclass;
BEGIN
    SELECT * INTO STRICT c FROM topic.topic_config t
    WHERE t.schema_name = ensure_partitions.schema_name AND t.topic = ensure_partitions.topic;
    parent := format('%I.%I', c.schema_name, c.topic)::regclass;
    SELECT r.rolname INTO owner_name FROM pg_class k JOIN pg_roles r ON r.oid = k.relowner
    WHERE k.oid = parent AND k.relkind = 'p';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic.ensure_partitions: % is not a partitioned table', parent;
    END IF;
    FOR i IN 0..ahead LOOP
        lo := date_bin(c.partition_interval, now(), timestamptz '2000-01-01 00:00:00+00') + i * c.partition_interval;
        p := c.topic || '_p' || to_char(lo, 'YYYYMMDDHH24MISS');
        existing := to_regclass(format('%I.%I', c.schema_name, p));
        IF existing IS NOT NULL THEN
            IF NOT EXISTS (SELECT FROM pg_inherits h WHERE h.inhrelid = existing AND h.inhparent = parent) THEN
                RAISE WARNING 'topic: % exists and is not a partition of %, so % gets no partition from %',
                    existing, parent, parent, lo;
            END IF;
            CONTINUE;
        END IF;
        EXECUTE format('CREATE TABLE %I.%I (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', c.schema_name, p, parent);
        EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', c.schema_name, p, owner_name);
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I (band, log_offset) WHERE log_offset IS NOT NULL', c.schema_name, p);
        EXECUTE format('ALTER TABLE %s ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
            parent, c.schema_name, p, lo, lo + c.partition_interval);
    END LOOP;
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
         WHEN (NEW.log_offset IS NOT NULL OR NEW.published_by <> current_user OR NEW.published_at > clock_timestamp())
         EXECUTE FUNCTION topic.refuse_forged_insert()', s, t);
    EXECUTE format(
        'CREATE TRIGGER topic_publish_floor BEFORE INSERT ON %I.%I FOR EACH STATEMENT
         EXECUTE FUNCTION topic.publish_floor()', s, t);
    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, t, owner_name);
    PERFORM topic.ensure_partitions(s, t, 1);
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

CREATE FUNCTION topic.retention_next(schema_name text, topic text) RETURNS text
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp SET DateStyle = ISO SET TimeZone = UTC
AS $$
DECLARE
    c topic.topic_config;
    parent regclass;
    pending boolean;
BEGIN
    SELECT * INTO STRICT c FROM topic.topic_config t
    WHERE t.schema_name = retention_next.schema_name AND t.topic = retention_next.topic;
    IF clock_timestamp() < c.retention_hold_until THEN
        RETURN NULL;
    END IF;
    parent := format('%I.%I', c.schema_name, c.topic)::regclass;
    IF c.detaching IS NOT NULL AND NOT EXISTS (SELECT FROM pg_class k WHERE k.oid = c.detaching) THEN
        c.detaching := NULL;
        UPDATE topic.topic_config t SET detaching = NULL
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
    END IF;
    IF c.detaching IS NULL THEN
        SELECT h.inhrelid, pg_get_expr(k.relpartbound, k.oid) INTO c.detaching, c.detaching_bound
        FROM pg_inherits h JOIN pg_class k ON k.oid = h.inhrelid
        CROSS JOIN LATERAL substring(pg_get_expr(k.relpartbound, k.oid) FROM ' TO \(''([^'']+)''\)$') b(upper)
        WHERE h.inhparent = parent AND NOT h.inhdetachpending
          AND b.upper::timestamptz < now() - c.retention_interval
        ORDER BY b.upper::timestamptz LIMIT 1;
        IF c.detaching IS NULL THEN
            RETURN NULL;
        END IF;
        UPDATE topic.topic_config t
        SET detaching = c.detaching, detaching_name = c.detaching::text, detaching_bound = c.detaching_bound
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
        c.detaching_name := c.detaching::text;
    END IF;

    SELECT h.inhdetachpending INTO pending FROM pg_inherits h WHERE h.inhrelid = c.detaching AND h.inhparent = parent;
    IF FOUND THEN
        RAISE LOG 'topic: retention detaches % from %', c.detaching, parent;
        RETURN format('ALTER TABLE %s DETACH PARTITION %s %s', parent, c.detaching,
                      CASE WHEN pending THEN 'FINALIZE' ELSE 'CONCURRENTLY' END);
    END IF;
    IF c.detaching::text IS DISTINCT FROM c.detaching_name
       OR EXISTS (SELECT FROM pg_inherits h WHERE h.inhrelid = c.detaching) THEN
        UPDATE topic.topic_config t SET detaching = NULL
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
        RAISE WARNING 'topic: % was renamed or attached to another table after retention detached it from %, so retention leaves it',
            c.detaching, parent;
        RETURN NULL;
    END IF;

    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', c.detaching);
    IF topic.retention_check(c.detaching) > 0 THEN
        EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s %s', parent, c.detaching, c.detaching_bound);
        UPDATE topic.topic_config t SET detaching = NULL, retention_hold_until = clock_timestamp() + c.retention_interval
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
        RAISE WARNING 'topic: % has rows with no log_offset, so retention attached it again to %', c.detaching, parent;
        RETURN NULL;
    END IF;
    UPDATE topic.topic_band_position p SET oldest_offset = f.floor
    FROM topic.retention_floor(c.schema_name, c.topic) f
    WHERE p.schema_name = c.schema_name AND p.topic = c.topic AND p.band = f.band;
    RAISE LOG 'topic: retention drops %', c.detaching;
    EXECUTE format('DROP TABLE %s', c.detaching);
    UPDATE topic.topic_config t SET detaching = NULL
    WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
    RETURN NULL;
END
$$;

CREATE FUNCTION topic.reap() RETURNS void
LANGUAGE sql SET search_path = pg_catalog, pg_temp
AS $$
    DELETE FROM topic.topic_producers WHERE updated_at < now() - interval '1 day';
    DELETE FROM topic.topic_groups g
    WHERE g.state = 'Empty'
      AND NOT starts_with(g.group_name, '__pg_topics_sync:')
      AND NOT EXISTS (SELECT FROM topic.topic_group_members m WHERE m.group_name = g.group_name)
      AND g.updated_at < now() - (
          SELECT max(c.offset_retention)
          FROM topic.topic_offsets o
          JOIN topic.topic_config c ON c.schema_name = o.schema_name AND c.topic = o.topic
          WHERE o.group_name = g.group_name
          HAVING bool_and(c.offset_retention IS NOT NULL));
$$;

CREATE FUNCTION topic.attach(
    base regclass,
    sync_key text,
    band_count int DEFAULT 4,
    retention interval DEFAULT '7 days',
    min_durability text DEFAULT 'durable'
) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    who text := topic.caller();
    s text;
    t text;
    base_owner name;
    queue_owner name;
    grp text;
BEGIN
    SELECT n.nspname, k.relname || '_q', r.rolname INTO s, t, base_owner
    FROM pg_class k JOIN pg_namespace n ON n.oid = k.relnamespace JOIN pg_roles r ON r.oid = k.relowner
    WHERE k.oid = attach.base AND k.relkind IN ('r', 'p');
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic.attach: % is not an ordinary table', base;
    END IF;
    IF NOT pg_has_role(who, base_owner, 'member') THEN
        RAISE EXCEPTION 'topic.attach: role % is not a member of %, the owner of %', who, base_owner, base
            USING ERRCODE = '42501';
    END IF;
    IF NOT has_schema_privilege(who, s, 'CREATE') THEN
        RAISE EXCEPTION 'topic.attach: role % has no CREATE privilege on schema %', who, s USING ERRCODE = '42501';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_attribute a WHERE a.attrelid = base AND a.attname = attach.sync_key
                   AND a.attnum > 0 AND NOT a.attisdropped) THEN
        RAISE EXCEPTION 'topic.attach: % has no column %', base, attach.sync_key;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_attribute a WHERE a.attrelid = base AND a.attname = attach.sync_key
                   AND a.attnotnull) THEN
        RAISE EXCEPTION 'topic.attach: column % of % allows NULL', attach.sync_key, base
            USING HINT = 'The sync key column must be NOT NULL.';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_attribute a WHERE a.attrelid = base AND a.attname = 'event_at'
                   AND a.atttypid = 'timestamptz'::regtype AND a.attnotnull AND NOT a.attisdropped) THEN
        RAISE EXCEPTION 'topic.attach: % has no column event_at timestamptz NOT NULL', base;
    END IF;
    IF NOT EXISTS (SELECT FROM topic.topic_config c WHERE c.schema_name = s AND c.topic = t) THEN
        PERFORM topic.create_topic(s || '.' || t, band_count, retention, min_durability);
    END IF;
    SELECT r.rolname INTO queue_owner FROM pg_class k JOIN pg_roles r ON r.oid = k.relowner
    WHERE k.oid = format('%I.%I', s, t)::regclass;
    IF NOT pg_has_role(who, queue_owner, 'member') THEN
        RAISE EXCEPTION 'topic.attach: role % is not a member of %, the owner of %.%', who, queue_owner, s, t
            USING ERRCODE = '42501';
    END IF;

    EXECUTE format(
        'CREATE TABLE %I.%I (
            band               smallint    NOT NULL,
            log_offset         bigint      NOT NULL,
            seq                bigint      NOT NULL,
            key                varchar(40),
            value              jsonb,
            headers            jsonb,
            published_by       name        NOT NULL,
            published_at       timestamptz NOT NULL,
            producer_timestamp timestamptz,
            failed_at          timestamptz NOT NULL DEFAULT now(),
            error              text        NOT NULL,
            PRIMARY KEY (band, log_offset)
        )', s, t || 'e');
    EXECUTE format('CREATE INDEX ON %I.%I (failed_at)', s, t || 'e');
    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, t || 'e', base_owner);

    grp := '__pg_topics_sync:' || s || '.' || t;
    UPDATE topic.topic_config c SET sync_table = base, sync_key = attach.sync_key, sync_enabled = true
    WHERE c.schema_name = s AND c.topic = t;
    INSERT INTO topic.topic_groups (group_name, owner_role) VALUES (grp, base_owner);
    INSERT INTO topic.topic_offsets (schema_name, topic, group_name, band, owner_role, committed_offset)
    SELECT p.schema_name, p.topic, grp, p.band, base_owner, p.oldest_offset
    FROM topic.topic_band_position p WHERE p.schema_name = s AND p.topic = t;
    RETURN s || '.' || t;
END
$$;

CREATE FUNCTION topic.create_table_topic(
    table_name text,
    columns json,
    sync_key text,
    band_count int DEFAULT 4,
    retention interval DEFAULT '7 days',
    min_durability text DEFAULT 'durable'
) RETURNS text
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(table_name, '.', 1);
    t text := substr(table_name, length(s) + 2);
    defs text := '';
    c record;
BEGIN
    IF json_typeof(columns) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'topic.create_table_topic: columns must be a JSON object of "name": "type"';
    END IF;
    FOR c IN SELECT * FROM json_each(columns) LOOP
        IF json_typeof(c.value) <> 'string' OR to_regtype(c.value #>> '{}') IS NULL THEN
            RAISE EXCEPTION 'topic.create_table_topic: % is not a type', c.value;
        END IF;
        defs := defs || format('%I %s, ', c.key,
            format_type(to_regtype(c.value #>> '{}'), to_regtypemod(c.value #>> '{}')));
    END LOOP;
    EXECUTE format('CREATE TABLE %I.%I (%s event_at timestamptz NOT NULL, PRIMARY KEY (%I))', s, t, defs, sync_key);
    RETURN topic.attach(format('%I.%I', s, t)::regclass, sync_key, band_count, retention, min_durability);
END
$$;

CREATE FUNCTION topic.ddl_end() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    q record;
BEGIN
    FOR q IN
        SELECT c.schema_name, c.topic, c.band_count,
               array_agg(pg_get_constraintdef(k.oid)) FILTER (WHERE k.oid IS NOT NULL) AS checks
        FROM (SELECT DISTINCT d.objid FROM pg_event_trigger_ddl_commands() d
              WHERE d.classid = 'pg_class'::regclass) d
        JOIN pg_class r ON r.oid = d.objid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        JOIN topic.topic_config c ON c.schema_name = n.nspname AND c.topic = r.relname
        LEFT JOIN pg_attribute a ON a.attrelid = r.oid AND a.attname = 'band'
        LEFT JOIN pg_constraint k ON k.conrelid = r.oid AND k.contype = 'c' AND a.attnum = ANY (k.conkey)
        GROUP BY c.schema_name, c.topic, c.band_count
    LOOP
        IF q.checks IS DISTINCT FROM ARRAY[format('CHECK (((band >= 0) AND (band <= %s)))', q.band_count - 1)] THEN
            RAISE WARNING 'topic: the CHECK constraints on band of %.% are %, which do not match band_count %',
                q.schema_name, q.topic, q.checks, q.band_count;
        END IF;
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'topic: the % event trigger failed on %: %', TG_EVENT, TG_TAG, SQLERRM;
END
$$;

CREATE FUNCTION topic.sql_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    r record;
BEGIN
    FOR r IN
        UPDATE topic.topic_config c SET sync_enabled = false
        FROM pg_event_trigger_dropped_objects() d
        WHERE c.sync_enabled AND d.classid = 'pg_class'::regclass AND d.objid = c.sync_table
          AND (d.objsubid = 0 OR d.address_names[3] = c.sync_key)
        RETURNING c.schema_name, c.topic
    LOOP
        RAISE WARNING 'topic: %.% stops syncing, because its base table or its sync key column was dropped',
            r.schema_name, r.topic;
    END LOOP;
    FOR r IN
        SELECT c.schema_name, c.topic FROM pg_event_trigger_dropped_objects() d
        JOIN topic.topic_config c ON c.schema_name = d.schema_name AND c.topic = d.object_name
        WHERE d.classid = 'pg_class'::regclass AND d.objsubid = 0 AND d.object_type = 'table'
    LOOP
        DELETE FROM topic.topic_groups g WHERE g.group_name = '__pg_topics_sync:' || r.schema_name || '.' || r.topic;
        DELETE FROM topic.topic_config c WHERE c.schema_name = r.schema_name AND c.topic = r.topic;
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'topic: the % event trigger failed on %: %', TG_EVENT, TG_TAG, SQLERRM;
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
REVOKE EXECUTE ON FUNCTION topic.retention_check(oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.retention_floor(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.ensure_partitions(text, text, int) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.retention_next(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.reap() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.check_duplicates(text, text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.sync_topic(text, text, int) FROM PUBLIC;
CREATE EVENT TRIGGER pg_topics_ddl_end ON ddl_command_end EXECUTE FUNCTION topic.ddl_end();
CREATE EVENT TRIGGER pg_topics_sql_drop ON sql_drop EXECUTE FUNCTION topic.sql_drop();
