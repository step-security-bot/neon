DO $$
BEGIN
    IF NOT EXISTS(
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE tablename = 'drop_subscriptions_done'
        AND schemaname = 'neon_migration'
    )
    THEN
        CREATE SCHEMA IF NOT EXISTS neon_migration;
        ALTER SCHEMA neon_migration OWNER TO cloud_admin;
        REVOKE ALL ON SCHEMA neon_migration FROM PUBLIC;

        CREATE TABLE neon_migration.drop_subscriptions_done
        (timeline_id text);
    END IF;

    INSERT INTO neon_migration.drop_subscriptions_done
    VALUES (current_setting('neon.timeline_id'));
END
$$