SQL_WAL_LEVEL = "SHOW wal_level"
SQL_IS_REPLICA_PASUSED = "SELECT pg_is_wal_replay_paused()"
SQL_PAUSE_REPLICA = "SELECT pg_wal_replay_pause()"
SQL_RESUME_REPLICA = "SELECT pg_wal_replay_resume()"
SQL_DROP_DATABASE = "DROP DATABASE {0}"
SQL_CREATE_DATABASE = "CREATE DATABASE {db} OWNER {owner}"
SQL_DROP_SUBSCRIPTION = "DROP SUBSCRIPTION {name}"
SQL_SHOW_VERSION = "SHOW server_version"
SQL_ADVANCE_REPLICA_POSITION = "SELECT pg_replication_origin_advance ('{id}', '{position}');"
SQL_ENABLE_SUBSCRIPTION = "ALTER SUBSCRIPTION {name} ENABLE"
SQL_SET_SLOT_NAME = "ALTER SUBSCRIPTION {name} SET (slot_name={slot_name})"
SQL_DISABLE_SUBSCRIPTION = "ALTER SUBSCRIPTION {name} DISABLE"
SQL_GET_DB_SUBSCRIPTION = "SELECT subname FROM pg_subscription"
SQL_CREATE_REPL_LOGICAL_SLOT = "SELECT pg_create_logical_replication_slot('{0}', 'pgoutput')"
SQL_DROP_REPL_SLOT = "SELECT pg_drop_replication_slot('{0}')"
SQL_CREAATE_PUB = "CREATE publication {0} for all tables"
SQL_DROP_PUB = "DROP publication {0}"
SQL_DATA_DIRECTORY = "SHOW data_directory"
SQL_DB_SIZE = "SELECT pg_database_size('{db}')"
SQL_ANALYZE = "ANALYZE VERBOSE"

SQL_SELECT_ALL_SEQUENCES = """
SELECT
  sequence_schema,
  sequence_name
FROM
  information_schema.sequences
"""

SQL_CREATE_SUBSCRIPTION = """
CREATE SUBSCRIPTION {name} connection 'host={master} port=5432 dbname={db} user=repmgr'
PUBLICATION {pub_name}
WITH
  (
    copy_data = FALSE,
    create_slot = FALSE,
    enabled = FALSE,
    slot_name = {repl_slot}
  )"""

SQL_CHECK_DATABASE = """
SELECT
  count(*)
FROM
  pg_database
WHERE
  datname = '{0}'"""

SQL_CHECK_REPLICA_SLOT = """
SELECT
  CASE
    WHEN active = FALSE
    AND slot_name = '{0}' THEN TRUE
    ELSE FALSE
  END AS slot_status
FROM
  pg_replication_slots
WHERE
  slot_name = '{0}'"""

SQL_CHECK_PUBLICATION = """
SELECT
  CASE WHEN
    pubname = '{0}'
      THEN true
      ELSE false
  END AS pub_status
FROM
  pg_publication
WHERE
  pubname = '{0}'"""

SQL_SELECT_DB_OWNER = """
SELECT
  pg_catalog.pg_get_userbyid (d.datdba)
FROM
  pg_catalog.pg_database d
WHERE
  d.datname = '{0}'"""

SQL_GET_REPLAY_LSN = """
SELECT
  replay_lsn
FROM
  pg_stat_replication
WHERE
  application_name = '{app_name}'"""

SQL_SELECT_SUB_NAME = """
SELECT
  roname
FROM
  pg_subscription sub,
  pg_replication_origin ro
WHERE
  'pg_' || sub.oid = ro.roname AND
  sub.subname = '{name}'"""

SQL_NO_PRIMARY_KEYS = """
SELECT
  t.table_schema,
  t.table_name
FROM
  information_schema.tables AS t
  LEFT JOIN information_schema.table_constraints AS tc ON (
    t.table_schema = t.table_schema
    AND t.table_name = tc.table_name
    AND tc.constraint_type = 'PRIMARY KEY'
  )
WHERE
  t.table_type = 'BASE TABLE'
  AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
  AND tc.constraint_name IS NULL
ORDER BY
  t.table_schema"""
