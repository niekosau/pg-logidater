SQL_WAL_LEVEL = "SHOW wal_level"
SQL_IS_REPLICA_PASUSED = "SELECT pg_is_wal_replay_paused()"
SQL_PAUSE_REPLICA = "SELECT pg_wal_replay_pause()"
SQL_RESUME_REPLICA = "SELECT pg_wal_replay_resume()"
SQL_DROP_DATABASE = "DROP DATABASE {0}"
SQL_CREATE_DATABASE = "CREATE DATABASE {db} OWNER {owner}"
SQL_DROP_SUBSCRIPTION = "DROP SUBSCRIPTION {name}"
SQL_SHOW_VERSION = "SHOW server_version"

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
