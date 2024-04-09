from logging import getLogger
from pg_logidater.utils import SqlConn
from pg_logidater.exceptions import (
    PublicationExists,
    ReplicaLevelNotCorrect,
    ReplicaSlotExists,
)

PG_DUMP = "/usr/bin/pg_dump --no-publications --no-subscriptions"
PG_DUMPALL = "/usr/bin/pg_dumpall"
PSQL = "/usr/bin/psql"

_logger = getLogger(__name__)


def master_checks(psql: SqlConn, slot_name: str, pub_name: str, skip_pkey_check: bool) -> int:
    _logger.info("Starting master checks")
    _logger.debug("Checking wal_level")
    wal_level = psql.get_wal_level()
    if wal_level != "logical":
        raise ReplicaLevelNotCorrect(f"Current wal_level: {wal_level}, minimum required: logical")
    _logger.debug(f"Checking if replication slot {slot_name} doesn't exists")
    replica_slot = psql.get_replica_slot(slot_name)
    if replica_slot:
        raise ReplicaSlotExists(f"Replication slot {slot_name} already exists!")
    _logger.debug(f"Checking in publication {pub_name} doesn't exists")
    publication = psql.is_pub_exists(pub_name)
    if publication:
        raise PublicationExists(f"Publication {pub_name} already exists for db: {psql.sql_conn.get_dsn_parameters()['dbname']}")
    if not skip_pkey_check:
        _logger.debug("Checking tables without primary keys")
        no_keys_list = psql.get_no_primary_key()
        if no_keys_list:
            _logger.warning("Database have tables without primary key, that can cause issues with logical replication")
            _logger.warning("Following tables doesn't have primary key")
            for item in no_keys_list:
                print(f"{item[0]}.{item[1]}")
            _logger.warning("If that's fine, add --ignore-pkey switch")
            exit(1)
    _logger.debug("Getting db size")
    db = psql.sql_conn.get_dsn_parameters()["dbname"]
    db_size = psql.get_db_size(db)
    return db_size


def master_prepare(psql: SqlConn, name: str, database: str) -> str:
    _logger.info("Creating logical replication slot")
    psql.create_logical_slot(name)
    _logger.info("Creating publication")
    psql.create_publication(name)
    _logger.info(f"Geting db owner for {database}")
    db_owner = psql.get_database_owner(database)
    return db_owner
