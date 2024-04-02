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


def master_checks(psql: SqlConn, slot_name: str, pub_name: str) -> None:
    _logger.info("Starting master checks")
    _logger.debug("Checking wal_level")
    wal_level = psql.get_wal_level()
    if wal_level != "logical":
        _logger.critical(f"wal_level config not correct, current value: {wal_level}, must be: logical")
        raise ReplicaLevelNotCorrect(f"Current wal_level: {wal_level}, minimum required: logical")
    _logger.debug(f"Checking if replication slot {slot_name} doesn't exists")
    replica_slot = psql.get_replica_slot(slot_name)
    if replica_slot:
        _logger.critical(f"Replica slot: {slot_name} already exists and it's active")
        psql.drop_replication_slot(slot_name)
        raise ReplicaSlotExists(f"Replication slot {slot_name} already exists!")
    _logger.debug(f"Checking in publication {pub_name} doesn't exists")
    publication = psql.is_pub_exists(pub_name)
    if publication:
        _logger.critical(f"Publication: {pub_name} already exists")
        psql.drop_publication(pub_name)
        raise PublicationExists(f"Publication {pub_name} already exists for db: {psql.sql_conn.get_dsn_parameters()['dbname']}")


def master_prepare(psql: SqlConn, name: str, database: str) -> str:
    _logger.info("Creating logical replication slot")
    psql.create_logical_slot(name)
    _logger.info("Creating publication")
    psql.create_publication(name)
    _logger.info(f"Geting db owner for {database}")
    db_owner = psql.get_database_owner(database)
    return db_owner
