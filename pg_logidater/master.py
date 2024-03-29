from logging import getLogger
from pg_logidater.utils import ServerConn, SqlConn
from sys import exit
from pg_logidater.exceptions import (
    PsqlConnectionError,
    PublicationExists,
    ReplicaAlreadyPaused,
    ReplicaLevelNotCorrect,
    ReplicaSlotExists,
)
from pg_logidater.sqlqueries import (
    SQL_WAL_LEVEL,
    SQL_CHECK_REPLICA_SLOT,
    SQL_CHECK_PUBLICATION,
    SQL_SELECT_DB_OWNER
)

PG_DUMP = "/usr/bin/pg_dump --no-publications --no-subscriptions"
PG_DUMPALL = "/usr/bin/pg_dumpall"
PSQL = "/usr/bin/psql"

_logger = getLogger(__name__)


def master_checks(psql: SqlConn, **kwargs) -> None:
    _logger.debug("Starting master server checks")
    if "slot_name" in kwargs:
        slot_name = kwargs["slot_name"]
    else:
        slot_name = "py_script_dev_slot"
        _logger.warning(f"Slot name not provided, using default: {slot_name}")
    if "pub_name" in kwargs:
        pub_name = kwargs["pub_name"]
    else:
        pub_name = "py_script_dev_pub"
        _logger.warning(f"Publication name not provided, using defaul: {pub_name}")
    wal_level = psql.query(SQL_WAL_LEVEL, fetchone=True)
    if wal_level[0] != "logical":
        _logger.critical(f"wal_level config not correct, current value: {wal_level[0]}, must be: logical")
        raise ReplicaLevelNotCorrect(f"Current wal_level: {wal_level[0]}, minimum required: logical")
    replica_slot = psql.query(SQL_CHECK_REPLICA_SLOT.format(slot_name), fetchone=True)
    if isinstance(replica_slot, tuple):
        if replica_slot[0]:
            _logger.error(f"Replica slot: {slot_name} already exists and it's active")
            # raise ReplicaSlotExists
            psql.drop_replication_slot(slot_name)
    publication = psql.query(SQL_CHECK_PUBLICATION.format(pub_name), fetchone=True)
    if isinstance(publication, tuple):
        if publication[0]:
            _logger.error(f"Publication: {pub_name} for database {kwargs['database']['name']} already exists")
            # raise PublicationExists(f"Publication {pub_name} already exists")
            psql.drop_publication(pub_name)


def master(**kwargs):
    host = kwargs["master"]
    slot_name = kwargs["slot_name"]
    pub_name = kwargs["pub_name"]
    database = kwargs["database"]["name"]
    try:
        psql = SqlConn(host, database)
        master_checks(psql, **kwargs)
        db_owner = psql.query(SQL_SELECT_DB_OWNER.format(database), fetchone=True)
        kwargs["database"]["owner"] = db_owner[0]
    except PsqlConnectionError:
        _logger.critical(f"Unable to connect to {host} with database {database}")
        exit(1)

    _logger.info("Creating logical replication slot")
    psql.create_logical_slot(slot_name)
    _logger.info("Creating publication")
    psql.create_publication(pub_name)
