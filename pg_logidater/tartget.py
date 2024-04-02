from logging import getLogger
from pg_logidater.utils import SqlConn
from subprocess import Popen
from os import path
from pg_logidater.exceptions import (
    PsqlConnectionError,
    DatabaseExists
)

PG_DUMP_DB = "/usr/bin/pg_dump --no-publications --no-subscriptions -h {host} -d {db} -U {user}"
PG_DUMP_ROLES = "/usr/bin/pg_dumpall --roles-only -h {host} -U repmgr"
PSQL_SQL_RESTORE = "/usr/bin/psql -f {file} -d {db}"

_logger = getLogger(__name__)


def tmp_drop_sub(name, database) -> None:
    try:
        psql = SqlConn("/tmp", user="postgres", password="", db=database)
        psql.drop_subscriber()
    except PsqlConnectionError:
        _logger.critical(f"Unable to connect to localhost with database {database}")
        exit(1)


def target_check(psql: SqlConn, database: str, name: str) -> None:
    if psql.check_database(database):
        _logger.critical(f"Database {database} already exists!")
        _logger.warning("Cleaning up, must be removed for prod use")
        tmp_drop_sub(name, database)
        psql.drop_database(database)
        raise DatabaseExists


def run_local_cli(cli, std_log, err_log) -> None:
    with open(std_log, "w") as log:
        with open(err_log, "w") as err:
            _logger.debug(f"Executing: {cli}")
            Popen(cli.split(" "), stdout=log, stderr=err).communicate()


def get_replica_position(psql: SqlConn, app_name: str) -> str:
    _logger.info("Getting replication position")
    return psql.get_replay_lsn(app_name)


def sync_roles(host: str, tmp_path: str, log_dir: str) -> None:
    _logger.info("Syncing roles")
    roles_dump_path = path.join(tmp_path, "roles.sql")
    roles_dump_err_log = path.join(log_dir, "roles_dump.err")
    _logger.debug(f"Dumping roles to {roles_dump_path}")
    run_local_cli(
        PG_DUMP_ROLES.format(host=host),
        roles_dump_path,
        roles_dump_err_log
    )
    roles_restore_log = path.join(log_dir, "roles_restore.log")
    roles_restore_err_log = path.join(tmp_path, "roles_restore.err")
    _logger.debug(f"Restoring roles from {roles_dump_path}")
    run_local_cli(
        PSQL_SQL_RESTORE.format(file=roles_dump_path, db='postgres'),
        roles_restore_log,
        roles_restore_err_log
    )


def sync_database(host: str, user: str, database: str, tmp_dir: str, log_dir: str) -> None:
    _logger.info(f"Syncing database {database}")
    db_dump_path = path.join(tmp_dir, f"{database}.sql")
    db_dump_err_log = path.join(log_dir, f"{database}.err")
    _logger.debug(f"Dumping {database} to {db_dump_path} from {host}")
    run_local_cli(
        PG_DUMP_DB.format(db=database, host=host, user=user),
        db_dump_path,
        db_dump_err_log
    )
    db_restore_log = path.join(log_dir, f"{database}_restore.log")
    db_restore_err_log = path.join(tmp_dir, f"{database}_restore.err")
    _logger.debug(f"Restoring {database} from {db_dump_path} on target")
    run_local_cli(
        PSQL_SQL_RESTORE.format(file=db_dump_path, db=database),
        db_restore_log,
        db_restore_err_log
    )


def create_subscriber(sub_target: str, database: str, slot_name: str, repl_position: str) -> None:
    try:
        psql = SqlConn("/tmp", user="postgres", password="", db=database)
        _logger.info(f"Creating subsriber to {sub_target}")
        sub_id = psql.create_subscriber(
            name=slot_name,
            host=sub_target,
            database=database,
            repl_slot=slot_name
        )
        psql.enable_subscription(
            sub_name=slot_name,
            sub_id=sub_id,
            pos=repl_position
        )
    except PsqlConnectionError:
        _logger.critical(f"Unable to connect to localhost with database {database}")
        exit(1)


def create_database(psql: SqlConn, database: str, owner: str) -> None:
    _logger.info(f"Creating database {database}")
    psql.create_database(
        database=database,
        owner=owner
    )
