from logging import getLogger
from pg_logidater.utils import SqlConn
from subprocess import Popen
from os import path
from pg_logidater.exceptions import (
    DatabaseExists
)

PG_DUMP_DB = "/usr/bin/pg_dump --no-publications --no-subscriptions -h {host} -d {db} -U {user}"
PG_DUMP_SEQ = "/usr/bin/pg_dump --no-publications --no-subscriptions -h {host} -d {db} -U {user} -t {seq_name}"
PG_DUMP_ROLES = "/usr/bin/pg_dumpall --roles-only -h {host} -U repmgr"
PSQL_SQL_RESTORE = "/usr/bin/psql -f {file} -d {db}"

_logger = getLogger(__name__)


def target_check(psql: SqlConn, database: str, name: str) -> None:
    if psql.check_database(database):
        raise DatabaseExists


def run_local_cli(cli, std_log, err_log) -> None:
    with open(std_log, "w") as log:
        with open(err_log, "w") as err:
            _logger.debug(f"Executing: {cli}")
            Popen(cli.split(), stdout=log, stderr=err).communicate()


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
    psql = SqlConn("/tmp", user="postgres", db=database)
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


def create_database(psql: SqlConn, database: str, owner: str) -> None:
    _logger.info(f"Creating database {database}")
    psql.create_database(
        database=database,
        owner=owner
    )


def dump_restore_seq(psql: SqlConn, tmp_dir: str, log_dir: str) -> None:
    dsn = psql.sql_conn.get_dsn_parameters()
    database = dsn["dbname"]
    host = dsn["host"]
    user = dsn["user"]
    _logger.info(f"Syncing sequences for {database}")
    sequences = psql.get_sequences()
    for seq in sequences:
        sql_seq_name = f"{seq[0]}.\"{seq[1]}\""
        file_seq_name = f"{seq[0]}.{seq[1]}"
        dump_path = path.join(tmp_dir, f"seq_dump_{sql_seq_name}.sql")
        dump_err_log = path.join(log_dir, f"seq_dump_{file_seq_name}.err")
        restore_log = path.join(log_dir, f"seq_restore_{file_seq_name}.log")
        restore_err_log = path.join(log_dir, f"seq_restore_{file_seq_name}.err")
        dump_seq(
            host=host,
            database=database,
            user=user,
            seq_name=sql_seq_name,
            file_path=dump_path,
            err_log=dump_err_log
        )
        restore_seq(
            file_path=dump_path,
            database=database,
            restore_log=restore_log,
            restore_err_log=restore_err_log
        )


def dump_seq(host: str, database: str, user: str, seq_name: str, file_path: str, err_log: str) -> None:
    _logger.debug(f"Dumping sequence: {seq_name}")
    run_local_cli(
        cli=PG_DUMP_SEQ.format(host=host, db=database, user=user, seq_name=seq_name),
        std_log=file_path,
        err_log=err_log
    )


def restore_seq(file_path: str, database: str, restore_log: str, restore_err_log: str) -> None:
    _logger.debug(f"Restoring {file_path}")
    run_local_cli(
        cli=PSQL_SQL_RESTORE.format(file=file_path, db=database),
        std_log=restore_log,
        err_log=restore_err_log
    )
