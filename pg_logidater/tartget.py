from logging import getLogger
from pg_logidater.utils import SqlConn
from subprocess import Popen, PIPE
from os import path
from shutil import disk_usage
from pg_logidater.exceptions import (
    DatabaseExists,
    DiskSpaceTooLow
)
from threading import Event
from pycotore import ProgressBar

PG_DUMP_DB = "/usr/bin/pg_dump --no-publications --no-subscriptions -h {host} -U {user} {db}"
PG_DUMP_SEQ = "/usr/bin/pg_dump --no-publications --no-subscriptions -h {host} -d {db} -U {user} -t {seq_name}"
PG_DUMP_ROLES = "/usr/bin/pg_dumpall --roles-only -h {host} -U repmgr"
PSQL_SQL_RESTORE = "/usr/bin/psql -f {file} -d {db}"
PSQL_SQL_PIPE_RESTORE = "/usr/bin/psql -d {db}"


_logger = getLogger(__name__)


def target_check(psql: SqlConn, database: str, name: str, db_size: int) -> None:
    _logger.info("Executing target checks")
    _logger.debug("Checking available disk space")
    data_path = psql.get_datadirectory()
    available_disk = int(disk_usage(path=data_path).free * 0.9)
    if available_disk < db_size:
        raise DiskSpaceTooLow(f"Low disk space for {data_path}")
    _logger.debug("Checking if database not exists")
    if psql.check_database(database):
        raise DatabaseExists


def run_local_cli(cli, std_log, err_log, cli2: str = None, pipe: bool = False) -> None:
    with open(std_log, "w") as log:
        with open(err_log, "w") as err:
            if pipe:
                if not cli2:
                    _logger.critical("cli2 parameter mandaroty for pipe true")
                    exit(1)
                _logger.debug(f"Running: {cli} | {cli2}")
                pipe_output = Popen(cli.split(), stdout=PIPE)
                pipe_sync = Popen(cli2.split(), stdin=pipe_output.stdout, stdout=log, stderr=err)
                pipe_sync.communicate()
            else:
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


def sync_database(host: str, user: str, database: str, tmp_dir: str, log_dir: str, event: Event) -> None:
    _logger.info(f"Syncing database {database}")
    sync_log = path.join(log_dir, f"sync_{database}.log")
    sync_err_log = path.join(log_dir, f"sync_{database}.err")
    event.set()
    run_local_cli(
        cli=PG_DUMP_DB.format(db=database, host=host, user=user),
        cli2=PSQL_SQL_PIPE_RESTORE.format(db=database),
        std_log=sync_log,
        err_log=sync_err_log,
        pipe=True
    )
    event.set()


def db_sync_progress_bar(psql: SqlConn, total: float, event: Event, db: str, update_interval: float) -> None:
    _logger.debug("Startign progress bar function")
    bar = ProgressBar()
    suffix = f"{db} sync in progress"
    bar.suffix = suffix
    bar.total = total
    event.wait(timeout=10)
    _logger.debug("Continue progrss function")
    event.clear()
    while True:
        if event.is_set():
            break
        synced_size = psql.get_db_size(db)
        bar.progress = synced_size
        bar.draw()
        event.wait(update_interval)
    bar.update_progress = total
    bar.draw()


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


def sync_seq_pipe(psql: SqlConn, log_dir: str) -> None:
    dsn = psql.sql_conn.get_dsn_parameters()
    database = dsn["dbname"]
    host = dsn["host"]
    user = dsn["user"]
    _logger.info(f"Syncing sequences for {database}")
    sequences = psql.get_sequences()
    for seq in sequences:
        file_seq_name = f"{seq[0]}.{seq[1]}"
        sql_seq_name = f"{seq[0]}.\"{seq[1]}\""
        sync_seq_log = path.join(log_dir, f"sync_seq_{file_seq_name}.log")
        sync_seq_err_log = path.join(log_dir, f"sync_seq_{file_seq_name}.err")
        run_local_cli(
            cli=PG_DUMP_SEQ.format(host=host, db=database, user=user, seq_name=sql_seq_name),
            cli2=PSQL_SQL_PIPE_RESTORE.format(db=database),
            std_log=sync_seq_log,
            err_log=sync_seq_err_log,
            pipe=True
        )


def analyse_target(psql: SqlConn) -> None:
    _logger.info("Updating database statistics")
    psql.analyze()
