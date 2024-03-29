from logging import getLogger
from pg_logidater.utils import SqlConn
from subprocess import Popen
from os import path, makedirs
from pg_logidater.exceptions import (
    PsqlConnectionError,
    DatabaseExists
)

PG_DUMP_DB = "/usr/bin/pg_dump --no-publications --no-subscriptions -h {host} -d {db} -U {user}"
PG_DUMP_ROLES = "/usr/bin/pg_dumpall --roles-only -h {host} -U repmgr"
PSQL_SQL_RESTORE = "/usr/bin/psql -f {file} -d {db}"
LOG_PATH = "/tmp/pg-loginator/logs"
TMP_PATH = "/tmp/pg-loginator/tmp"

_logger = getLogger(__name__)


def tmp_drop_sub(name, database) -> None:
    try:
        psql = SqlConn("/tmp", user="postgres", password="", db=database)
        psql.drop_subscriber(name)
    except PsqlConnectionError:
        _logger.critical(f"Unable to connect to localhost with database {database}")
        exit(1)


def get_replica_position(host, user, app_name) -> str:
    try:
        psql = SqlConn(host, user=user, password="", db=user)
        return psql.get_replay_lsn(app_name)
    except PsqlConnectionError:
        _logger.critical(f"Unable to connect to localhost with database {user}")
        exit(1)


def target_check(psql: SqlConn, **kwargs) -> None:
    database = kwargs["database"]["name"]
    sub_name = kwargs["pub_name"]
    if psql.check_database(database):
        _logger.critical(f"Database {database} already exists!")
        # raise DatabaseExists
        tmp_drop_sub(sub_name, database)
        psql.drop_database(database)


def run_local_cli(cli, std_log, err_log) -> None:
    with open(std_log, "w") as log:
        with open(err_log, "w") as err:
            _logger.debug(f"Executing: {cli}")
            Popen(cli.split(" "), stdout=log, stderr=err).communicate()


def target(**kwargs) -> None:
    database = kwargs["database"]["name"]
    db_owner = kwargs["database"].pop("owner")
    replica = kwargs["replica"]
    master = kwargs["master"]
    user = kwargs["user"]
    pub_name = kwargs["pub_name"]
    slot_name = kwargs["slot_name"]
    replica_app_name = kwargs["replica_info"]["app"]
    replica_slot_name = kwargs["replica_info"]["slot"]
    makedirs(LOG_PATH, exist_ok=True)
    makedirs(TMP_PATH, exist_ok=True)
    try:
        psql = SqlConn("/tmp", user="postgres", password="", db="postgres")
        target_check(psql, **kwargs)
        psql.create_database(database, db_owner)
    except PsqlConnectionError:
        _logger.critical(f"Unable to connect to localhost with database {database}")
        exit(1)
    replica_dump_position = get_replica_position(master, user, replica_app_name)
    print(replica_dump_position)
    roles_dump_path = path.join(TMP_PATH, "roles.sql")
    roles_dump_err_log = path.join(LOG_PATH, "roles_dump.err")
    _logger.info(f"Dumping roles to {roles_dump_path}")
    run_local_cli(PG_DUMP_ROLES.format(host=replica), roles_dump_path, roles_dump_err_log)
    roles_restore_log = path.join(LOG_PATH, "roles_restore.log")
    roles_restore_err_log = path.join(LOG_PATH, "roles_restore.err")
    _logger.info(f"Restoring roles from {roles_dump_path}")
    run_local_cli(PSQL_SQL_RESTORE.format(file=roles_dump_path, db='postgres'), roles_restore_log, roles_restore_err_log)
    db_dump_path = path.join(TMP_PATH, f"{database}.sql")
    db_dump_err_log = path.join(LOG_PATH, f"{database}.err")
    run_local_cli(PG_DUMP_DB.format(db=database, host=replica, user=user), db_dump_path, db_dump_err_log)
    db_restore_log = path.join(TMP_PATH, f"{database}_restore.log")
    db_restore_err_log = path.join(TMP_PATH, f"{database}_restore.err")
    run_local_cli(PSQL_SQL_RESTORE.format(file=db_dump_path, db=database), db_restore_log, db_restore_err_log)
    try:
        psql = SqlConn("/tmp", user="postgres", password="", db=database)
        _logger.info(f"Creating subsriber to {master}")
        psql.create_subscriber(name=pub_name, host=master, database=database, repl_slot=slot_name)
    except PsqlConnectionError:
        _logger.critical(f"Unable to connect to localhost with database {database}")
        exit(1)
