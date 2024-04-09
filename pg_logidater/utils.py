import logging
from sys import stdout
import paramiko
import psycopg2
import psycopg2.extras
from sys import exit
from pg_logidater import sqlqueries as sql
from os import makedirs, path

LOG_FORMAT_CON = "[%(module)-8s:%(funcName)-20s| %(levelname)-8s] %(message)-40s"
LOG_FORMAT_FH = "[%(asctime)s - %(module)s:%(funcName)s|%(levelname)s] %(message)-40s"


_logger = logging.getLogger(__name__)


class ServerConn(paramiko.SSHClient):
    def __init__(self, host, user):
        super().__init__()
        self.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.load_system_host_keys()
        self.host = host
        self.user = user

    def __enter__(self):
        try:
            self.connect(hostname=self.host, username=self.user)
        except paramiko.ssh_exception.SSHException:
            _logger.critical(f"Unable connect to {self.host} with user: {self.user}")
            exit(1)
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def run_cmd(self, command: str) -> str:
        _, out, err = self.exec_command(command=command, timeout=60)
        error = err.read().decode()
        if len(error) > 0:
            _logger.error(f"Command {command} not found")
        return out.read().decode()


class SqlConn():
    def __init__(self, host, db="repmgr", user="repmgr", port="5432"):
        self.sql_conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            database=db
        )
        self.cursor = self.sql_conn.cursor()
        if host.startswith("/tmp"):
            host = "localhost"
        _logger.debug(f"PSQL Connection to {host} with database {db} - established")

    def __del__(self) -> None:
        try:
            self.cursor.close()
            self.sql_conn.close()
        except AttributeError:
            pass

    def query(self, query, fetchone=False, fetchall=False) -> tuple:
        _logger.debug(f"Executing: {query}")
        try:
            self.cursor.execute(query)
        except psycopg2.errors.DuplicateObject as e:
            _logger.warning(f"Dublicate object {str(e).strip()}")
        self.sql_conn.commit()
        if fetchone:
            query_results = self.cursor.fetchone()
            return query_results
        if fetchall:
            query_results = self.cursor.fetchall()
            return query_results

    def create_logical_slot(self, slot_name):
        try:
            self.query(sql.SQL_CREATE_REPL_LOGICAL_SLOT.format(slot_name))
        except psycopg2.errors.DuplicateObject:
            _logger.warning(f"Replication slot {slot_name} exists")
            self.sql_conn.rollback()

    def drop_repl_slot(self, slot_name):
        try:
            self.query(sql.SQL_DROP_REPL_SLOT.format(slot_name))
        except psycopg2.errors.UndefinedObject:
            _logger.warning(f"Replication slot {slot_name} - doesn't exists")
            self.sql_conn.rollback()

    def create_publication(self, pub_name):
        try:
            self.query(sql.SQL_CREAATE_PUB.format(pub_name))
        except psycopg2.errors.DuplicateObject:
            _logger.warning(f"Publication {pub_name} - already exists")

    def drop_publication(self, pub_name):
        try:
            self.query(sql.SQL_DROP_PUB.format(pub_name))
        except psycopg2.errors.UndefinedObject:
            _logger.warning(f"Publication {pub_name} - doesn't exist")
            self.sql_conn.rollback()

    def create_subscriber(self, name, host, database, repl_slot) -> str:
        self.query(sql.SQL_CREATE_SUBSCRIPTION.format(name=name, master=host, db=database, pub_name=name, repl_slot=repl_slot))
        return self.query(sql.SQL_SELECT_SUB_NAME.format(name=name), fetchone=True)[0]

    def drop_subscriber(self, drop_slot: bool = False) -> None:
        self.sql_conn.autocommit = True
        try:
            sub_name = self.get_db_sub()
            if sub_name:
                self.disable_subscription(sub_name)
                if not drop_slot:
                    self.set_slot_name(sub_name, "NONE")
                self.query(sql.SQL_DROP_SUBSCRIPTION.format(name=sub_name))
        except psycopg2.errors.UndefinedObject:
            _logger.warning(f"Subscription {sub_name} doesn't exist!")
        finally:
            self.sql_conn.autocommit = False

    def is_replica_pause(self) -> bool:
        if self.query(sql.SQL_IS_REPLICA_PASUSED, fetchone=True)[0]:
            _logger.warning("Replication is paused!")
            return True
        return False

    def pause_replica(self) -> None:
        self.query(sql.SQL_PAUSE_REPLICA)

    def resume_replica(self) -> None:
        self.query(sql.SQL_RESUME_REPLICA)

    def check_database(self, database) -> bool:
        if self.query(sql.SQL_CHECK_DATABASE.format(database), fetchone=True)[0] > 0:
            _logger.debug(f"Database {database} exists")
            return True
        return False

    def drop_database(self, database) -> None:
        _logger.debug(f"Droping database {database}")
        try:
            self.sql_conn.autocommit = True
            self.query(sql.SQL_DROP_DATABASE.format(database))
        except psycopg2.errors.ObjectInUse as err:
            _logger.error(err)
        finally:
            self.sql_conn.autocommit = False

    def create_database(self, database, owner="postgres") -> None:
        self.sql_conn.autocommit = True
        self.query(sql.SQL_CREATE_DATABASE.format(db=database, owner=owner))
        self.sql_conn.autocommit = False

    def get_database_owner(self, database) -> str:
        return self.query(sql.SQL_SELECT_DB_OWNER.format(database), fetchone=True)[0]

    def server_version(self) -> float:
        return float(self.query(sql.SQL_SHOW_VERSION, fetchone=True)[0])

    def get_replay_lsn(self, app_name) -> str:
        return self.query(sql.SQL_GET_REPLAY_LSN.format(app_name=app_name), fetchone=True)[0]

    def enable_subscription(self, sub_name, sub_id, pos) -> None:
        _logger.info(f"Changing starting position to {pos}")
        self.query(sql.SQL_ADVANCE_REPLICA_POSITION.format(id=sub_id, position=pos))
        _logger.info(f"Enabling subscription {sub_name}")
        self.query(sql.SQL_ENABLE_SUBSCRIPTION.format(name=sub_name))

    def disable_subscription(self, sub_name) -> None:
        _logger.info(f"Disabling subscription: {sub_name}")
        self.query(sql.SQL_DISABLE_SUBSCRIPTION.format(name=sub_name))

    def set_slot_name(self, sub_name, slot_name) -> None:
        _logger.info(f"Changing subscription {sub_name} slot to {slot_name}")
        self.query(sql.SQL_SET_SLOT_NAME.format(name=sub_name, slot_name=slot_name))

    def get_db_sub(self):
        _logger.info("Fetching subscription name")
        sub_name = self.query(sql.SQL_GET_DB_SUBSCRIPTION, fetchone=True)
        if isinstance(sub_name, tuple):
            _logger.debug(f"Subscription name: {sub_name[0]}")
            return sub_name[0]
        return False

    def get_wal_level(self) -> str:
        return self.query(sql.SQL_WAL_LEVEL, fetchone=True)[0]

    def get_replica_slot(self, name) -> str:
        slot = self.query(sql.SQL_CHECK_REPLICA_SLOT.format(name), fetchone=True)
        if isinstance(slot, tuple):
            return slot[0]

    def is_pub_exists(self, name) -> bool:
        if self.query(sql.SQL_CHECK_PUBLICATION.format(name), fetchone=True):
            return True
        return False

    def get_sequences(self) -> list[tuple]:
        return self.query(sql.SQL_SELECT_ALL_SEQUENCES, fetchall=True)

    def get_datadirectory(self) -> float:
        return (self.query(sql.SQL_DATA_DIRECTORY, fetchone=True))[0]


def setup_logging(log_level: str, save_log: str, debug_ssh: bool = False,  log_path: str = None) -> None:
    log_level_int = logging.getLevelName(str(log_level).upper())
    handlers = []
    if debug_ssh:
        logging.getLogger("paramiko").setLevel(logging.DEBUG)
    else:
        logging.getLogger("paramiko").setLevel(logging.WARNING)
    con_log = logging.StreamHandler(stdout)
    con_log.setFormatter(
        logging.Formatter(LOG_FORMAT_CON)
    )
    con_log.setLevel(log_level_int)
    handlers.append(con_log)
    if save_log:
        fh = logging.FileHandler(
            path.join(save_log)
        )
        fh.setFormatter(
            logging.Formatter(LOG_FORMAT_FH)
        )
        fh.setLevel(logging.DEBUG)
        handlers.append(fh)
    logging.basicConfig(
        level=logging.DEBUG,
        handlers=handlers
    )
    if save_log:
        _logger.info(f"Saving debug logging to {save_log}")


def prepare_directories(log_dir, tmp_dir) -> None:
    _logger.info("Creating needed directories")
    _logger.debug(f"Creating log dir: {log_dir}")
    makedirs(log_dir, exist_ok=True)
    _logger.debug(f"Creating tmp dir: {tmp_dir}")
    makedirs(tmp_dir, exist_ok=True)
