from logging import getLogger, WARNING
# from threading import BrokenBarrierError
from pg_logidater.exceptions import PsqlConnectionError
import paramiko
import psycopg2
import psycopg2.extras
from sys import exit
from pg_logidater.sqlqueries import (
    SQL_IS_REPLICA_PASUSED,
    SQL_PAUSE_REPLICA,
    SQL_RESUME_REPLICA,
    SQL_CHECK_DATABASE,
    SQL_DROP_DATABASE,
    SQL_CREATE_DATABASE,
    SQL_CHECK_PUBLICATION,
    SQL_CREATE_SUBSCRIPTION,
    SQL_DROP_SUBSCRIPTION,
    SQL_SHOW_VERSION,
    SQL_GET_REPLAY_LSN,
)


_logger = getLogger(__name__)
getLogger("paramiko").setLevel(WARNING)


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
            # print(err.read().decode())
            _logger.error(f"Command {command} not found")
        return out.read().decode()


class SqlConn():
    def __init__(self, host, db="repmgr", user="repmgr", password="labas123", port="5432"):
        try:
            self.sql_conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=db
            )
        except psycopg2.OperationalError:
            _logger.error(f"Unable to connect to {host} with user {user} and database {db}")
            raise PsqlConnectionError(f"Unable to connect to {host} with user {user} and database {db}")
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

    def query(self, query, fetchone=False, fetchall=False) -> list:
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
        query = f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
        try:
            self.query(query)
        except psycopg2.errors.DuplicateObject:
            _logger.warning(f"Replication slot {slot_name} exists")
            self.sql_conn.rollback()

    def drop_replication_slot(self, slot_name):
        query = f"SELECT pg_drop_replication_slot('{slot_name}')"
        try:
            self.query(query)
        except psycopg2.errors.UndefinedObject:
            _logger.warning(f"Replication slot {slot_name} - doesn't exists")
            self.sql_conn.rollback()

    def create_publication(self, pub_name):
        query = f"CREATE publication {pub_name} for all tables"
        try:
            self.query(query)
        except psycopg2.errors.DuplicateObject:
            _logger.warning(f"Publication {pub_name} - already exists")

    def drop_publication(self, pub_name):
        query = f"DROP publication {pub_name}"
        try:
            self.query(query)
        except psycopg2.errors.UndefinedObject:
            _logger.warning(f"Publication {pub_name} - doesn't exist")
            self.sql_conn.rollback()

    def create_subscriber(self, name, host, database, repl_slot) -> None:
        self.query(SQL_CREATE_SUBSCRIPTION.format(name=name, master=host, db=database, pub_name=name, repl_slot=repl_slot))

    def drop_subscriber(self, name) -> None:
        self.sql_conn.autocommit = True
        try:
            self.query(SQL_DROP_SUBSCRIPTION.format(name=name))
        except psycopg2.errors.UndefinedObject:
            _logger.warning(f"Subscription {name} doesn't exist!")
        finally:
            self.sql_conn.autocommit = False

    def is_replica_pause(self) -> bool:
        if self.query(SQL_IS_REPLICA_PASUSED, fetchone=True)[0]:
            _logger.warning("Replication is paused!")
            return True
        return False

    def pause_replica(self) -> None:
        self.query(SQL_PAUSE_REPLICA)

    def resume_replica(self) -> None:
        self.query(SQL_RESUME_REPLICA)

    def check_database(self, database) -> bool:
        if self.query(SQL_CHECK_DATABASE.format(database), fetchone=True)[0] > 0:
            _logger.debug(f"Database {database} exists")
            return True
        return False

    def drop_database(self, database) -> None:
        _logger.debug(f"Droping database {database}")
        self.sql_conn.autocommit = True
        self.query(SQL_DROP_DATABASE.format(database))
        self.sql_conn.autocommit = False

    def create_database(self, database, owner="postgres") -> None:
        self.sql_conn.autocommit = True
        self.query(SQL_CREATE_DATABASE.format(db=database, owner=owner))
        self.sql_conn.autocommit = False

    def server_version(self) -> float:
        return float(self.query(SQL_SHOW_VERSION, fetchone=True)[0])

    def get_replay_lsn(self, app_name) -> str:
        return self.query(SQL_GET_REPLAY_LSN.format(app_name=app_name), fetchone=True)[0]
