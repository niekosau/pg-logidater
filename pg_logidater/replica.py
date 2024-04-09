from logging import getLogger
from pg_logidater.utils import SqlConn, ServerConn
from os import path
from pg_logidater.exceptions import (
    ReplicaPaused,
    VersionNotSupported
)

_logger = getLogger(__name__)

RECOVYRY_CONF_BY_VERSION = {
    11: "recovery.conf",
    12: "postgresql.auto.conf"
}


def pause_replica(psql: SqlConn) -> None:
    _logger.info("Pausing replica")
    if psql.is_replica_pause():
        raise ReplicaPaused
    psql.pause_replica()


def replica_info(host, psql: SqlConn, user="postgres") -> (str, str):
    _logger.info("Collecting replica info")
    pgdata = psql.get_datadirectory()
    psql_version = int(psql.server_version())
    if psql_version not in RECOVYRY_CONF_BY_VERSION.keys():
        raise VersionNotSupported
    recovey_conf = RECOVYRY_CONF_BY_VERSION[psql_version]
    with ServerConn(host, user) as ssh:
        auto_conf_name = path.join(pgdata, recovey_conf)
        cli = f"cat {auto_conf_name}"
        _logger.debug(f"Executing: {cli}")
        psql_auto_conf = ssh.run_cmd(cli)
        for line in reversed(psql_auto_conf.splitlines()):
            if "application_name" in line:
                replica_app_name = line.split(" ")[-1].removeprefix("application_name=").strip("'")
                _logger.debug(f"Got replica app name: {replica_app_name}")
                break
        for line in reversed(psql_auto_conf.splitlines()):
            if "primary_slot_name" in line:
                replica_slot_name = line.split(" ")[-1].strip("'")
                _logger.debug(f"Got replica slot name: {replica_slot_name}")
                break
        return replica_app_name, replica_slot_name
