from logging import getLogger
from pg_logidater.utils import SqlConn, ServerConn
from os import path
from pg_logidater.exceptions import (
    ReplicaPaused
)

_logger = getLogger(__name__)


def pause_replica(psql: SqlConn) -> None:
    _logger.info("Pausing replica")
    if psql.is_replica_pause():
        psql.resume_replica()
        raise ReplicaPaused
    psql.pause_replica()


def replica_info(host, user="postgres") -> (str, str):
    _logger.info("Collecting replica info")
    with ServerConn(host, user) as ssh:
        cli = "awk -F '=' /PGDATA=/'{print $NF}' .bash_profile"
        _logger.debug(f"Executing: {cli}")
        pgdata = ssh.run_cmd(cli)
        auto_conf_name = path.join(pgdata.strip(), "postgresql.auto.conf")
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
