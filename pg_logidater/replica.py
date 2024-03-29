from logging import getLogger
from pg_logidater.utils import SqlConn, ServerConn
from sys import exit
from math import floor
from os import path
from pg_logidater.exceptions import (
    PsqlConnectionError,
)

_logger = getLogger(__name__)


def replica_check(psql: SqlConn, **kwargs) -> None:
    if psql.is_replica_pause():
        _logger.critical("Replica paused!!! We need up to date replica")
        # raise ReplicaAlreadyPaused
        psql.resume_replica()


def replica(**kwargs) -> None:
    host = kwargs["replica"]
    database = kwargs["database"]["name"]
    try:
        psql = SqlConn(host, database)
        replica_check(psql, **kwargs)
        psql.pause_replica()
        if not psql.is_replica_pause():
            _logger.critical("Unable to pause replica!!!, ABORTING")
            exit(1)
        psql_version = floor(psql.server_version())
        kwargs.update({"psql_version": psql_version})
    except PsqlConnectionError:
        _logger.critical(f"UNable to connect to {host} with database {database}")
        exit(1)
    replica_app, replica_slot = replica_info(host, psql_version)
    kwargs["replica_info"]["app"] = replica_app
    kwargs["replica_info"]["slot"] = replica_slot


def replica_info(host, psql_version, user="postgres") -> None:
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
