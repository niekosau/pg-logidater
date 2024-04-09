import os
import pwd
import argparse
import json
from psycopg2 import OperationalError
from logging import getLogger
from sys import exit
from pg_logidater.exceptions import (
    PsqlConnectionError,
)
from pg_logidater.utils import (
    SqlConn,
    setup_logging,
    prepare_directories
)
from pg_logidater.master import (
    master_prepare,
    master_checks
)
from pg_logidater.replica import (
    pause_replica,
    replica_info,
)
from pg_logidater.tartget import (
    create_subscriber,
    create_database,
    target_check,
    sync_roles,
    sync_database,
    get_replica_position,
    dump_restore_seq
)


_logger = getLogger(__name__)
parser = argparse.ArgumentParser()
parser.add_argument(
    "--save-log",
    help="Save pg-logidater log to file",
    default="/tmp/pg-logidater.log"
)
parser.add_argument(
    "--app-tmp-dir",
    help="Temp directory to store dumps",
    type=str,
    default="/tmp/pg-logidater/tmp"
)
parser.add_argument(
    "--app-log-dir",
    help="Cli output log dir, default: /tmp/pg-logidater/log",
    type=str,
    default="/tmp/pg-logidater/log"
)
parser.add_argument(
    "--saved-conf",
    help="Path to file with saved json config",
    type=str
)
parser.add_argument(
    "-u",
    "--user",
    type=str,
    help="User for running application, default=postgres",
    default="postgres"
)
parser.add_argument(
    "--database",
    help="Database to setup logical replication",
)
parser.add_argument(
    "--master-host",
    help="Master host from which to setup replica",
    type=str,
)
parser.add_argument(
    "--replica-host",
    help="Replica host were to take dump",
    type=str,
)
parser.add_argument(
    "--psql-user",
    help="User for connecting to psql",
    type=str,
)
parser.add_argument(
    "--repl-name",
    help="Name for publication, subscription and replication slot",
    type=str,
)
log_level = parser.add_mutually_exclusive_group()
log_level.add_argument(
    "--log-level",
    type=str,
    choices=["debug", "info", "warning", "eror", "critical"],
    help="Log level for console outpu, default=info",
    default="info"
)
log_level.add_argument(
    "-d",
    "--debug",
    action="store_true"
)
log_level.add_argument(
    "--verbose",
    action="store_true"
)
subparser = parser.add_subparsers(dest="cli")


def argument(*name_of_flags, **kwargs) -> list:
    return (list(name_of_flags), kwargs)


def cli(args=[], parent=subparser, cmd_aliases=None):
    if cmd_aliases is None:
        cmd_aliases = []

        def decorator(func):
            parser = parent.add_parser(
                func.__name__.replace("_", "-"),
                description=func.__doc__,
                aliases=cmd_aliases
            )
            for arg in args:
                parser.add_argument(*arg[0], **arg[1])
            parser.set_defaults(func=func)
        return decorator


def drop_privileges(user) -> None:
    _logger.info(f"Chnaging user to: {user}")
    try:
        change_user = pwd.getpwnam(user)
        os.setgid(change_user.pw_gid)
        os.setuid(change_user.pw_uid)
        os.environ["HOME"] = change_user.pw_dir
    except PermissionError:
        _logger.error("Program must be executed as root!!")
        exit(1)
    except KeyError:
        _logger.error(f"{user} user doesn't exist")
        exit(1)


@cli(
    [
        argument(
            "--ignore-pkey",
            help="Ignore missing pkeys",
            action="store_true"
        )
    ]
)
def setup_replica(args) -> None:
    try:
        master_sql = SqlConn(args["master_host"], user=args["psql_user"], db=args["database"])
        replica_sql = SqlConn(args["replica_host"], args["psql_user"])
        target_sql = SqlConn("/tmp", user="postgres", db="postgres")
    except PsqlConnectionError as e:
        _logger.critical(e)
    db_size = master_checks(
        psql=master_sql,
        slot_name=args["repl_name"],
        pub_name=args["repl_name"],
        skip_pkey_check=args["ignore_pkey"]
    )
    target_check(
        psql=target_sql,
        database=args["database"],
        name=args["repl_name"],
        db_size=db_size
    )
    db_owner = master_prepare(
        psql=master_sql,
        name=args["repl_name"],
        database=args["database"]
    )
    create_database(
        psql=target_sql,
        database=args["database"],
        owner=db_owner
    )
    pause_replica(
        psql=replica_sql
    )
    app_name, slot_name = replica_info(
        host=args["replica_host"],
        psql=replica_sql
    )
    replica_stop_position = get_replica_position(
        psql=master_sql,
        app_name=app_name
    )
    sync_roles(
        host=args["replica_host"],
        tmp_path=args["app_tmp_dir"],
        log_dir=args["app_log_dir"],
    )
    sync_database(
        host=args["replica_host"],
        user=args["psql_user"],
        database=args["database"],
        tmp_dir=args["app_tmp_dir"],
        log_dir=args["app_log_dir"]
    )
    create_subscriber(
       sub_target=args["master_host"],
       database=args["database"],
       slot_name=args["repl_name"],
       repl_position=replica_stop_position
    )
    _logger.info("Rresuming replication")
    replica_sql.resume_replica()


@cli()
def drop_setup(args) -> None:
    _logger.info("Cleaning target server")
    try:
        target_sql = SqlConn("/tmp", user="postgres", db=args["database"])
        target_sql.drop_subscriber()
        target_sql = SqlConn("/tmp", user="postgres", db="postgres")
        target_sql.drop_database(args["database"])
    except OperationalError as err:
        _logger.warning(err)
    _logger.info("Cleaning up master")
    master_sql = SqlConn(args["master_host"], user=args["psql_user"], db=args["database"])
    master_sql.drop_publication(args["repl_name"])
    master_sql.drop_repl_slot(args["repl_name"])
    _logger.info("Cleaning up replica")
    replica_sql = SqlConn(args["replica_host"], args["psql_user"])
    replica_sql.resume_replica()


@cli()
def sync_sequences(args) -> None:
    master_sql = SqlConn(args["master_host"], user=args["psql_user"], db=args["database"])
    dump_restore_seq(
        psql=master_sql,
        tmp_dir=args["app_tmp_dir"],
        log_dir=args["app_log_dir"]
    )


@cli()
def remove_repl_config(args) -> None:
    _logger.info("Removing logical replication configuration")
    try:
        target_sql = SqlConn("/tmp", user="postgres", db=args["database"])
        _logger.debug(f"Dropping subscriber on localhost for db {args['database']}")
        target_sql.drop_subscriber(drop_slot=True)
    except OperationalError as err:
        _logger.warning(err)
    master_sql = SqlConn(args["master_host"], user=args["psql_user"], db=args["database"])
    _logger.debug(f"Dropping publication on host {args['master_host']} for db {args['database']}")
    master_sql.drop_publication(args["repl_name"])


@cli(
    [
        argument(
            "--conf-save-path",
            help="Path were to save cli options for later use, default: current directory",
            default=os.path.join(os.getcwd(), "pg_logidater.conf")
        )
    ]
)
def save_cli_options(args: dict) -> None:
    _logger.info(f"Saving cli options to file {args['conf_save_path']}")
    with open(args["conf_save_path"], "w") as write_conf:
        args_dict = reduce_dict(args)
        args_dict.pop("func")
        json.dump(args_dict, write_conf, indent=2)


def reduce_dict(args: dict) -> dict:
    try:
        args.pop("conf_save_path")
    except KeyError:
        pass
    try:
        args.pop("cli")
    except KeyError:
        pass
    try:
        args.pop("saved_conf")
    except KeyError:
        pass
    return args


def resolve_config(args: dict) -> dict:
    _logger.info("Merging cli params and config file")
    conf_file = args["saved_conf"]
    conf = {}
    if os.path.isfile(conf_file):
        _logger.debug(f"Reading saved config from: {conf_file}")
        with open(conf_file, "r") as cf:
            conf = dict(json.load(cf))
            _logger.debug(f"Config file content:\n{json.dumps(conf, indent=2)}")
    else:
        raise FileNotFoundError(f"{conf_file} doesn't exists")
    reduce_dict(args)
    merged_dicts = args | conf
    return merged_dicts


def main():
    args = parser.parse_args()
    if args.debug:
        setup_logging(
            log_level="debug",
            debug_ssh=True,
            save_log=args.save_log,
            log_path=args.app_log_dir
        )
    elif args.verbose:
        setup_logging(
            log_level="debug",
            save_log=args.save_log,
            log_path=args.app_log_dir
        )
    else:
        setup_logging(
            log_level=args.log_level,
            save_log=args.save_log,
            log_path=args.app_log_dir
        )
    _logger.debug(f"Cli args: {args}")
    args_dict = vars(args)
    if args.cli is None:
        parser.print_help()
    elif args.cli == "save-cli-options":
        args.func(args_dict)
    else:
        if args.saved_conf is not None:
            args_dict = resolve_config(args_dict)
        drop_privileges(args_dict.pop("user"))
        prepare_directories(args_dict["app_log_dir"], args_dict["app_tmp_dir"])
        args.func(args_dict)
        _logger.info(f"App debug log: {args.save_log}")
        _logger.info(f"Dump/restore logs: {args.app_log_dir}")


if __name__ == "__main__":
    main()
