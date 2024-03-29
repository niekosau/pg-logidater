from logging import getLogger, basicConfig, DEBUG
import os
import pwd
from sys import exit
from pg_logidater.master import master
from pg_logidater.replica import replica
from pg_logidater.tartget import target

CURREN_MASTER = "10.123.9.11"
CURRENT_REPLICA = "10.123.9.12"
NEW_MASTER = "10.123.9.13"
USER = "repmgr"
SLOT_NAME = "test_from_py_script"
PUB_NAME = "vu_bitbucket_test_script"
LOG_FORMAT = "[%(module)-8s:%(funcName)-20s| %(levelname)-8s] %(message)-40s"


basicConfig(level=DEBUG, format=LOG_FORMAT)
_logger = getLogger(__name__)


def drop_privileges(user="postgres") -> None:
    try:
        change_user = pwd.getpwnam('postgres')
        os.setgid(change_user.pw_gid)
        os.setuid(change_user.pw_uid)
        os.environ["HOME"] = change_user.pw_dir
    except PermissionError:
        _logger.error("Program must be executed as root!!")
        exit(1)
    except KeyError:
        _logger.error(f"{user} user doesn't exist")
        exit(1)


def main() -> None:
    database = {
        "name": "bitbucket",
    }
    replica_info = {}
    kwargs = {
        "master": CURREN_MASTER,
        "replica": CURRENT_REPLICA,
        "user": USER,
        "slot_name": SLOT_NAME,
        "pub_name": PUB_NAME,
        "database": database,
        "replica_info": replica_info
    }
    master(**kwargs)
    replica(**kwargs)
    target(**kwargs)


if __name__ == "__main__":
    drop_privileges()
    main()
