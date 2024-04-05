pg-logidater
============
> [!CAUTION]
> Use with yout own risk, not batletested

PostgreSQL logical replication setup utility.
Must be executed on target host.

## Requirements:
* Must be executed with root or user must have setuid/setgid capabilities
* User must have ability to connect to replica host using ssh key (no password accepted)
* .pagpass containing all needed passwords if required
* postgresql installed and running

## Examples
```
pg-logidater --database db_name --master-host 127.0.0.1 --replica-host 127.0.0.2 --psql-user super_user --repl-name name_for_pub_sub_repl save-cli-options
pg-logidater --saved-conf setup-replica
pg-logidater --saved-conf sync-sequences
pg-logidater --saved-conf remove-repl-config
```

Clean up target host:
```
pg-logidater --saved-conf drop-setup
```
