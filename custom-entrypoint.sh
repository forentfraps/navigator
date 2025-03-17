#!/bin/bash
set -e

# Example: load a dump file into the default database.
# Adjust the path and DB name for your case.
rm /data/neo4j -rf
neo4j-admin database load neo4j --from-path=/backups --overwrite-destination=true

# Now chain to the official entrypoint script with all args
exec /startup/docker-entrypoint.sh "$@"
