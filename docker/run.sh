#!/bin/bash

CASSANDRA_HOST=${1:-localhost}

# Try to update checks
wget -q https://raw.githubusercontent.com/eonpatapon/contrail-gremlin/master/gremlin-checks/checks.groovy -O checks.groovy

gremlin-dump --cassandra ${CASSANDRA_HOST} dump.json && bin/gremlin.sh -i checks.groovy dump.json
