#!/usr/bin/env bash

GREMLIN_YAML="$GREMLIN_HOME/$1"
JAVA="java"
JAVA_OPTIONS="-Xms32m -Xmx512m"
LOG4J_CONF="file:$GREMLIN_HOME/conf/log4j-server.properties"
# Build Java CLASSPATH
CP="$GREMLIN_HOME/conf/"
CP="$CP":$( echo $GREMLIN_HOME/lib/*.jar . | sed 's/ /:/g')
CP="$CP":$( find -L "$GREMLIN_HOME"/ext -mindepth 1 -maxdepth 1 -type d | \
        sort | sed 's/$/\/plugin\/*/' | tr '\n' ':' )
CLASSPATH="${CLASSPATH:-}:$CP"
GREMLIN_SERVER_CMD=org.apache.tinkerpop.gremlin.server.GremlinServer

exec $JAVA -Dlog4j.configuration=$LOG4J_CONF $JAVA_OPTIONS -cp $CP:$CLASSPATH $GREMLIN_SERVER_CMD "$GREMLIN_YAML"
