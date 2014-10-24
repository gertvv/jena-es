if [ -z "$FUSEKI_HOME" ]; then
  echo "Please set \$FUSEKI_HOME to enable pages";
fi

if [ ! -d "$FUSEKI_HOME" ]; then
  echo "\$FUSEKI_HOME=$FUSEKI_HOME does not exist" 1>&2
fi

java -Xmx1200M -jar target/JenaEventSourcing-0.0.1-SNAPSHOT-jar-with-dependencies.jar --pages $FUSEKI_HOME/pages --conf assemble-es.ttl
