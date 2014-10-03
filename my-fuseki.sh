if [ -z "$FUSEKI_HOME" ]; then
  echo "Please set \$FUSEKI_HOME";
  exit 1
fi

if [ ! -d "$FUSEKI_HOME" ]; then
  echo "\$FUSEKI_HOME=$FUSEKI_HOME does not exist" 1>&2
  exit 1
fi

java -cp $FUSEKI_HOME/fuseki-server.jar:target/classes/ -Xmx1200M org.apache.jena.fuseki.FusekiCmd --pages $FUSEKI_HOME/pages --conf assemble-es.ttl
