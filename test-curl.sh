DATA=http://localhost:8080/datasets/hello/data
QUERY=http://localhost:8080/datasets/hello/query
GRAPH=http://trials.drugis.org/studies/9c7bb39e-441c-4a64-a6b9-615f51eb046a

V1=http://drugis.org/eventSourcing/event/4818e24b-50fe-42ce-babc-ffe3569e3da5
V2=http://drugis.org/eventSourcing/event/3156c8c6-df88-4352-9f29-a1dbddfe0278

curl -D headers-current-xml -H "Accept: application/rdf+xml" $DATA?graph=$GRAPH > body-current.xml
curl -D headers-current-ttl -H "Accept: text/turtle" $DATA?graph=$GRAPH > body-current.ttl

curl -D headers-v1-ttl -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: $V1" $DATA?graph=$GRAPH > body-v1.ttl
curl -D headers-v2-ttl -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: $V2" $DATA?graph=$GRAPH > body-v2.ttl

curl -H "X-Accept-EventSource-Version: $V1" "$QUERY?query=SELECT * WHERE { GRAPH <$GRAPH> { <$GRAPH> ?p ?o }} LIMIT 10"

LABEL=http://www.w3.org/2000/01/rdf-schema#label
curl -G -H "X-Accept-EventSource-Version: $V1" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { <$GRAPH> <$LABEL> ?o }}" \
  $QUERY

curl -G -H "X-Accept-EventSource-Version: $V2" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { <$GRAPH> <$LABEL> ?o }}" \
  $QUERY
