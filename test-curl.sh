#!/bin/bash

set -e # exit on any error

DATASETS="http://localhost:8080/datasets/"

function checkResponse {
  read str <  <(tr -d '\r')
  if [[ $str != "HTTP/1.1 $1"* ]]; then
    >&2 echo $str "did not match expected ($1)"
    exit 1
  fi
}

function extractVersion {
  str=$(grep "X-EventSource-Version: " | sed 's/X-EventSource-Version: //')
  if [ -z "$str" ]; then
    >&2 echo "NULL version"
    exit 1
  fi
  echo "$str" | tr -d '\r'
}

function extractLocation {
  str=$(grep "Location: " | sed 's/Location: //')
  if [ -z "$str" ]; then
    >&2 echo "NULL location"
    exit 1
  fi
  echo "$str" | tr -d '\r'
}

function checkEmpty {
  if [[ ! -z $(<&0) ]]; then
    echo "expected empty response"
    exit 1
  fi
}

function checkVersion {
  expect=$1
  actual=$(extractVersion)
  if [ "$expect" != "$actual" ]; then
    echo "expected versions to be equal"
    exit 1
  fi
}

function checkEqual {
  expect="$1"
  actual="$2"
  if [ "$actual" != "$expect" ]; then
    echo "Expected \'$expect\', got \'$actual\'"; exit 1;
  fi
}

echo "== Create a dataset =="

curl -s -D 00-headers -o 00-body -X POST $DATASETS

checkResponse 201 < 00-headers
DATASET=$(extractLocation < 00-headers)
V0=$(extractVersion < 00-headers)

echo $DATASET
echo $V0

DATA="$DATASET/data"
QUERY=${DATASET}/query
UPDATE=${DATASET}/update
GRAPH=http://example.com/graph1

echo "== Get (empty) content =="

curl -s -D 01-headers -H "Accept: text/turtle" $DATA?graph=$GRAPH > 01-body
checkResponse 200 < 01-headers
checkVersion $V0 < 01-headers
checkEmpty < 01-body

echo "== Upload content =="

curl -s -D 02-headers -H "Content-Type: text/turtle" $DATA?graph=$GRAPH  \
  --data "<a> <b> <c>, <d>" > 02-body
checkResponse 200 < 02-headers
checkEmpty < 02-body
V1=$(extractVersion < 02-headers)

echo $V1

echo "== Get new content =="

curl -s -D 03-headers -H "Accept: text/turtle" $DATA?graph=$GRAPH
checkResponse 200 < 03-headers
checkVersion $V1 < 03-headers

echo "== Version negotiation =="

curl -s -D 04-headers -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: $V0" $DATA?graph=$GRAPH
checkResponse 200 < 04-headers
checkVersion $V0 < 04-headers

curl -s -D 05-headers -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: $V1" $DATA?graph=$GRAPH
checkResponse 200 < 05-headers
checkVersion $V1 < 05-headers

curl -s -D 06-headers -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: http://example.com/nonversion" $DATA?graph=$GRAPH
checkResponse 406 < 06-headers

echo "== Content-type negotiation =="

curl -s -D 07-headers -H "Accept: application/rdf+xml" $DATA?graph=$GRAPH
checkResponse 200 < 07-headers

curl -s -D 08-headers -H "Accept: audio/ogg" $DATA?graph=$GRAPH
checkResponse 406 < 08-headers

echo "== SPARQL query =="

curl -G -s -D 09-headers $QUERY \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 200 < 09-headers
checkVersion $V1 < 09-headers

echo "== SPARQL query version negotiation =="

curl -G -s -D 10-headers $QUERY -H "X-Accept-EventSource-Version: $V1" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 200 < 10-headers
checkVersion $V1 < 10-headers

curl -G -s -D 11-headers $QUERY -H "X-Accept-EventSource-Version: $V0" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 200 < 11-headers
checkVersion $V0 < 11-headers

curl -G -s -D 12-headers $QUERY -H "X-Accept-EventSource-Version: http://example.com/nonversion" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }" > 12-body
checkResponse 406 < 12-headers

echo "== SPARQL query content-type negotiation =="

curl -G -s -D 13-headers $QUERY -H "Accept: application/sparql-results+json" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 200 < 13-headers
checkVersion $V1 < 13-headers

curl -G -s -D 14-headers $QUERY -H "Accept: application/sparql-results+xml" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 200 < 14-headers
checkVersion $V1 < 14-headers

curl -G -s -D 15-headers $QUERY -H "Accept: text/plain" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 200 < 15-headers
checkVersion $V1 < 15-headers

curl -G -s -D 16-headers $QUERY -H "Accept: audio/ogg" \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 406 < 16-headers

echo "== CONSTRUCT query =="

curl -G -s -D 17-headers $QUERY \
  --data-urlencode "query=CONSTRUCT { ?s <x> 3 } WHERE { GRAPH <$GRAPH> { ?s <b> <d> } }"
checkResponse 200 < 17-headers
checkVersion $V1 < 17-headers

curl -G -s -D 18-headers $QUERY -H "Accept: application/rdf+xml" \
  -H "X-Accept-EventSource-Version: $V0" \
  --data-urlencode "query=CONSTRUCT { ?s <x> 3 } WHERE { GRAPH <$GRAPH> { ?s <b> <d> } }"
checkResponse 200 < 18-headers
checkVersion $V0 < 18-headers

echo "== ASK query =="

curl -G -s -D 19-headers $QUERY -H "Accept: text/plain" \
  -H "X-Accept-EventSource-Version: $V0" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { ?s <b> <d> } }" > 19-body
checkResponse 200 < 19-headers
checkVersion $V0 < 19-headers
checkEqual "no" $(< 19-body)

curl -G -s -D 20-headers $QUERY -H "Accept: text/plain" \
  -H "X-Accept-EventSource-Version: $V1" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { ?s <b> <d> } }" > 20-body
checkResponse 200 < 20-headers
checkVersion $V1 < 20-headers
checkEqual "yes" $(< 20-body)

curl -G -s -D 21-headers $QUERY -H "Accept: application/sparql-results+json" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { ?s <b> <d> } }" > 21-body
checkResponse 200 < 21-headers
checkVersion $V1 < 21-headers

curl -G -s -D 22-headers $QUERY -H "Accept: application/sparql-results+xml" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { ?s <b> <d> } }" > 22-body
checkResponse 200 < 22-headers
checkVersion $V1 < 22-headers

echo "== DESCRIBE query =="

curl -G -H "Accept: text/turtle" $QUERY \
  --data-urlencode "query=DESCRIBE <$GRAPH>"
# TODO

echo "== Invalid query =="

curl -G -s -D 24-headers $QUERY \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT ?name WHERE { ?x foaf:name ?name ORDER BY ?name }"
checkResponse 400 < 24-headers

echo "== Invalid update =="

curl -D 25-headers $UPDATE -H "Content-Type: application/sparql-update" \
  --data "PREFIX foaf: <http://xmlns.com/foa/> SELECT ?name WHERE { ?x foaf:name ?name ORDER BY ?name }"
checkResponse 400 < 25-headers

echo "== Update conflict =="

curl -D 26-headers $UPDATE \
  -H "X-Accept-EventSource-Version: $V0" -H "Content-Type: application/sparql-update" \
  --data "INSERT DATA { <a> <b> <c> }"
echo
checkResponse 409 < 26-headers

echo "== Insert data =="

curl -D 27-headers $UPDATE \
  -H "X-Accept-EventSource-Version: $V1" -H "Content-Type: application/sparql-update" \
  --data "INSERT DATA { GRAPH <$GRAPH> { <a> <b> <e> } }"
checkResponse 200 < 27-headers
V2=$(extractVersion < 27-headers)
echo $V2

curl -G -s -D 28-headers $QUERY -H "Accept: text/plain" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { ?s <b> <e> } }" > 28-body
checkResponse 200 < 28-headers
checkVersion $V2 < 28-headers
checkEqual "yes" $(< 28-body)

exit 0

curl -s -D - -X PUT -H "Content-Type: text/turtle" -H "X-Accept-EventSource-Version: $UPDATED" \
  --data "<a> <b> <d>" $DATA?graph=$GRAPH

curl -H "Accept: text/turtle" $DATA?graph=$GRAPH

curl -s -D - -X POST -H "Content-Type: text/turtle" --data "<a> <b> <e>" $DATA?graph=$GRAPH

curl -H "Accept: text/turtle" $DATA?graph=$GRAPH

curl -s -D - -X DELETE $DATA?graph=$GRAPH

curl -I -H "Accept: text/turtle" $DATA?graph=$GRAPH

# Get latest version info

curl $DATASET

# Get history

curl $DATASET/history


curl -s -D - -X POST -H "Content-Type: text/turtle" \
  -H "X-EventSource-Creator: http://example.com/PeterParker" \
  -H "X-EventSource-Title: Q29weSBHcmVlbkdvYmxpbi9TcGlkZXJtYW4=" \
  --data "<a> <b> <f>" $DATA?graph=$GRAPH
