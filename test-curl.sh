#!/bin/bash

set -e # exit on any error

DATASETS="http://localhost:8080/datasets"

function checkResponse {
  # read first line (eliminate carriage returns and skip '100 Continue' blocks)
  read str <  <(tr -d '\r' | sed -e '/HTTP\/1.1 100/,+1d')
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

echo "== Update query =="

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

echo "== Graph store PUT =="

curl -s -D 29-headers -X PUT $DATA?graph=$GRAPH \
  -H "Content-Type: text/turtle" -H "X-Accept-EventSource-Version: $V2" \
  --data "<x> <y> <z>" 
checkResponse 200 < 29-headers
V3=$(extractVersion < 29-headers)
echo $V3

curl -G -s -D 30-headers $QUERY -H "Accept: text/plain" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { ?s <b> <e> } }" > 30-body
checkResponse 200 < 30-headers
checkVersion $V3 < 30-headers
checkEqual "no" $(< 30-body)

curl -G -s -D 31-headers $QUERY -H "Accept: text/plain" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { <x> <y> <z> } }" > 31-body
checkResponse 200 < 31-headers
checkVersion $V3 < 31-headers
checkEqual "yes" $(< 31-body)

echo "== Graph store POST =="

curl -s -D 32-headers -X POST $DATA?graph=$GRAPH \
  -H "Content-Type: text/turtle" -H "X-Accept-EventSource-Version: $V3" \
  --data "<d> <e> <f>" 
checkResponse 200 < 32-headers
V4=$(extractVersion < 32-headers)
echo $V4

curl -G -s -D 33-headers $QUERY -H "Accept: text/plain" \
  --data-urlencode "query=ASK { GRAPH <$GRAPH> { <x> <y> <z> . <d> <e> <f> . } }" > 33-body
checkResponse 200 < 33-headers
checkVersion $V4 < 33-headers
checkEqual "yes" $(< 33-body)

echo "== Graph store GET =="

curl -s -D 34-headers -X GET $DATA?graph=$GRAPH \
  -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: $V3"
checkResponse 200 < 34-headers

curl -s -D 35-headers -X GET $DATA?graph=$GRAPH \
  -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: $V4"
checkResponse 200 < 35-headers

curl -s -D 36-headers -X GET $DATA?graph=$GRAPH \
  -H "Accept: application/rdf+xml" -H "X-Accept-EventSource-Version: $V4"
checkResponse 200 < 36-headers

echo "== Graph store HEAD =="

curl -s -D 37-headers -I $DATA?graph=$GRAPH \
  -H "Accept: application/rdf+xml" -H "X-Accept-EventSource-Version: $V4"
checkResponse 200 < 37-headers

echo "== Graph store DELETE =="

curl -s -D 38-headers -X DELETE $DATA?graph=$GRAPH \
  -H "X-Accept-EventSource-Version: $V4"
checkResponse 200 < 38-headers
V5=$(extractVersion < 38-headers)
echo $V5

curl -s -D 39-headers -X GET $DATA?graph=$GRAPH \
  -H "Accept: application/rdf+xml" > 39-body
checkResponse 200 < 39-headers
checkVersion $V5 < 39-headers
checkEmpty < 39-body

echo "== GET default graph =="

curl -s -D 40-headers -X GET $DATA?default \
  -H "Accept: text/turtle" -H "X-Accept-EventSource-Version: $V3"
checkResponse 200 < 40-headers

echo "== PUT default graph =="

curl -s -D 41-headers -X PUT $DATA?default \
  -H "Content-Type: text/turtle" -H "X-Accept-EventSource-Version: $V5" \
  --data "<x> <y> <z>" 
checkResponse 200 < 41-headers
V6=$(extractVersion < 41-headers)
echo $V6

curl -s -D 42-headers -X GET $DATA?default \
  -H "Accept: text/turtle"
checkResponse 200 < 42-headers
checkVersion $V6 < 42-headers

echo "== POST default graph =="

curl -s -D 43-headers -X PUT $DATA?default \
  -H "Content-Type: text/turtle" -H "X-Accept-EventSource-Version: $V6" \
  --data "<x> <y> <a>" 
checkResponse 200 < 43-headers
V7=$(extractVersion < 43-headers)
echo $V7

curl -s -D 44-headers -X GET $DATA?default \
  -H "Accept: text/turtle"
checkResponse 200 < 44-headers
checkVersion $V7 < 44-headers

echo "== DELETE default graph =="

curl -s -D 45-headers -X DELETE $DATA?default \
  -H "X-Accept-EventSource-Version: $V7"
checkResponse 200 < 45-headers
V8=$(extractVersion < 45-headers)
echo $V8

curl -s -D 46-headers -X GET $DATA?default \
  -H "Accept: text/turtle"
checkResponse 200 < 46-headers
checkVersion $V8 < 46-headers

echo "== Create dataset with default graph content"

curl -s -D 47-headers -H "Content-Type: text/turtle" $DATASETS  \
  --data "<d> <e> <f>" > 47-body
checkResponse 201 < 47-headers
checkEmpty < 47-body
D2=$(extractLocation < 47-headers)
D2_V0=$(extractVersion < 47-headers)

echo $D2
echo $D2_V0

curl -s -D 48-headers -X GET $D2/data?default \
  -H "Accept: text/turtle"
checkResponse 200 < 48-headers
checkVersion $D2_V0 < 48-headers

echo "== Create dataset with commit meta-data"

CREATOR="http://example.com/SomeAuthor"
TITLE=$(echo -n "This is a title brå" | base64)
DESCRIPTION=$(echo -e "This is a long description\nWith newlines!" | base64)

curl -s -D 49-headers -X POST $DATASETS \
  -H "X-EventSource-Creator: $CREATOR" \
  -H "X-EventSource-Title: $TITLE" \
  -H "X-EventSource-Description: $DESCRIPTION"
D3=$(extractLocation < 49-headers)
D3_V0=$(extractVersion < 49-headers)

curl $D3

echo "== Testing with a larger dataset =="

GRAPH=http://example.com/sp2b

curl -s -D 50-headers -X POST $DATASET/data?graph=$GRAPH \
  -H "Content-Type: text/turtle" \
  --data-binary @sp2b.n3
checkResponse 200 < 50-headers
V9=$(extractVersion < 50-headers)

curl -G -s -D 51-headers $DATASET/query \
  -H "Accept: application/sparql-results+json" \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * WHERE { GRAPH <$GRAPH> { ?s a foaf:Person . ?s ?p ?o . } }" > 51-body
checkResponse 200 < 51-headers

echo "== Query with OPTIONAL (triggers delayed processing of ResultSet) =="

curl -s -D 52-headers -X POST $DATASET/data?graph=http://example.com/study \
  -H "Content-Type: text/turtle" \
  --data " @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . <http://example.com/study> rdfs:label \"Name\" ; rdfs:comment \"Title\" ."
checkResponse 200 < 52-headers

QUERYSTR="PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?obj ?label ?comment
WHERE {
  GRAPH ?obj {
    ?obj rdfs:label ?label .
    OPTIONAL { ?obj rdfs:comment ?comment . }
  }
}"

curl -G -s -D 53-headers $DATASET/query \
  -H "Accept: application/sparql-results+json" \
  --data-urlencode "query=$QUERYSTR"
checkResponse 200 < 53-headers

echo "== SPARQL Update default graph =="

curl -s -D 54-headers -X POST $D2/update \
  -H "Content-Type: application/sparql-update" \
  --data "INSERT DATA { <a> <b> <c> }"
checkResponse 200 < 54-headers

curl -G -s -D 55-headers $D2/query \
  -H "Content-Type: application/sparql-query" -H "Accept: text/plain" \
  --data-urlencode "query=ASK { <a> <b> <c> . <d> <e> <f> . }" > 55-body
checkResponse 200 < 55-headers
checkEqual "yes" $(< 55-body)

echo "== Graph store version conflict =="

curl -s -D 56-headers -X PUT $DATA?default \
  -H "Content-Type: text/turtle" -H "X-Accept-EventSource-Version: $D2_V0" \
  --data "<g> <h> <i>" 
checkResponse 409 < 56-headers

echo "== Graph store invalid request =="

curl -s -D 57-headers -X PUT $DATA?default \
  -H "Content-Type: text/turtle" \
  --data "<g> <h> ." 
checkResponse 400 < 57-headers

echo "== History =="

curl $DATASET/history > 58-body
echo `wc -l 58-body` "lines"

echo "== Blank nodes =="

GRAPH=http://example.com/blankNodes
curl -s -D 59-headers -X PUT $DATA?graph=$GRAPH \
  -H "Content-Type: text/turtle" \
  --data " @prefix foaf: <http://xmlns.com/foaf/0.1/> . <x> foaf:knows [ foaf:name \"Alice\" ] , [ foaf:name \"Bob\" ] ."
checkResponse 200 < 59-headers
V10=$(extractVersion < 59-headers)
echo $V10

curl -G -s -D 60-headers $DATASET/query \
  -H "Content-Type: application/sparql-query" -H "Accept: text/plain" \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * { GRAPH <$GRAPH> { ?x foaf:knows ?node . ?node foaf:name ?name } }"
checkResponse 200 < 60-headers

echo "== Via ?default-graph-uri and ?named-graph-uri =="

curl -G -s -D 61-headers $DATASET/query \
  -H "Content-Type: application/sparql-query" -H "Accept: text/plain" \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * { ?x foaf:knows ?node . ?node foaf:name ?name }" \
  --data-urlencode "default-graph-uri=$GRAPH"
checkResponse 200 < 61-headers

curl -G -s -D 62-headers $DATASET/query \
  -H "Content-Type: application/sparql-query" -H "Accept: text/plain" \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * { GRAPH <$GRAPH> { ?x foaf:knows ?node . ?node foaf:name ?name } }" \
  --data-urlencode "named-graph-uri=http://example.com/nonGraph"
checkResponse 200 < 62-headers

curl -G -s -D 63-headers $DATASET/query \
  -H "Content-Type: application/sparql-query" -H "Accept: text/plain" \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * { GRAPH <$GRAPH> { ?x foaf:knows ?node . ?node foaf:name ?name } }" \
  --data-urlencode "named-graph-uri=$GRAPH"
checkResponse 200 < 63-headers

curl -s -D 64-headers -X PUT $DATA?graph=$GRAPH- \
  -H "Content-Type: text/turtle" \
  --data " @prefix foaf: <http://xmlns.com/foaf/0.1/> . <x> foaf:knows [ foaf:name \"Carol\" ] . [ foaf:name \"Eve\" ] foaf:knows <x> ."
checkResponse 200 < 64-headers
# V11

curl -G -s -D 65-headers $DATASET/query \
  -H "Content-Type: application/sparql-query" -H "Accept: text/plain" \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT * { ?x foaf:knows ?node . ?node foaf:name ?name }" \
  --data-urlencode "default-graph-uri=$GRAPH" \
  --data-urlencode "default-graph-uri=$GRAPH-"
checkResponse 200 < 65-headers

curl -G -s -D 66-headers $DATASET/query \
  -H "Content-Type: application/sparql-query" -H "Accept: text/plain" \
  --data-urlencode "query=PREFIX foaf: <http://xmlns.com/foaf/0.1/> SELECT ?node ?name { GRAPH ?g { ?x foaf:knows ?node . ?node foaf:name ?name } }" \
  --data-urlencode "named-graph-uri=$GRAPH" \
  --data-urlencode "named-graph-uri=$GRAPH-"
checkResponse 200 < 66-headers

echo "=== Non-existent dataset should 404 ==="

curl -G -s -D 67-headers $DATASETS/not-a-dataset/data?graph=http://example.com/ >/dev/null
checkResponse 404 < 67-headers

curl -G -s -D 68-headers $DATASET/data?graph=http://example.com/
checkResponse 200 < 68-headers

curl -G -s -D 69-headers $DATASETS/not-a-dataset/query \
  --data-urlencode "query=SELECT * WHERE { GRAPH <$GRAPH> { ?s ?p ?o } }"
checkResponse 404 < 69-headers

curl -G -s -D 70-headers $DATASETS/not-a-dataset
checkResponse 404 < 70-headers

curl -G -s -D 71-headers $DATASETS/not-a-dataset/history
checkResponse 404 < 71-headers

# Copy a graph from another dataset

echo "=== Copying of graphs ==="

curl -s -D 72-headers -o 72-body -X POST $DATASETS
checkResponse 201 < 72-headers
DSOURCE=$(extractLocation < 72-headers)

curl -s -D 73-headers -H "Content-Type: text/turtle" $DSOURCE/data?graph=$GRAPH  \
	  --data "<a> <b> <c>, <d>" > 73-body
checkResponse 200 < 73-headers

function extractRevisions {
  str=$(grep "^<.*/revisions/.*>$" | sed 's/<//' | sed 's/>//')
  if [ -z "$str" ]; then
    >&2 echo "NULL revision"
    exit 1
  fi
  echo "$str" | tr -d '\r'
}

curl -s -D 74-headers -o 74-body $DSOURCE
checkResponse 200 < 74-headers
RSOURCE=$(extractRevisions < 74-body)

curl -s -D 75-headers -o 75-body -X POST $DATASETS
checkResponse 201 < 75-headers
DSINK=$(extractLocation < 75-headers)

curl -s -D 76-headers -X POST "$DSINK/data?graph=$GRAPH&copyOf=$RSOURCE" > 76-body
checkResponse 200 < 76-headers

curl $DSINK

curl $DSINK/data?graph=$GRAPH

# TODO: update query with using list

exit 0

# Get latest version info

curl $DATASET

# Get history


# Commit meta-data

curl -s -D - -X POST -H "Content-Type: text/turtle" \
  -H "X-EventSource-Creator: http://example.com/PeterParker" \
  -H "X-EventSource-Title: Q29weSBHcmVlbkdvYmxpbi9TcGlkZXJtYW4=" \
  --data "<a> <b> <f>" $DATA?graph=$GRAPH
