#!/bin/bash

java -Xmx1200M -cp target/JenaEventSourcing-0.0.1-SNAPSHOT-jar-with-dependencies.jar util.InsertData --desc assemble-es.ttl "$1"
