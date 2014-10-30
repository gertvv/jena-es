Event Sourcing in Jena
======================

A design of the [Event Sourcing pattern](http://martinfowler.com/eaaDev/EventSourcing.html) for the [Apache Jena](https://jena.apache.org/) triple store.

This consists of two parts:

 - An implementation of event sourcing for a SPARQL/RDF dataset
 - An HTTP API to enable both as close to standard as possible SPARQL interactions, as well as interactions specific to an event sourced store, such as querying the event log.

Event sourced SPARQL Dataset
============================

A SPARQL dataset is simply a collection of graphs, where each graph is a collection of statements (triples). In analogy to version control systems, think of the dataset as a repository and the graphs as files or documents. The event log itself is stored as RDF, where we track the statements added to and removed from each graph. eote that it does not make sense to check whether or not graphs were added or removed, since this is equivalent to them being (respectively) non-empty or empty.

Each event source dataset is described by an event log, which is stored in its own graph:

```
@prefix es: <http://drugis.org/eventSourcing/es#> .
@prefix ex: <http://example.com/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:log42 {

  ex:log42 a es:Log ;
    es:head rdf:nil .

}
```

This represents an empty log, which corresponds to an emtpy dataset. The `es:head` property points to an `rdf:List` of events so far, newest to oldest, for example:

```
@prefix es: <http://drugis.org/eventSourcing/es#> .
@prefix ex: <http://example.com/> .

ex:log42 {

  ex:log42 a es:Log ;
    es:head ( ex:event1  ex:event0 ) .

}
```

Each event is essentially a collection of revisions, plus optionally meta-data (not shown):

```
ex:log42 {

  ex:event0 a es:Event ;
    es:has_revision ex:rev0 , ex:rev1 .

}
```

Each revision names the graph to which it applies, plus the changes to be made:

```

ex:log42 {

  ex:rev0 a es:Revision ;
    es:graph ex:graph ;
    es:assertions ex:assert0 ;
    ex:retractions ex:retract0 .

```

Here, both `ex:assert0` and `ex:retract0` are graphs in the underlying triple store, and `ex:graph` is a graph in the event sourced dataset.
No two revisions on the same event may ever modify the same graph.
If A is the state of the graph before applying a revision, B is the set of assertions and C the set of retractions, then `UNION(DIFFERENCE(A, C), B)` is the state of the graph after applying the revision.

To construct any past state of the database, start at the end of the event log, and apply each event up to and including the desired event.
Applying an event entails applying each revision, in any order.

NOTE: this design is still open for discussion. We *may* want to enable selective borrowing / copying of graphs between datasets.
This could be supported by making revisions "stand alone" entities that refer to their own parent.
Alternatively, this could be an insert of the graph in the other dataset with appropriate meta-data describing its provenance.

Write and transaction support is possible using proxy objects.
Implementations of the standard Jena DatasetGraph and Graph interfaces that keep track of a set of changes on top of the current version enables fully transparent use of the Jena API, including SPARQL query and update.

HTTP API - single dataset 
=========================

There are 2 + n endpoints:

 - on the CURRENT version:
   - SPARQL graph store API (read/write)
   - SPARQL query
   - SPARQL update
   - a default graph that describes meta-data, such as a permanent URI to the version
   - each write operation must support meta-data about the transaction, and must return a URI to the created version
 - on the LOG (history):
   - SPARQL query
 - on each version:
   - SPARQL graph store API (read)
   - SPARQL query
   - a default graph that describes meta-data, such the URI of the CURRENT endpoint 

These are the unresolved questions:

 - A URI scheme, and whether event URIs will be identical to the endpoint where they are accessed. Whether the event ID should be a query parameter or (part of) the URI is also TBD. A consideration here might be what happens if someone builds a dataset on deployment A, and wants to import it on deployment B.
 - How meta-data will be transmitted in conjunction with a SPARQL or RDF payload.
 - Returning the event ID is easy. We can simply send an RDF response body with meta-data on the state of the transaction.

HTTP API - multiple datasets
============================

Simply serve multiple of the above. Multiple datasets can actually be served from the same underlying RDF store.

 - Needs a mechanism for creating new datasets.
