Jena Event Sourcing
===================

An implementation of the [Event Sourcing pattern](http://martinfowler.com/eaaDev/EventSourcing.html) for the [Apache Jena](https://jena.apache.org/) triple store.
The implementation is transparent in the sense that it exposes the same API as other Jena backing stores (i.e. the DatasetGraph and Transactional interfaces).
This means that the Fuseki server can be used to serve these datasets without modifications, see [assemble-es.ttl](assemble-es.ttl) for an example of this.
The event log is stored in a standard Jena storage backend, most likely TDB.
This enables the history itself to be exposed through Fuseki and queried using SPARQL.

However, a number of features do require modifications to Fuseki and the HTTP API, namely:

 - Access to older versions of the dataset.
 - Attaching meta-data to events, e.g. the account making the change.
 - Returning the event ID to the client after a change has been committed.

These are cumbersome to implement as Fuseki was clearly not intended for such modifications.
Therefore, Fuseki has been replaced by a custom server based on the Spring framework.
