package org.drugis.rdf.versioning.store;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import org.drugis.rdf.versioning.server.Util;

import com.github.rholder.fauxflake.IdGenerators;
import com.github.rholder.fauxflake.api.IdGenerator;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphExtract;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleBoundary;
import com.hp.hpl.jena.graph.compose.Delta;
import com.hp.hpl.jena.graph.compose.Difference;
import com.hp.hpl.jena.graph.compose.Union;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.util.graph.GraphUtils;
import com.hp.hpl.jena.util.iterator.Filter;
import com.hp.hpl.jena.vocabulary.RDF;

@SuppressWarnings("deprecation")
public class EventSource {
	public static final String ES="http://drugis.org/eventSourcing/es#",
			DCTERMS="http://purl.org/dc/terms/";
	public static final Node esClassDataset = NodeFactory.createURI(ES + "Dataset"),
			esClassDatasetVersion = NodeFactory.createURI(ES + "DatasetVersion"),
			esClassRevision = NodeFactory.createURI(ES + "Revision"),
			esClassMergeTypeCopyTheirs = NodeFactory.createURI(ES + "MergeTypeCopyTheirs"),
			esClassNamedGraphRevision = NodeFactory.createURI(ES + "NamedGraphRevision"),
			esClassDefaultGraphRevision = NodeFactory.createURI(ES + "DefaultGraphRevision"),
			esPropertyHead = NodeFactory.createURI(ES + "head"),
			esPropertyDataset = NodeFactory.createURI(ES + "dataset"),
			esPropertyDefaultGraphRevision = NodeFactory.createURI(ES + "default_graph_revision"),
			esPropertyGraphRevision = NodeFactory.createURI(ES + "graph_revision"),
			esPropertyGraph = NodeFactory.createURI(ES + "graph"),
			esPropertyRevision = NodeFactory.createURI(ES + "revision"),
			esPropertyPrevious = NodeFactory.createURI(ES + "previous"),
			esPropertyAssertions = NodeFactory.createURI(ES + "assertions"),
			esPropertyRetractions = NodeFactory.createURI(ES + "retractions"),
			esPropertyMergedRevision = NodeFactory.createURI(ES + "merged_revision"),
			esPropertyMergeType = NodeFactory.createURI(ES + "merge_type"),
			dctermsDate = NodeFactory.createURI(DCTERMS + "date"),
			dctermsCreator = NodeFactory.createURI(DCTERMS + "creator"),
			dctermsTitle = NodeFactory.createURI(DCTERMS + "title"),
			dctermsDescription = NodeFactory.createURI(DCTERMS + "description");

	public static class EventNotFoundException extends RuntimeException {
		private static final long serialVersionUID = -1603163798182523814L;
		
		public EventNotFoundException(String message) {
			super(message);
		}
	}

	private IdGenerator d_idgen = IdGenerators.newFlakeIdGenerator();

	private DatasetGraph d_datastore;
	private String VERSION;
	private String REVISION;
	private String ASSERT;
	private String RETRACT;
	private String SKOLEM;
	private String d_uriPrefix;
	
	public EventSource(DatasetGraph datastore, String uriPrefix) {
		d_datastore = datastore;
		d_uriPrefix = uriPrefix;
		
		VERSION = uriPrefix + "/versions/";
		REVISION = uriPrefix + "/revisions/";
		ASSERT = uriPrefix + "/assert/";
		RETRACT = uriPrefix + "/retract/";
		SKOLEM = uriPrefix + "/.well-known/genid/";
	}
	
	public DatasetGraph getDataStore() {
		return d_datastore;
	}
	
	public String getUriPrefix() {
		return d_uriPrefix;
	}

	public Node getLatestVersionUri(Node dataset) {
		assertDatasetExists(dataset);
		return Util.getUniqueObject(d_datastore.getDefaultGraph().find(dataset, esPropertyHead, Node.ANY));
	}

	private void assertDatasetExists(Node dataset) {
		if (!datasetExists(dataset)) {
			throw new DatasetNotFoundException(dataset);
		}
	}
	
	private static Map<Node, Node> getGraphRevisions(DatasetGraph eventSource, Node version) {
		Map<Node, Node> map = new HashMap<Node, Node>();
		
		// Named graphs
		for (Iterator<Triple> triples = eventSource.getDefaultGraph().find(version, esPropertyGraphRevision, Node.ANY); triples.hasNext(); ) {
			Node graphRevision = triples.next().getObject();
			Node graphName = Util.getUniqueObject(eventSource.getDefaultGraph().find(graphRevision, esPropertyGraph, Node.ANY));
			Node revision = Util.getUniqueObject(eventSource.getDefaultGraph().find(graphRevision, esPropertyRevision, Node.ANY));
			map.put(graphName, revision);
		}
		
		// Default graph
		Node graphRevision = Util.getUniqueOptionalObject(eventSource.getDefaultGraph().find(version, esPropertyDefaultGraphRevision, Node.ANY));
		if (graphRevision != null) {
			Node revision = Util.getUniqueObject(eventSource.getDefaultGraph().find(graphRevision, esPropertyRevision, Node.ANY));
			map.put(Quad.defaultGraphNodeGenerated, revision);
		}
		return map;
	}

	public DatasetGraph getVersion(Node dataset, Node version) {
		assertDatasetExists(dataset);
		if (!versionExists(dataset, version)) {
			return null;
		}
		DatasetGraph ds = DatasetGraphFactory.createMem();
		for (Map.Entry<Node, Node> entry : getGraphRevisions(d_datastore, version).entrySet()) {
			Node graphName = entry.getKey();
			Node revision = entry.getValue();
			Graph graph = getRevision(revision);
			if (graphName.equals(Quad.defaultGraphNodeGenerated)) {
				ds.setDefaultGraph(graph);
			} else {
				ds.addGraph(graphName, graph);
			}
		}
		return ds;
	}
	
	public boolean datasetExists(Node dataset) {
		return d_datastore.getDefaultGraph().find(dataset, RDF.Nodes.type, esClassDataset).hasNext();
	}
	
	private boolean versionExists(Node dataset, Node version) {
		Node current = Util.getUniqueObject(d_datastore.getDefaultGraph().find(dataset, esPropertyHead, Node.ANY));
		while (!version.equals(current)) {
			current = Util.getUniqueOptionalObject(d_datastore.getDefaultGraph().find(current, esPropertyPrevious, Node.ANY));
			if (current == null) {
				return false;
			}
		}
		return true;
	}
	
	private boolean revisionExists(Node revision) {
		return d_datastore.getDefaultGraph().find(revision, RDF.Nodes.type, esClassRevision).hasNext();
	}

	public DatasetGraph getLatestVersion(Node dataset) {
		return getVersion(dataset, getLatestVersionUri(dataset));
	}

	public Graph getRevision(Node revision) {
		if (!revisionExists(revision)) {
			return null;
		}
		Node previous = Util.getUniqueOptionalObject(d_datastore.getDefaultGraph().find(revision, esPropertyPrevious, Node.ANY));
		Graph graph = GraphFactory.createGraphMem();
		if (previous != null) {
			graph = getRevision(previous);
		}
		return applyRevision(d_datastore, graph, revision);
	}
	
	private static Graph matchingGraph(DatasetGraph eventSource, Iterator<Triple> result) {
		if (result.hasNext()) {
			Graph graph = eventSource.getGraph(result.next().getObject());
			if(result.hasNext()) {
				throw new IllegalStateException("Multiple objects on property of arity 0/1");
			}
			return graph;
		}
		return GraphFactory.createGraphMem();
	}

	public static Graph applyRevision(DatasetGraph eventSource, Graph base, Node revision) {
		Graph additions = matchingGraph(eventSource, eventSource.getDefaultGraph().find(revision, esPropertyAssertions, Node.ANY));
		Graph retractions = matchingGraph(eventSource, eventSource.getDefaultGraph().find(revision, esPropertyRetractions, Node.ANY));
		return new Union(new Difference(base, retractions), additions);
	}

    // http://stackoverflow.com/questions/3914404
	private static String now() {
	    TimeZone tz = TimeZone.getTimeZone("UTC");
	    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	    df.setTimeZone(tz);
	    String nowAsISO = df.format(new Date());
	    return nowAsISO;
	}
	
	/**
	 * Write an event to the log, assuming it is consistent with the current state.
	 * @param event The event (changeset).
	 * @return The ID of the event.
	 */
	public Node writeToLog(Node dataset, DatasetGraphDelta event) {
		assertDatasetExists(dataset);
		return writeToLog(dataset, event, GraphFactory.createGraphMem());
	}
	
	/**
	 * Add a triple to the default graph.
	 */
	private static void addTriple(DatasetGraph eventSource, Node s, Node p, Node o) {
		eventSource.getDefaultGraph().add(new Triple(s, p, o));
	}

	/**
	 * Write an event to the log, assuming it is consistent with the current state.
	 * @param d_datastore The DatasetGraph containing the event log.
	 * @param log The URI of the event log.
	 * @param event The event (changeset).
	 * @param meta A graph containing meta-data. It must contain a single blank node of class es:DatasetVersion, the properties of which will be added to the event meta-data.
	 * @return The ID of the event.
	 */
	public Node writeToLog(Node dataset, DatasetGraphDelta event, Graph meta) {
		Node previous = getLatestVersionUri(dataset);
		Node version = NodeFactory.createURI(VERSION + UUID.randomUUID().toString());
		
		addTriple(d_datastore, version, RDF.Nodes.type, esClassDatasetVersion);
		addTriple(d_datastore, version, esPropertyDataset, dataset);
		addTriple(d_datastore, version, dctermsDate, NodeFactory.createLiteral(now(), XSDDatatype.XSDdateTime));
		
		addMetaData(d_datastore, meta, version, esClassDatasetVersion);

		Map<Node, Node> previousRevisions = getGraphRevisions(d_datastore, previous);
		for (Iterator<Node> graphs = event.listGraphNodes(); graphs.hasNext(); ) {
			writeGraphRevision(event, version, previousRevisions, graphs.next(), meta);
		}
		writeGraphRevision(event, version, previousRevisions, Quad.defaultGraphNodeGenerated, meta);
		
		addTriple(d_datastore, version, esPropertyPrevious, previous);
		d_datastore.getDefaultGraph().remove(dataset, esPropertyHead, previous);
		addTriple(d_datastore, dataset, esPropertyHead, version);
		
		return version;
	}

	private void writeGraphRevision(DatasetGraphDelta event, Node version, Map<Node, Node> previousRevisions, final Node graph, final Graph meta) {
		if (event.getGraph(graph).isEmpty()) {
			return;
		}

		if (!event.getModifications().containsKey(graph)) {
			addGraphRevision(d_datastore, version, graph, previousRevisions.get(graph));
		} else {
			Node newRevision = writeRevision(event.getModifications().get(graph), previousRevisions.get(graph));
			addRevisionMetaData(graph, meta, newRevision);
			addGraphRevision(d_datastore, version, graph, newRevision);
		}
	}

	private void addRevisionMetaData(final Node graph, final Graph meta,
			Node newRevision) {
		Node graphRevision = null;
		if (graph.equals(Quad.defaultGraphNodeGenerated)) {
			graphRevision = Util.getUniqueOptionalSubject(meta.find(Node.ANY, RDF.Nodes.type, EventSource.esClassDefaultGraphRevision));
		} else {
			graphRevision = Util.getUniqueOptionalSubject(meta.find(Node.ANY, RDF.Nodes.type, EventSource.esClassNamedGraphRevision).filterKeep(new Filter<Triple>() {
				@Override public boolean accept(Triple o) {
					return meta.find(o.getSubject(), EventSource.esPropertyGraph, graph).hasNext();
				}}));
		}
		if (graphRevision != null) {
			Node revisionMetaRoot = Util.getUniqueObject(meta.find(graphRevision, esPropertyRevision, Node.ANY));
			addMetaData(d_datastore, newRevision, meta, revisionMetaRoot);
		}
	}

	private static void addMetaData(DatasetGraph eventSource, Graph meta, Node resource, Node resourceClass) {
		Node root = getMetaDataRoot(meta, resourceClass);
		if (root == null) {
			return;
		}
		
		addMetaData(eventSource, resource, meta, root);
	}

	/**
	 * Add meta-data to a resource. Filters the given meta-data graph.
	 */
	private static void addMetaData(DatasetGraph eventSource, Node resource, Graph meta, Node metaDataRoot) {
		// Restrict meta-data to describing the current event only
		meta = (new GraphExtract(TripleBoundary.stopNowhere)).extract(metaDataRoot, meta);
		
		// Prevent the setting of predicates we set ourselves
		meta.remove(metaDataRoot, RDF.Nodes.type, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyHead, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyPrevious, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyGraphRevision, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyDefaultGraphRevision, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyRevision, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyGraph, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyAssertions, Node.ANY);
		meta.remove(metaDataRoot, EventSource.esPropertyRetractions, Node.ANY);
		meta.remove(metaDataRoot, EventSource.dctermsDate, Node.ANY);
		
		// Replace the temporary root node by the event URI
		replaceSubject(meta, metaDataRoot, resource);
		
		// Copy the data into the event log
		GraphUtil.addInto(eventSource.getDefaultGraph(), meta);
	}

	private static Node getMetaDataRoot(Graph meta, Node resourceClass) {
		Set<Triple> metaRoots = meta.find(Node.ANY, RDF.Nodes.type, resourceClass).toSet();

		if (metaRoots.size() == 1) {
			return metaRoots.iterator().next().getSubject();
		} else if (metaRoots.size() == 0) {
			return null;
		}
		
		throw new IllegalStateException(
					"The supplied meta-data must have at most one resource of class "
					+ resourceClass.getURI() + " but found " + metaRoots.size());
	}

	/**
	 * Add a graphRevision to a DatasetVersion.
	 * @param version The version to add this graphRevision to.
	 * @param graph The graph URI.
	 * @param revision The revision of the graph.
	 */
	private static void addGraphRevision(DatasetGraph eventSource, Node version, Node graph, Node revision) {
		Node graphRevision = NodeFactory.createAnon();
		if (graph.equals(Quad.defaultGraphNodeGenerated)) {
			addTriple(eventSource, version, esPropertyDefaultGraphRevision, graphRevision);
		} else {
			addTriple(eventSource, version, esPropertyGraphRevision, graphRevision);
			addTriple(eventSource, graphRevision, esPropertyGraph, graph);
		}
		addTriple(eventSource, graphRevision, esPropertyRevision, revision);
	}
	
	private static void replaceNode(Graph graph, Node oldNode, Node newNode) {
		replaceSubject(graph, oldNode, newNode);
		replaceObject(graph, oldNode, newNode);
	}
	
	private static void replaceSubject(Graph graph, Node oldNode, Node newNode) {
		for (Iterator<Triple> it = graph.find(oldNode, Node.ANY, Node.ANY); it.hasNext(); ) {
			Triple triple = it.next();
			graph.add(new Triple(newNode, triple.getPredicate(), triple.getObject()));
		}
		graph.remove(oldNode, Node.ANY, Node.ANY);
	}

	private static void replaceObject(Graph graph, Node oldNode, Node newNode) {
		for (Iterator<Triple> it = graph.find(Node.ANY, Node.ANY, oldNode); it.hasNext(); ) {
			Triple triple = it.next();
			graph.add(new Triple(triple.getSubject(), triple.getPredicate(), newNode));
		}
		graph.remove(Node.ANY, Node.ANY, oldNode);
	}

	/**
	 * Write a revision to the log.
	 */
	private Node writeRevision(Delta delta, Node previousRevision) {
		String revId = UUID.randomUUID().toString();
		Node revisionId = NodeFactory.createURI(REVISION + revId);
		Node assertId = NodeFactory.createURI(ASSERT + revId);
		Node retractId = NodeFactory.createURI(RETRACT + revId);
		
		addTriple(d_datastore, revisionId, RDF.Nodes.type, esClassRevision);
		if (previousRevision != null) {
			addTriple(d_datastore, revisionId, esPropertyPrevious, previousRevision);
		}

		if (!delta.getAdditions().isEmpty()) {
			addTriple(d_datastore, revisionId, esPropertyAssertions, assertId);
			d_datastore.addGraph(assertId, skolemize(delta.getAdditions()));
		}
		if (!delta.getDeletions().isEmpty()) {
			addTriple(d_datastore, revisionId, esPropertyRetractions, retractId);
			d_datastore.addGraph(retractId, delta.getDeletions());
		}

		return revisionId;
	}

	private Graph skolemize(Graph graph) {
		Set<Node> blanks = new HashSet<Node>();
		
		for (Iterator<Node> nodes = GraphUtils.allNodes(graph); nodes.hasNext(); ) {
			Node node = nodes.next();
			if (node.isBlank()) {
				blanks.add(node);
			}
		}
		
		for (Node blank : blanks) {
			Node skolem;
			try {
				skolem = NodeFactory.createURI(SKOLEM + d_idgen.generateId(10).asString());
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			replaceNode(graph, blank, skolem);
		}
		
		return graph;
	}

	public Node createDatasetIfNotExists(Node dataset) {
		Transactional trans = (Transactional) d_datastore;

		trans.begin(ReadWrite.READ);
		boolean exists = d_datastore.getDefaultGraph().contains(dataset, RDF.Nodes.type, esClassDataset);
		trans.end();

		if (!exists) {
			return createDataset(dataset, null, GraphFactory.createGraphMem());
		}

		return getLatestVersionUri(dataset);
	}
	
	public Node createDataset(Node dataset, Graph defaultGraphContent, Graph meta) {
		Transactional trans = (Transactional) d_datastore;
		trans.begin(ReadWrite.WRITE);
		
		addTriple(d_datastore, dataset, RDF.Nodes.type, esClassDataset);
		Node version = NodeFactory.createURI(VERSION + UUID.randomUUID().toString());
		addTriple(d_datastore, dataset, esPropertyHead, version);
		addTriple(d_datastore, version, RDF.Nodes.type, esClassDatasetVersion);
		addTriple(d_datastore, version, esPropertyDataset, dataset);
		Node date = NodeFactory.createLiteral(now(), XSDDatatype.XSDdateTime);
		addTriple(d_datastore, version, dctermsDate, date);
		addTriple(d_datastore, dataset, dctermsDate, date);

		addMetaData(d_datastore, meta, version, esClassDatasetVersion);
		Node root = getMetaDataRoot(meta, esClassDatasetVersion);
		Node creator = root == null ? null : Util.getUniqueOptionalObject(meta.find(root, dctermsCreator, Node.ANY));
		if (creator != null) {
			addTriple(d_datastore, dataset, dctermsCreator, creator);
		}
		
		if (defaultGraphContent != null) {
			Delta delta = new Delta(GraphFactory.createGraphMem());
			GraphUtil.addInto(delta, defaultGraphContent);
			Node revision = writeRevision(delta, null);
			addGraphRevision(d_datastore, version, Quad.defaultGraphNodeGenerated, revision);
		}
		
		trans.commit();
		return version;
	}

	public String getDatasetUri(String id) {
		return getUriPrefix() + "/datasets/" + id;
	}

	public String getRetractionsUri(String id) {
		return RETRACT + id;
	}

	public String getAssertionsUri(String id) {
		return ASSERT + id;
	}

	public String getVersionUri(String id) {
		return VERSION + id;
	}
}
