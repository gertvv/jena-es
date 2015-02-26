package es;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

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
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.vocabulary.RDF;

@SuppressWarnings("deprecation")
public class EventSource {
	public static final String ES="http://drugis.org/eventSourcing/es#",
			DCTERMS="http://purl.org/dc/terms/";
	public static final Node esClassDataset = NodeFactory.createURI(ES + "Dataset"),
			esClassDatasetVersion = NodeFactory.createURI(ES + "DatasetVersion"),
			esClassRevision = NodeFactory.createURI(ES + "Revision"),
			esPropertyHead = NodeFactory.createURI(ES + "head"),
			esPropertyDefaultGraphRevision = NodeFactory.createURI(ES + "default_graph_revision"),
			esPropertyGraphRevision = NodeFactory.createURI(ES + "graph_revision"),
			esPropertyGraph = NodeFactory.createURI(ES + "graph"),
			esPropertyRevision = NodeFactory.createURI(ES + "revision"),
			esPropertyPrevious = NodeFactory.createURI(ES + "previous"),
			esPropertyAssertions = NodeFactory.createURI(ES + "assertions"),
			esPropertyRetractions = NodeFactory.createURI(ES + "retractions"),
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

	private DatasetGraph d_datastore;
	private String VERSION;
	private String REVISION;
	private String ASSERT;
	private String RETRACT;
	private String d_uriPrefix;
	
	public EventSource(DatasetGraph datastore, String uriPrefix) {
		d_datastore = datastore;
		d_uriPrefix = uriPrefix;
		
		VERSION = uriPrefix + "versions/";
		REVISION = uriPrefix + "revisions/";
		ASSERT = uriPrefix + "assert/";
		RETRACT = uriPrefix + "retract/";
	}
	
	public DatasetGraph getDataStore() {
		return d_datastore;
	}
	
	public String getUriPrefix() {
		return d_uriPrefix;
	}

	public Node getLatestVersionUri(Node dataset) {
		return getUniqueObject(d_datastore.getDefaultGraph().find(dataset, esPropertyHead, Node.ANY));
	}
	
	private static Node getUniqueOptionalObject(Iterator<Triple> result) {
		if (result.hasNext()) {
			Node object = result.next().getObject();
			if (result.hasNext()) {
				throw new IllegalStateException("Multiple subjects on property of arity 1");
			}
			return object;
		}
		return null;
	}
	
	private static Node getUniqueObject(Iterator<Triple> result) {
		Node object = getUniqueOptionalObject(result);
		if (object == null) {
			throw new IllegalStateException("Zero subjects on property of arity 1");
		}
		return object;
	}
	
	private static Map<Node, Node> getGraphRevisions(DatasetGraph eventSource, Node version) {
		Map<Node, Node> map = new HashMap<Node, Node>();
		for (Iterator<Triple> triples = eventSource.getDefaultGraph().find(version, esPropertyGraphRevision, Node.ANY); triples.hasNext(); ) {
			Node graphRevision = triples.next().getObject();
			Node graphName = getUniqueObject(eventSource.getDefaultGraph().find(graphRevision, esPropertyGraph, Node.ANY));
			Node revision = getUniqueObject(eventSource.getDefaultGraph().find(graphRevision, esPropertyRevision, Node.ANY));
			map.put(graphName, revision);
		}
		// TODO default graph
		return map;
	}

	public DatasetGraph getVersion(Node version) {
		DatasetGraph ds = DatasetGraphFactory.createMem();
		for (Map.Entry<Node, Node> entry : getGraphRevisions(d_datastore, version).entrySet()) {
			Node graphName = entry.getKey();
			Node revision = entry.getValue();
			Graph graph = getRevision(revision);
			ds.addGraph(graphName, graph);
		}
		return ds;
	}
	
	public DatasetGraph getLatestVersion(Node dataset) {
		return getVersion(getLatestVersionUri(dataset));
	}

	public Graph getRevision(Node revision) {
		Node previous = getUniqueOptionalObject(d_datastore.getDefaultGraph().find(revision, esPropertyPrevious, Node.ANY));
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
	 * @param meta A graph containing meta-data. It must contain a single blank node of class es:Event, the properties of which will be added to the event meta-data.
	 * @return The ID of the event.
	 */
	public Node writeToLog(Node dataset, DatasetGraphDelta event, Graph meta) {
		Node previous = getLatestVersionUri(dataset);
		Node version = NodeFactory.createURI(VERSION + UUID.randomUUID().toString());
		
		addTriple(d_datastore, version, RDF.Nodes.type, esClassDatasetVersion);
		addTriple(d_datastore, version, dctermsDate, NodeFactory.createLiteral(now(), XSDDatatype.XSDdateTime));
		
		addMetaData(d_datastore, meta, version, esClassDatasetVersion);

		Map<Node, Node> previousRevisions = getGraphRevisions(d_datastore, previous);
		for (Iterator<Node> graphs = event.listGraphNodes(); graphs.hasNext(); ) {
			Node graph = graphs.next();
			if (!event.getModifications().containsKey(graph)) {
				addGraphRevision(d_datastore, version, graph, previousRevisions.get(graph));
			} else if (!event.getGraph(graph).isEmpty()) {
				addGraphRevision(d_datastore, version, graph, writeRevision(event.getModifications().get(graph), graph, previousRevisions.get(graph)));
			}
		}
		
		addTriple(d_datastore, version, esPropertyPrevious, previous);
		d_datastore.getDefaultGraph().remove(dataset, esPropertyHead, previous);
		addTriple(d_datastore, dataset, esPropertyHead, version);
		
		return version;
	}

	/**
	 * Add meta-data to a resource. Filters the given meta-data graph.
	 */
	private static void addMetaData(DatasetGraph eventSource, Graph meta, Node resource, Node resourceClass) {
		// If meta-data is supplied, add it to the event log
		Set<Triple> metaRoots = meta.find(Node.ANY, RDF.Nodes.type, resourceClass).toSet();
		if (metaRoots.size() > 1) {
			throw new IllegalStateException(
					"The supplied meta-data must have at most one resource of class "
					+ resourceClass.getURI() + " but found " + metaRoots.size());
		} else if (metaRoots.size() == 1) {
			Node root = metaRoots.iterator().next().getSubject();
			
			// Restrict meta-data to describing the current event only
			meta = (new GraphExtract(TripleBoundary.stopNowhere)).extract(root, meta);
			
			// Prevent the setting of predicates we set ourselves
			meta.remove(root, RDF.Nodes.type, Node.ANY);
			meta.remove(root, EventSource.esPropertyHead, Node.ANY);
			meta.remove(root, EventSource.esPropertyPrevious, Node.ANY);
			meta.remove(root, EventSource.esPropertyGraphRevision, Node.ANY);
			meta.remove(root, EventSource.esPropertyDefaultGraphRevision, Node.ANY);
			meta.remove(root, EventSource.esPropertyRevision, Node.ANY);
			meta.remove(root, EventSource.esPropertyGraph, Node.ANY);
			meta.remove(root, EventSource.esPropertyAssertions, Node.ANY);
			meta.remove(root, EventSource.esPropertyRetractions, Node.ANY);
			meta.remove(root, EventSource.dctermsDate, Node.ANY);
			
			// Replace the temporary root node by the event URI
			replaceNode(meta, root, resource);
			
			// Copy the data into the event log
			GraphUtil.addInto(eventSource.getDefaultGraph(), meta);
		}
	}

	/**
	 * Add a graphRevision to a DatasetVersion.
	 * @param version The version to add this graphRevision to.
	 * @param graph The graph URI.
	 * @param revision The revision of the graph.
	 */
	private static void addGraphRevision(DatasetGraph eventSource, Node version, Node graph, Node revision) {
		Node graphRevision = NodeFactory.createAnon();
		addTriple(eventSource, version, esPropertyGraphRevision, graphRevision);
		addTriple(eventSource, graphRevision, esPropertyGraph, graph);
		addTriple(eventSource, graphRevision, esPropertyRevision, revision);
	}

	private static void replaceNode(Graph graph, Node oldNode, Node newNode) {
		for (Iterator<Triple> it = graph.find(oldNode, Node.ANY, Node.ANY); it.hasNext(); ) {
			Triple triple = it.next();
			graph.add(new Triple(newNode, triple.getPredicate(), triple.getObject()));
		}
		graph.remove(oldNode, Node.ANY, Node.ANY);
	}

	/**
	 * Write a revision to the log.
	 */
	private Node writeRevision(Delta delta, Node graph, Node previousRevision) {
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
			d_datastore.addGraph(assertId, delta.getAdditions());
		}
		if (!delta.getDeletions().isEmpty()) {
			addTriple(d_datastore, revisionId, esPropertyRetractions, retractId);
			d_datastore.addGraph(retractId, delta.getDeletions());
		}

		return revisionId;
	}

	public void createDatasetIfNotExists(Node dataset) {
		Transactional trans = (Transactional) d_datastore;

		trans.begin(ReadWrite.READ);
		boolean exists = d_datastore.getDefaultGraph().contains(dataset, RDF.Nodes.type, esClassDataset);
		trans.end();

		if (!exists) {
			trans.begin(ReadWrite.WRITE);
			addTriple(d_datastore, dataset, RDF.Nodes.type, esClassDataset);
			Node version = NodeFactory.createURI(VERSION + UUID.randomUUID().toString());
			addTriple(d_datastore, dataset, esPropertyHead, version);
			addTriple(d_datastore, version, RDF.Nodes.type, esClassDatasetVersion);
			Node date = NodeFactory.createLiteral(now(), XSDDatatype.XSDdateTime);
			addTriple(d_datastore, version, dctermsDate, date);
			addTriple(d_datastore, dataset, dctermsDate, date);
			trans.commit();
		}

	}
}
